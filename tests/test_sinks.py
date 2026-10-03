"""Function destinations and the filesystem sink.

The rule under test is the one HTTP backends get: a destination only ever sees the
normal end of an upload whose whole request validated. A sink written as "read every
chunk, then commit" must never commit a failed upload, whatever the failure.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import stat
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from starlette.applications import Starlette

from mcp_upload import (
    Destination,
    IncomingFile,
    MemoryStore,
    Registry,
    UploadAborted,
    UploadGateway,
)
from mcp_upload.sinks import Sink, filesystem
from mcp_upload.tickets import b64url
from tests.conftest import BASE_URL, multipart
from tests.test_live import Live, free_port, multipart_stream


@dataclass
class Recorder:
    """A sink that reads every chunk and commits only on a normal end, recording
    everything it saw along the way."""

    committed: dict[str, bytes] = field(default_factory=dict)
    received: dict[str, int] = field(default_factory=dict)
    aborted: dict[str, str] = field(default_factory=dict)
    seen: list[IncomingFile] = field(default_factory=list)

    async def __call__(self, upload: IncomingFile) -> None:
        self.seen.append(upload)
        data = bytearray()
        try:
            async for chunk in upload:
                data += chunk
                self.received[upload.record_id] = len(data)
        except UploadAborted as exc:
            self.aborted[upload.record_id] = exc.code
            raise
        self.committed[upload.record_id] = bytes(data)


def make_gateway(sink: Sink, **options: Any) -> UploadGateway:
    destination = {
        "timeout": options.pop("timeout", 60.0),
        "max_size": options.pop("max_size", 1 << 20),
    }
    return UploadGateway(
        base_url=BASE_URL,
        registry=Registry(Destination(name="sink", sink=sink, **destination)),
        store=MemoryStore(),
        **options,
    )


def client_for(gateway: UploadGateway) -> httpx.AsyncClient:
    app = Starlette(routes=gateway.routes())
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE_URL)


async def send(
    client: httpx.AsyncClient, url: str, data: bytes, *, filename: str = "a.bin", **kw: Any
) -> httpx.Response:
    body, content_type = multipart([("file", filename, data, kw.pop("media_type", None))])
    return await client.post(url, content=body, headers={"Content-Type": content_type})


def b64(data: bytes) -> str:
    return b64url(hashlib.sha256(data).hexdigest())


# ----- the destination itself ---------------------------------------------------------


def test_a_destination_is_a_url_or_a_sink_never_both_or_neither() -> None:
    async def sink(upload: IncomingFile) -> None:
        return None

    with pytest.raises(ValueError):
        Destination(name="both", url="http://x.test/", sink=sink)
    with pytest.raises(ValueError):
        Destination(name="neither")
    with pytest.raises(TypeError):
        Destination(name="junk", sink="not callable")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        Destination(name="s", sink=sink).build_url("id", "f")
    # Positional use from earlier releases still works.
    assert Destination("old", "http://x.test/{id}").build_url("a", "b") == "http://x.test/a"


# ----- the happy path ------------------------------------------------------------------


async def test_sink_receives_the_bytes_and_metadata() -> None:
    recorder = Recorder()
    gateway = make_gateway(recorder)
    data = os.urandom(700_000)
    issued = await gateway.issue("sink", expected_size=len(data), expected_digest=b64(data))
    async with client_for(gateway) as client:
        response = await send(
            client, issued.upload_url, data, filename="../../evil.bin", media_type="image/png"
        )
    assert response.status_code == 200, response.text
    assert response.json()["file"]["digest"]["value"] == b64(data)
    assert recorder.committed == {issued.record.id: data}
    seen = recorder.seen[0]
    assert (seen.record_id, seen.destination, seen.filename, seen.media_type) == (
        issued.record.id,
        "sink",
        "evil.bin",
        "image/png",
    )
    assert seen.expected_size == len(data)
    assert seen.expected_sha256 == hashlib.sha256(data).hexdigest()
    status = await gateway.status(issued.record.id)
    assert status["status"] == "completed"


async def test_a_slow_sink_slows_the_client_instead_of_filling_memory() -> None:
    release = asyncio.Event()
    received = 0

    async def slow(upload: IncomingFile) -> None:
        nonlocal received
        await release.wait()
        async for chunk in upload:
            received += len(chunk)

    gateway = make_gateway(slow, max_size=64 << 20)
    issued = await gateway.issue("sink")
    sent = 0
    total = 16 << 20

    async def body() -> AsyncIterator[bytes]:
        nonlocal sent
        boundary = "slowsink"
        yield (
            f"--{boundary}\r\n"
            'Content-Disposition: form-data; name="file"; filename="big.bin"\r\n\r\n'
        ).encode()
        block = b"x" * 65536
        while sent < total:
            sent += len(block)
            yield block
        yield f"\r\n--{boundary}--\r\n".encode()

    async with client_for(gateway) as client:
        task = asyncio.create_task(
            client.post(
                issued.upload_url,
                content=body(),
                headers={"Content-Type": "multipart/form-data; boundary=slowsink"},
            )
        )
        await asyncio.sleep(0.3)
        held = sent
        release.set()
        response = await task
    assert response.status_code == 200, response.text
    assert received == total
    # Four queued pieces of 256 KiB, one more in the parser, and a few reads in flight.
    assert held < 3 << 20, f"read {held} bytes from the client while the sink read none"


# ----- failures never look like a normal end ------------------------------------------
#
# The files here are larger than the forwarding queue holds, so the sink is already
# reading when the failure happens. (A failure the gateway sees before the sink is
# called never calls it, as a backend then never sees a request.)


async def test_too_large_aborts_the_sink() -> None:
    recorder = Recorder()
    gateway = make_gateway(recorder, max_size=2_000_000)
    issued = await gateway.issue("sink")
    async with client_for(gateway) as client:
        # Just over the limit, so the declared length passes the early check and the
        # running count is what refuses it.
        response = await send(client, issued.upload_url, os.urandom(2_050_000))
    assert response.status_code == 413
    assert recorder.committed == {}
    assert recorder.aborted == {issued.record.id: "too_large"}
    assert (await gateway.status(issued.record.id))["error"] == "too_large"


@pytest.mark.parametrize(
    ("declared", "code"),
    [
        ({"expected_digest": b64(b"something else")}, "digest_mismatch"),
        ({"expected_size": 3_000_001}, "size_mismatch"),
    ],
)
async def test_declared_mismatch_aborts_after_every_byte_arrived(
    declared: dict[str, Any], code: str
) -> None:
    recorder = Recorder()
    gateway = make_gateway(recorder, max_size=None)
    data = os.urandom(3_000_000)
    issued = await gateway.issue("sink", **declared)
    async with client_for(gateway) as client:
        response = await send(client, issued.upload_url, data)
    assert response.status_code == 422
    assert response.json()["error"] == code
    # The sink had read most of the file when the check at the end refused it. Chunks
    # still queued at that moment are not handed out.
    assert recorder.received[issued.record.id] > len(data) // 2
    assert recorder.aborted == {issued.record.id: code}
    assert recorder.committed == {}


async def test_something_after_the_file_part_aborts_the_sink() -> None:
    recorder = Recorder()
    gateway = make_gateway(recorder, max_size=None)
    issued = await gateway.issue("sink")
    body, content_type = multipart(
        [("file", "a.bin", os.urandom(3_000_000), None), ("file", "b.bin", b"EVIL", None)]
    )
    async with client_for(gateway) as client:
        response = await client.post(
            issued.upload_url, content=body, headers={"Content-Type": content_type}
        )
    assert response.json()["error"] == "duplicate_file"
    assert recorder.aborted == {issued.record.id: "duplicate_file"}
    assert recorder.committed == {}


async def test_a_stalled_client_aborts_the_sink() -> None:
    recorder = Recorder()
    gateway = make_gateway(recorder, stall_timeout=timedelta(seconds=0.2))
    issued = await gateway.issue("sink")

    async def stalls() -> AsyncIterator[bytes]:
        yield (
            b'--s\r\nContent-Disposition: form-data; name="file"; filename="a.bin"\r\n\r\n'
        ) + b"x" * 300_000
        await asyncio.sleep(1.0)
        yield b"\r\n--s--\r\n"

    async with client_for(gateway) as client:
        response = await client.post(
            issued.upload_url,
            content=stalls(),
            headers={"Content-Type": "multipart/form-data; boundary=s"},
        )
    assert response.json()["error"] == "too_slow"
    assert recorder.aborted == {issued.record.id: "too_slow"}
    assert recorder.committed == {}


async def test_client_disconnect_aborts_the_sink_over_real_sockets() -> None:
    recorder = Recorder()
    port = free_port()
    gateway = UploadGateway(
        base_url=f"http://127.0.0.1:{port}",
        registry=Registry(Destination(name="sink", sink=recorder)),
        store=MemoryStore(),
    )
    server = Live(Starlette(routes=gateway.routes()))
    server.port = port
    server.server.config.port = port
    async with server, httpx.AsyncClient(timeout=30) as client:
        issued = await gateway.issue("sink")
        body, content_type, _ = multipart_stream(64 * 65536, fail_after=8)
        with pytest.raises((ConnectionResetError, httpx.HTTPError)):
            await client.post(
                issued.upload_url, content=body, headers={"Content-Type": content_type}
            )
        for _ in range(200):
            if (await gateway.status(issued.record.id))["status"] == "failed":
                break
            await asyncio.sleep(0.02)
    status = await gateway.status(issued.record.id)
    assert status["error"] in ("client_disconnected", "truncated")
    assert recorder.aborted == {issued.record.id: status["error"]}
    assert recorder.committed == {}


# ----- the sink misbehaving -------------------------------------------------------------


async def test_a_sink_that_raises_fails_the_upload_without_echoing_it() -> None:
    async def broken(upload: IncomingFile) -> None:
        async for _ in upload:
            raise RuntimeError("database password is hunter2")

    gateway = make_gateway(broken, max_size=None)
    issued = await gateway.issue("sink")
    async with client_for(gateway) as client:
        response = await send(client, issued.upload_url, os.urandom(2_000_000))
    assert response.status_code == 502
    assert response.json() == {"status": "failed", "error": "sink_failed", "id": issued.record.id}
    assert "hunter2" not in response.text
    assert (await gateway.status(issued.record.id))["error"] == "sink_failed"


@pytest.mark.parametrize("reads", [0, 1])
@pytest.mark.parametrize("size", [10, 3_000_000])
async def test_a_sink_that_returns_early_fails_the_upload(reads: int, size: int) -> None:
    async def quitter(upload: IncomingFile) -> None:
        for _ in range(reads):
            await upload.__anext__()

    gateway = make_gateway(quitter, max_size=None)
    issued = await gateway.issue("sink")
    async with client_for(gateway) as client:
        response = await send(client, issued.upload_url, os.urandom(size))
    assert response.status_code == 502
    assert response.json()["error"] == "upstream_closed_early"


async def test_a_sink_that_swallows_the_abort_still_fails_with_the_real_cause() -> None:
    committed: list[bytes] = []

    async def careless(upload: IncomingFile) -> None:
        data = b""
        with pytest.raises(UploadAborted):
            async for chunk in upload:
                data += chunk
        # Reading again keeps failing; there is no way to reach a normal end.
        with pytest.raises(UploadAborted):
            await upload.__anext__()
        committed.append(data)

    gateway = make_gateway(careless, max_size=1000)
    issued = await gateway.issue("sink")
    async with client_for(gateway) as client:
        response = await send(client, issued.upload_url, os.urandom(5000))
    assert response.json()["error"] == "too_large"
    assert (await gateway.status(issued.record.id))["error"] == "too_large"


async def test_a_sink_stuck_after_the_upload_is_cut_off() -> None:
    async def stuck(upload: IncomingFile) -> None:
        async for _ in upload:
            pass
        await asyncio.Event().wait()

    gateway = make_gateway(stuck, timeout=0.2)
    issued = await gateway.issue("sink")
    async with client_for(gateway) as client:
        response = await asyncio.wait_for(send(client, issued.upload_url, b"hello"), 5)
    assert response.status_code == 502
    assert response.json()["error"] == "sink_failed"


async def test_a_failure_before_the_file_part_never_calls_the_sink() -> None:
    recorder = Recorder()
    gateway = make_gateway(recorder)
    issued = await gateway.issue("sink")
    body, content_type = multipart([("other", None, b"x", None)])
    async with client_for(gateway) as client:
        response = await client.post(
            issued.upload_url, content=body, headers={"Content-Type": content_type}
        )
    assert response.status_code == 400
    assert recorder.seen == []


# ----- the filesystem sink --------------------------------------------------------------


def listing(directory: Path) -> list[str]:
    return sorted(p.name for p in directory.iterdir())


async def test_filesystem_sink_writes_exactly_the_bytes(tmp_path: Path) -> None:
    gateway = make_gateway(filesystem(tmp_path), max_size=None)
    data = os.urandom(3_000_000)
    issued = await gateway.issue("sink")
    async with client_for(gateway) as client:
        response = await send(client, issued.upload_url, data, filename="../report.pdf")
    assert response.status_code == 200, response.text
    name = f"{issued.record.id}-report.pdf"
    assert listing(tmp_path) == [name]
    final = tmp_path / name
    assert final.read_bytes() == data
    assert stat.S_IMODE(final.stat().st_mode) == 0o600


async def test_filesystem_sink_without_fsync(tmp_path: Path) -> None:
    gateway = make_gateway(filesystem(tmp_path, fsync=False, name_template="{filename}"))
    issued = await gateway.issue("sink")
    async with client_for(gateway) as client:
        response = await send(client, issued.upload_url, b"plain", filename="p.txt")
    assert response.status_code == 200
    assert (tmp_path / "p.txt").read_bytes() == b"plain"


@pytest.mark.parametrize(
    ("issue_kw", "size", "code"),
    [
        ({"max_size": 2_000_000}, 2_050_000, "too_large"),
        ({"expected_digest": b64(b"other")}, 3_000_000, "digest_mismatch"),
        ({"expected_size": 3_000_001}, 3_000_000, "size_mismatch"),
    ],
)
async def test_filesystem_sink_leaves_nothing_after_a_failure(
    tmp_path: Path, issue_kw: dict[str, Any], size: int, code: str
) -> None:
    sink = filesystem(tmp_path)
    called: list[str] = []

    async def counted(upload: IncomingFile) -> None:
        called.append(upload.record_id)
        await sink(upload)

    gateway = make_gateway(counted, max_size=None)
    issued = await gateway.issue("sink", **issue_kw)
    async with client_for(gateway) as client:
        response = await send(client, issued.upload_url, os.urandom(size))
    assert response.json()["error"] == code
    assert called == [issued.record.id]
    await asyncio.sleep(0.05)  # cleanup of a write still in a thread runs after it
    assert listing(tmp_path) == []


async def test_filesystem_sink_leaves_nothing_after_a_disconnect(tmp_path: Path) -> None:
    port = free_port()
    gateway = UploadGateway(
        base_url=f"http://127.0.0.1:{port}",
        registry=Registry(Destination(name="sink", sink=filesystem(tmp_path))),
        store=MemoryStore(),
    )
    server = Live(Starlette(routes=gateway.routes()))
    server.port = port
    server.server.config.port = port
    async with server, httpx.AsyncClient(timeout=30) as client:
        issued = await gateway.issue("sink")
        body, content_type, _ = multipart_stream(64 * 65536, fail_after=20)
        with pytest.raises((ConnectionResetError, httpx.HTTPError)):
            await client.post(
                issued.upload_url, content=body, headers={"Content-Type": content_type}
            )
        for _ in range(200):
            if (await gateway.status(issued.record.id))["status"] == "failed":
                break
            await asyncio.sleep(0.02)
    await asyncio.sleep(0.05)
    assert (await gateway.status(issued.record.id))["status"] == "failed"
    assert listing(tmp_path) == []


async def test_filesystem_sink_never_replaces_an_existing_file(tmp_path: Path) -> None:
    (tmp_path / "taken.txt").write_bytes(b"original")
    gateway = make_gateway(filesystem(tmp_path, name_template="{filename}"))
    issued = await gateway.issue("sink")
    async with client_for(gateway) as client:
        response = await send(client, issued.upload_url, b"replacement", filename="taken.txt")
    assert response.json()["error"] == "sink_failed"
    assert listing(tmp_path) == ["taken.txt"]
    assert (tmp_path / "taken.txt").read_bytes() == b"original"


async def test_filesystem_sink_can_overwrite_when_asked(tmp_path: Path) -> None:
    (tmp_path / "taken.txt").write_bytes(b"original")
    sink = filesystem(tmp_path, name_template="{filename}", overwrite=True)
    gateway = make_gateway(sink)
    issued = await gateway.issue("sink")
    async with client_for(gateway) as client:
        response = await send(client, issued.upload_url, b"replacement", filename="taken.txt")
    assert response.status_code == 200
    assert (tmp_path / "taken.txt").read_bytes() == b"replacement"


@pytest.mark.parametrize("template", ["../{filename}", "sub/{filename}", "..", "{filename}/x"])
async def test_filesystem_sink_refuses_to_leave_its_directory(
    tmp_path: Path, template: str
) -> None:
    inside = tmp_path / "inside"
    inside.mkdir()
    gateway = make_gateway(filesystem(inside, name_template=template))
    issued = await gateway.issue("sink")
    async with client_for(gateway) as client:
        response = await send(client, issued.upload_url, b"data", filename="a.txt")
    assert response.json()["error"] == "sink_failed"
    assert listing(tmp_path) == ["inside"]
    assert listing(inside) == []


async def test_filesystem_sink_removes_the_file_if_the_directory_sync_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_fsync = os.fsync

    def fsync(fd: int) -> None:
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError("disk on fire")
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", fsync)
    gateway = make_gateway(filesystem(tmp_path))
    issued = await gateway.issue("sink")
    async with client_for(gateway) as client:
        response = await send(client, issued.upload_url, b"data")
    assert response.json()["error"] == "sink_failed"
    assert listing(tmp_path) == []


def test_filesystem_sink_needs_an_existing_directory(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        filesystem(tmp_path / "missing")
    (tmp_path / "file").write_bytes(b"")
    with pytest.raises(ValueError):
        filesystem(tmp_path / "file")


async def test_filesystem_writes_do_not_run_on_the_event_loop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    loop_thread: list[bool] = []
    real_write: Callable[[int, Any], int] = os.write

    def write(fd: int, data: Any) -> int:
        try:
            asyncio.get_running_loop()
            loop_thread.append(True)
        except RuntimeError:
            loop_thread.append(False)
        return real_write(fd, data)

    monkeypatch.setattr(os, "write", write)
    gateway = make_gateway(filesystem(tmp_path), max_size=None)
    issued = await gateway.issue("sink")
    async with client_for(gateway) as client:
        response = await send(client, issued.upload_url, os.urandom(1_000_000))
    assert response.status_code == 200
    assert loop_thread and not any(loop_thread)


async def test_filesystem_sink_cut_off_mid_write_cleans_up_after_the_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The disk is slow and the sink is given 0.2 s once the body has arrived, so it is
    # cancelled while a write is running in its thread. The descriptor must stay open
    # until that write returns, and the temporary file must then go.
    real_write = os.write

    def slow_write(fd: int, data: Any) -> int:
        time.sleep(0.15)
        return real_write(fd, data)

    monkeypatch.setattr(os, "write", slow_write)
    gateway = make_gateway(filesystem(tmp_path), max_size=None, timeout=0.2)
    issued = await gateway.issue("sink")
    async with client_for(gateway) as client:
        response = await send(client, issued.upload_url, os.urandom(1_500_000))
    assert response.json()["error"] == "sink_failed"
    for _ in range(50):
        if not listing(tmp_path):
            break
        await asyncio.sleep(0.05)
    assert listing(tmp_path) == []
