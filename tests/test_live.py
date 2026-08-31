"""Behaviour that only shows up over real sockets.

Everything runs in-process on loopback: a uvicorn server for the gateway, another for
a backend, and an httpx client. These cover what the mocked-transport tests cannot:
memory staying flat while a slow backend applies backpressure, a client that vanishes
mid-upload, a chunked upload with no Content-Length, and a backend that answers before
it has read the body.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import resource
import socket
import sys
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import httpx
import pytest
import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from mcp_upload import Destination, MemoryStore, Registry, UploadGateway

CHUNK = 64 * 1024


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class Live:
    """A uvicorn server on a free loopback port, started and stopped inside the test."""

    def __init__(self, app: Any) -> None:
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        config = uvicorn.Config(
            app, host="127.0.0.1", port=self.port, log_level="critical", lifespan="off"
        )
        self.server = uvicorn.Server(config)
        self.task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> Live:
        self.task = asyncio.create_task(self.server.serve())
        while not self.server.started:
            await asyncio.sleep(0.01)
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.server.should_exit = True
        if self.task is not None:
            await self.task


@dataclass
class Backend:
    """Records complete uploads, counts incomplete ones, and can be made slow or rude."""

    delay: float = 0.0
    answer_early: int | None = None
    received: list[dict[str, Any]] = field(default_factory=list)
    incomplete: int = 0

    def app(self) -> Starlette:
        async def put(request: Request) -> JSONResponse:
            if self.answer_early is not None:
                return JSONResponse({"early": True}, status_code=self.answer_early)
            size = 0
            digest = hashlib.sha256()
            try:
                async for chunk in request.stream():
                    size += len(chunk)
                    digest.update(chunk)
                    if self.delay:
                        await asyncio.sleep(self.delay)
            except BaseException:
                self.incomplete += 1
                raise
            self.received.append(
                {"name": request.path_params["name"], "size": size, "sha256": digest.hexdigest()}
            )
            return JSONResponse({"ok": True}, status_code=201)

        return Starlette(routes=[Route("/files/{name}", put, methods=["PUT"])])


@dataclass
class Stack:
    gateway: UploadGateway
    backend: Backend
    client: httpx.AsyncClient


@pytest.fixture
async def stack() -> AsyncIterator[Stack]:
    backend = Backend()
    async with Live(backend.app()) as backend_server:
        registry = Registry(
            Destination(
                name="files",
                url=f"{backend_server.url}/files/{{filename}}",
                method="PUT",
                max_size=256 * 1024 * 1024,
                timeout=30.0,
            )
        )
        port = free_port()
        gateway = UploadGateway(
            base_url=f"http://127.0.0.1:{port}",
            registry=registry,
            store=MemoryStore(),
            server_name="live",
        )
        app = Starlette(routes=gateway.routes())
        gateway_server = Live(app)
        gateway_server.port = port
        gateway_server.url = f"http://127.0.0.1:{port}"
        gateway_server.server.config.port = port
        async with gateway_server, httpx.AsyncClient(timeout=30.0) as client:
            yield Stack(gateway, backend, client)
        await gateway.aclose()


def multipart_stream(
    total: int, *, boundary: str = "liveboundary", fail_after: int | None = None
) -> tuple[AsyncIterator[bytes], str, hashlib._Hash]:
    """A multipart body produced on the fly, so neither the client nor the test holds
    the file in memory. Returns the generator, the content type, and a hasher that
    accumulates the file bytes as they are produced."""
    digest = hashlib.sha256()

    async def gen() -> AsyncIterator[bytes]:
        yield (
            f"--{boundary}\r\n"
            'Content-Disposition: form-data; name="file"; filename="big.bin"\r\n'
            "Content-Type: application/octet-stream\r\n\r\n"
        ).encode()
        sent = 0
        chunks = 0
        while sent < total:
            if fail_after is not None and chunks >= fail_after:
                raise ConnectionResetError("client gave up")
            chunk = os.urandom(min(CHUNK, total - sent))
            digest.update(chunk)
            sent += len(chunk)
            chunks += 1
            yield chunk
        yield f"\r\n--{boundary}--\r\n".encode()

    return gen(), f"multipart/form-data; boundary={boundary}", digest


def max_rss_bytes() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if sys.platform == "darwin" else value * 1024


async def wait_for_status(gateway: UploadGateway, record_id: str, wanted: str) -> dict[str, Any]:
    for _ in range(200):
        status = await gateway.status(record_id)
        if status["status"] == wanted:
            return dict(status)
        await asyncio.sleep(0.02)
    raise AssertionError(f"record never reached {wanted}: {await gateway.status(record_id)}")


async def test_memory_stays_flat_under_a_slow_backend(stack: Stack) -> None:
    # The backend sleeps on every chunk it receives. With real backpressure the
    # gateway holds a handful of chunks and the client stalls. Without it the gateway
    # buffers the difference, and the process grows by about the size of the upload.
    stack.backend.delay = 0.002
    size = 32 * 1024 * 1024
    issued = await stack.gateway.issue("files")
    body, content_type, digest = multipart_stream(size)

    before = max_rss_bytes()
    response = await stack.client.post(
        issued.upload_url, content=body, headers={"Content-Type": content_type}
    )
    growth = max_rss_bytes() - before
    print(f"process grew {growth / 2**20:.1f} MiB while streaming {size / 2**20:.0f} MiB")

    assert response.status_code == 200, response.text
    reported = response.json()["file"]
    assert reported["size"] == size
    assert reported["digest"]["value"] == digest.hexdigest()
    assert stack.backend.received == [
        {"name": "big.bin", "size": size, "sha256": digest.hexdigest()}
    ]
    assert growth < size // 2, (
        f"process grew by {growth / 2**20:.1f} MiB on a {size / 2**20:.0f} MiB upload"
    )


async def test_chunked_upload_with_no_content_length_is_capped(stack: Stack) -> None:
    issued = await stack.gateway.issue("files", max_size=100_000)
    body, content_type, _ = multipart_stream(400_000)
    # httpx sends a generator body with Transfer-Encoding: chunked and no Content-Length,
    # so only the running counter can enforce the limit. The server may answer while the
    # client is still writing, which some stacks surface as a write error rather than a
    # response; the record is the authoritative check either way.
    try:
        response = await stack.client.post(
            issued.upload_url, content=body, headers={"Content-Type": content_type}
        )
        assert response.status_code == 413
        assert response.json()["error"] == "too_large"
    except httpx.HTTPError:
        pass
    status = await wait_for_status(stack.gateway, issued.record.id, "failed")
    assert status["error"] == "too_large"
    assert stack.backend.received == []


async def test_client_disconnect_mid_upload_is_recorded_and_not_committed(stack: Stack) -> None:
    issued = await stack.gateway.issue("files")
    body, content_type, _ = multipart_stream(64 * CHUNK, fail_after=8)
    with pytest.raises((ConnectionResetError, httpx.HTTPError)):
        await stack.client.post(
            issued.upload_url, content=body, headers={"Content-Type": content_type}
        )
    status = await wait_for_status(stack.gateway, issued.record.id, "failed")
    assert status["error"] in ("client_disconnected", "truncated")
    assert stack.backend.received == []


async def test_backend_that_answers_before_reading_does_not_hang(stack: Stack) -> None:
    stack.backend.answer_early = 413
    issued = await stack.gateway.issue("files")
    body, content_type, _ = multipart_stream(8 * 1024 * 1024)
    try:
        response = await asyncio.wait_for(
            stack.client.post(
                issued.upload_url, content=body, headers={"Content-Type": content_type}
            ),
            timeout=15,
        )
        assert response.status_code == 502
    except httpx.HTTPError:
        pass
    status = await wait_for_status(stack.gateway, issued.record.id, "failed")
    assert status["error"] in ("upstream_rejected", "upstream_unreachable", "upstream_closed_early")
    assert stack.backend.received == []
