"""End-to-end stress harness for mcp-upload.

Every scenario starts a fresh backend process and a fresh gateway process, drives them
over real loopback sockets with a hand-written HTTP/1.1 client (so the harness, not a
library, decides exactly what goes on the wire and when), and samples the gateway's
resident memory from outside with ``ps``. Results are written as JSON so two runs can
be compared.

    python stress/run.py --src path/to/src --out results.json [--only name,...]

``--src`` picks which copy of the library the gateway imports, so the same harness
runs against a released tree and a working tree.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import os
import random
import socket
import statistics
import subprocess
import sys
import threading
import time
from collections import Counter
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
MB = 1 << 20
RSS_KILL_MB = 6000  # protect the machine: the gateway is killed past this


# ----- processes ------------------------------------------------------------------------


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_port(port: int, timeout: float = 15.0) -> None:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        with socket.socket() as sock:
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.05)
    raise RuntimeError(f"port {port} never opened")


class RssWatch(threading.Thread):
    """Samples a process's RSS every 50 ms and kills it past RSS_KILL_MB."""

    def __init__(self, proc: subprocess.Popen[bytes]) -> None:
        super().__init__(daemon=True)
        self.proc = proc
        self.peak_mb = 0.0
        self.base_mb = 0.0
        self.killed = False
        self._stop = threading.Event()

    def sample(self) -> float:
        out = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(self.proc.pid)], capture_output=True, text=True
        ).stdout.strip()
        return int(out) / 1024 if out else 0.0

    def run(self) -> None:
        while not self._stop.is_set() and self.proc.poll() is None:
            mb = self.sample()
            self.peak_mb = max(self.peak_mb, mb)
            if mb > RSS_KILL_MB:
                self.killed = True
                self.proc.kill()
                return
            time.sleep(0.05)

    def stop(self) -> None:
        self._stop.set()


@dataclass
class Stack:
    gateway_port: int
    backend_port: int
    gateway: subprocess.Popen[bytes]
    backend: subprocess.Popen[bytes]
    watch: RssWatch

    @property
    def backend_url(self) -> str:
        return f"http://127.0.0.1:{self.backend_port}"


def start(src: str, **gateway_args: Any) -> Stack:
    env = {**os.environ, "PYTHONPATH": src}
    bport, gport = free_port(), free_port()
    backend = subprocess.Popen([sys.executable, str(HERE / "backend.py"), str(bport)], env=env)
    wait_port(bport)
    cmd = [
        sys.executable,
        str(HERE / "gateway.py"),
        "--port",
        str(gport),
        "--backend",
        f"http://127.0.0.1:{bport}",
    ]
    for key, value in gateway_args.items():
        if value is not None:
            cmd += [f"--{key.replace('_', '-')}", str(value)]
    gateway = subprocess.Popen(cmd, env=env)
    wait_port(gport)
    watch = RssWatch(gateway)
    watch.base_mb = watch.sample()
    watch.start()
    return Stack(gport, bport, gateway, backend, watch)


def stop(stack: Stack) -> None:
    stack.watch.stop()
    for proc in (stack.gateway, stack.backend):
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(5)
            except subprocess.TimeoutExpired:
                proc.kill()


# ----- a minimal HTTP/1.1 client --------------------------------------------------------


@dataclass
class Reply:
    status: int
    body: dict[str, Any]
    elapsed: float
    sent: int
    error: str | None = None


Body = Callable[[], AsyncIterator[bytes]]

# macOS caps a listen backlog at 128. Opening hundreds of connections in the same instant
# measures the kernel, not the gateway, so connection attempts are paced.
_CONNECTS: asyncio.Semaphore | None = None


def connect_gate() -> asyncio.Semaphore:
    global _CONNECTS
    if _CONNECTS is None:
        _CONNECTS = asyncio.Semaphore(32)
    return _CONNECTS


async def http(
    port: int,
    method: str,
    path: str,
    *,
    headers: dict[str, str] | None = None,
    body: Body | bytes | None = None,
    timeout: float = 300.0,
    abort_after: int | None = None,
) -> Reply:
    """Send one request. A streamed body goes out chunked. The response is read
    concurrently, so a server that answers early stops the upload, like a real
    client would. ``abort_after`` closes the socket mid-body to simulate a client
    that vanishes."""
    start_t = time.perf_counter()
    sent = 0
    try:
        async with connect_gate():
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", port), timeout=30
            )
    except Exception as exc:
        return Reply(0, {}, time.perf_counter() - start_t, 0, f"connect: {exc!r}")
    head = {"Host": f"127.0.0.1:{port}", "Connection": "close", **(headers or {})}
    if isinstance(body, bytes):
        head["Content-Length"] = str(len(body))
    elif body is not None:
        head["Transfer-Encoding"] = "chunked"
    lines = [f"{method} {path} HTTP/1.1"] + [f"{k}: {v}" for k, v in head.items()]
    writer.write(("\r\n".join(lines) + "\r\n\r\n").encode())

    async def read_reply() -> tuple[int, dict[str, Any]]:
        status_line = await reader.readline()
        status = int(status_line.split()[1]) if status_line else 0
        length = None
        while True:
            line = await reader.readline()
            if line in (b"\r\n", b""):
                break
            name, _, value = line.decode().partition(":")
            if name.lower() == "content-length":
                length = int(value.strip())
        raw = await (reader.readexactly(length) if length is not None else reader.read())
        try:
            return status, json.loads(raw) if raw else {}
        except ValueError:
            return status, {"raw": raw[:200].decode(errors="replace")}

    reply_task = asyncio.create_task(read_reply())
    error = None
    try:
        if isinstance(body, bytes):
            writer.write(body)
            sent = len(body)
            await writer.drain()
        elif body is not None:
            async for chunk in body():
                if reply_task.done() or writer.transport.is_closing():
                    break
                if abort_after is not None and sent >= abort_after:
                    writer.transport.abort()
                    return Reply(0, {}, time.perf_counter() - start_t, sent, "aborted")
                writer.write(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
                sent += len(chunk)
                await writer.drain()
            if not reply_task.done():
                writer.write(b"0\r\n\r\n")
                await writer.drain()
        status, data = await asyncio.wait_for(reply_task, timeout)
        return Reply(status, data, time.perf_counter() - start_t, sent)
    except Exception as exc:
        error = repr(exc)
        # A server that refuses early answers and closes while the body is still going
        # out, so the write fails. The answer is usually already readable.
        with contextlib.suppress(Exception):
            await asyncio.wait_for(asyncio.shield(reply_task), 5)
        if reply_task.done() and not reply_task.cancelled() and reply_task.exception() is None:
            status, data = reply_task.result()
            return Reply(status, data, time.perf_counter() - start_t, sent, error)
        reply_task.cancel()
        return Reply(0, {}, time.perf_counter() - start_t, sent, error)
    finally:
        writer.transport.abort()


async def get_json(port: int, path: str) -> dict[str, Any]:
    return (await http(port, "GET", path)).body


async def post_json(port: int, path: str, data: dict[str, Any]) -> Reply:
    return await http(
        port,
        "POST",
        path,
        headers={"Content-Type": "application/json"},
        body=json.dumps(data).encode(),
    )


async def issue(stack: Stack, **kw: Any) -> dict[str, Any]:
    return (await post_json(stack.gateway_port, "/_issue", kw)).body


async def backend(stack: Stack) -> dict[str, Any]:
    return await get_json(stack.backend_port, "/_stats")


async def configure_backend(stack: Stack, **kw: Any) -> None:
    await post_json(stack.backend_port, "/_config", kw)


# ----- multipart bodies -----------------------------------------------------------------

BOUNDARY = "stressboundary7f3a"
CT = {"Content-Type": f"multipart/form-data; boundary={BOUNDARY}"}


def part_head(filename: str = "f.bin", media_type: str = "application/octet-stream") -> bytes:
    return (
        f"--{BOUNDARY}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
        f"Content-Type: {media_type}\r\n\r\n"
    ).encode()


TAIL = f"\r\n--{BOUNDARY}--\r\n".encode()


def file_body(
    size: int,
    *,
    chunk: int = 256 * 1024,
    seed: int = 0,
    media_type: str = "application/octet-stream",
    epilogue: bytes = b"",
    delay: float = 0.0,
) -> tuple[Body, str]:
    """A streamed file of ``size`` pseudo-random bytes and its SHA-256."""
    rng = random.Random(seed)
    block = rng.randbytes(min(chunk, max(size, 1)))
    digest = hashlib.sha256()
    left = size
    while left:
        n = min(len(block), left)
        digest.update(block[:n])
        left -= n

    async def gen() -> AsyncIterator[bytes]:
        yield part_head(media_type=media_type)
        left = size
        while left:
            n = min(len(block), left)
            yield block[:n]
            left -= n
            if delay:
                await asyncio.sleep(delay)
        yield TAIL + epilogue

    return gen, digest.hexdigest()


async def upload(stack: Stack, body: Body, *, retries: int = 0, **kw: Any) -> Reply:
    """Mint a ticket and upload. A 503 leaves the ticket untouched, so an honest client
    retries the same ticket after a pause, which ``retries`` allows."""
    ticket = await issue(stack)
    for attempt in range(retries + 1):
        reply = await http(stack.gateway_port, "POST", ticket["path"], headers=CT, body=body, **kw)
        if reply.status != 503 or attempt == retries:
            break
        await asyncio.sleep(0.25 + random.random() * 0.5)
    reply.body.setdefault("id", ticket.get("id"))
    return reply


def outcomes(replies: list[Reply]) -> dict[str, int]:
    return dict(
        Counter(
            r.body.get("error") or r.body.get("status") or r.error or str(r.status) for r in replies
        )
    )


def pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    return values[min(len(values) - 1, int(p / 100 * len(values)))]


# ----- scenarios ------------------------------------------------------------------------

Scenario = Callable[[str], Awaitable[dict[str, Any]]]
SCENARIOS: dict[str, Scenario] = {}


def scenario(fn: Scenario) -> Scenario:
    SCENARIOS[fn.__name__] = fn
    return fn


def finish(stack: Stack, result: dict[str, Any]) -> dict[str, Any]:
    stop(stack)
    result["gateway_peak_rss_mb"] = round(stack.watch.peak_mb)
    result["gateway_rss_growth_mb"] = round(stack.watch.peak_mb - stack.watch.base_mb)
    result["gateway_killed_for_memory"] = stack.watch.killed
    return result


@scenario
async def throughput_single(src: str) -> dict[str, Any]:
    """One 1 GiB upload. Regression guard for the happy path."""
    stack = start(src)
    body, digest = file_body(1024 * MB, chunk=256 * 1024)
    reply = await upload(stack, body)
    stats = await backend(stack)
    commit = stats["commits"].get(reply.body.get("id"), {})
    return finish(
        stack,
        {
            "outcome": reply.body.get("status") or reply.body.get("error"),
            "mib_per_s": round(1024 / reply.elapsed),
            "integrity_ok": commit.get("sha256") == digest,
        },
    )


@scenario
async def throughput_concurrent(src: str) -> dict[str, Any]:
    """200 uploads of 8 MiB at once, cap set to 256. Aggregate throughput and memory."""
    stack = start(src, max_in_flight=256)
    bodies = [file_body(8 * MB, seed=i) for i in range(200)]
    t = time.perf_counter()
    replies = await asyncio.gather(*(upload(stack, b, retries=20) for b, _ in bodies))
    wall = time.perf_counter() - t
    stats = await backend(stack)
    bad = sum(
        1
        for r, (_, d) in zip(replies, bodies, strict=True)
        if r.body.get("status") == "completed"
        and stats["commits"].get(r.body["id"], {}).get("sha256") != d
    )
    return finish(
        stack,
        {
            "outcomes": outcomes(replies),
            "aggregate_mib_per_s": round(200 * 8 / wall),
            "p99_latency_s": round(pct([r.elapsed for r in replies], 99), 2),
            "integrity_violations": bad,
        },
    )


@scenario
async def pool_ceiling(src: str) -> dict[str, Any]:
    """250 uploads, cap set to 250, backend holds every request 3 s. Does the gateway
    really run 250 at once, or does a hidden limit queue them after the ticket is spent?"""
    stack = start(src, max_in_flight=250)
    await configure_backend(stack, delay=3.0)
    t = time.perf_counter()
    replies = await asyncio.gather(
        *(upload(stack, file_body(4096, seed=i)[0], retries=20) for i in range(250))
    )
    wall = time.perf_counter() - t
    stats = await backend(stack)
    return finish(
        stack,
        {
            "outcomes": outcomes(replies),
            "backend_peak_concurrency": stats["peak"],
            "wall_s": round(wall, 1),
        },
    )


@scenario
async def burst_over_cap(src: str) -> dict[str, Any]:
    """SQLite store, cap 8, backend holds each request 1 s, 200 uploads at once. The cap
    must hold: never more than 8 upstream, the rest refused with a retryable 503."""
    stack = start(src, store="sqlite", max_in_flight=8)
    await configure_backend(stack, delay=1.0)
    tickets = [await issue(stack) for _ in range(200)]
    replies = await asyncio.gather(
        *(
            http(stack.gateway_port, "POST", t["path"], headers=CT, body=file_body(1024, seed=i)[0])
            for i, t in enumerate(tickets)
        )
    )
    stats = await backend(stack)
    return finish(
        stack,
        {"outcomes": outcomes(replies), "backend_peak_concurrency": stats["peak"], "cap": 8},
    )


@scenario
async def header_bomb(src: str) -> dict[str, Any]:
    """A valid ticket, max_size 1 MiB, and a 64 MiB part header sent chunked so there is
    no Content-Length to reject up front."""
    stack = start(src, max_size=MB)

    async def bomb() -> AsyncIterator[bytes]:
        yield (
            f"--{BOUNDARY}\r\n"
            'Content-Disposition: form-data; name="file"; filename="a.bin"\r\nX-Pad: '
        ).encode()
        pad = b"A" * MB
        for _ in range(64):
            yield pad
        yield b"\r\nContent-Type: text/plain\r\n\r\nhi" + TAIL

    reply = await upload(stack, bomb, timeout=120)
    return finish(
        stack,
        {
            "outcome": reply.body.get("error") or reply.body.get("status") or reply.error,
            "bytes_sent_mib": round(reply.sent / MB, 1),
            "elapsed_s": round(reply.elapsed, 1),
        },
    )


@scenario
async def header_bomb_under_load(src: str) -> dict[str, Any]:
    """Four 32 MiB header bombs while 50 honest 2 MiB uploads run. Honest uploads should
    not notice."""
    stack = start(src, max_size=8 * MB, max_in_flight=64)

    async def bomb() -> AsyncIterator[bytes]:
        yield (
            f"--{BOUNDARY}\r\n"
            'Content-Disposition: form-data; name="file"; filename="a.bin"\r\nX-Pad: '
        ).encode()
        pad = b"A" * MB
        for _ in range(32):
            yield pad
        yield b"\r\nContent-Type: text/plain\r\n\r\nhi" + TAIL

    honest = [file_body(2 * MB, seed=i) for i in range(50)]
    results = await asyncio.gather(
        *(upload(stack, bomb, timeout=120) for _ in range(4)),
        *(upload(stack, b, timeout=120) for b, _ in honest),
    )
    bombs, good = results[:4], results[4:]
    return finish(
        stack,
        {
            "bomb_outcomes": outcomes(list(bombs)),
            "honest_outcomes": outcomes(list(good)),
            "honest_p99_latency_s": round(pct([r.elapsed for r in good], 99), 2),
        },
    )


@scenario
async def epilogue_valid(src: str) -> dict[str, Any]:
    """RFC 2046 allows text after the closing boundary. 50 uploads with a trailing CRLF,
    50 with a short epilogue line: all should complete."""
    stack = start(src)
    bodies = [file_body(64 * 1024, seed=i, epilogue=b"\r\n") for i in range(50)]
    bodies += [file_body(64 * 1024, seed=i, epilogue=b"end of message\r\n") for i in range(50)]
    replies = await asyncio.gather(*(upload(stack, b) for b, _ in bodies))
    stats = await backend(stack)
    bad = sum(
        1
        for r, (_, d) in zip(replies, bodies, strict=True)
        if r.body.get("status") == "completed"
        and stats["commits"].get(r.body["id"], {}).get("sha256") != d
    )
    return finish(stack, {"outcomes": outcomes(replies), "integrity_violations": bad})


@scenario
async def epilogue_flood(src: str) -> dict[str, Any]:
    """A small file, then 512 MiB after the closing boundary, no size limit set. How
    much does the gateway read before it stops?"""
    stack = start(src)

    async def flood() -> AsyncIterator[bytes]:
        yield part_head() + b"hello" + TAIL
        junk = b"J" * MB
        for _ in range(512):
            yield junk

    reply = await upload(stack, flood, timeout=180)
    return finish(
        stack,
        {
            "outcome": reply.body.get("error") or reply.body.get("status") or reply.error,
            "bytes_read_before_stop_mib": round(reply.sent / MB, 1),
            "elapsed_s": round(reply.elapsed, 1),
        },
    )


@scenario
async def slowloris(src: str) -> dict[str, Any]:
    """32 holders of valid tickets each send one byte every 5 s, filling the cap of 32.
    Then 40 honest 1 MiB uploads arrive over 90 s. How many honest uploads get through,
    and how long do the slow ones hold their slots?"""
    stack = start(src, max_in_flight=32, dest_timeout=10)

    async def drip() -> AsyncIterator[bytes]:
        yield part_head()
        for _ in range(60):
            await asyncio.sleep(5)
            yield b"x"
        yield TAIL

    attackers = [asyncio.create_task(upload(stack, drip, timeout=400)) for _ in range(32)]
    await asyncio.sleep(2)

    async def honest(i: int) -> Reply:
        await asyncio.sleep(i * 90 / 40)
        for _attempt in range(3):
            r = await upload(stack, file_body(MB, seed=i)[0], timeout=60)
            if r.status != 503:
                return r
            await asyncio.sleep(2)
        return r

    good = await asyncio.gather(*(honest(i) for i in range(40)))
    done, pending = await asyncio.wait(attackers, timeout=5)
    for task in pending:
        task.cancel()
    held = [t.result().elapsed for t in done]
    return finish(
        stack,
        {
            "honest_outcomes": outcomes(list(good)),
            "honest_completed": sum(r.body.get("status") == "completed" for r in good),
            "attackers_still_holding_after_95s": len(pending),
            "attacker_outcomes": outcomes([t.result() for t in done]),
            "attacker_median_hold_s": round(statistics.median(held), 1) if held else None,
        },
    )


@scenario
async def policy_edges(src: str) -> dict[str, Any]:
    """Live checks of two issuing rules: a per-ticket accept list must not widen the
    destination's, and a ttl of zero must not turn into fifteen minutes."""
    stack = start(src)
    widened = await issue(stack, destination="images", accept=["application/x-msdownload"])
    exe = None
    if "path" in widened:

        async def exe_body() -> AsyncIterator[bytes]:
            yield part_head("evil.exe", "application/x-msdownload") + b"MZ" + b"\0" * 64 + TAIL

        exe = await http(stack.gateway_port, "POST", widened["path"], headers=CT, body=exe_body)
    zero = await issue(stack, ttl=0)
    return finish(
        stack,
        {
            "widen_accept_issue": widened.get("refused") or "issued",
            "exe_into_image_destination": (exe.body.get("status") or exe.body.get("error"))
            if exe
            else "not attempted",
            "ttl_zero": zero.get("refused") or f"issued with ttl {zero.get('ttl')} s",
        },
    )


@scenario
async def chaos(src: str) -> dict[str, Any]:
    """400 requests at once, mixed: honest uploads of random size, clients that vanish
    mid-body, oversize bodies, garbage bodies, two requests racing on one ticket, large
    junk headers, and a backend that fails 5% of requests at random. Then the invariants:
    every completed upload committed with the exact bytes, nothing failed was committed,
    one winner per raced ticket, memory bounded, and the gateway still serves."""
    stack = start(src, max_size=16 * MB, max_in_flight=128)
    await configure_backend(stack, fail_rate=0.05)
    rng = random.Random(42)
    jobs: list[Awaitable[tuple[str, Reply, str | None]]] = []

    async def honest(i: int) -> tuple[str, Reply, str | None]:
        body, digest = file_body(rng.randint(1, 12 * MB), seed=i)
        return "honest", await upload(stack, body, retries=40, timeout=120), digest

    async def vanish(i: int) -> tuple[str, Reply, str | None]:
        body, _ = file_body(8 * MB, seed=i)
        return (
            "vanish",
            await upload(stack, body, retries=40, abort_after=rng.randint(1, 6) * MB),
            None,
        )

    async def oversize(i: int) -> tuple[str, Reply, str | None]:
        body, _ = file_body(20 * MB, seed=i)
        return "oversize", await upload(stack, body, retries=40, timeout=120), None

    async def garbage(i: int) -> tuple[str, Reply, str | None]:
        blob = random.Random(i).randbytes(rng.randint(10, 200_000))

        async def gen() -> AsyncIterator[bytes]:
            yield blob

        return "garbage", await upload(stack, gen, retries=40, timeout=60), None

    async def race(i: int) -> tuple[str, Reply, str | None]:
        ticket = await issue(stack)
        body, digest = file_body(2 * MB, seed=i)

        async def attempt() -> Reply:
            for _ in range(40):
                r = await http(stack.gateway_port, "POST", ticket["path"], headers=CT, body=body)
                if r.status != 503:
                    return r
                await asyncio.sleep(0.25 + random.random() * 0.5)
            return r

        a, b = await asyncio.gather(attempt(), attempt())
        wins = sum(r.body.get("status") == "completed" for r in (a, b))
        a.body["race_wins"] = wins
        a.body.setdefault("id", ticket["id"])
        return "race", a, digest

    async def junk_header(i: int) -> tuple[str, Reply, str | None]:
        async def gen() -> AsyncIterator[bytes]:
            yield (
                f"--{BOUNDARY}\r\n"
                'Content-Disposition: form-data; name="file"; filename="j.bin"\r\nX-Pad: '
            ).encode()
            for _ in range(rng.randint(1, 4)):
                yield b"P" * MB
            yield b"\r\nContent-Type: text/plain\r\n\r\nhi" + TAIL

        digest = hashlib.sha256(b"hi").hexdigest()
        return "junk_header", await upload(stack, gen, retries=40, timeout=120), digest

    kinds = [honest] * 240 + [vanish] * 40 + [oversize] * 40 + [garbage] * 40
    kinds += [race] * 20 + [junk_header] * 20
    rng.shuffle(kinds)
    jobs = [k(i) for i, k in enumerate(kinds)]
    t = time.perf_counter()
    results = await asyncio.gather(*jobs)
    wall = time.perf_counter() - t
    await asyncio.sleep(1)
    stats = await backend(stack)
    commits = stats["commits"]
    violations: Counter[str] = Counter()
    by_kind: dict[str, Counter[str]] = {}
    for kind, reply, digest in results:
        by_kind.setdefault(kind, Counter())[
            reply.body.get("error") or reply.body.get("status") or reply.error or "?"
        ] += 1
        rid = reply.body.get("id")
        status = reply.body.get("status")
        if status == "completed":
            if commits.get(rid, {}).get("sha256") != digest:
                violations["completed_but_wrong_or_missing_bytes"] += 1
            if reply.body.get("file", {}).get("size") != commits.get(rid, {}).get("size"):
                violations["reported_size_mismatch"] += 1
        elif kind != "race" and rid in commits:
            violations["failed_but_committed"] += 1
        if kind == "race" and reply.body.get("race_wins", 0) > 1:
            violations["two_winners_on_one_ticket"] += 1
    alive = await get_json(stack.gateway_port, "/_version")
    return finish(
        stack,
        {
            "wall_s": round(wall, 1),
            "outcomes_by_kind": {k: dict(v) for k, v in by_kind.items()},
            "invariant_violations": dict(violations),
            "gateway_alive_after": bool(alive.get("version")),
        },
    )


# ----- main -----------------------------------------------------------------------------


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--only", default="")
    args = parser.parse_args()
    src = str(Path(args.src).resolve())
    version = subprocess.run(
        [sys.executable, "-c", "import mcp_upload;print(mcp_upload.__version__)"],
        env={**os.environ, "PYTHONPATH": src},
        capture_output=True,
        text=True,
    ).stdout.strip()
    names = [n for n in args.only.split(",") if n] or list(SCENARIOS)
    results: dict[str, Any] = {"version": version, "src": src, "scenarios": {}}
    for name in names:
        print(f"[{version}] {name} ...", flush=True)
        t = time.perf_counter()
        try:
            result = await SCENARIOS[name](src)
        except Exception as exc:
            result = {"harness_error": repr(exc)}
        result["scenario_wall_s"] = round(time.perf_counter() - t, 1)
        results["scenarios"][name] = result
        print(f"    {json.dumps(result)}", flush=True)
        Path(args.out).write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
