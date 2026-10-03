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
import base64
import contextlib
import hashlib
import json
import os
import random
import re
import socket
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import unicodedata
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
        flag = f"--{key.replace('_', '-')}"
        if value is True:
            cmd.append(flag)
        elif value is not None and value is not False:
            cmd += [flag, str(value)]
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


# ----- 0.4.0 features: declared digests, owners, claims, abandoned records ----------------
#
# Each of these asks the gateway for its capabilities first and reports
# ``{"supported": false}`` on a version that lacks the feature.

OLD_SRC: str | None = None  # set by --old-src, for sqlite_migration


def b64url_of(hex_digest: str) -> str:
    return base64.urlsafe_b64encode(bytes.fromhex(hex_digest)).rstrip(b"=").decode()


def digest_matches(value: str, hex_digest: str) -> bool:
    """A reported digest value, hex or base64url, against a hex SHA-256."""
    if re.fullmatch(r"[0-9a-f]{64}", value):
        return value == hex_digest
    if re.fullmatch(r"[A-Za-z0-9_-]{43}", value):
        return base64.urlsafe_b64decode(value + "=").hex() == hex_digest
    return False


async def capabilities(stack: Stack) -> dict[str, Any]:
    return await get_json(stack.gateway_port, "/_caps")


async def status_of(stack: Stack, record_id: str, owner: str | None = None) -> dict[str, Any]:
    query = f"?owner={owner}" if owner is not None else ""
    return await get_json(stack.gateway_port, f"/_status/{record_id}{query}")


async def claim(stack: Stack, record_id: str, owner: str | None = None) -> dict[str, Any]:
    query = f"?owner={owner}" if owner is not None else ""
    return (await http(stack.gateway_port, "POST", f"/_claim/{record_id}{query}")).body


def unsupported(stack: Stack, missing: list[str]) -> dict[str, Any]:
    return finish(stack, {"supported": False, "missing": missing})


def lacking(caps: dict[str, Any], *names: str) -> list[str]:
    return [n for n in names if not caps.get(n)]


async def send(stack: Stack, ticket: dict[str, Any], body: Body, **kw: Any) -> Reply:
    reply = await http(stack.gateway_port, "POST", ticket["path"], headers=CT, body=body, **kw)
    reply.body.setdefault("id", ticket.get("id"))
    return reply


def named_body(data: bytes, filename: str, media_type: str = "application/octet-stream") -> Body:
    async def gen() -> AsyncIterator[bytes]:
        yield part_head(filename, media_type) + data + TAIL

    return gen


@scenario
async def digest_integrity(src: str) -> dict[str, Any]:
    """250 concurrent uploads with a declared digest or size. 100 match, 100 declare the
    wrong digest, 50 the wrong size (25 one byte over, 25 one byte under). A mismatch
    must be refused and must never reach a backend commit."""
    stack = start(src, max_in_flight=300)
    caps = await capabilities(stack)
    if missing := lacking(caps, "expected_size", "expected_digest"):
        return unsupported(stack, missing)
    size = 256 * 1024
    jobs: list[tuple[str, dict[str, Any], Body, str]] = []
    for i in range(250):
        body, digest = file_body(size, seed=1000 + i)
        if i < 100:
            kind, declared = (
                "match",
                {
                    "expected_digest": {"algorithm": "sha-256", "value": b64url_of(digest)},
                    "expected_size": size,
                },
            )
        elif i < 200:
            wrong = hashlib.sha256(f"other {i}".encode()).hexdigest()
            kind, declared = (
                "wrong_digest",
                {"expected_digest": b64url_of(wrong), "expected_size": size},
            )
        elif i < 225:
            kind, declared = "size_over", {"expected_size": size + 1}
        else:
            kind, declared = "size_under", {"expected_size": size - 1}
        ticket = await issue(stack, **declared)
        jobs.append((kind, ticket, body, digest))
    replies = await asyncio.gather(*(send(stack, t, b) for _, t, b, _ in jobs))
    await asyncio.sleep(1)
    commits = (await backend(stack))["commits"]
    by_kind: dict[str, Counter[str]] = {}
    committed_despite_mismatch = 0
    match_integrity_bad = 0
    http_codes: dict[str, Counter[int]] = {}
    for (kind, ticket, _, digest), reply in zip(jobs, replies, strict=True):
        by_kind.setdefault(kind, Counter())[
            reply.body.get("error") or reply.body.get("status") or reply.error or "?"
        ] += 1
        http_codes.setdefault(kind, Counter())[reply.status] += 1
        if kind == "match":
            if commits.get(ticket["id"], {}).get("sha256") != digest:
                match_integrity_bad += 1
        elif ticket["id"] in commits:
            committed_despite_mismatch += 1
    return finish(
        stack,
        {
            "supported": True,
            "outcomes_by_kind": {k: dict(v) for k, v in by_kind.items()},
            "http_status_by_kind": {k: dict(v) for k, v in http_codes.items()},
            "match_completed_but_bytes_wrong": match_integrity_bad,
            "committed_despite_mismatch": committed_despite_mismatch,
            "backend_commits": len(commits),
        },
    )


@scenario
async def owner_isolation(src: str) -> dict[str, Any]:
    """50 records owned by alice, uploaded. Bob must see each as unknown and must not
    claim it. Alice and an owner-less query see it completed."""
    stack = start(src)
    caps = await capabilities(stack)
    if missing := lacking(caps, "owner", "status_owner", "claim"):
        return unsupported(stack, missing)
    tickets = [await issue(stack, owner="alice") for _ in range(50)]
    replies = await asyncio.gather(
        *(send(stack, t, file_body(4096, seed=i)[0]) for i, t in enumerate(tickets))
    )
    ids = [t["id"] for t in tickets]
    bob = [await status_of(stack, i, "bob") for i in ids]
    alice = [await status_of(stack, i, "alice") for i in ids]
    anyone = [await status_of(stack, i) for i in ids]
    bob_claims = [await claim(stack, i, "bob") for i in ids]
    after = [await status_of(stack, i, "alice") for i in ids]
    alice_claims = [await claim(stack, i, "alice") for i in ids]
    return finish(
        stack,
        {
            "supported": True,
            "upload_outcomes": outcomes(list(replies)),
            "bob_status": dict(Counter(s.get("status") for s in bob)),
            "bob_status_leaks": sum(s.get("status") != "unknown" or len(s) > 2 for s in bob),
            "alice_status": dict(Counter(s.get("status") for s in alice)),
            "no_owner_status": dict(Counter(s.get("status") for s in anyone)),
            "bob_claim_results": dict(
                Counter(
                    c.get("refused") or ("claimed" if c.get("claimed") else "?") for c in bob_claims
                )
            ),
            "bob_claim_wins": sum(bool(c.get("claimed")) for c in bob_claims),
            "status_after_bob_claims": dict(Counter(s.get("status") for s in after)),
            "alice_claim_wins": sum(bool(c.get("claimed")) for c in alice_claims),
        },
    )


@scenario
async def claim_race(src: str) -> dict[str, Any]:
    """50 completed uploads per store, then 20 concurrent claims on each record. Exactly
    one claim per record may win. Run on the memory store and the SQLite store."""
    result: dict[str, Any] = {"supported": True}
    for store in ("memory", "sqlite"):
        stack = start(src, store=store)
        caps = await capabilities(stack)
        if missing := lacking(caps, "claim"):
            return unsupported(stack, missing)
        tickets = [await issue(stack) for _ in range(50)]
        replies = await asyncio.gather(
            *(send(stack, t, file_body(4096, seed=i)[0]) for i, t in enumerate(tickets))
        )
        winners: Counter[int] = Counter()
        refusals: Counter[str] = Counter()
        for t in tickets:
            claims = await asyncio.gather(*(claim(stack, t["id"]) for _ in range(20)))
            winners[sum(bool(c.get("claimed")) for c in claims)] += 1
            refusals.update(c["refused"] for c in claims if "refused" in c)
        statuses = Counter([(await status_of(stack, t["id"])).get("status") for t in tickets])
        result[store] = finish(
            stack,
            {
                "upload_outcomes": outcomes(list(replies)),
                "records_by_winner_count": {str(k): v for k, v in sorted(winners.items())},
                "total_wins": sum(k * v for k, v in winners.items()),
                "refusals": dict(refusals),
                "status_after": dict(statuses),
            },
        )
    return result


@scenario
async def abandoned_after_crash(src: str) -> dict[str, Any]:
    """A slow upload is mid-stream when the gateway is SIGKILLed. A new gateway on the
    same SQLite file (upload_timeout 2 s) reads the record right away and again once
    redeemed_at + upload_timeout + 30 s has passed. The first gateway runs with a 60 s
    upload_timeout so its own watchdog cannot end the upload before the kill."""
    db = os.path.join(tempfile.mkdtemp(), "tickets.db")
    first = start(src, store="sqlite", db=db, upload_timeout=60)

    async def slow() -> AsyncIterator[bytes]:
        yield part_head("slow.bin")
        chunk = b"s" * (128 * 1024)
        for _ in range(60):
            yield chunk
            await asyncio.sleep(2)
        yield TAIL

    ticket = await issue(first)
    t0 = time.monotonic()
    task = asyncio.create_task(send(first, ticket, slow, timeout=120))
    await asyncio.sleep(3)
    upstream = (await backend(first))["concurrent"]
    before_kill = await status_of(first, ticket["id"])
    first.gateway.kill()
    first.gateway.wait(5)
    client = await task
    second = start(src, store="sqlite", db=db, upload_timeout=2)
    right_after = await status_of(second, ticket["id"])
    # redeemed_at is a moment after t0. Sleep to just past the abandon point, then poll.
    await asyncio.sleep(max(0.0, t0 + 2 + 30 + 1 - time.monotonic()))
    after_wait = await status_of(second, ticket["id"])
    for _ in range(8):
        if after_wait.get("status") != "redeemed":
            break
        await asyncio.sleep(1)
        after_wait = await status_of(second, ticket["id"])
    commits = (await backend(first))["commits"]
    finish(first, {})
    return finish(
        second,
        {
            "backend_requests_open_at_kill": upstream,
            "status_before_kill": before_kill,
            "client_saw": {"http": client.status, "error": client.error, "body": client.body},
            "status_right_after_restart": right_after,
            "status_after_wait": after_wait,
            "waited_s": round(time.monotonic() - t0, 1),
            "backend_committed": ticket["id"] in commits,
        },
    )


@scenario
async def error_details(src: str) -> dict[str, Any]:
    """Three refusals, and the machine-readable ``details`` each one carries: an
    oversize upload (max_size 1 MiB), a text file into the image destination, and a
    declared digest that does not match (where supported)."""
    stack = start(src, max_size=MB)
    caps = await capabilities(stack)
    cases: dict[str, Any] = {}

    async def run(name: str, issue_kw: dict[str, Any], body: Body) -> None:
        ticket = await issue(stack, **issue_kw)
        if "path" not in ticket:
            cases[name] = {"issue": ticket}
            return
        reply = await send(stack, ticket, body)
        stored = await status_of(stack, ticket["id"])
        cases[name] = {
            "http": reply.status,
            "error": reply.body.get("error"),
            "details": reply.body.get("details", "none returned"),
            "status_details": stored.get("details", "none returned"),
        }

    await run("oversize", {}, file_body(2 * MB, seed=1)[0])
    await run(
        "wrong_media_type",
        {"destination": "images"},
        named_body(b"just text", "notes.txt", "text/plain"),
    )
    if caps.get("expected_digest"):
        wrong = b64url_of(hashlib.sha256(b"something else").hexdigest())
        await run("digest_mismatch", {"expected_digest": wrong}, named_body(b"hello", "h.txt"))
    else:
        cases["digest_mismatch"] = {"supported": False}
    return finish(stack, cases)


@scenario
async def digest_format(src: str) -> dict[str, Any]:
    """The digest a completed upload reports: hex or base64url, and does it decode to the
    SHA-256 of the bytes sent."""
    stack = start(src)
    body, digest = file_body(100 * 1024, seed=5)
    reply = await upload(stack, body)
    value = str(reply.body.get("file", {}).get("digest", {}).get("value", ""))
    is_hex = bool(re.fullmatch(r"[0-9a-f]{64}", value))
    is_b64 = bool(re.fullmatch(r"[A-Za-z0-9_-]{43}", value))
    matches = digest_matches(value, digest)
    return finish(
        stack,
        {
            "outcome": reply.body.get("status") or reply.body.get("error"),
            "digest": reply.body.get("file", {}).get("digest"),
            "format": "hex" if is_hex else "base64url" if is_b64 else "other",
            "matches_sha256_of_bytes_sent": matches,
        },
    )


@scenario
async def filename_hygiene(src: str) -> dict[str, Any]:
    """Names the gateway passes on: a right-to-left override, a 300-character name in a
    two-byte script, and a decomposed accent."""
    stack = start(src)
    names = {
        "rtl_override": "invoice‮gpj.exe",
        "long_multibyte": "é" * 300 + ".txt",
        "decomposed": "café.txt",
    }
    result: dict[str, Any] = {}
    for key, name in names.items():
        reply = await upload(stack, named_body(b"data", name))
        got = reply.body.get("file", {}).get("name")
        entry: dict[str, Any] = {"outcome": reply.body.get("status") or reply.body.get("error")}
        if isinstance(got, str):
            entry |= {
                "name": got if len(got) < 40 else got[:12] + "..." + got[-8:],
                "chars": len(got),
                "utf8_bytes": len(got.encode()),
                "has_u202e": "‮" in got,
                "is_nfc": unicodedata.is_normalized("NFC", got),
            }
        result[key] = entry
    return finish(stack, result)


@scenario
async def sqlite_migration(src: str) -> dict[str, Any]:
    """A database written by the old release (``--old-src``), then opened by ``src``: the
    old completed record must still read completed, an old unused ticket must still
    redeem, and a new upload must work in the same file."""
    if OLD_SRC is None:
        return {"supported": False, "reason": "no --old-src given"}
    db = os.path.join(tempfile.mkdtemp(), "tickets.db")
    old = start(OLD_SRC, store="sqlite", db=db)
    old_body, old_digest = file_body(50_000, seed=11)
    old_reply = await upload(old, old_body)
    spare = await issue(old)
    finish(old, {})
    with contextlib.closing(sqlite3.connect(db)) as conn:
        old_columns = [row[1] for row in conn.execute("PRAGMA table_info(tickets)")]
    new = start(src, store="sqlite", db=db)
    caps = await capabilities(new)
    with contextlib.closing(sqlite3.connect(db)) as conn:
        new_columns = [row[1] for row in conn.execute("PRAGMA table_info(tickets)")]
    old_status = await status_of(new, old_reply.body["id"])
    spare_body, spare_digest = file_body(30_000, seed=12)
    spare_reply = await send(new, spare, spare_body)
    new_body, new_digest = file_body(40_000, seed=13)
    new_reply = await upload(new, new_body)
    new_status = await status_of(new, new_reply.body["id"])
    claimed = await claim(new, old_reply.body["id"]) if caps.get("claim") else None
    commits = (await backend(new))["commits"]
    return finish(
        new,
        {
            "trivial": Path(OLD_SRC).resolve() == Path(src).resolve(),
            "old_upload": old_reply.body.get("status") or old_reply.body.get("error"),
            "columns_before": old_columns,
            "columns_added": [c for c in new_columns if c not in old_columns],
            "old_record_status_in_new": old_status.get("status"),
            "old_record_size_ok": old_status.get("file", {}).get("size") == 50_000,
            "old_unused_ticket_in_new": spare_reply.body.get("status")
            or spare_reply.body.get("error"),
            "old_ticket_bytes_ok": commits.get(spare["id"], {}).get("sha256") == spare_digest,
            "new_upload": new_reply.body.get("status") or new_reply.body.get("error"),
            "new_record_status": new_status.get("status"),
            "new_bytes_ok": commits.get(new_reply.body["id"], {}).get("sha256") == new_digest,
            "claim_old_record": claimed if claimed is not None else "claim unsupported",
            "old_record_digest_ok": digest_matches(
                str(old_status.get("file", {}).get("digest", {}).get("value", "")), old_digest
            ),
        },
    )


# ----- 0.6.0 features: function destinations, the filesystem sink, raw uploads ----------
#
# Like the 0.4.0 scenarios, each asks the gateway for its capabilities first and
# reports ``{"supported": false}`` on a version that lacks the feature.


async def sink_stats(stack: Stack) -> dict[str, Any]:
    return await get_json(stack.gateway_port, "/_sink_stats")


def raw_file_body(size: int, *, seed: int = 0, chunk: int = 256 * 1024) -> tuple[Body, str]:
    """A streamed raw body of ``size`` pseudo-random bytes and its SHA-256."""
    block = random.Random(seed).randbytes(min(chunk, max(size, 1)))
    digest = hashlib.sha256()
    left = size
    while left:
        n = min(len(block), left)
        digest.update(block[:n])
        left -= n

    async def gen() -> AsyncIterator[bytes]:
        left = size
        while left:
            n = min(len(block), left)
            yield block[:n]
            left -= n

    return gen, digest.hexdigest()


def failed_but_kept(results: list[tuple[str, Reply, str | None]], commits: dict[str, Any]) -> int:
    return sum(
        1
        for _, reply, _ in results
        if reply.body.get("status") != "completed" and reply.body.get("id") in commits
    )


def wrong_bytes(results: list[tuple[str, Reply, str | None]], commits: dict[str, Any]) -> int:
    return sum(
        1
        for _, reply, digest in results
        if reply.body.get("status") == "completed"
        and commits.get(reply.body.get("id"), {}).get("sha256") != digest
    )


def by_kind(results: list[tuple[str, Reply, str | None]]) -> dict[str, dict[str, int]]:
    counts: dict[str, Counter[str]] = {}
    for kind, reply, _ in results:
        counts.setdefault(kind, Counter())[
            reply.body.get("error") or reply.body.get("status") or reply.error or str(reply.status)
        ] += 1
    return {k: dict(v) for k, v in counts.items()}


@scenario
async def sink_integrity(src: str) -> dict[str, Any]:
    """50 concurrent uploads of up to 4 MiB into a function destination, once into an
    in-process memory sink and once into the filesystem sink. Every completed upload
    must be committed with exactly its bytes, and no temporary file may remain."""
    result: dict[str, Any] = {"supported": True}
    for kind, cap in (("memory", "sink"), ("filesystem", "filesystem_sink")):
        stack = start(src, sink=kind, max_in_flight=100)
        caps = await capabilities(stack)
        if missing := lacking(caps, cap):
            result[kind] = unsupported(stack, missing)
            result["supported"] = False
            continue
        rng = random.Random(7)
        bodies = [file_body(rng.randint(1, 4 * MB), seed=300 + i) for i in range(50)]
        t = time.perf_counter()
        replies = await asyncio.gather(*(upload(stack, b, retries=10) for b, _ in bodies))
        wall = time.perf_counter() - t
        await asyncio.sleep(0.5)
        stats = await sink_stats(stack)
        results = [("honest", r, d) for r, (_, d) in zip(replies, bodies, strict=True)]
        result[kind] = finish(
            stack,
            {
                "sink_active": caps.get("sink_active"),
                "outcomes": outcomes(list(replies)),
                "committed": len(stats["commits"]),
                "completed_but_wrong_or_missing_bytes": wrong_bytes(results, stats["commits"]),
                "temp_files_left": stats.get("temp_files", 0),
                "wall_s": round(wall, 1),
            },
        )
    return result


@scenario
async def sink_mixed_failures(src: str) -> dict[str, Any]:
    """Into the filesystem sink, max_size 4 MiB, at once: 20 honest uploads, 10 clients
    that vanish mid-body, 10 oversize bodies, 10 declared digests that do not match.
    No failed upload may leave a file, and no temporary file may remain."""
    stack = start(src, sink="filesystem", max_size=4 * MB, max_in_flight=100)
    caps = await capabilities(stack)
    if missing := lacking(caps, "filesystem_sink"):
        return unsupported(stack, missing)
    rng = random.Random(11)

    async def honest(i: int) -> tuple[str, Reply, str | None]:
        body, digest = file_body(rng.randint(1, 3 * MB), seed=500 + i)
        return "honest", await upload(stack, body, retries=10, timeout=60), digest

    async def vanish(i: int) -> tuple[str, Reply, str | None]:
        body, _ = file_body(3 * MB, seed=600 + i)
        cut = rng.randint(256 * 1024, 2 * MB)
        return "vanish", await upload(stack, body, retries=10, abort_after=cut), None

    async def oversize(i: int) -> tuple[str, Reply, str | None]:
        body, _ = file_body(6 * MB, seed=700 + i)
        return "oversize", await upload(stack, body, retries=10, timeout=60), None

    async def wrong_digest(i: int) -> tuple[str, Reply, str | None]:
        body, _ = file_body(2 * MB, seed=800 + i)
        ticket = await issue(stack, expected_digest=b64url_of(hashlib.sha256(b"no").hexdigest()))
        return "wrong_digest", await send(stack, ticket, body, timeout=60), None

    kinds = [honest] * 20 + [vanish] * 10 + [oversize] * 10 + [wrong_digest] * 10
    rng.shuffle(kinds)
    results = await asyncio.gather(*(k(i) for i, k in enumerate(kinds)))
    # A vanished client is noticed when its next read fails, and a write still running
    # in a thread is cleaned up after it returns, so give both a moment.
    await asyncio.sleep(1.5)
    stats = await sink_stats(stack)
    commits = stats["commits"]
    vanished = [
        (await status_of(stack, reply.body["id"])).get("error")
        for kind, reply, _ in results
        if kind == "vanish"
    ]
    return finish(
        stack,
        {
            "supported": True,
            "outcomes_by_kind": by_kind(list(results)),
            "vanished_records": dict(Counter(vanished)),
            "files_committed": len(commits),
            "honest_completed": sum(
                k == "honest" and r.body.get("status") == "completed" for k, r, _ in results
            ),
            "completed_but_wrong_or_missing_bytes": wrong_bytes(list(results), commits),
            "failed_but_file_present": failed_but_kept(list(results), commits),
            "temp_files_left": stats["temp_files"],
        },
    )


@scenario
async def raw_uploads(src: str) -> dict[str, Any]:
    """Raw-body uploads, max_size 4 MiB, to the HTTP backend: 30 honest ones (POST and
    PUT, filename from Content-Disposition or the query), then refusals: declared length
    over the limit, chunked bodies over it, wrong declared digests, a wrong media type,
    URL-encoded forms and clients that vanish. Checks the bytes, the names, which
    refusals left the ticket unspent, and that nothing refused was committed."""
    stack = start(src, raw_uploads=True, max_size=4 * MB, max_in_flight=100)
    caps = await capabilities(stack)
    if missing := lacking(caps, "raw_uploads"):
        return unsupported(stack, missing)
    rng = random.Random(13)
    port = stack.gateway_port

    async def honest(i: int) -> tuple[str, Reply, str | None]:
        body, digest = raw_file_body(rng.randint(0, 2 * MB), seed=900 + i)
        ticket = await issue(stack)
        name = f"raw {i}.bin"
        headers = {"Content-Type": "application/octet-stream"}
        path = ticket["path"]
        if i % 2:
            path += f"?filename=raw%20{i}.bin"
        else:
            headers["Content-Disposition"] = f"attachment; filename*=UTF-8''raw%20{i}.bin"
        method = "PUT" if i % 3 else "POST"
        reply = await http(port, method, path, headers=headers, body=body, timeout=60)
        reply.body.setdefault("id", ticket["id"])
        reply.body["name_ok"] = reply.body.get("file", {}).get("name") == name
        return "honest", reply, digest

    async def refused(
        kind: str, body: Body | bytes, headers: dict[str, str], **issue_kw: Any
    ) -> tuple[str, Reply, str | None]:
        ticket = await issue(stack, **issue_kw)
        reply = await http(port, "PUT", ticket["path"], headers=headers, body=body, timeout=60)
        reply.body.setdefault("id", ticket["id"])
        reply.body["record_after"] = (await status_of(stack, ticket["id"])).get("status")
        return kind, reply, None

    octet = {"Content-Type": "application/octet-stream"}
    jobs: list[Awaitable[tuple[str, Reply, str | None]]] = [honest(i) for i in range(30)]
    jobs += [refused("declared_oversize", b"x" * (5 * MB), octet) for _ in range(5)]
    jobs += [refused("chunked_oversize", raw_file_body(6 * MB, seed=i)[0], octet) for i in range(5)]
    wrong = b64url_of(hashlib.sha256(b"not this").hexdigest())
    jobs += [
        refused("wrong_digest", raw_file_body(MB, seed=i)[0], octet, expected_digest=wrong)
        for i in range(5)
    ]
    jobs += [
        refused("text_into_images", b"hello", {"Content-Type": "text/plain"}, destination="images")
        for _ in range(3)
    ]
    form = {"Content-Type": "application/x-www-form-urlencoded"}
    jobs += [refused("urlencoded", b"file=hello", form) for _ in range(3)]

    async def vanish(i: int) -> tuple[str, Reply, str | None]:
        ticket = await issue(stack)
        body, _ = raw_file_body(3 * MB, seed=950 + i)
        cut = rng.randint(256 * 1024, 2 * MB)
        reply = await http(port, "PUT", ticket["path"], headers=octet, body=body, abort_after=cut)
        reply.body.setdefault("id", ticket["id"])
        return "vanish", reply, None

    jobs += [vanish(i) for i in range(5)]
    results = await asyncio.gather(*jobs)
    await asyncio.sleep(1)
    commits = (await backend(stack))["commits"]
    records_after: dict[str, Counter[str]] = {}
    for kind, reply, _ in results:
        if kind == "vanish":
            reply.body["record_after"] = (await status_of(stack, reply.body["id"])).get("error")
        if kind != "honest":
            records_after.setdefault(kind, Counter())[str(reply.body.get("record_after"))] += 1
    return finish(
        stack,
        {
            "supported": True,
            "outcomes_by_kind": by_kind(list(results)),
            "http_status_by_kind": {
                kind: dict(Counter(r.status for k, r, _ in results if k == kind))
                for kind in dict.fromkeys(k for k, _, _ in results)
            },
            "record_after_refusal": {k: dict(v) for k, v in records_after.items()},
            "honest_names_ok": sum(
                bool(r.body.get("name_ok")) for k, r, _ in results if k == "honest"
            ),
            "completed_but_wrong_or_missing_bytes": wrong_bytes(list(results), commits),
            "failed_but_committed": failed_but_kept(list(results), commits),
            "backend_commits": len(commits),
        },
    )


# ----- main -----------------------------------------------------------------------------


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--only", default="")
    parser.add_argument("--old-src", default=None, help="older tree, for sqlite_migration")
    args = parser.parse_args()
    src = str(Path(args.src).resolve())
    global OLD_SRC
    OLD_SRC = str(Path(args.old_src).resolve()) if args.old_src else None
    version = subprocess.run(
        [
            sys.executable,
            "-c",
            "import mcp_upload as m;"
            "print(m.__version__ + ('+claim' if hasattr(m, 'ClaimRefused') else ''))",
        ],
        env={**os.environ, "PYTHONPATH": src},
        capture_output=True,
        text=True,
    ).stdout.strip()
    names = [n for n in args.only.split(",") if n] or list(SCENARIOS)
    results: dict[str, Any] = {"version": version, "src": src, "old_src": OLD_SRC, "scenarios": {}}
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
