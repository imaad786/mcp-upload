"""Live end-to-end check of SEP-2631's files/authorizeUpload over real sockets.

Starts three processes on loopback: the stress backend (which commits only clean
bodies and records the SHA-256 of each), and an MCP server served by uvicorn with the
gateway attached, the extension serving ``files/authorizeUpload``, and one tool,
``ingest(file)``, that resolves and claims a file URI. Then it drives full flows with
the official SDK client over Streamable HTTP and reports counts and timings as JSON.

    python stress/sep_flow.py [--framework mcp|fastmcp] [--flows 50] [--concurrency 50]

Run the fastmcp variant with an interpreter that has FastMCP installed. Scenarios:

- ``good``: authorize with size and digest, upload, call ``ingest`` with the URI, and
  check the returned size and digest, and the backend's commit, against what was sent.
- ``mismatch``: authorize one digest, upload different bytes. The upload must be
  refused with 422 ``digest_mismatch``, the backend must not commit, and ``ingest``
  must fail.
- ``oversize``: declare a size over the destination's limit. ``files/authorizeUpload``
  must fail with -32602 and ``maxSizeExceeded`` data.
- ``double``: upload, then call ``ingest`` twice. The second call must be refused.
- ``other_owner``: upload as one user and call ``ingest`` as another. It must fail.

Timings are per call: ``authorize`` is a bare ``files/authorizeUpload``,
``upload_file`` is the client helper end to end (hash, authorize, POST, check),
``post`` is a raw upload POST, and ``tool`` is one ``ingest`` call.

Each flow is a separate client with its own ``x-user`` header. The server binds
tickets to that header so the owner check runs for real. A header is not an identity;
a real server would use the authenticated user from the access token.
"""

import argparse
import asyncio
import base64
import contextlib
import hashlib
import json
import os
import random
import socket
import statistics
import subprocess
import sys
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
MAX_SIZE = 1 << 20


# ----- the server process ----------------------------------------------------------------


def serve(framework: str, port: int, backend: str) -> None:
    import logging

    import uvicorn

    # Refused uploads and tool calls are expected here, and each one is logged.
    logging.disable(logging.ERROR)

    from mcp_upload import Destination, MemoryStore, Registry, UploadGateway
    from mcp_upload.resolve import FileReferenceError, resolve_file
    from mcp_upload.types import FileValue

    gateway = UploadGateway(
        base_url=f"http://127.0.0.1:{port}",
        registry=Registry(
            Destination(name="files", url=f"{backend}/files/{{id}}", max_size=MAX_SIZE)
        ),
        store=MemoryStore(),
        server_name="sep-flow",
    )

    def owner_of(ctx: Any) -> str | None:
        request = getattr(ctx, "request", None)
        headers = getattr(request, "headers", None)
        return None if headers is None else headers.get("x-user")

    if framework == "mcp":
        from mcp.server.mcpserver import Context, MCPServer
        from mcp.server.mcpserver.exceptions import ToolError

        from mcp_upload.adapters.mcp import attach
        from mcp_upload.adapters.mcp_extension import UploadTicketExtension

        server = MCPServer(
            "sep-flow",
            extensions=[UploadTicketExtension(gateway, destination="files", owner=owner_of)],
        )

        @server.tool()
        async def ingest(file: str, ctx: Context) -> FileValue:
            """Take an uploaded file by its mcp-file:// URI."""
            user = (ctx.headers or {}).get("x-user")
            try:
                return await resolve_file(gateway, file, owner=user)
            except FileReferenceError as exc:
                raise ToolError(str(exc)) from exc

        attach(server, gateway)
        app = server.streamable_http_app()
    else:
        from fastmcp import FastMCP
        from fastmcp.exceptions import ToolError as FastToolError
        from fastmcp.server.dependencies import get_http_headers

        from mcp_upload.adapters.fastmcp import attach as fast_attach
        from mcp_upload.adapters.fastmcp_extension import UploadTicketExtension as FastExt

        fast = FastMCP("sep-flow")
        fast.add_extension(FastExt(gateway, destination="files", owner=owner_of))

        @fast.tool()
        async def ingest(file: str) -> FileValue:
            """Take an uploaded file by its mcp-file:// URI."""
            user = get_http_headers().get("x-user")
            try:
                return await resolve_file(gateway, file, owner=user)
            except FileReferenceError as exc:
                raise FastToolError(str(exc)) from exc

        fast_attach(fast, gateway)
        app = fast.http_app(path="/mcp")

    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning", backlog=1024)


# ----- the driver ------------------------------------------------------------------------


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_port(port: int, timeout: float = 20.0) -> None:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        with socket.socket() as sock:
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.05)
    raise RuntimeError(f"port {port} never opened")


def b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()


class Run:
    def __init__(self, url: str) -> None:
        self.url = url
        self.timings: dict[str, list[float]] = {
            "authorize": [],
            "upload_file": [],
            "post": [],
            "tool": [],
        }
        self.sent: dict[str, str] = {}  # record id -> sha256 hex of bytes that must commit

    def client(self, user: str) -> Any:
        from mcp.client import Client
        from mcp.client.streamable_http import streamable_http_client
        from mcp.shared._httpx_utils import create_mcp_http_client

        http = create_mcp_http_client(headers={"x-user": user})
        return http, Client(streamable_http_client(self.url, http_client=http))

    async def timed(self, phase: str, call: Awaitable[Any]) -> Any:
        start = time.perf_counter()
        try:
            return await call
        finally:
            self.timings[phase].append(time.perf_counter() - start)

    async def upload(self, client: Any, data: bytes, name: str, uploads: Any) -> str:
        from mcp_upload.client import upload_file

        return str(
            await self.timed("upload_file", upload_file(client, data, name=name, http=uploads))
        )

    async def good(self, i: int, uploads: Any) -> None:
        data = random.randbytes(random.randint(1024, 256 * 1024))
        http, client = self.client(f"user-{i}")
        async with http, client:
            uri = await self.upload(client, data, f"good-{i}.bin", uploads)
            result = await self.timed("tool", client.call_tool("ingest", {"file": uri}))
        assert not result.is_error, f"ingest failed: {result.content}"
        file = result.structured_content
        assert file["uri"] == uri, file
        assert file["size"] == len(data), (file["size"], len(data))
        assert file["digest"] == {"algorithm": "sha-256", "value": b64(data)}, file["digest"]
        self.sent[uri.rsplit("/", 1)[1]] = hashlib.sha256(data).hexdigest()

    async def mismatch(self, i: int, uploads: Any) -> None:
        from mcp_upload.client import authorize_upload

        declared = random.randbytes(4096)
        sent = random.randbytes(4096)
        http, client = self.client(f"mismatch-{i}")
        async with http, client:
            auth = await self.timed(
                "authorize",
                authorize_upload(client, name="m.bin", size=len(declared), digest=b64(declared)),
            )
            response = await self.timed(
                "post",
                uploads.post(
                    auth["upload"]["url"],
                    headers={"Accept": "application/json"},
                    files={"file": ("m.bin", sent, "application/octet-stream")},
                ),
            )
            assert response.status_code == 422, response.status_code
            assert response.json()["error"] == "digest_mismatch", response.text
            result = await self.timed(
                "tool", client.call_tool("ingest", {"file": auth["file"]["uri"]})
            )
        assert result.is_error, "ingest accepted a refused upload"

    async def oversize(self, i: int, uploads: Any) -> None:
        from mcp.shared.exceptions import MCPError

        from mcp_upload.client import authorize_upload

        http, client = self.client(f"oversize-{i}")
        async with http, client:
            try:
                await self.timed(
                    "authorize",
                    authorize_upload(client, name="big.bin", size=MAX_SIZE + 1 + i),
                )
            except MCPError as exc:
                assert exc.code == -32602, exc.code
                assert exc.data == {
                    "reason": "maxSizeExceeded",
                    "maxSize": MAX_SIZE,
                    "actualSize": MAX_SIZE + 1 + i,
                }, exc.data
            else:
                raise AssertionError("an oversize declaration was authorized")

    async def double(self, i: int, uploads: Any) -> None:
        data = random.randbytes(8192)
        http, client = self.client(f"double-{i}")
        async with http, client:
            uri = await self.upload(client, data, f"double-{i}.bin", uploads)
            first = await self.timed("tool", client.call_tool("ingest", {"file": uri}))
            second = await self.timed("tool", client.call_tool("ingest", {"file": uri}))
        assert not first.is_error, first.content
        assert second.is_error, "the same URI was claimed twice"
        assert "already been used" in str(second.content), second.content
        self.sent[uri.rsplit("/", 1)[1]] = hashlib.sha256(data).hexdigest()

    async def other_owner(self, i: int, uploads: Any) -> None:
        data = random.randbytes(2048)
        http, client = self.client(f"alice-{i}")
        async with http, client:
            uri = await self.upload(client, data, f"alice-{i}.bin", uploads)
        self.sent[uri.rsplit("/", 1)[1]] = hashlib.sha256(data).hexdigest()
        http, client = self.client(f"mallory-{i}")
        async with http, client:
            result = await self.timed("tool", client.call_tool("ingest", {"file": uri}))
        assert result.is_error, "another user claimed the file"
        assert "no such upload" in str(result.content), result.content


def summary(values: list[float]) -> dict[str, float]:
    if not values:
        return {}
    ordered = sorted(values)
    return {
        "n": len(ordered),
        "p50_ms": round(statistics.median(ordered) * 1000, 1),
        "p95_ms": round(ordered[max(0, int(len(ordered) * 0.95) - 1)] * 1000, 1),
        "max_ms": round(ordered[-1] * 1000, 1),
    }


async def drive(args: argparse.Namespace, url: str, backend: str) -> dict[str, Any]:
    import httpx

    run = Run(url)
    gate = asyncio.Semaphore(args.concurrency)
    scenarios: dict[str, tuple[int, Callable[[int, Any], Awaitable[None]]]] = {
        "good": (args.flows, run.good),
        "mismatch": (10, run.mismatch),
        "oversize": (10, run.oversize),
        "double": (10, run.double),
        "other_owner": (10, run.other_owner),
    }
    results: dict[str, dict[str, Any]] = {
        name: {"flows": count, "passed": 0, "failed": 0, "errors": []}
        for name, (count, _) in scenarios.items()
    }

    limits = httpx.Limits(max_connections=args.concurrency)
    async with httpx.AsyncClient(limits=limits, timeout=60.0) as uploads:

        async def one(name: str, i: int, flow: Callable[[int, Any], Awaitable[None]]) -> None:
            async with gate:
                try:
                    await flow(i, uploads)
                    results[name]["passed"] += 1
                except Exception as exc:
                    results[name]["failed"] += 1
                    if len(results[name]["errors"]) < 3:
                        results[name]["errors"].append(f"{type(exc).__name__}: {exc}"[:300])

        tasks = [
            one(name, i, flow) for name, (count, flow) in scenarios.items() for i in range(count)
        ]
        random.shuffle(tasks)
        started = time.perf_counter()
        await asyncio.gather(*tasks)
        wall = time.perf_counter() - started

    async with httpx.AsyncClient() as plain:
        stats = (await plain.get(f"{backend}/_stats")).json()
    commits: dict[str, dict[str, Any]] = stats["commits"]
    committed_right = sum(
        1 for rid, sha in run.sent.items() if commits.get(rid, {}).get("sha256") == sha
    )
    for entry in results.values():
        if not entry["errors"]:
            del entry["errors"]
    return {
        "framework": args.framework,
        "python": sys.version.split()[0],
        "versions": versions(),
        "concurrency": args.concurrency,
        "flows": sum(count for count, _ in scenarios.values()),
        "wall_s": round(wall, 2),
        "scenarios": results,
        "backend": {
            "commits": len(commits),
            "expected_commits": len(run.sent),
            "commits_matching_sent_sha256": committed_right,
            "incomplete_bodies": stats["incomplete"],
        },
        "timings": {phase: summary(values) for phase, values in run.timings.items()},
        "all_passed": all(entry["failed"] == 0 for entry in results.values())
        and committed_right == len(run.sent) == len(commits),
    }


def versions() -> dict[str, str]:
    from importlib.metadata import PackageNotFoundError, version

    out = {}
    for name in ("mcp", "fastmcp", "httpx", "uvicorn"):
        with contextlib.suppress(PackageNotFoundError):
            out[name] = version(name)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--framework", choices=["mcp", "fastmcp"], default="mcp")
    parser.add_argument("--flows", type=int, default=50, help="good flows")
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--serve", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--port", type=int, default=0, help=argparse.SUPPRESS)
    parser.add_argument("--backend", default="", help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.serve:
        serve(args.framework, args.port, args.backend)
        return

    env = dict(os.environ)
    bport, sport = free_port(), free_port()
    backend_url = f"http://127.0.0.1:{bport}"
    backend = subprocess.Popen([sys.executable, str(HERE / "backend.py"), str(bport)], env=env)
    server = subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--serve",
            "--framework",
            args.framework,
            "--port",
            str(sport),
            "--backend",
            backend_url,
        ],
        env=env,
    )
    try:
        wait_port(bport)
        wait_port(sport)
        report = asyncio.run(drive(args, f"http://127.0.0.1:{sport}/mcp", backend_url))
    finally:
        for proc in (server, backend):
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
    print(json.dumps(report, indent=2))
    sys.exit(0 if report["all_passed"] else 1)


if __name__ == "__main__":
    main()
