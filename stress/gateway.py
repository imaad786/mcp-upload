"""The gateway under test, run as its own process so its memory can be watched from
outside. It serves the real upload endpoint plus harness-only routes: one that mints a
ticket the way a tool would, one that reads a record's status, one that claims a
record, and one that says which of those features the installed version has.

Options the installed version does not know are dropped, so the same harness runs
against old and new releases and each runs on its own defaults. A request for a
feature the version lacks gets ``{"unsupported": name}`` with status 400.
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import os
import tempfile
from collections import Counter
from datetime import timedelta
from typing import Any

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

import mcp_upload
from mcp_upload import Destination, MemoryStore, Registry, SqliteStore, UploadGateway


def build(args: argparse.Namespace) -> Starlette:
    registry = Registry(
        Destination(
            name="files",
            url=f"{args.backend}/files/{{id}}",
            max_size=args.max_size,
            timeout=args.dest_timeout,
        ),
        Destination(
            name="images",
            url=f"{args.backend}/files/{{id}}",
            max_size=args.max_size,
            accept=("image/*",),
            timeout=args.dest_timeout,
        ),
    )
    redis_options: dict[str, bool] = {}
    if args.store == "sqlite":
        path = args.db or os.path.join(tempfile.mkdtemp(), "tickets.db")
        store: Any = SqliteStore(path)
    elif args.store == "redis":
        from redis.asyncio import BlockingConnectionPool, Redis

        from mcp_upload.redis_store import RedisStore

        if args.redis_pool == "blocking":
            # Waits for a free connection. The default pool of redis-py 6 and later
            # raises MaxConnectionsError past 100 connections instead.
            client = Redis(
                connection_pool=BlockingConnectionPool.from_url(
                    args.redis_url, max_connections=args.redis_max_connections, timeout=30
                )
            )
        else:
            client = Redis.from_url(args.redis_url)

        store_params = inspect.signature(RedisStore.__init__).parameters
        for name in ("server_clock", "legacy_layout"):
            if getattr(args, name) and name in store_params:
                redis_options[name] = True
        store = RedisStore(client, prefix=args.prefix, **redis_options)
    else:
        store = MemoryStore(max_records=1_000_000)
    hooks: Counter[str] = Counter()

    async def on_complete(record: Any) -> None:
        if args.hook_delay:
            await asyncio.sleep(args.hook_delay)
        hooks[record.status.value] += 1

    options: dict[str, Any] = {
        "base_url": f"http://127.0.0.1:{args.port}",
        "registry": registry,
        "store": store,
    }
    if args.max_in_flight is not None:
        options["max_in_flight"] = args.max_in_flight
    if args.upload_timeout is not None:
        options["upload_timeout"] = timedelta(seconds=args.upload_timeout)
    if args.hook:
        options["on_complete"] = on_complete
    accepted = inspect.signature(UploadGateway.__init__).parameters
    gateway = UploadGateway(**{k: v for k, v in options.items() if k in accepted})
    issue_params = inspect.signature(gateway.issue).parameters
    status_params = inspect.signature(gateway.status).parameters
    caps = {
        "owner": "owner" in issue_params,
        "status_owner": "owner" in status_params,
        "expected_size": "expected_size" in issue_params,
        "expected_digest": "expected_digest" in issue_params,
        "claim": hasattr(gateway, "claim"),
        "upload_timeout": "upload_timeout" in accepted,
        "on_complete": "on_complete" in accepted,
        "hook_installed": args.hook and "on_complete" in accepted,
        "store": args.store,
        "redis_options": redis_options,
        "redis_pool": args.redis_pool if args.store == "redis" else None,
    }

    def unsupported(name: str) -> JSONResponse:
        return JSONResponse({"unsupported": name}, 400)

    async def issue(request: Request) -> JSONResponse:
        body = await request.json()
        kwargs: dict[str, Any] = {}
        if "ttl" in body:
            kwargs["ttl"] = timedelta(seconds=body["ttl"])
        if "accept" in body:
            kwargs["accept"] = tuple(body["accept"])
        if "max_size" in body:
            kwargs["max_size"] = body["max_size"]
        for name in ("owner", "expected_size", "expected_digest"):
            if name in body:
                if name not in issue_params:
                    return unsupported(name)
                kwargs[name] = body[name]
        try:
            issued = await gateway.issue(body.get("destination", "files"), **kwargs)
        except (ValueError, LookupError) as exc:
            return JSONResponse({"refused": type(exc).__name__, "detail": str(exc)}, 400)
        record = issued.record
        return JSONResponse(
            {
                "id": record.id,
                "path": f"{gateway.path}/{issued.secret}",
                "ttl": (record.expires_at - record.issued_at).total_seconds(),
            }
        )

    async def status(request: Request) -> JSONResponse:
        kwargs: dict[str, Any] = {}
        if "owner" in request.query_params:
            if "owner" not in status_params:
                return unsupported("owner")
            kwargs["owner"] = request.query_params["owner"]
        return JSONResponse(dict(await gateway.status(request.path_params["id"], **kwargs)))

    async def claim(request: Request) -> JSONResponse:
        if not caps["claim"]:
            return unsupported("claim")
        owner = request.query_params.get("owner")
        try:
            file = await gateway.claim(request.path_params["id"], owner=owner)
        except mcp_upload.ClaimRefused as exc:
            return JSONResponse({"refused": str(exc.reason)})
        return JSONResponse({"claimed": True, "file": dict(file)})

    async def capabilities(request: Request) -> JSONResponse:
        return JSONResponse(caps)

    async def version(request: Request) -> JSONResponse:
        return JSONResponse({"version": mcp_upload.__version__})

    async def hook_counts(request: Request) -> JSONResponse:
        if not caps["hook_installed"]:
            return unsupported("on_complete")
        return JSONResponse({"counts": dict(hooks), "total": sum(hooks.values())})

    return Starlette(
        routes=[
            *gateway.routes(),
            Route("/_issue", issue, methods=["POST"]),
            Route("/_status/{id}", status),
            Route("/_claim/{id}", claim, methods=["POST"]),
            Route("/_caps", capabilities),
            Route("/_version", version),
            Route("/_hooks", hook_counts),
        ]
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--backend", required=True)
    parser.add_argument("--store", choices=["memory", "sqlite", "redis"], default="memory")
    parser.add_argument("--max-size", type=int, default=None)
    parser.add_argument("--max-in-flight", type=int, default=None)
    parser.add_argument("--dest-timeout", type=float, default=60.0)
    parser.add_argument("--db", default=None, help="SqliteStore file, kept across restarts")
    parser.add_argument("--upload-timeout", type=float, default=None)
    parser.add_argument("--redis-url", default="redis://localhost:56379/0")
    parser.add_argument("--prefix", default="mcp_upload_stress", help="RedisStore key prefix")
    parser.add_argument(
        "--redis-pool",
        choices=["blocking", "default"],
        default="blocking",
        help="a BlockingConnectionPool, or Redis.from_url as the README shows",
    )
    parser.add_argument("--redis-max-connections", type=int, default=64)
    parser.add_argument(
        "--server-clock", action="store_true", help="RedisStore server_clock, if supported"
    )
    parser.add_argument(
        "--legacy-layout", action="store_true", help="RedisStore legacy_layout, if supported"
    )
    parser.add_argument("--hook", action="store_true", help="count on_complete calls")
    parser.add_argument("--hook-delay", type=float, default=0.0, help="seconds the hook sleeps")
    args = parser.parse_args()
    uvicorn.run(build(args), host="127.0.0.1", port=args.port, log_level="warning", backlog=4096)


if __name__ == "__main__":
    main()
