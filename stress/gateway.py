"""The gateway under test, run as its own process so its memory can be watched from
outside. It serves the real upload endpoint plus two harness-only routes: one that
mints a ticket the way a tool would, and one that reads a record's status.

Options the installed version does not know are dropped, so the same harness runs
against old and new releases and each runs on its own defaults.
"""

from __future__ import annotations

import argparse
import inspect
import os
import tempfile
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
    if args.store == "sqlite":
        store: Any = SqliteStore(os.path.join(tempfile.mkdtemp(), "tickets.db"))
    else:
        store = MemoryStore(max_records=1_000_000)
    options: dict[str, Any] = {
        "base_url": f"http://127.0.0.1:{args.port}",
        "registry": registry,
        "store": store,
    }
    if args.max_in_flight is not None:
        options["max_in_flight"] = args.max_in_flight
    accepted = inspect.signature(UploadGateway.__init__).parameters
    gateway = UploadGateway(**{k: v for k, v in options.items() if k in accepted})

    async def issue(request: Request) -> JSONResponse:
        body = await request.json()
        kwargs: dict[str, Any] = {}
        if "ttl" in body:
            kwargs["ttl"] = timedelta(seconds=body["ttl"])
        if "accept" in body:
            kwargs["accept"] = tuple(body["accept"])
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
        return JSONResponse(dict(await gateway.status(request.path_params["id"])))

    async def version(request: Request) -> JSONResponse:
        return JSONResponse({"version": mcp_upload.__version__})

    return Starlette(
        routes=[
            *gateway.routes(),
            Route("/_issue", issue, methods=["POST"]),
            Route("/_status/{id}", status),
            Route("/_version", version),
        ]
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--backend", required=True)
    parser.add_argument("--store", choices=["memory", "sqlite"], default="memory")
    parser.add_argument("--max-size", type=int, default=None)
    parser.add_argument("--max-in-flight", type=int, default=None)
    parser.add_argument("--dest-timeout", type=float, default=60.0)
    args = parser.parse_args()
    uvicorn.run(build(args), host="127.0.0.1", port=args.port, log_level="warning", backlog=4096)


if __name__ == "__main__":
    main()
