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
        path = args.db or os.path.join(tempfile.mkdtemp(), "tickets.db")
        store: Any = SqliteStore(path)
    else:
        store = MemoryStore(max_records=1_000_000)
    options: dict[str, Any] = {
        "base_url": f"http://127.0.0.1:{args.port}",
        "registry": registry,
        "store": store,
    }
    if args.max_in_flight is not None:
        options["max_in_flight"] = args.max_in_flight
    if args.upload_timeout is not None:
        options["upload_timeout"] = timedelta(seconds=args.upload_timeout)
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

    return Starlette(
        routes=[
            *gateway.routes(),
            Route("/_issue", issue, methods=["POST"]),
            Route("/_status/{id}", status),
            Route("/_claim/{id}", claim, methods=["POST"]),
            Route("/_caps", capabilities),
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
    parser.add_argument("--db", default=None, help="SqliteStore file, kept across restarts")
    parser.add_argument("--upload-timeout", type=float, default=None)
    args = parser.parse_args()
    uvicorn.run(build(args), host="127.0.0.1", port=args.port, log_level="warning", backlog=4096)


if __name__ == "__main__":
    main()
