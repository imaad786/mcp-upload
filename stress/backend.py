"""A storage backend for the stress harness, run as its own process.

It commits an upload only when the request body ended cleanly, which is what a real
backend should do, and records the size and SHA-256 of every commit so the harness can
check that what the gateway reported is what actually arrived. It can be made slow or
flaky at runtime through ``/_config``.
"""

from __future__ import annotations

import asyncio
import hashlib
import random
import sys

import uvicorn
from starlette.applications import Starlette
from starlette.requests import ClientDisconnect, Request
from starlette.responses import JSONResponse
from starlette.routing import Route

STATE: dict[str, object] = {}


def reset() -> None:
    STATE.update(
        commits={},
        incomplete=0,
        concurrent=0,
        peak=0,
        delay=0.0,
        fail_rate=0.0,
        rejected=0,
    )


async def put(request: Request) -> JSONResponse:
    STATE["concurrent"] = int(STATE["concurrent"]) + 1  # type: ignore[call-overload]
    STATE["peak"] = max(int(STATE["peak"]), int(STATE["concurrent"]))  # type: ignore[call-overload]
    try:
        if float(STATE["delay"]):  # type: ignore[arg-type]
            await asyncio.sleep(float(STATE["delay"]))  # type: ignore[arg-type]
        digest = hashlib.sha256()
        size = 0
        try:
            async for chunk in request.stream():
                digest.update(chunk)
                size += len(chunk)
        except ClientDisconnect:
            STATE["incomplete"] = int(STATE["incomplete"]) + 1  # type: ignore[call-overload]
            return JSONResponse({"error": "incomplete"}, status_code=400)
        if random.random() < float(STATE["fail_rate"]):  # type: ignore[arg-type]
            STATE["rejected"] = int(STATE["rejected"]) + 1  # type: ignore[call-overload]
            return JSONResponse({"error": "random failure"}, status_code=500)
        commits = STATE["commits"]
        assert isinstance(commits, dict)
        key = request.path_params["name"]
        if key in commits:
            return JSONResponse({"error": "duplicate commit"}, status_code=409)
        commits[key] = {"size": size, "sha256": digest.hexdigest()}
        return JSONResponse({"ok": True}, status_code=201)
    finally:
        STATE["concurrent"] = int(STATE["concurrent"]) - 1  # type: ignore[call-overload]


async def stats(request: Request) -> JSONResponse:
    return JSONResponse(STATE)


async def configure(request: Request) -> JSONResponse:
    body = await request.json()
    if body.get("reset"):
        reset()
    for key in ("delay", "fail_rate"):
        if key in body:
            STATE[key] = float(body[key])
    return JSONResponse({"ok": True})


reset()
app = Starlette(
    routes=[
        Route("/files/{name}", put, methods=["PUT"]),
        Route("/_stats", stats),
        Route("/_config", configure, methods=["POST"]),
    ]
)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(sys.argv[1]), log_level="warning", backlog=4096)
