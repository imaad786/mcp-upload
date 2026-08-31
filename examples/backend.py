"""A stand-in for the backend that already exists.

It accepts ``PUT /files/{name}`` and writes the body to a directory as it arrives,
chunk by chunk. It knows nothing about MCP or tickets. That is the point: the gateway
streams into an ordinary HTTP API, and the API does not have to change.

Run it with ``python examples/backend.py``. It listens on 127.0.0.1:8001 and prints the
directory it writes into.
"""

from __future__ import annotations

import hashlib
import os
import tempfile

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

DIRECTORY = os.environ.get("BACKEND_DIR") or tempfile.mkdtemp(prefix="mcp-upload-backend-")


async def put_file(request: Request) -> JSONResponse:
    name = os.path.basename(request.path_params["name"]) or "upload"
    final = os.path.join(DIRECTORY, name)
    partial = final + ".part"
    size = 0
    digest = hashlib.sha256()
    # Write to a temporary name and rename at the end. If the request body stops
    # early, which is what the gateway does when an upload fails validation after the
    # file part, the partial file is removed and nothing is committed.
    try:
        with open(partial, "wb") as handle:
            async for chunk in request.stream():
                handle.write(chunk)
                digest.update(chunk)
                size += len(chunk)
    except BaseException:
        os.unlink(partial)
        raise
    os.replace(partial, final)
    return JSONResponse(
        {"stored": name, "bytes": size, "sha256": digest.hexdigest()}, status_code=201
    )


app = Starlette(routes=[Route("/files/{name}", put_file, methods=["PUT"])])


if __name__ == "__main__":
    print(f"backend writing into {DIRECTORY}")
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("BACKEND_PORT", "8001")))
