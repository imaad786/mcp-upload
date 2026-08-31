"""An MCP server with one tool that asks for a file and one that reports on it.

The server never receives file bytes through MCP. ``request_upload`` hands back a
short-lived upload URL. Whoever holds the file posts it there: a person in a browser,
an agent running ``curl -F file=@path <url>``, or the client in ``client.py``. The
gateway streams the bytes to the backend in ``backend.py`` and keeps a record, which
``check_upload`` reads.

Run ``python examples/backend.py`` first, then ``python examples/server.py``. The MCP
endpoint is http://127.0.0.1:8000/mcp and the upload endpoint is under /upload.
"""

from __future__ import annotations

import os

import uvicorn
from mcp.server.mcpserver import MCPServer

from mcp_upload import Destination, MemoryStore, Registry, UploadGateway
from mcp_upload.adapters.mcp import attach
from mcp_upload.types import AwaitingUpload, UploadStatus

BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:8000")
BACKEND_URL = os.environ.get("BACKEND_URL", "http://127.0.0.1:8001")

# The only place a destination URL is ever written. Tools pick it by name.
registry = Registry(
    Destination(
        name="files",
        url=f"{BACKEND_URL}/files/{{filename}}",
        method="PUT",
        max_size=100 * 1024 * 1024,
    )
)

gateway = UploadGateway(
    base_url=BASE_URL,
    registry=registry,
    store=MemoryStore(),
    server_name="upload-demo",
)

mcp = MCPServer(
    "upload-demo",
    instructions=(
        "To send a file, call request_upload, then POST the file as multipart/form-data "
        "with the field name 'file' to the returned upload URL, then call check_upload "
        "with the returned id."
    ),
)


@mcp.tool()
async def request_upload() -> AwaitingUpload:
    """Ask for a file. Returns a single-use upload URL that expires in fifteen minutes.
    Send the file there as a multipart POST with the field name 'file'."""
    issued = await gateway.issue("files", caller="request_upload")
    return gateway.describe(issued)


@mcp.tool()
async def check_upload(id: str) -> UploadStatus:
    """Report what happened to an upload: issued, redeemed, completed, failed, expired
    or unknown. A completed upload includes the file's name, size and SHA-256."""
    return await gateway.status(id)


attach(mcp, gateway)
app = mcp.streamable_http_app()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("PORT", "8000")))
