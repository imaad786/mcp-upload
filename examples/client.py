"""Drives the demo from a program that already holds the bytes.

Three steps: ask the server for an upload URL over MCP, post the file to that URL over
plain HTTP, then ask the server what happened. Usage:

    python examples/client.py path/to/file [http://127.0.0.1:8000/mcp]
"""

from __future__ import annotations

import asyncio
import os
import sys

import httpx
from mcp.client import Client


async def main(path: str, url: str) -> None:
    async with Client(url) as client:
        asked = await client.call_tool("request_upload")
        issued = asked.structured_content or {}
        print("upload url:", issued["upload"]["url"])

        async with httpx.AsyncClient() as http:
            with open(path, "rb") as handle:
                posted = await http.post(
                    issued["upload"]["url"],
                    files={
                        issued["upload"]["multipart"]["fileField"]: (os.path.basename(path), handle)
                    },
                )
        print("upload response:", posted.status_code, posted.json())

        checked = await client.call_tool("check_upload", {"id": issued["id"]})
        print("status:", checked.structured_content)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    asyncio.run(
        main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "http://127.0.0.1:8000/mcp")
    )
