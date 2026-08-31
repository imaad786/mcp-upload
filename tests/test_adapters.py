"""The adapters register the endpoint on a real server object from each framework.

The two frameworks cannot be installed together (fastmcp 3.x pins the official SDK
below 2.0), so each test runs where its framework is present and is skipped elsewhere.
CI has one lane per framework, so both run there.
"""

from __future__ import annotations

import importlib.util
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

from mcp_upload import Destination, MemoryStore, Registry, UploadGateway
from tests.conftest import Upstream, multipart

HAS_OFFICIAL_SDK = importlib.util.find_spec("mcp.server.mcpserver") is not None
HAS_FASTMCP = importlib.util.find_spec("fastmcp") is not None


@pytest.fixture
async def gateway(upstream: Upstream) -> AsyncIterator[UploadGateway]:
    registry = Registry(Destination(name="files", url="http://backend.test/files/{filename}"))
    http = httpx.AsyncClient(transport=httpx.MockTransport(upstream.handler))
    gw = UploadGateway(
        base_url="http://server.test", registry=registry, store=MemoryStore(), http=http
    )
    yield gw
    await http.aclose()


async def round_trip(app: Any, gateway: UploadGateway, upstream: Upstream) -> None:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://server.test"
    ) as client:
        issued = await gateway.issue("files")
        form = await client.get(issued.upload_url)
        assert form.status_code == 200
        assert "<form" in form.text

        body, content_type = multipart([("file", "a.txt", b"hello", "text/plain")])
        posted = await client.post(
            issued.upload_url, content=body, headers={"Content-Type": content_type}
        )
        assert posted.status_code == 200, posted.text
        assert posted.json()["status"] == "completed"
        assert upstream.bodies == [b"hello"]


@pytest.mark.skipif(not HAS_OFFICIAL_SDK, reason="official SDK 2.x not installed")
async def test_official_sdk_adapter(gateway: UploadGateway, upstream: Upstream) -> None:
    from mcp.server.mcpserver import MCPServer

    from mcp_upload.adapters.mcp import attach

    server = MCPServer("adapter-test")
    attach(server, gateway)
    await round_trip(server.streamable_http_app(), gateway, upstream)


@pytest.mark.skipif(not HAS_OFFICIAL_SDK, reason="official SDK 2.x not installed")
async def test_official_sdk_auth_does_not_guard_the_upload_route(
    gateway: UploadGateway, upstream: Upstream
) -> None:
    # The whole design depends on the upload endpoint being reachable without a bearer
    # token: a browser form or a curl command has none. The SDK documents that custom
    # routes skip its authorization. If an SDK upgrade ever changes that, this fails.
    from mcp.server.auth.provider import AccessToken
    from mcp.server.auth.settings import AuthSettings
    from mcp.server.mcpserver import MCPServer
    from pydantic import AnyHttpUrl

    from mcp_upload.adapters.mcp import attach

    class DenyAll:
        async def verify_token(self, token: str) -> AccessToken | None:
            return None

    server = MCPServer(
        "adapter-auth-test",
        auth=AuthSettings(
            issuer_url=AnyHttpUrl("http://auth.test"),
            resource_server_url=AnyHttpUrl("http://server.test/mcp"),
        ),
        token_verifier=DenyAll(),
    )
    attach(server, gateway)
    app = server.streamable_http_app()

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://server.test"
    ) as client:
        guarded = await client.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "ping"})
        assert guarded.status_code == 401
        issued = await gateway.issue("files")
        assert (await client.get(issued.upload_url)).status_code == 200


@pytest.mark.skipif(not HAS_FASTMCP, reason="fastmcp not installed")
async def test_fastmcp_adapter(gateway: UploadGateway, upstream: Upstream) -> None:
    from fastmcp import FastMCP

    from mcp_upload.adapters.fastmcp import attach

    server = FastMCP("adapter-test")
    attach(server, gateway)
    await round_trip(server.http_app(), gateway, upstream)
