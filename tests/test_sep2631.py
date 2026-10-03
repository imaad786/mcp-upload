"""SEP-2631's files/authorizeUpload on both frameworks, end to end in process.

Each test runs on every framework that is installed: the official SDK's MCPServer and
FastMCP 4. The client is the real SDK client talking to the server object in memory,
and the upload goes to the gateway's own route through an ASGI transport, so the whole
flow runs: authorize, upload, pass the URI to a tool, resolve it there.
"""

from __future__ import annotations

import base64
import hashlib
import importlib.util
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pytest
from starlette.applications import Starlette

from mcp_upload import Destination, MemoryStore, Registry, UnknownDestination, UploadGateway
from mcp_upload.client import UploadFailed, authorize_upload, sha256_digest, upload_file
from mcp_upload.resolve import FileReferenceError, resolve_file
from mcp_upload.types import FileValue
from tests.conftest import Upstream, multipart

HAS_OFFICIAL_SDK = importlib.util.find_spec("mcp.server.mcpserver") is not None
HAS_FASTMCP = importlib.util.find_spec("fastmcp") is not None

pytestmark = pytest.mark.skipif(not HAS_OFFICIAL_SDK, reason="official SDK 2.x not installed")

if HAS_OFFICIAL_SDK:
    from mcp.shared.exceptions import MCPError

FRAMEWORKS = [
    "mcp",
    pytest.param("fastmcp", marks=pytest.mark.skipif(not HAS_FASTMCP, reason="no fastmcp")),
]

BASE = "http://server.test"
OLD_SETTINGS = {
    "version": "1",
    "transport": "multipart-form-data",
    "descriptor": "FileTransferDescriptor",
}


def b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()


@pytest.fixture
def store() -> MemoryStore:
    return MemoryStore()


@pytest.fixture
async def gateway(upstream: Upstream, store: MemoryStore) -> AsyncIterator[UploadGateway]:
    registry = Registry(
        Destination(name="files", url="http://backend.test/files/{id}", max_size=1000),
        Destination(name="images", url="http://backend.test/i/{id}", accept=("image/*",)),
    )
    http = httpx.AsyncClient(transport=httpx.MockTransport(upstream.handler))
    gw = UploadGateway(base_url=BASE, registry=registry, store=store, server_name="test", http=http)
    yield gw
    await http.aclose()


@pytest.fixture
async def uploads(gateway: UploadGateway) -> AsyncIterator[httpx.AsyncClient]:
    """An HTTP client that reaches the gateway's upload route."""
    app = Starlette(routes=gateway.routes())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE) as c:
        yield c


def build(
    framework: str,
    gateway: UploadGateway,
    *,
    enabled: bool = True,
    destination: str = "files",
    owner: Callable[[Any], Any] | None = None,
    tool_owner: str | None = None,
) -> Any:
    """A server with the extension and an ``ingest`` tool that claims a file URI."""

    tool_error: type[Exception]
    if framework == "mcp":
        from mcp.server.mcpserver.exceptions import ToolError

        tool_error = ToolError
    else:
        from fastmcp.exceptions import ToolError as FastToolError

        tool_error = FastToolError

    async def ingest(file: str) -> FileValue:
        # The SDK hides an unexpected exception's text from the client. A ToolError
        # carries it, so the model learns why the file could not be used.
        try:
            return await resolve_file(gateway, file, owner=tool_owner)
        except FileReferenceError as exc:
            raise tool_error(str(exc)) from exc

    if framework == "mcp":
        from mcp.server.mcpserver import MCPServer

        from mcp_upload.adapters.mcp_extension import UploadTicketExtension

        ext = (
            UploadTicketExtension(gateway, destination=destination, owner=owner)
            if enabled
            else UploadTicketExtension()
        )
        server = MCPServer("sep-test", extensions=[ext])
        server.tool()(ingest)
        return server

    from fastmcp import FastMCP

    from mcp_upload.adapters.fastmcp_extension import UploadTicketExtension as FastExt

    fast = FastMCP("sep-test")
    fast.add_extension(
        FastExt(gateway, destination=destination, owner=owner) if enabled else FastExt()
    )
    fast.tool()(ingest)
    return fast


@asynccontextmanager
async def connect(framework: str, server: Any) -> AsyncIterator[Any]:
    if framework == "mcp":
        from mcp.client.client import Client

        async with Client(server) as client:
            yield client
    else:
        from fastmcp import Client as FastClient

        async with FastClient(server) as fast_client:
            yield fast_client


async def call(framework: str, client: Any, name: str, args: dict[str, Any]) -> Any:
    if framework == "mcp":
        return await client.call_tool(name, args)
    return await client.call_tool_mcp(name, args)


# ----- the extension's settings and backward compatibility ------------------------------


def test_extension_without_arguments_is_unchanged() -> None:
    from mcp.server.mcpserver import MCPServer

    from mcp_upload.adapters.mcp_extension import IDENTIFIER, UploadTicketExtension

    ext = UploadTicketExtension()
    assert ext.settings() == OLD_SETTINGS
    assert list(ext.methods()) == []
    server = MCPServer("files", extensions=[ext])
    capabilities = server._lowlevel_server.get_capabilities()
    assert capabilities.extensions is not None
    assert capabilities.extensions[IDENTIFIER] == OLD_SETTINGS
    assert server._lowlevel_server.get_request_handler("files/authorizeUpload") is None


def test_enabled_extension_advertises_the_method(gateway: UploadGateway) -> None:
    from mcp.server.mcpserver import MCPServer

    from mcp_upload.adapters.mcp_extension import IDENTIFIER, UploadTicketExtension

    server = MCPServer("files", extensions=[UploadTicketExtension(gateway, destination="files")])
    capabilities = server._lowlevel_server.get_capabilities()
    assert capabilities.extensions is not None
    assert capabilities.extensions[IDENTIFIER] == {
        **OLD_SETTINGS,
        "methods": ["files/authorizeUpload"],
    }


def test_extension_arguments_are_checked(gateway: UploadGateway) -> None:
    from mcp_upload.adapters.mcp_extension import UploadTicketExtension

    with pytest.raises(ValueError, match="need a gateway"):
        UploadTicketExtension(destination="files")
    with pytest.raises(ValueError, match="destination name"):
        UploadTicketExtension(gateway)
    with pytest.raises(UnknownDestination):
        UploadTicketExtension(gateway, destination="nowhere")


@pytest.mark.skipif(not HAS_FASTMCP, reason="fastmcp not installed")
def test_fastmcp_extension_settings(gateway: UploadGateway) -> None:
    from mcp_upload.adapters.fastmcp_extension import UploadTicketExtension as FastExt

    assert FastExt().settings() == OLD_SETTINGS
    assert list(FastExt().methods()) == []
    enabled = FastExt(gateway, destination="files")
    assert enabled.settings()["methods"] == ["files/authorizeUpload"]
    assert [b.method for b in enabled.methods()] == ["files/authorizeUpload"]


# ----- the flow ------------------------------------------------------------------------


@pytest.mark.parametrize("framework", FRAMEWORKS)
async def test_authorize_upload_and_claim_through_a_tool(
    framework: str, gateway: UploadGateway, uploads: httpx.AsyncClient, upstream: Upstream
) -> None:
    data = b"hello, file"
    async with connect(framework, build(framework, gateway)) as client:
        uri = await upload_file(client, data, name="hello.txt", http=uploads)
        assert uri.startswith("mcp-file://test/up_")
        result = await call(framework, client, "ingest", {"file": uri})

    assert not result.is_error, result.content
    file = result.structured_content
    assert file is not None
    assert file["uri"] == uri
    assert file["size"] == len(data)
    assert file["digest"] == {"algorithm": "sha-256", "value": b64(data)}
    assert file["name"] == "hello.txt"
    assert file["mimeType"] == "text/plain"
    assert upstream.bodies == [data]
    record_id = uri.rsplit("/", 1)[1]
    assert (await gateway.status(record_id))["status"] == "claimed"


@pytest.mark.parametrize("framework", FRAMEWORKS)
async def test_upload_file_streams_from_disk(
    framework: str, gateway: UploadGateway, uploads: httpx.AsyncClient, tmp_path: Any
) -> None:
    path = tmp_path / "photo.png"
    data = bytes(range(256)) * 3
    path.write_bytes(data)
    async with connect(framework, build(framework, gateway, destination="images")) as client:
        authorized = await authorize_upload(
            client, name="photo.png", mime_type="image/png", size=len(data), digest=b64(data)
        )
        assert authorized["file"]["name"] == "photo.png"
        assert authorized["file"]["mimeType"] == "image/png"
        assert authorized["file"]["size"] == len(data)
        assert authorized["upload"]["method"] == "POST"
        assert authorized["upload"]["multipart"] == {"fileField": "file"}
        uri = await upload_file(client, path, http=uploads)
    file = await resolve_file(gateway, uri, claim=False)
    assert file["size"] == len(data)
    assert file["mimeType"] == "image/png"
    assert sha256_digest(path) == (len(data), {"algorithm": "sha-256", "value": b64(data)})


@pytest.mark.parametrize("framework", FRAMEWORKS)
async def test_bytes_that_differ_from_the_declared_digest_are_refused(
    framework: str, gateway: UploadGateway, uploads: httpx.AsyncClient, upstream: Upstream
) -> None:
    declared, sent = b"the real file", b"the fake file"
    async with connect(framework, build(framework, gateway)) as client:
        authorized = await authorize_upload(
            client, name="a.txt", size=len(declared), digest=b64(declared)
        )
        body, content_type = multipart([("file", "a.txt", sent, "text/plain")])
        posted = await uploads.post(
            authorized["upload"]["url"], content=body, headers={"Content-Type": content_type}
        )
        assert posted.status_code == 422
        assert posted.json()["error"] == "digest_mismatch"

        uri = authorized["file"]["uri"]
        result = await call(framework, client, "ingest", {"file": uri})
        assert result.is_error

    record_id = uri.rsplit("/", 1)[1]
    status = await gateway.status(record_id)
    assert status["status"] == "failed"
    assert status["error"] == "digest_mismatch"
    with pytest.raises(FileReferenceError) as refused:
        await resolve_file(gateway, uri)
    assert refused.value.reason == "failed"


async def test_upload_file_reports_a_refused_upload(
    gateway: UploadGateway, uploads: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The declared size becomes the ticket's limit, so bytes beyond it are refused by
    # the endpoint, and upload_file raises with the endpoint's code.
    from mcp.client.client import Client

    import mcp_upload.client as client_module

    async def understate(*args: Any, **kwargs: Any) -> dict[str, Any]:
        kwargs["size"] = 3
        return await authorize_upload(*args, **kwargs)

    monkeypatch.setattr(client_module, "authorize_upload", understate)
    async with Client(build("mcp", gateway)) as client:
        with pytest.raises(UploadFailed) as failed:
            await upload_file(client, b"0123456789", name="a.bin", http=uploads)
    assert failed.value.status_code in (413, 422)
    assert failed.value.error in ("too_large", "size_mismatch")


# ----- refusals --------------------------------------------------------------------------


@pytest.mark.parametrize("framework", FRAMEWORKS)
@pytest.mark.parametrize(
    ("destination", "kwargs", "expected"),
    [
        (
            "files",
            {"size": 5000},
            {"reason": "maxSizeExceeded", "maxSize": 1000, "actualSize": 5000},
        ),
        (
            "images",
            {"mime_type": "text/plain"},
            {"reason": "mimeTypeNotAccepted", "mimeType": "text/plain", "accept": ["image/*"]},
        ),
        ("files", {"mime_type": "not a type"}, {"reason": "invalidMimeType"}),
        ("files", {"digest": "not-base64!"}, {"reason": "invalidDigest"}),
        ("files", {"digest": b64(b"x")[:20]}, {"reason": "invalidDigest"}),
        ("files", {"digest": {"algorithm": "md5", "value": "x"}}, {"reason": "invalidDigest"}),
        ("files", {"size": -1}, {"reason": "invalidSize"}),
    ],
)
async def test_declarations_the_destination_refuses_are_invalid_params(
    framework: str,
    gateway: UploadGateway,
    store: MemoryStore,
    destination: str,
    kwargs: dict[str, Any],
    expected: dict[str, Any],
) -> None:
    async with connect(framework, build(framework, gateway, destination=destination)) as client:
        with pytest.raises(MCPError) as refused:
            await authorize_upload(client, name="x", **kwargs)
    assert refused.value.code == -32602
    data = refused.value.data
    assert isinstance(data, dict)
    assert {k: data[k] for k in expected} == expected
    assert store._by_id == {}


async def test_a_full_store_is_an_internal_error(upstream: Upstream) -> None:
    from mcp.client.client import Client

    registry = Registry(Destination(name="files", url="http://backend.test/{id}"))
    gateway = UploadGateway(
        base_url=BASE,
        registry=registry,
        store=MemoryStore(max_records=1),
        http=httpx.AsyncClient(transport=httpx.MockTransport(upstream.handler)),
    )
    async with Client(build("mcp", gateway)) as client:
        await authorize_upload(client, name="a")
        with pytest.raises(MCPError) as refused:
            await authorize_upload(client, name="b")
    assert refused.value.code == -32603
    assert refused.value.data == {"reason": "storeFull"}
    await gateway.aclose()


@pytest.mark.parametrize("framework", FRAMEWORKS)
@pytest.mark.parametrize("enabled", [False, None])
async def test_a_server_without_the_method_says_method_not_found(
    framework: str, gateway: UploadGateway, enabled: bool | None
) -> None:
    if enabled is None:
        # No extension at all.
        if framework == "mcp":
            from mcp.server.mcpserver import MCPServer

            server: Any = MCPServer("plain")
        else:
            from fastmcp import FastMCP

            server = FastMCP("plain")
    else:
        server = build(framework, gateway, enabled=False)
    async with connect(framework, server) as client:
        with pytest.raises(MCPError) as refused:
            await authorize_upload(client, name="a")
    assert refused.value.code == -32601


# ----- owners ----------------------------------------------------------------------------


@pytest.mark.parametrize("framework", FRAMEWORKS)
@pytest.mark.parametrize("is_async", [False, True])
async def test_tickets_are_bound_to_the_resolved_owner(
    framework: str, gateway: UploadGateway, uploads: httpx.AsyncClient, is_async: bool
) -> None:
    seen: list[Any] = []

    def sync_owner(ctx: Any) -> str:
        seen.append(ctx)
        return "alice"

    async def async_owner(ctx: Any) -> str:
        return sync_owner(ctx)

    owner = async_owner if is_async else sync_owner
    server = build(framework, gateway, owner=owner, tool_owner="bob")
    async with connect(framework, server) as client:
        uri = await upload_file(client, b"secret", name="s.txt", http=uploads)
        # The tool resolves as bob, who did not ask for the ticket.
        result = await call(framework, client, "ingest", {"file": uri})
        assert result.is_error

    assert len(seen) == 1
    assert seen[0].method == "files/authorizeUpload"
    record_id = uri.rsplit("/", 1)[1]
    assert (await gateway.status(record_id, owner="bob"))["status"] == "unknown"
    with pytest.raises(FileReferenceError) as refused:
        await resolve_file(gateway, uri, owner="bob")
    assert refused.value.reason == "not_found"
    file = await resolve_file(gateway, uri, owner="alice")
    assert file["size"] == len(b"secret")


# ----- the tool-side resolver ------------------------------------------------------------


async def test_resolve_file_refuses_what_it_cannot_use(
    gateway: UploadGateway, uploads: httpx.AsyncClient
) -> None:
    async def reason(uri: str, **kwargs: Any) -> str:
        with pytest.raises(FileReferenceError) as refused:
            await resolve_file(gateway, uri, **kwargs)
        assert refused.value.uri == uri
        return refused.value.reason

    assert await reason("https://example.com/up_abc") == "malformed"
    assert await reason("mcp-file://elsewhere/up_abc") == "other_server"
    assert await reason("mcp-file://test/up_abc/../x") == "malformed"
    assert await reason("mcp-file://test/") == "malformed"
    assert await reason("mcp-file://test/up_doesnotexist") == "not_found"
    assert await reason("mcp-file://test/up_doesnotexist", claim=False) == "not_found"

    issued = await gateway.issue("files")
    pending = gateway.uri(issued.record.id)
    assert await reason(pending) == "not_completed"
    assert await reason(pending, claim=False) == "not_completed"

    body, content_type = multipart([("file", "a.txt", b"abc", "text/plain")])
    posted = await uploads.post(
        issued.upload_url, content=body, headers={"Content-Type": content_type}
    )
    assert posted.status_code == 200
    # Reading does not use it up; claiming does, once.
    assert (await resolve_file(gateway, pending, claim=False))["size"] == 3
    assert (await resolve_file(gateway, pending))["size"] == 3
    assert await reason(pending) == "already_claimed"
    assert (await resolve_file(gateway, pending, claim=False))["size"] == 3


@pytest.mark.parametrize("framework", FRAMEWORKS)
async def test_the_same_uri_cannot_be_used_twice(
    framework: str, gateway: UploadGateway, uploads: httpx.AsyncClient
) -> None:
    async with connect(framework, build(framework, gateway)) as client:
        uri = await upload_file(client, b"once", name="once.txt", http=uploads)
        first = await call(framework, client, "ingest", {"file": uri})
        second = await call(framework, client, "ingest", {"file": uri})
    assert not first.is_error
    assert second.is_error
    assert "already been used" in str(second.content)
