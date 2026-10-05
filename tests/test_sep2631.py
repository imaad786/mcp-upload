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


@pytest.mark.parametrize("framework", FRAMEWORKS)
async def test_a_parameterized_media_type_is_echoed_exactly_through_the_flow(
    framework: str, gateway: UploadGateway, uploads: httpx.AsyncClient, upstream: Upstream
) -> None:
    # The wire shapes of waygate 1.1.0: a mimeType with parameters, and a file part
    # with no filename. A client may compare the mimeType it gets back exactly.
    data = b"caf\xc3\xa9\n" * 10
    declared = "text/plain; charset=utf-8"
    async with connect(framework, build(framework, gateway)) as client:
        authorized = await authorize_upload(
            client, mime_type=declared, size=len(data), digest=b64(data)
        )
        assert authorized["file"]["mimeType"] == declared
        body, content_type = multipart([("file", None, data, declared)])
        posted = await uploads.post(
            authorized["upload"]["url"], content=body, headers={"Content-Type": content_type}
        )
        assert posted.status_code == 200, posted.text
        assert posted.json()["file"]["mimeType"] == declared
        result = await call(framework, client, "ingest", {"file": authorized["file"]["uri"]})

    assert not result.is_error, result.content
    file = result.structured_content
    assert file is not None
    assert (file["name"], file["mimeType"], file["size"]) == ("upload", declared, len(data))
    assert file["digest"] == {"algorithm": "sha-256", "value": b64(data)}
    assert upstream.bodies == [data]
    assert upstream.requests[0].headers["content-type"] == declared


@pytest.mark.parametrize("framework", FRAMEWORKS)
async def test_an_authorized_type_matches_uploads_on_the_base_type(
    framework: str, gateway: UploadGateway, uploads: httpx.AsyncClient
) -> None:
    async with connect(framework, build(framework, gateway)) as client:
        authorized = await authorize_upload(client, mime_type="text/plain; charset=utf-8")
        body, content_type = multipart([("file", "a.png", b"x", "image/png")])
        posted = await uploads.post(
            authorized["upload"]["url"], content=body, headers={"Content-Type": content_type}
        )
    assert posted.status_code == 415
    assert posted.json()["details"]["accept"] == ["text/plain"]


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
        ("files", {"mime_type": "text/plain; charset"}, {"reason": "invalidMimeType"}),
        ("files", {"mime_type": 'text/plain; a="b\r\n'}, {"reason": "invalidMimeType"}),
        ("files", {"mime_type": "image/*"}, {"reason": "invalidMimeType"}),
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


def wire_request(params: dict[str, Any]) -> tuple[dict[str, Any], dict[str, str]]:
    """A hand-built 2026-07-28 files/authorizeUpload request and its headers."""
    params = {
        **params,
        "_meta": {
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientCapabilities": {
                "files": {"upload": True, "transports": ["https"]}
            },
        },
    }
    body = {"jsonrpc": "2.0", "id": 1, "method": "files/authorizeUpload", "params": params}
    headers = {
        "accept": "application/json, text/event-stream",
        "mcp-protocol-version": "2026-07-28",
        "mcp-method": "files/authorizeUpload",
    }
    return body, headers


@pytest.mark.parametrize("stateless", [False, True])
async def test_owner_from_the_access_token(gateway: UploadGateway, stateless: bool) -> None:
    # The owner resolver the docs recommend: the authenticated user from the bearer
    # token, read through the SDK's auth context inside the method handler.
    import httpx2
    from mcp.server.auth.middleware.auth_context import get_access_token
    from mcp.server.auth.provider import AccessToken
    from mcp.server.auth.settings import AuthSettings
    from mcp.server.mcpserver import MCPServer
    from pydantic import AnyHttpUrl

    from mcp_upload.adapters.mcp_extension import UploadTicketExtension

    class Tokens:
        async def verify_token(self, token: str) -> AccessToken | None:
            user = token.removesuffix("-token")
            return AccessToken(token=token, client_id="app", subject=user, scopes=[])

    def user_of(ctx: Any) -> str | None:
        token = get_access_token()
        return token.subject if token else None

    server = MCPServer(
        "auth",
        auth=AuthSettings(
            issuer_url=AnyHttpUrl("http://auth.test"),
            resource_server_url=AnyHttpUrl("http://127.0.0.1:8000/mcp"),
            validate_token_resource=False,
        ),
        token_verifier=Tokens(),
        extensions=[UploadTicketExtension(gateway, destination="files", owner=user_of)],
    )
    app = server.streamable_http_app(stateless_http=stateless, json_response=True)
    body, headers = wire_request({"name": "a.txt"})
    headers["authorization"] = "Bearer alice-token"
    async with (
        server.session_manager.run(),
        httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=app), base_url="http://127.0.0.1:8000"
        ) as http,
    ):
        response = await http.post("/mcp", json=body, headers=headers)
    assert response.status_code == 200, response.text
    uri = response.json()["result"]["file"]["uri"]
    record_id = uri.rsplit("/", 1)[1]
    assert (await gateway.status(record_id, owner="alice"))["status"] == "issued"
    assert (await gateway.status(record_id, owner="bob"))["status"] == "unknown"


@pytest.mark.parametrize("stateless", [False, True])
async def test_the_wire_shape_over_streamable_http(
    gateway: UploadGateway, uploads: httpx.AsyncClient, stateless: bool
) -> None:
    # A hand-built 2026-07-28 request, so the field names are the proposal's and not
    # whatever the SDK's own client happens to send.
    import httpx2

    server = build("mcp", gateway)
    app = server.streamable_http_app(stateless_http=stateless, json_response=True)
    data = b"wire"
    body, headers = wire_request(
        {
            "name": "w.txt",
            "mimeType": "text/plain",
            "size": len(data),
            "digest": {"algorithm": "sha-256", "value": b64(data)},
        }
    )
    # The SDK refuses Host headers other than loopback unless configured otherwise.
    loopback = "http://127.0.0.1:8000"
    async with (
        server.session_manager.run(),
        httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app), base_url=loopback) as http,
    ):
        response = await http.post("/mcp", json=body, headers=headers)
    assert response.status_code == 200, response.text
    result = response.json()["result"]
    file, upload = result["file"], result["upload"]
    assert file["uri"].startswith("mcp-file://test/up_")
    assert file["name"] == "w.txt"
    assert file["mimeType"] == "text/plain"
    assert file["size"] == len(data)
    assert file["digest"] == {"algorithm": "sha-256", "value": b64(data)}
    assert upload["method"] == "POST"
    assert upload["multipart"] == {"fileField": "file"}
    assert upload["expiresAt"].endswith("Z")

    form, content_type = multipart([("file", "w.txt", data, "text/plain")])
    posted = await uploads.post(upload["url"], content=form, headers={"Content-Type": content_type})
    assert posted.status_code == 200, posted.text
    assert (await resolve_file(gateway, file["uri"]))["digest"] == file["digest"]


# ----- choosing a destination ----------------------------------------------------------


def chooser(framework: str, gateway: UploadGateway, **ext: Any) -> Any:
    """A server whose extension offers a choice of destinations, and no tools."""
    if framework == "mcp":
        from mcp.server.mcpserver import MCPServer

        from mcp_upload.adapters.mcp_extension import UploadTicketExtension

        return MCPServer("choose", extensions=[UploadTicketExtension(gateway, **ext)])
    from fastmcp import FastMCP

    from mcp_upload.adapters.fastmcp_extension import UploadTicketExtension as FastExt

    fast = FastMCP("choose")
    fast.add_extension(FastExt(gateway, **ext))
    return fast


@pytest.fixture
async def three(upstream: Upstream, store: MemoryStore) -> AsyncIterator[UploadGateway]:
    """A gateway with a third destination that is registered but never offered."""
    registry = Registry(
        Destination(name="files", url="http://backend.test/files/{id}", max_size=1000),
        Destination(name="archive", url="http://backend.test/archive/{id}", max_size=10),
        Destination(name="images", url="http://backend.test/i/{id}", accept=("image/*",)),
        Destination(name="internal", url="http://backend.test/internal/{id}"),
    )
    http = httpx.AsyncClient(transport=httpx.MockTransport(upstream.handler))
    gw = UploadGateway(base_url=BASE, registry=registry, store=store, server_name="test", http=http)
    yield gw
    await http.aclose()


def test_offered_destinations_are_checked_and_advertised(three: UploadGateway) -> None:
    from mcp_upload.adapters.mcp_extension import UploadTicketExtension

    ext = UploadTicketExtension(three, destination="files", destinations=["archive", "images"])
    assert ext.settings() == {
        **OLD_SETTINGS,
        "methods": ["files/authorizeUpload"],
        "destinations": ["files", "archive", "images"],
    }
    # Without a list, settings are exactly as in 1.0.
    assert "destinations" not in UploadTicketExtension(three, destination="files").settings()
    with pytest.raises(UnknownDestination):
        UploadTicketExtension(three, destination="files", destinations=["nowhere"])
    with pytest.raises(TypeError):
        UploadTicketExtension(three, destination="files", destinations="archive")
    with pytest.raises(ValueError, match="need a gateway"):
        UploadTicketExtension(destinations=["archive"])


@pytest.mark.parametrize("framework", FRAMEWORKS)
async def test_a_client_chooses_an_offered_destination(
    framework: str, three: UploadGateway, upstream: Upstream
) -> None:
    server = chooser(framework, three, destination="files", destinations=["archive", "images"])
    app = Starlette(routes=three.routes())
    async with (
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE) as uploads,
        connect(framework, server) as client,
    ):
        default = await upload_file(client, b"default", name="d.txt", http=uploads)
        chosen = await upload_file(
            client, b"archived", name="a.txt", http=uploads, destination="archive"
        )
        image = await authorize_upload(client, mime_type="image/png", destination="images")
    urls = [str(r.url) for r in upstream.requests]
    assert urls == [
        f"http://backend.test/files/{default.rsplit('/', 1)[1]}",
        f"http://backend.test/archive/{chosen.rsplit('/', 1)[1]}",
    ]
    record = await three._store.get(image["file"]["uri"].rsplit("/", 1)[1])
    assert record is not None and record.destination == "images"


@pytest.mark.parametrize("framework", FRAMEWORKS)
@pytest.mark.parametrize(
    ("offered", "requested", "allowed"),
    [
        (["archive"], "internal", ["files", "archive"]),
        (["archive"], "http://evil.test/x", ["files", "archive"]),
        (["archive"], 7, ["files", "archive"]),
        ([], "archive", ["files"]),
    ],
)
async def test_a_destination_not_offered_is_refused(
    framework: str,
    three: UploadGateway,
    store: MemoryStore,
    offered: list[str],
    requested: Any,
    allowed: list[str],
) -> None:
    from mcp_upload.adapters.sep2631 import (
        IDENTIFIER,
        AuthorizeUploadParams,
        AuthorizeUploadRequest,
        AuthorizeUploadResult,
    )

    server = chooser(framework, three, destination="files", destinations=offered)
    meta: Any = {IDENTIFIER: {"destination": requested}}
    request = AuthorizeUploadRequest(params=AuthorizeUploadParams(name="x", _meta=meta))
    async with connect(framework, server) as client:
        session = client.session
        with pytest.raises(MCPError) as refused:
            await session.send_request(request, AuthorizeUploadResult)
    assert refused.value.code == -32602
    assert refused.value.data == {"reason": "destinationNotAllowed", "allowed": allowed}
    assert store._by_id == {}


@pytest.mark.parametrize("framework", FRAMEWORKS)
async def test_the_chosen_destination_sets_the_limits(framework: str, three: UploadGateway) -> None:
    server = chooser(framework, three, destination="files", destinations=["archive"])
    async with connect(framework, server) as client:
        with pytest.raises(MCPError) as refused:
            await authorize_upload(client, size=100, destination="archive")
    assert refused.value.data == {"reason": "maxSizeExceeded", "maxSize": 10, "actualSize": 100}


async def test_bearer_gateways_need_an_owner_to_authorize(upstream: Upstream) -> None:
    from mcp.client.client import Client
    from mcp.server.mcpserver import MCPServer

    from mcp_upload.adapters.mcp_extension import UploadTicketExtension

    async def anyone(request: Any) -> str | None:
        return "someone"

    gateway = UploadGateway(
        base_url=BASE,
        registry=Registry(Destination(name="files", url="http://backend.test/{id}")),
        store=MemoryStore(),
        authenticate=anyone,
        ticket_in_url=False,
    )
    server = MCPServer("owners", extensions=[UploadTicketExtension(gateway, destination="files")])
    async with Client(server) as client:
        with pytest.raises(MCPError) as refused:
            await authorize_upload(client, name="x")
    assert refused.value.code == -32603
    assert refused.value.data == {"reason": "ownerRequired"}


async def test_upload_file_sends_the_bearer_token_when_asked(
    upstream: Upstream, store: MemoryStore
) -> None:
    from mcp.client.client import Client
    from mcp.server.mcpserver import MCPServer

    from mcp_upload.adapters.mcp_extension import UploadTicketExtension

    async def by_header(request: Any) -> str | None:
        header = request.headers.get("authorization", "")
        return "alice" if header == "Bearer alice-token" else None

    gateway = UploadGateway(
        base_url=BASE,
        registry=Registry(Destination(name="files", url="http://backend.test/{id}")),
        store=store,
        http=httpx.AsyncClient(transport=httpx.MockTransport(upstream.handler)),
        authenticate=by_header,
        ticket_in_url=False,
    )
    ext = UploadTicketExtension(gateway, destination="files", owner=lambda ctx: "alice")
    server = MCPServer("bearer", extensions=[ext])
    app = Starlette(routes=gateway.routes())
    async with (
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE) as http,
        Client(server) as client,
    ):
        authorized = await authorize_upload(client, name="b.txt")
        assert authorized["upload"]["_meta"] == {"me.imaadkhan/upload-ticket": {"auth": "bearer"}}
        with pytest.raises(UploadFailed) as refused:
            await upload_file(client, b"no token", name="n.txt", http=http)
        uri = await upload_file(
            client, b"with token", name="b.txt", http=http, bearer="alice-token"
        )
    assert refused.value.status_code == 401
    assert refused.value.error == "auth_required"
    assert upstream.bodies == [b"with token"]
    assert (await resolve_file(gateway, uri, owner="alice"))["size"] == len(b"with token")
