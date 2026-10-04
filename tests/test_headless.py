"""``ask_for_upload`` for a harness with no person present, on both frameworks.

The tool returns input_required with a URL-mode elicitation. A client that only knows
URL elicitation sees the link. A harness reads the upload target from the result's
``_meta``, sends the bytes itself, and answers, and the retry returns the completed
record. These run the real SDK client against the server in memory, and the upload
goes to the gateway's own route through an ASGI transport.
"""

from __future__ import annotations

import base64
import hashlib
import importlib.util
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import httpx
import pytest
from starlette.applications import Starlette

from mcp_upload import Destination, MemoryStore, Registry, UploadGateway
from mcp_upload.auth import bearer_authenticator
from mcp_upload.client import send_file, upload_proof, upload_targets
from mcp_upload.types import EXTENSION_ID, UploadStatus
from tests.conftest import Upstream

HAS_OFFICIAL_SDK = importlib.util.find_spec("mcp.server.mcpserver") is not None
HAS_FASTMCP = importlib.util.find_spec("fastmcp") is not None

pytestmark = pytest.mark.skipif(not HAS_OFFICIAL_SDK, reason="official SDK 2.x not installed")

if HAS_OFFICIAL_SDK:
    # Module level: the SDK resolves a tool's annotations against module globals.
    from mcp.client.client import Client
    from mcp.server.mcpserver import Context, MCPServer
    from mcp_types import ElicitResult, InputRequiredResult

    from mcp_upload.adapters.mcp import ask_for_upload

if HAS_FASTMCP:
    from fastmcp import Context as FastContext

BASE = "http://server.test"
DATA = b"quarterly numbers\n" * 100


def b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()


class CountingStore(MemoryStore):
    def __init__(self) -> None:
        super().__init__()
        self.puts = 0

    async def put(self, record: Any) -> None:
        self.puts += 1
        await super().put(record)


class Tokens:
    async def verify_token(self, token: str) -> Any:
        from mcp.server.auth.provider import AccessToken

        if not token.endswith("-token"):
            return None
        return AccessToken(token=token, client_id="app", subject=token[:-6], scopes=[])


def subject(token: Any) -> str | None:
    value = getattr(token, "subject", None)
    return value if isinstance(value, str) else None


@pytest.fixture
def store() -> CountingStore:
    return CountingStore()


def make_gateway(upstream: Upstream, store: MemoryStore, **options: Any) -> UploadGateway:
    return UploadGateway(
        base_url=BASE,
        registry=Registry(Destination(name="files", url="http://backend.test/files/{id}")),
        store=store,
        server_name="test",
        http=httpx.AsyncClient(transport=httpx.MockTransport(upstream.handler)),
        **options,
    )


@pytest.fixture
def gateway(upstream: Upstream, store: CountingStore) -> UploadGateway:
    return make_gateway(upstream, store)


@pytest.fixture
async def uploads(gateway: UploadGateway) -> AsyncIterator[httpx.AsyncClient]:
    app = Starlette(routes=gateway.routes())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE) as c:
        yield c


Answer = Callable[[int, Any, Any], Awaitable[Any]]


async def drive(session: Any, answer: Answer, tool: str = "fetch_report") -> tuple[Any, int]:
    """What a harness does: call, and while the server asks for input, answer each
    request with its target and retry. ``answer(round, params, target)`` returns the
    ElicitResult. Returns the final result and how many rounds it took."""
    result = await session.call_tool(tool, {}, allow_input_required=True)
    rounds = 0
    while isinstance(result, InputRequiredResult):
        rounds += 1
        assert rounds <= 10, "the server never finished"
        targets = upload_targets(result)
        answers = {
            key: await answer(rounds, request.params, targets.get(key))
            for key, request in (result.input_requests or {}).items()
        }
        result = await session.call_tool(
            tool,
            {},
            input_responses=answers,
            request_state=result.request_state,
            allow_input_required=True,
        )
    return result, rounds


def server_for(gateway: UploadGateway, *, owner: str | None = None, **ask: Any) -> MCPServer:
    server = MCPServer("headless-test")

    async def fetch_report(ctx: Context[Any, Any]) -> UploadStatus | InputRequiredResult:
        return await ask_for_upload(ctx, gateway, "files", owner=owner, **ask)

    server.tool()(fetch_report)
    return server


async def test_the_first_round_carries_a_machine_readable_target(
    gateway: UploadGateway,
) -> None:
    server = server_for(
        gateway,
        name="q3.csv",
        media_type="text/csv",
        expected_size=len(DATA),
        expected_digest=b64(DATA),
    )
    async with Client(server) as client:
        first = await client.session.call_tool("fetch_report", allow_input_required=True)
    assert isinstance(first, InputRequiredResult)
    assert first.input_requests is not None
    params: Any = first.input_requests["upload"].params
    assert params.mode == "url"
    targets = upload_targets(first)
    assert list(targets) == ["upload"]
    file, upload = targets["upload"]["file"], targets["upload"]["upload"]
    record_id = file["uri"].rsplit("/", 1)[1]
    assert record_id.startswith("up_")
    assert file == {
        "uri": f"mcp-file://test/{record_id}",
        "name": "q3.csv",
        "mimeType": "text/csv",
        "size": len(DATA),
        "digest": {"algorithm": "sha-256", "value": b64(DATA)},
    }
    assert upload["url"] == params.url
    assert upload["method"] == "POST"
    assert first.request_state is not None
    # A client that knows nothing of the extension sees an ordinary URL elicitation.
    dumped = first.model_dump(by_alias=True, mode="json", exclude_none=True)
    assert set(dumped["inputRequests"]["upload"]["params"]) == {"mode", "message", "url"}
    assert set(dumped["_meta"][EXTENSION_ID]) == {"targets"}


async def test_a_harness_binds_the_bytes_and_gets_the_completed_record(
    gateway: UploadGateway, uploads: httpx.AsyncClient, upstream: Upstream, store: CountingStore
) -> None:
    server = server_for(gateway, expected_digest=b64(DATA))

    async def harness(round: int, params: Any, target: Any) -> ElicitResult:
        stored = await send_file(target, DATA, name="q3.csv", http=uploads)
        return ElicitResult(action="accept", _meta=upload_proof(stored))

    async with Client(server) as client:
        result, rounds = await drive(client.session, harness)
    status = result.structured_content
    assert status is not None
    assert status["status"] == "completed", status
    assert status["file"]["digest"] == {"algorithm": "sha-256", "value": b64(DATA)}
    assert status["file"]["size"] == len(DATA)
    assert status["file"]["name"] == "q3.csv"
    assert rounds == 1
    assert store.puts == 1
    assert upstream.bodies == [DATA]


async def test_an_early_retry_is_asked_again_for_the_same_ticket(
    gateway: UploadGateway, uploads: httpx.AsyncClient, store: CountingStore
) -> None:
    # A client that answers before the bytes are in used to get status "issued" back
    # and had nothing to do with it. It now gets the same request again.
    server = server_for(gateway)
    seen: list[tuple[str, str]] = []

    async def late(round: int, params: Any, target: Any) -> ElicitResult:
        seen.append((params.url, target["upload"]["url"]))
        if round == 3:
            await send_file(target, DATA, http=uploads)
        return ElicitResult(action="accept")

    async with Client(server) as client:
        result, rounds = await drive(client.session, late)
    assert result.structured_content is not None
    assert result.structured_content["status"] == "completed"
    assert rounds == 3
    assert len(set(seen)) == 1
    assert store.puts == 1

    # The SDK's own driver, with a person-style callback that accepts without
    # uploading, keeps being asked and gives up at its round limit rather than being
    # told "issued".
    from mcp.client import InputRequiredRoundsExceededError

    async def accept(context: Any, params: Any) -> ElicitResult:
        return ElicitResult(action="accept")

    async with Client(server_for(gateway), elicitation_callback=accept) as client:
        with pytest.raises(InputRequiredRoundsExceededError):
            await client.call_tool("fetch_report")


async def test_an_early_retry_while_bytes_arrive_is_asked_again(
    gateway: UploadGateway, store: CountingStore
) -> None:
    server = server_for(gateway)
    async with Client(server) as client:
        first = await client.session.call_tool("fetch_report", allow_input_required=True)
        assert isinstance(first, InputRequiredResult)
        target = upload_targets(first)["upload"]
        record_id = target["file"]["uri"].rsplit("/", 1)[1]
        await store.redeem(store._by_id[record_id].ticket_hash, gateway._clock())
        again = await client.session.call_tool(
            "fetch_report",
            input_responses={"upload": ElicitResult(action="accept")},
            request_state=first.request_state,
            allow_input_required=True,
        )
    assert isinstance(again, InputRequiredResult)
    assert upload_targets(again) == {"upload": target}
    assert store.puts == 1


@pytest.mark.parametrize(
    "proof",
    [
        {"digest": {"algorithm": "sha-256", "value": b64(b"something else")}},
        {"uri": "mcp-file://test/up_someoneelse"},
        {"size": 1},
        {"digest": "not an object"},
    ],
)
async def test_proof_that_does_not_match_the_record_is_refused(
    gateway: UploadGateway, uploads: httpx.AsyncClient, proof: dict[str, Any]
) -> None:
    server = server_for(gateway)

    async def liar(round: int, params: Any, target: Any) -> ElicitResult:
        await send_file(target, DATA, http=uploads)
        return ElicitResult(action="accept", _meta={EXTENSION_ID: {"file": proof}})

    async with Client(server) as client:
        result, rounds = await drive(client.session, liar)
    status = result.structured_content
    assert status is not None
    assert status["status"] == "failed"
    assert status["error"] == "proof_mismatch"
    assert status["details"]["reason"] == "proofMismatch"
    assert "file" not in status


async def test_proof_is_optional_and_the_record_decides(
    gateway: UploadGateway, uploads: httpx.AsyncClient
) -> None:
    # Proof claiming an upload that never happened changes nothing: still not done.
    server = server_for(gateway)
    async with Client(server) as client:
        first = await client.session.call_tool("fetch_report", allow_input_required=True)
        assert isinstance(first, InputRequiredResult)
        target = upload_targets(first)["upload"]
        claim = {EXTENSION_ID: {"file": {"uri": target["file"]["uri"], "digest": {}}}}
        bluff = await client.session.call_tool(
            "fetch_report",
            input_responses={"upload": ElicitResult(action="accept", _meta=claim)},
            request_state=first.request_state,
            allow_input_required=True,
        )
        assert isinstance(bluff, InputRequiredResult)
        stored = await send_file(target, DATA, http=uploads)
        done = await client.session.call_tool(
            "fetch_report",
            input_responses={"upload": ElicitResult(action="accept")},
            request_state=first.request_state,
        )
    assert done.structured_content is not None
    assert done.structured_content["status"] == "completed"
    assert done.structured_content["file"]["digest"] == stored["digest"]


async def test_a_forged_state_cannot_rebuild_a_link(gateway: UploadGateway) -> None:
    # The ticket in the state is checked against the stored hash. A state naming a real
    # record with a made-up ticket gets the record's status, never a URL.
    issued = await gateway.issue("files")

    class Ctx:
        request_state = f"{issued.record.id}.forgedticket"
        input_responses = {"upload": ElicitResult(action="accept")}

    result = await ask_for_upload(Ctx(), gateway, "files")
    assert result == {"id": issued.record.id, "status": "issued"}


async def test_a_raw_put_target(upstream: Upstream, store: CountingStore) -> None:
    gateway = make_gateway(upstream, store, raw_uploads=True)
    server = server_for(gateway, raw=True, media_type="application/pdf", name="a.pdf")
    app = Starlette(routes=gateway.routes())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE) as http:

        async def harness(round: int, params: Any, target: Any) -> ElicitResult:
            assert target["upload"]["method"] == "PUT"
            assert target["upload"]["headers"] == {"Content-Type": "application/pdf"}
            stored = await send_file(target, b"%PDF-1.7", http=http)
            return ElicitResult(action="accept", _meta=upload_proof(stored))

        async with Client(server) as client:
            result, rounds = await drive(client.session, harness)
    assert result.structured_content is not None
    file = result.structured_content["file"]
    assert (file["name"], file["mimeType"], file["size"]) == ("a.pdf", "application/pdf", 8)


async def test_bearer_only_headless_flow(upstream: Upstream, store: CountingStore) -> None:
    gateway = make_gateway(
        upstream,
        store,
        authenticate=bearer_authenticator(Tokens(), principal=subject),
        ticket_in_url=False,
    )
    server = server_for(gateway, owner="alice")
    app = Starlette(routes=gateway.routes())
    statuses: list[int] = []
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE) as http:

        async def harness(round: int, params: Any, target: Any) -> ElicitResult:
            assert target["upload"]["_meta"] == {EXTENSION_ID: {"auth": "bearer"}}
            assert "." not in params.url.rsplit("/", 1)[1]
            for token in (None, "bob-token"):
                headers = {} if token is None else {"Authorization": f"Bearer {token}"}
                refused = await http.post(
                    target["upload"]["url"], files={"file": ("x", b"x")}, headers=headers
                )
                statuses.append(refused.status_code)
            stored = await send_file(target, DATA, bearer="alice-token", http=http)
            return ElicitResult(action="accept", _meta=upload_proof(stored))

        async with Client(server) as client:
            result, rounds = await drive(client.session, harness)
    assert statuses == [401, 403]
    assert result.structured_content is not None
    assert result.structured_content["status"] == "completed"
    assert upstream.bodies == [DATA]


async def test_declining_still_reports_declined(gateway: UploadGateway) -> None:
    server = server_for(gateway)

    async def no(context: Any, params: Any) -> ElicitResult:
        return ElicitResult(action="decline")

    async with Client(server, elicitation_callback=no) as client:
        result = await client.call_tool("fetch_report")
    assert result.structured_content is not None
    assert result.structured_content["status"] == "declined"


@pytest.mark.skipif(not HAS_FASTMCP, reason="fastmcp not installed")
async def test_fastmcp_headless_flow(
    gateway: UploadGateway, uploads: httpx.AsyncClient, store: CountingStore
) -> None:
    from fastmcp import Client as FastClient
    from fastmcp import FastMCP

    from mcp_upload.adapters.fastmcp import ask_for_upload as fast_ask

    fast = FastMCP("headless-fast")

    async def fetch_report(ctx: FastContext) -> UploadStatus | InputRequiredResult:
        return await fast_ask(ctx, gateway, "files", expected_size=len(DATA))

    fast.tool()(fetch_report)

    urls: list[str] = []

    async def harness(round: int, params: Any, target: Any) -> ElicitResult:
        assert target["file"]["size"] == len(DATA)
        urls.append(params.url)
        if round == 1:
            return ElicitResult(action="accept")  # too early, asked again
        stored = await send_file(target, DATA, http=uploads)
        return ElicitResult(action="accept", _meta=upload_proof(stored))

    async with FastClient(fast) as client:
        result, rounds = await drive(client.session, harness)
    assert result.structured_content is not None
    assert result.structured_content["status"] == "completed", result
    assert result.structured_content["file"]["digest"] == {
        "algorithm": "sha-256",
        "value": b64(DATA),
    }
    assert rounds == 2 and urls[0] == urls[1]
    assert store.puts == 1


FRAMEWORKS = [
    "mcp",
    pytest.param("fastmcp", marks=pytest.mark.skipif(not HAS_FASTMCP, reason="no fastmcp")),
]


@pytest.mark.parametrize("framework", FRAMEWORKS)
async def test_the_same_token_authorizes_the_call_and_the_upload(
    framework: str, upstream: Upstream, store: CountingStore
) -> None:
    # The whole of the "same bearer token" story over Streamable HTTP: the server's own
    # token verifier guards the MCP route, the gateway checks uploads with the same
    # verifier, and the ticket's owner is the caller's principal, so only the token that
    # made the call can upload, and only to a URL that carries no secret.
    import contextlib

    import httpx2
    from mcp.client.streamable_http import streamable_http_client

    loopback = "http://127.0.0.1:8000"
    app: Any
    lifespan: Any
    if framework == "mcp":
        from mcp.server.auth.settings import AuthSettings
        from pydantic import AnyHttpUrl

        from mcp_upload.adapters import mcp as adapter

        verifier = Tokens()
        gateway = make_gateway(
            upstream, store, authenticate=adapter.authenticator(verifier), ticket_in_url=False
        )
        server = MCPServer(
            "same-token",
            auth=AuthSettings(
                issuer_url=AnyHttpUrl("http://auth.test"),
                resource_server_url=AnyHttpUrl(f"{loopback}/mcp"),
            ),
            token_verifier=verifier,
        )

        async def fetch_report(ctx: Context[Any, Any]) -> UploadStatus | InputRequiredResult:
            return await adapter.ask_for_upload(
                ctx, gateway, "files", owner=adapter.current_principal()
            )

        server.tool()(fetch_report)
        adapter.attach(server, gateway)
        app = server.streamable_http_app()
        lifespan = server.session_manager.run()
    else:
        from fastmcp import FastMCP
        from fastmcp.server.auth.providers.jwt import StaticTokenVerifier

        from mcp_upload.adapters import fastmcp as fast_adapter

        provider = StaticTokenVerifier(
            tokens={
                f"{user}-token": {"client_id": "app", "sub": user, "scopes": []}
                for user in ("alice", "bob")
            }
        )
        fast = FastMCP("same-token", auth=provider)
        gateway = make_gateway(
            upstream, store, authenticate=fast_adapter.authenticator(fast), ticket_in_url=False
        )

        async def fast_fetch(ctx: FastContext) -> UploadStatus | InputRequiredResult:
            return await fast_adapter.ask_for_upload(
                ctx, gateway, "files", owner=fast_adapter.current_principal()
            )

        fast.tool(name="fetch_report")(fast_fetch)
        fast_adapter.attach(fast, gateway)
        app = fast.http_app(path="/mcp")
        lifespan = app.router.lifespan_context(app)

    refusals: list[int] = []
    owners: list[str | None] = []
    async with contextlib.AsyncExitStack() as stack:
        await stack.enter_async_context(lifespan)
        mcp_http = await stack.enter_async_context(
            httpx2.AsyncClient(
                transport=httpx2.ASGITransport(app=app),
                base_url=loopback,
                headers={"Authorization": "Bearer alice-token"},
            )
        )
        uploads = await stack.enter_async_context(
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=loopback)
        )
        client = await stack.enter_async_context(
            Client(streamable_http_client(f"{loopback}/mcp", http_client=mcp_http))
        )

        async def harness(round: int, params: Any, target: Any) -> ElicitResult:
            record = await store.get(target["file"]["uri"].rsplit("/", 1)[1])
            owners.append(None if record is None else record.owner)
            for token in (None, "bob-token", "mallory"):
                try:
                    await send_file(target, b"x", bearer=token, http=uploads)
                except Exception as exc:
                    refusals.append(getattr(exc, "status_code", 0))
            stored = await send_file(target, DATA, bearer="alice-token", http=uploads)
            return ElicitResult(action="accept", _meta=upload_proof(stored))

        result, rounds = await drive(client.session, harness)

    assert refusals == [401, 403, 401]
    assert owners == ['["app",null,"alice"]']
    assert rounds == 1
    assert result.structured_content is not None
    assert result.structured_content["status"] == "completed"
    assert result.structured_content["file"]["digest"]["value"] == b64(DATA)
    assert upstream.bodies == [DATA]
