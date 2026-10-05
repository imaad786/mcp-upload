"""Bearer-token authentication on the upload endpoint, in all three modes.

The default mode (the ticket alone) must behave exactly as before. With an
authenticator, every refusal for a missing, invalid or other user's token must happen
before the ticket is spent, and must leave the backend untouched. With the token as
the only credential, single use must still hold under concurrency in every store.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from starlette.applications import Starlette
from starlette.requests import Request

from mcp_upload import Destination, MemoryStore, Registry, SqliteStore, Store, UploadGateway
from mcp_upload.auth import (
    InsufficientScope,
    TokenRefused,
    bearer_authenticator,
    bearer_token,
    canonical_resource,
    issued_for,
    token_principal,
)
from mcp_upload.tickets import utcnow
from mcp_upload.types import EXTENSION_ID
from tests.conftest import Upstream, multipart

BASE = "http://gateway.test"
HAS_OFFICIAL_SDK = importlib.util.find_spec("mcp.server.mcpserver") is not None
HAS_FASTMCP = importlib.util.find_spec("fastmcp") is not None


def _sdk_binds_resources() -> bool:
    """Whether the installed SDK has ``validate_token_resource``, new in ``mcp`` 2.2."""
    if not HAS_OFFICIAL_SDK:
        return False
    from mcp.server.auth.settings import AuthSettings

    return "validate_token_resource" in AuthSettings.model_fields


SDK_BINDS = _sdk_binds_resources()
needs_sdk_binding = pytest.mark.skipif(not SDK_BINDS, reason="needs mcp 2.2 or later")

# The resource this server's tokens are issued for, and another service's.
OWN = "https://mcp.test/mcp"
OTHER = "https://other.test/mcp"

Authenticate = Callable[[Request], Awaitable["str | None"]]


@dataclass
class Token:
    """Stands in for an SDK AccessToken."""

    token: str
    client_id: str = "app"
    subject: str | None = None
    scopes: list[str] = field(default_factory=lambda: ["files"])
    expires_at: int | None = None
    claims: dict[str, Any] | None = None
    resource: str | None = OWN


# ``<user>@<audience>-token`` is a token the verifier accepts for ``user`` but reports
# as issued for that audience: a valid token minted for another service.
AUDIENCES: dict[str, str | None] = {
    "other": OTHER,
    "none": None,
    "child": f"{OWN}/child",
    "respelled": "HTTPS://MCP.TEST:443/mcp/",
    "garbage": "not a url",
}


class Verifier:
    """Accepts ``<user>-token`` and refuses anything else. ``expired-token`` verifies
    but is past its expiry, and ``noscope-token`` carries no scopes, nor does
    ``<user>+noscope-token``. Every token is issued for ``OWN`` unless it names another
    audience after an ``@``."""

    def __init__(self) -> None:
        self.calls = 0

    async def verify_token(self, token: str) -> Token | None:
        self.calls += 1
        if not token.endswith("-token"):
            return None
        user = token.removesuffix("-token")
        if user.endswith("+noscope"):
            return Token(token, subject=user.removesuffix("+noscope"), scopes=[])
        if "@" in user:
            user, audience = user.split("@", 1)
            return Token(token, subject=user, resource=AUDIENCES[audience])
        if user == "expired":
            return Token(token, subject=user, expires_at=int(time.time()) - 10)
        if user == "noscope":
            return Token(token, subject=user, scopes=[])
        return Token(token, subject=user)


class CountingStore(MemoryStore):
    """Counts record lookups, to show a refusal happened before any."""

    def __init__(self) -> None:
        super().__init__()
        self.lookups = 0

    async def get(self, record_id: str) -> Any:
        self.lookups += 1
        return await super().get(record_id)

    async def get_by_hash(self, ticket_hash: str) -> Any:
        self.lookups += 1
        return await super().get_by_hash(ticket_hash)


def by_subject(token: Any) -> str | None:
    subject = getattr(token, "subject", None)
    return subject if isinstance(subject, str) else None


def build(
    upstream: Upstream,
    store: Store | None = None,
    *,
    authenticate: Authenticate | None = None,
    ticket_in_url: bool = True,
    clock: Callable[[], datetime] = utcnow,
) -> UploadGateway:
    registry = Registry(Destination(name="files", url="http://backend.test/files/{id}"))
    return UploadGateway(
        base_url=BASE,
        registry=registry,
        store=store or MemoryStore(),
        http=httpx.AsyncClient(transport=httpx.MockTransport(upstream.handler)),
        authenticate=authenticate,
        ticket_in_url=ticket_in_url,
        clock=clock,
        max_in_flight=None,
    )


@asynccontextmanager
async def serve(gateway: UploadGateway) -> AsyncIterator[httpx.AsyncClient]:
    app = Starlette(routes=gateway.routes())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE) as c:
        yield c


async def post(
    client: httpx.AsyncClient, url: str, data: bytes = b"hello", token: str | None = None
) -> httpx.Response:
    body, content_type = multipart([("file", "a.txt", data, "text/plain")])
    headers = {"Content-Type": content_type}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    return await client.post(url, content=body, headers=headers)


def subject_auth(resource: str | None = None, scopes: tuple[str, ...] = ()) -> Authenticate:
    return bearer_authenticator(
        Verifier(), principal=by_subject, resource=resource, required_scopes=scopes
    )


# ----- the default mode is unchanged ---------------------------------------------------


async def test_default_mode_needs_no_token_and_says_nothing_about_auth(
    upstream: Upstream,
) -> None:
    gateway = build(upstream)
    assert not gateway.authenticates
    assert gateway.ticket_in_url
    issued = await gateway.issue("files")
    assert "_meta" not in gateway.describe(issued)["upload"]
    async with serve(gateway) as client:
        assert (await client.get(issued.upload_url)).status_code == 200
        # A token that would be refused in another mode means nothing here.
        response = await post(client, issued.upload_url, token="garbage")
    assert response.status_code == 200, response.text
    assert "www-authenticate" not in response.headers
    assert upstream.bodies == [b"hello"]


def test_bearer_only_needs_an_authenticator(upstream: Upstream) -> None:
    with pytest.raises(ValueError, match="needs authenticate"):
        build(upstream, ticket_in_url=False)


# ----- ticket and bearer ---------------------------------------------------------------


async def test_ticket_and_bearer_accepts_the_owner(upstream: Upstream) -> None:
    gateway = build(upstream, authenticate=subject_auth())
    issued = await gateway.issue("files", owner="alice")
    upload = gateway.describe(issued)["upload"]
    assert upload["_meta"] == {EXTENSION_ID: {"auth": "bearer"}}
    assert issued.secret in upload["url"]
    assert "alice-token" not in json.dumps(upload)
    async with serve(gateway) as client:
        response = await post(client, issued.upload_url, token="alice-token")
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "completed"
    assert upstream.bodies == [b"hello"]


@pytest.mark.parametrize(
    ("token", "status", "code", "reason", "challenge"),
    [
        (None, 401, "auth_required", "authRequired", "Bearer"),
        ("not a token", 401, "invalid_token", "invalidToken", 'Bearer error="invalid_token"'),
        ("expired-token", 401, "invalid_token", "invalidToken", 'Bearer error="invalid_token"'),
        ("bob-token", 403, "forbidden", "ownerMismatch", None),
    ],
)
async def test_ticket_and_bearer_refusals_spend_nothing(
    upstream: Upstream,
    token: str | None,
    status: int,
    code: str,
    reason: str,
    challenge: str | None,
) -> None:
    gateway = build(upstream, authenticate=subject_auth())
    issued = await gateway.issue("files", owner="alice")
    async with serve(gateway) as client:
        refused = await post(client, issued.upload_url, token=token)
        assert refused.status_code == status, refused.text
        assert refused.json()["error"] == code
        assert refused.json()["details"] == {"reason": reason}
        assert refused.headers.get("www-authenticate") == challenge
        assert (await gateway.status(issued.record.id))["status"] == "issued"
        assert upstream.bodies == []
        # The ticket is still good for its owner.
        accepted = await post(client, issued.upload_url, token="alice-token")
    assert accepted.status_code == 200, accepted.text
    assert upstream.bodies == [b"hello"]


async def test_a_401_does_not_reveal_whether_the_ticket_exists(upstream: Upstream) -> None:
    gateway = build(upstream, authenticate=subject_auth())
    async with serve(gateway) as client:
        unknown = await post(client, f"{BASE}/upload/no-such-ticket")
    assert unknown.status_code == 401
    assert "id" not in unknown.json()


async def test_ticket_and_bearer_without_an_owner_takes_any_valid_token(
    upstream: Upstream,
) -> None:
    gateway = build(upstream, authenticate=subject_auth())
    issued = await gateway.issue("files")
    async with serve(gateway) as client:
        assert (await post(client, issued.upload_url)).status_code == 401
        response = await post(client, issued.upload_url, token="carol-token")
    assert response.status_code == 200, response.text


async def test_valid_token_but_spent_or_expired_ticket(upstream: Upstream) -> None:
    now = [utcnow()]
    gateway = build(upstream, authenticate=subject_auth(), clock=lambda: now[0])
    spent = await gateway.issue("files", owner="alice")
    stale = await gateway.issue("files", owner="alice", ttl=timedelta(seconds=5))
    async with serve(gateway) as client:
        assert (await post(client, spent.upload_url, token="alice-token")).status_code == 200
        again = await post(client, spent.upload_url, token="alice-token")
        now[0] += timedelta(seconds=10)
        late = await post(client, stale.upload_url, token="alice-token")
    assert (again.status_code, again.json()["error"]) == (410, "ticket_used")
    assert (late.status_code, late.json()["error"]) == (410, "ticket_expired")
    assert len(upstream.bodies) == 1


async def test_the_page_explains_instead_of_showing_a_form(upstream: Upstream) -> None:
    gateway = build(upstream, authenticate=subject_auth())
    issued = await gateway.issue("files", owner="alice")
    async with serve(gateway) as client:
        page = await client.get(issued.upload_url)
    assert page.status_code == 401
    assert page.headers["www-authenticate"] == "Bearer"
    assert "<form" not in page.text
    assert "browser" in page.text
    assert page.headers["content-security-policy"].startswith("default-src 'none'")


async def test_an_authenticator_that_raises_spends_nothing(upstream: Upstream) -> None:
    async def broken(request: Request) -> str | None:
        raise RuntimeError("introspection endpoint down")

    gateway = build(upstream, authenticate=broken)
    issued = await gateway.issue("files")
    async with serve(gateway) as client:
        response = await post(client, issued.upload_url, token="alice-token")
    assert response.status_code == 500
    assert response.json()["details"] == {"reason": "authenticatorFailed"}
    assert (await gateway.status(issued.record.id))["status"] == "issued"


# ----- bearer only ---------------------------------------------------------------------


async def test_bearer_only_urls_carry_no_secret(upstream: Upstream) -> None:
    gateway = build(upstream, authenticate=subject_auth(), ticket_in_url=False)
    assert not gateway.ticket_in_url
    with pytest.raises(ValueError, match="owner"):
        await gateway.issue("files")
    issued = await gateway.issue("files", owner="alice")
    assert issued.secret == ""
    assert issued.upload_url == f"{BASE}/upload/{issued.record.id}"
    described = gateway.describe(issued)
    assert described["upload"]["url"] == issued.upload_url
    assert described["upload"]["_meta"] == {EXTENSION_ID: {"auth": "bearer"}}


async def test_bearer_only_flow_and_refusals(upstream: Upstream) -> None:
    gateway = build(upstream, authenticate=subject_auth(), ticket_in_url=False)
    issued = await gateway.issue("files", owner="alice")
    url = issued.upload_url
    async with serve(gateway) as client:
        missing = await post(client, url)
        invalid = await post(client, url, token="forged")
        other = await post(client, url, token="bob-token")
        unknown = await post(client, f"{BASE}/upload/up_doesnotexist", token="alice-token")
        malformed = await post(client, f"{BASE}/upload/../../etc", token="alice-token")
        page = await client.get(url)
        assert (await gateway.status(issued.record.id))["status"] == "issued"
        assert upstream.bodies == []
        ok = await post(client, url, token="alice-token")
        replay = await post(client, url, token="alice-token")
    assert (missing.status_code, missing.json()["error"]) == (401, "auth_required")
    assert (invalid.status_code, invalid.json()["error"]) == (401, "invalid_token")
    assert (other.status_code, other.json()["error"]) == (403, "forbidden")
    assert unknown.status_code == 404
    assert malformed.status_code in (404, 405)
    assert page.status_code == 401 and "<form" not in page.text
    assert ok.status_code == 200, ok.text
    assert ok.json()["file"]["size"] == 5
    assert (replay.status_code, replay.json()["error"]) == (410, "ticket_used")
    assert upstream.bodies == [b"hello"]


async def test_bearer_only_ignores_a_ticket_in_the_url(upstream: Upstream) -> None:
    # A gateway switched to bearer-only must not keep honoring secrets: the path is
    # read as a record id and nothing else.
    shared = MemoryStore()
    old = build(upstream, shared)
    issued = await old.issue("files", owner="alice")
    gateway = build(upstream, shared, authenticate=subject_auth(), ticket_in_url=False)
    async with serve(gateway) as client:
        response = await post(client, issued.upload_url, token="alice-token")
    assert response.status_code == 404


def _redis_client() -> object:
    url = os.environ.get("MCP_UPLOAD_TEST_REDIS_URL")
    if url:
        from redis.asyncio import Redis

        return Redis.from_url(url)
    fakeredis = pytest.importorskip("fakeredis", reason="fakeredis is not installed")
    return fakeredis.aioredis.FakeRedis()


@pytest.fixture(params=["memory", "sqlite", "redis"])
def store(request: pytest.FixtureRequest, tmp_path: Path) -> Store:
    if request.param == "memory":
        return MemoryStore()
    if request.param == "sqlite":
        return SqliteStore(tmp_path / "tickets.db")
    from mcp_upload.redis_store import RedisStore

    return RedisStore(_redis_client(), prefix=f"t{uuid.uuid4().hex}")  # type: ignore[arg-type]


async def test_bearer_only_replay_race_has_one_winner(upstream: Upstream, store: Store) -> None:
    # The URL is public and the owner's token is reusable, so single use rests on the
    # store's atomic redemption alone. Fifty requests at once, one winner.
    gateway = build(upstream, store, authenticate=subject_auth(), ticket_in_url=False)
    issued = await gateway.issue("files", owner="alice")
    async with serve(gateway) as client:
        tries = [f"try {i}".encode() for i in range(50)]
        replies = await asyncio.gather(
            *(post(client, issued.upload_url, data, "alice-token") for data in tries)
        )
    codes = sorted(r.status_code for r in replies)
    assert codes.count(200) == 1, codes
    assert codes.count(410) == 49, codes
    assert len(upstream.bodies) == 1
    assert (await gateway.status(issued.record.id))["status"] == "completed"


async def test_ticket_and_bearer_race_has_one_winner(upstream: Upstream, store: Store) -> None:
    gateway = build(upstream, store, authenticate=subject_auth())
    issued = await gateway.issue("files", owner="alice")
    async with serve(gateway) as client:
        replies = await asyncio.gather(
            *(post(client, issued.upload_url, b"x", "alice-token") for _ in range(50))
        )
    assert sorted(r.status_code for r in replies).count(200) == 1
    assert len(upstream.bodies) == 1


# ----- resource binding (RFC 8707) ------------------------------------------------------


@pytest.mark.parametrize("ticket_in_url", [True, False])
@pytest.mark.parametrize("audience", ["other", "none", "child", "garbage"])
async def test_a_token_for_another_resource_spends_nothing(
    upstream: Upstream, ticket_in_url: bool, audience: str
) -> None:
    store = CountingStore()
    gateway = build(upstream, store, authenticate=subject_auth(OWN), ticket_in_url=ticket_in_url)
    issued = await gateway.issue("files", owner="alice")
    async with serve(gateway) as client:
        refused = await post(client, issued.upload_url, token=f"alice@{audience}-token")
        assert refused.status_code == 401, refused.text
        assert refused.json() == {
            "status": "failed",
            "error": "invalid_token",
            "details": {"reason": "wrongResource"},
        }
        assert refused.headers["www-authenticate"] == 'Bearer error="invalid_token"'
        assert store.lookups == 0
        assert (await gateway.status(issued.record.id))["status"] == "issued"
        assert upstream.bodies == []
        accepted = await post(client, issued.upload_url, token="alice-token")
    assert accepted.status_code == 200, accepted.text
    assert upstream.bodies == [b"hello"]


async def test_the_resource_compares_as_a_url(upstream: Upstream) -> None:
    # Case, a default port and a trailing slash do not make another resource.
    gateway = build(upstream, authenticate=subject_auth("https://MCP.test:443/mcp/"))
    issued = await gateway.issue("files", owner="alice")
    async with serve(gateway) as client:
        response = await post(client, issued.upload_url, token="alice@respelled-token")
    assert response.status_code == 200, response.text


async def test_without_a_resource_nothing_is_bound(upstream: Upstream) -> None:
    gateway = build(upstream, authenticate=subject_auth())
    issued = await gateway.issue("files", owner="alice")
    async with serve(gateway) as client:
        response = await post(client, issued.upload_url, token="alice@other-token")
    assert response.status_code == 200, response.text


async def test_custom_authenticators_can_refuse_with_a_reason(upstream: Upstream) -> None:
    async def refuse(request: Request) -> str | None:
        raise TokenRefused("tokenRevoked")

    gateway = build(upstream, authenticate=refuse)
    issued = await gateway.issue("files")
    async with serve(gateway) as client:
        response = await post(client, issued.upload_url, token="alice-token")
    assert response.status_code == 401
    assert response.json()["details"] == {"reason": "tokenRevoked"}
    assert (await gateway.status(issued.record.id))["status"] == "issued"


def test_a_resource_must_be_an_http_url() -> None:
    for bad in ("", "not a url", "ftp://mcp.test/mcp"):
        with pytest.raises(ValueError, match="HTTP"):
            bearer_authenticator(Verifier(), resource=bad)


@needs_sdk_binding
@pytest.mark.parametrize("configured", ["https://mcp.test/mcp", "https://mcp.test"])
async def test_resource_comparison_matches_the_sdk(configured: str) -> None:
    # The upload route must accept exactly the tokens the MCP endpoint accepts.
    from mcp.server.auth.middleware.bearer_auth import BearerAuthBackend
    from mcp.server.auth.settings import AuthSettings
    from pydantic import AnyHttpUrl

    settings = AuthSettings(
        issuer_url=AnyHttpUrl("https://auth.test"),
        resource_server_url=AnyHttpUrl(configured),
        validate_token_resource=True,
    )
    unused: Any = Verifier()  # the backend's verifier plays no part in the comparison
    sdk = BearerAuthBackend(unused, resource_server_url=settings.resource_server_url)
    ours = canonical_resource(configured)
    candidates: list[str | None] = [
        None,
        "",
        "not a url",
        configured,
        configured + "/",
        configured.upper(),
        configured.replace("https://mcp.test", "https://mcp.test:443"),
        configured.replace("https://mcp.test", "https://mcp.test:8443"),
        configured.replace("https://", "http://"),
        configured + "/child",
        configured + "?x=1",
        configured + "#frag",
        "https://mcp.test/mcpx",
        "https://other.test/mcp",
        "https://mcp.test.evil/mcp",
        " " + configured,
    ]
    for value in candidates:
        token = Token("t", resource=value)
        expected = sdk._issued_for_this_resource(value)
        assert issued_for(token, ours) is expected, value


async def outcome(authenticate: Authenticate, token: str) -> str:
    """What an authenticator makes of ``token``: the principal, or why it refused."""
    try:
        principal = await authenticate(request_with({"Authorization": f"Bearer {token}"}))
    except TokenRefused as exc:
        return exc.reason
    return principal or "refused"


def sdk_settings(validate: bool | None, scopes: list[str] | None = None) -> Any:
    from mcp.server.auth.settings import AuthSettings
    from pydantic import AnyHttpUrl

    return AuthSettings(
        issuer_url=AnyHttpUrl("https://auth.test"),
        resource_server_url=AnyHttpUrl(OWN),
        validate_token_resource=validate,
        required_scopes=scopes,
    )


@needs_sdk_binding
@pytest.mark.parametrize(
    ("validate", "resource", "own", "other", "unbound"),
    [
        # Follows the server's settings.
        (True, None, "alice", "wrongResource", "wrongResource"),
        (False, None, "alice", "alice", "alice"),
        # An explicit argument wins either way.
        (True, False, "alice", "alice", "alice"),
        (False, OWN, "alice", "wrongResource", "wrongResource"),
        (True, OTHER, "wrongResource", "alice", "wrongResource"),
    ],
)
async def test_official_adapter_follows_the_server_settings(
    validate: bool, resource: Any, own: str, other: str, unbound: str
) -> None:
    from mcp_upload.adapters import mcp as adapter

    authenticate = adapter.authenticator(
        Verifier(), principal=by_subject, auth=sdk_settings(validate), resource=resource
    )
    assert await outcome(authenticate, "alice-token") == own
    assert await outcome(authenticate, "alice@other-token") == other
    assert await outcome(authenticate, "alice@none-token") == unbound


async def test_settings_and_tokens_from_before_mcp_2_2() -> None:
    # On mcp 2.1 the settings have no validate_token_resource and the tokens no
    # resource. Nothing is bound by default, and an explicit binding refuses them all.
    from types import SimpleNamespace

    from mcp_upload.adapters import mcp as adapter

    @dataclass
    class OldToken:
        token: str
        client_id: str
        subject: str
        scopes: list[str] = field(default_factory=list)

    class OldVerifier:
        async def verify_token(self, token: str) -> OldToken | None:
            return OldToken(token, "app", "alice") if token == "alice-token" else None

    settings = SimpleNamespace(resource_server_url=OWN, required_scopes=None)
    follows = adapter.authenticator(OldVerifier(), principal=by_subject, auth=settings)
    explicit = adapter.authenticator(OldVerifier(), principal=by_subject, resource=OWN)
    assert await outcome(follows, "alice-token") == "alice"
    assert await outcome(explicit, "alice-token") == "wrongResource"


@pytest.mark.skipif(not HAS_OFFICIAL_SDK, reason="official SDK 2.x not installed")
async def test_official_adapter_without_settings_binds_nothing() -> None:
    from mcp_upload.adapters import mcp as adapter

    plain = adapter.authenticator(Verifier(), principal=by_subject)
    assert await outcome(plain, "alice@other-token") == "alice"
    assert await outcome(plain, "noscope-token") == "noscope"
    bound = adapter.authenticator(Verifier(), principal=by_subject, resource=OWN)
    assert await outcome(bound, "alice@other-token") == "wrongResource"


@pytest.mark.skipif(not HAS_OFFICIAL_SDK, reason="official SDK 2.x not installed")
async def test_official_adapter_takes_the_server_scopes() -> None:
    from mcp_upload.adapters import mcp as adapter

    settings = sdk_settings(False, scopes=["files"])
    inherited = adapter.authenticator(Verifier(), principal=by_subject, auth=settings)
    assert await outcome(inherited, "noscope-token") == "insufficientScope"
    assert await outcome(inherited, "alice-token") == "alice"
    overridden = adapter.authenticator(
        Verifier(), principal=by_subject, auth=settings, required_scopes=()
    )
    assert await outcome(overridden, "noscope-token") == "noscope"


@needs_sdk_binding
@pytest.mark.parametrize("validate", [True, False])
async def test_the_upload_route_takes_the_tokens_the_mcp_endpoint_takes(
    upstream: Upstream, validate: bool
) -> None:
    # One MCPServer, one verifier, one AuthSettings. Whatever the MCP endpoint does with
    # a token issued for another resource, the upload route does the same, before the
    # ticket is spent.
    from mcp.server.auth.provider import AccessToken
    from mcp.server.mcpserver import MCPServer

    from mcp_upload.adapters import mcp as adapter

    class SdkTokens:
        async def verify_token(self, token: str) -> AccessToken | None:
            found = await Verifier().verify_token(token)
            if found is None:
                return None
            return AccessToken(
                token=token,
                client_id="app",
                subject=found.subject,
                scopes=found.scopes,
                resource=found.resource,
            )

    settings = sdk_settings(validate)
    verifier = SdkTokens()
    gateway = build(
        upstream, authenticate=adapter.authenticator(verifier, auth=settings), ticket_in_url=False
    )
    server = MCPServer("resource-bound", auth=settings, token_verifier=verifier)
    adapter.attach(server, gateway)
    app = server.streamable_http_app(stateless_http=True, json_response=True)
    owner = token_principal(await verifier.verify_token("alice-token"))
    async with (
        server.session_manager.run(),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE) as client,
    ):
        ping = {"jsonrpc": "2.0", "id": 1, "method": "ping"}
        mcp_codes = {}
        upload_codes = {}
        for token in ("alice@other-token", "alice@none-token", "alice-token"):
            response = await client.post(
                "http://127.0.0.1:8000/mcp",
                json=ping,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json, text/event-stream",
                },
            )
            assert response.status_code in (200, 401), response.text
            mcp_codes[token] = response.status_code == 401
            issued = await gateway.issue("files", owner=owner)
            upload = await post(client, issued.upload_url, token=token)
            upload_codes[token] = upload.status_code == 401
            if upload.status_code == 401:
                assert upload.json()["details"] == {"reason": "wrongResource"}
                assert (await gateway.status(issued.record.id))["status"] == "issued"
    assert upload_codes == mcp_codes
    assert mcp_codes == {
        "alice@other-token": validate,
        "alice@none-token": validate,
        "alice-token": False,
    }
    assert len(upstream.bodies) == (1 if validate else 3)


@pytest.mark.skipif(not HAS_FASTMCP, reason="fastmcp not installed")
async def test_fastmcp_adapter_binds_only_when_asked() -> None:
    from fastmcp import FastMCP
    from fastmcp.server.auth.providers.jwt import StaticTokenVerifier

    from mcp_upload.adapters import fastmcp as fast_adapter

    provider = StaticTokenVerifier(
        tokens={"alice-token": {"client_id": "app", "sub": "alice", "scopes": []}}
    )
    fast = FastMCP("resource", auth=provider)
    plain = fast_adapter.authenticator(fast, principal=by_subject)
    bound = fast_adapter.authenticator(fast, principal=by_subject, resource=OWN)
    # FastMCP checks a token's audience in the provider, which both call. Its tokens
    # carry no resource, so an explicit binding refuses them.
    assert await outcome(plain, "alice-token") == "alice"
    assert await outcome(bound, "alice-token") == "wrongResource"
    assert await outcome(bound, "bob-token") == "refused"


# ----- scopes (RFC 6750 insufficient_scope) --------------------------------------------

SCOPE_CHALLENGE = (
    'Bearer error="insufficient_scope", error_description="Required scope: files", scope="files"'
)


@pytest.mark.parametrize("ticket_in_url", [True, False])
async def test_a_token_without_the_scope_is_403_and_spends_nothing(
    upstream: Upstream, ticket_in_url: bool
) -> None:
    store = CountingStore()
    gateway = build(
        upstream, store, authenticate=subject_auth(scopes=("files",)), ticket_in_url=ticket_in_url
    )
    issued = await gateway.issue("files", owner="alice")
    async with serve(gateway) as client:
        # The owner's own token, valid and for this server, but without the scope.
        refused = await post(client, issued.upload_url, token="alice+noscope-token")
        assert refused.status_code == 403, refused.text
        assert refused.json() == {
            "status": "failed",
            "error": "insufficient_scope",
            "details": {"reason": "insufficientScope", "required": ["files"]},
        }
        assert refused.headers["www-authenticate"] == SCOPE_CHALLENGE
        page = await client.post(
            issued.upload_url,
            content=b"x",
            headers={
                "Authorization": "Bearer alice+noscope-token",
                "Accept": "text/html",
                "Content-Type": "multipart/form-data; boundary=x",
            },
        )
        assert page.status_code == 403
        assert page.headers["www-authenticate"] == SCOPE_CHALLENGE
        assert store.lookups == 0
        assert (await gateway.status(issued.record.id))["status"] == "issued"
        assert upstream.bodies == []
        accepted = await post(client, issued.upload_url, token="alice-token")
    assert accepted.status_code == 200, accepted.text
    assert upstream.bodies == [b"hello"]


@pytest.mark.parametrize("ticket_in_url", [True, False])
@pytest.mark.parametrize(
    ("token", "status", "code", "reason"),
    [
        (None, 401, "auth_required", "authRequired"),
        ("not a token", 401, "invalid_token", "invalidToken"),
        ("expired-token", 401, "invalid_token", "invalidToken"),
        # The resource is checked first, as the SDK does: its backend refuses the token
        # before the scope middleware sees it.
        ("alice@other-token", 401, "invalid_token", "wrongResource"),
        ("bob-token", 403, "forbidden", "ownerMismatch"),
    ],
)
async def test_other_refusals_are_unchanged_when_scopes_are_required(
    upstream: Upstream, ticket_in_url: bool, token: str | None, status: int, code: str, reason: str
) -> None:
    authenticate = subject_auth(OWN, scopes=("files",))
    gateway = build(upstream, authenticate=authenticate, ticket_in_url=ticket_in_url)
    issued = await gateway.issue("files", owner="alice")
    async with serve(gateway) as client:
        refused = await post(client, issued.upload_url, token=token)
    assert (refused.status_code, refused.json()["error"]) == (status, code), refused.text
    assert refused.json()["details"] == {"reason": reason}
    assert (await gateway.status(issued.record.id))["status"] == "issued"
    assert upstream.bodies == []


async def test_the_challenge_names_every_required_scope(upstream: Upstream) -> None:
    authenticate = subject_auth(scopes=("files", "upload", "files"))
    gateway = build(upstream, authenticate=authenticate)
    issued = await gateway.issue("files", owner="alice")
    async with serve(gateway) as client:
        refused = await post(client, issued.upload_url, token="alice-token")
    assert refused.status_code == 403
    assert refused.json()["details"] == {
        "reason": "insufficientScope",
        "required": ["files", "upload"],
    }
    assert refused.headers["www-authenticate"] == (
        'Bearer error="insufficient_scope", error_description="Required scope: upload", '
        'scope="files upload"'
    )


async def test_custom_authenticators_can_refuse_for_scope(upstream: Upstream) -> None:
    async def refuse(request: Request) -> str | None:
        raise InsufficientScope(["files:write"])

    async def old_style(request: Request) -> str | None:
        raise TokenRefused("wrongResource")

    for authenticate, status, code in (
        (refuse, 403, "insufficient_scope"),
        (old_style, 401, "invalid_token"),
    ):
        gateway = build(upstream, authenticate=authenticate)
        issued = await gateway.issue("files")
        async with serve(gateway) as client:
            response = await post(client, issued.upload_url, token="alice-token")
        assert (response.status_code, response.json()["error"]) == (status, code)
        assert (await gateway.status(issued.record.id))["status"] == "issued"
    assert isinstance(InsufficientScope(["a"]), TokenRefused)
    assert InsufficientScope(["a", "b"]).missing == ("a", "b")


def _challenge_error(response: httpx.Response) -> str | None:
    """The ``error`` attribute of a response's Bearer challenge."""
    import re

    found = re.search(r'error="([^"]*)"', response.headers.get("www-authenticate", ""))
    return found.group(1) if found else None


@needs_sdk_binding
@pytest.mark.parametrize("ticket_in_url", [True, False])
async def test_the_upload_route_answers_a_missing_scope_as_the_mcp_endpoint_does(
    upstream: Upstream, ticket_in_url: bool
) -> None:
    # One MCPServer, one verifier, one AuthSettings with a required scope. The same
    # token without that scope gets the same status and error code from the MCP
    # endpoint (the SDK's RequireAuthMiddleware) and from the upload route.
    from mcp.server.auth.provider import AccessToken
    from mcp.server.mcpserver import MCPServer

    from mcp_upload.adapters import mcp as adapter

    class SdkTokens:
        async def verify_token(self, token: str) -> AccessToken | None:
            found = await Verifier().verify_token(token)
            if found is None:
                return None
            return AccessToken(
                token=token,
                client_id="app",
                subject=found.subject,
                scopes=found.scopes,
                resource=found.resource,
            )

    settings = sdk_settings(True, scopes=["files"])
    verifier = SdkTokens()
    gateway = build(
        upstream,
        authenticate=adapter.authenticator(verifier, auth=settings),
        ticket_in_url=ticket_in_url,
    )
    server = MCPServer("scoped", auth=settings, token_verifier=verifier)
    adapter.attach(server, gateway)
    app = server.streamable_http_app(stateless_http=True, json_response=True)
    owner = token_principal(await verifier.verify_token("alice-token"))
    answers: dict[str, dict[str, tuple[int, str | None]]] = {"mcp": {}, "upload": {}}
    async with (
        server.session_manager.run(),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE) as client,
    ):
        for token in ("alice+noscope-token", "alice@other-token", "forged", "alice-token"):
            mcp = await client.post(
                "http://127.0.0.1:8000/mcp",
                json={"jsonrpc": "2.0", "id": 1, "method": "ping"},
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json, text/event-stream",
                },
            )
            mcp_error = mcp.json().get("error") if mcp.status_code != 200 else None
            answers["mcp"][token] = (mcp.status_code, mcp_error)
            assert mcp_error == _challenge_error(mcp)
            issued = await gateway.issue("files", owner=owner)
            upload = await post(client, issued.upload_url, token=token)
            upload_error = upload.json().get("error") if upload.status_code != 200 else None
            answers["upload"][token] = (upload.status_code, upload_error)
            if upload.status_code != 200:
                assert upload_error == _challenge_error(upload)
                assert (await gateway.status(issued.record.id))["status"] == "issued"
            if token == "alice+noscope-token":
                description = 'error_description="Required scope: files"'
                assert description in mcp.headers["www-authenticate"]
                assert description in upload.headers["www-authenticate"]
    assert answers["upload"] == answers["mcp"]
    assert answers["mcp"] == {
        "alice+noscope-token": (403, "insufficient_scope"),
        "alice@other-token": (401, "invalid_token"),
        "forged": (401, "invalid_token"),
        "alice-token": (200, None),
    }
    assert len(upstream.bodies) == 1


@pytest.mark.skipif(not HAS_FASTMCP, reason="fastmcp not installed")
@pytest.mark.parametrize("checks_in_verifier", [True, False])
async def test_fastmcp_answers_a_missing_scope_as_its_mcp_endpoint_does(
    upstream: Upstream, checks_in_verifier: bool
) -> None:
    # FastMCP 4's own verifiers refuse a token without a required scope in
    # verify_token, so its MCP endpoint answers 401 invalid_token, and the upload
    # route, calling the same verify_token, does too. A verifier that leaves scopes to
    # the middleware gets 403 insufficient_scope from both.
    from fastmcp import FastMCP
    from fastmcp.server.auth import AccessToken as FastToken
    from fastmcp.server.auth import TokenVerifier as FastVerifier
    from fastmcp.server.auth.providers.jwt import StaticTokenVerifier

    from mcp_upload.adapters import fastmcp as fast_adapter

    tokens: dict[str, dict[str, Any]] = {
        "alice-token": {"client_id": "app", "sub": "alice", "scopes": ["files"]},
        "alice+noscope-token": {"client_id": "app", "sub": "alice", "scopes": []},
    }

    class Lenient(FastVerifier):  # type: ignore[misc,unused-ignore]
        async def verify_token(self, token: str) -> FastToken | None:
            data = tokens.get(token)
            if data is None:
                return None
            return FastToken(
                token=token, client_id="app", scopes=data["scopes"], subject=data["sub"]
            )

    provider: Any = (
        StaticTokenVerifier(tokens=tokens, required_scopes=["files"])
        if checks_in_verifier
        else Lenient(required_scopes=["files"])
    )
    server = FastMCP("scoped", auth=provider)
    gateway = build(upstream, authenticate=fast_adapter.authenticator(server), ticket_in_url=False)
    fast_adapter.attach(server, gateway)
    app = server.http_app(stateless_http=True, json_response=True)
    owner = token_principal(await provider.verify_token("alice-token"))
    answers: dict[str, dict[str, tuple[int, str | None]]] = {"mcp": {}, "upload": {}}
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE) as client,
    ):
        for token in ("alice+noscope-token", "alice-token"):
            mcp = await client.post(
                f"{BASE}/mcp",
                json={"jsonrpc": "2.0", "id": 1, "method": "ping"},
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json, text/event-stream",
                },
            )
            answers["mcp"][token] = (mcp.status_code, _challenge_error(mcp))
            issued = await gateway.issue("files", owner=owner)
            upload = await post(client, issued.upload_url, token=token)
            answers["upload"][token] = (upload.status_code, _challenge_error(upload))
            if upload.status_code != 200:
                assert (await gateway.status(issued.record.id))["status"] == "issued"
    assert answers["upload"] == answers["mcp"]
    refusal = (401, "invalid_token") if checks_in_verifier else (403, "insufficient_scope")
    assert answers["mcp"] == {"alice+noscope-token": refusal, "alice-token": (200, None)}
    assert len(upstream.bodies) == 1


# ----- resume --------------------------------------------------------------------------


async def test_resume_rebuilds_the_same_link_only_for_the_right_ticket(
    upstream: Upstream,
) -> None:
    gateway = build(upstream)
    issued = await gateway.issue("files", owner="alice")
    again = await gateway.resume(issued.record.id, issued.secret, owner="alice")
    assert again is not None and again.upload_url == issued.upload_url
    assert await gateway.resume(issued.record.id, "wrong", owner="alice") is None
    assert await gateway.resume(issued.record.id, None, owner="alice") is None
    assert await gateway.resume(issued.record.id, issued.secret, owner="bob") is None
    assert await gateway.resume("up_nothing", issued.secret) is None

    bearer = build(upstream, authenticate=subject_auth(), ticket_in_url=False)
    owned = await bearer.issue("files", owner="alice")
    back = await bearer.resume(owned.record.id, owner="alice")
    assert back is not None and back.upload_url == owned.upload_url and back.secret == ""


# ----- the authenticator helpers -------------------------------------------------------


def request_with(headers: dict[str, str]) -> Request:
    raw = [(k.lower().encode(), v.encode()) for k, v in headers.items()]
    return Request({"type": "http", "method": "POST", "path": "/", "headers": raw})


def test_bearer_token_parsing() -> None:
    assert bearer_token(request_with({"Authorization": "Bearer abc"})) == "abc"
    assert bearer_token(request_with({"Authorization": "bearer  abc "})) == "abc"
    assert bearer_token(request_with({"Authorization": "Basic abc"})) is None
    assert bearer_token(request_with({"Authorization": "Bearer"})) is None
    assert bearer_token(request_with({})) is None


async def test_bearer_authenticator_checks_expiry_and_scopes() -> None:
    plain = bearer_authenticator(Verifier(), principal=by_subject)
    scoped = bearer_authenticator(Verifier(), principal=by_subject, required_scopes=["files"])
    assert await plain(request_with({"Authorization": "Bearer alice-token"})) == "alice"
    assert await plain(request_with({"Authorization": "Bearer expired-token"})) is None
    assert await plain(request_with({"Authorization": "Bearer noscope-token"})) == "noscope"
    with pytest.raises(InsufficientScope) as refused:
        await scoped(request_with({"Authorization": "Bearer noscope-token"}))
    assert (refused.value.reason, refused.value.required) == ("insufficientScope", ("files",))
    assert await scoped(request_with({"Authorization": "Bearer alice-token"})) == "alice"
    assert await plain(request_with({})) is None


def test_token_principal_tells_users_and_issuers_apart() -> None:
    alice = token_principal(Token("t", subject="alice"))
    bob = token_principal(Token("t", subject="bob"))
    other_issuer = token_principal(Token("t", subject="alice", claims={"iss": "https://x"}))
    from_claims = token_principal(Token("t", claims={"sub": "alice"}))
    no_subject = token_principal(Token("t"))
    assert alice == '["app",null,"alice"]'
    assert len({alice, bob, other_issuer, no_subject}) == 4
    assert from_claims == alice
    assert token_principal(object()) is None
