"""Bearer-token authentication on the upload endpoint, in all three modes.

The default mode (the ticket alone) must behave exactly as before. With an
authenticator, every refusal for a missing, invalid or other user's token must happen
before the ticket is spent, and must leave the backend untouched. With the token as
the only credential, single use must still hold under concurrency in every store.
"""

from __future__ import annotations

import asyncio
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
from mcp_upload.auth import bearer_authenticator, bearer_token, token_principal
from mcp_upload.tickets import utcnow
from mcp_upload.types import EXTENSION_ID
from tests.conftest import Upstream, multipart

BASE = "http://gateway.test"

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


class Verifier:
    """Accepts ``<user>-token`` and refuses anything else. ``expired-token`` verifies
    but is past its expiry, and ``noscope-token`` carries no scopes."""

    def __init__(self) -> None:
        self.calls = 0

    async def verify_token(self, token: str) -> Token | None:
        self.calls += 1
        if not token.endswith("-token"):
            return None
        user = token.removesuffix("-token")
        if user == "expired":
            return Token(token, subject=user, expires_at=int(time.time()) - 10)
        if user == "noscope":
            return Token(token, subject=user, scopes=[])
        return Token(token, subject=user)


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


def subject_auth() -> Authenticate:
    return bearer_authenticator(Verifier(), principal=by_subject)


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
    assert await scoped(request_with({"Authorization": "Bearer noscope-token"})) is None
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
