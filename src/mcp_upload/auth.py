"""Bearer-token authentication on the upload endpoint.

By default the ticket in the URL is the only credential, which is what lets a person
upload from a browser and an agent upload with ``curl``. Some deployments cannot
accept a credential in a URL at all, and want every request to the server, uploads
included, to carry the same bearer token as its MCP requests. A gateway built with an
authenticator does that::

    gateway = UploadGateway(..., authenticate=bearer_authenticator(verifier))

An authenticator is any coroutine function that takes the upload request and returns
the principal it proves, or ``None`` when there is no usable credential. The gateway
compares that principal with the record's ``owner``. ``bearer_authenticator`` builds
one from a token verifier, an object with ``async verify_token(token)`` that returns
an access token or ``None``. The official SDK's ``TokenVerifier`` and every FastMCP 4
auth provider have that method, so the gateway can check uploads with exactly the
verifier the server already uses for MCP requests.

The principal has to be computed the same way when a ticket is issued (from the token
of the tool call) and when the upload arrives (from the token on the upload request),
or no upload would ever match its owner. ``token_principal`` is that one computation.
The adapters' ``current_principal()`` applies it to the tool call's token.

A token can also be bound to the server it was issued for (RFC 8707). With
``resource`` set, ``bearer_authenticator`` refuses a token whose ``resource`` is not
that URL, compared as the official SDK compares it for
``AuthSettings.validate_token_resource``, so a token minted for another service cannot
upload here even though the shared verifier accepts it.

Nothing here imports an MCP framework. Tokens are read by duck typing. Resource binding
parses URLs with pydantic, which both supported frameworks depend on.
"""

from __future__ import annotations

import json
import time
from collections.abc import Awaitable, Callable, Iterable
from typing import Any, Protocol

from starlette.requests import Request

#: Maps an upload request to the principal it authenticates, or ``None`` when it
#: carries no valid credential.
Authenticator = Callable[[Request], Awaitable["str | None"]]

#: Maps a verified access token to a principal string.
Principal = Callable[[Any], "str | None"]


class TokenRefused(Exception):
    """Raised by an authenticator for a token it verified but will not accept. The
    gateway answers 401 ``invalid_token`` with ``details: {"reason": reason}``, before
    the ticket is looked up or spent. Returning ``None`` gives the same status with the
    reason ``invalidToken``. The subclass ``InsufficientScope`` gets 403 instead."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class InsufficientScope(TokenRefused):
    """Raised by an authenticator for a verified token that lacks a scope the server
    requires. The gateway answers 403 ``insufficient_scope``, as the official SDK's MCP
    endpoint does (RFC 6750 section 3.1), with
    ``details: {"reason": "insufficientScope", "required": [...]}`` and a
    ``WWW-Authenticate`` challenge naming the scopes, before the ticket is looked up or
    spent. ``missing`` defaults to ``required``."""

    def __init__(self, required: Iterable[str], missing: Iterable[str] = ()):
        super().__init__("insufficientScope")
        self.required = tuple(required)
        self.missing = tuple(missing) or self.required


class TokenVerifier(Protocol):
    """Anything with the official SDK's ``verify_token``, FastMCP's auth providers
    included."""

    async def verify_token(self, token: str) -> Any: ...


def bearer_token(request: Request) -> str | None:
    """The token in an ``Authorization: Bearer`` header, or ``None``. The scheme is
    matched without regard to case, as RFC 7235 requires."""
    header = request.headers.get("authorization")
    if header is None:
        return None
    scheme, _, token = header.strip().partition(" ")
    token = token.strip()
    if scheme.lower() != "bearer" or not token:
        return None
    return token


def token_principal(token: Any) -> str | None:
    """The principal an access token stands for: its client id, issuer and subject,
    the same three parts the official SDK uses to tell principals apart, as a compact
    JSON array. Two users of one OAuth client are distinct whenever the verifier sets
    a subject. When it does not, every user of that client is one principal, so pass
    your own ``principal`` function in that case, for example one that reads a user id
    claim."""
    client_id = getattr(token, "client_id", None)
    if not isinstance(client_id, str) or not client_id:
        return None
    claims = getattr(token, "claims", None)
    issuer = claims.get("iss") if isinstance(claims, dict) else None
    subject = getattr(token, "subject", None)
    if subject is None and isinstance(claims, dict):
        subject = claims.get("sub")
    parts = [client_id, None if issuer is None else str(issuer)]
    parts.append(None if subject is None else str(subject))
    return json.dumps(parts, separators=(",", ":"))


def canonical_resource(url: Any) -> str:
    """``url`` as the official SDK compares resources: parsed as an HTTP(S) URL, so the
    case of the scheme and host and a default port do not matter, without a trailing
    slash. Raises ``ValueError`` for anything that is not an HTTP(S) URL."""
    from pydantic import AnyHttpUrl, ValidationError

    try:
        return str(AnyHttpUrl(str(url))).removesuffix("/")
    except ValidationError as exc:
        raise ValueError(f"not an HTTP(S) URL: {url!r}") from exc


def issued_for(token: Any, resource: str) -> bool:
    """Whether ``token.resource`` names ``resource`` (a ``canonical_resource``). A token
    without a ``resource`` was not issued for this server, as in the SDK."""
    value = getattr(token, "resource", None)
    if not isinstance(value, str) or not value:
        return False
    try:
        return canonical_resource(value) == resource
    except ValueError:
        return False


def bearer_authenticator(
    verifier: TokenVerifier,
    *,
    principal: Principal = token_principal,
    required_scopes: Iterable[str] = (),
    resource: str | None = None,
) -> Authenticator:
    """An authenticator that reads ``Authorization: Bearer <token>``, checks the token
    with ``verifier``, and returns ``principal(token)``.

    A token the verifier refuses and one past its ``expires_at`` authenticate nobody.
    One without every scope in ``required_scopes`` raises ``InsufficientScope``, which
    the gateway answers with 403 ``insufficient_scope``, as the SDK's MCP endpoint
    answers it. Pass the scopes your MCP endpoint requires, so a token that could not
    call a tool cannot upload either.

    ``resource`` is the server's canonical URL, its ``resource_server_url``. With it,
    a token whose ``resource`` (the RFC 8707 resource indicator the verifier reports)
    is another URL, or is missing, raises ``TokenRefused("wrongResource")``. This is the
    check the SDK's ``AuthSettings.validate_token_resource`` makes on the MCP endpoint;
    the official SDK's adapter turns it on when the server's settings do. Tokens from
    ``mcp`` 2.1 carry no ``resource``, so with binding requested every one of them is
    refused.
    """
    scopes = tuple(dict.fromkeys(required_scopes))
    bound = None if resource is None else canonical_resource(resource)

    async def authenticate(request: Request) -> str | None:
        token = bearer_token(request)
        if token is None:
            return None
        verified = await verifier.verify_token(token)
        if verified is None:
            return None
        expires_at = getattr(verified, "expires_at", None)
        if isinstance(expires_at, int | float) and expires_at < time.time():
            return None
        if bound is not None and not issued_for(verified, bound):
            raise TokenRefused("wrongResource")
        if scopes:
            held = set(getattr(verified, "scopes", None) or ())
            if missing := [scope for scope in scopes if scope not in held]:
                raise InsufficientScope(scopes, missing)
        return principal(verified)

    return authenticate


__all__ = [
    "Authenticator",
    "InsufficientScope",
    "Principal",
    "TokenRefused",
    "TokenVerifier",
    "bearer_authenticator",
    "bearer_token",
    "canonical_resource",
    "issued_for",
    "token_principal",
]
