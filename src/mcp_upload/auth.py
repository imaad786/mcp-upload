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

Nothing here imports an MCP framework. Tokens are read by duck typing.
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


def bearer_authenticator(
    verifier: TokenVerifier,
    *,
    principal: Principal = token_principal,
    required_scopes: Iterable[str] = (),
) -> Authenticator:
    """An authenticator that reads ``Authorization: Bearer <token>``, checks the token
    with ``verifier``, and returns ``principal(token)``.

    A token the verifier refuses, one past its ``expires_at``, and one without every
    scope in ``required_scopes`` all authenticate nobody. Pass the scopes your MCP
    endpoint requires, so a token that could not call a tool cannot upload either.
    """
    scopes = frozenset(required_scopes)

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
        if scopes and not scopes <= set(getattr(verified, "scopes", None) or ()):
            return None
        return principal(verified)

    return authenticate


__all__ = [
    "Authenticator",
    "Principal",
    "TokenVerifier",
    "bearer_authenticator",
    "bearer_token",
    "token_principal",
]
