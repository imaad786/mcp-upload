"""Adapter for FastMCP (the ``fastmcp`` package, 4.x).

``attach`` registers the upload endpoint. ``ask_for_upload`` is the same multi-round
helper as the official SDK's adapter: FastMCP 4 gives a tool ``ctx.request_state`` and
``ctx.input_responses`` and passes a returned ``InputRequiredResult`` through.
``authenticator`` and ``current_principal`` tie the upload endpoint to the server's
auth provider.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from ..auth import Authenticator, Principal, bearer_authenticator, token_principal
from ..gateway import UploadGateway
from . import HasCustomRoute
from ._rounds import DEFAULT_MESSAGE, INPUT_KEY, ask_for_upload, target_of


def attach(server: HasCustomRoute, gateway: UploadGateway) -> None:
    """Register the upload endpoint on a FastMCP server.

    The route is served by the app returned from ``http_app()``. FastMCP's auth
    middleware only guards the MCP route, so this endpoint is reachable without a
    token. By default that is intended: the ticket in the URL is the credential. A
    gateway built with ``authenticate=authenticator(mcp)`` checks the token itself.
    """
    for route in gateway.routes():
        server.custom_route(route.path, methods=sorted(route.methods or []))(route.endpoint)


def authenticator(
    server_or_provider: Any,
    *,
    principal: Principal = token_principal,
    required_scopes: Iterable[str] | None = None,
    resource: str | None = None,
) -> Authenticator:
    """An upload authenticator that checks ``Authorization: Bearer`` with the server's
    own auth provider, so an upload needs the same token as a tool call.

    Pass the ``FastMCP`` server (its ``auth`` is used) or an ``AuthProvider``.
    ``required_scopes`` defaults to the provider's own, as on the MCP route.

    FastMCP 4's MCP route leaves the token's audience to the provider's
    ``verify_token`` (``JWTVerifier(audience=...)``, for example), which this
    authenticator calls too, so by default the upload route accepts the same tokens.
    ``resource`` adds the SDK's RFC 8707 check on top: a token whose ``resource`` is not
    that URL, or is missing, is refused with 401 ``invalid_token`` and the reason
    ``wrongResource``. The providers in FastMCP 4.0.10 do not set ``resource``, so pass it
    only with a provider that does.
    """
    provider = getattr(server_or_provider, "auth", server_or_provider)
    if provider is None or not hasattr(provider, "verify_token"):
        raise ValueError("the server has no auth provider to check upload tokens with")
    if required_scopes is None:
        required_scopes = getattr(provider, "required_scopes", None) or ()
    return bearer_authenticator(
        provider, principal=principal, required_scopes=required_scopes, resource=resource
    )


def current_principal(principal: Principal = token_principal) -> str | None:
    """The principal of the access token on the request being handled, or ``None``.
    Pass it as a ticket's ``owner`` so only the same user's token can upload to it."""
    from fastmcp.server.dependencies import get_access_token

    token = get_access_token()
    return None if token is None else principal(token)


__all__ = [
    "DEFAULT_MESSAGE",
    "INPUT_KEY",
    "ask_for_upload",
    "attach",
    "authenticator",
    "current_principal",
    "target_of",
]
