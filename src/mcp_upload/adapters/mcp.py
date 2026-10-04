"""Adapter for the official MCP Python SDK (the ``mcp`` package, 2.x).

``attach`` registers the upload endpoint on an ``MCPServer``. ``ask_for_upload``
drives the multi-round-trip flow the 2026-07-28 protocol uses in place of
server-initiated requests, so a tool can hand the user to the upload page through
URL-mode elicitation, or a harness the upload target, instead of printing a link in
prose. ``authenticator`` and ``current_principal`` let the upload endpoint require the
same bearer token as the server's MCP requests.
"""

from __future__ import annotations

from collections.abc import Iterable

from ..auth import Authenticator, Principal, TokenVerifier, bearer_authenticator, token_principal
from ..gateway import UploadGateway
from . import HasCustomRoute
from ._rounds import DEFAULT_MESSAGE, INPUT_KEY, ask_for_upload, target_of


def attach(server: HasCustomRoute, gateway: UploadGateway) -> None:
    """Register the upload endpoint on an ``MCPServer``.

    The route is served by the same Starlette app the SDK returns from
    ``streamable_http_app()``. The SDK does not apply its authorization to custom
    routes. By default that is what this endpoint needs: a browser or a curl command
    has no bearer token, and the ticket in the URL is the credential. A gateway built
    with ``authenticate=authenticator(verifier)`` checks the token itself.
    """
    for route in gateway.routes():
        server.custom_route(route.path, methods=sorted(route.methods or []))(route.endpoint)


def authenticator(
    verifier: TokenVerifier,
    *,
    principal: Principal = token_principal,
    required_scopes: Iterable[str] = (),
) -> Authenticator:
    """An upload authenticator that checks ``Authorization: Bearer`` with the same
    ``TokenVerifier`` the server was built with, so an upload needs the same token as
    a tool call. Pass the result as ``UploadGateway(authenticate=...)``.

    ``principal`` must be the function ``current_principal`` uses when tickets are
    issued, which is the default for both."""
    return bearer_authenticator(verifier, principal=principal, required_scopes=required_scopes)


def current_principal(principal: Principal = token_principal) -> str | None:
    """The principal of the access token on the request being handled, or ``None``
    without one. Pass it as the ``owner`` of a ticket so that only the same user's
    token can upload to it, for example ``ask_for_upload(..., owner=current_principal())``.

    Reads the SDK's auth context, which is set inside tool and method handlers of a
    server built with a ``token_verifier``."""
    from mcp.server.auth.middleware.auth_context import get_access_token

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
