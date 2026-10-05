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
from typing import Any, Literal

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
    with ``authenticate=authenticator(verifier, auth=settings)`` checks the token
    itself, as the MCP endpoint does.
    """
    for route in gateway.routes():
        server.custom_route(route.path, methods=sorted(route.methods or []))(route.endpoint)


def authenticator(
    verifier: TokenVerifier,
    *,
    principal: Principal = token_principal,
    required_scopes: Iterable[str] | None = None,
    auth: Any = None,
    resource: str | Literal[False] | None = None,
) -> Authenticator:
    """An upload authenticator that checks ``Authorization: Bearer`` with the same
    ``TokenVerifier`` the server was built with, so an upload needs the same token as
    a tool call. Pass the result as ``UploadGateway(authenticate=...)``.

    ``auth`` is the server's ``AuthSettings``, the object given to
    ``MCPServer(auth=...)``. Pass it so the upload route checks a token exactly as the
    MCP endpoint does and is never looser: ``required_scopes`` defaults to its
    ``required_scopes``, and when it sets ``validate_token_resource=True`` a token
    issued for any resource but its ``resource_server_url`` is refused with 401
    ``invalid_token`` and the reason ``wrongResource``, before the ticket is spent. A
    valid token without a required scope gets 403 ``insufficient_scope``, as on the MCP
    endpoint. Without ``auth`` no resource is checked and no scope is required, as
    before.

    ``resource`` overrides the binding: a URL binds tokens to that resource whatever
    ``auth`` says, and ``False`` turns the check off. On ``mcp`` 2.1, whose settings
    have no ``validate_token_resource`` and whose tokens no ``resource``, nothing is
    bound by default, and an explicit ``resource`` refuses every token whose verifier
    does not report one.

    ``principal`` must be the function ``current_principal`` uses when tickets are
    issued, which is the default for both."""
    if required_scopes is None:
        required_scopes = getattr(auth, "required_scopes", None) or ()
    bound: str | None = None
    if resource is None:
        if auth is not None and getattr(auth, "validate_token_resource", None):
            server_url = getattr(auth, "resource_server_url", None)
            if server_url is None:
                raise ValueError("validate_token_resource needs resource_server_url")
            bound = str(server_url)
    elif resource is not False:
        bound = resource
    return bearer_authenticator(
        verifier, principal=principal, required_scopes=required_scopes, resource=bound
    )


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
