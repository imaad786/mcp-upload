"""Adapter for FastMCP (the ``fastmcp`` package, 3.x)."""

from __future__ import annotations

from ..gateway import UploadGateway
from . import HasCustomRoute


def attach(server: HasCustomRoute, gateway: UploadGateway) -> None:
    """Register the upload endpoint on a FastMCP server.

    The route is served by the app returned from ``http_app()``. FastMCP's auth
    middleware only guards the MCP route, so this endpoint is reachable without a
    token, which is intended: the ticket in the URL is the credential.
    """
    for route in gateway.routes():
        server.custom_route(route.path, methods=sorted(route.methods or []))(route.endpoint)
