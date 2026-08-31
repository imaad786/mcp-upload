"""Adapter for the official MCP Python SDK (the ``mcp`` package, 2.x)."""

from __future__ import annotations

from ..gateway import UploadGateway
from . import HasCustomRoute


def attach(server: HasCustomRoute, gateway: UploadGateway) -> None:
    """Register the upload endpoint on an ``MCPServer``.

    The route is served by the same Starlette app the SDK returns from
    ``streamable_http_app()``. The SDK does not apply its authorization to custom
    routes, which is what this endpoint needs: a browser or a curl command has no
    bearer token, and the ticket in the URL is the credential.
    """
    for route in gateway.routes():
        server.custom_route(route.path, methods=sorted(route.methods or []))(route.endpoint)
