"""Advertise the upload-ticket pattern as a named MCP extension, and optionally serve
SEP-2631's ``files/authorizeUpload``.

Without this, a client has no way to *ask* whether a server can take a file. It can
read a tool description in prose, but nothing machine-readable says "this server hands
out upload tickets". The 2026-07-28 protocol added an ``extensions`` capability for
exactly that, and SEP-2133 requires every identifier to carry a reverse-DNS prefix so
third parties can name things without colliding with the MCP organization or each
other.

Opt in when you build the server::

    from mcp.server.mcpserver import MCPServer
    from mcp_upload.adapters.mcp import attach
    from mcp_upload.adapters.mcp_extension import UploadTicketExtension

    server = MCPServer("files", extensions=[UploadTicketExtension()])
    attach(server, gateway)

Constructed with no arguments, the extension contributes no tools, resources or
methods, and intercepts nothing. It exists purely so the capability shows up under
``ServerCapabilities.extensions``, where a client that cares can find it.

Given a gateway and a destination, it also serves ``files/authorizeUpload``, so a
client can ask for an upload URL directly instead of through a tool::

    server = MCPServer(
        "files",
        extensions=[UploadTicketExtension(gateway, destination="reports", owner=user_of)],
    )

This module lives apart from ``adapters.mcp`` because it has to import from the SDK at
module scope in order to subclass ``Extension``. ``adapters.mcp`` imports the SDK only
inside functions and for type checking, so it stays importable without the SDK.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from mcp.server.extension import Extension, MethodBinding

from ..gateway import UploadGateway
from .sep2631 import IDENTIFIER as IDENTIFIER
from .sep2631 import OwnerResolver, binding_args, extension_settings, make_handler


class UploadTicketExtension(Extension):
    """Declares that this server issues short-lived single-use upload tickets, and
    with a ``gateway`` serves ``files/authorizeUpload`` from it.

    ``destination`` is the registered destination an authorized upload goes to. By
    default the request cannot name another. ``destinations`` lists the registered
    names a client may choose from instead, by naming one in the request's ``_meta``
    under ``IDENTIFIER`` as ``{"destination": name}``. Any other name is refused with
    -32602 and ``{"reason": "destinationNotAllowed", "allowed": [...]}``, and the list
    is advertised in the extension's settings. ``owner`` maps the request context to
    the user the ticket is bound to. Pass it on any server with more than one user, or
    anyone who learns a file URI can read its name, size and digest.
    """

    identifier = IDENTIFIER

    def __init__(
        self,
        gateway: UploadGateway | None = None,
        *,
        destination: str | None = None,
        owner: OwnerResolver | None = None,
        destinations: Sequence[str] = (),
    ) -> None:
        self._handler = make_handler(gateway, destination, owner, destinations)

    def settings(self) -> dict[str, Any]:
        """What the client sees at ``capabilities.extensions[IDENTIFIER]``. Lists
        ``files/authorizeUpload`` under ``methods`` when the server answers it."""
        return extension_settings(
            methods=self._handler is not None,
            destinations=self._handler.allowed if self._handler is not None else (),
        )

    def methods(self) -> Sequence[MethodBinding]:
        if self._handler is None:
            return ()
        return [MethodBinding(*binding_args(self._handler))]
