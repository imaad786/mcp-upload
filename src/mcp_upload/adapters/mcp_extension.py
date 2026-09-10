"""Advertise the upload-ticket pattern as a named MCP extension.

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

The extension contributes no tools, resources or methods, and intercepts nothing. It
exists purely so the capability shows up under ``ServerCapabilities.extensions``, where
a client that cares can find it. Uploads work exactly the same whether or not it is
declared, so this is discovery rather than a feature gate.

This module lives apart from ``adapters.mcp`` because it has to import from the SDK at
module scope in order to subclass ``Extension``, and the FastMCP lane pins ``mcp<2``,
where that module does not exist.
"""

from __future__ import annotations

from typing import Any

from mcp.server.extension import Extension

#: Reverse-DNS identifier for the pattern this library implements. The prefix is a
#: domain Imaad owns, per SEP-2133, and is not an MCP-organization namespace.
IDENTIFIER = "me.imaadkhan/upload-ticket"


class UploadTicketExtension(Extension):
    """Declares that this server issues short-lived single-use upload tickets."""

    identifier = IDENTIFIER

    def settings(self) -> dict[str, Any]:
        """What the client sees at ``capabilities.extensions[IDENTIFIER]``.

        Deliberately small. Anything a client needs in order to perform a particular
        upload already travels in that upload's own ``FileTransferDescriptor``, so
        repeating it here would be a second copy able to drift from the first.
        """
        return {
            "version": "1",
            "transport": "multipart-form-data",
            "descriptor": "FileTransferDescriptor",
        }
