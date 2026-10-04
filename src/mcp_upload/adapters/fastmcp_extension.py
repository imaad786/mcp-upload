"""The upload-ticket extension for FastMCP 4, with SEP-2631's ``files/authorizeUpload``.

FastMCP has its own extension base class, ``ServerExtension``, registered with
``FastMCP.add_extension``. This is the same extension as
``mcp_extension.UploadTicketExtension``, built on that class::

    from fastmcp import FastMCP
    from mcp_upload.adapters.fastmcp import attach
    from mcp_upload.adapters.fastmcp_extension import UploadTicketExtension

    mcp = FastMCP("files")
    mcp.add_extension(UploadTicketExtension(gateway, destination="reports", owner=user_of))
    attach(mcp, gateway)

FastMCP binds its request context before the handler runs, so an ``owner`` resolver
can call ``fastmcp.server.dependencies.get_access_token()``.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from fastmcp.server.extensions import MethodBinding, ServerExtension

from ..gateway import UploadGateway
from .sep2631 import IDENTIFIER as IDENTIFIER
from .sep2631 import OwnerResolver, binding_args, extension_settings, make_handler


# Where fastmcp is absent, type checking sees ServerExtension as Any.
class UploadTicketExtension(ServerExtension):  # type: ignore[misc,unused-ignore]
    """See ``mcp_extension.UploadTicketExtension``; the arguments are the same."""

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
        return extension_settings(
            methods=self._handler is not None,
            destinations=self._handler.allowed if self._handler is not None else (),
        )

    def methods(self) -> Sequence[MethodBinding]:
        if self._handler is None:
            return ()
        return [MethodBinding(*binding_args(self._handler))]
