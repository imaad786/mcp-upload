"""Adapter for the official MCP Python SDK (the ``mcp`` package, 2.x).

Two things live here. ``attach`` registers the upload endpoint on an ``MCPServer``.
``ask_for_upload`` drives the multi-round-trip flow the 2026-07-28 protocol uses in
place of server-initiated requests, so a tool can hand the user to the upload page
through URL-mode elicitation instead of printing a link in prose.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any

from ..gateway import UploadGateway
from ..types import UploadStatus
from . import HasCustomRoute

if TYPE_CHECKING:
    from mcp.server.mcpserver import Context
    from mcp_types import InputRequiredResult

# The key under which the elicitation travels in inputRequests and comes back in
# inputResponses. Any stable string works; the client echoes it.
INPUT_KEY = "upload"

DEFAULT_MESSAGE = "A file is needed. Open the link to upload it."


def attach(server: HasCustomRoute, gateway: UploadGateway) -> None:
    """Register the upload endpoint on an ``MCPServer``.

    The route is served by the same Starlette app the SDK returns from
    ``streamable_http_app()``. The SDK does not apply its authorization to custom
    routes, which is what this endpoint needs: a browser or a curl command has no
    bearer token, and the ticket in the URL is the credential.
    """
    for route in gateway.routes():
        server.custom_route(route.path, methods=sorted(route.methods or []))(route.endpoint)


async def ask_for_upload(
    ctx: Context[Any, Any],
    gateway: UploadGateway,
    destination: str,
    *,
    message: str = DEFAULT_MESSAGE,
    caller: str | None = None,
    ttl: timedelta | None = None,
    max_size: int | None = None,
    accept: tuple[str, ...] | None = None,
) -> InputRequiredResult | UploadStatus:
    """Ask the user for a file through URL-mode elicitation, in two rounds.

    Since protocol 2026-07-28 a server cannot open a request to the client in the
    middle of a call; there is no channel for it. Instead the tool returns an
    ``InputRequiredResult`` naming what it needs, the client collects it and retries
    the same call with the answers attached, and the tool body runs again from the
    top. This helper hides that. On the first round it mints a ticket and asks the
    client to send the user to the upload page. On the retry it reads the answer and
    reports the record's status. The record id rides in ``request_state``, which the
    client echoes back, so the retry does not mint a second ticket.

    ``request_state`` comes back from the client and is not trusted for anything but
    a lookup. The status of a record is not secret; only the ticket is, and the
    ticket is never in the state.

    Annotate the tool as returning ``UploadStatus | InputRequiredResult``. The SDK
    derives the output schema from the status half and passes the other through.
    """
    from mcp_types import ElicitRequest, ElicitRequestURLParams, InputRequiredResult

    state = ctx.request_state
    if not isinstance(state, str) or not state.startswith("up_"):
        issued = await gateway.issue(
            destination, caller=caller, ttl=ttl, max_size=max_size, accept=accept
        )
        params = ElicitRequestURLParams(
            mode="url",
            message=message,
            url=issued.upload_url,
            elicitation_id=issued.record.id,
        )
        return InputRequiredResult(
            input_requests={INPUT_KEY: ElicitRequest(params=params)},
            request_state=issued.record.id,
        )

    status = await gateway.status(state)
    answer = (ctx.input_responses or {}).get(INPUT_KEY)
    action = getattr(answer, "action", None)
    if status["status"] == "issued" and action in ("decline", "cancel"):
        # The user did not open the link. The ticket stays valid until it expires,
        # which is harmless: nobody has it but the client that just refused it.
        status["status"] = "declined" if action == "decline" else "cancelled"
    return status
