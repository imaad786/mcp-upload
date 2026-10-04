"""An MCP server for the live scenarios that need one, run as its own process.

It serves the official SDK's ``MCPServer`` over Streamable HTTP with the upload endpoint
attached, and guards the MCP route with a bearer token verifier, so tool calls and
uploads can be checked against the same token. Tokens are the stress gateway's
(``tok.<user>.<mac>``), and the principal is the library's default, built from the
token's client id and subject.

- ``fetch_report(size, digest)`` asks for a file with ``ask_for_upload``, declaring the
  size and digest first, and binds the ticket to the caller's principal.
- The upload-ticket extension serves ``files/authorizeUpload`` with ``files`` as the
  default destination and ``archive`` offered as a choice. ``internal`` is registered
  but never offered. Each destination writes to the stress backend under its own key
  prefix, so the harness can see where every upload landed.

``--auth`` picks what an upload needs, as in ``gateway.py``. ``GET /_caps`` says which
of these the library under test supports. On a version without them the server still
starts and reports ``false``, so the harness can mark the scenario unsupported.

    python stress/mcp_server.py --port 8000 --backend http://127.0.0.1:8001 --auth bearer
"""

# No ``from __future__ import annotations``: the SDK evaluates a tool's annotations
# against the module's globals, and the tool here is defined inside a function.
import argparse
import inspect
import logging
from pathlib import Path
from typing import Any

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

HERE = Path(__file__).resolve().parent


def capabilities() -> dict[str, bool]:
    caps = {"ask_target": False, "authenticate": False, "destinations": False}
    try:
        from mcp_upload import UploadGateway
        from mcp_upload.adapters import mcp as adapter
        from mcp_upload.adapters.mcp_extension import UploadTicketExtension
    except ImportError:
        return caps
    gateway_params = inspect.signature(UploadGateway.__init__).parameters
    caps["authenticate"] = "authenticate" in gateway_params and hasattr(
        adapter, "current_principal"
    )
    caps["ask_target"] = "expected_digest" in inspect.signature(adapter.ask_for_upload).parameters
    caps["destinations"] = (
        "destinations" in inspect.signature(UploadTicketExtension.__init__).parameters
    )
    return caps


def build(args: argparse.Namespace) -> Starlette:
    caps = capabilities()

    async def report(request: Request) -> JSONResponse:
        return JSONResponse({**caps, "auth_mode": args.auth})

    if not all(caps.values()):
        return Starlette(routes=[Route("/_caps", report)])

    import sys

    sys.path.insert(0, str(HERE))
    from gateway import token_for  # the stress gateway's tokens
    from mcp.server.auth.provider import AccessToken
    from mcp.server.auth.settings import AuthSettings
    from mcp.server.mcpserver import Context, MCPServer
    from mcp_types import InputRequiredResult
    from pydantic import AnyHttpUrl

    from mcp_upload import Destination, MemoryStore, Registry, UploadGateway
    from mcp_upload.adapters import mcp as adapter
    from mcp_upload.adapters.mcp_extension import UploadTicketExtension
    from mcp_upload.types import UploadStatus

    class Tokens:
        """The MCP route's verifier: the same tokens the upload endpoint takes."""

        async def verify_token(self, token: str) -> AccessToken | None:
            parts = token.split(".")
            if len(parts) != 3 or token != token_for(parts[1]):
                return None
            return AccessToken(token=token, client_id="harness", subject=parts[1], scopes=[])

    verifier = Tokens()
    options: dict[str, Any] = {}
    if args.auth != "ticket":
        # The library's own helper, around the same verifier the MCP route uses.
        options["authenticate"] = adapter.authenticator(verifier)
        options["ticket_in_url"] = args.auth == "ticket_bearer"
    origin = f"http://127.0.0.1:{args.port}"
    gateway = UploadGateway(
        base_url=origin,
        registry=Registry(
            Destination(name="files", url=f"{args.backend}/files/{{id}}", max_size=args.max_size),
            Destination(
                name="archive", url=f"{args.backend}/files/archive-{{id}}", max_size=args.max_size
            ),
            Destination(name="internal", url=f"{args.backend}/files/internal-{{id}}"),
        ),
        store=MemoryStore(max_records=1_000_000),
        server_name="stress",
        max_in_flight=512,
        **options,
    )

    def owner_of(ctx: Any) -> str | None:
        return adapter.current_principal()

    server = MCPServer(
        "stress-mcp",
        auth=AuthSettings(
            issuer_url=AnyHttpUrl("http://auth.invalid"),
            resource_server_url=AnyHttpUrl(f"{origin}/mcp"),
        ),
        token_verifier=verifier,
        extensions=[
            UploadTicketExtension(
                gateway, destination="files", destinations=["archive"], owner=owner_of
            )
        ],
    )

    async def fetch_report(
        ctx: Context[Any, Any], size: int | None = None, digest: str | None = None
    ) -> UploadStatus | InputRequiredResult:
        """Ask for the report file. Declares its size and SHA-256 when given."""
        return await adapter.ask_for_upload(
            ctx,
            gateway,
            "files",
            owner=adapter.current_principal(),
            expected_size=size,
            expected_digest=digest,
        )

    server.tool()(fetch_report)
    adapter.attach(server, gateway)
    server.custom_route("/_caps", methods=["GET"])(report)
    return server.streamable_http_app()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--backend", required=True)
    parser.add_argument("--auth", choices=["ticket", "ticket_bearer", "bearer"], default="bearer")
    parser.add_argument("--max-size", type=int, default=8 << 20)
    args = parser.parse_args()
    # Refused uploads are expected here, and each one is logged.
    logging.disable(logging.ERROR)
    uvicorn.run(build(args), host="127.0.0.1", port=args.port, log_level="warning", backlog=4096)


if __name__ == "__main__":
    main()
