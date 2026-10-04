"""SEP-2631's ``files/authorizeUpload`` request, shared by both server adapters.

SEP-2631 proposes a request a client sends before it has the file in a tool call: "I
am about to upload this name, type, size and digest; where do I send it?" The server
answers with a ``FileValue`` naming the file to be and a ``FileTransferDescriptor``
saying how to send the bytes. The client posts them, then passes the file's URI to a
tool as an ordinary string argument.

That maps onto the gateway one to one. The request becomes ``UploadGateway.issue``
with the declared size and digest, the answer is ``describe`` with the name and type
added, and the URI is ``gateway.uri(id)``. This module holds the parts both
frameworks share: the wire models, the handler, and the error mapping. The framework
classes that register it live in ``mcp_extension`` and ``fastmcp_extension``.

Errors follow the proposal. A declared size, type or digest the destination will
never take is ``-32602`` with machine-readable ``data``, such as
``{"reason": "maxSizeExceeded", "maxSize": 1000, "actualSize": 5000}``, so a client
can tell the user why without parsing a message. A full ticket store is ``-32603``.
A server without the method answers ``-32601``, which the SDK does on its own.

The proposal's request names no destination, and every upload goes to the one the
server configured. A server may also let clients choose from a closed list of
destination names it declares. A client names one in the request's ``_meta``, under
the extension's identifier, since the proposal's params have no such field::

    {"name": "q3.pdf", "_meta": {"me.imaadkhan/upload-ticket": {"destination": "archive"}}}

Any other name is ``-32602`` with ``{"reason": "destinationNotAllowed", "allowed":
[...]}``. A name is all a client can send. It never becomes a URL, host or path.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any, Literal

from mcp.shared.exceptions import MCPError
from mcp_types import INTERNAL_ERROR, INVALID_PARAMS, Request, RequestParams, Result
from pydantic import BaseModel, ConfigDict

from ..destinations import Destination, UnknownDestination
from ..gateway import UploadGateway
from ..multipart import split_media_type
from ..store import StoreFull
from ..tickets import Constraints, sha256_hex
from ..types import EXTENSION_ID, FileDigest, FileValue

if TYPE_CHECKING:
    from mcp.server.context import ServerRequestContext

#: Reverse-DNS identifier for the pattern this library implements. The prefix is a
#: domain Imaad owns, per SEP-2133, and is not an MCP-organization namespace.
IDENTIFIER = EXTENSION_ID

METHOD = "files/authorizeUpload"

#: Maps the request context to the owner the ticket is bound to, typically the
#: authenticated user. May be a plain function or a coroutine function.
OwnerResolver = Callable[["ServerRequestContext[Any, Any]"], "str | None | Awaitable[str | None]"]


class Digest(BaseModel):
    """``value`` is base64url without padding. Only ``sha-256`` is accepted."""

    model_config = ConfigDict(extra="ignore")

    algorithm: str
    value: str


class AuthorizeUploadParams(RequestParams):
    """Every field is optional, as in the proposal. Declaring a size and digest lets
    the gateway refuse bytes that do not match before the backend commits them."""

    name: str | None = None
    mime_type: str | None = None
    size: int | None = None
    digest: Digest | None = None


class AuthorizeUploadRequest(Request[AuthorizeUploadParams, Literal["files/authorizeUpload"]]):
    method: Literal["files/authorizeUpload"] = "files/authorizeUpload"


class AuthorizeUploadResult(Result):
    file: dict[str, Any]
    upload: dict[str, Any]


def extension_settings(methods: bool, destinations: Sequence[str] = ()) -> dict[str, Any]:
    """What the client sees at ``capabilities.extensions[IDENTIFIER]``.

    Deliberately small. Anything a client needs in order to perform a particular
    upload already travels in that upload's own ``FileTransferDescriptor``, so
    repeating it here would be a second copy able to drift from the first. The one
    addition is the list of destination names a client may choose from, when the
    server offers a choice, since a client cannot learn it any other way.
    """
    settings: dict[str, Any] = {
        "version": "1",
        "transport": "multipart-form-data",
        "descriptor": "FileTransferDescriptor",
    }
    if methods:
        settings["methods"] = [METHOD]
    if destinations:
        settings["destinations"] = list(destinations)
    return settings


def invalid(message: str, **data: Any) -> MCPError:
    return MCPError(code=INVALID_PARAMS, message=message, data=data)


class AuthorizeUpload:
    """The ``files/authorizeUpload`` handler: validate, issue, describe.

    ``destination`` names the registered destination a ticket goes to when the
    request names none. ``destinations`` lists the names a request may choose
    instead, in its ``_meta``. Empty, the default, means no choice: the request cannot
    pick a destination. ``owner`` resolves the request to the user the ticket is bound
    to, so only that user's ``status`` and ``claim`` see the record.
    """

    def __init__(
        self,
        gateway: UploadGateway,
        destination: str,
        *,
        owner: OwnerResolver | None = None,
        destinations: Sequence[str] = (),
    ) -> None:
        if isinstance(destinations, str):
            raise TypeError("destinations is a sequence of names, not one name")
        self._gateway = gateway
        self._destination = destination
        self._owner = owner
        #: The names a request may pick, the default first. Fixed at startup.
        self.allowed: tuple[str, ...] = (
            tuple(dict.fromkeys((destination, *destinations))) if destinations else ()
        )
        # Fail at startup rather than on the first request.
        for name in (destination, *destinations):
            destination_of(gateway, name)

    async def __call__(
        self, ctx: ServerRequestContext[Any, Any], params: AuthorizeUploadParams
    ) -> dict[str, Any]:
        name = self._chosen(params)
        try:
            dest = destination_of(self._gateway, name)
        except UnknownDestination:
            raise MCPError(
                code=INTERNAL_ERROR, message="upload destination is not configured"
            ) from None
        mime_type = check(params, dest)
        owner = await self._resolve_owner(ctx)
        if owner is None and not self._gateway.ticket_in_url:
            # Only the owner's token can upload on such a gateway, so a ticket without
            # one could never be used. The server is missing an owner resolver, or the
            # request reached it unauthenticated.
            raise MCPError(
                code=INTERNAL_ERROR,
                message="uploads here need an authenticated user",
                data={"reason": "ownerRequired"},
            )
        digest: FileDigest | None = None
        if params.digest is not None:
            digest = {"algorithm": params.digest.algorithm, "value": params.digest.value}
        try:
            issued = await self._gateway.issue(
                dest.name,
                caller=METHOD,
                owner=owner,
                # The ticket matches on type/subtype only, as an upload's is.
                accept=(mime_type.split(";", 1)[0].strip().lower(),) if mime_type else None,
                expected_size=params.size,
                expected_digest=digest,
            )
        except StoreFull:
            raise MCPError(
                code=INTERNAL_ERROR,
                message="too many uploads are pending, try again later",
                data={"reason": "storeFull"},
            ) from None
        except ValueError as exc:
            # check() covers what a client can get wrong. This is a backstop for a
            # limit the gateway enforces that check() does not know about.
            raise invalid(str(exc), reason="invalidParams") from None
        described = self._gateway.describe(issued)
        file: FileValue = described["file"]
        if params.name is not None:
            file["name"] = params.name
        if mime_type is not None:
            file["mimeType"] = mime_type
        return {"file": dict(file), "upload": dict(described["upload"])}

    def _chosen(self, params: AuthorizeUploadParams) -> str:
        """The destination the request names in its ``_meta``, if it may, else the
        default. Only a name from the declared list gets through."""
        meta = params.meta or {}
        entry = meta.get(IDENTIFIER) if isinstance(meta, dict) else None
        if not isinstance(entry, dict) or "destination" not in entry:
            return self._destination
        requested = entry["destination"]
        allowed = list(self.allowed or (self._destination,))
        if not isinstance(requested, str) or requested not in allowed:
            raise invalid(
                "that destination is not offered here",
                reason="destinationNotAllowed",
                allowed=allowed,
            )
        return requested

    async def _resolve_owner(self, ctx: ServerRequestContext[Any, Any]) -> str | None:
        if self._owner is None:
            return None
        owner = self._owner(ctx)
        if inspect.isawaitable(owner):
            owner = await owner
        return owner


def check(params: AuthorizeUploadParams, dest: Destination) -> str | None:
    """Refuse a declaration the destination will never accept, with the reason as
    data. Returns the declared media type as declared, parameters included, or
    ``None``. It is echoed in the result, where a client may compare it exactly with
    what it sent."""
    if params.digest is not None:
        algorithm = params.digest.algorithm.lower()
        if algorithm != "sha-256":
            raise invalid(
                "only sha-256 digests are supported",
                reason="invalidDigest",
                algorithm=params.digest.algorithm,
                supported=["sha-256"],
            )
        try:
            sha256_hex(params.digest.value)
        except ValueError as exc:
            raise invalid(str(exc), reason="invalidDigest") from None
    if params.size is not None:
        if params.size < 0:
            raise invalid("size must not be negative", reason="invalidSize", size=params.size)
        if dest.max_size is not None and params.size > dest.max_size:
            raise invalid(
                f"size {params.size} exceeds the limit of {dest.max_size} bytes",
                reason="maxSizeExceeded",
                maxSize=dest.max_size,
                actualSize=params.size,
            )
    if params.mime_type is None:
        return None
    # Malformed parameters are refused like a malformed type, and a wildcard is not a
    # file's type. Only type/subtype is matched against the accept list.
    parsed = split_media_type(params.mime_type)
    if parsed is None or "*" in parsed[0]:
        raise invalid(
            "mimeType is not a media type", reason="invalidMimeType", mimeType=params.mime_type
        )
    mime_type, declared = parsed
    if not Constraints(accept=dest.accept).allows(mime_type):
        raise invalid(
            f"{mime_type} is not accepted here",
            reason="mimeTypeNotAccepted",
            mimeType=mime_type,
            accept=list(dest.accept),
        )
    return declared


def destination_of(gateway: UploadGateway, name: str) -> Destination:
    """The registered destination ``name``. The error data needs its limits."""
    return gateway.destination(name)


def make_handler(
    gateway: UploadGateway | None,
    destination: str | None,
    owner: OwnerResolver | None,
    destinations: Sequence[str] = (),
) -> AuthorizeUpload | None:
    """The handler an extension serves, or ``None`` for an advertise-only extension."""
    if gateway is None:
        if destination is not None or owner is not None or destinations:
            raise ValueError("destination, destinations and owner need a gateway")
        return None
    if destination is None:
        raise ValueError("files/authorizeUpload needs a destination name")
    return AuthorizeUpload(gateway, destination, owner=owner, destinations=destinations)


def binding_args(handler: AuthorizeUpload) -> tuple[str, type[BaseModel], AuthorizeUpload]:
    """The method, params model and handler, in the order both frameworks'
    ``MethodBinding`` take them."""
    return METHOD, AuthorizeUploadParams, handler


__all__ = [
    "IDENTIFIER",
    "METHOD",
    "AuthorizeUpload",
    "AuthorizeUploadParams",
    "AuthorizeUploadRequest",
    "AuthorizeUploadResult",
    "Digest",
    "OwnerResolver",
    "binding_args",
    "extension_settings",
    "make_handler",
]
