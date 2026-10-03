"""Turn a file URI a tool received back into the uploaded file.

Under SEP-2631 a client uploads first and then passes the file's URI,
``mcp-file://<server>/<id>``, to a tool as a plain string argument. The tool has to
check that the URI is one this server minted, that the upload finished, and usually
that nobody has used it already. ``resolve_file`` does the three in one call::

    @mcp.tool()
    async def ingest(file: str) -> FileValue:
        value = await resolve_file(gateway, file, owner=user_id)
        ...

It needs no MCP framework. Raising ``FileReferenceError`` from a tool turns into a
tool error the model can read, which is the right outcome for a bad reference.
"""

from __future__ import annotations

import re

from .gateway import ClaimRefused, UploadGateway
from .types import FileValue

# What tickets.new_id produces: "up_" and base64url characters.
_RECORD_ID = re.compile(r"^up_[A-Za-z0-9_-]{1,64}$")


class FileReferenceError(ValueError):
    """A file URI that cannot be used. ``reason`` is one of ``malformed``,
    ``other_server``, ``not_found``, ``not_completed``, ``failed`` or
    ``already_claimed``. A record bound to another owner is ``not_found``."""

    def __init__(self, reason: str, uri: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason
        self.uri = uri


def parse_file_uri(gateway: UploadGateway, uri: str) -> str:
    """The record id in ``uri``, which must be a URI this gateway minted."""
    if not uri.startswith("mcp-file://"):
        raise FileReferenceError("malformed", uri, "not an mcp-file:// URI")
    prefix = gateway.uri("")
    if not uri.startswith(prefix):
        raise FileReferenceError("other_server", uri, "this file was not uploaded to this server")
    record_id = uri[len(prefix) :]
    if not _RECORD_ID.match(record_id):
        raise FileReferenceError("malformed", uri, "the URI does not name an upload")
    return record_id


async def resolve_file(
    gateway: UploadGateway, uri: str, *, owner: str | None = None, claim: bool = True
) -> FileValue:
    """The completed upload ``uri`` names, as a ``FileValue`` with its measured size
    and digest.

    With ``claim`` (the default) the upload is taken for use exactly once, atomically,
    so two tool calls naming the same URI cannot both act on it. Without it the record
    is only read. Pass ``owner`` whenever tickets are issued with one; a record bound
    to someone else is then reported as not found.
    """
    record_id = parse_file_uri(gateway, uri)
    if claim:
        try:
            return await gateway.claim(record_id, owner=owner)
        except ClaimRefused as refused:
            reason = refused.reason.value
            if reason == "not_completed":
                status = await gateway.status(record_id, owner=owner)
                if status["status"] == "failed":
                    raise FileReferenceError(
                        "failed", uri, f"the upload failed: {status.get('error', 'unknown')}"
                    ) from None
            raise FileReferenceError(reason, uri, _MESSAGES[reason]) from None
    status = await gateway.status(record_id, owner=owner)
    state = status["status"]
    if state in ("completed", "claimed") and "file" in status:
        return status["file"]
    if state == "unknown":
        raise FileReferenceError("not_found", uri, _MESSAGES["not_found"])
    if state == "failed":
        raise FileReferenceError(
            "failed", uri, f"the upload failed: {status.get('error', 'unknown')}"
        )
    raise FileReferenceError("not_completed", uri, _MESSAGES["not_completed"])


_MESSAGES = {
    "not_found": "no such upload",
    "not_completed": "the upload has not finished",
    "already_claimed": "this upload has already been used",
}


__all__ = ["FileReferenceError", "parse_file_uri", "resolve_file"]
