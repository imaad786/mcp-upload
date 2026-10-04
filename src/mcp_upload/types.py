"""Wire shapes returned to MCP clients.

These follow the vocabulary of the MCP file transfer proposal (SEP-2631): ``FileValue``
for a file reference, ``FileTransferDescriptor`` for "where and how to send the bytes".
A server built on this library then reads the same on the wire as the proposal, and if
the proposal lands, adopting it is a change of plumbing rather than of shape.

Keys are camelCase because they are JSON, not Python. They are TypedDicts rather than
models so the core has no dependency on a validation library; the MCP SDK builds a
tool output schema from them on its own.
"""

from __future__ import annotations

import sys
from typing import Any, Literal, NotRequired

# The MCP SDK builds tool output schemas with pydantic, which only understands the
# typing_extensions TypedDict on Python 3.11. On 3.11 the standard library one is
# accepted silently and produces no schema, so tools would return text instead of
# structured content.
if sys.version_info >= (3, 12):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict

#: Reverse-DNS identifier for the pattern this library implements, per SEP-2133. The
#: prefix is a domain Imaad owns and is not an MCP-organization namespace. It names the
#: extension in server capabilities, and it is the key under which this library puts
#: anything of its own into a ``_meta`` object.
EXTENSION_ID = "me.imaadkhan/upload-ticket"


class FileDigest(TypedDict):
    """``value`` is base64url without padding, as SEP-2631 specifies."""

    algorithm: str
    value: str


class FileValue(TypedDict):
    uri: str
    name: NotRequired[str]
    mimeType: NotRequired[str]
    size: NotRequired[int]
    digest: NotRequired[FileDigest]


class MultipartDescriptor(TypedDict):
    fileField: str
    fields: NotRequired[dict[str, str]]


class FileTransferDescriptor(TypedDict):
    """``_meta`` appears only on a gateway that authenticates uploads. Under
    ``EXTENSION_ID`` it says how, for example ``{"auth": "bearer"}``: send the same
    bearer token the client uses for MCP requests to this server. It never carries a
    token."""

    transport: str
    method: str
    url: str
    headers: NotRequired[dict[str, str]]
    multipart: NotRequired[MultipartDescriptor]
    expiresAt: str
    _meta: NotRequired[dict[str, Any]]


class UploadTarget(TypedDict):
    """Everything a program needs to send a file without a person: the file to be and
    how to send its bytes. It is the result of ``files/authorizeUpload``, and what
    ``ask_for_upload`` puts in its ``input_required`` result's ``_meta``, under
    ``EXTENSION_ID`` and ``targets``."""

    file: FileValue
    upload: FileTransferDescriptor


class AwaitingUpload(TypedDict):
    """What a tool returns when it needs a file it does not have yet."""

    status: Literal["awaiting_upload"]
    id: str
    file: FileValue
    upload: FileTransferDescriptor


class UploadStatus(TypedDict):
    """What a status lookup returns. ``status`` is one of issued, redeemed, completed,
    claimed, failed, expired or unknown, plus declined or cancelled when the user was
    asked through elicitation and refused. ``file`` is present once the upload
    completed. ``error`` names a failure, and ``details`` carries machine-readable
    specifics such as ``{"reason": "maxSizeExceeded", "maxSize": 1000}``."""

    id: str
    status: str
    file: NotRequired[FileValue]
    error: NotRequired[str]
    details: NotRequired[dict[str, Any]]
