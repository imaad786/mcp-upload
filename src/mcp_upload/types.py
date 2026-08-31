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

from typing import Literal, NotRequired, TypedDict


class FileDigest(TypedDict):
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
    transport: str
    method: str
    url: str
    headers: NotRequired[dict[str, str]]
    multipart: NotRequired[MultipartDescriptor]
    expiresAt: str


class AwaitingUpload(TypedDict):
    """What a tool returns when it needs a file it does not have yet."""

    status: Literal["awaiting_upload"]
    id: str
    file: FileValue
    upload: FileTransferDescriptor


class UploadStatus(TypedDict):
    """What a status lookup returns. ``status`` is one of issued, redeemed, completed,
    failed, or unknown. ``file`` is present once the upload completed."""

    id: str
    status: str
    file: NotRequired[FileValue]
    error: NotRequired[str]
