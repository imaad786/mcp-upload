"""File uploads for MCP servers using short-lived upload tickets.

MCP tool arguments are JSON, and by default the model generates every value in them
token by token. That makes inline file transfer impractical: a 126 KB image costs
roughly 120,000 output tokens as base64, and the reference Python SDK rejects request
bodies over 4 MiB before it even parses them. The protocol has no file input type and
no way to ask a host to attach a file itself.

This package gives a server a working upload path without changing the protocol.
A tool issues a ticket, which is a random single-use string naming a short-lived
record on the server. The upload URL carries that ticket. Whoever holds the bytes
posts them to the URL over plain HTTPS. The server redeems the ticket atomically,
streams the bytes straight through to the destination backend without buffering
them, and keeps the record so the outcome can be checked afterwards. Only a small
reference ever travels through MCP.
"""

from .destinations import Destination, Registry, UnknownDestination
from .gateway import ERROR_STATUS, ClaimRefused, Issued, UploadError, UploadGateway
from .resolve import FileReferenceError, resolve_file
from .sinks import IncomingFile, Sink, UploadAborted
from .store import MemoryStore, SqliteStore, Store, StoreFull
from .tickets import ClaimError, Constraints, Outcome, Record, RedeemError, Status
from .types import AwaitingUpload, FileDigest, FileTransferDescriptor, FileValue, UploadStatus

__version__ = "1.0.1"

__all__ = [
    "ERROR_STATUS",
    "AwaitingUpload",
    "ClaimError",
    "ClaimRefused",
    "Constraints",
    "Destination",
    "FileDigest",
    "FileReferenceError",
    "FileTransferDescriptor",
    "FileValue",
    "IncomingFile",
    "Issued",
    "MemoryStore",
    "Outcome",
    "Record",
    "RedeemError",
    "Registry",
    "Sink",
    "SqliteStore",
    "Status",
    "Store",
    "StoreFull",
    "UnknownDestination",
    "UploadAborted",
    "UploadError",
    "UploadGateway",
    "UploadStatus",
    "__version__",
    "resolve_file",
]
