"""The client half of SEP-2631: ask a server for an upload URL, then send the file.

``upload_file`` does the whole flow and returns the URI to pass to a tool::

    from mcp.client import Client
    from mcp_upload.client import upload_file

    async with Client(url) as client:
        uri = await upload_file(client, "report.pdf")
        await client.call_tool("ingest", {"file": uri})

It hashes the file, sends ``files/authorizeUpload`` with the size and SHA-256, posts
the bytes as ``multipart/form-data`` the way the returned descriptor says, and checks
the server's answer. The file is read from disk in chunks for both the hash and the
upload, so its size does not matter to memory.

``session`` is anything with the SDK's ``send_request``: a ``ClientSession``, or an
``mcp.client.Client`` or ``fastmcp.Client``, whose ``session`` is used. The SDK is
imported only when a request is sent.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import mimetypes
import os
from collections.abc import Mapping
from pathlib import Path
from typing import IO, Any

import httpx

from .tickets import b64url
from .types import FileDigest

_CHUNK = 1 << 20


class UploadFailed(Exception):
    """The upload endpoint refused the bytes. ``error`` is its code, such as
    ``digest_mismatch`` or ``too_large``, and ``details`` any data it sent with it."""

    def __init__(
        self,
        status_code: int,
        error: str,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(f"upload refused with HTTP {status_code}: {error}")
        self.status_code = status_code
        self.error = error
        self.details = dict(details) if details else None


def sha256_digest(source: str | os.PathLike[str] | bytes) -> tuple[int, FileDigest]:
    """The size and SEP-2631 digest of a file or a bytes value."""
    if isinstance(source, bytes):
        return len(source), {"algorithm": "sha-256", "value": b64url(_sha256(source))}
    hasher = hashlib.sha256()
    size = 0
    with open(source, "rb") as f:
        while chunk := f.read(_CHUNK):
            hasher.update(chunk)
            size += len(chunk)
    return size, {"algorithm": "sha-256", "value": b64url(hasher.hexdigest())}


async def authorize_upload(
    session: Any,
    *,
    name: str | None = None,
    mime_type: str | None = None,
    size: int | None = None,
    digest: FileDigest | str | None = None,
) -> dict[str, Any]:
    """Send ``files/authorizeUpload`` and return the result, ``{"file", "upload"}``.

    ``digest`` is a ``FileDigest`` or a bare base64url SHA-256. A server that does not
    serve the method raises ``MCPError`` with code -32601; one that refuses the
    declaration raises -32602 with the reason in ``error.data``.
    """
    from .adapters.sep2631 import (
        AuthorizeUploadParams,
        AuthorizeUploadRequest,
        AuthorizeUploadResult,
        Digest,
    )

    if isinstance(digest, str):
        digest = {"algorithm": "sha-256", "value": digest}
    params = AuthorizeUploadParams(
        name=name,
        mime_type=mime_type,
        size=size,
        digest=Digest(**digest) if digest is not None else None,
    )
    result = await _session(session).send_request(
        AuthorizeUploadRequest(params=params), AuthorizeUploadResult
    )
    return {"file": result.file, "upload": result.upload}


async def upload_file(
    session: Any,
    source: str | os.PathLike[str] | bytes,
    *,
    name: str | None = None,
    mime_type: str | None = None,
    http: httpx.AsyncClient | None = None,
) -> str:
    """Authorize, upload and verify a file, and return its ``mcp-file://`` URI.

    ``source`` is a path or the bytes themselves. ``name`` defaults to the path's
    file name. ``mime_type`` defaults to a guess from the name; when nothing can be
    guessed none is declared and the part is sent as ``application/octet-stream``.
    Raises ``UploadFailed`` if the endpoint refuses the bytes.
    """
    if not isinstance(source, bytes) and name is None:
        name = Path(source).name
    declared = mime_type or (mimetypes.guess_type(name)[0] if name else None)
    size, digest = await asyncio.to_thread(sha256_digest, source)
    authorized = await authorize_upload(
        session, name=name, mime_type=declared, size=size, digest=digest
    )
    descriptor = authorized["upload"]
    if descriptor.get("transport") not in ("https", "http"):
        raise ValueError(f"unsupported upload transport {descriptor.get('transport')!r}")
    multipart = descriptor.get("multipart") or {}
    field = multipart.get("fileField", "file")
    fields = multipart.get("fields") or {}

    client = http or httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=10.0))
    try:
        with _open(source) as f:
            response = await client.request(
                descriptor.get("method", "POST"),
                descriptor["url"],
                headers={"Accept": "application/json", **(descriptor.get("headers") or {})},
                data=fields,
                files={
                    field: (name or "upload", f, declared or "application/octet-stream"),
                },
            )
    finally:
        if http is None:
            await client.aclose()

    try:
        body = response.json()
    except ValueError:
        body = {}
    if not isinstance(body, dict):
        body = {}
    if response.status_code != 200 or body.get("status") != "completed":
        raise UploadFailed(
            response.status_code, str(body.get("error", "unknown")), body.get("details")
        )
    stored = body.get("file") or {}
    if stored.get("digest", {}).get("value") != digest["value"] or stored.get("size") != size:
        # The gateway checks the declared digest itself. This guards against a
        # server that does not.
        raise UploadFailed(response.status_code, "digest_mismatch")
    uri = authorized["file"].get("uri") or stored.get("uri")
    if not isinstance(uri, str):
        raise UploadFailed(response.status_code, "missing_uri")
    return uri


def _session(session: Any) -> Any:
    if hasattr(session, "send_request"):
        return session
    inner = getattr(session, "session", None)
    if inner is not None and hasattr(inner, "send_request"):
        return inner
    raise TypeError("session must be a ClientSession or a client with a .session")


def _open(source: str | os.PathLike[str] | bytes) -> IO[bytes]:
    if isinstance(source, bytes):
        return io.BytesIO(source)
    return open(source, "rb")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


__all__ = ["UploadFailed", "authorize_upload", "sha256_digest", "upload_file"]
