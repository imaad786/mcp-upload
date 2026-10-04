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

A harness answering a tool's ``ask_for_upload`` without a person uses the other
helpers: ``upload_targets`` finds the targets in the ``input_required`` result,
``send_file`` sends the bytes the way one says, and ``upload_proof`` builds the
``_meta`` that proves the upload on the retry::

    result = await client.session.call_tool("ingest_report", allow_input_required=True)
    while isinstance(result, InputRequiredResult):
        answers = {}
        for key, target in upload_targets(result).items():
            stored = await send_file(target, "report.pdf", bearer=token)
            answers[key] = ElicitResult(action="accept", _meta=upload_proof(stored))
        result = await client.session.call_tool(
            "ingest_report",
            input_responses=answers,
            request_state=result.request_state,
            allow_input_required=True,
        )
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import mimetypes
import os
from collections.abc import AsyncIterator, Mapping
from pathlib import Path
from typing import IO, Any

import httpx

from .tickets import b64url
from .types import EXTENSION_ID, FileDigest, FileValue, UploadTarget

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
    destination: str | None = None,
) -> dict[str, Any]:
    """Send ``files/authorizeUpload`` and return the result, ``{"file", "upload"}``.

    ``digest`` is a ``FileDigest`` or a bare base64url SHA-256. ``destination`` names
    one of the destinations the server lets clients choose, which it lists in its
    extension settings. It travels in the request's ``_meta``. A server that does not
    serve the method raises ``MCPError`` with code -32601; one that refuses the
    declaration or the destination raises -32602 with the reason in ``error.data``.
    """
    from .adapters.sep2631 import (
        AuthorizeUploadParams,
        AuthorizeUploadRequest,
        AuthorizeUploadResult,
        Digest,
    )

    if isinstance(digest, str):
        digest = {"algorithm": "sha-256", "value": digest}
    meta: Any = None if destination is None else {EXTENSION_ID: {"destination": destination}}
    params = AuthorizeUploadParams(
        name=name,
        mime_type=mime_type,
        size=size,
        digest=Digest(**digest) if digest is not None else None,
        _meta=meta,
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
    bearer: str | None = None,
    destination: str | None = None,
) -> str:
    """Authorize, upload and verify a file, and return its ``mcp-file://`` URI.

    ``source`` is a path or the bytes themselves. ``name`` defaults to the path's
    file name. ``mime_type`` defaults to a guess from the name; when nothing can be
    guessed none is declared and the part is sent as ``application/octet-stream``.
    ``bearer`` is the access token to send if the upload endpoint asks for one, which
    is the token used for MCP requests to the same server. ``destination`` names one
    of the destinations the server offers. Raises ``UploadFailed`` if the endpoint
    refuses the bytes.
    """
    if not isinstance(source, bytes) and name is None:
        name = Path(source).name
    declared = mime_type or (mimetypes.guess_type(name)[0] if name else None)
    size, digest = await asyncio.to_thread(sha256_digest, source)
    authorized = await authorize_upload(
        session,
        name=name,
        mime_type=declared,
        size=size,
        digest=digest,
        destination=destination,
    )
    target: Any = authorized
    stored = await send_file(
        target, source, name=name, mime_type=declared, http=http, bearer=bearer
    )
    if stored.get("digest", {}).get("value") != digest["value"] or stored.get("size") != size:
        # The gateway checks the declared digest itself. This guards against a
        # server that does not.
        raise UploadFailed(200, "digest_mismatch")
    uri = authorized["file"].get("uri") or stored.get("uri")
    if not isinstance(uri, str):
        raise UploadFailed(200, "missing_uri")
    return uri


def upload_targets(result: Any) -> dict[str, UploadTarget]:
    """The machine-readable upload targets in an ``input_required`` result, keyed like
    its ``inputRequests``. Empty when there are none.

    ``result`` is the ``InputRequiredResult`` a tool returned through
    ``ask_for_upload``, or the same as a plain dict. A client that finds no target can
    still show the person the elicitation's URL.
    """
    meta = result.get("_meta") if isinstance(result, dict) else getattr(result, "meta", None)
    entry = meta.get(EXTENSION_ID) if isinstance(meta, dict) else None
    targets = entry.get("targets") if isinstance(entry, dict) else None
    found: dict[str, UploadTarget] = {}
    if not isinstance(targets, dict):
        return found
    for key, value in targets.items():
        if not isinstance(value, dict):
            continue
        file, upload = value.get("file"), value.get("upload")
        if isinstance(file, dict) and isinstance(upload, dict) and "url" in upload:
            found[str(key)] = {"file": file, "upload": upload}  # type: ignore[typeddict-item]
    return found


async def send_file(
    target: UploadTarget,
    source: str | os.PathLike[str] | bytes,
    *,
    name: str | None = None,
    mime_type: str | None = None,
    bearer: str | None = None,
    http: httpx.AsyncClient | None = None,
) -> FileValue:
    """Send a file's bytes the way ``target`` describes and return the stored
    ``FileValue``, with the size and SHA-256 the server measured.

    ``target`` is an ``UploadTarget``: a ``files/authorizeUpload`` result, or one
    ``upload_targets`` found in an ``input_required`` result. A multipart descriptor gets a form
    post, one without gets the raw bytes. ``bearer`` is sent as
    ``Authorization: Bearer`` when given. Send it whenever the descriptor's ``_meta``
    asks for ``{"auth": "bearer"}``. ``name`` and ``mime_type`` default to the
    target's. Raises ``UploadFailed`` if the endpoint refuses the bytes or answers
    without the stored file.
    """
    descriptor: dict[str, Any] = dict(target["upload"])
    declared: dict[str, Any] = dict(target.get("file") or {})
    if descriptor.get("transport") not in ("https", "http"):
        raise ValueError(f"unsupported upload transport {descriptor.get('transport')!r}")
    if name is None:
        name = declared.get("name") or (None if isinstance(source, bytes) else Path(source).name)
    mime_type = mime_type or declared.get("mimeType")
    headers = {"Accept": "application/json", **(descriptor.get("headers") or {})}
    if bearer is not None:
        headers["Authorization"] = f"Bearer {bearer}"
    multipart = descriptor.get("multipart")

    client = http or httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=10.0))
    try:
        with _open(source) as f:
            if multipart is not None:
                response = await client.request(
                    descriptor.get("method", "POST"),
                    descriptor["url"],
                    headers=headers,
                    data=multipart.get("fields") or {},
                    files={
                        multipart.get("fileField", "file"): (
                            name or "upload",
                            f,
                            mime_type or "application/octet-stream",
                        )
                    },
                )
            else:
                headers.setdefault("Content-Type", mime_type or "application/octet-stream")
                if name:
                    headers["Content-Disposition"] = _disposition(name)
                response = await client.request(
                    descriptor.get("method", "PUT"),
                    descriptor["url"],
                    headers=headers,
                    content=_chunks(f),
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
    stored = body.get("file")
    if not isinstance(stored, dict):
        raise UploadFailed(response.status_code, "missing_file")
    value: FileValue = stored  # type: ignore[assignment]
    return value


def upload_proof(stored: FileValue) -> dict[str, Any]:
    """The ``_meta`` for an ``ElicitResult`` that proves an upload: the file's URI
    and digest as the server reported them. ``ask_for_upload`` compares them with
    its record on the retry and refuses a mismatch."""
    proof: dict[str, Any] = {"uri": stored["uri"]}
    if "digest" in stored:
        proof["digest"] = dict(stored["digest"])
    return {EXTENSION_ID: {"file": proof}}


def _session(session: Any) -> Any:
    if hasattr(session, "send_request"):
        return session
    inner = getattr(session, "session", None)
    if inner is not None and hasattr(inner, "send_request"):
        return inner
    raise TypeError("session must be a ClientSession or a client with a .session")


def _disposition(name: str) -> str:
    """A Content-Disposition carrying ``name`` as RFC 6266's UTF-8 ``filename*``."""
    from urllib.parse import quote

    return f"attachment; filename*=UTF-8''{quote(name, safe='')}"


async def _chunks(f: IO[bytes]) -> AsyncIterator[bytes]:
    while chunk := await asyncio.to_thread(f.read, _CHUNK):
        yield chunk


def _open(source: str | os.PathLike[str] | bytes) -> IO[bytes]:
    if isinstance(source, bytes):
        return io.BytesIO(source)
    return open(source, "rb")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


__all__ = [
    "UploadFailed",
    "authorize_upload",
    "send_file",
    "sha256_digest",
    "upload_file",
    "upload_proof",
    "upload_targets",
]
