"""The upload gateway: issues tickets, serves the upload endpoint, streams bytes through.

The order of operations on a POST is the whole design:

1. Header-only checks: content type, boundary, and a declared size that is obviously
   too large. These cost nothing and run before the ticket is touched, so a request
   that could never carry a file cannot spend a single-use ticket.
2. Read the record without changing it, to learn the destination and its limits.
3. Flip the record from issued to redeemed, atomically, in the store. Exactly one
   request wins. Everyone else gets 410.
4. Only now consume the body. The multipart stream is parsed incrementally, the size
   limit is enforced on the bytes actually seen, the bytes are hashed, and they are
   forwarded to the destination through a bounded queue so a slow backend throttles
   the client instead of filling memory. Nothing is written to disk.
5. Record the terminal state on the surviving record so it can be looked up later.

The endpoint asks for no session, header or OAuth token. The ticket is the
authorization. That is safe only because the ticket is 256 bits of CSPRNG entropy,
stored as a hash, valid for one redemption enforced atomically, expiring in minutes,
bound to a destination the server author chose, and useless for reading anything back.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import secrets
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

import httpx
from starlette.requests import ClientDisconnect, Request
from starlette.responses import HTMLResponse, JSONResponse, Response
from starlette.routing import Route
from streaming_form_data import ParseFailedException, StreamingFormDataParser
from streaming_form_data.parser import UnexpectedPartException
from streaming_form_data.targets import BaseTarget

from . import page
from .destinations import Destination, Registry, UnknownDestination
from .multipart import multipart_error, sanitize_filename
from .store import Store, StoreFull
from .tickets import (
    Constraints,
    Outcome,
    Record,
    RedeemError,
    Status,
    hash_secret,
    new_id,
    new_secret,
    utcnow,
)
from .types import AwaitingUpload, FileTransferDescriptor, FileValue, UploadStatus

# Every failure the endpoint can report, and the HTTP status it maps to. Backend error
# text is never passed through; a backend failure becomes one of these codes.
ERROR_STATUS: dict[str, int] = {
    "not_multipart": 415,
    "missing_boundary": 400,
    "unknown_ticket": 404,
    "ticket_used": 410,
    "ticket_expired": 410,
    "too_large": 413,
    "missing_file": 400,
    "duplicate_file": 400,
    "unsupported_media_type": 415,
    "bad_multipart": 400,
    "unexpected_part": 400,
    "truncated": 400,
    "client_disconnected": 400,
    "upstream_unreachable": 502,
    "upstream_rejected": 502,
    "upstream_closed_early": 502,
    "store_full": 503,
    "misconfigured": 500,
    "internal": 500,
}


class UploadError(Exception):
    """A failure with a code from ``ERROR_STATUS``. The code is what gets stored and
    returned. The message is for logs only."""

    def __init__(self, code: str, message: str = "", *, upstream_status: int | None = None):
        super().__init__(message or code)
        self.code = code
        self.upstream_status = upstream_status

    @property
    def http_status(self) -> int:
        return ERROR_STATUS.get(self.code, 500)


@dataclass(frozen=True, slots=True)
class Issued:
    """A freshly issued ticket. ``secret`` leaves the server exactly once, inside
    ``upload_url``. It is not stored anywhere."""

    record: Record
    secret: str
    upload_url: str


class UploadGateway:
    def __init__(
        self,
        *,
        base_url: str,
        registry: Registry,
        store: Store,
        server_name: str = "mcp-upload",
        path: str = "/upload",
        field_name: str = "file",
        ttl: timedelta = timedelta(minutes=15),
        retention: timedelta = timedelta(hours=24),
        http: httpx.AsyncClient | None = None,
        queue_size: int = 4,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        """
        ``base_url`` is the public origin clients will reach the upload endpoint at.
        It cannot be derived from a tool call, which has no HTTP request behind it.

        ``ttl`` is how long a ticket may be redeemed. Fifteen minutes is generous for a
        program and about right for a person following a link. ``retention`` is how
        long the record stays afterwards so the outcome can be read.

        ``queue_size`` is how many parsed chunks may sit between the parser and the
        backend request. Small on purpose: that bound is the backpressure.
        """
        self._base_url = base_url.rstrip("/")
        self._registry = registry
        self._store = store
        self._server_name = server_name
        self._path = "/" + path.strip("/")
        self._field = field_name
        self._ttl = ttl
        self._retention = retention
        self._http = http or httpx.AsyncClient()
        self._owns_http = http is None
        self._queue_size = queue_size
        self._clock = clock
        self._transport = urlsplit(self._base_url).scheme or "https"

    # ----- issuing -----------------------------------------------------------------

    @property
    def path(self) -> str:
        return self._path

    async def issue(
        self,
        destination: str,
        *,
        caller: str | None = None,
        ttl: timedelta | None = None,
        max_size: int | None = None,
        accept: tuple[str, ...] | None = None,
    ) -> Issued:
        """Mint a ticket for ``destination``, which must be a registered name.

        Per-ticket limits can only tighten the destination's defaults. A tool that
        passes a larger ``max_size`` than the destination allows gets the destination's.
        """
        dest = self._registry.get(destination)
        limit = dest.max_size
        if max_size is not None:
            limit = max_size if limit is None else min(limit, max_size)
        constraints = Constraints(
            max_size=limit, accept=accept if accept is not None else dest.accept
        )
        now = self._clock()
        secret = new_secret()
        record = Record(
            id=new_id(),
            ticket_hash=hash_secret(secret),
            destination=dest.name,
            caller=caller,
            issued_at=now,
            expires_at=now + (ttl or self._ttl),
            retention_until=now + self._retention,
            constraints=constraints,
        )
        await self._store.put(record)
        return Issued(record=record, secret=secret, upload_url=self.upload_url(secret))

    def upload_url(self, secret: str) -> str:
        return f"{self._base_url}{self._path}/{secret}"

    def uri(self, record_id: str) -> str:
        return f"mcp-file://{self._server_name}/{record_id}"

    def describe(self, issued: Issued) -> AwaitingUpload:
        """The tool result for "send the file here". Shaped like SEP-2631's transfer
        descriptor so the wire format survives the proposal landing."""
        record = issued.record
        upload: FileTransferDescriptor = {
            "transport": self._transport,
            "method": "POST",
            "url": issued.upload_url,
            "multipart": {"fileField": self._field},
            "expiresAt": _iso(record.expires_at),
        }
        return {
            "status": "awaiting_upload",
            "id": record.id,
            "file": {"uri": self.uri(record.id)},
            "upload": upload,
        }

    async def status(self, record_id: str) -> UploadStatus:
        record = await self._store.get(record_id)
        if record is None:
            return {"id": record_id, "status": "unknown"}
        return self.status_of(record)

    def status_of(self, record: Record) -> UploadStatus:
        status: UploadStatus = {"id": record.id, "status": record.status.value}
        if record.status is Status.COMPLETED:
            status["file"] = self.file_value(record)
        elif record.status is Status.FAILED and record.outcome and record.outcome.error:
            status["error"] = record.outcome.error
        elif record.status is Status.ISSUED and record.expired(self._clock()):
            status["status"] = "expired"
        return status

    def file_value(self, record: Record) -> FileValue:
        value: FileValue = {"uri": self.uri(record.id)}
        outcome = record.outcome
        if outcome is None:
            return value
        if outcome.filename:
            value["name"] = outcome.filename
        if outcome.media_type:
            value["mimeType"] = outcome.media_type
        value["size"] = outcome.size
        if outcome.sha256:
            value["digest"] = {"algorithm": "sha-256", "value": outcome.sha256}
        return value

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    # ----- the HTTP endpoint -------------------------------------------------------

    def routes(self) -> list[Route]:
        return [Route(f"{self._path}/{{ticket}}", self.handle, methods=["GET", "POST"])]

    async def handle(self, request: Request) -> Response:
        secret = str(request.path_params["ticket"])
        if request.method == "GET":
            return await self._render_form(request, secret)
        return await self._ingest(request, secret)

    async def _render_form(self, request: Request, secret: str) -> Response:
        # Reading the record does not spend the ticket. The form is shown only while
        # the ticket is still redeemable, so a stale link says so instead of failing
        # after the user picked a file.
        record = await self._store.get_by_hash(hash_secret(secret))
        now = self._clock()
        if record is None:
            return _html(page.message("Unknown upload link", "This link is not valid."), 404)
        if record.status is not Status.ISSUED:
            return _html(page.message("Link already used", "This link has been used."), 410)
        if record.expired(now):
            return _html(page.message("Link expired", "Ask for a new upload link."), 410)
        body = page.form(
            action=request.url.path,
            field_name=self._field,
            accept=record.constraints.accept,
            max_size=record.constraints.max_size,
            expires_at=_iso(record.expires_at),
        )
        return _html(body, 200)

    async def _ingest(self, request: Request, secret: str) -> Response:
        # Steps 1 and 2: nothing here touches the ticket.
        code = multipart_error(request.headers)
        if code is not None:
            return self._error_response(request, None, code)

        ticket_hash = hash_secret(secret)
        now = self._clock()
        record = await self._store.get_by_hash(ticket_hash)
        if record is None:
            return self._error_response(request, None, "unknown_ticket")
        if record.status is not Status.ISSUED:
            return self._error_response(request, record, "ticket_used")
        if record.expired(now):
            return self._error_response(request, record, "ticket_expired")

        # The multipart envelope adds a little to the file size. Reject only what is
        # clearly over; the exact check happens on the bytes as they stream.
        limit = record.constraints.max_size
        declared = request.headers.get("content-length")
        if limit is not None and declared and declared.isdigit() and int(declared) > limit + 65536:
            return self._error_response(request, record, "too_large")

        try:
            dest = self._registry.get(record.destination)
        except UnknownDestination:
            return self._error_response(request, record, "misconfigured")

        # Step 3: the atomic flip. From here on the ticket is spent.
        redeemed = await self._store.redeem(ticket_hash, now)
        if isinstance(redeemed, RedeemError):
            code = "ticket_expired" if redeemed is RedeemError.EXPIRED else "ticket_used"
            return self._error_response(request, record, code)

        # Steps 4 and 5.
        status, outcome = await self._forward(request, redeemed, dest)
        final = await self._store.finish(redeemed.id, status, outcome, self._clock())
        if final is None:
            final = redeemed.finished(status, outcome, self._clock())
        if status is Status.COMPLETED:
            return self._success_response(request, final)
        return self._error_response(request, final, outcome.error or "internal")

    # ----- streaming ---------------------------------------------------------------

    async def _forward(
        self, request: Request, record: Record, dest: Destination
    ) -> tuple[Status, Outcome]:
        queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=self._queue_size)
        state = _PumpState()
        target = _FileTarget(queue, record.constraints, state)
        parser = StreamingFormDataParser(headers=request.headers, strict=True)
        parser.register(self._field, target)
        pump = asyncio.create_task(_pump(request, parser, target, queue, state))
        try:
            # The upstream URL and headers need the filename and media type, which the
            # parser learns from the file part's headers. Wait for that, or for the pump
            # to finish first, which means there was no usable file part.
            waiter = asyncio.create_task(state.started.wait())
            try:
                await asyncio.wait({waiter, pump}, return_when=asyncio.FIRST_COMPLETED)
            finally:
                waiter.cancel()
            if state.error is not None or target.filename is None:
                # The body failed before a usable file part appeared. No upstream
                # request is opened, so the backend never sees a phantom upload.
                if not pump.done():
                    await pump
                raise state.error or UploadError("missing_file", "no file part in the body")

            filename = target.filename or "upload"
            media_type = target.media_type or "application/octet-stream"
            url = dest.build_url(record.id, filename)
            headers, body = _upstream(dest, filename, media_type, queue, state)
            try:
                response = await self._http.request(
                    dest.method, url, content=body, headers=headers, timeout=dest.timeout
                )
            except UploadError:
                raise
            except httpx.HTTPError as exc:
                raise UploadError("upstream_unreachable", str(exc)) from exc

            if not pump.done():
                # The backend answered before the whole body was forwarded. There is no
                # sensible way to continue, so stop reading and report it.
                pump.cancel()
                with contextlib.suppress(BaseException):
                    await pump
                if 200 <= response.status_code < 300:
                    raise UploadError(
                        "upstream_closed_early",
                        "backend accepted before the upload finished",
                        upstream_status=response.status_code,
                    )
            else:
                await pump
            if state.error is not None:
                raise state.error
            if not 200 <= response.status_code < 300:
                raise UploadError(
                    "upstream_rejected",
                    f"backend returned {response.status_code}",
                    upstream_status=response.status_code,
                )
            return Status.COMPLETED, Outcome(
                size=target.size,
                filename=filename,
                media_type=media_type,
                sha256=target.hasher.hexdigest(),
                upstream_status=response.status_code,
            )
        except UploadError as exc:
            return Status.FAILED, Outcome(
                size=target.size,
                filename=target.filename,
                media_type=target.media_type,
                error=exc.code,
                upstream_status=exc.upstream_status,
            )
        finally:
            if not pump.done():
                pump.cancel()
                with contextlib.suppress(BaseException):
                    await pump

    # ----- responses ---------------------------------------------------------------

    def _success_response(self, request: Request, record: Record) -> Response:
        status = self.status_of(record)
        if _wants_html(request):
            file = status.get("file", {})
            rows = [
                ("id", record.id),
                ("name", str(file.get("name", ""))),
                ("size", str(file.get("size", 0))),
                ("sha-256", str(file.get("digest", {}).get("value", ""))),
            ]
            return _html(page.result("Upload complete", rows), 200)
        return _json(dict(status), 200)

    def _error_response(self, request: Request, record: Record | None, code: str) -> Response:
        http_status = ERROR_STATUS.get(code, 500)
        body: dict[str, Any] = {"status": "failed", "error": code}
        if record is not None:
            body["id"] = record.id
        if _wants_html(request):
            return _html(page.message("Upload failed", code.replace("_", " ")), http_status)
        return _json(body, http_status)


# ----- the streaming machinery -------------------------------------------------------

_DONE = object()


class _Abort:
    def __init__(self, exc: BaseException) -> None:
        self.exc = exc


class _PumpState:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.error: UploadError | None = None


class _FileTarget(BaseTarget):
    """Receives the file part from the multipart parser and hands each chunk to the
    forwarding queue.

    The ``await`` on ``queue.put`` is the backpressure. When the backend is slow the
    queue fills, this coroutine blocks, the parser stops, the request body stops being
    read, and the client's upload stalls. The server buffers at most ``queue_size``
    chunks, whatever the file size.
    """

    def __init__(self, queue: asyncio.Queue[Any], constraints: Constraints, state: _PumpState):
        super().__init__()
        self._queue = queue
        self._constraints = constraints
        self._state = state
        self.parts = 0
        self.size = 0
        self.filename: str | None = None
        self.media_type: str | None = None
        self.hasher = hashlib.sha256()
        self.done = False
        self._announced = False

    async def on_start_async(self) -> None:
        self.parts += 1
        if self.parts > 1:
            # Two parts with the file's field name. The form helpers in most frameworks
            # silently keep one and drop the other, which lets a request pass validation
            # on one and deliver the other. Refuse the whole thing instead.
            raise UploadError("duplicate_file", "more than one file part")

    def _announce(self) -> None:
        """Settle the filename and media type and tell the gateway the part is real.

        This runs at the first data chunk rather than at start, because the parser
        calls start as soon as it has read Content-Disposition, before it has seen the
        part's Content-Type header. By the first chunk every part header is in.
        """
        if self._announced:
            return
        self._announced = True
        if not self.multipart_filename:
            # A part with the right name but no filename is a plain form field, not a
            # file. Treating it as a file is how a text value ends up read as bytes.
            raise UploadError("missing_file", "the file part has no filename")
        if not self._constraints.allows(self.multipart_content_type):
            raise UploadError("unsupported_media_type", str(self.multipart_content_type))
        self.filename = sanitize_filename(self.multipart_filename)
        declared = self.multipart_content_type or "application/octet-stream"
        self.media_type = declared.split(";", 1)[0].strip().lower()
        self._state.started.set()

    async def on_data_received_async(self, chunk: bytes) -> None:
        self._announce()
        self.size += len(chunk)
        limit = self._constraints.max_size
        if limit is not None and self.size > limit:
            # Enforced here and not on Content-Length, because a chunked upload has no
            # Content-Length and a lying one is trivial to send.
            raise UploadError("too_large", f"more than {limit} bytes")
        self.hasher.update(chunk)
        await self._queue.put(bytes(chunk))

    async def on_finish_async(self) -> None:
        # Only mark the part complete. The end-of-body signal is sent by the pump once
        # the whole request has been parsed, so the upstream request stays open until
        # it is known that nothing objectionable followed the file part. A backend then
        # only commits an upload whose entire request validated.
        self._announce()
        self.done = True


async def _pump(
    request: Request,
    parser: StreamingFormDataParser,
    target: _FileTarget,
    queue: asyncio.Queue[Any],
    state: _PumpState,
) -> None:
    """Read the request body and feed it to the parser. Any failure is recorded on
    ``state`` and signalled into the queue so the upstream body generator stops."""
    try:
        async for chunk in request.stream():
            await parser.adata_received(chunk)
        if target.parts == 0:
            raise UploadError("missing_file", "no file part in the body")
        if not target.done:
            raise UploadError("truncated", "body ended before the file part was closed")
        await queue.put(_DONE)
    except UploadError as exc:
        state.error = exc
    except ClientDisconnect:
        state.error = UploadError("client_disconnected", "client went away mid-upload")
    except UnexpectedPartException as exc:
        # Strict parsing: a part with any name other than the file field is refused,
        # so nothing can ride along in the body unnoticed.
        state.error = UploadError("unexpected_part", str(exc))
    except ParseFailedException as exc:
        state.error = UploadError("bad_multipart", str(exc))
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        state.error = UploadError("internal", repr(exc))
    finally:
        if state.error is not None:
            # Wake the generator if it is waiting on an empty queue. If the queue is
            # full the generator is not waiting; it checks state.error as it drains.
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(_Abort(state.error))
            state.started.set()


def _upstream(
    dest: Destination,
    filename: str,
    media_type: str,
    queue: asyncio.Queue[Any],
    state: _PumpState,
) -> tuple[dict[str, str], AsyncIterator[bytes]]:
    headers = dict(dest.headers)
    if dest.encoding == "multipart":
        boundary = "mcpupload" + secrets.token_hex(16)
        headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
        quoted = filename.replace("\\", "\\\\").replace('"', '\\"')
        preamble = (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="{dest.field_name}"; filename="{quoted}"\r\n'
            f"Content-Type: {media_type}\r\n\r\n"
        ).encode()
        epilogue = f"\r\n--{boundary}--\r\n".encode()
    else:
        headers["Content-Type"] = media_type
        preamble = b""
        epilogue = b""

    async def body() -> AsyncIterator[bytes]:
        if preamble:
            yield preamble
        while True:
            if queue.empty() and state.error is not None:
                raise state.error
            item = await queue.get()
            if item is _DONE:
                break
            if isinstance(item, _Abort):
                raise item.exc
            yield item
        if epilogue:
            yield epilogue

    return headers, body()


# ----- small helpers -------------------------------------------------------------------

_NO_STORE = {"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"}


def _json(body: dict[str, Any], status: int) -> Response:
    return JSONResponse(body, status_code=status, headers=_NO_STORE)


def _html(body: str, status: int) -> Response:
    headers = {**_NO_STORE, "Referrer-Policy": "no-referrer"}
    return HTMLResponse(body, status_code=status, headers=headers)


def _wants_html(request: Request) -> bool:
    accept = request.headers.get("accept", "")
    return "text/html" in accept and "application/json" not in accept.split(",")[0]


def _iso(when: datetime) -> str:
    return when.isoformat(timespec="seconds").replace("+00:00", "Z")


__all__ = ["ERROR_STATUS", "Issued", "StoreFull", "UploadError", "UploadGateway"]
