"""The upload gateway: issues tickets, serves the upload endpoint, streams bytes through.

The order of operations on a POST is the whole design:

1. Header-only checks: content type, boundary, and a declared size that is obviously
   too large. These cost nothing and run before the ticket is touched, so a request
   that could never carry a file cannot spend a single-use ticket.
2. Read the record without changing it, to learn the destination and its limits. A raw
   upload's media type is checked against the accept list here, since it is a header.
3. Flip the record from issued to redeemed, atomically, in the store. Exactly one
   request wins. Everyone else gets 410.
4. Only now consume the body. A multipart stream is parsed incrementally (a raw body is
   the file as it stands), the size limit is enforced on the bytes actually seen, the
   bytes are hashed, and they are forwarded to the destination through a bounded queue
   so a slow backend or sink throttles the client instead of filling memory. The
   gateway itself writes nothing to disk.
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
import logging
import re
import secrets
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

import httpx
from starlette.background import BackgroundTasks
from starlette.requests import ClientDisconnect, Request
from starlette.responses import HTMLResponse, JSONResponse, Response
from starlette.routing import Route
from streaming_form_data import ParseFailedException, StreamingFormDataParser
from streaming_form_data.parser import UnexpectedPartException
from streaming_form_data.targets import BaseTarget

from . import page
from .destinations import Destination, Registry, UnknownDestination
from .multipart import (
    Framer,
    boundary_of,
    filename_from_disposition,
    multipart_error,
    parse_content_type,
    raw_body_allowed,
    sanitize_filename,
)
from .sinks import IncomingFile, UploadAborted
from .store import Store, StoreFull
from .telemetry import Telemetry
from .tickets import (
    ClaimError,
    Constraints,
    Outcome,
    Record,
    RedeemError,
    Status,
    b64url,
    hash_secret,
    new_id,
    new_secret,
    sha256_hex,
    utcnow,
)
from .types import AwaitingUpload, FileDigest, FileTransferDescriptor, FileValue, UploadStatus

# The library logs through this name. Records are identified by their public id only.
# The ticket secret and the upload URL never appear in a log line.
logger = logging.getLogger("mcp_upload")
logger.addHandler(logging.NullHandler())

# The token grammar of RFC 7230, which is what a media type is made of. Anything the
# multipart parser hands us that does not match is refused rather than forwarded,
# because the value becomes a request header to the backend and a field on the record.
_TCHARS = r"[A-Za-z0-9!#$%&'*+.^_`|~-]+"
_MEDIA_TYPE = re.compile(rf"^{_TCHARS}/{_TCHARS}$")
_MAX_MEDIA_TYPE_LENGTH = 255

# Bounds on everything in the body that is not file data. The multipart parser buffers
# part headers whole, with cost that grows faster than their size: a 64 MiB header
# drove one process past 4 GB while max_size was 1 MiB, because max_size only counts
# file bytes. So the bytes handed to the parser beyond the file's own are capped, and
# the preamble and epilogue that RFC 2046 allows are bounded separately. The parser is
# fed at most one slice at a time so a single large read cannot slip past the check.
# The slice is the largest read asyncio delivers, so a normal read is one call: smaller
# slices multiplied the per-chunk cost of hashing, queueing and forwarding and took a
# third off throughput. The overhead cap leaves room for the parser holding back part
# of a slice while it looks for a delimiter.
_MAX_OVERHEAD = 512 * 1024
_FEED_SLICE = 256 * 1024
_MAX_PREAMBLE = 16 * 1024
_MAX_EPILOGUE = 64 * 1024

# Every failure the endpoint can report, and the HTTP status it maps to. Backend error
# text is never passed through; a backend failure becomes one of these codes.
ERROR_STATUS: dict[str, int] = {
    "not_multipart": 415,
    "missing_boundary": 400,
    "unknown_ticket": 404,
    "ticket_used": 410,
    "ticket_expired": 410,
    "too_large": 413,
    "digest_mismatch": 422,
    "size_mismatch": 422,
    "part_headers_too_large": 400,
    "too_slow": 408,
    "upload_timeout": 408,
    "too_many_uploads": 503,
    "missing_file": 400,
    "duplicate_file": 400,
    "invalid_media_type": 400,
    "unsupported_media_type": 415,
    "bad_multipart": 400,
    "unexpected_part": 400,
    "truncated": 400,
    "client_disconnected": 400,
    "upstream_unreachable": 502,
    "upstream_rejected": 502,
    "upstream_closed_early": 502,
    "sink_failed": 502,
    "store_full": 503,
    "misconfigured": 500,
    "internal": 500,
}


class UploadError(Exception):
    """A failure with a code from ``ERROR_STATUS``. The code is what gets stored and
    returned. The message is for logs only."""

    def __init__(
        self,
        code: str,
        message: str = "",
        *,
        upstream_status: int | None = None,
        details: dict[str, Any] | None = None,
    ):
        super().__init__(message or code)
        self.code = code
        self.upstream_status = upstream_status
        self.details = details

    @property
    def http_status(self) -> int:
        return ERROR_STATUS.get(self.code, 500)


class ClaimRefused(Exception):
    """``UploadGateway.claim`` could not claim the record. ``reason`` says why:
    not_found (no such record, or another owner's), not_completed, or already_claimed."""

    def __init__(self, reason: ClaimError):
        super().__init__(reason.value)
        self.reason = reason


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
        max_in_flight: int | None = 100,
        stall_timeout: timedelta | None = timedelta(seconds=30),
        stall_min_bytes: int = 64 * 1024,
        upload_timeout: timedelta | None = timedelta(hours=1),
        on_complete: Callable[[Record], Awaitable[None]] | None = None,
        telemetry: bool = True,
        raw_uploads: bool = False,
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

        ``max_in_flight`` caps how many uploads may be streaming at once in this
        process. Each one holds a parser, a queue and a connection to the backend.
        Beyond the cap a request gets 503 before its ticket is touched, so it can be
        retried. The default HTTP client is sized to the same number, so the cap is the
        only limit: a smaller hidden pool would queue uploads after their tickets were
        spent. ``None`` removes the cap and the pool limit together. If you pass your own
        ``http`` client, size its pool to at least ``max_in_flight``.

        ``stall_timeout`` and ``stall_min_bytes`` refuse an upload that delivers fewer
        than ``stall_min_bytes`` within ``stall_timeout`` of waiting on the client.
        Only time spent waiting for the client counts, never time spent waiting on a
        slow backend. Without this, a client sending a byte every few seconds holds a
        slot and a backend connection indefinitely. ``upload_timeout`` bounds the
        whole upload from redemption to the last byte. ``None`` disables either.

        ``on_complete`` is awaited with the final record of every redeemed upload,
        completed or failed, after the response has gone to the uploader, so a server
        can start work on a file without polling. An exception it raises is logged and
        does not affect the upload.

        ``telemetry`` emits OpenTelemetry traces and metrics when the API is installed
        and the application has configured an SDK. See ``mcp_upload.telemetry``.

        ``raw_uploads`` also accepts a POST or PUT whose body is the file itself, for
        clients that find a raw body easier than a form (``curl -T``, an S3-style
        uploader). The media type is the request's Content-Type and the filename comes
        from a Content-Disposition request header or a ``filename`` query parameter.
        Every limit applies as it does to a form upload. A URL-encoded form body is
        never taken as a file. Off by default, and then anything but
        ``multipart/form-data`` is refused as before.
        """
        if ttl <= timedelta(0):
            raise ValueError("ttl must be positive")
        if retention < ttl:
            raise ValueError("retention must be at least ttl, or records vanish while redeemable")
        if max_in_flight is not None and max_in_flight < 1:
            raise ValueError("max_in_flight must be at least 1, or None for no cap")
        if queue_size < 1:
            raise ValueError("queue_size must be at least 1")
        self._base_url = base_url.rstrip("/")
        self._registry = registry
        self._store = store
        self._server_name = server_name
        self._path = "/" + path.strip("/")
        self._field = field_name
        self._ttl = ttl
        self._retention = retention
        self._http = http or httpx.AsyncClient(
            limits=httpx.Limits(
                max_connections=max_in_flight,
                max_keepalive_connections=min(20, max_in_flight or 20),
            )
        )
        self._owns_http = http is None
        self._queue_size = queue_size
        self._max_in_flight = max_in_flight
        self._in_flight = 0
        self._stall_timeout = None if stall_timeout is None else stall_timeout.total_seconds()
        self._stall_min_bytes = stall_min_bytes
        self._upload_timeout = None if upload_timeout is None else upload_timeout.total_seconds()
        self._on_complete = on_complete
        self._telemetry = Telemetry(telemetry)
        self._raw_uploads = raw_uploads
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
        owner: str | None = None,
        ttl: timedelta | None = None,
        max_size: int | None = None,
        accept: tuple[str, ...] | None = None,
        expected_size: int | None = None,
        expected_digest: FileDigest | str | None = None,
    ) -> Issued:
        """Mint a ticket for ``destination``, which must be a registered name.

        ``owner`` binds the record to whoever asked for it, typically the
        authenticated user. ``status`` and ``claim`` then answer only that owner, and
        report the record as unknown to anyone else, so an id seen in a transcript
        does not leak a file's name, size or digest to another user.

        ``expected_size`` and ``expected_digest`` are what the uploader declared in
        advance, as SEP-2631's ``files/authorizeUpload`` carries them. The digest is a
        ``FileDigest`` or a bare base64url SHA-256. An upload whose bytes do not match
        is refused with ``size_mismatch`` or ``digest_mismatch`` before the backend
        sees the end of the body, so a backend that commits only complete bodies never
        stores it.

        Per-ticket limits can only tighten the destination's defaults. A tool that
        passes a larger ``max_size`` than the destination allows gets the destination's.
        An ``accept`` list must stay inside the destination's: a type the destination
        does not allow raises ``ValueError``, and an empty list means the destination's
        own. ``ttl`` must be positive and no longer than the gateway's retention.
        """
        dest = self._registry.get(destination)
        limit = dest.max_size
        if max_size is not None:
            if max_size < 0:
                raise ValueError("max_size must not be negative")
            limit = max_size if limit is None else min(limit, max_size)
        expected_sha256 = _expected_sha256(expected_digest)
        if expected_size is not None:
            if expected_size < 0:
                raise ValueError("expected_size must not be negative")
            if limit is not None and expected_size > limit:
                raise ValueError("expected_size exceeds the size limit")
            limit = expected_size
        constraints = Constraints(
            max_size=limit,
            accept=_narrow(dest.accept, accept),
            expected_size=expected_size,
            expected_sha256=expected_sha256,
        )
        if ttl is not None and ttl <= timedelta(0):
            raise ValueError("ttl must be positive")
        if ttl is not None and ttl > self._retention:
            raise ValueError("ttl must not exceed the gateway's retention")
        now = self._clock()
        secret = new_secret()
        record = Record(
            id=new_id(),
            ticket_hash=hash_secret(secret),
            destination=dest.name,
            caller=caller,
            issued_at=now,
            expires_at=now + (self._ttl if ttl is None else ttl),
            retention_until=now + self._retention,
            constraints=constraints,
            owner=owner,
        )
        await self._store.put(record)
        self._telemetry.issued(dest.name)
        logger.info("issued %s for destination %s", record.id, dest.name)
        return Issued(record=record, secret=secret, upload_url=self.upload_url(secret))

    def destination(self, name: str) -> Destination:
        """The registered destination ``name``, for adapters that need its limits.
        Raises ``UnknownDestination``."""
        return self._registry.get(name)

    def upload_url(self, secret: str) -> str:
        return f"{self._base_url}{self._path}/{secret}"

    def uri(self, record_id: str) -> str:
        return f"mcp-file://{self._server_name}/{record_id}"

    def describe(
        self, issued: Issued, *, raw: bool = False, media_type: str | None = None
    ) -> AwaitingUpload:
        """The tool result for "send the file here". Shaped like SEP-2631's transfer
        descriptor so the wire format survives the proposal landing.

        By default it describes a multipart form POST, which every client can send.
        ``raw=True`` describes a raw-body PUT instead, with no ``multipart`` key, and
        needs a gateway built with ``raw_uploads=True``. Its ``headers`` carry the
        Content-Type to send when one is known: ``media_type`` if given, else the
        ticket's accept list when that names exactly one type.
        """
        record = issued.record
        upload: FileTransferDescriptor
        if raw:
            if not self._raw_uploads:
                raise ValueError("raw uploads are not enabled on this gateway")
            content_type = media_type or _single_type(record.constraints.accept)
            if content_type is not None and not record.constraints.allows(content_type):
                raise ValueError(f"media type {content_type!r} is not accepted by this ticket")
            upload = {
                "transport": self._transport,
                "method": "PUT",
                "url": issued.upload_url,
                "expiresAt": _iso(record.expires_at),
            }
            if content_type is not None:
                upload["headers"] = {"Content-Type": content_type}
        else:
            upload = {
                "transport": self._transport,
                "method": "POST",
                "url": issued.upload_url,
                "multipart": {"fileField": self._field},
                "expiresAt": _iso(record.expires_at),
            }
        file: FileValue = {"uri": self.uri(record.id)}
        if record.constraints.expected_size is not None:
            file["size"] = record.constraints.expected_size
        if record.constraints.expected_sha256 is not None:
            file["digest"] = _digest(record.constraints.expected_sha256)
        return {
            "status": "awaiting_upload",
            "id": record.id,
            "file": file,
            "upload": upload,
        }

    async def status(self, record_id: str, *, owner: str | None = None) -> UploadStatus:
        """The state of a record. Pass ``owner`` whenever the question comes from a
        user: a record bound to a different owner is then reported as unknown."""
        record = await self._store.get(record_id)
        if record is None or not _visible(record, owner):
            return {"id": record_id, "status": "unknown"}
        return self.status_of(record)

    def status_of(self, record: Record) -> UploadStatus:
        status: UploadStatus = {"id": record.id, "status": record.status.value}
        now = self._clock()
        if record.status in (Status.COMPLETED, Status.CLAIMED):
            status["file"] = self.file_value(record)
        elif record.status is Status.FAILED and record.outcome and record.outcome.error:
            status["error"] = record.outcome.error
            if record.outcome.details:
                status["details"] = dict(record.outcome.details)
        elif record.status is Status.ISSUED and record.expired(now):
            status["status"] = "expired"
        elif record.status is Status.REDEEMED and self._abandoned(record, now):
            # The process streaming this upload went away without recording an end.
            # Without this the record would read "redeemed" until it is swept.
            status["status"] = "failed"
            status["error"] = "abandoned"
        return status

    def _abandoned(self, record: Record, now: datetime) -> bool:
        if self._upload_timeout is None or record.redeemed_at is None:
            return False
        limit = timedelta(seconds=self._upload_timeout) + _ABANDON_GRACE
        return now > record.redeemed_at + limit

    async def claim(self, record_id: str, *, owner: str | None = None) -> FileValue:
        """Take a completed upload for use, once. The first claim wins and every later
        one raises ``ClaimRefused``, so two tool calls naming the same file cannot both
        act on it. A record bound to another owner is refused as unknown."""
        record = await self._store.get(record_id)
        if record is None or not _visible(record, owner):
            raise ClaimRefused(ClaimError.NOT_FOUND)
        claimed = await self._store.claim(record_id, self._clock())
        if isinstance(claimed, ClaimError):
            raise ClaimRefused(claimed)
        return self.file_value(claimed)

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
            value["digest"] = _digest(outcome.sha256)
        return value

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    # ----- the HTTP endpoint -------------------------------------------------------

    def routes(self) -> list[Route]:
        methods = ["GET", "POST", "PUT"] if self._raw_uploads else ["GET", "POST"]
        return [Route(f"{self._path}/{{ticket}}", self.handle, methods=methods)]

    async def handle(self, request: Request) -> Response:
        secret = str(request.path_params["ticket"])
        if request.method == "GET":
            return await self._render_form(request, secret)
        response = await self._ingest(request, secret)
        after = BackgroundTasks()
        meter = getattr(request.state, "mcp_upload_meter", None)
        if meter is None or not meter.body_done:
            # Refused before the body was read to its end. Closing a socket with unread
            # data makes the kernel send a reset, which can destroy the response before
            # the client reads it, so a 503 or 413 arrives as "connection reset" and the
            # client cannot tell what happened. Reading a bounded amount after the
            # response is sent gives the client time to see the answer and stop.
            after.add_task(_drain, request.receive)
        final = getattr(request.state, "mcp_upload_final", None)
        if self._on_complete is not None and final is not None:
            after.add_task(_notify, self._on_complete, final)
        if after.tasks:
            response.background = after
        return response

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
        # A fresh nonce per response lets the page's one inline script run while the
        # policy still refuses every other script, inline or external.
        nonce = secrets.token_urlsafe(18)
        body = page.form(
            action=request.url.path,
            field_name=self._field,
            accept=record.constraints.accept,
            max_size=record.constraints.max_size,
            expires_at=_iso(record.expires_at),
            nonce=nonce,
        )
        return _html(body, 200, csp=_form_policy(nonce))

    async def _ingest(self, request: Request, secret: str) -> Response:
        # Steps 1 and 2: nothing here touches the ticket.
        code = multipart_error(request.headers)
        raw = False
        if code == "not_multipart" and self._raw_uploads and raw_body_allowed(request.headers):
            raw, code = True, None
        if code is not None:
            return self._error_response(request, None, code)
        # The slot is taken before the first await. Checking here and counting later
        # lets every request in a burst pass the check while the others are suspended
        # in the store, which with a cap of 8 put 100 uploads on the backend at once.
        if self._max_in_flight is not None and self._in_flight >= self._max_in_flight:
            return self._error_response(request, None, "too_many_uploads")
        self._in_flight += 1
        try:
            return await self._ingest_in_slot(request, secret, raw)
        finally:
            self._in_flight -= 1

    async def _ingest_in_slot(self, request: Request, secret: str, raw: bool) -> Response:
        ticket_hash = hash_secret(secret)
        now = self._clock()
        record = await self._store.get_by_hash(ticket_hash)
        if record is None:
            return self._error_response(request, None, "unknown_ticket")
        if record.status is not Status.ISSUED:
            return self._error_response(request, record, "ticket_used")
        if record.expired(now) and not getattr(self._store, "decides_expiry", False):
            # A store that judges expiry by its own clock (RedisStore with server_clock)
            # has the final word at redemption. Pre-checking here with this replica's
            # clock would refuse tickets the store would accept when the clocks differ.
            return self._error_response(request, record, "ticket_expired")

        # The multipart envelope adds a little to the file size. Reject only what is
        # clearly over; the exact check happens on the bytes as they stream. A raw body
        # is the file, so its declared length is held to the limit exactly.
        limit = record.constraints.max_size
        declared = request.headers.get("content-length")
        slack = 0 if raw else 65536
        if limit is not None and declared and declared.isdigit() and int(declared) > limit + slack:
            return self._error_response(
                request, record, "too_large", {"reason": "maxSizeExceeded", "maxSize": limit}
            )

        # A raw upload names its type and filename in headers, so the type is checked
        # before the ticket is spent rather than after, as a form part's must be.
        raw_file: tuple[str, str] | None = None
        if raw:
            try:
                raw_file = _raw_file(request, record.constraints)
            except UploadError as exc:
                return self._error_response(request, record, exc.code, exc.details)

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
        started = time.monotonic()
        with self._telemetry.upload(redeemed.id, dest.name) as span:
            status, outcome = await self._forward(request, redeemed, dest, raw_file)
            self._telemetry.finished(
                span,
                dest.name,
                "completed" if status is Status.COMPLETED else outcome.error or "internal",
                outcome.size,
                time.monotonic() - started,
            )
        final = await self._store.finish(redeemed.id, status, outcome, self._clock())
        if final is None:
            final = redeemed.finished(status, outcome, self._clock())
        request.state.mcp_upload_final = final
        if status is Status.COMPLETED:
            logger.info("completed %s: %d bytes to %s", final.id, outcome.size, dest.name)
            return self._success_response(request, final)
        details = dict(outcome.details) if outcome.details else None
        return self._error_response(request, final, outcome.error or "internal", details)

    # ----- streaming ---------------------------------------------------------------

    async def _forward(
        self, request: Request, record: Record, dest: Destination, raw: tuple[str, str] | None
    ) -> tuple[Status, Outcome]:
        queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=self._queue_size)
        state = _PumpState()
        target = _FileTarget(queue, record.constraints, state)
        body: _MultipartBody | _RawBody = (
            _MultipartBody(request.headers, self._field, target)
            if raw is None
            else _RawBody(target, *raw)
        )
        meter = _Meter()
        request.state.mcp_upload_meter = meter
        pump = asyncio.create_task(
            _pump(request, body, target, queue, state, meter, self._stall_min_bytes)
        )
        watchdog = asyncio.create_task(
            _watch(
                pump,
                meter,
                state,
                stall_timeout=self._stall_timeout,
                upload_timeout=self._upload_timeout,
            )
        )
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
                # request is opened and no sink is called, so the destination never
                # sees a phantom upload.
                if not pump.done():
                    await pump
                raise state.error or UploadError("missing_file", "no file part in the body")

            filename = target.filename or "upload"
            media_type = target.media_type or "application/octet-stream"
            stream = _Stream(queue, state)
            upstream_status: int | None = None
            if dest.sink is not None:
                await self._to_sink(dest, record, filename, media_type, stream, pump)
            else:
                upstream_status = await self._to_http(
                    dest, record, filename, media_type, stream, pump
                )
            return Status.COMPLETED, Outcome(
                size=target.size,
                filename=filename,
                media_type=media_type,
                sha256=target.hasher.hexdigest(),
                upstream_status=upstream_status,
            )
        except UploadError as exc:
            return Status.FAILED, Outcome(
                size=target.size,
                filename=target.filename,
                media_type=target.media_type,
                error=exc.code,
                upstream_status=exc.upstream_status,
                details=exc.details,
            )
        finally:
            watchdog.cancel()
            if not pump.done():
                pump.cancel()
                with contextlib.suppress(BaseException):
                    await pump

    # Both ways out read the file from the same ``_Stream``, which ends only once the
    # whole request validated and raises on any failure. Each returns the status worth
    # recording (the backend's HTTP status, if any), or raises an ``UploadError``.

    async def _to_http(
        self,
        dest: Destination,
        record: Record,
        filename: str,
        media_type: str,
        stream: _Stream,
        pump: asyncio.Task[None],
    ) -> int:
        url = dest.build_url(record.id, filename)
        headers, body = _upstream(dest, filename, media_type, stream)
        try:
            response = await self._http.request(
                dest.method, url, content=body, headers=headers, timeout=dest.timeout
            )
        except UploadError:
            raise
        except httpx.HTTPError as exc:
            raise UploadError("upstream_unreachable", str(exc)) from exc
        accepted = 200 <= response.status_code < 300
        if await _stop_reading(pump) and accepted:
            # The backend answered before the whole body was forwarded. There is no
            # sensible way to continue, so stop reading and report it.
            raise UploadError(
                "upstream_closed_early",
                "backend accepted before the upload finished",
                upstream_status=response.status_code,
            )
        stream.raise_for_error()
        if not accepted:
            raise UploadError(
                "upstream_rejected",
                f"backend returned {response.status_code}",
                upstream_status=response.status_code,
            )
        return response.status_code

    async def _to_sink(
        self,
        dest: Destination,
        record: Record,
        filename: str,
        media_type: str,
        stream: _Stream,
        pump: asyncio.Task[None],
    ) -> None:
        sink = dest.sink
        if sink is None:  # pragma: no cover - the caller checked
            raise UploadError("misconfigured")
        incoming = IncomingFile(
            record_id=record.id,
            destination=dest.name,
            filename=filename,
            media_type=media_type,
            expected_size=record.constraints.expected_size,
            expected_sha256=record.constraints.expected_sha256,
            read=stream.read_for_sink,
        )
        loop = asyncio.get_running_loop()
        try:
            # The watchdog bounds the client. Once the body stops arriving, ended or
            # failed, the sink has dest.timeout to finish, as a backend has its read
            # timeout, so a sink stuck in its own I/O cannot hold a slot forever.
            async with asyncio.timeout(None) as bound:

                def arm(_: object) -> None:
                    with contextlib.suppress(RuntimeError):
                        bound.reschedule(loop.time() + dest.timeout)

                pump.add_done_callback(arm)
                try:
                    await sink(incoming)
                finally:
                    pump.remove_done_callback(arm)
        except Exception as exc:
            # A sink that saw UploadAborted and raised, or raised something of its own
            # after the upload failed, is reporting the upload's own failure.
            stream.raise_for_error()
            logger.warning("sink for %s raised on %s: %r", dest.name, record.id, exc)
            reason = "sink did not finish in time" if isinstance(exc, TimeoutError) else repr(exc)
            raise UploadError("sink_failed", reason) from exc
        stopped = await _stop_reading(pump)
        stream.raise_for_error()
        if stopped or not stream.ended:
            raise UploadError(
                "upstream_closed_early", "the sink returned before the end of the upload"
            )

    # ----- responses ---------------------------------------------------------------

    def _success_response(self, request: Request, record: Record) -> Response:
        status = self.status_of(record)
        if _wants_html(request):
            file = status.get("file", {})
            rows = [
                ("id", record.id),
                ("name", str(file.get("name", ""))),
                ("size", str(file.get("size", 0))),
                ("sha-256 (base64url)", str(file.get("digest", {}).get("value", ""))),
            ]
            return _html(page.result("Upload complete", rows), 200)
        return _json(dict(status), 200)

    def _error_response(
        self,
        request: Request,
        record: Record | None,
        code: str,
        details: dict[str, Any] | None = None,
    ) -> Response:
        http_status = ERROR_STATUS.get(code, 500)
        if record is None or record.status is Status.ISSUED:
            self._telemetry.refused(code)
        body: dict[str, Any] = {"status": "failed", "error": code}
        if record is not None:
            body["id"] = record.id
        if details:
            body["details"] = details
        logger.warning("refused %s: %s", record.id if record else "-", code)
        headers = {"Retry-After": "5"} if code == "too_many_uploads" else None
        if _wants_html(request):
            return _html(page.message("Upload failed", code.replace("_", " ")), http_status)
        return _json(body, http_status, headers)


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
        self.media_type = _checked_media_type(self.multipart_content_type, self._constraints)
        self.filename = sanitize_filename(self.multipart_filename)
        self._state.started.set()

    async def on_data_received_async(self, chunk: bytes) -> None:
        self._announce()
        self.size += len(chunk)
        expected = self._constraints.expected_size
        if expected is not None and self.size > expected:
            # More bytes than the uploader declared. Named for what went wrong rather
            # than as too_large, since the declared size is also the size limit.
            raise UploadError(
                "size_mismatch",
                details={
                    "reason": "sizeMismatch",
                    "expectedSize": expected,
                    "receivedSize": self.size,
                },
            )
        limit = self._constraints.max_size
        if limit is not None and self.size > limit:
            # Enforced here and not on Content-Length, because a chunked upload has no
            # Content-Length and a lying one is trivial to send.
            raise UploadError(
                "too_large",
                f"more than {limit} bytes",
                details={"reason": "maxSizeExceeded", "maxSize": limit, "receivedSize": self.size},
            )
        self.hasher.update(chunk)
        # Each read is forwarded as it arrives. Joining small reads into larger pieces
        # was tried in 0.5.0 development and measured: no throughput gain with 16 KiB
        # client frames, and peak memory 30 to 45 percent higher, because the queue
        # bounds items rather than bytes.
        await self._queue.put(bytes(chunk))

    def verify(self) -> None:
        """Check the complete file against what the uploader declared, if anything."""
        expected_size = self._constraints.expected_size
        if expected_size is not None and self.size != expected_size:
            raise UploadError(
                "size_mismatch",
                details={
                    "reason": "sizeMismatch",
                    "expectedSize": expected_size,
                    "actualSize": self.size,
                },
            )
        expected = self._constraints.expected_sha256
        actual = self.hasher.hexdigest()
        if expected is not None and actual != expected:
            raise UploadError(
                "digest_mismatch",
                details={
                    "reason": "digestMismatch",
                    "expected": dict(_digest(expected)),
                    "actual": dict(_digest(actual)),
                },
            )

    async def on_finish_async(self) -> None:
        # Only mark the part complete. The end-of-body signal is sent by the pump once
        # the whole request has been parsed, so the upstream request stays open until
        # it is known that nothing objectionable followed the file part. A backend then
        # only commits an upload whose entire request validated.
        self._announce()
        self.done = True


class _Meter:
    """How long the pump has waited on the client, shared with the watchdog.

    The pump only stamps the clock around each read, which costs nothing measurable.
    The checking happens in ``_watch`` once a second, off the hot path. A timeout armed
    around every read cost a third of single-upload throughput.
    """

    __slots__ = ("waiting_since", "waited", "window_bytes", "body_done")

    def __init__(self) -> None:
        self.waiting_since: float | None = None
        self.waited = 0.0
        self.window_bytes = 0
        self.body_done = False


async def _watch(
    pump: asyncio.Task[None],
    meter: _Meter,
    state: _PumpState,
    *,
    stall_timeout: float | None,
    upload_timeout: float | None,
) -> None:
    """Stop an upload whose client is too slow or that has run too long.

    Only time spent waiting for the client counts toward the stall limit. Time the pump
    spends blocked because a slow backend is applying backpressure is not the client's
    fault and is not charged to it.
    """
    loop = asyncio.get_running_loop()
    deadline = None if upload_timeout is None else loop.time() + upload_timeout
    tick = 1.0 if stall_timeout is None else min(1.0, stall_timeout / 4)
    while not pump.done():
        await asyncio.sleep(tick)
        now = loop.time()
        code = None
        if deadline is not None and now >= deadline:
            code = "upload_timeout"
        elif stall_timeout is not None:
            since = meter.waiting_since
            waited = meter.waited + (now - since if since is not None else 0.0)
            if waited >= stall_timeout:
                code = "too_slow"
        if code is not None and not pump.done():
            state.error = UploadError(code, "client did not deliver the body in time")
            pump.cancel()
            return


class _MultipartBody:
    """A form body: framed, parsed incrementally, the file part going to the target."""

    def __init__(self, headers: Mapping[str, str], field: str, target: _FileTarget) -> None:
        self._parser = StreamingFormDataParser(headers=headers, strict=True)
        self._parser.register(field, target)
        self._framer = Framer(boundary_of(headers), max_preamble=_MAX_PREAMBLE)
        self._target = target
        self._fed = 0

    async def feed(self, chunk: bytes) -> bool:
        """Hand one read to the parser. True when the rest of the body can be ignored."""
        try:
            body = self._framer.feed(chunk)
        except ValueError as exc:
            raise UploadError("bad_multipart", str(exc)) from None
        pieces = (
            (body,)
            if len(body) <= _FEED_SLICE
            else [body[at : at + _FEED_SLICE] for at in range(0, len(body), _FEED_SLICE)]
        )
        for piece in pieces:
            await self._parser.adata_received(piece)
            self._fed += len(piece)
            if self._fed - self._target.size > _MAX_OVERHEAD:
                raise UploadError("part_headers_too_large", "too much non-file data")
        # Once the close delimiter has passed, everything the parser needs has arrived.
        # The rest is ignorable by definition, so stop reading it rather than refuse a
        # complete upload.
        return self._framer.closed and self._framer.epilogue > _MAX_EPILOGUE

    async def end(self) -> None:
        if self._target.parts == 0:
            raise UploadError("missing_file", "no file part in the body")
        if not self._target.done:
            raise UploadError("truncated", "body ended before the file part was closed")


class _RawBody:
    """A raw body: the bytes are the file. Its name and type came from the headers and
    were checked before the ticket was spent, so there is nothing to parse."""

    def __init__(self, target: _FileTarget, filename: str, media_type: str) -> None:
        target.set_multipart_filename(filename)
        target.set_multipart_content_type(media_type)
        target.parts = 1
        self._target = target

    async def feed(self, chunk: bytes) -> bool:
        await self._target.on_data_received_async(chunk)
        return False

    async def end(self) -> None:
        await self._target.on_finish_async()


async def _pump(
    request: Request,
    body: _MultipartBody | _RawBody,
    target: _FileTarget,
    queue: asyncio.Queue[Any],
    state: _PumpState,
    meter: _Meter,
    stall_min_bytes: int,
) -> None:
    """Read the request body and feed it on. Any failure is recorded on ``state`` and
    signalled into the queue so whatever reads the stream stops."""
    loop = asyncio.get_running_loop()
    try:
        stream = request.stream().__aiter__()
        while True:
            meter.waiting_since = loop.time()
            try:
                chunk = await stream.__anext__()
            except StopAsyncIteration:
                meter.body_done = True
                break
            finally:
                meter.waited += loop.time() - meter.waiting_since
                meter.waiting_since = None
            meter.window_bytes += len(chunk)
            if meter.window_bytes >= stall_min_bytes:
                meter.window_bytes = 0
                meter.waited = 0.0
            if await body.feed(chunk):
                break
        await body.end()
        # The end of the stream has not been signalled yet, so a mismatch here leaves
        # the backend with an incomplete request, or the sink with UploadAborted, rather
        # than a committed file.
        target.verify()
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
            # Wake the reader if it is waiting on an empty queue. If the queue is full
            # the reader is not waiting, and it checks state.error on its next read.
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(_Abort(state.error))
            state.started.set()


class _Stream:
    """The file's bytes on their way out of the forwarding queue, for the backend
    request or the sink alike.

    ``read`` returns None only at the end the pump signals after the whole request
    validated. On any failure it raises the upload's error instead, on this read and
    every later one, even if valid chunks are still queued. A consumer therefore
    cannot mistake a failed upload for a finished one.
    """

    def __init__(self, queue: asyncio.Queue[Any], state: _PumpState) -> None:
        self._queue = queue
        self._state = state
        self.ended = False

    def raise_for_error(self) -> None:
        if self._state.error is not None:
            raise self._state.error

    async def read(self) -> bytes | None:
        if self.ended:
            return None
        self.raise_for_error()
        item = await self._queue.get()
        if isinstance(item, _Abort):
            raise item.exc
        self.raise_for_error()
        if item is _DONE:
            self.ended = True
            return None
        data: bytes = item
        return data

    async def read_for_sink(self) -> bytes | None:
        try:
            return await self.read()
        except UploadError as exc:
            raise UploadAborted(exc.code, exc.details) from None


async def _stop_reading(pump: asyncio.Task[None]) -> bool:
    """Collect the pump if it has finished, or stop it if it is still reading. True
    when it had to be stopped, which means the destination quit before the end."""
    if pump.done():
        if not pump.cancelled():
            await pump
        return False
    pump.cancel()
    with contextlib.suppress(BaseException):
        await pump
    return True


def _upstream(
    dest: Destination,
    filename: str,
    media_type: str,
    stream: _Stream,
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
        while (chunk := await stream.read()) is not None:
            yield chunk
        if epilogue:
            yield epilogue

    return headers, body()


# ----- small helpers -------------------------------------------------------------------


# How long past upload_timeout a redeemed record may sit before it is reported as
# abandoned. The watchdog ends a live upload at upload_timeout, so a record still
# redeemed after this belongs to a process that died.
_ABANDON_GRACE = timedelta(seconds=30)


def _digest(hex_digest: str) -> FileDigest:
    return {"algorithm": "sha-256", "value": b64url(hex_digest)}


def _expected_sha256(digest: FileDigest | str | None) -> str | None:
    if digest is None:
        return None
    if isinstance(digest, str):
        return sha256_hex(digest)
    if digest.get("algorithm", "").lower() != "sha-256":
        raise ValueError("only sha-256 digests are supported")
    return sha256_hex(digest["value"])


def _visible(record: Record, owner: str | None) -> bool:
    return owner is None or record.owner is None or record.owner == owner


async def _notify(hook: Callable[[Record], Awaitable[None]], record: Record) -> None:
    try:
        await hook(record)
    except Exception:
        logger.exception("on_complete hook failed for %s", record.id)


async def _drain(receive: Any, limit: int = 1 << 20, timeout: float = 2.0) -> None:
    """Read and discard up to ``limit`` body bytes or ``timeout`` seconds, whichever
    ends first."""
    seen = 0
    with contextlib.suppress(Exception):
        async with asyncio.timeout(timeout):
            while seen <= limit:
                message = await receive()
                if message.get("type") != "http.request":
                    return
                seen += len(message.get("body", b""))
                if not message.get("more_body", False):
                    return


def _narrow(allowed: tuple[str, ...], requested: tuple[str, ...] | None) -> tuple[str, ...]:
    """A per-ticket accept list, checked to stay inside the destination's."""
    if not requested:
        return allowed
    if not allowed:
        return requested
    destination = Constraints(accept=allowed)
    for pattern in requested:
        lowered = pattern.lower()
        inside = (
            any(a.lower() in ("*/*", lowered) for a in allowed)
            if lowered.endswith("/*")
            else destination.allows(lowered)
        )
        if not inside:
            raise ValueError(f"accept type {pattern!r} is outside the destination's list")
    return requested


_NO_STORE = {"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"}

# The page carries a live credential in its URL and a form that mutates state. It
# loads nothing external, so the policy can refuse everything but its own inline
# style (and on the form, its own script), and it must never be framed by another site.
_POLICY = "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'"

_PAGE_HEADERS = {
    **_NO_STORE,
    "Referrer-Policy": "no-referrer",
    "Content-Security-Policy": _POLICY,
    "X-Frame-Options": "DENY",
}


def _form_policy(nonce: str) -> str:
    """The upload form's policy: the same, plus its one script, identified by a nonce
    minted for this response, and requests back to this origin, which is where that
    script sends the file."""
    return (
        "default-src 'none'; style-src 'unsafe-inline'; "
        f"script-src 'nonce-{nonce}'; connect-src 'self'; form-action 'self'"
    )


def _json(body: dict[str, Any], status: int, extra: dict[str, str] | None = None) -> Response:
    return JSONResponse(body, status_code=status, headers={**_NO_STORE, **(extra or {})})


def _html(body: str, status: int, csp: str = _POLICY) -> Response:
    return HTMLResponse(
        body, status_code=status, headers={**_PAGE_HEADERS, "Content-Security-Policy": csp}
    )


def _wants_html(request: Request) -> bool:
    accept = request.headers.get("accept", "")
    return "text/html" in accept and "application/json" not in accept.split(",")[0]


def _iso(when: datetime) -> str:
    return when.isoformat(timespec="seconds").replace("+00:00", "Z")


def _checked_media_type(declared: str | None, constraints: Constraints) -> str:
    """A declared media type, reduced to ``type/subtype`` and checked. It is about to
    become a header on the backend request and a field on the record, and both the
    multipart parser and a raw request pass the value through as-is, control characters
    and all, so it has to be a real token. Then it has to be on the accept list."""
    media_type = (declared or "application/octet-stream").split(";", 1)[0].strip().lower()
    if len(media_type) > _MAX_MEDIA_TYPE_LENGTH or not _MEDIA_TYPE.fullmatch(media_type):
        raise UploadError("invalid_media_type", "declared media type is not a valid token")
    if not constraints.allows(media_type):
        raise UploadError(
            "unsupported_media_type",
            media_type,
            details={
                "reason": "mimeTypeNotAccepted",
                "mimeType": media_type,
                "accept": list(constraints.accept),
            },
        )
    return media_type


def _raw_file(request: Request, constraints: Constraints) -> tuple[str, str]:
    """The filename and media type of a raw upload, from its headers. The filename is
    the Content-Disposition header's, else the ``filename`` query parameter, else
    "upload". It is sanitized like a form part's before anything sees it."""
    media_type, _ = parse_content_type(request.headers.get("content-type"))
    checked = _checked_media_type(media_type or None, constraints)
    name = filename_from_disposition(request.headers.get("content-disposition"))
    return name or request.query_params.get("filename") or "upload", checked


def _single_type(accept: tuple[str, ...]) -> str | None:
    return accept[0].lower() if len(accept) == 1 and "*" not in accept[0] else None


__all__ = ["ERROR_STATUS", "ClaimRefused", "Issued", "StoreFull", "UploadError", "UploadGateway"]
