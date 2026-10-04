"""``ask_for_upload``: a tool asks for a file through the multi-round-trip flow.

Shared by both framework adapters. Since protocol 2026-07-28 a tool cannot open a
request to the client in the middle of a call. It returns an ``InputRequiredResult``
naming what it needs, the client collects it and retries the same call with the
answers attached, and the tool body runs again from the top. Both the official SDK and
FastMCP 4 hand the tool ``ctx.request_state`` and ``ctx.input_responses`` for that, and
nothing else here depends on which framework is running.

The request is a URL-mode elicitation, so a client that only knows URL elicitation
shows a person the upload page. The result also carries the whole upload target in its
``_meta`` under ``EXTENSION_ID``, so a harness with no person present can send the
bytes itself. Both reach the same ticket.

The target sits on the result, not on the elicitation. The 2026-07-28 schema gives URL
elicitation params no ``_meta`` (nor ``elicitationId``), and the official SDK drops
unknown fields there on the wire, while a result's ``_meta`` is an open map::

    {"resultType": "input_required",
     "inputRequests": {"upload": {"method": "elicitation/create",
                                  "params": {"mode": "url", "message": "...", "url": "..."}}},
     "requestState": "...",
     "_meta": {"me.imaadkhan/upload-ticket": {"targets": {"upload": UploadTarget}}}}

``targets`` is keyed like ``inputRequests``, so a harness knows which request each
target answers.
"""

from __future__ import annotations

import re
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Protocol

from ..gateway import Issued, UploadGateway
from ..multipart import split_media_type
from ..tickets import Constraints
from ..types import EXTENSION_ID, FileDigest, UploadStatus, UploadTarget

if TYPE_CHECKING:
    from mcp_types import InputRequiredResult

# The key under which the elicitation travels in inputRequests and comes back in
# inputResponses. Any stable string works; the client echoes it.
INPUT_KEY = "upload"

DEFAULT_MESSAGE = "A file is needed. Open the link to upload it."

# The request state is the record id, and on a gateway whose URLs carry the ticket, a
# dot and the ticket. A ticket is base64url, so it never contains a dot.
_STATE = re.compile(r"^(up_[A-Za-z0-9_-]{1,64})(?:\.([A-Za-z0-9_-]{1,128}))?$")


class RoundContext(Protocol):
    """What ``ask_for_upload`` reads from a tool's context. The official SDK's
    ``Context`` and FastMCP's both have it."""

    @property
    def request_state(self) -> Any: ...

    @property
    def input_responses(self) -> Any: ...


async def ask_for_upload(
    ctx: RoundContext,
    gateway: UploadGateway,
    destination: str,
    *,
    message: str = DEFAULT_MESSAGE,
    caller: str | None = None,
    owner: str | None = None,
    ttl: timedelta | None = None,
    max_size: int | None = None,
    accept: tuple[str, ...] | None = None,
    name: str | None = None,
    media_type: str | None = None,
    expected_size: int | None = None,
    expected_digest: FileDigest | str | None = None,
    raw: bool = False,
) -> InputRequiredResult | UploadStatus:
    """Ask for a file through URL-mode elicitation, and report it on the retry.

    On the first round it mints a ticket and returns an ``InputRequiredResult``. Its
    elicitation's ``url`` is the upload page for a person. Its own ``_meta`` holds,
    under ``EXTENSION_ID``, ``{"targets": {INPUT_KEY: UploadTarget}}``: the file's URI
    with any declared name, type, size and digest, and the ``FileTransferDescriptor``
    to send the bytes with. A harness can send them without a person and then retry.

    On a retry it reports the record. While the upload has not finished (the ticket
    is unused, or bytes are still arriving) and the client did not decline or cancel,
    it returns the same request again for the same ticket, so a client that retries
    early is asked again rather than told ``issued``. ``declined`` or ``cancelled``
    report a refusal before any upload. Otherwise it returns the record's status:
    ``completed`` with the file's measured size and digest, or why it failed.

    A client may prove what it uploaded: an ``ElicitResult`` whose ``_meta`` holds
    ``{EXTENSION_ID: {"file": {"uri": ..., "digest": ...}}}`` (``size`` too, if it
    likes). Each field given must match the completed record, or the result is
    ``failed`` with ``proof_mismatch``. Without proof the record alone decides.

    ``name``, ``media_type``, ``expected_size`` and ``expected_digest`` are declared
    up front, as ``files/authorizeUpload`` declares them. A size or digest becomes the
    ticket's exact limit. A media type becomes its only accepted type unless ``accept``
    says otherwise. ``raw=True`` describes a raw-body PUT, which needs a gateway built
    with ``raw_uploads=True``.

    The record id rides in ``request_state``, which the client echoes back, so a retry
    never mints a second ticket. When the URL carries the ticket the state carries it
    too, so the same link can be sent again. That reveals nothing new to the client,
    which has the link already, and both frameworks seal request state with AES-GCM
    by default. The ticket from the state is checked against the stored hash before
    it is used, so a forged state yields nothing.

    Annotate the tool as returning ``UploadStatus | InputRequiredResult``. The
    framework derives the output schema from the status half.
    """
    parsed = _parse_state(ctx.request_state)
    if parsed is None:
        if media_type is not None:
            accept = _accept_for(media_type, accept)
        issued = await gateway.issue(
            destination,
            caller=caller,
            owner=owner,
            ttl=ttl,
            max_size=max_size,
            accept=accept,
            expected_size=expected_size,
            expected_digest=expected_digest,
        )
        return _ask(gateway, issued, message, name=name, media_type=media_type, raw=raw)

    record_id, secret = parsed
    status = await gateway.status(record_id, owner=owner)
    answer = (ctx.input_responses or {}).get(INPUT_KEY)
    action = _field(answer, "action")
    state = status["status"]
    if state == "issued" and action in ("decline", "cancel"):
        # The user did not open the link. The ticket stays valid until it expires,
        # which is harmless: nobody has it but the client that just refused it.
        status["status"] = "declined" if action == "decline" else "cancelled"
        return status
    if state in ("issued", "redeemed"):
        # Too early: nothing uploaded yet, or bytes still arriving. Ask again for the
        # same ticket. The record decides this, whatever proof the client sent.
        again = await gateway.resume(record_id, secret, owner=owner)
        if again is not None:
            return _ask(gateway, again, message, name=name, media_type=media_type, raw=raw)
        return status
    if state in ("completed", "claimed"):
        wrong = _proof_mismatch(status, _proof(answer))
        if wrong:
            return {
                "id": record_id,
                "status": "failed",
                "error": "proof_mismatch",
                "details": {"reason": "proofMismatch", "fields": wrong},
            }
    return status


def target_of(
    gateway: UploadGateway,
    issued: Issued,
    *,
    name: str | None = None,
    media_type: str | None = None,
    raw: bool = False,
) -> UploadTarget:
    """The machine-readable target for ``issued``: what ``files/authorizeUpload``
    returns, and what ``ask_for_upload`` puts in its result's ``_meta``."""
    described = gateway.describe(issued, raw=raw, media_type=media_type if raw else None)
    file = described["file"]
    if name is not None:
        file["name"] = name
    if media_type is not None:
        file["mimeType"] = media_type
    return {"file": file, "upload": described["upload"]}


def _ask(
    gateway: UploadGateway,
    issued: Issued,
    message: str,
    *,
    name: str | None,
    media_type: str | None,
    raw: bool,
) -> InputRequiredResult:
    from mcp_types import ElicitRequest, ElicitRequestURLParams, InputRequiredResult

    target = target_of(gateway, issued, name=name, media_type=media_type, raw=raw)
    record_id = issued.record.id
    params = ElicitRequestURLParams(
        mode="url", message=message, url=issued.upload_url, elicitation_id=record_id
    )
    return InputRequiredResult(
        input_requests={INPUT_KEY: ElicitRequest(params=params)},
        request_state=f"{record_id}.{issued.secret}" if issued.secret else record_id,
        _meta={EXTENSION_ID: {"targets": {INPUT_KEY: dict(target)}}},
    )


def _parse_state(state: Any) -> tuple[str, str | None] | None:
    if not isinstance(state, str):
        return None
    match = _STATE.match(state)
    if match is None:
        return None
    return match.group(1), match.group(2)


def _accept_for(media_type: str, accept: tuple[str, ...] | None) -> tuple[str, ...]:
    parsed = split_media_type(media_type)
    if parsed is None or "*" in parsed[0]:
        raise ValueError(f"media_type {media_type!r} is not a media type")
    if not accept:
        return (parsed[0],)
    if not Constraints(accept=accept).allows(parsed[0]):
        raise ValueError(f"media_type {media_type!r} is outside accept")
    return accept


def _field(value: Any, name: str) -> Any:
    if isinstance(value, dict):
        return value.get(name)
    return getattr(value, name, None)


def _proof(answer: Any) -> dict[str, Any] | None:
    """The file a client says it uploaded, from its answer's ``_meta``, if any."""
    meta = answer.get("_meta") if isinstance(answer, dict) else getattr(answer, "meta", None)
    entry = meta.get(EXTENSION_ID) if isinstance(meta, dict) else None
    file = entry.get("file") if isinstance(entry, dict) else None
    if not isinstance(file, dict) or not {"uri", "digest", "size"} & file.keys():
        return None
    return file


def _proof_mismatch(status: UploadStatus, proof: dict[str, Any] | None) -> list[str]:
    """The fields of ``proof`` that disagree with the completed record."""
    if proof is None:
        return []
    actual: dict[str, Any] = dict(status.get("file") or {})
    wrong = []
    if "uri" in proof and proof["uri"] != actual.get("uri"):
        wrong.append("uri")
    if "digest" in proof:
        claimed, measured = proof["digest"], actual.get("digest")
        if (
            not isinstance(claimed, dict)
            or measured is None
            or str(claimed.get("algorithm", "")).lower() != measured["algorithm"]
            or claimed.get("value") != measured["value"]
        ):
            wrong.append("digest")
    if "size" in proof and proof["size"] != actual.get("size"):
        wrong.append("size")
    return wrong


__all__ = ["DEFAULT_MESSAGE", "INPUT_KEY", "RoundContext", "ask_for_upload", "target_of"]
