"""Upload tickets: the record a ticket names, and its lifecycle.

A ticket is a random string handed to whoever will upload the file. On the server it
names a record that says where the file may go, who asked for it, and until when. The
string itself is never stored. Only its SHA-256 is, so a copy of the store does not
yield usable tickets, and lookups are by hash rather than by comparing secrets.

The record outlives its redemption on purpose. Destroying it on first use (the obvious
"get and delete" design) makes it impossible to answer "did that upload finish?" and
leaves a failed upload with no ticket to reason about. Instead the record moves through
a small set of states and stays around until its retention deadline.

Two clocks apply to every record. ``expires_at`` is when redemption stops being
allowed, minutes after issue. ``retention_until`` is when the record is deleted, hours
after issue. Expiry is derived from the clock at read time, never stored as a state.
"""

from __future__ import annotations

import enum
import hashlib
import secrets
from dataclasses import dataclass, replace
from datetime import UTC, datetime


class Status(enum.StrEnum):
    ISSUED = "issued"
    REDEEMED = "redeemed"
    COMPLETED = "completed"
    FAILED = "failed"


class RedeemError(enum.StrEnum):
    """Why a redemption did not happen. Distinguishing these is the reason the record
    survives redemption: a destroyed record cannot tell a replay from a typo."""

    NOT_FOUND = "not_found"
    EXPIRED = "expired"
    ALREADY_USED = "already_used"


@dataclass(frozen=True, slots=True)
class Constraints:
    """What the upload must satisfy. Enforced on the bytes actually received, not on
    what the request headers claim."""

    max_size: int | None = None
    accept: tuple[str, ...] = ()

    def allows(self, media_type: str | None) -> bool:
        """Match a declared media type against the accept list. Patterns are exact types
        or ``type/*``. An empty list allows anything. This is a policy check on the
        declared type, not content sniffing."""
        if not self.accept:
            return True
        if media_type is None:
            return False
        given = media_type.split(";", 1)[0].strip().lower()
        for raw in self.accept:
            pattern = raw.lower()
            if pattern in ("*/*", given):
                return True
            if pattern.endswith("/*") and given.startswith(pattern[:-1]):
                return True
        return False


@dataclass(frozen=True, slots=True)
class Outcome:
    """What happened once bytes arrived. ``error`` is one of a closed set of codes.
    Nothing a backend said in a response body is ever stored here, so a failed upload
    cannot become a way to read internal error messages."""

    size: int = 0
    filename: str | None = None
    media_type: str | None = None
    sha256: str | None = None
    error: str | None = None
    upstream_status: int | None = None


@dataclass(frozen=True, slots=True)
class Record:
    id: str
    ticket_hash: str
    destination: str
    caller: str | None
    issued_at: datetime
    expires_at: datetime
    retention_until: datetime
    constraints: Constraints
    status: Status = Status.ISSUED
    redeemed_at: datetime | None = None
    finished_at: datetime | None = None
    outcome: Outcome | None = None

    def expired(self, now: datetime) -> bool:
        return now >= self.expires_at

    def redeemed(self, now: datetime) -> Record:
        return replace(self, status=Status.REDEEMED, redeemed_at=now)

    def finished(self, status: Status, outcome: Outcome, now: datetime) -> Record:
        return replace(self, status=status, outcome=outcome, finished_at=now)


def utcnow() -> datetime:
    return datetime.now(UTC)


def new_secret() -> str:
    """256 bits from the OS CSPRNG, URL safe. Guessing is not a realistic attack at this
    size; the threats that matter are leaks (logs, transcripts), which is what the short
    expiry and single use are for."""
    return secrets.token_urlsafe(32)


def new_id() -> str:
    """Public identifier for a record. Safe to show, log, and put in a URI. It is not
    derived from the secret."""
    return "up_" + secrets.token_urlsafe(9)


def hash_secret(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()
