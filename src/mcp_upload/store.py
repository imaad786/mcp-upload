"""Ticket stores.

The store is the one place where single use is enforced, so its ``redeem`` has one job:
flip a record from issued to redeemed exactly once, even when several requests present
the same ticket at the same moment. Each backend does that with the primitive its
engine offers. Both backends here were checked with fifty concurrent redemptions of one
ticket and exactly one winner.

Redemption flips a status field rather than deleting the record. That is just as atomic
and it keeps the record around to hold the outcome, which "get and delete" throws away.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from os import PathLike
from typing import Protocol

from .tickets import (
    ClaimError,
    Outcome,
    Record,
    RedeemError,
    Status,
    dump_constraints,
    dump_outcome,
    load_constraints,
    load_outcome,
)


class StoreFull(Exception):
    """The in-memory store refused a new record. Ticket issuance is a small allocation
    that outlives redemption, so an unbounded store is a way to run a server out of
    memory by asking for tickets. The cap makes that a 503 instead."""


class Store(Protocol):
    async def put(self, record: Record) -> None: ...

    async def get(self, record_id: str) -> Record | None: ...

    async def get_by_hash(self, ticket_hash: str) -> Record | None: ...

    async def redeem(self, ticket_hash: str, now: datetime) -> Record | RedeemError: ...

    async def finish(
        self, record_id: str, status: Status, outcome: Outcome, now: datetime
    ) -> Record | None: ...

    async def claim(self, record_id: str, now: datetime) -> Record | ClaimError: ...

    async def sweep(self, now: datetime) -> int: ...


class MemoryStore:
    """Single-process store for development and tests.

    Correct under concurrency only because ``redeem`` does its read, check and write
    with no ``await`` in between. The event loop cannot switch coroutines inside a
    synchronous block, so no second redemption can interleave. Putting an ``await``
    anywhere between the check and the write would make every concurrent redemption
    win. This is the easiest bug to introduce when an async store interface makes
    ``await self.get(); ...; await self.set()`` look natural.
    """

    def __init__(self, *, max_records: int = 10_000) -> None:
        self._max = max_records
        self._by_id: dict[str, Record] = {}
        self._id_by_hash: dict[str, str] = {}

    async def put(self, record: Record) -> None:
        if len(self._by_id) >= self._max:
            self._sweep(record.issued_at)
            if len(self._by_id) >= self._max:
                raise StoreFull(f"memory store holds {self._max} records")
        self._by_id[record.id] = record
        self._id_by_hash[record.ticket_hash] = record.id

    async def get(self, record_id: str) -> Record | None:
        return self._by_id.get(record_id)

    async def get_by_hash(self, ticket_hash: str) -> Record | None:
        record_id = self._id_by_hash.get(ticket_hash)
        return None if record_id is None else self._by_id.get(record_id)

    async def redeem(self, ticket_hash: str, now: datetime) -> Record | RedeemError:
        # No await from here to the write. See the class docstring.
        record_id = self._id_by_hash.get(ticket_hash)
        if record_id is None:
            return RedeemError.NOT_FOUND
        record = self._by_id[record_id]
        if record.status is not Status.ISSUED:
            return RedeemError.ALREADY_USED
        if record.expired(now):
            return RedeemError.EXPIRED
        record = record.redeemed(now)
        self._by_id[record_id] = record
        return record

    async def finish(
        self, record_id: str, status: Status, outcome: Outcome, now: datetime
    ) -> Record | None:
        record = self._by_id.get(record_id)
        if record is None:
            return None
        record = record.finished(status, outcome, now)
        self._by_id[record_id] = record
        return record

    async def claim(self, record_id: str, now: datetime) -> Record | ClaimError:
        # The same rule as redeem: no await between the check and the write.
        record = self._by_id.get(record_id)
        if record is None:
            return ClaimError.NOT_FOUND
        if record.status is Status.CLAIMED:
            return ClaimError.ALREADY_CLAIMED
        if record.status is not Status.COMPLETED:
            return ClaimError.NOT_COMPLETED
        record = record.claimed(now)
        self._by_id[record_id] = record
        return record

    async def sweep(self, now: datetime) -> int:
        return self._sweep(now)

    def _sweep(self, now: datetime) -> int:
        dead = [r for r in self._by_id.values() if now >= r.retention_until]
        for record in dead:
            del self._by_id[record.id]
            self._id_by_hash.pop(record.ticket_hash, None)
        return len(dead)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS tickets (
    id              TEXT PRIMARY KEY,
    ticket_hash     TEXT NOT NULL UNIQUE,
    destination     TEXT NOT NULL,
    caller          TEXT,
    issued_at       REAL NOT NULL,
    expires_at      REAL NOT NULL,
    retention_until REAL NOT NULL,
    constraints     TEXT NOT NULL,
    status          TEXT NOT NULL,
    redeemed_at     REAL,
    finished_at     REAL,
    outcome         TEXT,
    owner           TEXT,
    claimed_at      REAL
);
CREATE INDEX IF NOT EXISTS tickets_retention ON tickets (retention_until);
"""

# Columns added after the first release. A database created by an older version gets
# them on open, so upgrading never needs a manual migration.
_ADDED_COLUMNS = {"owner": "TEXT", "claimed_at": "REAL"}

_SELECT_BY = {
    "id": "SELECT * FROM tickets WHERE id = ?",
    "ticket_hash": "SELECT * FROM tickets WHERE ticket_hash = ?",
}


class SqliteStore:
    """A store in a database file, for a single host running one or more workers.

    Redemption is one conditional UPDATE. The WHERE clause carries the status check and
    the expiry check, so the database serializes competing writers and exactly one of
    them sees ``rowcount == 1``. There is no read-then-write window to race. A losing
    request then reads the row to learn why it lost.

    Each call opens its own connection. SQLite connections are cheap and this keeps the
    store safe to call from any thread or task. Calls run in a worker thread so they do
    not block the event loop. WAL mode and a busy timeout are set so concurrent writers
    wait instead of failing with "database is locked".
    """

    def __init__(
        self, path: str | PathLike[str], *, timeout: float = 10.0, max_records: int | None = 100_000
    ) -> None:
        """``max_records`` bounds the table the same way the in-memory store is bounded.
        When it is reached, records past their retention deadline are deleted first,
        and if that frees nothing the new record is refused with ``StoreFull``. Pass
        ``None`` only if something else sweeps the table on a schedule."""
        self._path = str(path)
        self._timeout = timeout
        self._max = max_records
        with closing(self._connect()) as conn:
            conn.executescript(_SCHEMA)
            present = {row["name"] for row in conn.execute("PRAGMA table_info(tickets)")}
            for column, kind in _ADDED_COLUMNS.items():
                if column not in present:
                    conn.execute(f"ALTER TABLE tickets ADD COLUMN {column} {kind}")

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, timeout=self._timeout, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(f"PRAGMA busy_timeout={int(self._timeout * 1000)}")
        conn.row_factory = sqlite3.Row
        return conn

    async def put(self, record: Record) -> None:
        await asyncio.to_thread(self._put, record)

    def _put(self, record: Record) -> None:
        with closing(self._connect()) as conn:
            if self._max is not None:
                count = conn.execute("SELECT COUNT(*) FROM tickets").fetchone()[0]
                if count >= self._max:
                    conn.execute(
                        "DELETE FROM tickets WHERE retention_until <= ?",
                        (record.issued_at.timestamp(),),
                    )
                    count = conn.execute("SELECT COUNT(*) FROM tickets").fetchone()[0]
                    if count >= self._max:
                        raise StoreFull(f"sqlite store holds {self._max} records")
            conn.execute(
                "INSERT INTO tickets (id, ticket_hash, destination, caller, issued_at,"
                " expires_at, retention_until, constraints, status, redeemed_at,"
                " finished_at, outcome, owner, claimed_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    record.id,
                    record.ticket_hash,
                    record.destination,
                    record.caller,
                    record.issued_at.timestamp(),
                    record.expires_at.timestamp(),
                    record.retention_until.timestamp(),
                    json.dumps(dump_constraints(record.constraints)),
                    record.status.value,
                    None,
                    None,
                    None,
                    record.owner,
                    None,
                ),
            )

    async def get(self, record_id: str) -> Record | None:
        return await asyncio.to_thread(self._select, "id", record_id)

    async def get_by_hash(self, ticket_hash: str) -> Record | None:
        return await asyncio.to_thread(self._select, "ticket_hash", ticket_hash)

    def _select(self, column: str, value: str) -> Record | None:
        # One literal statement per indexed column. The value is always a parameter.
        with closing(self._connect()) as conn:
            row = conn.execute(_SELECT_BY[column], (value,)).fetchone()
        return None if row is None else _row_to_record(row)

    async def redeem(self, ticket_hash: str, now: datetime) -> Record | RedeemError:
        return await asyncio.to_thread(self._redeem, ticket_hash, now)

    def _redeem(self, ticket_hash: str, now: datetime) -> Record | RedeemError:
        ts = now.timestamp()
        with closing(self._connect()) as conn:
            cur = conn.execute(
                "UPDATE tickets SET status = ?, redeemed_at = ? "
                "WHERE ticket_hash = ? AND status = ? AND expires_at > ?",
                (Status.REDEEMED.value, ts, ticket_hash, Status.ISSUED.value, ts),
            )
            if cur.rowcount == 1:
                row = conn.execute(
                    "SELECT * FROM tickets WHERE ticket_hash = ?", (ticket_hash,)
                ).fetchone()
                return _row_to_record(row)
            row = conn.execute(
                "SELECT status FROM tickets WHERE ticket_hash = ?", (ticket_hash,)
            ).fetchone()
        if row is None:
            return RedeemError.NOT_FOUND
        if row["status"] != Status.ISSUED.value:
            return RedeemError.ALREADY_USED
        return RedeemError.EXPIRED

    async def finish(
        self, record_id: str, status: Status, outcome: Outcome, now: datetime
    ) -> Record | None:
        return await asyncio.to_thread(self._finish, record_id, status, outcome, now)

    def _finish(
        self, record_id: str, status: Status, outcome: Outcome, now: datetime
    ) -> Record | None:
        with closing(self._connect()) as conn:
            conn.execute(
                "UPDATE tickets SET status = ?, outcome = ?, finished_at = ? WHERE id = ?",
                (status.value, json.dumps(dump_outcome(outcome)), now.timestamp(), record_id),
            )
            row = conn.execute("SELECT * FROM tickets WHERE id = ?", (record_id,)).fetchone()
        return None if row is None else _row_to_record(row)

    async def claim(self, record_id: str, now: datetime) -> Record | ClaimError:
        return await asyncio.to_thread(self._claim, record_id, now)

    def _claim(self, record_id: str, now: datetime) -> Record | ClaimError:
        with closing(self._connect()) as conn:
            cur = conn.execute(
                "UPDATE tickets SET status = ?, claimed_at = ? WHERE id = ? AND status = ?",
                (Status.CLAIMED.value, now.timestamp(), record_id, Status.COMPLETED.value),
            )
            row = conn.execute("SELECT * FROM tickets WHERE id = ?", (record_id,)).fetchone()
        if row is None:
            return ClaimError.NOT_FOUND
        if cur.rowcount == 1:
            return _row_to_record(row)
        if row["status"] == Status.CLAIMED.value:
            return ClaimError.ALREADY_CLAIMED
        return ClaimError.NOT_COMPLETED

    async def sweep(self, now: datetime) -> int:
        return await asyncio.to_thread(self._sweep, now)

    def _sweep(self, now: datetime) -> int:
        with closing(self._connect()) as conn:
            cur = conn.execute("DELETE FROM tickets WHERE retention_until <= ?", (now.timestamp(),))
            return int(cur.rowcount)


def _ts(value: float | None) -> datetime | None:
    return None if value is None else datetime.fromtimestamp(value, UTC)


def _row_to_record(row: sqlite3.Row) -> Record:
    return Record(
        id=row["id"],
        ticket_hash=row["ticket_hash"],
        destination=row["destination"],
        caller=row["caller"],
        issued_at=datetime.fromtimestamp(row["issued_at"], UTC),
        expires_at=datetime.fromtimestamp(row["expires_at"], UTC),
        retention_until=datetime.fromtimestamp(row["retention_until"], UTC),
        constraints=load_constraints(json.loads(row["constraints"])),
        status=Status(row["status"]),
        redeemed_at=_ts(row["redeemed_at"]),
        finished_at=_ts(row["finished_at"]),
        outcome=None if row["outcome"] is None else load_outcome(json.loads(row["outcome"])),
        owner=row["owner"],
        claimed_at=_ts(row["claimed_at"]),
    )
