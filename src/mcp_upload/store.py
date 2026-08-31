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
from typing import Any, Protocol

from .tickets import Constraints, Outcome, Record, RedeemError, Status


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
    outcome         TEXT
);
CREATE INDEX IF NOT EXISTS tickets_retention ON tickets (retention_until);
"""


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

    def __init__(self, path: str | PathLike[str], *, timeout: float = 10.0) -> None:
        self._path = str(path)
        self._timeout = timeout
        with closing(self._connect()) as conn:
            conn.executescript(_SCHEMA)

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
            conn.execute(
                "INSERT INTO tickets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    record.id,
                    record.ticket_hash,
                    record.destination,
                    record.caller,
                    record.issued_at.timestamp(),
                    record.expires_at.timestamp(),
                    record.retention_until.timestamp(),
                    _dump_constraints(record.constraints),
                    record.status.value,
                    None,
                    None,
                    None,
                ),
            )

    async def get(self, record_id: str) -> Record | None:
        return await asyncio.to_thread(self._select, "id", record_id)

    async def get_by_hash(self, ticket_hash: str) -> Record | None:
        return await asyncio.to_thread(self._select, "ticket_hash", ticket_hash)

    def _select(self, column: str, value: str) -> Record | None:
        with closing(self._connect()) as conn:
            row = conn.execute(f"SELECT * FROM tickets WHERE {column} = ?", (value,)).fetchone()
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
                (status.value, _dump_outcome(outcome), now.timestamp(), record_id),
            )
            row = conn.execute("SELECT * FROM tickets WHERE id = ?", (record_id,)).fetchone()
        return None if row is None else _row_to_record(row)

    async def sweep(self, now: datetime) -> int:
        return await asyncio.to_thread(self._sweep, now)

    def _sweep(self, now: datetime) -> int:
        with closing(self._connect()) as conn:
            cur = conn.execute("DELETE FROM tickets WHERE retention_until <= ?", (now.timestamp(),))
            return int(cur.rowcount)


def _dump_constraints(c: Constraints) -> str:
    return json.dumps({"max_size": c.max_size, "accept": list(c.accept)})


def _load_constraints(text: str) -> Constraints:
    data: dict[str, Any] = json.loads(text)
    return Constraints(max_size=data.get("max_size"), accept=tuple(data.get("accept") or ()))


def _dump_outcome(o: Outcome) -> str:
    return json.dumps(
        {
            "size": o.size,
            "filename": o.filename,
            "media_type": o.media_type,
            "sha256": o.sha256,
            "error": o.error,
            "upstream_status": o.upstream_status,
        }
    )


def _load_outcome(text: str | None) -> Outcome | None:
    if text is None:
        return None
    data: dict[str, Any] = json.loads(text)
    return Outcome(**data)


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
        constraints=_load_constraints(row["constraints"]),
        status=Status(row["status"]),
        redeemed_at=_ts(row["redeemed_at"]),
        finished_at=_ts(row["finished_at"]),
        outcome=_load_outcome(row["outcome"]),
    )
