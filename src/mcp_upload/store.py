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
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import UTC, datetime
from os import PathLike
from typing import Protocol, TypeVar

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

_T = TypeVar("_T")


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


_SCHEMA = (
    """
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
)
""",
    "CREATE INDEX IF NOT EXISTS tickets_retention ON tickets (retention_until)",
)

# Columns added after the first release. A database created by an older version gets
# them on open, so upgrading never needs a manual migration.
_ADDED_COLUMNS = {"owner": "TEXT", "claimed_at": "REAL"}

# The row count, kept by triggers so the record cap costs one primary-key read rather
# than a COUNT(*) that walks the whole table on every insert. Triggers live in the
# database file, so every writer keeps the count right, including an older version of
# this library sharing the file during a rolling upgrade. The seed runs in the same
# transaction that creates the triggers, so no insert can land between the two unseen.
_COUNTER = (
    "CREATE TABLE IF NOT EXISTS ticket_count"
    " (id INTEGER PRIMARY KEY CHECK (id = 0), n INTEGER NOT NULL)",
    "INSERT OR IGNORE INTO ticket_count (id, n) SELECT 0, COUNT(*) FROM tickets",
    "CREATE TRIGGER IF NOT EXISTS tickets_count_insert AFTER INSERT ON tickets"
    " BEGIN UPDATE ticket_count SET n = n + 1 WHERE id = 0; END",
    "CREATE TRIGGER IF NOT EXISTS tickets_count_delete AFTER DELETE ON tickets"
    " BEGIN UPDATE ticket_count SET n = n - 1 WHERE id = 0; END",
)

_SELECT_BY = {
    "id": "SELECT * FROM tickets WHERE id = ?",
    "ticket_hash": "SELECT * FROM tickets WHERE ticket_hash = ?",
}

_INSERT = (
    "INSERT INTO tickets (id, ticket_hash, destination, caller, issued_at, expires_at,"
    " retention_until, constraints, status, redeemed_at, finished_at, outcome, owner,"
    " claimed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)
# The cap check and the insert are one statement, so two writers in different processes
# cannot both see room for one more record and both take it.
_INSERT_CAPPED = (
    "INSERT INTO tickets (id, ticket_hash, destination, caller, issued_at, expires_at,"
    " retention_until, constraints, status, redeemed_at, finished_at, outcome, owner,"
    " claimed_at) SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
    " WHERE (SELECT n FROM ticket_count WHERE id = 0) < ?"
)


class SqliteStore:
    """A store in a database file, for a single host running one or more workers.

    Redemption is one conditional UPDATE. The WHERE clause carries the status check and
    the expiry check, so the database serializes competing writers and exactly one of
    them sees ``rowcount == 1``. There is no read-then-write window to race. A losing
    request then reads the row to learn why it lost.

    Calls run on a small thread pool owned by the store, so they do not block the event
    loop. Each of its threads keeps one connection, opened on its first call with WAL
    mode and a busy timeout set once. Writes from this process take a lock first, so
    they queue for each other directly instead of colliding inside SQLite and backing
    off in its busy handler. Writers in other processes on the same host still meet in
    SQLite, where WAL mode and the busy timeout make them wait instead of failing with
    "database is locked". Connections are in autocommit mode and no transaction is
    held open between calls.

    The pool is the store's own rather than the event loop's default executor because
    more threads made it slower, not faster: on a 14-core machine, 64 concurrent
    lookups ran at about a quarter of the speed on the default pool's 18 threads that
    they reach on two, as the threads contend for the interpreter lock. SQLite
    serializes writers anyway, so extra threads only help reads, and only a little.

    Call ``aclose()`` (or ``close()`` from synchronous code) at shutdown. It waits for
    calls already running, then stops the threads and closes every connection. A call
    made afterwards starts them again, so closing is safe but not final.
    """

    def __init__(
        self,
        path: str | PathLike[str],
        *,
        timeout: float = 10.0,
        max_records: int | None = 100_000,
        threads: int = 2,
    ) -> None:
        """``max_records`` bounds the table the same way the in-memory store is bounded.
        When it is reached, records past their retention deadline are deleted first,
        and if that frees nothing the new record is refused with ``StoreFull``. The cap
        holds across every process sharing the file. Pass ``None`` only if something
        else sweeps the table on a schedule.

        ``threads`` is the size of the store's thread pool, and so the number of
        connections it keeps open."""
        self._path = str(path)
        self._timeout = timeout
        self._max = max_records
        self._threads = threads
        self._executor: ThreadPoolExecutor | None = None
        self._local = threading.local()
        self._lock = threading.Lock()
        self._write = threading.Lock()
        self._conns: list[sqlite3.Connection] = []
        with closing(self._connect()) as conn:
            # One transaction, so two processes opening an old file at the same moment
            # cannot both add a column or both seed the counter.
            conn.execute("BEGIN IMMEDIATE")
            try:
                for statement in _SCHEMA:
                    conn.execute(statement)
                present = {row["name"] for row in conn.execute("PRAGMA table_info(tickets)")}
                for column, kind in _ADDED_COLUMNS.items():
                    if column not in present:
                        conn.execute(f"ALTER TABLE tickets ADD COLUMN {column} {kind}")
                for statement in _COUNTER:
                    conn.execute(statement)
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")

    def _connect(self) -> sqlite3.Connection:
        # check_same_thread is off only so that close() can close connections opened by
        # other threads. Each connection is otherwise used by the thread that opened it.
        conn = sqlite3.connect(
            self._path, timeout=self._timeout, isolation_level=None, check_same_thread=False
        )
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(f"PRAGMA busy_timeout={int(self._timeout * 1000)}")
        conn.row_factory = sqlite3.Row
        return conn

    def _conn(self) -> sqlite3.Connection:
        """The calling thread's connection, opened on its first use. Only the store's
        own pool threads call this, and they live until ``close()``."""
        conn: sqlite3.Connection | None = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._connect()
            with self._lock:
                self._conns.append(conn)
            self._local.conn = conn
        return conn

    async def _call(self, fn: Callable[..., _T], *args: object) -> _T:
        with self._lock:
            if self._executor is None:
                self._executor = ThreadPoolExecutor(
                    self._threads, thread_name_prefix="mcp-upload-sqlite"
                )
            executor = self._executor
        return await asyncio.get_running_loop().run_in_executor(executor, fn, *args)

    def close(self) -> None:
        """Wait for running calls, stop the pool and close every connection."""
        with self._lock:
            executor, self._executor = self._executor, None
        if executor is not None:
            # Waiting means no connection is closed under a call that is using it. The
            # pool's threads are gone afterwards, and their connections with them.
            executor.shutdown(wait=True)
        with self._lock:
            conns, self._conns = self._conns, []
        for conn in conns:
            conn.close()

    async def aclose(self) -> None:
        """``close()`` without blocking the event loop: it waits for running calls,
        and closing the last connection to the file checkpoints the write-ahead log."""
        await asyncio.to_thread(self.close)

    async def put(self, record: Record) -> None:
        await self._call(self._put, record)

    def _put(self, record: Record) -> None:
        row = (
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
        )
        conn = self._conn()
        with self._write:
            if self._max is None:
                conn.execute(_INSERT, row)
                return
            if conn.execute(_INSERT_CAPPED, (*row, self._max)).rowcount == 1:
                return
            conn.execute(
                "DELETE FROM tickets WHERE retention_until <= ?", (record.issued_at.timestamp(),)
            )
            if conn.execute(_INSERT_CAPPED, (*row, self._max)).rowcount != 1:
                raise StoreFull(f"sqlite store holds {self._max} records")

    def _write_one(self, sql: str, params: tuple[object, ...]) -> sqlite3.Cursor:
        conn = self._conn()
        with self._write:
            return conn.execute(sql, params)

    async def get(self, record_id: str) -> Record | None:
        return await self._call(self._select, "id", record_id)

    async def get_by_hash(self, ticket_hash: str) -> Record | None:
        return await self._call(self._select, "ticket_hash", ticket_hash)

    def _select(self, column: str, value: str) -> Record | None:
        # One literal statement per indexed column. The value is always a parameter.
        row = self._conn().execute(_SELECT_BY[column], (value,)).fetchone()
        return None if row is None else _row_to_record(row)

    async def redeem(self, ticket_hash: str, now: datetime) -> Record | RedeemError:
        return await self._call(self._redeem, ticket_hash, now)

    def _redeem(self, ticket_hash: str, now: datetime) -> Record | RedeemError:
        ts = now.timestamp()
        cur = self._write_one(
            "UPDATE tickets SET status = ?, redeemed_at = ? "
            "WHERE ticket_hash = ? AND status = ? AND expires_at > ?",
            (Status.REDEEMED.value, ts, ticket_hash, Status.ISSUED.value, ts),
        )
        conn = self._conn()
        if cur.rowcount == 1:
            row = conn.execute(_SELECT_BY["ticket_hash"], (ticket_hash,)).fetchone()
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
        return await self._call(self._finish, record_id, status, outcome, now)

    def _finish(
        self, record_id: str, status: Status, outcome: Outcome, now: datetime
    ) -> Record | None:
        self._write_one(
            "UPDATE tickets SET status = ?, outcome = ?, finished_at = ? WHERE id = ?",
            (status.value, json.dumps(dump_outcome(outcome)), now.timestamp(), record_id),
        )
        row = self._conn().execute(_SELECT_BY["id"], (record_id,)).fetchone()
        return None if row is None else _row_to_record(row)

    async def claim(self, record_id: str, now: datetime) -> Record | ClaimError:
        return await self._call(self._claim, record_id, now)

    def _claim(self, record_id: str, now: datetime) -> Record | ClaimError:
        cur = self._write_one(
            "UPDATE tickets SET status = ?, claimed_at = ? WHERE id = ? AND status = ?",
            (Status.CLAIMED.value, now.timestamp(), record_id, Status.COMPLETED.value),
        )
        row = self._conn().execute(_SELECT_BY["id"], (record_id,)).fetchone()
        if row is None:
            return ClaimError.NOT_FOUND
        if cur.rowcount == 1:
            return _row_to_record(row)
        if row["status"] == Status.CLAIMED.value:
            return ClaimError.ALREADY_CLAIMED
        return ClaimError.NOT_COMPLETED

    async def sweep(self, now: datetime) -> int:
        return await self._call(self._sweep, now)

    def _sweep(self, now: datetime) -> int:
        cur = self._write_one("DELETE FROM tickets WHERE retention_until <= ?", (now.timestamp(),))
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
