"""The stores' one hard job: exactly one redemption wins, and the record survives it."""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import os
import sqlite3
import uuid
from collections.abc import Awaitable, Callable
from concurrent.futures import ProcessPoolExecutor
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from mcp_upload import (
    ClaimError,
    Constraints,
    MemoryStore,
    Outcome,
    Record,
    RedeemError,
    SqliteStore,
    Status,
    Store,
    StoreFull,
)
from mcp_upload.tickets import dump_constraints, hash_secret, utcnow

NOW = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)


def make_record(secret: str = "s", *, ttl_minutes: int = 15, **overrides: object) -> Record:
    fields: dict[str, object] = dict(
        id="up_" + secret,
        ticket_hash=hash_secret(secret),
        destination="files",
        caller=None,
        issued_at=NOW,
        expires_at=NOW + timedelta(minutes=ttl_minutes),
        retention_until=NOW + timedelta(hours=24),
        constraints=Constraints(max_size=10),
    )
    fields.update(overrides)
    return Record(**fields)  # type: ignore[arg-type]


def _redis_client() -> object:
    """A real Redis when MCP_UPLOAD_TEST_REDIS_URL is set, otherwise fakeredis.

    Both are exercised on purpose. fakeredis keeps the suite runnable with no service,
    but it reimplements Lua through lupa, so CI also points this at a real server. A
    store whose whole job is atomicity should not be certified by a simulator alone.
    """
    url = os.environ.get("MCP_UPLOAD_TEST_REDIS_URL")
    if url:
        from redis.asyncio import Redis

        return Redis.from_url(url)
    fakeredis = pytest.importorskip("fakeredis", reason="fakeredis is not installed")
    return fakeredis.aioredis.FakeRedis()


@pytest.fixture(params=["memory", "sqlite", "redis"])
def store(request: pytest.FixtureRequest, tmp_path: Path) -> Store:
    if request.param == "memory":
        return MemoryStore()
    if request.param == "sqlite":
        return SqliteStore(tmp_path / "tickets.db")
    from mcp_upload.redis_store import RedisStore

    # A prefix per test so a real shared Redis cannot leak records between them.
    return RedisStore(_redis_client(), prefix=f"t{uuid.uuid4().hex}")  # type: ignore[arg-type]


async def test_fifty_concurrent_redemptions_have_one_winner(store: Store) -> None:
    record = make_record()
    await store.put(record)
    results = await asyncio.gather(
        *(store.redeem(record.ticket_hash, NOW + timedelta(seconds=1)) for _ in range(50))
    )
    winners = [r for r in results if isinstance(r, Record)]
    losers = [r for r in results if not isinstance(r, Record)]
    assert len(winners) == 1
    assert winners[0].status is Status.REDEEMED
    assert all(r is RedeemError.ALREADY_USED for r in losers)


async def test_record_survives_redemption_and_reaches_a_terminal_state(store: Store) -> None:
    record = make_record()
    await store.put(record)
    redeemed = await store.redeem(record.ticket_hash, NOW)
    assert isinstance(redeemed, Record)
    assert redeemed.redeemed_at == NOW

    outcome = Outcome(size=7, filename="a.txt", media_type="text/plain", sha256="ab")
    done = await store.finish(record.id, Status.COMPLETED, outcome, NOW + timedelta(seconds=2))
    assert done is not None
    assert done.status is Status.COMPLETED
    assert done.outcome == outcome
    assert done.finished_at == NOW + timedelta(seconds=2)

    again = await store.get(record.id)
    assert again == done
    assert await store.get_by_hash(record.ticket_hash) == done


async def test_fifty_concurrent_claims_have_one_winner(store: Store) -> None:
    record = make_record(owner="alice", constraints=Constraints(max_size=10, expected_size=7))
    await store.put(record)
    assert await store.claim(record.id, NOW) is ClaimError.NOT_COMPLETED
    await store.redeem(record.ticket_hash, NOW)
    outcome = Outcome(size=7, sha256="ab", details={"note": "kept"})
    await store.finish(record.id, Status.COMPLETED, outcome, NOW)

    results = await asyncio.gather(*(store.claim(record.id, NOW) for _ in range(50)))
    winners = [r for r in results if isinstance(r, Record)]
    assert len(winners) == 1
    assert winners[0].status is Status.CLAIMED
    assert winners[0].owner == "alice"
    assert winners[0].constraints.expected_size == 7
    assert winners[0].outcome is not None and winners[0].outcome.details == {"note": "kept"}
    assert all(r is ClaimError.ALREADY_CLAIMED for r in results if not isinstance(r, Record))
    assert await store.claim("up_missing", NOW) is ClaimError.NOT_FOUND


async def test_finish_does_not_resurrect_a_swept_record(store: Store) -> None:
    record = make_record()
    await store.put(record)
    await store.redeem(record.ticket_hash, NOW)
    await store.sweep(NOW + timedelta(days=2))
    assert await store.finish(record.id, Status.COMPLETED, Outcome(size=1), NOW) is None
    assert await store.get(record.id) is None


async def test_expired_ticket_is_refused_and_distinguished(store: Store) -> None:
    record = make_record()
    await store.put(record)
    late = NOW + timedelta(minutes=16)
    assert await store.redeem(record.ticket_hash, late) is RedeemError.EXPIRED
    assert await store.redeem(hash_secret("nope"), NOW) is RedeemError.NOT_FOUND
    assert isinstance(await store.redeem(record.ticket_hash, NOW), Record)
    assert await store.redeem(record.ticket_hash, NOW) is RedeemError.ALREADY_USED


async def test_sweep_deletes_only_past_retention(store: Store) -> None:
    keep = make_record("keep")
    drop = make_record("drop", retention_until=NOW + timedelta(hours=1))
    await store.put(keep)
    await store.put(drop)
    assert await store.sweep(NOW + timedelta(minutes=30)) == 0
    assert await store.sweep(NOW + timedelta(hours=1)) == 1
    assert await store.get(keep.id) is not None
    assert await store.get(drop.id) is None


async def test_memory_store_refuses_beyond_its_cap() -> None:
    store = MemoryStore(max_records=2)
    await store.put(make_record("a"))
    await store.put(make_record("b"))
    with pytest.raises(StoreFull):
        await store.put(make_record("c"))


async def test_sqlite_store_refuses_beyond_its_cap(tmp_path: Path) -> None:
    store = SqliteStore(tmp_path / "t.db", max_records=2)
    await store.put(make_record("a"))
    await store.put(make_record("b"))
    with pytest.raises(StoreFull):
        await store.put(make_record("c"))


async def test_sqlite_store_sweeps_before_refusing(tmp_path: Path) -> None:
    store = SqliteStore(tmp_path / "t.db", max_records=1)
    old = make_record("old", retention_until=NOW - timedelta(seconds=1))
    await store.put(old)
    fresh = make_record("fresh", issued_at=NOW)
    await store.put(fresh)
    assert await store.get(old.id) is None
    assert await store.get(fresh.id) is not None


async def test_memory_store_sweeps_before_refusing() -> None:
    store = MemoryStore(max_records=1)
    old = make_record("old", retention_until=NOW - timedelta(seconds=1))
    await store.put(old)
    fresh = make_record("fresh", issued_at=NOW)
    await store.put(fresh)
    assert await store.get(old.id) is None
    assert await store.get(fresh.id) is not None


# ----- SQLite: connections and the record cap -----------------------------------------


async def test_sqlite_aclose_closes_every_connection_and_the_store_reopens(
    tmp_path: Path,
) -> None:
    store = SqliteStore(tmp_path / "t.db")
    records = [make_record(str(i)) for i in range(20)]
    await asyncio.gather(*(store.put(r) for r in records))
    assert (tmp_path / "t.db-wal").exists()
    await store.aclose()
    # SQLite removes the write-ahead log when the last connection to the file closes,
    # so its absence shows that no connection was left open.
    assert not (tmp_path / "t.db-wal").exists()
    assert await store.get(records[0].id) == records[0]
    await store.aclose()


async def test_sqlite_cap_counts_rows_in_a_database_written_by_040(tmp_path: Path) -> None:
    # A file written by 0.4.0 has no counter. Opening it must seed the counter from the
    # rows already there, or the cap would let the table grow past it.
    path = tmp_path / "old.db"
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.execute(
            "CREATE TABLE tickets (id TEXT PRIMARY KEY, ticket_hash TEXT NOT NULL UNIQUE,"
            " destination TEXT NOT NULL, caller TEXT, issued_at REAL NOT NULL,"
            " expires_at REAL NOT NULL, retention_until REAL NOT NULL,"
            " constraints TEXT NOT NULL, status TEXT NOT NULL, redeemed_at REAL,"
            " finished_at REAL, outcome TEXT, owner TEXT, claimed_at REAL)"
        )
        for secret in ("a", "b"):
            r = make_record(secret)
            conn.execute(
                "INSERT INTO tickets (id, ticket_hash, destination, issued_at, expires_at,"
                " retention_until, constraints, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    r.id,
                    r.ticket_hash,
                    r.destination,
                    r.issued_at.timestamp(),
                    r.expires_at.timestamp(),
                    r.retention_until.timestamp(),
                    '{"max_size": 10, "accept": []}',
                    "issued",
                ),
            )
    store = SqliteStore(path, max_records=3)
    assert await store.get(make_record("a").id) == make_record("a")
    await store.put(make_record("c"))
    with pytest.raises(StoreFull):
        await store.put(make_record("d"))
    await store.aclose()


async def test_sqlite_cap_holds_across_store_instances(tmp_path: Path) -> None:
    # Two instances on one file stand in for two processes: each has its own
    # connections, and the cap is kept by the database rather than by either of them.
    one = SqliteStore(tmp_path / "t.db", max_records=3)
    two = SqliteStore(tmp_path / "t.db", max_records=3)
    await one.put(make_record("a"))
    await two.put(make_record("b"))
    await one.put(make_record("c"))
    with pytest.raises(StoreFull):
        await two.put(make_record("d"))
    assert await one.sweep(NOW + timedelta(days=2)) == 3
    await two.put(make_record("d"))
    await one.aclose()
    await two.aclose()


def _redeem_in_another_process(path: str, hashes: list[str], barrier: Any) -> list[str]:
    async def run() -> list[str]:
        store = SqliteStore(path, max_records=None)
        barrier.wait()
        wins: list[str] = []
        for ticket_hash in hashes:
            results = await asyncio.gather(*(store.redeem(ticket_hash, NOW) for _ in range(20)))
            wins += [ticket_hash for r in results if isinstance(r, Record)]
        await store.aclose()
        return wins

    return asyncio.run(run())


async def test_sqlite_redemption_has_one_winner_across_processes(tmp_path: Path) -> None:
    path = str(tmp_path / "t.db")
    store = SqliteStore(path)
    records = [make_record(f"race{i}") for i in range(10)]
    for record in records:
        await store.put(record)
    hashes = [r.ticket_hash for r in records]
    ctx = multiprocessing.get_context("spawn")
    loop = asyncio.get_running_loop()
    with ctx.Manager() as manager, ProcessPoolExecutor(3, mp_context=ctx) as pool:
        barrier = manager.Barrier(3)
        runs = await asyncio.gather(
            *(
                loop.run_in_executor(pool, _redeem_in_another_process, path, hashes, barrier)
                for _ in range(3)
            )
        )
    assert sorted(h for run in runs for h in run) == sorted(hashes)  # one win per ticket
    for record in records:
        got = await store.get(record.id)
        assert got is not None and got.status is Status.REDEEMED
    await store.aclose()


# ----- Redis: layout, round trips, records from 0.4.0, server clock ---------------------


def _redis_store(**kwargs: Any) -> tuple[Any, str, Any]:
    """A client, a fresh prefix, and a store on both."""
    from mcp_upload.redis_store import RedisStore

    client = _redis_client()
    prefix = f"t{uuid.uuid4().hex}"
    return client, prefix, RedisStore(client, prefix=prefix, **kwargs)  # type: ignore[arg-type]


async def _put_like_040(client: Any, prefix: str, record: Record) -> None:
    """Write a record exactly the way 0.4.0 did, independently of the current code."""
    fields = {
        "id": record.id,
        "ticket_hash": record.ticket_hash,
        "destination": record.destination,
        "issued_at": repr(record.issued_at.timestamp()),
        "expires_at": repr(record.expires_at.timestamp()),
        "retention_until": repr(record.retention_until.timestamp()),
        "constraints": json.dumps(dump_constraints(record.constraints)),
        "status": record.status.value,
    }
    if record.owner is not None:
        fields["owner"] = record.owner
    ttl = int((record.retention_until - record.issued_at).total_seconds())
    pipe = client.pipeline(transaction=True)
    pipe.hset(f"{prefix}:rec:{record.id}", mapping=fields)
    pipe.expire(f"{prefix}:rec:{record.id}", ttl)
    pipe.set(f"{prefix}:hash:{record.ticket_hash}", record.id, ex=ttl)
    await pipe.execute()


async def test_redis_serves_records_written_by_040() -> None:
    from mcp_upload.redis_store import RedisStore

    client, prefix, store = _redis_store()
    record = make_record("old", owner="alice")
    other = make_record("other", retention_until=NOW + timedelta(hours=1))
    await _put_like_040(client, prefix, record)
    await _put_like_040(client, prefix, other)

    assert await store.get_by_hash(record.ticket_hash) == record
    assert await store.get(record.id) == record
    results = await asyncio.gather(*(store.redeem(record.ticket_hash, NOW) for _ in range(50)))
    assert sum(isinstance(r, Record) for r in results) == 1
    assert all(r is RedeemError.ALREADY_USED for r in results if not isinstance(r, Record))
    late = NOW + timedelta(minutes=16)
    assert await store.redeem(other.ticket_hash, late) is RedeemError.EXPIRED

    outcome = Outcome(size=3, sha256="ab")
    replica = RedisStore(client, prefix=prefix)
    done = await replica.finish(record.id, Status.COMPLETED, outcome, NOW)
    assert done is not None and done.status is Status.COMPLETED and done.outcome == outcome
    claims = await asyncio.gather(*(store.claim(record.id, NOW) for _ in range(20)))
    assert sum(isinstance(c, Record) for c in claims) == 1

    assert await store.sweep(NOW + timedelta(hours=2)) == 1
    assert await store.get(other.id) is None
    assert await store.get_by_hash(other.ticket_hash) is None
    assert await store.finish(other.id, Status.COMPLETED, outcome, NOW) is None
    assert await client.exists(f"{prefix}:rec:{other.id}") == 0
    assert await client.exists(f"{prefix}:hash:{other.ticket_hash}") == 0
    assert (await store.get(record.id)).status is Status.CLAIMED


async def test_redis_legacy_layout_writes_what_040_reads() -> None:
    from mcp_upload.redis_store import RedisStore

    client, prefix, store = _redis_store(legacy_layout=True)
    record = make_record("compat")
    await store.put(record)
    # How 0.4.0 finds a record: the hash key names the id, and the id key is the record.
    assert await client.get(f"{prefix}:hash:{record.ticket_hash}") == record.id.encode()
    fields = await client.hgetall(f"{prefix}:rec:{record.id}")
    assert fields[b"status"] == b"issued"
    assert await client.ttl(f"{prefix}:rec:{record.id}") > 86000
    # The current layout's reader serves it through the fallback.
    redeemed = await RedisStore(client, prefix=prefix).redeem(record.ticket_hash, NOW)
    assert isinstance(redeemed, Record)


async def test_redis_ttl_is_the_retention_window_relative_to_issue() -> None:
    client, prefix, store = _redis_store()
    # NOW is in the past by the server's clock. A TTL computed as an absolute deadline
    # from it would expire these keys as they were written.
    record = make_record("ttl")
    await store.put(record)
    for key in (f"{prefix}:tk:{record.ticket_hash}", f"{prefix}:id:{record.id}"):
        assert 86000 < await client.ttl(key) <= 86400


async def test_redis_upload_path_is_one_round_trip_per_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from redis.asyncio.connection import AbstractConnection

    from mcp_upload.redis_store import RedisStore

    client, prefix, store = _redis_store()
    warm, record = make_record("warm"), make_record("rt")
    for r in (warm, record):
        await store.put(r)
    # Connection setup and script loading are not the steady state.
    await store.redeem(warm.ticket_hash, NOW)
    await store.finish(warm.id, Status.COMPLETED, Outcome(size=1), NOW)
    await store.claim(warm.id, NOW)

    sent = 0
    original = AbstractConnection.send_packed_command

    async def counted(conn: Any, command: Any, check_health: bool = True) -> None:
        nonlocal sent
        sent += 1
        await original(conn, command, check_health)

    monkeypatch.setattr(AbstractConnection, "send_packed_command", counted)
    calls: list[Callable[[], Awaitable[object]]] = [
        lambda: store.get_by_hash(record.ticket_hash),
        lambda: store.redeem(record.ticket_hash, NOW),
        lambda: store.finish(record.id, Status.COMPLETED, Outcome(size=1), NOW),
        lambda: store.claim(record.id, NOW),
    ]
    for call in calls:
        sent = 0
        assert isinstance(await call(), Record)
        assert sent == 1
    # A replica that never saw the record pays one more round trip to find it by id.
    sent = 0
    assert await RedisStore(client, prefix=prefix).get(record.id) is not None
    assert sent == 2


async def test_redis_a_wrong_id_mapping_writes_nothing() -> None:
    _, _, store = _redis_store()
    a, b = make_record("a"), make_record("b")
    await store.put(a)
    await store.put(b)
    store._remember(a.id, b.ticket_hash)  # a mapping that points at the wrong record
    assert await store.finish(a.id, Status.COMPLETED, Outcome(size=1), NOW) is None
    assert await store.claim(a.id, NOW) is ClaimError.NOT_FOUND
    assert await store.get(a.id) is None
    assert await store.get(b.id) == b


async def test_redis_server_clock_decides_expiry() -> None:
    real_now = utcnow()
    day = timedelta(days=1)
    # Expired by the server's clock, though the caller's clock is behind and says not.
    stale = make_record(
        "stale",
        issued_at=real_now - timedelta(hours=1),
        expires_at=real_now - timedelta(minutes=1),
        retention_until=real_now + day,
    )
    # Valid by the server's clock, though the caller's clock is ahead and says expired.
    fresh = make_record(
        "fresh",
        issued_at=real_now,
        expires_at=real_now + timedelta(hours=1),
        retention_until=real_now + day,
    )
    behind, ahead = real_now - timedelta(minutes=30), real_now + timedelta(hours=2)

    _, _, plain = _redis_store()
    _, _, served = _redis_store(server_clock=True)
    for store in (plain, served):
        await store.put(stale)
        await store.put(fresh)

    assert isinstance(await plain.redeem(stale.ticket_hash, behind), Record)
    assert await plain.redeem(fresh.ticket_hash, ahead) is RedeemError.EXPIRED

    assert await served.redeem(stale.ticket_hash, behind) is RedeemError.EXPIRED
    won = await served.redeem(fresh.ticket_hash, ahead)
    assert isinstance(won, Record)
    assert won.redeemed_at == ahead  # the caller's time is what gets recorded
