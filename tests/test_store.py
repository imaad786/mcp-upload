"""The stores' one hard job: exactly one redemption wins, and the record survives it."""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from mcp_upload import (
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
from mcp_upload.tickets import hash_secret

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
