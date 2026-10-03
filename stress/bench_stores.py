"""Throughput of the ticket stores, measured directly rather than through the gateway.

    python stress/bench_stores.py --src path/to/src [--redis-url URL] [--runs 3] [--out f.json]
    python stress/bench_stores.py --src path/to/src --race [--procs 4] [--contenders 50]

``--src`` picks which copy of the library is imported, so the same script runs against
a released tree and a working tree. Each run builds fresh stores, and the report gives
the median of the runs for every number.

For each store it measures issues per second one at a time and with 64 concurrent
callers, redemptions per second with 64 concurrent callers (each on its own ticket),
and ``get_by_hash`` lookups per second with 64 concurrent callers. For Redis it also
counts round trips for the upload path (``get_by_hash``, ``redeem``, ``finish``) and for
a status lookup by id from a replica that did not issue the ticket. A round trip is one
write of commands to a socket, counted by wrapping redis-py's connection, so a pipeline
counts once. Commands are counted from the server's ``INFO commandstats``, which also
sees other clients of the same server, so that column is only exact on a quiet server.

``--race`` is a correctness check, not a benchmark: several processes share one SQLite
file and all of them redeem the same tickets at once, each with many concurrent tasks.
Every ticket must end up with exactly one winner across all processes.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import multiprocessing as mp
import os
import random
import statistics
import sys
import tempfile
import time
import uuid
from collections.abc import Awaitable, Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

CONCURRENCY = 64
NOW = datetime(2026, 10, 3, 12, 0, tzinfo=UTC)


def use_src(src: str) -> None:
    sys.path.insert(0, str(Path(src).resolve()))


def make_record(i: int, tag: str) -> Any:
    from mcp_upload import Constraints, Record
    from mcp_upload.tickets import hash_secret

    secret = f"{tag}-{i}"
    return Record(
        id=f"up_{tag}_{i}",
        ticket_hash=hash_secret(secret),
        destination="files",
        caller=None,
        issued_at=NOW,
        expires_at=NOW + timedelta(minutes=15),
        retention_until=NOW + timedelta(hours=24),
        constraints=Constraints(max_size=1000, accept=("text/plain",)),
    )


async def close_store(store: Any) -> None:
    aclose = getattr(store, "aclose", None)
    if aclose is not None:
        await aclose()


async def run_workers(items: list[Any], fn: Callable[[Any], Awaitable[Any]]) -> list[Any]:
    """Run ``fn`` over ``items`` with CONCURRENCY workers pulling from one iterator."""
    out: list[Any] = []
    source: Iterator[Any] = iter(items)

    async def worker() -> None:
        for item in source:
            out.append(await fn(item))

    await asyncio.gather(*(worker() for _ in range(CONCURRENCY)))
    return out


async def bench_store(make: Callable[[], Awaitable[Any]], n: int) -> dict[str, float]:
    from mcp_upload import Record

    result: dict[str, float] = {}

    store = await make()
    records = [make_record(i, uuid.uuid4().hex[:8]) for i in range(n)]
    start = time.perf_counter()
    for record in records:
        await store.put(record)
    result["issue_seq_per_s"] = n / (time.perf_counter() - start)
    await close_store(store)

    store = await make()
    records = [make_record(i, uuid.uuid4().hex[:8]) for i in range(n)]
    start = time.perf_counter()
    await run_workers(records, store.put)
    result["issue_conc_per_s"] = n / (time.perf_counter() - start)

    start = time.perf_counter()
    found = await run_workers([r.ticket_hash for r in records], store.get_by_hash)
    result["get_by_hash_conc_per_s"] = n / (time.perf_counter() - start)
    assert all(isinstance(r, Record) for r in found), "get_by_hash missed a record"

    later = NOW + timedelta(seconds=1)
    start = time.perf_counter()
    redeemed = await run_workers([r.ticket_hash for r in records], lambda h: store.redeem(h, later))
    result["redeem_conc_per_s"] = n / (time.perf_counter() - start)
    assert all(isinstance(r, Record) for r in redeemed), "a redemption lost"
    await close_store(store)
    return result


class RoundTrips:
    """Counts writes to redis-py connections. A pipeline is written once, so it counts
    as one round trip however many commands it carries."""

    def __init__(self) -> None:
        from redis.asyncio.connection import AbstractConnection

        self.count = 0
        self._cls = AbstractConnection
        self._orig = AbstractConnection.send_packed_command
        counter = self

        async def counted(conn: Any, command: Any, check_health: bool = True) -> None:
            counter.count += 1
            await counter._orig(conn, command, check_health)

        AbstractConnection.send_packed_command = counted  # type: ignore[method-assign]

    def restore(self) -> None:
        self._cls.send_packed_command = self._orig  # type: ignore[method-assign]


async def command_total(client: Any) -> int:
    stats = await client.info("commandstats")
    return sum(int(v["calls"]) for v in stats.values())


async def redis_round_trips(url: str, k: int = 200) -> dict[str, float]:
    from redis.asyncio import Redis

    from mcp_upload import Outcome, Record, Status
    from mcp_upload.redis_store import RedisStore

    client = Redis.from_url(url)
    prefix = f"bench{uuid.uuid4().hex}"
    store = RedisStore(client, prefix=prefix)
    tag = uuid.uuid4().hex[:8]
    records = [make_record(i, tag) for i in range(k)]
    for record in records:
        await store.put(record)
    # Warm up: connection setup and script loading are not part of the steady state.
    warm = make_record(k, tag)
    await store.put(warm)
    await store.get_by_hash(warm.ticket_hash)
    await store.redeem(warm.ticket_hash, NOW)
    await store.finish(warm.id, Status.COMPLETED, Outcome(size=1), NOW)

    out: dict[str, float] = {}
    counter = RoundTrips()
    try:
        for label, op in (
            ("get_by_hash", lambda r: store.get_by_hash(r.ticket_hash)),
            ("redeem", lambda r: store.redeem(r.ticket_hash, NOW)),
            ("finish", lambda r: store.finish(r.id, Status.COMPLETED, Outcome(size=1), NOW)),
        ):
            before_cmds = await command_total(client)
            counter.count = 0
            for record in records:
                result = await op(record)
                assert isinstance(result, Record), f"{label} failed: {result!r}"
            out[f"rt_{label}"] = counter.count / k
            after_cmds = await command_total(client)
            # The INFO call that took the "before" sample is counted in "after".
            out[f"cmds_{label}"] = (after_cmds - before_cmds - 1) / k
        out["rt_upload_path"] = out["rt_get_by_hash"] + out["rt_redeem"] + out["rt_finish"]
        out["cmds_upload_path"] = out["cmds_get_by_hash"] + out["cmds_redeem"] + out["cmds_finish"]

        # A status lookup on a replica that did not issue or handle the ticket.
        cold = RedisStore(client, prefix=prefix)
        await cold.get(records[0].id)  # warm the connection, not the record
        counter.count = 0
        for record in records[1:]:
            assert await cold.get(record.id) is not None
        out["rt_get_by_id_cold"] = counter.count / (k - 1)
    finally:
        counter.restore()
        await store.sweep(NOW + timedelta(days=30))
        await client.aclose()
    return out


async def one_run(args: argparse.Namespace, run_dir: Path) -> dict[str, dict[str, float]]:
    from mcp_upload import MemoryStore, SqliteStore

    results: dict[str, dict[str, float]] = {}

    async def memory() -> Any:
        return MemoryStore(max_records=10_000_000)

    results["memory"] = await bench_store(memory, args.n_memory)

    async def sqlite() -> Any:
        path = run_dir / f"{uuid.uuid4().hex}.db"
        return SqliteStore(path)  # the default cap, as the gateway would use it

    results["sqlite"] = await bench_store(sqlite, args.n_sqlite)

    if args.redis_url:
        from redis.asyncio import Redis

        from mcp_upload.redis_store import RedisStore

        clients: list[Any] = []
        prefixes: list[str] = []

        async def redis() -> Any:
            client = Redis.from_url(args.redis_url, max_connections=CONCURRENCY + 8)
            clients.append(client)
            prefix = f"bench{uuid.uuid4().hex}"
            prefixes.append(prefix)
            return RedisStore(client, prefix=prefix)

        try:
            results["redis"] = await bench_store(redis, args.n_redis)
        finally:
            for client, prefix in zip(clients, prefixes, strict=True):
                await RedisStore(client, prefix=prefix).sweep(NOW + timedelta(days=30))
                await client.aclose()
        results["redis"].update(await redis_round_trips(args.redis_url))
    return results


def median_of(runs: list[dict[str, dict[str, float]]]) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for store in runs[0]:
        out[store] = {
            metric: statistics.median(run[store][metric] for run in runs)
            for metric in runs[0][store]
        }
    return out


# ----- multi-process SQLite race --------------------------------------------------------


def race_child(
    src: str, db: str, hashes: list[str], contenders: int, seed: int, barrier: Any, queue: Any
) -> None:
    use_src(src)
    from mcp_upload import Record, SqliteStore

    async def main() -> list[str]:
        store = SqliteStore(db, max_records=None)
        # Every attempt at once, in an order of the process's own, so each process
        # reaches some tickets first and meets the others on the rest. In one shared
        # order, whichever process started first would win nearly every ticket.
        attempts = [h for h in hashes for _ in range(contenders)]
        random.Random(seed).shuffle(attempts)
        barrier.wait()
        results = await asyncio.gather(*(store.redeem(h, NOW) for h in attempts))
        await close_store(store)
        return [h for h, r in zip(attempts, results, strict=True) if isinstance(r, Record)]

    queue.put((os.getpid(), asyncio.run(main())))


def race(args: argparse.Namespace) -> dict[str, Any]:
    from mcp_upload import Record, SqliteStore

    with tempfile.TemporaryDirectory() as tmp:
        db = str(Path(tmp) / "race.db")
        store = SqliteStore(db, max_records=None)
        tag = uuid.uuid4().hex[:8]
        records = [make_record(i, tag) for i in range(args.tickets)]

        async def seed() -> None:
            for record in records:
                await store.put(record)
            await close_store(store)

        asyncio.run(seed())
        hashes = [r.ticket_hash for r in records]
        ctx = mp.get_context("spawn")
        barrier = ctx.Barrier(args.procs)
        queue = ctx.Queue()
        procs = [
            ctx.Process(
                target=race_child,
                args=(args.src, db, hashes, args.contenders, seed, barrier, queue),
            )
            for seed in range(args.procs)
        ]
        start = time.perf_counter()
        for p in procs:
            p.start()
        wins_by_proc = dict(queue.get(timeout=300) for _ in procs)
        for p in procs:
            p.join()
        elapsed = time.perf_counter() - start

        wins: dict[str, int] = {h: 0 for h in hashes}
        for proc_wins in wins_by_proc.values():
            for h in proc_wins:
                wins[h] += 1

        async def final_states() -> list[str]:
            check = SqliteStore(db, max_records=None)
            states = []
            for h in hashes:
                record = await check.get_by_hash(h)
                states.append(record.status.value if isinstance(record, Record) else "missing")
            await close_store(check)
            return states

        states = asyncio.run(final_states())
    return {
        "processes": args.procs,
        "contenders_per_ticket_per_process": args.contenders,
        "tickets": args.tickets,
        "redemption_attempts": args.procs * args.contenders * args.tickets,
        "tickets_with_exactly_one_winner": sum(1 for n in wins.values() if n == 1),
        "max_winners_for_one_ticket": max(wins.values()),
        "wins_per_process": {str(pid): len(w) for pid, w in wins_by_proc.items()},
        "final_states": sorted(set(states)),
        "exit_codes": [p.exitcode for p in procs],
        "seconds": round(elapsed, 2),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--src", required=True, help="the src directory to import from")
    parser.add_argument("--redis-url", default=None, help="a real Redis to measure too")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--n-memory", type=int, default=50_000)
    parser.add_argument("--n-sqlite", type=int, default=3_000)
    parser.add_argument("--n-redis", type=int, default=5_000)
    parser.add_argument("--out", default=None, help="write the JSON report here too")
    parser.add_argument("--race", action="store_true", help="run the multi-process race")
    parser.add_argument("--procs", type=int, default=4)
    parser.add_argument("--contenders", type=int, default=50)
    parser.add_argument("--tickets", type=int, default=50)
    args = parser.parse_args()
    use_src(args.src)
    import mcp_upload

    report: dict[str, Any] = {"src": mcp_upload.__file__, "version": mcp_upload.__version__}
    if args.race:
        report["race"] = race(args)
    else:
        runs = []
        with tempfile.TemporaryDirectory() as tmp:
            for _ in range(args.runs):
                runs.append(asyncio.run(one_run(args, Path(tmp))))
        report["runs"] = runs
        report["median"] = median_of(runs)
    text = json.dumps(report, indent=2)
    print(text)
    if args.out:
        Path(args.out).write_text(text + "\n")


if __name__ == "__main__":
    main()
