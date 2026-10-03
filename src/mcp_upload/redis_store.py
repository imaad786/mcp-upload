"""A ticket store in Redis, for a server that runs on more than one machine.

The other two stores are correct only within one process (``MemoryStore``) or one
host (``SqliteStore``). Neither survives a load balancer: an upload almost never
arrives at the replica that issued the ticket, so the replica handling it has to be
able to see the record. That is the whole reason this store exists.

Import it explicitly::

    from mcp_upload.redis_store import RedisStore

It is not re-exported from the package root, so ``import mcp_upload`` never requires
``redis`` to be installed. Install it with the extra: ``pip install "mcp-upload[redis]"``.

Redemption is a Lua script, because no single Redis command does compare-and-swap on
a hash field. The script checks the status and the expiry and flips the status in one
indivisible step, so exactly one of any number of simultaneous uploads wins.

Two details worth knowing, both of which the obvious implementation gets wrong:

* The Redis key TTL is the **retention** window, never the redemption deadline.
  Redemption expiry is checked inside the script against ``expires_at``. Using the key
  TTL as the deadline forces you back into destroy-on-read, which throws away the
  record that answers "did that upload finish?". The TTL is also relative, computed
  from the record's own timestamps, because an absolute deadline taken from an
  injected clock deletes the key on write as soon as that clock and Redis disagree.
* The script never rewrites the immutable part of the record. Status and timestamps
  live in their own hash fields, so a decode and re-encode of the whole record cannot
  corrupt anything. That matters more than it sounds: round-tripping JSON through Lua
  turns an empty list into an empty object, which would silently rewrite an empty
  ``accept`` tuple into something else.

**Key layout.** A record is found two ways: by ticket hash on the upload path
(``get_by_hash``, ``redeem``) and by record id for ``finish``, status and ``claim``. Redis
Cluster lets a script touch only keys passed in ``KEYS`` that hash to the same slot,
and a script cannot look up one key and then open the key named by its value. So a
lookup that goes through an index costs two round trips, and the layout decides which
lookup pays. The record is stored under its ticket hash (``{prefix}:tk:{hash}``) and the
id key (``{prefix}:id:{id}``) is a small index holding the hash. ``get_by_hash`` and
``redeem`` are then one round trip each. The id side would cost two, so the store keeps
a bounded in-process map from id to hash, filled whenever it sees a record. The
mapping never changes for the life of a record, so the map cannot go stale in a way
that matters: every script also checks that the record it finds carries the id it was
asked for. On the upload path the replica that just redeemed a ticket finishes it, so
``finish`` finds the hash in the map and the whole path is three round trips, one per
call. A status lookup or claim on a replica that has never seen the record costs two.
The other way round (record under its id, as in 0.4.0) made the upload path pay two
round trips for both ``get_by_hash`` and ``redeem``. Putting a hash tag derived from the
ticket hash in both key names would put them in one slot, but then the id alone could
not name the record, so it does not help.

**Records written by 0.4.0** (``{prefix}:rec:{id}`` holding the record, and
``{prefix}:hash:{hash}`` holding the id) are still read, redeemed, finished, claimed and
swept: every lookup that misses the new layout falls back to the old one. Records live
at most for their retention window, so the fallback only costs anything during an
upgrade. A 0.4.0 replica cannot read records written in the new layout, so for a
rolling upgrade start the new replicas with ``legacy_layout=True``, which writes
records the old way, and turn it off once no 0.4.0 replica is left.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from redis.asyncio import Redis
from redis.commands.core import AsyncScript
from redis.typing import EncodableT

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

# KEYS[1] is the record hash. ARGV is (issued status, now, redeemed status, server
# clock flag). Returns the flat hash on success, or a bare error string. With the flag
# set, the expiry is compared against Redis's own clock instead of the caller's, so
# replicas whose clocks disagree still agree on when a ticket expires. TIME before a
# write is allowed because scripts replicate their effects, the default since Redis 5.
_REDEEM_LUA = """
local status = redis.call('HGET', KEYS[1], 'status')
if not status then
    return 'not_found'
end
if status ~= ARGV[1] then
    return 'already_used'
end
local now = tonumber(ARGV[2])
if ARGV[4] == '1' then
    local t = redis.call('TIME')
    now = tonumber(t[1]) + tonumber(t[2]) / 1000000
end
local expires = tonumber(redis.call('HGET', KEYS[1], 'expires_at'))
if expires == nil or now >= expires then
    return 'expired'
end
redis.call('HSET', KEYS[1], 'status', ARGV[3], 'redeemed_at', ARGV[2])
return redis.call('HGETALL', KEYS[1])
"""

# KEYS[1] is the record hash. ARGV is (record id, status, outcome json, finished_at).
# Writing to a key that has already expired would recreate it as a hash with no TTL
# that is never reaped, so the check and the write happen in one step. The check is on
# the id rather than mere existence, so a stale id-to-hash mapping can never write
# into some other record.
_FINISH_LUA = """
if redis.call('HGET', KEYS[1], 'id') ~= ARGV[1] then
    return 0
end
redis.call('HSET', KEYS[1], 'status', ARGV[2], 'outcome', ARGV[3], 'finished_at', ARGV[4])
return redis.call('HGETALL', KEYS[1])
"""

# KEYS[1] is the record hash. ARGV is (record id, completed status, claimed status, now).
_CLAIM_LUA = """
if redis.call('HGET', KEYS[1], 'id') ~= ARGV[1] then
    return 'not_found'
end
local status = redis.call('HGET', KEYS[1], 'status')
if status == ARGV[3] then
    return 'already_claimed'
end
if status ~= ARGV[2] then
    return 'not_completed'
end
redis.call('HSET', KEYS[1], 'status', ARGV[3], 'claimed_at', ARGV[4])
return redis.call('HGETALL', KEYS[1])
"""

# KEYS[1] is the record hash. ARGV is (ttl seconds, field, value, field, value, ...).
# Writing the fields and the TTL in one step means a dropped connection cannot leave a
# record behind that never expires. Sent with EVAL inside a pipeline, because a script
# object in a redis-py pipeline costs an extra SCRIPT EXISTS round trip on every call.
_PUT_LUA = """
redis.call('HSET', KEYS[1], unpack(ARGV, 2))
redis.call('EXPIRE', KEYS[1], ARGV[1])
return 1
"""

_CLAIM_ERRORS = {
    "not_found": ClaimError.NOT_FOUND,
    "already_claimed": ClaimError.ALREADY_CLAIMED,
    "not_completed": ClaimError.NOT_COMPLETED,
}

_ERRORS = {
    "not_found": RedeemError.NOT_FOUND,
    "already_used": RedeemError.ALREADY_USED,
    "expired": RedeemError.EXPIRED,
}

# How many id-to-hash mappings a store remembers. About 200 bytes each.
_MEMO_SIZE = 10_000


class RedisStore:
    """Ticket store backed by Redis or Valkey.

    ``client`` is an existing ``redis.asyncio.Redis``. The store never closes it,
    because it did not open it. ``prefix`` namespaces every key, so one Redis can hold
    tickets for several servers.

    Growth is bounded by the retention window rather than by a record cap. Every key is
    written with a TTL covering the retention window, so abandoned tickets disappear on
    their own and calling ``sweep`` is optional. Set a ``maxmemory`` policy on the server
    if you want a hard ceiling as well.

    ``server_clock=True`` makes redemption compare the ticket's expiry with the Redis
    server's clock instead of the ``now`` the caller passes. Use it when replicas'
    clocks drift apart. ``redeemed_at`` still records the caller's ``now``. It needs
    Redis 5 or later, or Valkey.

    ``legacy_layout=True`` writes new records in the 0.4.0 key layout, for a rolling
    upgrade while 0.4.0 replicas still serve traffic. See the module docstring.
    """

    def __init__(
        self,
        client: Redis,
        *,
        prefix: str = "mcp_upload",
        server_clock: bool = False,
        legacy_layout: bool = False,
    ) -> None:
        self._redis = client
        self._prefix = prefix
        self._server_clock = "1" if server_clock else "0"
        self._legacy_layout = legacy_layout
        self._redeem: AsyncScript = client.register_script(_REDEEM_LUA)
        self._finish: AsyncScript = client.register_script(_FINISH_LUA)
        self._claim: AsyncScript = client.register_script(_CLAIM_LUA)
        # Record id to ticket hash, for records in the current layout only.
        self._memo: dict[str, str] = {}

    def _ticket_key(self, ticket_hash: str) -> str:
        return f"{self._prefix}:tk:{ticket_hash}"

    def _id_key(self, record_id: str) -> str:
        return f"{self._prefix}:id:{record_id}"

    # The 0.4.0 layout: the record under its id, and an index from hash to id.
    def _legacy_record_key(self, record_id: str) -> str:
        return f"{self._prefix}:rec:{record_id}"

    def _legacy_hash_key(self, ticket_hash: str) -> str:
        return f"{self._prefix}:hash:{ticket_hash}"

    def _remember(self, record_id: str, ticket_hash: str) -> None:
        memo = self._memo
        if record_id not in memo and len(memo) >= _MEMO_SIZE:
            del memo[next(iter(memo))]  # the oldest entry
        memo[record_id] = ticket_hash

    async def put(self, record: Record) -> None:
        fields = _to_fields(record)
        # A relative TTL taken from the record's own two timestamps, never an absolute
        # EXPIREAT. Everything else in this library takes an injected clock, and an
        # absolute deadline computed from that clock against Redis's wall clock deletes
        # the key on write the moment the two disagree.
        ttl = max(1, int((record.retention_until - record.issued_at).total_seconds()))
        if self._legacy_layout:
            record_key = self._legacy_record_key(record.id)
            index_key, index_value = self._legacy_hash_key(record.ticket_hash), record.id
        else:
            record_key = self._ticket_key(record.ticket_hash)
            index_key, index_value = self._id_key(record.id), record.ticket_hash
        flat = [str(ttl)]
        for name, value in fields.items():
            flat += [str(name), str(value)]
        # Not a MULTI: the two keys live in different cluster slots. Nobody holds the
        # ticket until put returns, so no reader can see the record without its index,
        # and if the index write fails the record expires on its own.
        pipe = self._redis.pipeline(transaction=False)
        pipe.eval(_PUT_LUA, 1, record_key, *flat)
        pipe.set(index_key, index_value, ex=ttl)
        await pipe.execute()
        if not self._legacy_layout:
            self._remember(record.id, record.ticket_hash)

    async def get(self, record_id: str) -> Record | None:
        key = await self._locate(record_id)
        if key is None:
            return None
        fields = _decode_mapping(await self._redis.hgetall(key))
        return _from_fields(fields) if fields.get("id") == record_id else None

    async def _locate(self, record_id: str) -> str | None:
        """The key holding a record, from the map or in one round trip."""
        ticket_hash = self._memo.get(record_id)
        if ticket_hash is not None:
            return self._ticket_key(ticket_hash)
        legacy_key = self._legacy_record_key(record_id)
        pipe = self._redis.pipeline(transaction=False)
        pipe.get(self._id_key(record_id))
        pipe.exists(legacy_key)
        found, legacy = await pipe.execute()
        if found is not None:
            self._remember(record_id, _text(found))
            return self._ticket_key(_text(found))
        return legacy_key if legacy else None

    async def get_by_hash(self, ticket_hash: str) -> Record | None:
        # Both layouts are asked at once, so a miss costs no extra round trip.
        pipe = self._redis.pipeline(transaction=False)
        pipe.hgetall(self._ticket_key(ticket_hash))
        pipe.get(self._legacy_hash_key(ticket_hash))
        fields, legacy_id = await pipe.execute()
        if fields:
            record = _from_fields(_decode_mapping(fields))
            self._remember(record.id, ticket_hash)
            return record
        if legacy_id is None:
            return None
        legacy = _decode_mapping(
            await self._redis.hgetall(self._legacy_record_key(_text(legacy_id)))
        )
        return _from_fields(legacy) if legacy else None

    async def redeem(self, ticket_hash: str, now: datetime) -> Record | RedeemError:
        result = await self._redeem_at(self._ticket_key(ticket_hash), now)
        if result is RedeemError.NOT_FOUND:
            # The index is written once and never changes, so reading it outside the
            # script races with nothing. The status flip is entirely inside the script.
            legacy_id = await self._redis.get(self._legacy_hash_key(ticket_hash))
            if legacy_id is None:
                return RedeemError.NOT_FOUND
            return await self._redeem_at(self._legacy_record_key(_text(legacy_id)), now)
        if isinstance(result, Record):
            self._remember(result.id, ticket_hash)
        return result

    async def _redeem_at(self, key: str, now: datetime) -> Record | RedeemError:
        result = await self._redeem(
            keys=[key],
            args=[
                Status.ISSUED.value,
                repr(now.timestamp()),
                Status.REDEEMED.value,
                self._server_clock,
            ],
        )
        if isinstance(result, (bytes, str)):
            return _ERRORS.get(_text(result), RedeemError.NOT_FOUND)
        return _from_fields(_decode_mapping(_pairs_to_mapping(result)))

    async def finish(
        self, record_id: str, status: Status, outcome: Outcome, now: datetime
    ) -> Record | None:
        key = await self._locate(record_id)
        if key is None:
            return None
        result = await self._finish(
            keys=[key],
            args=[
                record_id,
                status.value,
                json.dumps(dump_outcome(outcome)),
                repr(now.timestamp()),
            ],
        )
        if not isinstance(result, list):
            return None
        return _from_fields(_decode_mapping(_pairs_to_mapping(result)))

    async def claim(self, record_id: str, now: datetime) -> Record | ClaimError:
        key = await self._locate(record_id)
        if key is None:
            return ClaimError.NOT_FOUND
        result = await self._claim(
            keys=[key],
            args=[record_id, Status.COMPLETED.value, Status.CLAIMED.value, repr(now.timestamp())],
        )
        if isinstance(result, (bytes, str)):
            return _CLAIM_ERRORS.get(_text(result), ClaimError.NOT_FOUND)
        return _from_fields(_decode_mapping(_pairs_to_mapping(result)))

    async def sweep(self, now: datetime) -> int:
        """Delete records past their retention deadline and return how many went.

        Redis expires the keys on its own, so this is not needed to bound growth. It
        exists because the deadline the rest of the library reasons about comes from an
        injected clock, and an operator may want to force the reap rather than wait for
        Redis to notice. SCAN is used rather than KEYS so a large keyspace is not
        blocked while it runs. Records in the 0.4.0 layout are swept too.
        """
        deadline = now.timestamp()
        deleted = 0
        for legacy in (False, True):
            pattern = f"{self._prefix}:{'rec' if legacy else 'tk'}:*"
            async for key in self._redis.scan_iter(match=pattern, count=100):
                fields = await self._redis.hmget(key, ["retention_until", "id", "ticket_hash"])
                retention, record_id, ticket_hash = fields[0], fields[1], fields[2]
                if retention is None or float(_text(retention)) > deadline:
                    continue
                # Two keys in different cluster slots, so not a MULTI. A record whose
                # index outlives it is simply not found.
                pipe = self._redis.pipeline(transaction=False)
                pipe.delete(key)
                if legacy and ticket_hash is not None:
                    pipe.delete(self._legacy_hash_key(_text(ticket_hash)))
                if not legacy and record_id is not None:
                    pipe.delete(self._id_key(_text(record_id)))
                    self._memo.pop(_text(record_id), None)
                await pipe.execute()
                deleted += 1
        return deleted


def _to_fields(record: Record) -> dict[EncodableT, EncodableT]:
    # Typed with redis-py's own alias rather than dict[str, str]: a dict is invariant
    # in its key type, so the narrower annotation is rejected where hset wants a
    # Mapping[EncodableT, EncodableT].
    fields: dict[EncodableT, EncodableT] = {
        "id": record.id,
        "ticket_hash": record.ticket_hash,
        "destination": record.destination,
        "issued_at": repr(record.issued_at.timestamp()),
        "expires_at": repr(record.expires_at.timestamp()),
        "retention_until": repr(record.retention_until.timestamp()),
        "constraints": json.dumps(dump_constraints(record.constraints)),
        "status": record.status.value,
    }
    if record.caller is not None:
        fields["caller"] = record.caller
    if record.owner is not None:
        fields["owner"] = record.owner
    return fields


def _from_fields(f: dict[str, str]) -> Record:
    return Record(
        id=f["id"],
        ticket_hash=f["ticket_hash"],
        destination=f["destination"],
        caller=f.get("caller"),
        issued_at=_time(f["issued_at"]),
        expires_at=_time(f["expires_at"]),
        retention_until=_time(f["retention_until"]),
        constraints=load_constraints(json.loads(f["constraints"])),
        status=Status(f["status"]),
        redeemed_at=_maybe_time(f.get("redeemed_at")),
        finished_at=_maybe_time(f.get("finished_at")),
        outcome=None if f.get("outcome") is None else load_outcome(json.loads(f["outcome"])),
        owner=f.get("owner"),
        claimed_at=_maybe_time(f.get("claimed_at")),
    )


def _time(value: str) -> datetime:
    return datetime.fromtimestamp(float(value), UTC)


def _maybe_time(value: str | None) -> datetime | None:
    return None if value is None else _time(value)


def _text(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _decode_mapping(mapping: dict[Any, Any]) -> dict[str, str]:
    return {_text(k): _text(v) for k, v in mapping.items()}


def _pairs_to_mapping(flat: list[Any]) -> dict[Any, Any]:
    # HGETALL through a Lua script comes back as a flat array, not a map.
    return dict(zip(flat[::2], flat[1::2], strict=True))
