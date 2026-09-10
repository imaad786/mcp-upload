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
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from redis.asyncio import Redis
from redis.commands.core import AsyncScript
from redis.typing import EncodableT

from .tickets import Constraints, Outcome, Record, RedeemError, Status

# KEYS[1] is the record hash. ARGV is (issued status, now, redeemed status).
# Returns the flat hash on success, or a bare error string.
_REDEEM_LUA = """
local status = redis.call('HGET', KEYS[1], 'status')
if not status then
    return 'not_found'
end
if status ~= ARGV[1] then
    return 'already_used'
end
local expires = tonumber(redis.call('HGET', KEYS[1], 'expires_at'))
if expires == nil or tonumber(ARGV[2]) >= expires then
    return 'expired'
end
redis.call('HSET', KEYS[1], 'status', ARGV[3], 'redeemed_at', ARGV[2])
return redis.call('HGETALL', KEYS[1])
"""

_ERRORS = {
    "not_found": RedeemError.NOT_FOUND,
    "already_used": RedeemError.ALREADY_USED,
    "expired": RedeemError.EXPIRED,
}


class RedisStore:
    """Ticket store backed by Redis or Valkey.

    ``client`` is an existing ``redis.asyncio.Redis``. The store never closes it,
    because it did not open it. ``prefix`` namespaces every key, so one Redis can hold
    tickets for several servers.

    Growth is bounded by the retention window rather than by a record cap. Every key is
    written with a TTL covering the retention window, so abandoned tickets disappear on
    their own and calling ``sweep`` is optional. Set a ``maxmemory`` policy on the server
    if you want a hard ceiling as well.
    """

    def __init__(self, client: Redis, *, prefix: str = "mcp_upload") -> None:
        self._redis = client
        self._prefix = prefix
        self._redeem: AsyncScript = client.register_script(_REDEEM_LUA)

    def _record_key(self, record_id: str) -> str:
        return f"{self._prefix}:rec:{record_id}"

    def _hash_key(self, ticket_hash: str) -> str:
        return f"{self._prefix}:hash:{ticket_hash}"

    async def put(self, record: Record) -> None:
        record_key = self._record_key(record.id)
        hash_key = self._hash_key(record.ticket_hash)
        # A relative TTL taken from the record's own two timestamps, never an absolute
        # EXPIREAT. Everything else in this library takes an injected clock, and an
        # absolute deadline computed from that clock against Redis's wall clock deletes
        # the key on write the moment the two disagree.
        ttl = max(1, int((record.retention_until - record.issued_at).total_seconds()))
        pipe = self._redis.pipeline(transaction=True)
        pipe.hset(record_key, mapping=_to_fields(record))
        pipe.expire(record_key, ttl)
        pipe.set(hash_key, record.id, ex=ttl)
        await pipe.execute()

    async def get(self, record_id: str) -> Record | None:
        fields = await self._redis.hgetall(self._record_key(record_id))
        return _from_fields(_decode_mapping(fields)) if fields else None

    async def get_by_hash(self, ticket_hash: str) -> Record | None:
        record_id = await self._redis.get(self._hash_key(ticket_hash))
        if record_id is None:
            return None
        return await self.get(_text(record_id))

    async def redeem(self, ticket_hash: str, now: datetime) -> Record | RedeemError:
        # The index is written once and never changes, so reading it outside the script
        # races with nothing. The status flip, which is the part that must not race, is
        # entirely inside the script and touches exactly one key.
        record_id = await self._redis.get(self._hash_key(ticket_hash))
        if record_id is None:
            return RedeemError.NOT_FOUND
        result = await self._redeem(
            keys=[self._record_key(_text(record_id))],
            args=[Status.ISSUED.value, repr(now.timestamp()), Status.REDEEMED.value],
        )
        if isinstance(result, (bytes, str)):
            return _ERRORS.get(_text(result), RedeemError.NOT_FOUND)
        return _from_fields(_decode_mapping(_pairs_to_mapping(result)))

    async def finish(
        self, record_id: str, status: Status, outcome: Outcome, now: datetime
    ) -> Record | None:
        record_key = self._record_key(record_id)
        if not await self._redis.exists(record_key):
            return None
        await self._redis.hset(
            record_key,
            mapping={
                "status": status.value,
                "outcome": _dump_outcome(outcome),
                "finished_at": repr(now.timestamp()),
            },
        )
        return await self.get(record_id)

    async def sweep(self, now: datetime) -> int:
        """Delete records past their retention deadline and return how many went.

        Redis expires the keys on its own, so this is not needed to bound growth. It
        exists because the deadline the rest of the library reasons about comes from an
        injected clock, and an operator may want to force the reap rather than wait for
        Redis to notice. SCAN is used rather than KEYS so a large keyspace is not
        blocked while it runs.
        """
        deadline = now.timestamp()
        deleted = 0
        async for key in self._redis.scan_iter(match=f"{self._prefix}:rec:*", count=100):
            fields = await self._redis.hmget(key, ["retention_until", "ticket_hash"])
            retention, ticket_hash = fields[0], fields[1]
            if retention is None or float(_text(retention)) > deadline:
                continue
            pipe = self._redis.pipeline(transaction=True)
            pipe.delete(key)
            if ticket_hash is not None:
                pipe.delete(self._hash_key(_text(ticket_hash)))
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
        "constraints": json.dumps(
            {"max_size": record.constraints.max_size, "accept": list(record.constraints.accept)}
        ),
        "status": record.status.value,
    }
    if record.caller is not None:
        fields["caller"] = record.caller
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
        constraints=_load_constraints(f["constraints"]),
        status=Status(f["status"]),
        redeemed_at=_maybe_time(f.get("redeemed_at")),
        finished_at=_maybe_time(f.get("finished_at")),
        outcome=_load_outcome(f.get("outcome")),
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
