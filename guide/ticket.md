# The ticket and the endpoint

Why the ticket alone can authorize an upload, the three stores that hold it, what the
upload endpoint refuses and in what order, and how the body streams. Back to the
[README](https://github.com/imaad786/mcp-upload/blob/main/README.md).

## The ticket

The upload endpoint takes no session, header or OAuth token. The ticket is the
authorization. That is safe only because the ticket is 256 bits from the OS CSPRNG,
stored only as its SHA-256 so a copy of the store yields nothing usable, valid for
one redemption enforced atomically in the store, expiring in minutes, bound to a
destination the server author chose, and useless for reading anything back (a `GET`
on the URL renders an upload form, never a file). Remove any one of those and it is
a hole. Reusing the caller's session token here would be worse: the model often
cannot supply one, and sending a broad long-lived credential to a second origin
widens the blast radius when it leaks.

The ticket is in the URL path, because a browser form needs it there. URLs land in
access logs and browser history. The page sends `Referrer-Policy: no-referrer`. Scrub
the path from your access logs, or accept that a leaked log yields tickets that
expire in fifteen minutes and work once.

Redemption flips a status field instead of deleting the record. Deleting is just as
atomic, and it is what the obvious Redis `GETDEL` gives you, but it destroys the only
place the outcome could live. A record here moves `issued` to `redeemed` to
`completed` or `failed`, optionally `completed` to `claimed`, and stays until its
retention deadline (24 hours by default)
so "did that upload finish?" has an answer. Redemption stops being allowed at
`expires_at` (15 minutes by default). The two clocks are independent.

## The stores

Three stores ship, each using the strongest primitive its engine offers.
`MemoryStore` is for one process and for tests. It is correct under concurrency only
because its redeem does the read, check and write with no `await` in between, and it
has a record cap so ticket issuance cannot grow memory without bound. `SqliteStore` is
for one host with several workers: redemption is one conditional `UPDATE` whose `WHERE`
clause carries the status and expiry checks, so the database serializes competing
writers and exactly one sees a row change. `RedisStore` is for a server behind a load
balancer, where the upload almost never arrives at the replica that issued the ticket:
redemption is a Lua script, because no single Redis command does compare-and-swap on a
hash field. All three were checked with fifty concurrent redemptions of one ticket and
one winner. The `Store` protocol is seven methods. Bring your own for anything else.

`SqliteStore` keeps one connection per thread on a small pool of its own (`threads=`,
2 by default) and serializes writes within a process, so concurrent calls queue
instead of spinning in SQLite's busy handler. Call `await store.aclose()` on shutdown.

`RedisStore` needs the extra and an explicit import, so `import mcp_upload` never
requires `redis`:

```
pip install "mcp-upload[mcp,redis]"
```

```python
from redis.asyncio import BlockingConnectionPool, Redis
from mcp_upload.redis_store import RedisStore

pool = BlockingConnectionPool.from_url("redis://localhost:6379/0", max_connections=64)
store = RedisStore(Redis(connection_pool=pool))
```

Use a blocking pool. With redis-py's default pool, a burst past its connection limit
raises instead of waiting: in a test of 400 simultaneous uploads, 300 failed with 500.

Two things it does deliberately. The Redis key TTL is the retention window and never
the redemption deadline, because expiring the key at the redemption deadline is what
forces you back into destroy-on-read and throws away the record that answers "did that
upload finish?". And the script only ever writes the status and timestamp fields, never
the record as a whole, so a decode and re-encode in Lua cannot quietly rewrite an empty
`accept` list into an empty object. Its tests run against fakeredis by default and
against a real Redis in CI, because a store whose one job is atomicity should not be
certified by a simulator alone.

Each step of an upload is one round trip: looking the ticket up, redeeming it and
recording the outcome. Every script touches exactly one key, so the layout works on
Redis Cluster. Two options matter for fleets. `server_clock=True` checks expiry
against Redis's own clock, for replicas whose clocks drift. `legacy_layout=True` keeps
writing the 0.4.0 key layout during a rolling upgrade, until no 0.4.0 replica is left.
Records in the old layout are read either way.

## What the endpoint refuses, and when

The order matters. Nothing that can be judged from the headers alone may cost the
ticket, so a stray or hostile non-multipart POST cannot burn someone's pending upload.

1. Content type must be exactly `multipart/form-data` (case-insensitive, since a
   naive exact comparison rejects valid uppercase) with a boundary. Otherwise 415 or
   400, ticket untouched. Framework form helpers that also accept URL-encoded bodies
   are how a text field ends up read as a file. With `raw_uploads=True`, any other
   type except a URL-encoded form or another multipart type is a raw upload instead
   (see [Raw-body uploads](https://github.com/imaad786/mcp-upload/blob/main/guide/destinations.md#raw-body-uploads)).
2. A declared `Content-Length` far over the limit is refused with 413, ticket untouched.
   A raw upload's media type is checked here too.
3. The ticket is looked up by hash. Unknown is 404, already used or expired is 410.
4. The atomic flip. From here the ticket is spent.
5. The body streams. The size limit is enforced on the bytes actually seen, because a
   chunked upload has no `Content-Length` and a lying one is trivial to send. Exactly
   one part, named `file`, with a filename, of an accepted type. A second file part,
   a part without a filename, or any other part is refused. Frameworks that silently
   keep one of two same-named parts are how a request passes validation on one and
   delivers the other. Filenames are reduced to a base name before forwarding.
6. The terminal state is recorded.

| Code | HTTP | Meaning |
|---|---|---|
| `not_multipart`, `missing_boundary` | 415, 400 | Header-only, ticket untouched |
| `unknown_ticket` | 404 | |
| `ticket_used`, `ticket_expired` | 410 | |
| `too_large` | 413 | On declared length or on the running count |
| `size_mismatch`, `digest_mismatch` | 422 | Bytes differ from the size or digest declared at issue. Not committed. |
| `part_headers_too_large` | 400 | More than 512 KiB of the body is not file data |
| `too_slow`, `upload_timeout` | 408 | Client stalled (under 64 KiB in 30 s of waiting), or the upload ran past an hour |
| `too_many_uploads` | 503 | `max_in_flight` reached, ticket untouched, `Retry-After` set |
| `missing_file`, `duplicate_file`, `unexpected_part`, `bad_multipart`, `truncated` | 400 | |
| `invalid_media_type` | 400 | Declared type is not a valid `type/subtype` token. Ticket untouched on a raw upload. |
| `unsupported_media_type` | 415 | Declared type not in the accept list. Ticket untouched on a raw upload. |
| `client_disconnected` | 400 | Recorded on the record. No response reaches the client. |
| `abandoned` | (status only) | The process streaming the upload died. Reported once `upload_timeout` plus 30 s has passed. |
| `upstream_unreachable`, `upstream_rejected` | 502 | Backend failure, mapped, never echoed |
| `upstream_closed_early` | 502 | The backend answered, or a sink returned, before the end of the upload |
| `sink_failed` | 502 | A sink raised, or did not return within the destination's `timeout` after the body ended. Never echoed. |

## Streaming

The obvious implementation, `await request.form()`, does not blow up memory. It
writes the whole upload to a temporary file on disk before your handler runs, and
puts no limit on file parts. That is not streaming and it is not "stores nothing".

Here the multipart body is parsed incrementally as it arrives, each chunk is hashed
and handed to a bounded queue, and an outgoing request to the backend drains that
queue. When the backend is slow the queue fills, the parser stops, the request body
stops being read, and the client's upload stalls. The test suite streams 32 MiB
through a deliberately slow backend and the process grows by about 5 MiB.

The parser is `streaming-form-data`, a compiled extension. Wheels exist for CPython
3.11 to 3.13 as of its 2.1.0 release, which is why 3.14 is not yet in the test matrix.
