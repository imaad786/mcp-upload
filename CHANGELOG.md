# Changelog

## Unreleased

Integration features for 0.6.0: uploads into the server's own code, raw-body uploads,
and a browser page with progress. Nothing changes for an existing server unless it
opts in. Checked with three new end-to-end scenarios in `stress/run.py`, run at small
scale, and with headless Chrome against the page.

**Added**

- **Function destinations.** `Destination(name=..., sink=fn)` registers an async
  function instead of a `url`; exactly one of the two is required. The function gets an
  `IncomingFile` (record id, sanitized filename, checked media type, declared size and
  digest) and iterates it for the bytes. Iteration ends normally only after the whole
  request validated, including the declared size and digest. On any failure the next
  read raises `UploadAborted` with the error code, so a sink that commits after its
  loop cannot commit a bad upload. A sink that raises fails the upload as `sink_failed`
  (502, never echoed); one that returns before the end fails it as
  `upstream_closed_early`. Once the body has stopped arriving, the sink has the
  destination's `timeout` to return. Live: 50 of 50 concurrent uploads of up to 4 MiB
  committed with the exact bytes into an in-process sink.
- **Filesystem sink.** `mcp_upload.sinks.filesystem(directory, name_template=...,
  overwrite=False, fsync=True)` writes to a `.upload-*.part` file in the directory
  (mode 0600), syncs it, and links or renames it into place only after a normal end,
  then syncs the directory. On any failure the temporary file is deleted. It never
  replaces an existing file unless asked, refuses names outside its directory, and does
  its file work on its own thread pool. Live: 50 of 50 concurrent uploads written with
  the exact bytes; in a mixed run of 20 honest uploads, 10 clients that vanished
  mid-body, 10 oversize bodies and 10 wrong declared digests, the 20 honest files were
  written correctly, none of the 30 failures left a file, and no temporary file
  remained.
- **Raw-body uploads.** `UploadGateway(raw_uploads=True)` also accepts a POST or PUT
  whose body is the file. The media type is the Content-Type, held to the same token
  grammar and accept list, and checked before the ticket is spent. The filename comes
  from `Content-Disposition` (RFC 8187 `filename*` first) or a `filename` query
  parameter. Every existing limit applies, and the declared length is held to
  `max_size` exactly. URL-encoded forms are never taken as files. Off by default, with
  the old behaviour unchanged. `describe(issued, raw=True)` returns a PUT transfer
  descriptor with no `multipart` key. Live: 30 of 30 raw uploads completed with the
  right bytes and names; oversize, wrong-digest, wrong-type, URL-encoded and vanished
  uploads were all refused, the header-only refusals left their tickets unspent, and
  nothing refused was committed.
- **Upload page progress.** The form now has drag and drop, a progress bar and an
  in-place result, from one inline script admitted by a per-response CSP nonce. The
  policy is otherwise unchanged apart from `connect-src 'self'`, there are no inline
  handlers, and the plain form still posts with scripts off. In headless Chrome the
  script ran with no CSP violation, reported progress through a throttled 24 MiB
  upload, and showed both a completed upload and a `too_large` refusal.
- **`sink_failed`** in `ERROR_STATUS`. `IncomingFile`, `UploadAborted` and `Sink` are
  exported from the package root.

**Changed**

- `Destination.url` is now optional (it is `None` for a sink). Positional construction
  as `Destination(name, url)` still works.

## 0.5.0 (2026-10-03)

Speed and scale in the stores, and hooks for what happens after an upload. Measured
against 0.4.0 with the end-to-end harness, which gained scenarios for small client
frames, store-backed upload rates, two gateway processes sharing one Redis or one
SQLite file, and the completion hook. Every invariant held on both versions.

**Upgrading a fleet:** `RedisStore` has a new key layout. It reads records written by
0.4.0, but 0.4.0 cannot read records written by 0.5.0. During a rolling upgrade, run
the new replicas with `legacy_layout=True` until no 0.4.0 replica is left. Live: with
the flag, uploads, status and claims worked 20 of 20 in both directions between a
0.4.0 and a 0.5.0 replica; without it, tickets issued on 0.5.0 are unknown to 0.4.0.

- **SQLite store.** One connection per thread on a small pool of its own, pragmas set
  once, writes serialized within a process, and the record cap kept by a trigger
  instead of a `COUNT(*)` on every insert. Issuing runs at 11,648 tickets/s against
  1,627, and 64 concurrent redemptions at 24,799/s against 6,191. Through the gateway,
  400 concurrent uploads backed by SQLite completed at 744/s against 500, with p99
  latency 534 ms against 794 and a third of the gateway CPU. New `aclose()`.
- **Redis store.** Looking up, redeeming and finishing an upload are one round trip
  each, three per upload against six. Every script touches one key, so the layout
  suits Redis Cluster (not tested on a cluster). `server_clock=True` checks expiry
  against Redis's clock for replicas whose clocks drift. Two gateway processes on one
  Redis: 200 tickets issued on one and uploaded to the other all completed, 100
  simultaneous posts of one ticket to both had one winner each, and so did claims. A
  status lookup on a replica that never saw the record now takes two round trips.
- **`on_complete`** is awaited with the final record of every redeemed upload after
  the response has gone out. Live: 150 completed and 50 failed uploads produced
  exactly 150 and 50 calls, and a hook taking 50 ms did not change upload latency.
- **OpenTelemetry** traces and metrics when the API is installed and an SDK is
  configured. No-ops otherwise. New `otel` extra.
- **README:** the Redis example now uses a blocking connection pool. With redis-py's
  default pool, 300 of 400 simultaneous uploads failed with 500 on every version.

Throughput is unchanged: one upload at 875 MiB/s against 887, and 200 concurrent at
738 against 737, medians of three alternating runs. Joining small reads before
forwarding was tried during development and dropped: no gain with 16 KiB client
frames, and 30 to 45 percent more memory.

## 0.4.0 (2026-10-03)

Conformance with SEP-2631's shapes, and the pieces a multi-user server needs. Measured
against 0.3.1 with the end-to-end harness, which gained eight scenarios for this
release. None of the existing scenarios regressed: one upload streams at 858 MiB/s
against 878, and 200 concurrent ones at 740 against 747, within run-to-run noise.

**Breaking**

- **Digests are base64url.** `FileValue.digest.value` was hex. SEP-2631 specifies
  base64url without padding, and a client following it saw a mismatch.
- **`Store` gained `claim`.** A custom store must implement it.

**Added**

- **Declared size and digest.** `issue(expected_size=..., expected_digest=...)` takes
  what `files/authorizeUpload` carries. Bytes that do not match are refused with
  `size_mismatch` or `digest_mismatch` (422) before the backend sees the end of the
  body. Live: 100 of 100 wrong digests and 50 of 50 wrong sizes refused, none
  committed, and 100 of 100 matching uploads completed with the right bytes.
- **Owners.** `issue(owner=...)` binds a record to a user, and `status` and `claim`
  with a different owner report it as unknown. Live: 50 of 50 lookups by another user
  saw nothing, and none of their claims won.
- **`claim`.** Takes a completed upload for use exactly once, atomically in every
  store. Live: 20 concurrent claims on each of 50 records produced exactly one winner
  per record on the memory and SQLite stores, and 50 concurrent claims produced one
  winner on a real Redis 8.
- **Machine-readable `details`** on every failure that has them, in the response and
  in status, for example `{"reason": "maxSizeExceeded", "maxSize": 1000}`.
- **`abandoned`.** A record left `redeemed` by a process that died now reads as
  `failed` with error `abandoned` once `upload_timeout` plus 30 seconds has passed.
  Live: a gateway killed mid-upload left its record `redeemed` forever on 0.3.1, and
  `abandoned` 33 seconds after restart on 0.4.0.

**Fixed**

- **Redis `finish` could recreate a swept record** as a key with no TTL. The check and
  the write are now one Lua script.
- **Filenames** are normalized to NFC, lose invisible format characters such as the
  right-to-left override that made `invoice\u202egpj.exe` display as
  `invoiceexe.jpg`, and are cut to 255 bytes of UTF-8, not 255 characters, keeping a
  short extension.
- **SQLite databases from earlier versions** gain the new columns on open. Live: a
  0.3.1 database kept its completed record, and its unused ticket redeemed correctly.

## 0.3.1 (2026-10-03)

Security, integrity and robustness fixes, each measured against 0.3.0 on the same
machine with the new end-to-end harness and framing fuzz in `stress/`. Upgrading is
recommended.

- **Uploads could complete with the wrong bytes.** When the text after the closing
  delimiter repeated the delimiter, and for some binary epilogues, the extra bytes were
  appended to the file sent to the backend while the upload reported success, with a
  digest of the wrong bytes. In 3,000 randomized bodies, 398 completed this way on
  0.3.0 and none on 0.3.1.
- **Part headers are bounded.** The multipart parser buffers a part's headers whole, at
  a cost that grows faster than their size, and `max_size` only counted file bytes. One
  holder of a valid ticket could send a 64 MiB header with no `Content-Length` and take
  the gateway past 4 GB of memory while the upload reported success. Non-file data in
  the body is now capped at 512 KiB (`part_headers_too_large`).
- **Slow clients are cut off.** A client sending a byte every few seconds held its slot
  and a backend connection indefinitely, so 32 of them shut out every honest upload.
  An upload is now refused with `too_slow` when it delivers under 64 KiB within 30
  seconds of waiting on the client, and with `upload_timeout` after an hour. Time spent
  waiting on a slow backend is not counted. See `stall_timeout`, `stall_min_bytes` and
  `upload_timeout`.
- **`max_in_flight` holds under bursts.** The slot was counted after the store lookup,
  so a burst passed the check together: with a cap of 8, 100 uploads reached the
  backend at once in an unpaced burst. The slot is now taken before the first await.
  The default is now 100 instead of unlimited, and the default HTTP client is sized to
  it. Before, the client's hidden pool of 100 queued uploads after their tickets were
  spent: with a cap of 250, only 100 reached the backend at once, and now all 250 do.
- **Preambles and epilogues are accepted.** RFC 2046 allows text before the first
  delimiter and after the last. The parser rejected both, so an upload followed by a
  single trailing CRLF failed as `truncated`: 100 of 100 such uploads failed on 0.3.0,
  none on 0.3.1. The epilogue is now ignored, read to at most 64 KiB.
- **Early refusals reach the client.** A refusal sent before the body was read closed
  the socket with unread data, and the reset could destroy the response, so a client
  saw "connection reset" instead of 503 or 413. Up to 1 MiB is now read after the
  response is sent. In the slow-client run, honest uploads that ended in a connection
  error instead of a status fell from 37 of 40 to 8 of 40. Bodies much larger than
  1 MiB can still see a reset.
- **Per-ticket `accept` cannot widen the destination's list.** It replaced the list, so
  an `image/*` destination took a `.exe`. A type outside the destination's list now
  raises `ValueError`, and an empty list means the destination's own.
- **`ttl` is validated.** `ttl=timedelta(0)` silently became fifteen minutes, and a
  `ttl` longer than the retention let a record vanish while still redeemable. Both now
  raise `ValueError`, as do a negative `max_size` and nonsensical gateway settings.

The cost, on loopback: one upload streams at 911 MiB/s against 1,000 before, and 200
concurrent uploads at 760 MiB/s against 785. Most of it is the scan for the closing
delimiter, which is what makes the epilogue handling correct. Peak memory with 200
concurrent uploads is about 15% higher, because all 200 now stream at once where the
old hidden pool let 100 through.

## 0.3.0 (2026-10-03)

- **The `fastmcp` extra now requires FastMCP 4.x** (`fastmcp>=4,<5`). FastMCP 4.x builds
  on the official SDK 2.x, so the `mcp` and `fastmcp` extras can now be installed
  together. FastMCP 3.x is no longer supported.

## 0.2.0 (2026-09-09)

- **`RedisStore`**, for a server running behind a load balancer, where an upload almost
  never arrives at the replica that issued the ticket. Redemption is a Lua script,
  because no single Redis command does compare-and-swap on a hash field. Install with
  the new `redis` extra and import it explicitly from `mcp_upload.redis_store`, so
  `import mcp_upload` never requires `redis`. The key TTL is the retention window and
  never the redemption deadline, and the script writes only the status and timestamp
  fields, never the record as a whole. Its tests run against fakeredis locally and
  against a real Redis service in CI.
- **`UploadTicketExtension`**, which advertises `me.imaadkhan/upload-ticket` under
  `ServerCapabilities.extensions` so a client can discover that a server takes files.
  Reverse-DNS prefix per SEP-2133. It contributes no tools and intercepts nothing, so
  uploads behave the same whether or not it is declared.

## 0.1.3 (2026-08-31)

Documentation only. No code changes, no API changes.

- The README now separates two claims that the upload ticket pattern gets credited
  for. A backend that already issues presigned upload URLs keeps file bytes off the
  application server, which is a different hop from the tool interface this library
  addresses. A tool wrapping such a backend still takes base64 in an argument. Stated
  in the problem section and added as an entry to the alternatives comparison, along
  with the note that the gateway stays in the byte path deliberately.

## 0.1.2 (2026-08-31)

Hardening release after a security review. No API removals.

- The declared media type of the file part is validated against the RFC 7230 token
  grammar and capped in length before it becomes a backend request header or a record
  field. A bad value is refused with `invalid_media_type` (400).
- New `max_in_flight` option on `UploadGateway`. Beyond the cap a request gets
  `too_many_uploads` (503) with `Retry-After`, before its ticket is touched.
- `SqliteStore` gains `max_records` (default 100,000) with the same sweep-then-refuse
  behaviour as the in-memory store.
- The browser page sends `Content-Security-Policy` and `X-Frame-Options: DENY`.
- The library logs through the `mcp_upload` logger: record ids, destinations and
  outcome codes. Never the ticket or the upload URL, and a test asserts it.
- Supply chain: GitHub Actions pinned to commits, read-only workflow tokens, a
  committed `uv.lock` installed with `--locked` in CI, the build backend pinned,
  a known-vulnerability audit and a static security scan on every push, Dependabot
  for the lockfile and the action pins, and `SECURITY.md`.

## 0.1.1 (2026-08-31)

No code changes. The README published to PyPI now matches the repository: the
tokenizer note says Claude's counts were not measured rather than asserting they run
higher, and a provenance section carries the AI disclosure, credits SEP-2631 by Casey
Chow for the wire shapes, and names the implementers who arrived at the pattern
independently.

## 0.1.0 (2026-08-31)

First release. Targets MCP protocol revision 2026-07-28.

- Upload tickets: 256-bit secrets stored only as their SHA-256, naming a record that
  moves from issued to redeemed to completed or failed and survives redemption. Two
  clocks per record: a redemption deadline and a retention deadline.
- Stores: an in-memory store with a record cap, and a SQLite store. Both redeem
  atomically, checked with fifty concurrent redemptions and one winner. A six-method
  `Store` protocol for anything else.
- Destinations come from a registry declared by the server author. Tools pick one by
  name. No API accepts a URL from a tool argument.
- The upload endpoint. Header-only checks run before the ticket is touched, then the
  atomic flip, then the multipart body streams through a bounded queue to the
  destination with nothing on disk. Size limits are enforced on the bytes seen, not
  on `Content-Length`. Exactly one file part is accepted. The end of the upstream
  body is held until the whole request has parsed. A `GET` on the ticket URL renders
  a browser upload form.
- Wire shapes in the vocabulary of the MCP file transfer proposal (SEP-2631):
  `AwaitingUpload`, `FileTransferDescriptor`, `FileValue`, `UploadStatus`.
- Adapters for the official SDK (`mcp` 2.x) and FastMCP (`fastmcp` 3.x), one
  registration call each. `ask_for_upload` for URL-mode elicitation through the
  multi-round-trip flow on the official SDK.
- Examples: a stub backend, an MCP server with `request_upload`,
  `request_upload_interactive` and `check_upload`, and a Python client.
- Written with AI assistance (Claude Code) under human direction and review.
