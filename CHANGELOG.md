# Changelog

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
