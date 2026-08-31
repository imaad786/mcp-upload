# Changelog

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
