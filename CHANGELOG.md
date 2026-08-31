# Changelog

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
