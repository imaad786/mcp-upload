# mcp-upload

File uploads for MCP servers, using short-lived upload tickets.

An MCP tool that needs a file hands back a single-use URL. Whoever holds the file
posts it there over plain HTTPS. The server streams the bytes straight through to
the backend you already have, and keeps a small record of what happened. Nothing
about the file ever travels through MCP except a reference to it. No change to the
protocol is needed, and no host has to know the library exists.

Targets protocol revision 2026-07-28. Works with the official Python SDK (`mcp` 2.x)
and with FastMCP (`fastmcp` 3.x). Python 3.11 or later. Apache 2.0.

## The problem

MCP can carry binary data from a server back to a model, as image, audio or embedded
resource content. It cannot carry a file the other way. A tool call is JSON, its
inputs are described by JSON Schema, and there is no file type in that vocabulary
and no way to tell a host "this argument is a file, show a picker".

The usual workaround is to base64 the file into a string argument. The objection
people reach for is context-window size. The real problem is who types the bytes.
Tool arguments are model output, generated token by token, so the model has to
produce the whole encoded file, perfectly. Base64 tokenizes at roughly 1.4 to 1.5
characters per token on OpenAI's encoders (English runs about five). A 126 KB image
costs about 120,000 output tokens. That is 94% of the 128,000-token per-response
ceiling of the largest models available today and over the 64,000-token ceiling of
smaller ones. A 500 KiB photo costs about 490,000 tokens, almost four times the larger
ceiling. These are OpenAI tokenizer counts. Anthropic publishes no offline tokenizer,
so Claude's were not measured, and nothing about base64 suggests they would be kinder.
The request does not finish. Bigger context windows do not help.

There is a second wall that has nothing to do with the model. The official Python
SDK's Streamable HTTP server rejects any request body over 4 MiB with HTTP 413
before it parses the JSON. Inline transfer of anything but a small file fails in the
transport regardless of who produced the bytes.

Hosts have no standard way around this. Claude.ai, Claude Desktop, Cursor and VS
Code have no path for a user-attached file to reach an MCP tool. ChatGPT has one, and
it is proprietary. So a server either accepts base64 and works for toy files, takes a
local path and only works on one machine, or fetches a URL the model supplies and
becomes a request forgery. The full argument, with the measurements, is at
https://imaadkhan.me/writing/the-file-shaped-hole-in-mcp.html.

A backend that already issues presigned upload URLs does not fix this. That keeps
bytes off your application server, which is a different hop from the one that fails
here: a tool in front of such a backend still takes base64 in an argument, and the
model still has to type it.

The MCP changelog for 2026-07-28, in the note on removing sessions, describes the
replacement for cross-call state: "explicit, server-minted handles passed as ordinary
tool arguments". An upload ticket is that handle.

## How it works

```
  model / client                     your MCP server                    your backend
  --------------                     ---------------                    ------------
  tools/call request_upload   -->    issue a ticket
                              <--    { status: awaiting_upload,
                                       upload: { url: ".../upload/<ticket>" } }

  whoever has the file
  POST <url> multipart/form-data --> header checks, then redeem the
                                     ticket atomically, then stream   -->   PUT /files/x
                                     the bytes as they arrive         <--   201
                              <--    { status: completed, file: { size, digest } }

  tools/call check_upload     -->    read the record
                              <--    { status: completed, file: {...} }
```

1. A tool is called and finds it needs a file. It asks the gateway for a ticket for a
   named destination and returns the upload URL.
2. The bytes are posted to that URL by whoever holds them: a person in a browser (a
   `GET` on the URL renders a form), an agent with a shell (`curl -F file=@path <url>`),
   or a program.
3. The gateway checks the request headers, redeems the ticket in one atomic step so
   only one request can ever use it, then parses the multipart body incrementally and
   forwards the bytes to the destination through a bounded queue. Nothing is buffered
   in memory beyond a few chunks and nothing is written to disk.
4. The record survives redemption and holds the outcome: the filename, media type,
   size, SHA-256, or a failure code. A tool reads it back on request.

## Who can complete an upload

The upload URL is the whole interface, so anything that can make an HTTP request
with the bytes can finish the job. What differs is who does it.

| Where the tool is called | Who sends the bytes |
|---|---|
| Claude Code, or any agent with a shell | The agent itself, with `curl -F file=@path <url>`. |
| Claude.ai, Claude Desktop, ChatGPT, Cursor, VS Code | The person, by opening the URL. The page is a file picker and a button. |
| A client that supports URL-mode elicitation | The client shows the link and asks for consent, through the two-round flow below. |
| Your own program | It posts the file, then asks the server what happened. |

No host today acts on an upload request by itself, and none renders a native file
picker for MCP. The browser page exists so the pattern works everywhere anyway.

## Install

```
pip install "mcp-upload[mcp]"        # official SDK, mcp 2.x
pip install "mcp-upload[fastmcp]"    # FastMCP 3.x
```

The two extras cannot be installed together. FastMCP 3.x pins the official SDK below
2.0, and the `mcp` extra targets 2.x. Pick the one your server uses. The core has no
dependency on either.

## Use it

Declare where uploads may go, build a gateway, attach it to your server, and return
what the gateway describes.

```python
from mcp.server.mcpserver import MCPServer
from mcp_upload import Destination, MemoryStore, Registry, UploadGateway
from mcp_upload.adapters.mcp import attach
from mcp_upload.types import AwaitingUpload, UploadStatus

registry = Registry(
    Destination(
        name="reports",
        url="https://api.internal/reports/{filename}",
        method="PUT",
        max_size=50 * 1024 * 1024,
        accept=("application/pdf", "text/*"),
    )
)

gateway = UploadGateway(
    base_url="https://mcp.example.com",  # where clients reach the upload endpoint
    registry=registry,
    store=MemoryStore(),
    server_name="example",
)

mcp = MCPServer("example")
attach(mcp, gateway)  # serves GET and POST /upload/{ticket}


@mcp.tool()
async def request_upload() -> AwaitingUpload:
    """Ask for a report. Returns a single-use upload URL valid for fifteen minutes."""
    issued = await gateway.issue("reports", caller="request_upload")
    return gateway.describe(issued)


@mcp.tool()
async def check_upload(id: str) -> UploadStatus:
    return await gateway.status(id)


app = mcp.streamable_http_app()
```

With FastMCP the only differences are `from fastmcp import FastMCP`,
`from mcp_upload.adapters.fastmcp import attach`, and `app = mcp.http_app()`.

`request_upload` returns this, shaped like the transfer descriptor in the MCP file
transfer proposal (SEP-2631), so a server built on this library reads the same on the
wire as the proposal:

```json
{
  "status": "awaiting_upload",
  "id": "up_896t-nivLU7Q",
  "file": { "uri": "mcp-file://example/up_896t-nivLU7Q" },
  "upload": {
    "transport": "https",
    "method": "POST",
    "url": "https://mcp.example.com/upload/_87OdaFTiH0cgPtDfBiZ0L30x--2AnpIUcp92iez_F0",
    "multipart": { "fileField": "file" },
    "expiresAt": "2026-08-31T02:09:11Z"
  }
}
```

Then send the file:

```
curl -F file=@report.pdf https://mcp.example.com/upload/_87OdaFT...
```

or open that URL in a browser. The response, and later `check_upload`, report:

```json
{
  "id": "up_896t-nivLU7Q",
  "status": "completed",
  "file": {
    "uri": "mcp-file://example/up_896t-nivLU7Q",
    "name": "report.pdf",
    "mimeType": "application/pdf",
    "size": 5000000,
    "digest": { "algorithm": "sha-256", "value": "5d3cb542..." }
  }
}
```

### Asking through the client

Since protocol 2026-07-28 a server cannot open a request to the client in the middle
of a tool call. What it can do is return `input_required`, naming what it needs. The
client collects it and retries the call with the answer attached. `ask_for_upload`
drives that in one line, using URL-mode elicitation so the client shows the upload
link and asks the user for consent:

```python
from mcp.server.mcpserver import Context
from mcp_types import InputRequiredResult
from mcp_upload.adapters.mcp import ask_for_upload


@mcp.tool()
async def request_upload_interactive(ctx: Context) -> UploadStatus | InputRequiredResult:
    return await ask_for_upload(ctx, gateway, "reports", message="Upload the report here.")
```

The first round mints the ticket and returns the elicitation. The retry reports the
record's status, or `declined` or `cancelled` if the user refused. One ticket is
minted across both rounds. This needs a client that supports URL-mode elicitation.
The official SDK's `Client` does, most chat hosts do not yet, and the plain
`request_upload` tool above works regardless.

## Where the bytes go

A `Destination` is an HTTP endpoint you declare at startup. `url` may contain `{id}`
and `{filename}`, filled in at upload time and percent-encoded. `encoding="raw"`
(the default) sends the bytes as the request body with the file's media type.
`encoding="multipart"` wraps them in a single-part form body under `field_name` for
backends that expect a form upload. `max_size` and `accept` are defaults for tickets
issued against the destination. A ticket can be issued with tighter limits, never
looser.

Tools pick a destination by name. There is no way to pass a URL, a host or a path
from a tool argument, and that is deliberate. Letting a model hand the server a URL to
fetch is a request forgery. Letting it hand the server a URL to stream a file into is
the same hole with a body attached. The unsafe shape is not discouraged, it is
unrepresentable.

What the backend has to do: accept a streamed body. It will not get a `Content-Length`
(the gateway forwards as the bytes arrive), and its response body is never passed
through to the uploader. A failure becomes one of a closed set of codes. The gateway
also holds back the end of the upstream body until the whole incoming request has
been parsed, so if something objectionable follows the file part, the backend sees an
incomplete request rather than a committed upload. A backend should treat an
incomplete body as a failed upload. `examples/backend.py` shows the shape: write to a
temporary name, rename on success, delete on an incomplete body.

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
`completed` or `failed`, and stays until its retention deadline (24 hours by default)
so "did that upload finish?" has an answer. Redemption stops being allowed at
`expires_at` (15 minutes by default). The two clocks are independent.

Two stores ship. `MemoryStore` is for one process and for tests. It is correct under
concurrency only because its redeem does the read, check and write with no `await`
in between, and it has a record cap so ticket issuance cannot grow memory without
bound. `SqliteStore` is for one host with several workers: redemption is one
conditional `UPDATE` whose `WHERE` clause carries the status and expiry checks, so
the database serializes competing writers and exactly one sees a row change. Both
were checked with fifty concurrent redemptions of one ticket and one winner. The
`Store` protocol is six methods. Bring your own for anything else.

## What the endpoint refuses, and when

The order matters. Nothing that can be judged from the headers alone may cost the
ticket, so a stray or hostile non-multipart POST cannot burn someone's pending upload.

1. Content type must be exactly `multipart/form-data` (case-insensitive, since a
   naive exact comparison rejects valid uppercase) with a boundary. Otherwise 415 or
   400, ticket untouched. Framework form helpers that also accept URL-encoded bodies
   are how a text field ends up read as a file.
2. A declared `Content-Length` far over the limit is refused with 413, ticket untouched.
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
| `too_many_uploads` | 503 | `max_in_flight` reached, ticket untouched, `Retry-After` set |
| `missing_file`, `duplicate_file`, `unexpected_part`, `bad_multipart`, `truncated` | 400 | |
| `invalid_media_type` | 400 | Declared type is not a valid `type/subtype` token |
| `unsupported_media_type` | 415 | Declared type not in the accept list |
| `client_disconnected` | 400 | Recorded on the record. No response reaches the client. |
| `upstream_unreachable`, `upstream_rejected`, `upstream_closed_early` | 502 | Backend failure, mapped, never echoed |

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

## Compared with the alternatives

**Base64 in a tool argument.** Works for toy files. Fails on anything real, and
fails in the transport at 4 MiB before the model's output ceiling is even a factor.

**A backend that already issues presigned upload URLs.** Canvas works that way, so
does S3 presigned POST, and structurally it is the same ticket. It applies at a
different hop. Keeping bytes off your application server and keeping them out of the
model are separate properties, and a tool wrapping such a backend still takes base64
in an argument unless its own interface hands the ticket outward. This library is
about the second property. It does not do the first: the gateway stays in the byte
path on purpose, which is what lets it enforce the size cap on what actually arrives
and record whether the upload finished.

**FastMCP's `FileUpload` provider.** A drag-and-drop widget in an MCP Apps host
calls an app-only tool, so the model never types the bytes. It needs no
infrastructure and no public endpoint, and for a 2 MB PDF in a host that renders MCP
Apps it is the simpler choice. It still sends the bytes through JSON-RPC as base64,
caps at 10 MB against the transport's 4 MiB message limit, needs an Apps host, and
keys its default storage on a session id the stateless transport no longer provides.
This library wins above a few megabytes, on the stateless transport, across several
replicas, and in any host that is not an Apps host.

**The server fetches a URL the model supplies.** A request forgery from inside your
network, steered by a model-controlled string. Safe only with allowlists, redirect
handling and private-range blocking, and then only mostly.

**The MCP file transfer proposal (SEP-2631).** Same architecture, same vocabulary
(`FileValue`, `FileTransferDescriptor`, `transferModes`). In the proposal the client
asks for an upload authorization, uploads, and then calls the tool with a file URI.
The tool-first ordering used here is the proposal's stated fallback for when the
client cannot bind the file before the call, which today is every host. The record
carries a stable `mcp-file://` URI from the start so that a `files/authorizeUpload`
adapter is a thin addition when the proposal lands.

## Deploying it

Four things the library cannot do for you.

- **TLS.** The ticket travels in the URL. Serve the endpoint over HTTPS only and set
  `base_url` to the `https` origin clients will use.
- **Limits.** Set `max_in_flight` on the gateway. Each streaming upload holds a parser,
  a queue and a backend connection, and beyond the cap a request gets 503 with
  `Retry-After` before its ticket is touched. Per-client rate limiting and a request
  size ceiling still belong at your reverse proxy. Both stores cap the number of
  records they hold (`max_records`) and sweep expired ones when full.
- **Destinations.** A destination is an address inside your network that the server
  streams client-supplied bytes to. Register only endpoints built to receive uploads.
  `Destination.headers` is where a backend credential goes if one is needed. It stays
  in memory and is never logged or returned.
- **Logs.** The library logs through the `mcp_upload` logger: record ids, destination
  names and outcome codes, never the ticket or the upload URL. Your access logs will
  hold the ticket URL, so scrub the path or accept that a leaked log yields tickets
  that expire in fifteen minutes and work once.

Vulnerabilities go through GitHub's private reporting on this repository. See
`SECURITY.md`.

## Limits and non-goals

Upload only. Server-to-client delivery is already covered by MCP resources. One file
per ticket. No resumable or chunked uploads. No content sniffing: the accept list is
checked against the declared type, and that is a policy check, not a security
guarantee. No storage of its own: bytes go to your backend and nowhere else. A Redis
store and the proposal-shaped `files/authorizeUpload` adapter are planned. The
`Store` protocol and the stable record URI are the seams for them.

## Running the example

```
pip install "mcp-upload[mcp]" uvicorn
python examples/backend.py     # a stub backend on :8001 that writes files to a temp dir
python examples/server.py      # the MCP server on :8000, upload endpoint under /upload
python examples/client.py path/to/any/file
```

The client asks `request_upload`, posts the file, and calls `check_upload`. For the
other two paths, call `request_upload` from any MCP client and either open the URL
in a browser or run `curl -F file=@path <url>`.

## Development

```
uv venv .venv && uv pip install --python .venv/bin/python -e ".[dev,mcp]"
uv venv .venv-fastmcp && uv pip install --python .venv-fastmcp/bin/python -e ".[dev,fastmcp]"
.venv/bin/ruff check . && .venv/bin/mypy && .venv/bin/pytest
```

Two environments because the two frameworks cannot share one. The suite includes
socket-level tests that run the gateway and a backend under uvicorn on loopback.

## Provenance

The design comes from production systems I built and ran, in C# on ASP.NET Core, and
rebuilt here in Python. The code, the tests and this README were written with AI
assistance (Claude Code) under my direction and review.

The wire shapes follow the MCP file transfer proposal,
[SEP-2631](https://github.com/modelcontextprotocol/modelcontextprotocol/pull/2631), by
Casey Chow (OpenAI). The pattern itself has been arrived at independently by several
implementers, among them
[Notion's MCP server](https://developers.notion.com/guides/mcp/mcp-supported-tools),
[FutureSearch](https://futuresearch.ai/blog/mcp-large-dataset-upload/) and
[zenk-co/mcp-upload-kit](https://github.com/zenk-co/mcp-upload-kit). The earliest
request for it in the MCP repository is
[discussion #1197](https://github.com/modelcontextprotocol/modelcontextprotocol/discussions/1197),
from December 2024.

## License

Apache 2.0. See LICENSE.
