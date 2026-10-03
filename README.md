# mcp-upload

File uploads for MCP servers, using short-lived upload tickets.

An MCP tool that needs a file hands back a single-use URL. Whoever holds the file
posts it there over plain HTTPS. The server streams the bytes straight through to
the backend you already have, and keeps a small record of what happened. Nothing
about the file ever travels through MCP except a reference to it. No change to the
protocol is needed, and no host has to know the library exists.

Targets protocol revision 2026-07-28. Works with the official Python SDK (`mcp` 2.x)
and with FastMCP (`fastmcp` 4.x). Python 3.11 or later. Apache 2.0.

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
   in memory beyond a few chunks, and the gateway writes nothing to disk unless the
   destination is the filesystem sink.
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
pip install "mcp-upload[fastmcp]"    # FastMCP 4.x
```

Pick the one your server uses. FastMCP 4.x builds on the official SDK 2.x, so the two
can also be installed together. The core has no dependency on either.

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
    "digest": { "algorithm": "sha-256", "value": "XTy1QnS2..." }
  }
}
```

The digest value is base64url without padding, as SEP-2631 specifies.

### Owners, declared digests and claiming

Three options on `issue` close gaps a multi-user server has:

```python
issued = await gateway.issue(
    "reports",
    owner=user_id,  # status and claim answer only this owner
    expected_size=248123,  # declared by the uploader, if known
    expected_digest="uU0nuZNNPgilLlLX2n2r-sSE7-N6U4D6ZVe-_rYh2sU",
)
...
status = await gateway.status(record_id, owner=user_id)
file = await gateway.claim(record_id, owner=user_id)  # once; later claims raise
```

- **`owner`** binds the record to whoever asked for it. Record ids appear in
  transcripts, so without it anyone who sees an id can read the file's name, size and
  digest. With it, `status` and `claim` report another owner's record as unknown.
- **`expected_size` and `expected_digest`** are what SEP-2631's `files/authorizeUpload`
  carries. Bytes that do not match are refused with `size_mismatch` or
  `digest_mismatch`, and the backend never sees the end of the body, so a backend that
  commits only complete bodies never stores them.
- **`claim`** takes a completed upload for use exactly once, atomically in every
  store. Two tool calls naming the same file cannot both act on it. Status becomes
  `claimed`. Claiming is optional; `status` works either way.

A failed upload carries machine-readable `details` next to its error code, in the
shape SEP-2631 suggests:

```json
{
  "status": "failed",
  "error": "too_large",
  "details": { "reason": "maxSizeExceeded", "maxSize": 10485760, "receivedSize": 10485761 }
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

### After an upload

`on_complete` is awaited with the final record of every redeemed upload, completed or
failed, after the response has gone to the uploader. A server can start work on a file
without polling:

```python
async def ingest(record: Record) -> None:
    if record.status is Status.COMPLETED:
        await queue.put(record.id)


gateway = UploadGateway(..., on_complete=ingest)
```

With the OpenTelemetry API installed (the official SDK depends on it; otherwise the
`otel` extra) and an SDK configured, the gateway emits a span per upload and metrics
for tickets issued, uploads by outcome, bytes and duration, under the scope
`mcp_upload`. Without an SDK these are no-ops.

### Advertising it as an extension

A client can read a tool description in prose, but nothing machine-readable otherwise
says "this server hands out upload tickets". The 2026-07-28 protocol added an
`extensions` capability for that, and SEP-2133 requires a reverse-DNS prefix on every
identifier. Opt in when you build the server:

```python
from mcp.server.mcpserver import MCPServer
from mcp_upload.adapters.mcp import attach
from mcp_upload.adapters.mcp_extension import UploadTicketExtension

server = MCPServer("files", extensions=[UploadTicketExtension()])
attach(server, gateway)
```

That advertises `me.imaadkhan/upload-ticket` under `ServerCapabilities.extensions`.
It contributes no tools and intercepts nothing, so uploads behave identically whether
or not it is declared. It is discovery, not a feature gate, and no client looks for it
today.

### `files/authorizeUpload` (SEP-2631)

SEP-2631 proposes a request a client sends before a tool call, instead of asking a
tool for an upload link: "I am about to upload this name, type, size and digest;
where do I send it?" The server answers with the file's future URI and a transfer
descriptor. The client posts the bytes, then passes the URI to a tool as an ordinary
string argument. The extension can serve that request from the gateway.

On the official SDK:

```python
from mcp.server.auth.middleware.auth_context import get_access_token
from mcp.server.mcpserver import MCPServer
from mcp_upload.adapters.mcp import attach
from mcp_upload.adapters.mcp_extension import UploadTicketExtension


def user_of(ctx) -> str | None:
    token = get_access_token()
    return token.subject if token else None


mcp = MCPServer(
    "files",
    extensions=[UploadTicketExtension(gateway, destination="reports", owner=user_of)],
)
attach(mcp, gateway)
```

On FastMCP 4 the class has the same arguments and is registered with `add_extension`:

```python
from fastmcp import FastMCP
from mcp_upload.adapters.fastmcp import attach
from mcp_upload.adapters.fastmcp_extension import UploadTicketExtension

mcp = FastMCP("files")
mcp.add_extension(UploadTicketExtension(gateway, destination="reports", owner=user_of))
attach(mcp, gateway)
```

- **`destination`** is where every authorized upload goes. The request carries no
  destination and cannot pick one.
- **`owner`** maps the request to the user the ticket is bound to. It may be a plain
  function or a coroutine function. Without it, anyone who learns a file URI can
  read the file's name, size and digest.
- The declared `size` becomes the ticket's exact size and the declared `digest` its
  exact SHA-256, so different bytes are refused with `digest_mismatch` or
  `size_mismatch` before the backend commits them. A declared `mimeType` becomes the
  ticket's only accepted type.

`UploadTicketExtension()` with no arguments still only advertises the capability.

A tool takes the file by its URI with `resolve_file`. It checks that the URI is one
this server issued, that the upload finished, and that the caller owns it, then
claims it so a second call with the same URI is refused:

```python
from mcp.server.mcpserver.exceptions import ToolError
from mcp_upload.resolve import FileReferenceError, resolve_file
from mcp_upload.types import FileValue


@mcp.tool()
async def ingest(file: str) -> FileValue:
    try:
        return await resolve_file(gateway, file, owner=current_user())
    except FileReferenceError as exc:
        raise ToolError(str(exc)) from exc
```

The official SDK hides the text of an unexpected exception from the client, so
re-raise as `ToolError` for the model to see why. `exc.reason` is one of
`malformed`, `other_server`, `not_found`, `not_completed`, `failed` or
`already_claimed`. Pass `claim=False` to read the file without using it up.

On the client, `upload_file` does the whole flow and returns the URI:

```python
from mcp.client import Client
from mcp_upload.client import upload_file

async with Client("https://files.example.com/mcp") as client:
    uri = await upload_file(client, "report.pdf")
    await client.call_tool("ingest", {"file": uri})
```

It hashes the file, sends `files/authorizeUpload` with the size and SHA-256, posts
the bytes as described, and checks the answer. The file is streamed from disk, never
loaded whole. `authorize_upload` sends only the request, for clients that upload
some other way. A refused upload raises `UploadFailed` with the endpoint's error code.

Errors follow the proposal:

| Code | When | `data` |
| --- | --- | --- |
| -32601 | The server does not serve the method | |
| -32602 | `size` over the destination's limit | `{"reason": "maxSizeExceeded", "maxSize": 1000, "actualSize": 5000}` |
| -32602 | `mimeType` outside the destination's list | `{"reason": "mimeTypeNotAccepted", "mimeType": "text/plain", "accept": ["image/*"]}` |
| -32602 | A digest that is not base64url SHA-256 | `{"reason": "invalidDigest"}` |
| -32602 | A negative size, or a malformed type | `{"reason": "invalidSize"}`, `{"reason": "invalidMimeType"}` |
| -32603 | The ticket store is full | `{"reason": "storeFull"}` |

**Discovery is not where SEP-2631 puts it.** The proposal has client and server
declare a top-level `files` capability. The SDK's `ClientCapabilities` and
`ServerCapabilities` drop keys they do not know, so neither side can declare it
through the SDK today. The server instead lists the method in its extension settings,
at `capabilities.extensions["me.imaadkhan/upload-ticket"]["methods"]`, which clients
see on protocol 2026-07-28 connections. A client that does not find it there should
send the request anyway and treat -32601 as "not supported". On the server, a raw
`files` capability a client sent is readable at
`ctx.params["_meta"]["io.modelcontextprotocol/clientCapabilities"]["files"]`.

SEP-2631 is a proposal, not part of the specification, and the method name and
shapes may change before it lands.

## Where the bytes go

A `Destination` is an HTTP endpoint or an async function you declare at startup.
Either way, `max_size` and `accept` are defaults for tickets issued against it. A
ticket can be issued with tighter limits, never looser.

For an HTTP endpoint, `url` may contain `{id}` and `{filename}`, filled in at upload
time and percent-encoded. `encoding="raw"` (the default) sends the bytes as the
request body with the file's media type. `encoding="multipart"` wraps them in a
single-part form body under `field_name` for backends that expect a form upload.

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

### Function destinations

When the bytes should land in your own code (local disk, an object store's SDK, a
pipeline in the same process), register an async function as `sink` instead of a
`url`, and no second service is needed:

```python
from mcp_upload import Destination, IncomingFile, Registry, UploadAborted


async def store_scan(upload: IncomingFile) -> None:
    # storage stands for your own code: an object store client, a database, a queue.
    part = await storage.begin(upload.record_id, upload.filename, upload.media_type)
    try:
        async for chunk in upload:
            await part.write(chunk)
    except UploadAborted:
        await part.discard()
        raise
    await part.commit()


registry = Registry(
    Destination(name="scans", sink=store_scan, max_size=200 * 1024 * 1024, accept=("image/*",))
)
```

A destination takes exactly one of `url` and `sink`. The registry stays closed: the
function is registered by the server author, and a tool can only name it.

The sink gets an `IncomingFile` with `record_id`, `destination`, `filename` (already
reduced to a safe base name), `media_type` (already checked against the accept list),
`expected_size` and `expected_sha256` (what was declared at issue, if anything), and
iterating it yields the bytes as they arrive. The contract is the HTTP one, made
explicit:

- Iteration ends normally only after the whole request validated, including the
  declared size and digest. A sink that commits after its `async for` loop never
  commits a bad upload.
- On any failure (the client vanished, the file was too large, the digest did not
  match, the client stalled, something followed the file part), the next read raises
  `UploadAborted`, whose `code` is the error recorded for the upload. Reads after that
  keep raising. Let it propagate, or clean up and re-raise it. A sink that swallows it
  and returns still has the upload recorded as failed, with the real cause.
- If the sink raises, the upload fails as `sink_failed` (502). The exception is logged
  through the `mcp_upload` logger and never sent to the uploader.
- If the sink returns before reading to the end, the upload fails as
  `upstream_closed_early`.
- Reads are backpressure: the gateway holds a few chunks at most, so a slow sink
  slows the client down.
- The client-side limits (`stall_timeout`, `upload_timeout`, `max_in_flight`) apply as
  they do to an HTTP destination. Once the body has stopped arriving, ended or failed,
  the sink has the destination's `timeout` (60 seconds by default) to return, or it is
  cancelled and the upload fails as `sink_failed`.

A failure the gateway detects before the file part has started (a missing or
duplicate part, a bad media type) never calls the sink, as it never opens a request
to an HTTP backend.

### The filesystem sink

`mcp_upload.sinks.filesystem` is a ready-made sink:

```python
from mcp_upload.sinks import filesystem

Destination(name="scans", sink=filesystem("/srv/uploads"), max_size=200 * 1024 * 1024)
```

Each upload is written to a temporary file in the target directory, named
`.upload-*.part` and created with mode 0600. Only after the iteration ended normally
is it synced and moved to its final name in one atomic step, so a reader of the
directory sees a complete, verified file or nothing. On any failure the temporary file
is deleted. The final name is `name_template` with `{id}` and `{filename}` filled in,
`{id}-{filename}` by default. It must be a single name inside the directory; a template
that yields a path elsewhere fails the upload. An existing file is never replaced
unless `overwrite=True`.

Durability: with `fsync=True` (the default) the file's data is synced before the
rename and the directory after it, so a completed upload survives a power loss on a
filesystem that honours fsync. If the directory sync fails, the file is removed and the
upload fails. With `fsync=False` both syncs are skipped, and a crash soon after
completion can lose the file but never leave a partial one under the final name. A
process killed mid-upload leaves its `.upload-*.part` file behind; sweep those at
startup if that matters. File operations run on a thread pool owned by the sink, so a
slow disk does not block the event loop.

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
writing the 0.4.0 key layout during a rolling upgrade, until no 0.4.0 replica is left;
records in the old layout are read either way.

## Raw-body uploads

Some clients find a raw body easier than a form: `curl -T`, an uploader written for
presigned PUT URLs, a script that already has the bytes. Build the gateway with
`raw_uploads=True` and a POST or PUT whose Content-Type is not `multipart/form-data`
is taken as the file itself:

```
curl -T report.pdf -H "Content-Type: application/pdf" \
     -H 'Content-Disposition: attachment; filename="report.pdf"' <url>
```

- The media type is the request's Content-Type, held to the same token grammar and
  accept list as a form part's. Without one it is `application/octet-stream`.
- The filename comes from a `Content-Disposition` request header, where the RFC 8187
  form `filename*=UTF-8''...` wins over plain `filename=`, else from a `filename`
  query parameter, else it is `upload`. It is reduced to a safe base name like any
  other.
- Every protection applies: the declared `Content-Length` precheck (exact, since the
  body is the file), `max_size` on the bytes seen, the declared size and digest before
  the end is forwarded, the stall and total timeouts, the drain after an early refusal,
  `max_in_flight` and the single-use ticket. Since the type is a header, a type that is
  invalid or not accepted is refused before the ticket is spent.
- A URL-encoded form (`application/x-www-form-urlencoded`, what `curl --data` and a
  form without an enctype send) is never taken as a file, nor is another multipart
  type. Both are refused with `not_multipart`, ticket untouched. Use `--data-binary`
  with an explicit Content-Type, or `-T`.

Off by default, and then the endpoint behaves exactly as before: PUT is not routed and
anything but `multipart/form-data` is refused. `describe(issued, raw=True)` advertises
the raw form as a SEP-2631 transfer descriptor with `method: "PUT"` and no `multipart`
key. Its `headers` carry a Content-Type when one is known: pass `media_type=`, or it is
taken from the ticket's accept list when that names exactly one type. The default
`describe(issued)` still describes the form POST, which every client can send.

## The upload page

A `GET` on the upload URL renders a plain form that posts back to the same URL, and
that alone works in any browser, scripts on or off. Where scripts run, a short inline
script adds drag and drop onto the page, a progress bar while the file is sent, and
the outcome (or the error code and its details) shown in place. It sends the same form
body with `XMLHttpRequest`, since fetch reports no upload progress, and asks for JSON.

The page's Content-Security-Policy stays `default-src 'none'` and admits that script by
a nonce minted for each response (`script-src 'nonce-...'`), plus `connect-src 'self'`
so it can post back to its own URL. No other script runs, inline or loaded, there are
no inline event handlers, and the page loads nothing external. Every other page the
endpoint renders keeps the policy without scripts.

## What the endpoint refuses, and when

The order matters. Nothing that can be judged from the headers alone may cost the
ticket, so a stray or hostile non-multipart POST cannot burn someone's pending upload.

1. Content type must be exactly `multipart/form-data` (case-insensitive, since a
   naive exact comparison rejects valid uppercase) with a boundary. Otherwise 415 or
   400, ticket untouched. Framework form helpers that also accept URL-encoded bodies
   are how a text field ends up read as a file. With `raw_uploads=True`, any other
   type except a URL-encoded form or another multipart type is a raw upload instead.
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
- **Limits.** `max_in_flight` (100 by default) caps concurrent uploads per process.
  Each one holds a parser, a queue and a backend connection, and beyond the cap a
  request gets 503 with `Retry-After` before its ticket is touched. The default HTTP
  client is sized to the same number. If you pass your own, give its pool at least
  `max_in_flight` connections, or uploads queue for a connection after their tickets
  are spent. Slow clients are cut off by `stall_timeout` and `stall_min_bytes` and long
  ones by `upload_timeout`. Per-client rate limiting and a request size ceiling still
  belong at your reverse proxy. Both stores cap the number of
  records they hold (`max_records`) and sweep expired ones when full.
- **Destinations.** A destination is an address inside your network, or a function
  in your process, that the server streams client-supplied bytes to. Register only
  endpoints and sinks built to receive uploads. `Destination.headers` is where a
  backend credential goes if one is needed. It stays in memory and is never logged or
  returned. A sink runs in the server's event loop: anything blocking belongs in a
  thread, as the filesystem sink does.
- **Logs.** The library logs through the `mcp_upload` logger: record ids, destination
  names and outcome codes, never the ticket or the upload URL. Your access logs will
  hold the ticket URL, so scrub the path or accept that a leaked log yields tickets
  that expire in fifteen minutes and work once.

Vulnerabilities go through GitHub's private reporting on this repository. See
`SECURITY.md`.

## Stability

From 1.0 the library follows semantic versioning. The public API is every name in
`mcp_upload.__all__`, the adapters in `mcp_upload.adapters`, `mcp_upload.sinks`,
`mcp_upload.client`, `mcp_upload.redis_store`, the error codes in `ERROR_STATUS` and
the JSON shapes the endpoint returns. Anything starting with an underscore is not.

The `files/authorizeUpload` method and the `FileValue` and `FileTransferDescriptor`
shapes follow SEP-2631, which is a proposal. If it changes before it lands, the
library will follow it in a minor release and say so in the changelog. Everything else
changes only in a major release.

## Limits and non-goals

Upload only. Server-to-client delivery is already covered by MCP resources. One file
per ticket. No resumable or chunked uploads. No content sniffing: the accept list is
checked against the declared type, and that is a policy check, not a security
guarantee. No storage of its own: bytes go to your backend or your sink and nowhere
else (the filesystem sink writes where you point it). `files/authorizeUpload`
follows a proposal that may still change. The Tasks extension tier is not built: the official SDK's 2.2.0
release notes still list SEP-2663 as unimplemented, so there is nothing to negotiate
against yet.

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

Two environments so each framework's adapter is tested without the other installed.
The suite includes socket-level tests that run the gateway and a backend under uvicorn
on loopback.

`stress/run.py` is an end-to-end harness: the gateway, a backend and the load each run
as separate processes over real sockets, and the gateway's memory is sampled from
outside. It covers sustained throughput, hundreds of concurrent uploads, slow clients,
oversized part headers, epilogues, bursts past the concurrency cap, a mixed run
with failing clients and a flaky backend, uploads into function and filesystem sinks,
and raw-body uploads, and it writes JSON so two versions can be compared:

```
.venv/bin/python stress/run.py --src src --out after.json
```

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
