# Using it

How an upload flows, who sends the bytes, and every way a server can ask for a file,
from a plain tool to SEP-2631's `files/authorizeUpload`, with or without a person
present, and with or without a bearer token on the upload. Back to the
[README](https://github.com/imaad786/mcp-upload/blob/main/README.md).

Install first, with `pip install "mcp-upload[mcp]"` for the official SDK or
`pip install "mcp-upload[fastmcp]"` for FastMCP 4.x.

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
| A client that supports URL-mode elicitation | The client shows the link and asks for consent, through the two-round flow in [Asking through the client](#asking-through-the-client). |
| A harness or scheduled agent with no person | It reads the upload target from the tool's `input_required` result and sends the bytes itself, as in [A harness with no person](#a-harness-with-no-person). |
| Your own program | It posts the file, then asks the server what happened. |

No host today acts on an upload request by itself, and none renders a native file
picker for MCP. The browser page exists so the pattern works everywhere anyway.

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
record: `completed` with the file's measured size and digest, why it failed, or
`declined` or `cancelled` if the user refused. If the client retries before the upload
has finished, the tool returns the same request again for the same ticket, so the
client can keep asking until the file is in. Before 1.1.0 it returned `issued` there.
One ticket is minted however many rounds it takes. This needs a client that supports
URL-mode elicitation. The official SDK's `Client` does, most chat hosts do not yet,
and the plain `request_upload` tool above works regardless.

The ask can declare the file first, as `files/authorizeUpload` does:

```python
return await ask_for_upload(
    ctx,
    gateway,
    "reports",
    name="q3.pdf",
    media_type="application/pdf",  # the only type the ticket accepts
    expected_size=248123,  # exact, so other bytes are refused
    expected_digest="uU0nuZNNPgilLlLX2n2r-sSE7-N6U4D6ZVe-_rYh2sU",
)
```

`raw=True` describes a raw-body PUT instead of a form post, on a gateway built with
`raw_uploads=True`. On FastMCP 4 the helper is the same function, imported from
`mcp_upload.adapters.fastmcp`, and the tool takes FastMCP's `Context`.

### A harness with no person

A scheduled job or a headless agent has no one to click a link. It can still answer
`ask_for_upload`, because the `input_required` result carries the whole upload target
in its `_meta`, next to the URL meant for a person:

```json
{
  "resultType": "input_required",
  "inputRequests": {
    "upload": {
      "method": "elicitation/create",
      "params": { "mode": "url", "message": "...", "url": "https://mcp.example.com/upload/..." }
    }
  },
  "requestState": "...",
  "_meta": {
    "me.imaadkhan/upload-ticket": {
      "targets": {
        "upload": {
          "file": { "uri": "mcp-file://example/up_...", "name": "q3.pdf", "size": 248123 },
          "upload": { "transport": "https", "method": "POST", "url": "...", "multipart": { "fileField": "file" } }
        }
      }
    }
  }
}
```

`targets` is keyed like `inputRequests`. Each target is the same `{file, upload}` pair
that `files/authorizeUpload` returns, so code that handles one handles both. A client
that knows nothing of this sees an ordinary URL elicitation. The target is on the
result rather than on the elicitation because the 2026-07-28 schema gives URL
elicitation params no `_meta`, and the official SDK drops unknown fields there.

The harness sends the bytes and retries. `mcp_upload.client` has the pieces:

```python
from mcp_types import ElicitResult, InputRequiredResult
from mcp_upload.client import send_file, upload_proof, upload_targets

result = await client.session.call_tool("fetch_report", allow_input_required=True)
while isinstance(result, InputRequiredResult):
    answers = {}
    for key, target in upload_targets(result).items():
        stored = await send_file(target, "q3.pdf", bearer=access_token)
        answers[key] = ElicitResult(action="accept", _meta=upload_proof(stored))
    result = await client.session.call_tool(
        "fetch_report",
        input_responses=answers,
        request_state=result.request_state,
        allow_input_required=True,
    )
```

`send_file` posts a form or a raw body, whichever the descriptor says, and returns
the file as the server measured it. `upload_proof` puts that file's URI and digest in
the answer's `_meta` under `me.imaadkhan/upload-ticket`, as `{"file": {"uri", "digest"}}`.
The tool compares them with its record and returns `failed` with `proof_mismatch` if
either differs. Proof is optional. Without it the record alone decides, and an answer
that claims an upload the record does not show is simply asked again.

### Requiring a bearer token

By default the ticket in the URL is the only credential, which is what lets a person
upload from a browser. Some deployments cannot accept a credential in a URL at all, and
want every request, uploads included, to carry the same bearer token as their MCP
requests. Give the gateway an authenticator built from the verifier and the auth
settings the server already uses:

```python
from mcp_upload.adapters.mcp import ask_for_upload, authenticator, current_principal

verifier = MyTokenVerifier()  # the SDK's TokenVerifier, also passed to MCPServer
settings = AuthSettings(
    issuer_url=...,
    resource_server_url="https://files.example.com/mcp",
    validate_token_resource=True,
)
gateway = UploadGateway(
    ...,
    authenticate=authenticator(verifier, auth=settings),
    ticket_in_url=False,  # the token is the only credential
)
mcp = MCPServer("files", auth=settings, token_verifier=verifier)


@mcp.tool()
async def fetch_report(ctx: Context) -> UploadStatus | InputRequiredResult:
    return await ask_for_upload(ctx, gateway, "reports", owner=current_principal())
```

There are three modes:

| `authenticate` | `ticket_in_url` | An upload needs |
| --- | --- | --- |
| not set | `True` (default) | The ticket in the URL, as before 1.1.0 |
| set | `True` | The ticket and a valid token, whose principal must be the record's owner if it has one |
| set | `False` | A valid token whose principal is the record's owner. The URL names the record by its id and carries no secret |

- **The principal** is the token's client id, issuer and subject, the same parts the
  official SDK uses to tell principals apart. `current_principal()` reads it from the
  token on the tool call, and the authenticator reads it from the token on the upload,
  so the two match only for the same user. If your verifier sets no subject, pass
  `principal=` to both with a function that picks a user id claim.
- **Refusals** happen before the ticket is spent and before the record is looked up.
  No token is 401 with `WWW-Authenticate: Bearer` and `{"reason": "authRequired"}`. A
  token the verifier refuses, or one past its expiry, is 401 `invalid_token`. A token
  issued for another resource is 401 `invalid_token` with
  `{"reason": "wrongResource"}`. A valid token without a scope the authenticator
  requires is 403 `insufficient_scope`, as the official SDK's MCP endpoint answers it
  (RFC 6750 section 3.1), with
  `WWW-Authenticate: Bearer error="insufficient_scope", error_description="Required scope: files", scope="files"`
  and `{"reason": "insufficientScope", "required": ["files"]}`. Another user's valid
  token is 403 `forbidden` with `{"reason": "ownerMismatch"}`.
- **The same audience as the MCP endpoint.** With `auth=settings`, the upload route
  checks a token as the MCP endpoint does: the settings' `required_scopes`, and with
  `validate_token_resource=True` only tokens whose `resource` is `resource_server_url`,
  compared as the SDK compares it. A token with no `resource` is refused there too.
  `resource="https://..."` binds to another URL, and `resource=False` turns the check
  off. Without `auth`, neither is checked, as in 1.1.0. On `mcp` 2.1 the settings have
  no `validate_token_resource` and tokens no `resource`, so nothing is bound unless you
  pass `resource`, and then only tokens whose verifier reports one get through.
- **Single use still holds** with no secret in the URL. The URL is public and the
  owner's token is reusable, so the gateway redeems through the hash the record
  stores, in the same atomic step as a ticket. The test suite sends fifty concurrent
  uploads to one record on each store and expects one winner, and CI runs the Redis
  case against a real Redis.
- **The browser page cannot send a header**, so with `authenticate` set a `GET` on the
  upload URL explains that the program that asked for the file sends it, instead of
  showing a form. This mode is for harnesses and programs, not for people.
- **The descriptor says so.** Every `FileTransferDescriptor` from such a gateway
  carries `"_meta": {"me.imaadkhan/upload-ticket": {"auth": "bearer"}}`, meaning send
  the token you use for MCP requests to this server. No token is ever put in it.
  `send_file(..., bearer=token)` and `upload_file(..., bearer=token)` send it.
- **Without a token-checked owner nothing guards the upload**, so with
  `ticket_in_url=False` every ticket must be issued with an `owner`. `issue` raises
  without one, and `files/authorizeUpload` answers -32603 with
  `{"reason": "ownerRequired"}`.

On FastMCP 4, `authenticator(mcp)` uses the server's own auth provider and its
required scopes, and `current_principal()` reads FastMCP's access token. FastMCP leaves
the token's audience to the provider (`JWTVerifier(audience=...)`), whose
`verify_token` the upload route calls too. `resource=` adds the SDK's resource check on
top, for a provider that sets `AccessToken.resource`; the providers in FastMCP 4.0.10
do not. FastMCP 4.0.10's `JWTVerifier` and `StaticTokenVerifier` also refuse a token
without a required scope inside `verify_token`, so both routes answer it with 401
`invalid_token`. A provider that leaves scopes to FastMCP's middleware gets 403
`insufficient_scope` from both.

```python
from mcp_upload.adapters.fastmcp import authenticator, current_principal

mcp = FastMCP("files", auth=provider)
gateway = UploadGateway(..., authenticate=authenticator(mcp), ticket_in_url=False)
```

Any other scheme fits too. `authenticate` is any coroutine function that takes the
Starlette request and returns a principal or `None`, and
`mcp_upload.auth.bearer_authenticator` builds one from any object with
`async verify_token(token)`, and takes `resource=` for the same binding. An
authenticator can raise `mcp_upload.auth.TokenRefused(reason)` to answer 401
`invalid_token` with that reason, or `mcp_upload.auth.InsufficientScope(required)` to
answer 403 `insufficient_scope`.

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

With the OpenTelemetry API installed (the official SDK depends on it, and the `otel`
extra installs it otherwise) and an SDK configured, the gateway emits a span per upload and metrics
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
tool for an upload link: "I am about to upload this name, type, size and digest.
Where do I send it?" The server answers with the file's future URI and a transfer
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

- **`destination`** is where an authorized upload goes. By default the request
  cannot pick another. See [Letting the client choose a destination](#letting-the-client-choose-a-destination).
- **`owner`** maps the request to the user the ticket is bound to. It may be a plain
  function or a coroutine function. Without it, anyone who learns a file URI can
  read the file's name, size and digest.
- The declared `size` becomes the ticket's exact size and the declared `digest` its
  exact SHA-256, so different bytes are refused with `digest_mismatch` or
  `size_mismatch` before the backend commits them. A declared `mimeType` becomes the
  ticket's only accepted type. Parameters such as `charset` are kept: the result
  echoes `text/plain; charset=utf-8` exactly as sent, and the upload is matched on
  `text/plain` alone.
- Some clients send the file part with no `filename`. The library accepts it, since
  the part is under the field the descriptor named for the file, and names the file
  `upload`. A part under any other field name is still refused.

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
| -32602 | A destination the server does not offer | `{"reason": "destinationNotAllowed", "allowed": ["reports", "archive"]}` |
| -32603 | The ticket store is full | `{"reason": "storeFull"}` |
| -32603 | A bearer-only gateway and no owner for the request | `{"reason": "ownerRequired"}` |

### Letting the client choose a destination

Some backends need the final location of a file when the upload starts, and a client
may have one to pick, such as a team folder or an archive. The extension can offer a
closed list of destination names:

```python
UploadTicketExtension(
    gateway,
    destination="reports",  # used when the request names none
    destinations=["archive", "legal"],  # names a client may choose instead
    owner=user_of,
)
```

SEP-2631's request has no destination field, so the client names one in the request's
`_meta`, under the extension's identifier:

```json
{
  "method": "files/authorizeUpload",
  "params": {
    "name": "q3.pdf",
    "size": 248123,
    "_meta": { "me.imaadkhan/upload-ticket": { "destination": "archive" } }
  }
}
```

`authorize_upload(..., destination="archive")` and `upload_file(..., destination=...)`
send it. Any name outside the list, including one that is registered but not offered,
is -32602 with `{"reason": "destinationNotAllowed", "allowed": ["reports", "archive",
"legal"]}`, and no ticket is minted. The chosen destination's own size and type limits
apply. The list is advertised at
`capabilities.extensions["me.imaadkhan/upload-ticket"]["destinations"]`. A client only
ever sends a name. It cannot supply a URL, host or path, and the registry stays the
closed set the server author declared. Without `destinations`, naming anything but the
default is refused, and the settings are exactly as before.

**Discovery is not where SEP-2631 puts it.** The proposal has client and server
declare a top-level `files` capability. The SDK's `ClientCapabilities` and
`ServerCapabilities` drop keys they do not know, so neither side can declare it
through the SDK today. The server instead lists the method in its extension settings,
at `capabilities.extensions["me.imaadkhan/upload-ticket"]["methods"]`, which clients
see on protocol 2026-07-28 connections. A client that does not find it there should
send the request anyway and treat -32601 as "not supported". On the server, a raw
`files` capability a client sent is readable at
`ctx.params["_meta"]["io.modelcontextprotocol/clientCapabilities"]["files"]`.

**Downloads are not served.** The library does not serve `files/authorizeDownload`,
because downloads are a non-goal. A host that sees an `mcp-file://` URI in a tool
result may try to download it through that method and fail. So if your tool's
callers include hosts that do that, such as some gateways, return the file's metadata
without the `uri` member. Keep the `uri` only when you know they will not try.

SEP-2631 is a proposal, not part of the specification, and the method name and
shapes may change before it lands.

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

The examples live in the repository's
[`examples/`](https://github.com/imaad786/mcp-upload/tree/main/examples) folder.
