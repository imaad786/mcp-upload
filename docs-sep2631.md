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
