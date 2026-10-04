# Why it exists

Why a file cannot reach an MCP tool today, how this library compares with the other ways
around that, and what it leaves out. Back to the
[README](https://github.com/imaad786/mcp-upload/blob/main/README.md).

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
carries a stable `mcp-file://` URI from the start, and since 1.0 the library also
serves the proposal's own `files/authorizeUpload`, so a client that can bind the file
first does that instead.

## Limits and non-goals

Upload only. Server-to-client delivery is already covered by MCP resources. One file
per ticket. No resumable or chunked uploads. No content sniffing: the accept list is
checked against the declared type, and that is a policy check, not a security
guarantee. No storage of its own: bytes go to your backend or your sink and nowhere
else (the filesystem sink writes where you point it). `files/authorizeUpload`
follows a proposal that may still change. The Tasks extension tier is not built: the
official SDK's 2.2.0 release notes still list SEP-2663 as unimplemented, so there is
nothing to negotiate against yet.
