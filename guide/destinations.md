# Where the bytes go

Destinations: an HTTP backend, a function in your own process, or the ready-made
filesystem sink, plus raw-body uploads and the browser upload page. Back to the
[README](https://github.com/imaad786/mcp-upload/blob/main/README.md).

## Destinations

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
incomplete body as a failed upload.
[`examples/backend.py`](https://github.com/imaad786/mcp-upload/blob/main/examples/backend.py)
shows the shape: write to a temporary name, rename on success, delete on an incomplete
body.

## Function destinations

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

## The filesystem sink

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
filesystem that honors fsync. If the directory sync fails, the file is removed and the
upload fails. With `fsync=False` both syncs are skipped, and a crash soon after
completion can lose the file but never leave a partial one under the final name. A
process killed mid-upload leaves its `.upload-*.part` file behind. Sweep those at
startup if that matters. File operations run on a thread pool owned by the sink, so a
slow disk does not block the event loop.

## Raw-body uploads

Some clients find a raw body easier than a form: `curl -T`, an uploader written for
presigned PUT URLs, a script that already has the bytes. Build the gateway with
`raw_uploads=True` and a POST or PUT whose Content-Type is not `multipart/form-data`
is taken as the file itself:

```
curl -T report.pdf -H "Content-Type: application/pdf" \
     -H 'Content-Disposition: attachment; filename="report.pdf"' <url>
```

- The media type is the request's Content-Type, held to the same grammar and accept
  list as a form part's. Parameters such as `charset` are kept and forwarded, and the
  accept list is matched on the type alone. Without one it is `application/octet-stream`.
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
