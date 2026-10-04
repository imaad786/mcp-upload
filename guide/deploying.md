# Deploying it

What a deployment has to supply that the library cannot, and where to report a
vulnerability. Back to the
[README](https://github.com/imaad786/mcp-upload/blob/main/README.md).

## Four things to supply

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

Running several replicas behind a load balancer needs `RedisStore`, and a rolling
upgrade from 0.4.0 needs `legacy_layout=True`. Both are covered in
[The stores](https://github.com/imaad786/mcp-upload/blob/main/guide/ticket.md#the-stores).
Why the ticket alone is enough to authorize an upload is in
[The ticket](https://github.com/imaad786/mcp-upload/blob/main/guide/ticket.md#the-ticket).

## Security

Vulnerabilities go through GitHub's private reporting on this repository. See
[`SECURITY.md`](https://github.com/imaad786/mcp-upload/blob/main/SECURITY.md), which
also has the threat model: who can reach the endpoint, what each of them can do, and
what a deployment must do.
