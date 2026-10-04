# Deploying it

What a deployment has to supply that the library cannot, what changes with bearer
tokens and destination choice, and where to report a vulnerability. Back to the
[README](https://github.com/imaad786/mcp-upload/blob/main/README.md).

## Four things to supply

Four things the library cannot do for you.

- **TLS.** The ticket travels in the URL, or a bearer token in a header. Serve the
  endpoint over HTTPS only and set `base_url` to the `https` origin clients will use.
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

## Bearer tokens on uploads

A gateway built with `authenticate` checks a bearer token on every upload, and with
`ticket_in_url=False` the token is the only credential. See
[Requiring a bearer token](https://github.com/imaad786/mcp-upload/blob/main/guide/usage.md#requiring-a-bearer-token)
for the modes. What changes for a deployment:

- **Use the server's own verifier.** Build the authenticator from the same token
  verifier or auth provider that guards the MCP route, with the same required scopes,
  so a token that cannot call a tool cannot upload either.
- **Forward the Authorization header.** A proxy or gateway in front of the upload path
  must pass `Authorization` through, as it does for the MCP route.
- **Owners are mandatory in bearer-only mode.** Issue every ticket with
  `owner=current_principal()` and give the extension an `owner` resolver. A token
  whose verifier sets no subject identifies a client app, not a user, so pass a
  `principal` function that picks a user claim in that case.
- **People cannot upload in these modes.** The upload page explains that the program
  that asked for the file sends it. Keep a ticket-only gateway for flows where a
  person opens the link.
- **Logs get simpler.** With `ticket_in_url=False` the upload URL carries the record
  id and no secret, so access logs no longer hold a usable credential. The bearer token
  travels in a header, which most access logs leave out. Check that yours do.
- **Sender-constrained tokens.** A DPoP proof is bound to a URL and method, so a token
  that requires DPoP will not verify on the upload URL unless your verifier checks a
  proof made for it. Plain bearer tokens work as they do on the MCP route.

## Letting clients choose a destination

`UploadTicketExtension(..., destinations=[...])` lets a client pick one of those names
for `files/authorizeUpload`. List only destinations that any caller of the method may
write to, since the choice is the client's. The owner check, the size and type limits
and the closed registry all still apply, and a client can only send a name.

## Several replicas

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
