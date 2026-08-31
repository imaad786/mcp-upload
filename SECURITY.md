# Security

## Reporting a vulnerability

Use GitHub's private vulnerability reporting on this repository: the Security tab,
then "Report a vulnerability". Please do not open a public issue for anything that
could be exploited before it is fixed. You will get a first reply within a week, and a
fix or a clear answer as soon as the problem is understood.

## Supported versions

The latest release on PyPI. Fixes are not backported.

## What the library guarantees

The upload endpoint is protected by the ticket alone. That is safe because the ticket
is 256 bits from the operating system's random source, stored only as its SHA-256,
accepted exactly once (enforced atomically in the store), expiring in minutes, bound to
one destination the server author registered, and useless for reading anything back.

Beyond that, the library refuses requests that cannot carry a file before the ticket
is touched, enforces the size limit on the bytes it receives rather than on the
declared length, accepts exactly one file part with a valid media type and a sanitized
filename, streams to the destination without writing to disk, holds the end of the
backend request until the whole upload has validated, never echoes a backend response,
and never lets a tool argument become a URL, host or path.

## What it does not guarantee, and what a deployment must do

- **Transport security.** The ticket travels in the URL. Serve the endpoint over TLS
  only. Set `base_url` to an `https` origin.
- **Rate limiting.** The library caps concurrent uploads when `max_in_flight` is set,
  and refuses beyond it with 503. Per-client rate limiting and request-size limits at
  the reverse proxy are still yours to configure.
- **Destinations.** A destination is an address inside your network that the server
  will stream client-supplied bytes to. Register only endpoints built to receive
  uploads. Nothing a client sends can change which destination is used, but the
  destination itself is your choice.
- **Content.** The declared media type is checked against the accept list. The bytes
  are not inspected. If the backend must not receive certain content, the backend has
  to check.
- **Logs.** The library logs record ids and outcome codes and never the ticket. Your
  access logs will contain the ticket URL. Scrub the path or accept that a leaked log
  yields tickets that expire in fifteen minutes and work once.

## Supply chain

Releases are published from this repository's `publish.yml` workflow through PyPI
trusted publishing, so no publishing token exists to steal, and PyPI holds provenance
attestations for every file. Actions are pinned to commit SHAs, dependencies are
resolved from a committed lockfile in CI, and every CI run audits the locked
dependencies against known-vulnerability databases.
