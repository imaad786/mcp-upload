# Testing it

How to run the test suite, the end-to-end stress harness behind the measurements in the
changelog, the nightly job that runs it, and how to compare two versions. Back to the
[README](https://github.com/imaad786/mcp-upload/blob/main/README.md).

## Development

```
uv venv .venv && uv pip install --python .venv/bin/python -e ".[dev,mcp]"
uv venv .venv-fastmcp && uv pip install --python .venv-fastmcp/bin/python -e ".[dev,fastmcp]"
.venv/bin/ruff check . && .venv/bin/mypy && .venv/bin/pytest
```

Two environments so each framework's adapter is tested without the other installed.
The suite includes socket-level tests that run the gateway and a backend under uvicorn
on loopback.

## The stress harness

`stress/run.py` is an end-to-end harness: the gateway, a backend and the load each run
as separate processes over real sockets, and the gateway's memory is sampled from
outside. It covers sustained throughput, hundreds of concurrent uploads, slow clients,
oversized part headers, epilogues, bursts past the concurrency cap, a mixed run
with failing clients and a flaky backend, uploads into function and filesystem sinks,
raw-body uploads, bearer tokens on uploads, headless MCP flows and destination choice,
and it writes JSON so two versions can be compared:

```
.venv/bin/python stress/run.py --src src --out after.json
```

The two processes are `stress/gateway.py` and `stress/backend.py`. `headless_flow` and
`destination_choice` start `stress/mcp_server.py` in place of the gateway: an official
SDK `MCPServer` with the gateway attached, its MCP route guarded by a bearer token
verifier, a tool that asks for a file with `ask_for_upload`, and the extension
offering one destination besides the default. The harness drives it with the official
SDK client, as a headless harness would. The backend commits
an upload only when the request body ended cleanly, and records the size and SHA-256
of every commit, so the harness can check that what the gateway reported is what
actually arrived. The gateway is killed if its memory passes 6,000 MB, to protect the
machine.

Each scenario is a function in `stress/run.py`, and `--only` runs some of them by name,
for example `--only header_bomb,claim_race`. The Redis scenarios, such as
`redis_two_replicas` and `store_backed_uploads`, need a real Redis at `--redis-url`
(`redis://localhost:56379/0` by default). `bearer_auth` runs its Redis race only when
one is reachable there, and says so in its result otherwise. `sqlite_migration` needs an older tree in
`--old-src` and reports itself unsupported without one.

### Comparing two versions

`--src` picks which copy of the library the gateway imports, so the same harness runs
against a released tree and a working tree. Options a version does not know are
dropped, so each runs on its own defaults, and a scenario that needs a feature the
version lacks reports `{"supported": false}`. To compare a release with your working
tree:

```
git worktree add ../mcp-upload-0.3.0 v0.3.0
.venv/bin/python stress/run.py --src ../mcp-upload-0.3.0/src --out before.json
.venv/bin/python stress/run.py --src src --out after.json
```

`--label` names a run instead of its `__version__`. The throughput figures in the
changelog are medians of three alternating runs.

### Multipart fuzz

`stress/fuzz.py` builds multipart bodies with every legal variation the gateway has to
get right (a preamble, an epilogue of any content, a final CRLF or none, file data full
of near-miss delimiters) and splits each at random points, so delimiters land across
reads. Every upload must complete and deliver exactly the file's bytes to the backend.
It runs in one process, with the gateway and a mock backend in memory.

```
.venv/bin/python stress/fuzz.py --src src --n 3000 --seed 7
```

### SEP-2631 flows

`stress/sep_flow.py` checks `files/authorizeUpload` end to end over real sockets. It
starts the stress backend and an MCP server with the gateway attached, the extension
serving `files/authorizeUpload`, and one tool, `ingest(file)`, that resolves and claims
a file URI. Then it drives full flows with the official SDK client over Streamable HTTP
and prints counts and timings as JSON:

- `good`: authorize with size and digest, upload, call `ingest` with the URI, and check
  the returned size and digest, and the backend's commit, against what was sent.
- `mismatch`: authorize one digest, upload different bytes. The upload must be refused
  with 422 `digest_mismatch`, the backend must not commit, and `ingest` must fail.
- `oversize`: declare a size over the destination's limit. `files/authorizeUpload` must
  fail with -32602 and `maxSizeExceeded` data.
- `double`: upload, then call `ingest` twice. The second call must be refused.
- `other_owner`: upload as one user and call `ingest` as another. It must fail.

Each flow is a separate client with its own `x-user` header, and the server binds
tickets to that header so the owner check runs for real. A header is not an identity.
A real server would use the authenticated user from the access token.

```
.venv/bin/python stress/sep_flow.py --flows 50 --concurrency 50
.venv-fastmcp/bin/python stress/sep_flow.py --framework fastmcp
```

It imports the installed library, so run it with the interpreter of the environment
you want to test. It exits 1 when a flow fails.

### Checking invariants

`stress/check.py` reads what the other three produce and exits 1 with one line per
broken invariant. It checks only invariants that hold on any machine: bytes, counts of
winners, refusals and leftovers. Throughput, latency and memory figures vary with the
runner and are never compared.

```
.venv/bin/python stress/check.py after.json --require header_bomb,claim_race
.venv/bin/python stress/fuzz.py --src src > fuzz.txt
.venv/bin/python stress/sep_flow.py > sep_flow.json
.venv/bin/python stress/check.py --fuzz fuzz.txt --sep-flow sep_flow.json
```

A scenario, or a part of one, that reports `{"supported": false}` is skipped, unless it
is named in `--require`: a required scenario must be present and supported.

## The nightly job

[`.github/workflows/stress.yml`](https://github.com/imaad786/mcp-upload/blob/main/.github/workflows/stress.yml)
runs once a day and on demand. It installs from the lockfile, starts a real Redis 8 as
a service, and runs the scenarios named in its `SCENARIOS` variable, the multipart fuzz
with 3,000 bodies and the SEP-2631 flows. `stress/check.py` fails the job on any broken
invariant, and the results are kept as a workflow artifact for 30 days.

The nightly list leaves out the throughput scenarios and `slowloris`, which measure
the machine (`slowloris` alone takes 90 s of wall time). `abandoned_after_crash`,
`pool_ceiling` and `sqlite_migration` are not in it either.
