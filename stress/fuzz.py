"""Randomized multipart framing check.

Builds bodies with every legal variation the gateway has to get right (a preamble, an
epilogue of any content, a final CRLF or none, file data full of near-miss delimiters)
and splits each at random points, so delimiters land across reads. Every upload must
complete and deliver exactly the file's bytes to the backend.

    python stress/fuzz.py --src src [--n 3000] [--seed 7]

``--src`` picks which copy of the library to test, as with ``run.py``.
"""

from __future__ import annotations

import argparse
import asyncio
import random
import sys
from collections import Counter
from pathlib import Path


async def fuzz(n: int, seed: int) -> dict[str, object]:
    import httpx
    from starlette.applications import Starlette

    import mcp_upload
    from mcp_upload import Destination, MemoryStore, Registry, UploadGateway

    received: list[bytes] = []

    async def backend(request: httpx.Request) -> httpx.Response:
        received.append(await request.aread())
        return httpx.Response(201)

    boundary = "fz9Q"
    rng = random.Random(seed)
    http = httpx.AsyncClient(transport=httpx.MockTransport(backend))
    gateway = UploadGateway(
        base_url="http://gateway",
        registry=Registry(Destination(name="f", url="http://backend/{id}")),
        store=MemoryStore(max_records=n + 1),
        http=http,
    )
    app = Starlette(routes=gateway.routes())
    failures: Counter[str] = Counter()
    wrong_bytes = 0
    near_misses = [
        f"--{boundary}--".encode(),
        f"\r\n--{boundary[:-1]}".encode(),
        b"\r\n-",
        b"\r\n--",
        b"\r",
        b"--",
    ]
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://gateway"
    ) as client:
        for _ in range(n):
            data = b"".join(
                rng.randbytes(rng.randint(0, 3000)) + rng.choice(near_misses)
                for _ in range(rng.randint(1, 4))
            )
            preamble = rng.choice([b"", b"preamble text\r\n", b"\r\n"])
            epilogue = rng.choice(
                [
                    b"",
                    b"\r\n",
                    b"epilogue\r\n",
                    f"\r\n--{boundary}--\r\n".encode(),
                    rng.randbytes(rng.randint(0, 5000)),
                ]
            )
            body = (
                preamble
                + f"--{boundary}\r\n".encode()
                + b'Content-Disposition: form-data; name="file"; filename="f.bin"\r\n'
                + b"Content-Type: application/octet-stream\r\n\r\n"
                + data
                + f"\r\n--{boundary}--".encode()
                + rng.choice([b"\r\n", b""])
                + epilogue
            )
            cuts = sorted(rng.sample(range(1, len(body)), min(len(body) - 1, rng.randint(0, 12))))
            parts = [body[a:b] for a, b in zip([0, *cuts], [*cuts, len(body)], strict=True)]

            async def stream(parts: list[bytes] = parts) -> object:
                for part in parts:
                    yield part

            before = len(received)
            issued = await gateway.issue("f")
            reply = await client.post(
                issued.upload_url,
                content=stream(),
                headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
            )
            outcome = reply.json()
            sent = received[-1] if len(received) > before else None
            if outcome.get("status") == "completed" and sent == data:
                continue
            if outcome.get("status") == "completed":
                wrong_bytes += 1
                failures["completed_with_wrong_bytes"] += 1
            else:
                failures[str(outcome.get("error"))] += 1
    await http.aclose()
    return {
        "version": mcp_upload.__version__,
        "bodies": n,
        "failed": sum(failures.values()),
        "completed_with_wrong_bytes": wrong_bytes,
        "by_outcome": dict(failures),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", required=True)
    parser.add_argument("--n", type=int, default=3000)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()
    sys.path.insert(0, str(Path(args.src).resolve()))
    print(asyncio.run(fuzz(args.n, args.seed)))


if __name__ == "__main__":
    main()
