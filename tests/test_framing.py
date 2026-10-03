"""Multipart framing: preambles, epilogues and delimiters split across reads.

The streaming parser accepts neither a preamble nor an epilogue, and an epilogue that
repeats the close delimiter used to be appended to the stored file while the upload
reported success. These run randomized bodies through the real endpoint and require
every one to deliver exactly the file's bytes. ``stress/fuzz.py`` runs the same check
at a larger count.
"""

from __future__ import annotations

import random
from collections.abc import AsyncIterator

import httpx

from mcp_upload import UploadGateway
from mcp_upload.multipart import Framer
from tests.conftest import Upstream

BOUNDARY = "fz9Q"


def build(rng: random.Random, largest: int = 2000) -> tuple[bytes, bytes]:
    near_misses = [f"--{BOUNDARY}--".encode(), f"\r\n--{BOUNDARY[:-1]}".encode(), b"\r\n--", b"\r"]
    data = b"".join(
        rng.randbytes(rng.randint(0, largest)) + rng.choice(near_misses)
        for _ in range(rng.randint(1, 3))
    )
    body = (
        rng.choice([b"", b"preamble\r\n", b"\r\n"])
        + f"--{BOUNDARY}\r\n".encode()
        + b'Content-Disposition: form-data; name="file"; filename="f.bin"\r\n\r\n'
        + data
        + f"\r\n--{BOUNDARY}--".encode()
        + rng.choice([b"\r\n", b""])
        + rng.choice(
            [b"", b"\r\n", b"epilogue\r\n", f"\r\n--{BOUNDARY}--\r\n".encode(), rng.randbytes(300)]
        )
    )
    return body, data


def split(rng: random.Random, body: bytes) -> list[bytes]:
    cuts = sorted(rng.sample(range(1, len(body)), min(len(body) - 1, rng.randint(0, 10))))
    return [body[a:b] for a, b in zip([0, *cuts], [*cuts, len(body)], strict=True)]


def test_framer_cuts_at_the_close_delimiter_wherever_reads_split() -> None:
    rng = random.Random(1)
    for _ in range(2000):
        body, data = build(rng)
        framer = Framer(BOUNDARY, max_preamble=16 * 1024)
        out = b"".join(framer.feed(part) for part in split(rng, body))
        assert framer.closed
        start = out.index(b"\r\n\r\n") + 4
        assert out[start:] == data + f"\r\n--{BOUNDARY}--\r\n".encode()


async def test_randomized_bodies_deliver_exact_bytes(
    client: httpx.AsyncClient, gateway: UploadGateway, upstream: Upstream
) -> None:
    rng = random.Random(2)
    for _ in range(200):
        body, data = build(rng, largest=250)  # the fixture's destination caps at 1000
        parts = split(rng, body)

        async def stream(parts: list[bytes] = parts) -> AsyncIterator[bytes]:
            for part in parts:
                yield part

        issued = await gateway.issue("files")
        reply = await client.post(
            issued.upload_url,
            content=stream(),
            headers={"Content-Type": f"multipart/form-data; boundary={BOUNDARY}"},
        )
        assert reply.json()["status"] == "completed", reply.text
        assert upstream.bodies[-1] == data
