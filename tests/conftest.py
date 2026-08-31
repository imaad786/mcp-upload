"""Shared fixtures.

The gateway is exercised through an in-process ASGI app, and the destination backend
is an httpx mock transport that records what it received. That keeps these tests fast
and deterministic. Streaming behaviour that needs real sockets (memory stays flat under
a slow backend, a client vanishing mid-upload) is covered separately.
"""

from __future__ import annotations

import secrets
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

import httpx
import pytest
from starlette.applications import Starlette

from mcp_upload import Destination, MemoryStore, Registry, UploadGateway

BASE_URL = "http://gateway.test"


@dataclass
class Upstream:
    """Stands in for the backend. Records every request it fully received."""

    requests: list[httpx.Request] = field(default_factory=list)
    bodies: list[bytes] = field(default_factory=list)
    status_code: int = 201

    async def handler(self, request: httpx.Request) -> httpx.Response:
        body = await request.aread()
        self.requests.append(request)
        self.bodies.append(body)
        return httpx.Response(self.status_code, json={"ok": True})


@pytest.fixture
def upstream() -> Upstream:
    return Upstream()


@pytest.fixture
def registry() -> Registry:
    return Registry(
        Destination(
            name="files",
            url="http://backend.test/files/{filename}",
            method="PUT",
            max_size=1000,
        ),
        Destination(
            name="images",
            url="http://backend.test/images/{id}",
            method="POST",
            encoding="multipart",
            field_name="upload",
            accept=("image/*",),
        ),
    )


@pytest.fixture
async def gateway(upstream: Upstream, registry: Registry) -> AsyncIterator[UploadGateway]:
    http = httpx.AsyncClient(transport=httpx.MockTransport(upstream.handler))
    gw = UploadGateway(
        base_url=BASE_URL,
        registry=registry,
        store=MemoryStore(),
        server_name="test",
        http=http,
    )
    yield gw
    await http.aclose()


@pytest.fixture
async def client(gateway: UploadGateway) -> AsyncIterator[httpx.AsyncClient]:
    app = Starlette(routes=gateway.routes())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=BASE_URL
    ) as client:
        yield client


def multipart(
    parts: list[tuple[str, str | None, bytes, str | None]],
    boundary: str | None = None,
) -> tuple[bytes, str]:
    """Build a multipart body by hand so headers and part order are under test control.

    Each part is (field name, filename or None, data, content type or None). Returns
    the body and the Content-Type header value, with the boundary as given so a test
    can vary the header's casing or drop the boundary.
    """
    boundary = boundary or "b" + secrets.token_hex(8)
    out = b""
    for name, filename, data, content_type in parts:
        out += f"--{boundary}\r\n".encode()
        disposition = f'Content-Disposition: form-data; name="{name}"'
        if filename is not None:
            disposition += f'; filename="{filename}"'
        out += disposition.encode() + b"\r\n"
        if content_type:
            out += f"Content-Type: {content_type}\r\n".encode()
        out += b"\r\n" + data + b"\r\n"
    out += f"--{boundary}--\r\n".encode()
    return out, f"multipart/form-data; boundary={boundary}"
