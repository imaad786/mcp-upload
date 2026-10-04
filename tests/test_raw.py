"""Raw-body uploads: the request body is the file.

Every protection a form upload has must hold here too, and with the option off the
endpoint must behave exactly as before.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any

import httpx
import pytest
from starlette.applications import Starlette

from mcp_upload import Destination, IncomingFile, MemoryStore, Registry, UploadGateway
from mcp_upload.multipart import filename_from_disposition
from mcp_upload.tickets import b64url
from tests.conftest import BASE_URL, Upstream, multipart
from tests.test_live import Backend, Live, free_port


def raw_gateway(upstream: Upstream, registry: Registry, **options: Any) -> UploadGateway:
    return UploadGateway(
        base_url=BASE_URL,
        registry=registry,
        store=MemoryStore(),
        http=httpx.AsyncClient(transport=httpx.MockTransport(upstream.handler)),
        raw_uploads=options.pop("raw_uploads", True),
        **options,
    )


def client_for(gateway: UploadGateway) -> httpx.AsyncClient:
    app = Starlette(routes=gateway.routes())
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE_URL)


@pytest.fixture
async def raw(upstream: Upstream, registry: Registry) -> AsyncIterator[UploadGateway]:
    gateway = raw_gateway(upstream, registry)
    yield gateway
    await gateway.aclose()


@pytest.fixture
async def client(raw: UploadGateway) -> AsyncIterator[httpx.AsyncClient]:
    async with client_for(raw) as client:
        yield client


def b64(data: bytes) -> str:
    return b64url(hashlib.sha256(data).hexdigest())


# ----- the happy path and the filename sources ------------------------------------------


@pytest.mark.parametrize("method", ["POST", "PUT"])
async def test_raw_body_is_the_file(
    client: httpx.AsyncClient, raw: UploadGateway, upstream: Upstream, method: str
) -> None:
    issued = await raw.issue("files")
    data = b"plain text body" * 20
    response = await client.request(
        method,
        issued.upload_url,
        content=data,
        headers={
            "Content-Type": "Text/Plain; charset=utf-8",
            "Content-Disposition": 'attachment; filename="notes.txt"',
        },
    )
    assert response.status_code == 200, response.text
    file = response.json()["file"]
    # The media type is kept as declared, parameters included, and forwarded as is.
    assert (file["name"], file["mimeType"], file["size"]) == (
        "notes.txt",
        "Text/Plain; charset=utf-8",
        len(data),
    )
    assert file["digest"]["value"] == b64(data)
    assert upstream.bodies == [data]
    assert str(upstream.requests[0].url) == "http://backend.test/files/notes.txt"
    assert upstream.requests[0].headers["content-type"] == "Text/Plain; charset=utf-8"


async def test_raw_media_type_matches_the_accept_list_on_the_base_type(
    client: httpx.AsyncClient, raw: UploadGateway, upstream: Upstream
) -> None:
    issued = await raw.issue("images")
    declared = "image/svg+xml; charset=utf-8"
    response = await client.put(
        issued.upload_url, content=b"<svg/>", headers={"Content-Type": declared}
    )
    assert response.status_code == 200, response.text
    assert response.json()["file"]["mimeType"] == declared


@pytest.mark.parametrize(
    ("headers", "query", "expected"),
    [
        (
            {"Content-Disposition": "attachment; filename*=UTF-8''%E2%82%AC%20rates.txt"},
            "",
            "€ rates.txt",
        ),
        (
            {
                "Content-Disposition": (
                    "attachment; filename=\"fallback.txt\"; filename*=utf-8''real%20name.txt"
                )
            },
            "",
            "real name.txt",
        ),
        ({"Content-Disposition": 'attachment; filename="a;b \\"q\\".txt"'}, "", 'a;b "q".txt'),
        ({"Content-Disposition": "attachment; filename=../../etc/passwd"}, "", "passwd"),
        ({}, "?filename=..%2F..%2Fquery.bin", "query.bin"),
        (
            {"Content-Disposition": 'attachment; filename="header.bin"'},
            "?filename=q.bin",
            "header.bin",
        ),
        ({}, "", "upload"),
    ],
)
async def test_filename_sources(
    client: httpx.AsyncClient,
    raw: UploadGateway,
    headers: dict[str, str],
    query: str,
    expected: str,
) -> None:
    issued = await raw.issue("files")
    response = await client.put(
        issued.upload_url + query,
        content=b"x",
        headers={"Content-Type": "application/octet-stream", **headers},
    )
    assert response.status_code == 200, response.text
    assert response.json()["file"]["name"] == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("attachment; filename=plain.txt", "plain.txt"),
        ('inline; FILENAME="Upper.txt"', "Upper.txt"),
        ("attachment; filename*=ISO-8859-1''caf%E9.txt", "café.txt"),
        ("attachment; filename*=UTF-8''%FF%FE; filename=ok.txt", "ok.txt"),
        ("attachment; filename*=KOI8-R''x.txt; filename=ok.txt", "ok.txt"),
        ("attachment", None),
        ("", None),
        (None, None),
    ],
)
def test_filename_from_disposition(value: str | None, expected: str | None) -> None:
    assert filename_from_disposition(value) == expected


async def test_missing_content_type_is_octet_stream(
    client: httpx.AsyncClient, raw: UploadGateway
) -> None:
    issued = await raw.issue("files")

    async def body() -> AsyncIterator[bytes]:
        yield b"bytes"

    # A generator body carries no Content-Type unless one is given.
    response = await client.put(issued.upload_url, content=body())
    assert response.status_code == 200, response.text
    assert response.json()["file"]["mimeType"] == "application/octet-stream"


async def test_empty_raw_body_is_an_empty_file(
    client: httpx.AsyncClient, raw: UploadGateway, upstream: Upstream
) -> None:
    issued = await raw.issue("files")
    response = await client.put(
        issued.upload_url, content=b"", headers={"Content-Type": "text/plain"}
    )
    assert response.status_code == 200, response.text
    assert response.json()["file"]["size"] == 0
    assert upstream.bodies == [b""]


async def test_multipart_still_works_with_raw_enabled(
    client: httpx.AsyncClient, raw: UploadGateway, upstream: Upstream
) -> None:
    for method in ("POST", "PUT"):
        issued = await raw.issue("files")
        body, content_type = multipart([("file", "f.txt", b"form", "text/plain")])
        response = await client.request(
            method, issued.upload_url, content=body, headers={"Content-Type": content_type}
        )
        assert response.status_code == 200, response.text
    assert upstream.bodies == [b"form", b"form"]


async def test_raw_upload_into_a_sink() -> None:
    got: list[tuple[str, bytes]] = []

    async def sink(upload: IncomingFile) -> None:
        data = b""
        async for chunk in upload:
            data += chunk
        got.append((upload.filename, data))

    gateway = UploadGateway(
        base_url=BASE_URL,
        registry=Registry(Destination(name="s", sink=sink)),
        store=MemoryStore(),
        raw_uploads=True,
    )
    issued = await gateway.issue("s")
    data = os.urandom(2_000_000)
    async with client_for(gateway) as client:
        response = await client.put(
            issued.upload_url + "?filename=blob.bin",
            content=data,
            headers={"Content-Type": "application/octet-stream"},
        )
    assert response.status_code == 200, response.text
    assert got == [("blob.bin", data)]


# ----- limits and refusals --------------------------------------------------------------


async def test_declared_length_over_the_limit_is_refused_before_the_ticket(
    client: httpx.AsyncClient, raw: UploadGateway, upstream: Upstream
) -> None:
    # A raw body is the file, so the declared length is held to the limit exactly.
    issued = await raw.issue("files")  # max_size 1000
    response = await client.put(
        issued.upload_url, content=b"x" * 1001, headers={"Content-Type": "text/plain"}
    )
    assert response.status_code == 413
    assert response.json()["details"] == {"reason": "maxSizeExceeded", "maxSize": 1000}
    assert (await raw.status(issued.record.id))["status"] == "issued"
    assert upstream.requests == []


async def test_size_limit_is_enforced_on_the_bytes(
    client: httpx.AsyncClient, raw: UploadGateway, upstream: Upstream
) -> None:
    issued = await raw.issue("files", max_size=100)
    # Lie about the length so it passes the early check.
    response = await client.put(
        issued.upload_url,
        content=b"z" * 200,
        headers={"Content-Type": "text/plain", "Content-Length": "50"},
    )
    assert response.status_code == 413
    assert response.json()["error"] == "too_large"
    assert upstream.requests == []
    assert (await raw.status(issued.record.id))["error"] == "too_large"


async def test_chunked_raw_body_over_the_limit(
    client: httpx.AsyncClient, raw: UploadGateway, upstream: Upstream
) -> None:
    issued = await raw.issue("files")

    async def body() -> AsyncIterator[bytes]:
        for _ in range(10):
            yield b"y" * 500

    response = await client.put(
        issued.upload_url, content=body(), headers={"Content-Type": "text/plain"}
    )
    assert response.json()["error"] == "too_large"
    assert upstream.requests == []


@pytest.mark.parametrize(
    ("declared", "code"),
    [
        ({"expected_digest": b64(b"other")}, "digest_mismatch"),
        ({"expected_size": 11}, "size_mismatch"),
    ],
)
async def test_declared_size_and_digest_are_verified(
    client: httpx.AsyncClient,
    raw: UploadGateway,
    upstream: Upstream,
    declared: dict[str, Any],
    code: str,
) -> None:
    issued = await raw.issue("files", **declared)
    response = await client.put(
        issued.upload_url, content=b"0123456789", headers={"Content-Type": "text/plain"}
    )
    assert response.status_code == 422
    assert response.json()["error"] == code
    assert upstream.requests == []


async def test_matching_declared_digest_completes(
    client: httpx.AsyncClient, raw: UploadGateway, upstream: Upstream
) -> None:
    data = b"0123456789"
    issued = await raw.issue("files", expected_size=10, expected_digest=b64(data))
    response = await client.put(
        issued.upload_url, content=data, headers={"Content-Type": "text/plain"}
    )
    assert response.status_code == 200, response.text
    assert upstream.bodies == [data]


@pytest.mark.parametrize(
    ("destination", "content_type", "code", "status"),
    [
        ("images", "text/plain", "unsupported_media_type", 415),
        ("files", "image/png x", "invalid_media_type", 400),
        ("files", "a" * 300 + "/b", "invalid_media_type", 400),
        ("files", "text/plain; charset", "invalid_media_type", 400),
        ("files", 'text/plain; charset="utf-8', "invalid_media_type", 400),
        ("files", "text/plain;; charset=utf-8", "invalid_media_type", 400),
        ("images", "text/plain; charset=utf-8", "unsupported_media_type", 415),
        ("files", "application/x-www-form-urlencoded", "not_multipart", 415),
        ("files", "multipart/mixed; boundary=x", "not_multipart", 415),
        ("files", "multipart/form-data", "missing_boundary", 400),
    ],
)
async def test_header_refusals_leave_the_ticket_untouched(
    client: httpx.AsyncClient,
    raw: UploadGateway,
    upstream: Upstream,
    destination: str,
    content_type: str,
    code: str,
    status: int,
) -> None:
    issued = await raw.issue(destination)
    response = await client.put(
        issued.upload_url, content=b"data", headers={"Content-Type": content_type}
    )
    assert response.status_code == status
    assert response.json()["error"] == code
    if code == "unsupported_media_type":
        assert response.json()["details"]["reason"] == "mimeTypeNotAccepted"
    assert (await raw.status(issued.record.id))["status"] == "issued"
    assert upstream.requests == []


async def test_raw_ticket_is_single_use(
    client: httpx.AsyncClient, raw: UploadGateway, upstream: Upstream
) -> None:
    issued = await raw.issue("files")
    headers = {"Content-Type": "text/plain"}
    first = await client.put(issued.upload_url, content=b"one", headers=headers)
    second = await client.put(issued.upload_url, content=b"two", headers=headers)
    assert (first.status_code, second.status_code) == (200, 410)
    assert second.json()["error"] == "ticket_used"
    assert upstream.bodies == [b"one"]


async def test_raw_in_flight_cap(upstream: Upstream, registry: Registry) -> None:
    gateway = raw_gateway(upstream, registry, max_in_flight=1)
    async with client_for(gateway) as client:
        issued = await gateway.issue("files")
        gateway._in_flight = 1
        refused = await client.put(
            issued.upload_url, content=b"x", headers={"Content-Type": "text/plain"}
        )
        assert refused.status_code == 503
        assert (await gateway.status(issued.record.id))["status"] == "issued"
        gateway._in_flight = 0
        accepted = await client.put(
            issued.upload_url, content=b"x", headers={"Content-Type": "text/plain"}
        )
        assert accepted.status_code == 200


@pytest.mark.parametrize("limit", ["stall", "total"])
async def test_raw_slow_clients_are_cut_off(
    upstream: Upstream, registry: Registry, limit: str
) -> None:
    options: dict[str, Any] = (
        {"stall_timeout": timedelta(seconds=0.2)}
        if limit == "stall"
        else {"upload_timeout": timedelta(seconds=0.3), "stall_timeout": None}
    )
    gateway = raw_gateway(upstream, registry, **options)
    issued = await gateway.issue("files", max_size=None)

    async def drip() -> AsyncIterator[bytes]:
        for _ in range(20):
            yield b"d"
            await asyncio.sleep(0.05)

    async with client_for(gateway) as client:
        response = await client.put(
            issued.upload_url, content=drip(), headers={"Content-Type": "text/plain"}
        )
    expected = "too_slow" if limit == "stall" else "upload_timeout"
    assert response.json()["error"] == expected
    assert upstream.bodies == []


# ----- off by default -------------------------------------------------------------------


async def test_raw_uploads_off_is_the_old_behaviour(upstream: Upstream, registry: Registry) -> None:
    gateway = raw_gateway(upstream, registry, raw_uploads=False)
    async with client_for(gateway) as client:
        issued = await gateway.issue("files")
        put = await client.put(
            issued.upload_url, content=b"x", headers={"Content-Type": "text/plain"}
        )
        assert put.status_code == 405
        post = await client.post(
            issued.upload_url, content=b"x", headers={"Content-Type": "text/plain"}
        )
        assert post.status_code == 415
        assert post.json()["error"] == "not_multipart"
        assert (await gateway.status(issued.record.id))["status"] == "issued"
    assert upstream.requests == []


# ----- describe ---------------------------------------------------------------------------


async def test_describe_raw(raw: UploadGateway, gateway: UploadGateway) -> None:
    issued = await raw.issue("files")
    upload = raw.describe(issued, raw=True)["upload"]
    assert upload["method"] == "PUT"
    assert upload["url"] == issued.upload_url
    assert "multipart" not in upload
    assert "headers" not in upload
    assert raw.describe(issued)["upload"]["multipart"] == {"fileField": "file"}

    single = await raw.issue("images", accept=("image/png",))
    assert raw.describe(single, raw=True)["upload"]["headers"] == {"Content-Type": "image/png"}
    typed = raw.describe(issued, raw=True, media_type="application/pdf")
    assert typed["upload"]["headers"] == {"Content-Type": "application/pdf"}
    with pytest.raises(ValueError):
        raw.describe(single, raw=True, media_type="text/plain")

    plain = await gateway.issue("files")
    with pytest.raises(ValueError):
        gateway.describe(plain, raw=True)


# ----- over real sockets ----------------------------------------------------------------


async def test_raw_early_refusal_reaches_the_client_over_sockets() -> None:
    # Refused on the declared length before the body is read. The drain after the
    # response lets the client read the 413 instead of seeing a reset.
    backend = Backend()
    async with Live(backend.app()) as backend_server:
        port = free_port()
        gateway = UploadGateway(
            base_url=f"http://127.0.0.1:{port}",
            registry=Registry(
                Destination(name="files", url=f"{backend_server.url}/files/{{id}}", max_size=1000)
            ),
            store=MemoryStore(),
            raw_uploads=True,
        )
        server = Live(Starlette(routes=gateway.routes()))
        server.port = port
        server.server.config.port = port
        async with server, httpx.AsyncClient(timeout=30) as client:
            issued = await gateway.issue("files")
            response = await client.put(
                issued.upload_url,
                content=os.urandom(512 * 1024),
                headers={"Content-Type": "application/octet-stream"},
            )
            assert response.status_code == 413
            ok = await gateway.issue("files")
            data = os.urandom(900)
            done = await client.put(
                ok.upload_url, content=data, headers={"Content-Type": "application/pdf"}
            )
            assert done.status_code == 200, done.text
        await gateway.aclose()
    assert backend.received == [
        {"name": ok.record.id, "size": 900, "sha256": hashlib.sha256(data).hexdigest()}
    ]
