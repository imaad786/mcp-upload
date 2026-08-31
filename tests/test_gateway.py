"""The upload endpoint, request by request.

Most of these encode one rule: nothing that can be judged from the headers may cost
the ticket, and nothing that fails validation may reach the backend as a committed
upload.
"""

from __future__ import annotations

import hashlib
import inspect
import logging
from datetime import timedelta

import httpx
import pytest
from starlette.applications import Starlette

from mcp_upload import Issued, MemoryStore, Registry, Status, UnknownDestination, UploadGateway
from mcp_upload.multipart import parse_content_type, sanitize_filename
from tests.conftest import BASE_URL, Upstream, multipart

PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 100


async def issue(gateway: UploadGateway, destination: str = "files", **kw: object) -> Issued:
    return await gateway.issue(destination, **kw)  # type: ignore[arg-type]


async def post(
    client: httpx.AsyncClient,
    url: str,
    parts: list[tuple[str, str | None, bytes, str | None]],
    *,
    content_type: str | None = None,
    boundary: str | None = None,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    body, default_type = multipart(parts, boundary)
    sent = {"Content-Type": content_type or default_type, **(headers or {})}
    return await client.post(url, content=body, headers=sent)


# ----- the happy path -------------------------------------------------------------------


async def test_upload_streams_to_the_destination_and_records_the_outcome(
    client: httpx.AsyncClient, gateway: UploadGateway, upstream: Upstream
) -> None:
    issued = await issue(gateway)
    data = b"hello upload" * 10
    response = await post(client, issued.upload_url, [("file", "report.txt", data, "text/plain")])

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "completed"
    assert body["id"] == issued.record.id
    assert body["file"]["name"] == "report.txt"
    assert body["file"]["mimeType"] == "text/plain"
    assert body["file"]["size"] == len(data)
    assert body["file"]["digest"] == {
        "algorithm": "sha-256",
        "value": hashlib.sha256(data).hexdigest(),
    }
    assert body["file"]["uri"] == f"mcp-file://test/{issued.record.id}"

    assert len(upstream.requests) == 1
    sent = upstream.requests[0]
    assert sent.method == "PUT"
    assert str(sent.url) == "http://backend.test/files/report.txt"
    assert sent.headers["content-type"] == "text/plain"
    assert upstream.bodies[0] == data

    status = await gateway.status(issued.record.id)
    assert status["status"] == "completed"
    assert status["file"]["size"] == len(data)


async def test_multipart_destination_reencodes_the_file(
    client: httpx.AsyncClient, gateway: UploadGateway, upstream: Upstream
) -> None:
    issued = await issue(gateway, "images")
    response = await post(client, issued.upload_url, [("file", "a.png", PNG, "image/png")])
    assert response.status_code == 200, response.text

    sent = upstream.requests[0]
    assert sent.method == "POST"
    assert str(sent.url) == f"http://backend.test/images/{issued.record.id}"
    media_type, params = parse_content_type(sent.headers["content-type"])
    assert media_type == "multipart/form-data"
    boundary = params["boundary"].encode()
    body = upstream.bodies[0]
    assert body.startswith(b"--" + boundary + b"\r\n")
    assert b'name="upload"; filename="a.png"' in body
    assert b"Content-Type: image/png" in body
    assert PNG in body
    assert body.endswith(b"\r\n--" + boundary + b"--\r\n")


async def test_describe_matches_the_transfer_descriptor_shape(gateway: UploadGateway) -> None:
    issued = await issue(gateway)
    described = gateway.describe(issued)
    assert described["status"] == "awaiting_upload"
    assert described["id"] == issued.record.id
    assert described["file"] == {"uri": f"mcp-file://test/{issued.record.id}"}
    upload = described["upload"]
    assert upload["method"] == "POST"
    assert upload["transport"] == "http"
    assert upload["url"] == issued.upload_url
    assert upload["multipart"] == {"fileField": "file"}
    assert upload["expiresAt"].endswith("Z")
    assert issued.secret in issued.upload_url
    assert issued.secret not in repr(issued.record)


async def test_status_of_unknown_id(gateway: UploadGateway) -> None:
    assert await gateway.status("nope") == {"id": "nope", "status": "unknown"}


# ----- ordering: header-only failures never spend the ticket -------------------------


async def test_urlencoded_post_does_not_burn_the_ticket(
    client: httpx.AsyncClient, gateway: UploadGateway, upstream: Upstream
) -> None:
    issued = await issue(gateway)
    bogus = await client.post(issued.upload_url, data={"file": "not a file"})
    assert bogus.status_code == 415
    assert bogus.json()["error"] == "not_multipart"
    assert upstream.requests == []
    assert (await gateway.status(issued.record.id))["status"] == "issued"

    real = await post(client, issued.upload_url, [("file", "a.txt", b"x", "text/plain")])
    assert real.status_code == 200, real.text


@pytest.mark.parametrize(
    ("content_type", "expected"),
    [
        ("application/json", "not_multipart"),
        ("multipart/mixed; boundary=abc", "not_multipart"),
        ("multipart/related; boundary=abc", "not_multipart"),
        ("multipart/form-data-evil; boundary=abc", "not_multipart"),
        ("multipart/form-data", "missing_boundary"),
        ("", "not_multipart"),
    ],
)
async def test_non_file_content_types_are_rejected_before_redemption(
    client: httpx.AsyncClient, gateway: UploadGateway, content_type: str, expected: str
) -> None:
    issued = await issue(gateway)
    response = await client.post(
        issued.upload_url, content=b"{}", headers={"Content-Type": content_type}
    )
    assert response.json()["error"] == expected
    assert (await gateway.status(issued.record.id))["status"] == "issued"


async def test_uppercase_content_type_is_accepted(
    client: httpx.AsyncClient, gateway: UploadGateway
) -> None:
    issued = await issue(gateway)
    response = await post(
        client,
        issued.upload_url,
        [("file", "a.txt", b"x", "text/plain")],
        boundary="XYZ",
        content_type=" MULTIPART/FORM-DATA ; boundary=XYZ",
    )
    assert response.status_code == 200, response.text


async def test_declared_length_far_over_the_limit_is_rejected_early(
    client: httpx.AsyncClient, gateway: UploadGateway
) -> None:
    issued = await issue(gateway)
    body, content_type = multipart([("file", "a.txt", b"x", None)])
    response = await client.post(
        issued.upload_url,
        content=body,
        headers={"Content-Type": content_type, "Content-Length": str(10_000_000)},
    )
    assert response.status_code == 413
    assert (await gateway.status(issued.record.id))["status"] == "issued"


# ----- single use -----------------------------------------------------------------------


async def test_replay_is_refused_and_nothing_reaches_the_backend_twice(
    client: httpx.AsyncClient, gateway: UploadGateway, upstream: Upstream
) -> None:
    issued = await issue(gateway)
    first = await post(client, issued.upload_url, [("file", "a.txt", b"x", None)])
    assert first.status_code == 200
    second = await post(client, issued.upload_url, [("file", "a.txt", b"y", None)])
    assert second.status_code == 410
    assert second.json()["error"] == "ticket_used"
    assert len(upstream.requests) == 1


async def test_unknown_ticket(client: httpx.AsyncClient) -> None:
    response = await post(client, "/upload/not-a-ticket", [("file", "a.txt", b"x", None)])
    assert response.status_code == 404
    assert response.json() == {"status": "failed", "error": "unknown_ticket"}


async def test_expired_ticket(
    client: httpx.AsyncClient, gateway: UploadGateway, upstream: Upstream
) -> None:
    issued = await issue(gateway, ttl=timedelta(seconds=-1))
    response = await post(client, issued.upload_url, [("file", "a.txt", b"x", None)])
    assert response.status_code == 410
    assert response.json()["error"] == "ticket_expired"
    assert upstream.requests == []
    assert (await gateway.status(issued.record.id))["status"] == "expired"


# ----- the multipart body itself --------------------------------------------------------


async def test_duplicate_file_parts_are_refused_and_not_committed(
    client: httpx.AsyncClient, gateway: UploadGateway, upstream: Upstream
) -> None:
    issued = await issue(gateway)
    response = await post(
        client,
        issued.upload_url,
        [("file", "clean.txt", b"CLEAN", None), ("file", "evil.txt", b"EVIL", None)],
    )
    assert response.status_code == 400
    assert response.json()["error"] == "duplicate_file"
    # The first part streamed, but the request to the backend was never completed.
    assert upstream.requests == []
    assert (await gateway.status(issued.record.id))["status"] == "failed"


async def test_part_without_filename_is_not_a_file(
    client: httpx.AsyncClient, gateway: UploadGateway, upstream: Upstream
) -> None:
    issued = await issue(gateway)
    response = await post(client, issued.upload_url, [("file", None, b"just text", None)])
    assert response.status_code == 400
    assert response.json()["error"] == "missing_file"
    assert upstream.requests == []


async def test_unexpected_part_is_refused(
    client: httpx.AsyncClient, gateway: UploadGateway, upstream: Upstream
) -> None:
    issued = await issue(gateway)
    response = await post(
        client,
        issued.upload_url,
        [("file", "a.txt", b"x", None), ("token", None, b"smuggled", None)],
    )
    assert response.status_code == 400
    assert response.json()["error"] == "unexpected_part"
    assert upstream.requests == []


async def test_body_with_no_file_part(
    client: httpx.AsyncClient, gateway: UploadGateway, upstream: Upstream
) -> None:
    issued = await issue(gateway)
    response = await post(client, issued.upload_url, [])
    assert response.status_code == 400
    assert response.json()["error"] in ("missing_file", "bad_multipart")
    assert upstream.requests == []


async def test_size_limit_is_enforced_on_the_bytes_not_the_header(
    client: httpx.AsyncClient, gateway: UploadGateway, upstream: Upstream
) -> None:
    issued = await issue(gateway, max_size=100)
    body, content_type = multipart([("file", "big.bin", b"z" * 200, None)])
    # Lie about the length: small enough to pass the header pre-check.
    response = await client.post(
        issued.upload_url,
        content=body,
        headers={"Content-Type": content_type, "Content-Length": "50"},
    )
    assert response.status_code == 413
    assert response.json()["error"] == "too_large"
    assert upstream.requests == []
    status = await gateway.status(issued.record.id)
    assert status["status"] == "failed"
    assert status["error"] == "too_large"


async def test_per_ticket_limit_can_only_tighten(gateway: UploadGateway) -> None:
    looser = await issue(gateway, max_size=10_000)
    assert looser.record.constraints.max_size == 1000
    tighter = await issue(gateway, max_size=10)
    assert tighter.record.constraints.max_size == 10


async def test_accept_list_is_enforced_on_the_declared_type(
    client: httpx.AsyncClient, gateway: UploadGateway, upstream: Upstream
) -> None:
    issued = await issue(gateway, "images")
    response = await post(client, issued.upload_url, [("file", "a.txt", b"x", "text/plain")])
    assert response.status_code == 415
    assert response.json()["error"] == "unsupported_media_type"
    assert upstream.requests == []


@pytest.mark.parametrize("declared", ["image/png\tx", "text/plain\x00evil", "a" * 300 + "/b"])
async def test_invalid_media_type_is_refused_before_anything_is_forwarded(
    client: httpx.AsyncClient, gateway: UploadGateway, upstream: Upstream, declared: str
) -> None:
    # The declared type becomes a header on the backend request and a field on the
    # record, so it has to be a real token. The parser hands these three through as-is.
    # (A value it cannot parse at all, such as one with no slash, it replaces with its
    # text/plain default before we ever see it.)
    issued = await issue(gateway)
    response = await post(client, issued.upload_url, [("file", "a.txt", b"x", declared)])
    assert response.status_code == 400
    assert response.json()["error"] == "invalid_media_type"
    assert upstream.requests == []
    status = await gateway.status(issued.record.id)
    assert status["status"] == "failed"
    assert status["error"] == "invalid_media_type"


async def test_in_flight_cap_refuses_with_503_and_leaves_the_ticket_usable(
    upstream: Upstream, registry: Registry
) -> None:
    gateway = UploadGateway(
        base_url=BASE_URL,
        registry=registry,
        store=MemoryStore(),
        http=httpx.AsyncClient(transport=httpx.MockTransport(upstream.handler)),
        max_in_flight=1,
    )
    app = Starlette(routes=gateway.routes())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=BASE_URL
    ) as client:
        issued = await issue(gateway)
        gateway._in_flight = 1  # another upload is streaming right now
        refused = await post(client, issued.upload_url, [("file", "a.txt", b"x", None)])
        assert refused.status_code == 503
        assert refused.json()["error"] == "too_many_uploads"
        assert refused.headers["retry-after"] == "5"
        assert (await gateway.status(issued.record.id))["status"] == "issued"

        gateway._in_flight = 0
        accepted = await post(client, issued.upload_url, [("file", "a.txt", b"x", None)])
        assert accepted.status_code == 200, accepted.text


async def test_page_sets_content_security_headers(
    client: httpx.AsyncClient, gateway: UploadGateway
) -> None:
    issued = await issue(gateway)
    page = await client.get(issued.upload_url)
    assert page.headers["x-frame-options"] == "DENY"
    assert "default-src 'none'" in page.headers["content-security-policy"]
    assert "form-action 'self'" in page.headers["content-security-policy"]


async def test_logs_name_the_record_and_never_the_ticket(
    client: httpx.AsyncClient,
    gateway: UploadGateway,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger="mcp_upload")
    issued = await issue(gateway)
    await post(client, issued.upload_url, [("file", "a.txt", b"x", None)])
    await post(client, issued.upload_url, [("file", "a.txt", b"x", None)])
    assert issued.record.id in caplog.text
    assert "completed" in caplog.text
    assert "ticket_used" in caplog.text
    assert issued.secret not in caplog.text
    assert issued.upload_url not in caplog.text


async def test_filename_is_reduced_to_a_basename_before_forwarding(
    client: httpx.AsyncClient, gateway: UploadGateway, upstream: Upstream
) -> None:
    issued = await issue(gateway)
    response = await post(
        client, issued.upload_url, [("file", "../../../etc/passwd", b"root:x", None)]
    )
    assert response.status_code == 200
    assert response.json()["file"]["name"] == "passwd"
    assert str(upstream.requests[0].url) == "http://backend.test/files/passwd"


# ----- the destination can never be model-controlled ---------------------------------


async def test_a_url_cannot_become_a_destination(gateway: UploadGateway) -> None:
    with pytest.raises(UnknownDestination):
        await gateway.issue("http://169.254.169.254/latest/meta-data")
    with pytest.raises(UnknownDestination):
        await gateway.issue("backend.test")
    assert "url" not in inspect.signature(gateway.issue).parameters


async def test_ticket_for_one_destination_streams_only_there(
    client: httpx.AsyncClient, gateway: UploadGateway, upstream: Upstream
) -> None:
    issued = await issue(gateway, "images")
    await post(client, issued.upload_url, [("file", "a.png", PNG, "image/png")])
    assert str(upstream.requests[0].url).startswith("http://backend.test/images/")


# ----- backend failures -----------------------------------------------------------------


async def test_backend_rejection_is_mapped_not_echoed(
    client: httpx.AsyncClient, gateway: UploadGateway, upstream: Upstream
) -> None:
    upstream.status_code = 500
    issued = await issue(gateway)
    response = await post(client, issued.upload_url, [("file", "a.txt", b"x", None)])
    assert response.status_code == 502
    assert response.json() == {
        "status": "failed",
        "error": "upstream_rejected",
        "id": issued.record.id,
    }
    assert "ok" not in response.text
    record = await gateway.status(issued.record.id)
    assert record["status"] == "failed"
    assert record["error"] == "upstream_rejected"


async def test_unreachable_backend(
    client: httpx.AsyncClient, gateway: UploadGateway, upstream: Upstream
) -> None:
    async def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    gateway._http = httpx.AsyncClient(transport=httpx.MockTransport(boom))  # noqa: SLF001
    issued = await issue(gateway)
    response = await post(client, issued.upload_url, [("file", "a.txt", b"x", None)])
    assert response.status_code == 502
    assert response.json()["error"] == "upstream_unreachable"


# ----- the browser path -----------------------------------------------------------------


async def test_get_renders_a_form_only_while_redeemable(
    client: httpx.AsyncClient, gateway: UploadGateway
) -> None:
    issued = await issue(gateway, "images")
    page = await client.get(issued.upload_url)
    assert page.status_code == 200
    assert 'enctype="multipart/form-data"' in page.text
    assert 'name="file"' in page.text
    assert 'accept="image/*"' in page.text
    assert page.headers["referrer-policy"] == "no-referrer"
    assert page.headers["cache-control"] == "no-store"

    await post(client, issued.upload_url, [("file", "a.png", PNG, "image/png")])
    used = await client.get(issued.upload_url)
    assert used.status_code == 410
    assert "<form" not in used.text

    assert (await client.get("/upload/unknown")).status_code == 404


async def test_browser_post_gets_an_html_result(
    client: httpx.AsyncClient, gateway: UploadGateway
) -> None:
    issued = await issue(gateway)
    response = await post(
        client,
        issued.upload_url,
        [("file", "a.txt", b"x", None)],
        headers={"Accept": "text/html,application/xhtml+xml"},
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "Upload complete" in response.text
    assert "a.txt" in response.text


# ----- helpers --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("multipart/form-data; boundary=abc", ("multipart/form-data", {"boundary": "abc"})),
        (
            'MULTIPART/FORM-DATA; Boundary="abc def"',
            ("multipart/form-data", {"boundary": "abc def"}),
        ),
        ("text/plain;charset=utf-8", ("text/plain", {"charset": "utf-8"})),
        ("", ("", {})),
        (None, ("", {})),
    ],
)
def test_parse_content_type(value: str | None, expected: tuple[str, dict[str, str]]) -> None:
    assert parse_content_type(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("report.pdf", "report.pdf"),
        ("../../../etc/passwd", "passwd"),
        ("C:\\Users\\me\\Desktop\\x.txt", "x.txt"),
        ("..", "upload"),
        ("", "upload"),
        (None, "upload"),
        ("a\x00b\r\n.txt", "ab.txt"),
        ("   spaced.txt  ", "spaced.txt"),
    ],
)
def test_sanitize_filename(value: str | None, expected: str) -> None:
    assert sanitize_filename(value) == expected


async def test_record_states_are_the_documented_set() -> None:
    assert [s.value for s in Status] == ["issued", "redeemed", "completed", "failed"]
