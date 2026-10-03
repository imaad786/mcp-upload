"""The browser page: a plain form, enhanced by one nonce'd script.

The policy must allow exactly that script and nothing else, and the form must still
work as a plain POST for a browser with scripts off.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import httpx
import pytest

from mcp_upload import UploadGateway, page
from tests.conftest import multipart


def policy(response: httpx.Response) -> dict[str, str]:
    directives = response.headers["content-security-policy"].split(";")
    return {d.strip().split(" ", 1)[0]: d.strip().partition(" ")[2] for d in directives}


async def test_form_carries_a_script_with_the_policy_nonce(
    client: httpx.AsyncClient, gateway: UploadGateway
) -> None:
    issued = await gateway.issue("files")
    first = await client.get(issued.upload_url)
    second = await client.get(issued.upload_url)
    nonces = []
    for response in (first, second):
        scripts = re.findall(r'<script nonce="([^"]+)">', response.text)
        assert len(scripts) == 1
        assert response.text.count("<script") == 1
        rules = policy(response)
        assert rules["script-src"] == f"'nonce-{scripts[0]}'"
        assert rules["default-src"] == "'none'"
        assert rules["connect-src"] == "'self'"
        assert rules["form-action"] == "'self'"
        assert rules["style-src"] == "'unsafe-inline'"
        assert "unsafe-eval" not in response.headers["content-security-policy"]
        nonces.append(scripts[0])
    assert nonces[0] != nonces[1]
    assert len(nonces[0]) >= 22


async def test_form_has_no_inline_handlers_or_external_resources(
    client: httpx.AsyncClient, gateway: UploadGateway
) -> None:
    issued = await gateway.issue("files")
    html = (await client.get(issued.upload_url)).text
    assert not re.search(r"\son[a-z]+\s*=", html)
    assert "src=" not in html
    assert "http:" not in html and "https:" not in html
    assert "innerHTML" not in html


async def test_form_still_posts_without_scripts(
    client: httpx.AsyncClient, gateway: UploadGateway
) -> None:
    issued = await gateway.issue("files")
    html = (await client.get(issued.upload_url)).text
    form = re.search(r"<form[^>]*>", html)
    assert form is not None
    assert 'method="post"' in form.group(0)
    assert f'action="{httpx.URL(issued.upload_url).path}"' in form.group(0)
    assert 'enctype="multipart/form-data"' in form.group(0)
    assert re.search(r'<input type="file" name="file" required>', html)
    # What a browser with scripts off sends: the form body, asking for HTML.
    body, content_type = multipart([("file", "a.txt", b"hi", "text/plain")])
    response = await client.post(
        issued.upload_url,
        content=body,
        headers={"Content-Type": content_type, "Accept": "text/html"},
    )
    assert response.status_code == 200
    assert "Upload complete" in response.text


async def test_what_the_script_sends_gets_json(
    client: httpx.AsyncClient, gateway: UploadGateway
) -> None:
    # The script posts the same form body with Accept: application/json.
    issued = await gateway.issue("files")
    body, content_type = multipart([("file", "a.txt", b"hi", "text/plain")])
    response = await client.post(
        issued.upload_url,
        content=body,
        headers={"Content-Type": content_type, "Accept": "application/json"},
    )
    assert response.json()["status"] == "completed"


async def test_other_pages_have_no_script_and_the_strict_policy(
    client: httpx.AsyncClient, gateway: UploadGateway
) -> None:
    assert "<script" not in page.message("t", "x")
    assert "<script" not in page.result("t", [("a", "b")])
    unknown = await client.get("/upload/nope")
    assert "<script" not in unknown.text
    assert "script-src" not in unknown.headers["content-security-policy"]
    assert "connect-src" not in unknown.headers["content-security-policy"]


def test_form_without_a_nonce_has_no_script() -> None:
    html = page.form(action="/u/x", field_name="file", accept=(), max_size=None, expires_at="t")
    assert "<script" not in html


@pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
def test_script_is_valid_javascript(tmp_path: Path) -> None:
    target = tmp_path / "page.js"
    target.write_text(page._SCRIPT)
    result = subprocess.run(["node", "--check", str(target)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
