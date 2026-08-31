"""Request-level checks that run before a ticket is spent, and filename hygiene.

The rule these enforce: nothing that can be judged from the headers alone may cost the
ticket. A request whose content type says it cannot carry a file is rejected with the
ticket untouched, so a stray or malicious non-multipart POST cannot burn someone's
pending upload.

The content type check is deliberately strict and deliberately case-insensitive. The
form-parsing helpers in common Python frameworks accept ``application/x-www-form-urlencoded``
as a form too, and return an empty form with no error for anything else, so "is this a
form?" is the wrong question. Media types are case-insensitive per RFC 9110, and a naive
exact comparison rejects a valid ``MULTIPART/FORM-DATA``.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import PurePosixPath

MULTIPART_FORM_DATA = "multipart/form-data"

_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")


def parse_content_type(value: str | None) -> tuple[str, dict[str, str]]:
    """Split a Content-Type header into a lowercased media type and its parameters."""
    if not value:
        return "", {}
    head, *rest = value.split(";")
    params: dict[str, str] = {}
    for item in rest:
        if "=" not in item:
            continue
        key, raw = item.split("=", 1)
        val = raw.strip()
        if len(val) >= 2 and val[0] == val[-1] == '"':
            val = val[1:-1]
        params[key.strip().lower()] = val
    return head.strip().lower(), params


def multipart_error(headers: Mapping[str, str]) -> str | None:
    """Return an error code if this request cannot carry a file, else None."""
    media_type, params = parse_content_type(headers.get("content-type"))
    if media_type != MULTIPART_FORM_DATA:
        return "not_multipart"
    if not params.get("boundary"):
        return "missing_boundary"
    return None


def sanitize_filename(name: str | None, default: str = "upload") -> str:
    """Reduce a client-supplied filename to a safe base name.

    The multipart parser hands the filename through untouched, so ``../../etc/passwd``
    arrives exactly like that. Since the name is forwarded to the backend, which may
    well write it to disk, both separator conventions are collapsed and only the last
    component survives. Control characters are dropped. An empty or dot-only result
    falls back to the default.
    """
    if not name:
        return default
    base = PurePosixPath(name.replace("\\", "/")).name
    base = _CONTROL_CHARS.sub("", base).strip()
    if base in ("", ".", ".."):
        return default
    return base[:255]
