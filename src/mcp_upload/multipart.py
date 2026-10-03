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
    boundary = params.get("boundary")
    if not boundary:
        return "missing_boundary"
    if not boundary.isascii():
        return "bad_multipart"
    return None


def boundary_of(headers: Mapping[str, str]) -> str:
    """The boundary of a request that ``multipart_error`` already accepted."""
    return parse_content_type(headers.get("content-type"))[1]["boundary"]


class Framer:
    """Finds where the multipart body starts and ends, so the parser only ever sees the
    part between the first delimiter and the close delimiter.

    RFC 2046 allows a preamble before the first delimiter and an epilogue after the
    close delimiter, both to be ignored. The streaming parser accepts neither: it
    refuses a preamble outright, and when anything follows the close delimiter, even a
    single CRLF, it never reports the part as finished and holds back its last bytes.
    A conforming request then fails as truncated. So the framer drops the preamble,
    passes the body through untouched, and at the close delimiter hands the parser the
    canonical end and counts the rest as epilogue for the caller to bound.

    The close delimiter is ``CRLF--boundary--``, which cannot occur inside a part, so
    finding it is exact. A copy of the last few bytes is kept to catch a delimiter
    split across two reads.
    """

    def __init__(self, boundary: str, *, max_preamble: int) -> None:
        self._open = b"--" + boundary.encode("latin-1")
        self._close = b"\r\n" + self._open + b"--"
        self._max_preamble = max_preamble
        self._buf = b""
        self._carry = b""
        self.started = False
        self.closed = False
        self.epilogue = 0

    def feed(self, chunk: bytes) -> bytes:
        """Return the bytes the parser should see for this chunk, possibly none.
        Raises ``ValueError`` when the preamble runs past ``max_preamble``."""
        if self.closed:
            self.epilogue += len(chunk)
            return b""
        if not self.started:
            self._buf += chunk
            if self._buf.startswith(self._open):
                chunk = self._buf
            elif len(self._buf) < len(self._open) and self._open.startswith(self._buf):
                return b""
            else:
                at = self._buf.find(b"\r\n" + self._open)
                if at < 0:
                    if len(self._buf) > self._max_preamble:
                        raise ValueError("no multipart delimiter within the preamble limit")
                    return b""
                chunk = self._buf[at + 2 :]
            self._buf = b""
            self.started = True
        n = len(self._close)
        straddle = (self._carry + chunk[: n - 1]).find(self._close)
        if straddle >= 0:
            end = straddle + n - len(self._carry)
        else:
            at = chunk.find(self._close)
            end = -1 if at < 0 else at + n
        if end >= 0:
            self.closed = True
            self.epilogue = len(chunk) - end
            return chunk[:end] + b"\r\n"
        if len(chunk) >= n - 1:
            self._carry = chunk[-(n - 1) :]
        else:
            self._carry = (self._carry + chunk)[-(n - 1) :]
        return chunk


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
