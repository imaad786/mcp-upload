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
import unicodedata
from collections.abc import Mapping
from pathlib import PurePosixPath
from urllib.parse import unquote

MULTIPART_FORM_DATA = "multipart/form-data"
FORM_URLENCODED = "application/x-www-form-urlencoded"

_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")
# Unicode categories that render as nothing or rearrange text: controls, format
# characters (bidi overrides, zero-width joiners), and line and paragraph separators.
_INVISIBLE = frozenset({"Cc", "Cf", "Zl", "Zp"})


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


# A media type with its parameters, as RFC 9110 writes one, but stricter: each
# parameter is ``token=token`` or ``token="quoted string"``, only spaces may sit around
# the semicolons, and nothing outside printable ASCII is allowed anywhere, so no CR, LF
# or other control character can get through. The value is kept as the client wrote
# it, because it becomes a header to the backend and the ``mimeType`` a client may
# compare with what it declared.
_TCHARS = r"[!#$%&'*+.^_`|~0-9A-Za-z-]+"
_QUOTED = r'"(?:[ !#-\[\]-~]|\\[ -~])*"'
_MEDIA_TYPE = re.compile(rf"({_TCHARS}/{_TCHARS})((?: *; *{_TCHARS}=(?:{_TCHARS}|{_QUOTED}))*)")
MAX_MEDIA_TYPE_LENGTH = 255


def split_media_type(value: str) -> tuple[str, str] | None:
    """Check a declared media type. Returns its ``type/subtype`` in lowercase, for
    matching, and the whole value as declared, parameters included, for recording.
    None when it is not a media type, its parameters are malformed, or it is longer
    than ``MAX_MEDIA_TYPE_LENGTH``."""
    declared = value.strip(" \t")
    if len(declared) > MAX_MEDIA_TYPE_LENGTH:
        return None
    match = _MEDIA_TYPE.fullmatch(declared)
    if match is None:
        return None
    return match.group(1).lower(), declared


def content_type_of_part(head: bytes) -> str | None:
    """The Content-Type value in a multipart part's header block, as sent. ``head`` is
    everything from the delimiter line up to the blank line. The last Content-Type
    wins, as it does in the parser. A folded continuation line stays in the value with
    its CRLF, so ``split_media_type`` refuses it. None when there is no Content-Type."""
    found: str | None = None
    in_type = False
    for line in head.split(b"\r\n")[1:]:
        if line[:1] in (b" ", b"\t"):
            if in_type and found is not None:
                found += "\r\n" + line.decode("latin-1")
            continue
        name, colon, value = line.partition(b":")
        in_type = bool(colon) and name.strip().lower() == b"content-type"
        if in_type:
            found = value.decode("latin-1").strip(" \t")
    return found


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


def raw_body_allowed(headers: Mapping[str, str]) -> bool:
    """Whether a request that ``multipart_error`` refused may be taken as a raw upload,
    with the body as the file. A URL-encoded form never is: it is what a browser form
    without an enctype and ``curl --data`` send, and reading it as a file is how a text
    field becomes file content. Other multipart types are not files either."""
    media_type, _ = parse_content_type(headers.get("content-type"))
    return media_type != FORM_URLENCODED and not media_type.startswith("multipart/")


# One ``; name=value`` parameter of a Content-Disposition header, where the value is a
# token or a quoted string that may contain semicolons and escaped quotes.
_DISPOSITION_PARAM = re.compile(
    r";\s*([!#$%&'*+.^_`|~0-9A-Za-z-]+)\s*=\s*(\"(?:[^\"\\]|\\.)*\"|[^;]*)"
)


def filename_from_disposition(value: str | None) -> str | None:
    """The filename a ``Content-Disposition`` request header names, unsanitized.

    The RFC 8187 form ``filename*=UTF-8''%E2%82%AC.pdf`` wins over plain ``filename``,
    as RFC 6266 says, when its charset is UTF-8 or ISO-8859-1 and it decodes cleanly.
    Otherwise the plain form is used. None when neither is present.
    """
    if not value:
        return None
    params: dict[str, str] = {}
    for key, raw in _DISPOSITION_PARAM.findall(value):
        val = raw.strip()
        if len(val) >= 2 and val[0] == val[-1] == '"':
            val = re.sub(r"\\(.)", r"\1", val[1:-1])
        params.setdefault(key.lower(), val)
    extended = params.get("filename*")
    if extended:
        charset, _, rest = extended.partition("'")
        _, quote, encoded = rest.partition("'")
        if quote and charset.lower() in ("utf-8", "iso-8859-1"):
            try:
                return unquote(encoded, encoding=charset.lower(), errors="strict")
            except (UnicodeDecodeError, LookupError):
                pass
    return params.get("filename") or None


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
    component survives. The name is normalized to NFC so one name has one spelling.
    Control characters are dropped, and so are invisible format characters, which
    include the bidirectional overrides that make ``invoice\u202egpj.exe`` display
    as ``invoiceexe.jpg``. The result is cut to 255 bytes of UTF-8, the usual
    filesystem limit, on a character boundary. An empty or dot-only result falls back
    to the default.
    """
    if not name:
        return default
    base = unicodedata.normalize("NFC", name)
    base = "".join(ch for ch in base if unicodedata.category(ch) not in _INVISIBLE)
    base = PurePosixPath(base.replace("\\", "/")).name
    base = _CONTROL_CHARS.sub("", base).strip()
    if base in ("", ".", ".."):
        return default
    return _fit(base, 255)


def _fit(name: str, limit: int) -> str:
    """Cut ``name`` to ``limit`` bytes of UTF-8 on a character boundary, keeping a
    short extension so the backend still sees what kind of file it is."""
    if len(name.encode("utf-8")) <= limit:
        return name
    stem, dot, ext = name.rpartition(".")
    suffix = (dot + ext).encode("utf-8") if stem and len(ext.encode("utf-8")) <= 16 else b""
    head = (stem if suffix else name).encode("utf-8")[: limit - len(suffix)]
    return head.decode("utf-8", "ignore") + suffix.decode("utf-8")
