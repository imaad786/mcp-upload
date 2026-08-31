"""The browser path.

Most MCP hosts cannot make an HTTP request with a local file on the user's behalf. What
every host and every person can do is open a URL. So a GET on the ticket URL renders a
small form that POSTs back to the same URL. That makes the pattern usable from a chat
window, not only from an agent with a shell.

The page never shows uploaded content. A ticket is a credential to send bytes, not to
read them, and this page is the only place a ticket URL is loaded in a browser.
"""

from __future__ import annotations

from html import escape

_STYLE = (
    "body{font-family:system-ui,sans-serif;max-width:36rem;margin:3rem auto;padding:0 1rem;"
    "color:#15181d;background:#f6f6f3}"
    "h1{font-size:1.25rem}p,li{line-height:1.5}code{font-size:.9em}"
    "input[type=file]{display:block;margin:1rem 0}"
    "button{font:inherit;padding:.5rem 1rem}"
)


def _document(title: str, body: str) -> str:
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        '<meta name="referrer" content="no-referrer">'
        f"<title>{escape(title)}</title><style>{_STYLE}</style></head>"
        f"<body>{body}</body></html>"
    )


def form(
    *,
    action: str,
    field_name: str,
    accept: tuple[str, ...],
    max_size: int | None,
    expires_at: str,
) -> str:
    limits: list[str] = []
    if max_size is not None:
        limits.append(f"Maximum size {_human(max_size)}.")
    if accept:
        limits.append("Accepted types: " + escape(", ".join(accept)) + ".")
    limits.append(f"This link expires at {escape(expires_at)} and works once.")
    accept_attr = f' accept="{escape(",".join(accept))}"' if accept else ""
    body = (
        "<h1>Upload a file</h1>"
        "<p>" + " ".join(limits) + "</p>"
        f'<form method="post" action="{escape(action)}" enctype="multipart/form-data">'
        f'<input type="file" name="{escape(field_name)}" required{accept_attr}>'
        '<button type="submit">Upload</button></form>'
    )
    return _document("Upload a file", body)


def message(title: str, text: str) -> str:
    return _document(title, f"<h1>{escape(title)}</h1><p>{escape(text)}</p>")


def result(title: str, rows: list[tuple[str, str]]) -> str:
    items = "".join(f"<li>{escape(k)}: <code>{escape(v)}</code></li>" for k, v in rows)
    return _document(title, f"<h1>{escape(title)}</h1><ul>{items}</ul>")


def _human(n: int) -> str:
    value = float(n)
    for unit in ("bytes", "KiB", "MiB", "GiB"):
        if value < 1024 or unit == "GiB":
            return f"{value:.0f} {unit}" if unit == "bytes" else f"{value:.1f} {unit}"
        value /= 1024
    return f"{n} bytes"
