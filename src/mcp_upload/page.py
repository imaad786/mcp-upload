"""The browser path.

Most MCP hosts cannot make an HTTP request with a local file on the user's behalf. What
every host and every person can do is open a URL. So a GET on the ticket URL renders a
small form that POSTs back to the same URL. That makes the pattern usable from a chat
window, not only from an agent with a shell.

The form works on its own. A short inline script improves it where scripts run: the
file can be dropped onto the page, a progress bar shows how much has been sent, and
the outcome appears in place. The script carries a nonce minted for each response, and
the page's Content-Security-Policy allows only a script with that nonce, so the policy
still refuses every other script. It adds no inline event handlers and loads nothing.
With scripts off, the plain form posts as before.

The page never shows uploaded content. A ticket is a credential to send bytes, not to
read them, and this page is the only place a ticket URL is loaded in a browser.
"""

from __future__ import annotations

from html import escape

_STYLE = (
    "body{font-family:system-ui,sans-serif;max-width:36rem;margin:3rem auto;padding:0 1rem;"
    "color:#15181d;background:#f6f6f3}"
    "h1{font-size:1.25rem}p,li{line-height:1.5}code{font-size:.9em;word-break:break-all}"
    "input[type=file]{display:block;margin:1rem 0}"
    "button{font:inherit;padding:.5rem 1rem}"
    "form.drop{outline:2px dashed #4a6fa5;outline-offset:.5rem}"
    "progress{display:block;width:100%;margin:1rem 0}progress[hidden]{display:none}"
    "pre{white-space:pre-wrap;font-size:.85em}"
)

# Progressive enhancement for the form above it. It sends the same multipart body the
# form would, with XMLHttpRequest because fetch reports no upload progress, and asks
# for JSON so it can show the outcome. Everything it writes goes through textContent,
# so nothing from a response is ever parsed as HTML.
_SCRIPT = """(function () {
  var form = document.getElementById("upload");
  if (!form || !window.XMLHttpRequest || !window.FormData) return;
  var input = form.querySelector("input[type=file]");
  var button = form.querySelector("button");
  var bar = document.createElement("progress");
  bar.max = 100;
  bar.value = 0;
  bar.hidden = true;
  var note = document.createElement("p");
  note.setAttribute("role", "status");
  form.appendChild(bar);
  form.parentNode.insertBefore(note, form.nextSibling);
  note.textContent = "You can also drop the file onto this page.";

  function element(tag, text) {
    var node = document.createElement(tag);
    if (text !== undefined) node.textContent = text;
    return node;
  }
  function row(list, key, value) {
    var item = element("li", key + ": ");
    item.appendChild(element("code", String(value)));
    list.appendChild(item);
  }
  function finish(title, build) {
    var holder = element("div");
    build(holder);
    form.parentNode.replaceChild(holder, form);
    document.querySelector("h1").textContent = title;
    note.textContent = "";
  }
  function failed(code, details, again) {
    if (again) {
      bar.hidden = true;
      button.disabled = false;
      input.disabled = false;
      note.textContent = "Upload failed: " + code + ". Try again in a moment.";
      return;
    }
    finish("Upload failed", function (holder) {
      holder.appendChild(element("p", "Error: " + code));
      if (details) holder.appendChild(element("pre", JSON.stringify(details, null, 2)));
    });
  }

  ["dragenter", "dragover"].forEach(function (name) {
    document.addEventListener(name, function (event) {
      event.preventDefault();
      form.classList.add("drop");
    });
  });
  ["dragleave", "drop"].forEach(function (name) {
    document.addEventListener(name, function (event) {
      event.preventDefault();
      form.classList.remove("drop");
    });
  });
  document.addEventListener("drop", function (event) {
    var files = event.dataTransfer && event.dataTransfer.files;
    if (!files || !files.length || input.disabled) return;
    input.files = files;
    note.textContent = "Selected " + files[0].name + ".";
  });

  form.addEventListener("submit", function (event) {
    if (!input.files || !input.files.length) return;
    event.preventDefault();
    var data = new FormData(form);
    var request = new XMLHttpRequest();
    request.open("POST", form.action);
    request.setRequestHeader("Accept", "application/json");
    request.upload.addEventListener("progress", function (progress) {
      if (!progress.lengthComputable) return;
      var percent = Math.floor((progress.loaded / progress.total) * 100);
      bar.value = percent;
      note.textContent = "Sent " + percent + "%.";
    });
    request.addEventListener("load", function () {
      var body = null;
      try {
        body = JSON.parse(request.responseText);
      } catch (error) {
        body = null;
      }
      if (body && body.status === "completed") {
        var file = body.file || {};
        finish("Upload complete", function (holder) {
          var list = element("ul");
          row(list, "id", body.id);
          row(list, "name", file.name || "");
          row(list, "size", file.size || 0);
          row(list, "sha-256 (base64url)", (file.digest || {}).value || "");
          holder.appendChild(list);
        });
      } else {
        var code = body && body.error ? body.error : "HTTP " + request.status;
        failed(code, body && body.details, code === "too_many_uploads");
      }
    });
    request.addEventListener("error", function () {
      failed("the connection failed before the upload finished", null, false);
    });
    button.disabled = true;
    input.disabled = true;
    bar.hidden = false;
    note.textContent = "Sending.";
    request.send(data);
  });
})();"""


def _document(title: str, body: str, script: str = "") -> str:
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        '<meta name="referrer" content="no-referrer">'
        f"<title>{escape(title)}</title><style>{_STYLE}</style></head>"
        f"<body>{body}{script}</body></html>"
    )


def form(
    *,
    action: str,
    field_name: str,
    accept: tuple[str, ...],
    max_size: int | None,
    expires_at: str,
    nonce: str | None = None,
) -> str:
    """The upload form. With a ``nonce`` it also carries the enhancing script, which
    runs only if the response's policy names the same nonce."""
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
        f'<form id="upload" method="post" action="{escape(action)}"'
        ' enctype="multipart/form-data">'
        f'<input type="file" name="{escape(field_name)}" required{accept_attr}>'
        '<button type="submit">Upload</button></form>'
    )
    script = f'<script nonce="{escape(nonce)}">{_SCRIPT}</script>' if nonce else ""
    return _document("Upload a file", body, script)


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
