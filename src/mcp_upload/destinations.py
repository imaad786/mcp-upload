"""Where uploads go.

A destination is an HTTP endpoint or an async function (a sink) the server author
declares at startup. The registry is a closed set of them, looked up by name. A tool
may choose a destination by name. Nothing that arrives in a tool argument can become a
URL, a host, a port or a path.

That restriction is the point. The reason this library exists is that letting a model
hand the server a URL to fetch is a server-side request forgery. Letting a model hand
the server a URL to stream a file into is the same hole with a request body attached.
So the obvious ``issue_ticket(url=...)`` does not exist, and a tool that wants to
influence the destination can only pick from the names the author registered. The same
holds for sinks: the callable is registered by the author, and a tool argument can only
name it, never supply code.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal
from urllib.parse import quote

from .sinks import Sink


class UnknownDestination(LookupError):
    pass


@dataclass(frozen=True, slots=True)
class Destination:
    """Where uploads for one name go: an upstream HTTP endpoint (``url``) or an async
    function in this process (``sink``). Exactly one of the two must be given.

    ``url`` may contain ``{id}`` and ``{filename}``, filled at upload time with the
    record id and the sanitized filename, both percent-encoded. ``encoding`` says how
    the bytes are sent on: ``raw`` puts them in the request body as-is with the file's
    media type, ``multipart`` wraps them in a single-part ``multipart/form-data`` body
    under ``field_name`` for backends that expect a form upload. Either way the bytes
    are forwarded as they arrive and never stored.

    ``sink`` is awaited with an ``IncomingFile`` and reads the bytes by iterating it.
    See ``mcp_upload.sinks``. ``method``, ``headers``, ``encoding`` and ``field_name``
    do not apply to a sink. ``timeout`` does, differently: it is how long the sink may
    keep running once the upload has stopped arriving, whether it ended or failed.

    ``max_size`` and ``accept`` are defaults for tickets issued against this destination.
    A ticket can be issued with tighter values, never looser.
    """

    name: str
    url: str | None = None
    method: str = "PUT"
    headers: Mapping[str, str] = field(default_factory=dict)
    encoding: Literal["raw", "multipart"] = "raw"
    field_name: str = "file"
    max_size: int | None = None
    accept: tuple[str, ...] = ()
    timeout: float = 60.0
    sink: Sink | None = None

    def __post_init__(self) -> None:
        if (self.url is None) == (self.sink is None):
            raise ValueError(f"destination {self.name!r} needs exactly one of url and sink")
        if self.sink is not None and not callable(self.sink):
            raise TypeError(f"destination {self.name!r}: sink must be an async callable")

    def build_url(self, record_id: str, filename: str) -> str:
        if self.url is None:
            raise ValueError(f"destination {self.name!r} is a sink and has no url")
        return self.url.replace("{id}", quote(record_id, safe="")).replace(
            "{filename}", quote(filename, safe="")
        )


class Registry:
    def __init__(self, *destinations: Destination) -> None:
        self._by_name: dict[str, Destination] = {}
        for destination in destinations:
            if destination.name in self._by_name:
                raise ValueError(f"destination {destination.name!r} registered twice")
            self._by_name[destination.name] = destination

    def get(self, name: str) -> Destination:
        try:
            return self._by_name[name]
        except KeyError:
            raise UnknownDestination(name) from None

    def names(self) -> tuple[str, ...]:
        return tuple(self._by_name)

    def __contains__(self, name: object) -> bool:
        return name in self._by_name
