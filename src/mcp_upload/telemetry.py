"""Traces and metrics through OpenTelemetry, when it is installed.

The library depends only on the OpenTelemetry API, and only optionally (the ``otel``
extra; the official MCP SDK already pulls it in). The API does nothing until the
application configures an SDK, so an uninstrumented server pays for a few no-op calls
per upload. Without the package installed every hook here is a no-op of our own.

What is emitted, all under the instrumentation scope ``mcp_upload``:

* span ``mcp_upload.upload`` around each redeemed upload, with the record id, the
  destination, the outcome and the byte count as attributes;
* counter ``mcp_upload.tickets.issued``, by destination;
* counter ``mcp_upload.uploads``, by destination and outcome (``completed`` or the
  error code, including refusals before a ticket was spent);
* counter ``mcp_upload.upload.bytes`` of bytes forwarded, by destination and outcome;
* histogram ``mcp_upload.upload.duration`` in seconds, by destination and outcome.

Record ids and destination names are safe to export. The ticket never is.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from typing import Any

try:
    from opentelemetry import metrics as _metrics
    from opentelemetry import trace as _trace
except ImportError:  # pragma: no cover - exercised only without the package
    _metrics = None  # type: ignore[assignment]
    _trace = None  # type: ignore[assignment]

SCOPE = "mcp_upload"


class Telemetry:
    def __init__(self, enabled: bool = True) -> None:
        self.enabled = enabled and _trace is not None and _metrics is not None
        if not self.enabled:
            return
        from . import __version__

        self._tracer = _trace.get_tracer(SCOPE, __version__)
        meter = _metrics.get_meter(SCOPE, __version__)
        self._issued = meter.create_counter(
            "mcp_upload.tickets.issued", unit="{ticket}", description="Tickets issued"
        )
        self._uploads = meter.create_counter(
            "mcp_upload.uploads", unit="{upload}", description="Upload requests by outcome"
        )
        self._bytes = meter.create_counter(
            "mcp_upload.upload.bytes", unit="By", description="File bytes received"
        )
        self._duration = meter.create_histogram(
            "mcp_upload.upload.duration", unit="s", description="Time from redemption to end"
        )

    def issued(self, destination: str) -> None:
        if self.enabled:
            self._issued.add(1, {"mcp_upload.destination": destination})

    def refused(self, code: str) -> None:
        if self.enabled:
            self._uploads.add(1, {"mcp_upload.outcome": code})

    @contextlib.contextmanager
    def upload(self, record_id: str, destination: str) -> Iterator[Any]:
        if not self.enabled:
            yield None
            return
        with self._tracer.start_as_current_span(
            "mcp_upload.upload",
            attributes={"mcp_upload.record_id": record_id, "mcp_upload.destination": destination},
        ) as span:
            yield span

    def finished(
        self, span: Any, destination: str, outcome: str, size: int, seconds: float
    ) -> None:
        if not self.enabled:
            return
        attributes = {"mcp_upload.destination": destination, "mcp_upload.outcome": outcome}
        self._uploads.add(1, attributes)
        self._bytes.add(size, attributes)
        self._duration.record(seconds, attributes)
        if span is not None:
            span.set_attribute("mcp_upload.outcome", outcome)
            span.set_attribute("mcp_upload.size", size)
            if outcome != "completed":
                span.set_status(_trace.Status(_trace.StatusCode.ERROR, outcome))
