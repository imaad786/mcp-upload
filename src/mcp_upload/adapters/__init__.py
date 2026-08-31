"""Adapters that register the upload endpoint on a server framework.

Each adapter is a few lines because both supported frameworks expose the same
``custom_route`` decorator and serve it from the same Starlette application as the MCP
transport. The core never imports a framework; the adapters only need an object with
that one method, which is described structurally so neither framework has to be
installed to type-check the other's adapter.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol


class HasCustomRoute(Protocol):
    def custom_route(
        self,
        path: str,
        methods: list[str],
        name: str | None = ...,
        include_in_schema: bool = ...,
    ) -> Callable[[Any], Any]: ...
