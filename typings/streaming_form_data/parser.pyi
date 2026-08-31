from collections.abc import Callable, Mapping

from .targets import BaseTarget

class ParseFailedException(Exception): ...

class UnexpectedPartException(ParseFailedException):
    part_name: str
    def __init__(self, message: str, part_name: str) -> None: ...

class StreamingFormDataParser:
    headers: Mapping[str, str]
    def __init__(self, headers: Mapping[str, str], strict: bool = ...) -> None: ...
    def register(
        self,
        name: str,
        target: BaseTarget,
        matches: Callable[[str, str], bool] | None = ...,
    ) -> None: ...
    def data_received(self, data: bytes) -> None: ...
    async def adata_received(self, data: bytes) -> None: ...
