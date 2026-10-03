"""Function destinations: uploads that land in the server author's own code.

An HTTP destination needs a second service to receive the bytes. A sink is an async
callable registered on a ``Destination`` instead, so an upload can go to local disk, an
object store SDK or an in-process pipeline without one. The registry stays closed:
the author registers the callable at startup, and a tool can still only pick it by
name. Nothing from a tool argument can choose code to run.

A sink receives an ``IncomingFile`` and iterates it::

    async def store(upload: IncomingFile) -> None:
        async with open_somewhere(upload.filename) as out:
            async for chunk in upload:
                await out.write(chunk)
            await out.commit()

The contract mirrors what an HTTP backend gets. Iteration ends normally only after the
whole request validated, including the declared size and digest. On any failure (the
client vanished, the file was too large, the digest did not match, the client stalled)
the next read raises ``UploadAborted`` instead, so a sink written as above can never
commit a bad upload. If the sink raises, the upload fails as ``sink_failed``. If it
returns before reading to the end, the upload fails as ``upstream_closed_early``.

``filesystem`` is a ready-made sink that writes to a directory.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import os
import tempfile
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from typing import Any

Sink = Callable[["IncomingFile"], Awaitable[None]]


class UploadAborted(Exception):
    """Raised from iteration over an ``IncomingFile`` when the upload failed. ``code``
    is the error code the upload is recorded with, such as ``client_disconnected``,
    ``too_large`` or ``digest_mismatch``. A sink should let it propagate, or clean up
    and re-raise it. It never means the bytes so far are a complete file."""

    def __init__(self, code: str, details: Mapping[str, Any] | None = None) -> None:
        super().__init__(code)
        self.code = code
        self.details = dict(details) if details else None


class IncomingFile:
    """One upload, as a sink sees it: what is known about the file, and an async
    iterator over its bytes as they arrive.

    ``filename`` is already reduced to a safe base name. ``media_type`` is the declared
    type, checked against the ticket's accept list. ``expected_size`` and
    ``expected_sha256`` (hex) are what the uploader declared at issue, if anything;
    the gateway checks them before ending the iteration, so a sink need not.

    The chunks are bounded in number between the client and the sink: when the sink
    reads slowly, the client is slowed down rather than the server buffering.
    """

    __slots__ = (
        "record_id",
        "destination",
        "filename",
        "media_type",
        "expected_size",
        "expected_sha256",
        "_read",
    )

    def __init__(
        self,
        *,
        record_id: str,
        destination: str,
        filename: str,
        media_type: str,
        expected_size: int | None,
        expected_sha256: str | None,
        read: Callable[[], Awaitable[bytes | None]],
    ) -> None:
        self.record_id = record_id
        self.destination = destination
        self.filename = filename
        self.media_type = media_type
        self.expected_size = expected_size
        self.expected_sha256 = expected_sha256
        self._read = read

    def __aiter__(self) -> IncomingFile:
        return self

    async def __anext__(self) -> bytes:
        chunk = await self._read()
        if chunk is None:
            raise StopAsyncIteration
        return chunk

    def __repr__(self) -> str:
        return (
            f"IncomingFile(record_id={self.record_id!r}, filename={self.filename!r}, "
            f"media_type={self.media_type!r})"
        )


def filesystem(
    directory: str | os.PathLike[str],
    *,
    name_template: str = "{id}-{filename}",
    overwrite: bool = False,
    fsync: bool = True,
    max_threads: int | None = None,
) -> Sink:
    """A sink that writes each upload to a file in ``directory``.

    The bytes go to a temporary file in the same directory, named ``.upload-*.part``
    and created with mode 0600. Only after the iteration ended normally, which means
    the whole request validated, is the file flushed, synced to disk and moved to its
    final name in one atomic step. On any failure the temporary file is deleted and no
    final file appears. A reader of the directory therefore sees a complete, verified
    file or nothing.

    The final name is ``name_template`` with ``{id}`` (the record id) and
    ``{filename}`` (the sanitized filename) filled in. It must be a single file name
    inside ``directory``; a template that yields a path elsewhere fails the upload.
    With ``overwrite=False`` (the default) an existing file of the same name is never
    replaced and the upload fails as ``sink_failed``. The default template includes
    the record id, so names do not collide.

    Durability: with ``fsync=True`` the file's data is synced before the rename and the
    directory is synced after it, so a completed upload survives a power loss on
    filesystems that honour fsync. With ``fsync=False`` both are skipped and a crash
    shortly after completion can lose the file, though never leave a partial one under
    the final name. A process killed mid-upload leaves its ``.upload-*.part`` file
    behind; sweep those at startup if that matters.

    File operations run on a small thread pool owned by this sink (``max_threads``,
    by default the same size as asyncio's default pool), so a slow disk never blocks
    the event loop. Each upload has at most one file operation outstanding.
    """
    root = Path(os.path.abspath(directory))
    if not root.is_dir():
        raise ValueError(f"{root} is not a directory")
    workers = max_threads or min(32, (os.cpu_count() or 1) + 4)
    pool = concurrent.futures.ThreadPoolExecutor(
        max_workers=workers, thread_name_prefix="mcp-upload-fs"
    )

    async def sink(upload: IncomingFile) -> None:
        final = _final_path(root, name_template, upload)
        created = pool.submit(tempfile.mkstemp, dir=root, prefix=".upload-", suffix=".part")
        try:
            fd, temp = await asyncio.wrap_future(created)
        except BaseException:
            created.add_done_callback(_discard_created)
            raise
        spool = _Spool(fd, temp, str(final))
        # The one file operation in flight, if any. If this coroutine is cancelled while
        # a write runs in a thread, closing the descriptor here would let the thread
        # write into whatever file next reuses the number, so cleanup runs after it.
        pending: concurrent.futures.Future[None] | None = None

        async def run(fn: Callable[..., None], *args: Any) -> None:
            nonlocal pending
            pending = pool.submit(fn, *args)
            await asyncio.wrap_future(pending)

        committed = False
        try:
            async for chunk in upload:
                await run(spool.write, chunk)
            await run(spool.seal, fsync)
            await run(spool.commit, overwrite, fsync)
            committed = True
        finally:
            if not committed:
                if pending is not None and not pending.done():
                    pending.add_done_callback(lambda _: spool.discard())
                else:
                    spool.discard()

    return sink


def _final_path(root: Path, template: str, upload: IncomingFile) -> Path:
    name = template.format(id=upload.record_id, filename=upload.filename)
    separators = {"/", "\x00", os.sep, os.altsep or "/"}
    if name in ("", ".", "..") or any(s in name for s in separators):
        raise ValueError("the name template must produce a single file name")
    final = root / name
    if Path(os.path.abspath(final)).parent != root:
        raise ValueError("the final path is outside the directory")
    return final


def _discard_created(future: concurrent.futures.Future[tuple[int, str]]) -> None:
    if not future.cancelled() and future.exception() is None:
        fd, temp = future.result()
        _Spool(fd, temp, "").discard()


class _Spool:
    """One upload's temporary file. Every method runs in the sink's thread pool, one at
    a time per upload, so none of them races another."""

    def __init__(self, fd: int, temp: str, final: str) -> None:
        self.fd = fd
        self.temp = temp
        self.final = final
        self.linked = False

    def write(self, data: bytes) -> None:
        view = memoryview(data)
        while view:
            view = view[os.write(self.fd, view) :]

    def seal(self, sync: bool) -> None:
        try:
            if sync:
                os.fsync(self.fd)
        finally:
            os.close(self.fd)
            self.fd = -1

    def commit(self, overwrite: bool, sync: bool) -> None:
        if overwrite:
            os.replace(self.temp, self.final)
            self.linked = True
        else:
            # A hard link fails if the name exists, which a rename would not, so this
            # cannot replace a file that is already there.
            os.link(self.temp, self.final)
            self.linked = True
            os.unlink(self.temp)
        if sync and hasattr(os, "O_DIRECTORY"):
            fd = os.open(os.path.dirname(self.final), os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

    def discard(self) -> None:
        """Undo whatever happened. Also removes the final file if the commit went
        through but the upload is being reported as failed, for instance because the
        directory sync failed or the sink was cancelled during the commit."""
        if self.fd >= 0:
            with contextlib.suppress(OSError):
                os.close(self.fd)
            self.fd = -1
        with contextlib.suppress(OSError):
            os.unlink(self.temp)
        if self.linked:
            with contextlib.suppress(OSError):
                os.unlink(self.final)
            self.linked = False


__all__ = ["IncomingFile", "Sink", "UploadAborted", "filesystem"]
