"""Linux nonblocking stdio; no threads, unbounded buffering or orphan readers."""

from __future__ import annotations

import asyncio
import json
import os

MAX_LINE = 1_500_000  # one MiB artifact + base64 + closed envelope


class IPCError(Exception):
    pass


class FDChannel:
    def __init__(self, input_fd: int, output_fd: int) -> None:
        self.input_fd, self.output_fd = input_fd, output_fd
        self._input_blocking = os.get_blocking(input_fd)
        self._output_blocking = os.get_blocking(output_fd)
        os.set_blocking(input_fd, False)
        os.set_blocking(output_fd, False)
        self.buffer = bytearray()
        self.eof = False
        self._lock = asyncio.Lock()

    async def _ready(self, fd: int, *, write: bool = False) -> None:
        loop = asyncio.get_running_loop()
        ready = loop.create_future()

        def wake() -> None:
            if not ready.done():
                ready.set_result(None)

        add = loop.add_writer if write else loop.add_reader
        remove = loop.remove_writer if write else loop.remove_reader
        add(fd, wake)
        try:
            await ready
        finally:
            remove(fd)

    async def receive(self) -> dict[str, object] | None:
        while True:
            end = self.buffer.find(b"\n")
            if end >= 0:
                if end > MAX_LINE:
                    raise IPCError("browser_input_limit")
                raw = bytes(self.buffer[:end])
                del self.buffer[: end + 1]
                try:
                    result = json.loads(raw)
                except (ValueError, UnicodeError):
                    raise IPCError("browser_invalid_input") from None
                if not isinstance(result, dict):
                    raise IPCError("browser_invalid_input")
                return result
            if self.eof:
                if self.buffer:
                    raise IPCError("browser_incomplete_input")
                return None
            if len(self.buffer) > MAX_LINE:
                raise IPCError("browser_input_limit")
            try:
                chunk = os.read(self.input_fd, min(65536, MAX_LINE + 1 - len(self.buffer)))
            except BlockingIOError:
                await self._ready(self.input_fd)
                continue
            if chunk:
                self.buffer.extend(chunk)
            else:
                self.eof = True

    async def send(self, value: dict[str, object]) -> None:
        raw = json.dumps(value, separators=(",", ":"), allow_nan=False).encode() + b"\n"
        if len(raw) > MAX_LINE:
            raise IPCError("browser_output_limit")
        async with self._lock:
            remaining = memoryview(raw)
            while remaining:
                try:
                    written = os.write(self.output_fd, remaining)
                except BlockingIOError:
                    await self._ready(self.output_fd, write=True)
                    continue
                if written <= 0:
                    raise IPCError("browser_output_closed")
                remaining = remaining[written:]

    def close(self) -> None:
        # fd ownership stays with the entrypoint. No outstanding readiness
        # callbacks survive a cancelled receive/send coroutine.
        os.set_blocking(self.input_fd, self._input_blocking)
        os.set_blocking(self.output_fd, self._output_blocking)
