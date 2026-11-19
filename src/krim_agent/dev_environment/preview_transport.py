"""Bounded framed exec bridge. E receives only a managed socketpair endpoint."""
from __future__ import annotations

import asyncio
import socket
import struct
import time
import uuid
from collections.abc import Callable, Coroutine
from typing import Any

from .docker import DockerCLI
from .preview_control import PreviewLimits
from .preview_relay import RELAY_CODE

_HEADER = struct.Struct("!BI")
_COUNT = struct.Struct("!I")
_WINDOW = 262144
_MAX_FRAME = 65536
_DATA, _FIN, _PING, _CLOSE, _CREDIT = 1, 2, 3, 4, 5
_READY, _REMOTE_DATA, _EOF, _FAILED, _REMOTE_CREDIT, _DONE = 10, 11, 12, 13, 14, 15


class RelayNotListening(Exception):
    pass


class RelayUnavailable(Exception):
    pass


class ExecRelay:
    """Transport state belongs to B. No arbitrary host/address/argv input."""

    def __init__(self, execution_id: str, port: int, limits: PreviewLimits,
                 ended: Callable[[ExecRelay], None]) -> None:
        self.execution_id = execution_id
        self.port = port
        self.limits = limits
        self.token = uuid.uuid4().hex
        self.pid: int | None = None
        self.process: asyncio.subprocess.Process | None = None
        self._spawn: asyncio.Task[asyncio.subprocess.Process] | None = None
        self.endpoint: socket.socket | None = None
        self.peer: socket.socket | None = None
        self._ended = ended
        self._write_lock = asyncio.Lock()
        self._credit = _WINDOW
        self._credit_event = asyncio.Event()
        self._credit_event.set()
        self._incoming = bytearray()
        self._incoming_event = asyncio.Event()
        self._uncredited = 0
        self._remote_eof = False
        self._ready: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        self._tasks: set[asyncio.Task[None]] = set()
        self._closing = False
        self._close_task: asyncio.Task[None] | None = None
        self._notify = False
        self._delivered = False
        self._failure_type: str | None = None
        self._stderr_hint = b""
        self._last_business = time.monotonic()
        self._born = self._last_business

    def _task(self, coroutine: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        task = asyncio.create_task(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._finished)
        return task

    def _finished(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            self._failure_type = type(task.exception()).__name__
            self.invalidate()

    async def start(self) -> None:
        args = (*DockerCLI.prefix, "exec", "--interactive", "--detach-keys=",
                "--user=65532:65532",
                "--workdir=/", self.execution_id, "/usr/bin/python3", "-I", "-c", RELAY_CODE,
                str(self.port), str(self.limits.connect_seconds),
                str(self.limits.relay_silence_seconds), str(self.limits.max_age_seconds),
                str(self.limits.idle_seconds), self.token)
        self._spawn = asyncio.create_task(asyncio.create_subprocess_exec(
            *args, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE, limit=_WINDOW,
            env={"PATH": "/usr/bin:/bin", "HOME": "/nonexistent", "LC_ALL": "C"},
        ))
        try:
            async with asyncio.timeout(self.limits.connect_seconds):
                self.process = await asyncio.shield(self._spawn)
                if self._closing:
                    raise RelayUnavailable
                self._task(self._read_remote())
                self._task(self._stderr())
                self._task(self._heartbeat())
                self._task(self._lifetime())
                result = await asyncio.shield(self._ready)
                if result is False:
                    raise RelayNotListening
                if self._closing:
                    raise RelayUnavailable
        except BaseException:
            self.invalidate()  # the reaper owns pending spawn until it actually settles
            raise

    async def _send(self, kind: int, payload: bytes = b"") -> None:
        if self.process is None or self.process.stdin is None:
            raise RelayUnavailable
        async with self._write_lock:
            if self._closing and kind != _CLOSE:
                raise RelayUnavailable
            self.process.stdin.write(_HEADER.pack(kind, len(payload)) + payload)
            await self.process.stdin.drain()

    async def _read_remote(self) -> None:
        assert self.process is not None and self.process.stdout is not None
        stream = self.process.stdout
        ready = False
        try:
            while not self._closing:
                header = await stream.readexactly(_HEADER.size)
                kind, count = _HEADER.unpack(header)
                if count > _MAX_FRAME:
                    raise RelayUnavailable
                payload = await stream.readexactly(count)
                if not ready:
                    if kind == _FAILED and count == 0:
                        self._ready.set_result(False)
                        return
                    if kind != _READY or count != 4:
                        raise RelayUnavailable
                    self.pid = _COUNT.unpack(payload)[0]
                    if self.pid < 1:
                        raise RelayUnavailable
                    ready = True
                    self._ready.set_result(True)
                elif kind == _REMOTE_DATA and count and not self._remote_eof:
                    self._uncredited += count
                    if self._uncredited > _WINDOW:
                        raise RelayUnavailable
                    self._incoming.extend(payload)
                    self._incoming_event.set()
                    self._last_business = time.monotonic()
                elif kind == _REMOTE_CREDIT and count == 4:
                    credit = _COUNT.unpack(payload)[0]
                    if not credit or self._credit + credit > _WINDOW:
                        raise RelayUnavailable
                    self._credit += credit
                    self._credit_event.set()
                elif kind == _EOF and count == 0 and not self._remote_eof:
                    self._remote_eof = True
                    self._incoming_event.set()
                elif kind == _DONE and count == 0:
                    if not self._remote_eof:
                        raise RelayUnavailable
                    return  # _write_socket drains the queued final bytes.
                else:
                    raise RelayUnavailable
        except asyncio.IncompleteReadError:
            if not self._ready.done():
                self._ready.set_exception(RelayUnavailable())
            if not self._remote_eof:
                self.invalidate()
        except Exception:
            if not self._ready.done():
                self._ready.set_exception(RelayUnavailable())
            raise

    async def _stderr(self) -> None:
        assert self.process is not None and self.process.stderr is not None
        size = 0
        while data := await self.process.stderr.read(4096):
            size += len(data)
            self._stderr_hint = (self._stderr_hint + data)[:64]
            if size > 16384:
                raise RelayUnavailable
        # Raw stderr is never returned to E or the browser.

    async def _heartbeat(self) -> None:
        while not self._closing:
            await self._send(_PING)
            await asyncio.sleep(self.limits.heartbeat_seconds)

    async def _lifetime(self) -> None:
        while not self._closing:
            remaining = min(self._born + self.limits.max_age_seconds,
                            self._last_business + self.limits.idle_seconds) - time.monotonic()
            if remaining <= 0:
                self.invalidate()
                return
            await asyncio.sleep(min(remaining, 0.5))

    def attach_socket(self) -> socket.socket:
        if self._closing or self._delivered or not self._ready.done() or not self._ready.result():
            raise RelayUnavailable
        self.endpoint, self.peer = socket.socketpair()
        self.endpoint.setblocking(False)
        self.peer.setblocking(False)
        self.peer.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, _MAX_FRAME)
        self.peer.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, _MAX_FRAME)
        self._delivered = True
        self._task(self._read_socket())
        self._task(self._write_socket())
        return self.endpoint

    async def _read_socket(self) -> None:
        assert self.peer is not None
        loop = asyncio.get_running_loop()
        while not self._closing:
            while self._credit == 0:
                self._credit_event.clear()
                await self._credit_event.wait()
            data = await loop.sock_recv(self.peer, min(_MAX_FRAME, self._credit))
            if not data:
                await self._send(_FIN)
                return  # TCP write-half close; response reading remains live.
            self._credit -= len(data)
            self._last_business = time.monotonic()
            await self._send(_DATA, data)

    async def _write_socket(self) -> None:
        assert self.peer is not None
        loop = asyncio.get_running_loop()
        while not self._closing:
            if self._incoming:
                data = bytes(self._incoming[:_MAX_FRAME])
                del self._incoming[:len(data)]
                await loop.sock_sendall(self.peer, data)
                self._uncredited -= len(data)
                await self._send(_CREDIT, _COUNT.pack(len(data)))
                continue
            if self._remote_eof:
                try:
                    self.peer.shutdown(socket.SHUT_WR)
                except OSError:
                    pass
                return  # E may still send in the other direction.
            self._incoming_event.clear()
            await self._incoming_event.wait()

    def invalidate(self) -> None:
        """Synchronous socket shutdown; no project lock waits for remote draining."""
        if self._closing:
            return
        self._closing = True
        if self.peer is not None:
            try:
                self.peer.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        if not self._ready.done():
            self._ready.set_exception(RelayUnavailable())
        self._close_task = asyncio.create_task(self._reap())

    @staticmethod
    async def _discard(stream: asyncio.StreamReader) -> None:
        size = 0
        while data := await stream.read(65536):
            size += len(data)
            if size > _WINDOW + 16384:
                return

    async def _reap(self) -> None:
        try:
            if self.process is None and self._spawn is not None:
                try:
                    self.process = await asyncio.shield(self._spawn)
                except Exception:
                    pass
            process = self.process
            if process is not None:
                try:
                    async with asyncio.timeout(0.25):
                        await self._send(_CLOSE)
                except (TimeoutError, ConnectionError, OSError, RelayUnavailable):
                    pass
                if process.stdin is not None:
                    process.stdin.close()
                # A spawn cancelled before startup has no reader tasks yet.
                # Drain bounded relay output so local pipe EOF can be reaped.
                if not self._tasks:
                    if process.stdout is not None:
                        self._task(self._discard(process.stdout))
                    if process.stderr is not None:
                        self._task(self._discard(process.stderr))
                try:
                    async with asyncio.timeout(self.limits.relay_silence_seconds + 0.5):
                        await process.wait()
                except TimeoutError:
                    # Only a local attachment cleanup. Tests independently prove
                    # container relay's heartbeat/absolute-deadline exit bound.
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                    await process.wait()
        finally:
            for task in tuple(self._tasks):
                task.cancel()
            if self._tasks:
                await asyncio.gather(*tuple(self._tasks), return_exceptions=True)
            if self.peer is not None:
                self.peer.close()
            if self.endpoint is not None:
                self.endpoint.close()
            # Retrieve exceptions even if acquire/start was cancelled first.
            if self._ready.done() and not self._ready.cancelled():
                self._ready.exception()
            if not self._notify:
                self._notify = True
                self._ended(self)

    async def aclose(self) -> None:
        self.invalidate()
        if self._close_task is not None:
            try:
                # Caller cancellation stays prompt, but capacity remains charged
                # until _reap has settled a pending spawn and released its FDs.
                async with asyncio.timeout(self.limits.relay_silence_seconds + 1.0):
                    await asyncio.shield(self._close_task)
            except TimeoutError:
                return
            except asyncio.CancelledError:
                raise
