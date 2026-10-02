"""进程内四原语；超时和取消属于传输错误，不转换为业务失败。"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from uuid import uuid4

from pydantic import JsonValue

from .models import ChannelResult, StreamFrame, check_json

Handler = Callable[[JsonValue], Awaitable[JsonValue]]
StreamHandler = Callable[[JsonValue], AsyncGenerator[JsonValue, None]]


class InProcessBridge:
    def __init__(self, version: int = 2, max_pending: int = 128) -> None:
        if version != 2:
            raise ValueError("不支持的协议版本")
        self._peer: InProcessBridge | None = None
        self._handlers: dict[str, Handler] = {}
        self._streams: dict[str, StreamHandler] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._inflight: set[asyncio.Task[JsonValue]] = set()
        self._closed = False
        self._pending = 0
        self._max_pending = max_pending

    @classmethod
    def pair(cls) -> tuple[InProcessBridge, InProcessBridge]:
        host, app = cls(), cls()
        host._peer, app._peer = app, host
        return host, app

    @property
    def ready(self) -> bool:
        return not self._closed and self._peer is not None and not self._peer._closed

    def on(self, channel: str, handler: Handler) -> Callable[[], None]:
        if channel in self._handlers:
            raise ValueError(f"重复信道: {channel}")
        self._handlers[channel] = handler

        def unregister() -> None:
            if self._handlers.get(channel) is handler:
                del self._handlers[channel]

        return unregister

    def on_stream(self, channel: str, handler: StreamHandler) -> Callable[[], None]:
        if channel in self._streams:
            raise ValueError(f"重复流信道: {channel}")
        self._streams[channel] = handler

        def unregister() -> None:
            if self._streams.get(channel) is handler:
                del self._streams[channel]

        return unregister

    def _target(self, data: JsonValue) -> InProcessBridge:
        check_json(data)
        if not self.ready or self._peer is None:
            raise ConnectionError("桥接未就绪或已经关闭")
        if self._pending >= self._max_pending:
            raise ConnectionError("待处理请求超过上限")
        return self._peer

    async def emit(
        self, channel: str, data: JsonValue = None, *, timeout: float = 10
    ) -> ChannelResult:
        target = self._target(data)
        handler = target._handlers.get(channel)
        if handler is None:
            return ChannelResult(ok=False, error=f"未注册信道: {channel}")
        self._pending += 1

        async def invoke_handler() -> JsonValue:
            return await handler(data)

        task: asyncio.Task[JsonValue] = asyncio.create_task(invoke_handler())
        self._inflight.add(task)
        target._inflight.add(task)
        try:
            async with asyncio.timeout(timeout):
                try:
                    value = await task
                    return ChannelResult(ok=True, data=value)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    return ChannelResult(ok=False, error=f"信道处理失败: {channel}")
        finally:
            self._pending -= 1
            self._inflight.discard(task)
            target._inflight.discard(task)

    def notify(self, channel: str, data: JsonValue = None) -> None:
        self._target(data)
        if len(self._tasks) >= self._max_pending:
            raise ConnectionError("通知队列超过上限")

        async def deliver() -> None:
            try:
                await self.emit(channel, data)
            except (ConnectionError, TimeoutError):
                pass

        task = asyncio.create_task(deliver())
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def stream(
        self, channel: str, data: JsonValue = None, *, timeout: float = 10
    ) -> AsyncIterator[StreamFrame]:
        target = self._target(data)
        stream_id = str(uuid4())
        seq = 0
        yield StreamFrame(stream_id=stream_id, seq=seq, kind="start")
        handler = target._streams.get(channel)
        if handler is None:
            yield StreamFrame(
                stream_id=stream_id, seq=1, kind="error", data={"error": "未注册流信道"}
            )
            return
        iterator = handler(data)
        self._pending += 1
        try:
            while True:
                try:
                    async with asyncio.timeout(timeout):
                        if not self.ready:
                            raise ConnectionError("桥接已经关闭")
                        task = asyncio.create_task(anext(iterator))
                        self._inflight.add(task)
                        target._inflight.add(task)
                        try:
                            value = await task
                        finally:
                            self._inflight.discard(task)
                            target._inflight.discard(task)
                except StopAsyncIteration:
                    break
                check_json(value)
                seq += 1
                yield StreamFrame(stream_id=stream_id, seq=seq, kind="chunk", data=value)
        except (asyncio.CancelledError, ConnectionError, TimeoutError):
            raise
        except Exception:
            yield StreamFrame(
                stream_id=stream_id, seq=seq + 1, kind="error", data={"error": "流处理失败"}
            )
            return
        finally:
            self._pending -= 1
            await iterator.aclose()
        yield StreamFrame(stream_id=stream_id, seq=seq + 1, kind="end")

    async def close(self) -> None:
        self._closed = True
        tasks = [*self._tasks, *self._inflight]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._handlers.clear()
        self._streams.clear()
