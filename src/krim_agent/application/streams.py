"""有界读侧多路复用；断开收束订阅，不停止生产或重放执行。"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncGenerator, Awaitable
from dataclasses import dataclass

from anyio import CancelScope

from krim_agent.protocol.domain import RunView, ServiceError
from krim_agent.runtime.chat import ChatRuntime
from krim_agent.streams import MultiplexRequest, RunCursor, RunStreamFrame


@dataclass(frozen=True)
class SourceEnd:
    run_id: str


@dataclass(frozen=True)
class SourceFailure:
    error: Exception


class MultiplexReader:
    def __init__(self, runtime: ChatRuntime, *, queue_size: int = 64) -> None:
        if not 1 <= queue_size <= 256:
            raise ValueError("多路订阅队列必须在 1–256")
        self.runtime, self.queue_size = runtime, queue_size

    @staticmethod
    async def _read[T](operation: Awaitable[T]) -> T:
        """取消只结束读取循环，已开始的短事务必须先结束并释放连接。"""
        task = asyncio.ensure_future(operation)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            with CancelScope(shield=True):
                await asyncio.shield(task)
            raise

    async def prepare(self, request: MultiplexRequest) -> list[tuple[RunCursor, RunView]]:
        prepared = []
        for source in request.sources:
            run = await self.runtime.repository.get_run(source.run_id)
            if run.topic_id != source.topic_id:
                raise ServiceError("stream_source_mismatch", "Run 不属于订阅话题", 409)
            summary = await self.runtime.repository.event_summary(source.run_id)
            if source.next_seq > summary.count:
                raise ServiceError("invalid_cursor", "流游标超出已有事件", 416)
            if summary.count == 0 and run.status not in {"queued", "running"}:
                raise ServiceError("stream_gone", "该运行没有持久事件", 410)
            prepared.append((source.model_copy(deep=True), run))
        return prepared

    async def subscribe(
        self, prepared: list[tuple[RunCursor, RunView]], *, heartbeat: float = 15
    ) -> AsyncGenerator[RunStreamFrame | None, None]:
        queue: asyncio.Queue[RunStreamFrame | SourceEnd | SourceFailure] = asyncio.Queue(
            self.queue_size
        )

        async def read(source: RunCursor, _run: RunView) -> None:
            try:
                cursor = source.next_seq
                terminal_since: float | None = None
                while True:
                    # 每个来源最多预读一帧，背压不能堆积一整份历史日志。
                    frames = await self._read(
                        self.runtime.repository.event_page(source.run_id, cursor, limit=1)
                    )
                    for frame in frames:
                        if frame.seq != cursor:
                            raise ServiceError("event_gap", "持久事件有游标缺口", 409)
                        await queue.put(RunStreamFrame(topic_id=source.topic_id, frame=frame))
                        cursor += 1
                        if frame.kind in {"end", "error"}:
                            await queue.put(SourceEnd(source.run_id))
                            return
                    if frames:
                        continue
                    summary = await self._read(self.runtime.repository.event_summary(source.run_id))
                    if summary.terminal and cursor == summary.count:
                        break
                    run = await self._read(self.runtime.repository.get_run(source.run_id))
                    if run.status not in {"queued", "running"}:
                        terminal_since = terminal_since or time.monotonic()
                        if time.monotonic() - terminal_since >= 5:
                            raise ServiceError("stream_incomplete", "终态运行缺少持久终帧", 409)
                    await asyncio.sleep(0.05)
                await queue.put(SourceEnd(source.run_id))
            except asyncio.CancelledError:
                raise
            except Exception as error:
                await queue.put(SourceFailure(error))

        tasks = [
            asyncio.create_task(read(source, state), name=f"krim-subscribe-{source.run_id}")
            for source, state in prepared
        ]
        remaining = len(tasks)
        try:
            while remaining:
                try:
                    item = await asyncio.wait_for(queue.get(), heartbeat)
                except TimeoutError:
                    yield None
                    continue
                if isinstance(item, SourceEnd):
                    remaining -= 1
                elif isinstance(item, SourceFailure):
                    raise item.error
                else:
                    yield item
        finally:
            for task in tasks:
                task.cancel()
            # ASGI 的取消作用域会重复取消 await；必须允许数据库回滚/关闭完成。
            with CancelScope(shield=True):
                await asyncio.gather(*tasks, return_exceptions=True)
