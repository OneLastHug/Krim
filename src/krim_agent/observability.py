"""默认关闭的元数据追踪；不接受正文、秘密或异常文本。"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import math
import os
import secrets
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol

from pydantic import ConfigDict, Field, JsonValue, field_validator

from krim_agent.protocol.models import StrictModel

if TYPE_CHECKING:
    from krim_agent.persistence.traces import TraceStore

SpanKind = Literal["run", "model", "tool", "workflow", "node", "host"]
SpanStatus = Literal["running", "ok", "error", "cancelled", "interrupted"]
OwnerKind = Literal["run", "workflow", "host"]


def owner_ref(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def safe_attributes(values: Mapping[str, object]) -> dict[str, JsonValue]:
    """只接纳固定计数与枚举；未知键和值均不透传。"""
    counters = {
        "step",
        "attempt",
        "depth",
        "epoch",
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "node_count",
        "redacted_fields",
    }
    flags = {"ok", "recovered", "completed"}
    enums = {
        "encoding": {"approx-v1", "cl100k_base", "o200k_base", "custom"},
        "status": {"queued", "running", "completed", "failed", "aborted", "paused", "interrupted"},
    }
    result: dict[str, JsonValue] = {}
    redacted = 0
    for key, value in values.items():
        if key in counters and type(value) is int and 0 <= value <= 9007199254740991:
            result[key] = value
        elif key in flags and type(value) is bool:
            result[key] = value
        elif key in enums and isinstance(value, str) and value in enums[key]:
            result[key] = value
        else:
            redacted += 1
    if redacted:
        previous = result.get("redacted_fields")
        result["redacted_fields"] = (previous if type(previous) is int else 0) + redacted
    return result


class TraceContext(StrictModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    trace_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    span_id: str = Field(pattern=r"^[0-9a-f]{16}$")
    sampled: bool = True


class SpanRecord(StrictModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    trace_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    span_id: str = Field(pattern=r"^[0-9a-f]{16}$")
    parent_span_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{16}$")
    owner_kind: OwnerKind
    owner_ref: str = Field(pattern=r"^[0-9a-f]{64}$")
    kind: SpanKind
    start_ns: int = Field(ge=0)
    end_ns: int | None = Field(default=None, ge=0)
    status: SpanStatus = "running"
    attributes: dict[str, JsonValue] = Field(default_factory=dict)

    @field_validator("attributes", mode="before")
    @classmethod
    def filter_attributes(cls, value: object) -> dict[str, JsonValue]:
        return safe_attributes(value) if isinstance(value, dict) else {}


class TraceView(StrictModel):
    trace_id: str
    spans: list[SpanRecord] = Field(default_factory=list)
    complete: bool = False


class ObservationStatus(StrictModel):
    enabled: bool
    queued: int = Field(ge=0)
    dropped: int = Field(ge=0)
    store_failures: int = Field(ge=0)
    export_failures: int = Field(ge=0)


class SpanSink(Protocol):
    async def export(self, record: SpanRecord) -> None: ...


class JsonlSink:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    async def export(self, record: SpanRecord) -> None:
        record = SpanRecord.model_validate(record.model_dump())

        # 仅结束事实导出；正文从未进入 record。文件权限不扩大到全局配置。
        def append() -> None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(
                self.path,
                os.O_CREAT | os.O_APPEND | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            try:
                data = (record.model_dump_json() + "\n").encode()
                while data:
                    written = os.write(fd, data)
                    if written <= 0:
                        raise OSError("观测写入没有进展")
                    data = data[written:]
            finally:
                os.close(fd)

        await asyncio.to_thread(append)


@dataclass
class SpanHandle:
    context: TraceContext | None
    record: SpanRecord | None
    status: SpanStatus = "ok"


@dataclass(frozen=True)
class ActiveSpan:
    tracer: Tracer
    context: TraceContext


_active: ContextVar[ActiveSpan | None] = ContextVar("krim_trace", default=None)


class Tracer:
    def __init__(
        self,
        store: TraceStore,
        *,
        enabled: bool = False,
        sample_rate: float = 1,
        queue_size: int = 512,
        timeout_seconds: float = 0.25,
        sink: SpanSink | None = None,
    ) -> None:
        if not math.isfinite(sample_rate) or not 0 <= sample_rate <= 1:
            raise ValueError("采样率必须在 0–1")
        if not 1 <= queue_size <= 4096 or not 0 < timeout_seconds <= 10:
            raise ValueError("观测容量/超时超出范围")
        self.store, self.enabled, self.sample_rate = store, enabled, sample_rate
        self.timeout_seconds, self.sink = timeout_seconds, sink
        self.queue: asyncio.Queue[SpanRecord] = asyncio.Queue(queue_size)
        self.task: asyncio.Task[None] | None = None
        self.closed = False
        self.dropped = self.store_failures = self.export_failures = 0
        self.inflight = False

    def status(self) -> ObservationStatus:
        return ObservationStatus(
            enabled=self.enabled,
            queued=self.queue.qsize() + int(self.inflight),
            dropped=self.dropped,
            store_failures=self.store_failures,
            export_failures=self.export_failures,
        )

    def _enqueue(self, record: SpanRecord) -> None:
        if self.closed:
            self.dropped += 1
            return
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self._drain(), name="krim-observation-writer")
        try:
            self.queue.put_nowait(record)
        except asyncio.QueueFull:
            self.dropped += 1

    @contextmanager
    def scope(
        self,
        kind: SpanKind,
        owner_kind: OwnerKind,
        owner_id: str,
        attributes: Mapping[str, object] | None = None,
    ) -> Iterator[SpanHandle]:
        if not self.enabled or self.closed:
            yield SpanHandle(None, None)
            return
        parent = _active.get()
        trace_id = parent.context.trace_id if parent else secrets.token_hex(16)
        sampled = (
            parent.context.sampled
            if parent
            else int(trace_id, 16) < int(self.sample_rate * (1 << 128))
        )
        context = TraceContext(trace_id=trace_id, span_id=secrets.token_hex(8), sampled=sampled)
        record = (
            SpanRecord(
                trace_id=trace_id,
                span_id=context.span_id,
                parent_span_id=parent.context.span_id if parent else None,
                owner_kind=owner_kind,
                owner_ref=owner_ref(owner_id),
                kind=kind,
                start_ns=time.time_ns(),
                attributes=safe_attributes(attributes or {}),
            )
            if sampled
            else None
        )
        handle = SpanHandle(context, record)
        token = _active.set(ActiveSpan(self, context))
        if record:
            self._enqueue(record)
        try:
            yield handle
        except asyncio.CancelledError:
            handle.status = "cancelled"
            raise
        except BaseException:
            handle.status = "error"
            raise
        finally:
            _active.reset(token)
            if record:
                self._enqueue(
                    record.model_copy(
                        update={
                            "end_ns": max(time.time_ns(), record.start_ns),
                            "status": handle.status,
                        },
                        deep=True,
                    )
                )

    async def _drain(self) -> None:
        while True:
            record = await self.queue.get()
            self.inflight = True
            try:
                try:
                    async with asyncio.timeout(self.timeout_seconds):
                        await self.store.write(record)
                except Exception:
                    self.store_failures += 1
                if record.end_ns is not None and self.sink is not None:
                    try:
                        async with asyncio.timeout(self.timeout_seconds):
                            await self.sink.export(SpanRecord.model_validate(record.model_dump()))
                    except Exception:
                        self.export_failures += 1
            except asyncio.CancelledError:
                self.dropped += 1
                raise
            finally:
                self.inflight = False
                self.queue.task_done()

    async def flush(self, seconds: float = 2) -> bool:
        try:
            async with asyncio.timeout(seconds):
                await self.queue.join()
            return True
        except TimeoutError:
            return False

    async def close(self) -> None:
        if self.closed:
            return
        await self.flush()
        self.closed = True
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        close = getattr(self.sink, "close", None)
        if close is not None:
            try:
                async with asyncio.timeout(self.timeout_seconds):
                    result = (
                        close()
                        if inspect.iscoroutinefunction(close)
                        else await asyncio.to_thread(close)
                    )
                    if inspect.isawaitable(result):
                        await result
            except Exception:
                self.export_failures += 1
        while not self.queue.empty():
            self.queue.get_nowait()
            self.queue.task_done()
            self.dropped += 1


@contextmanager
def scope(
    kind: SpanKind,
    owner_kind: OwnerKind,
    owner_id: str,
    attributes: Mapping[str, object] | None = None,
    tracer: Tracer | None = None,
) -> Iterator[SpanHandle]:
    current = _active.get()
    selected = tracer or (current.tracer if current else None)
    if selected is None:
        yield SpanHandle(None, None)
    else:
        with selected.scope(kind, owner_kind, owner_id, attributes) as span:
            yield span
