"""可信调用扩展的纯类型；权限内核仍只有ToolExecutor。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Protocol

from pydantic import BaseModel, JsonValue

from krim_agent.tools.core import Invocation
from krim_agent.workspace.types import ExecutionAccess, WorkFrame

type Middleware = Callable[[Invocation], Awaitable[str | None]]
type Next = Callable[[], Awaitable[JsonValue]]
type Wrap = Callable[[Invocation, Next], Awaitable[JsonValue]]


class Operations(Protocol):
    async def execute(self, invocation: Invocation, parameters: BaseModel,
        context: object) -> JsonValue: ...

    async def observe(self, invocation: Invocation) -> dict[str, JsonValue] | None: ...


@dataclass(frozen=True)
class Scope:
    frame: WorkFrame
    access: ExecutionAccess
    operations: Operations
    check: Callable[[], None]
    deadline: float
    invocation: Invocation | None = None


CURRENT: ContextVar[Scope | None] = ContextVar("krim-workspace-invocation", default=None)

# 仅当前调用链的阶段标记；不是持久状态、队列或可授予能力。
PHASE: ContextVar[str | None] = ContextVar("krim-workspace-phase", default=None)


@dataclass(eq=False)
class _Producer:
    service: object
    run_id: str
    task: asyncio.Task[None]
    active: bool = True


@dataclass(eq=False)
class _Call:
    producer: _Producer
    run_id: str
    call_id: str
    invocation: Invocation
    active: bool = True


_PRODUCER: ContextVar[_Producer | None] = ContextVar("krim-producer", default=None)
_CALL: ContextVar[_Call | None] = ContextVar("krim-tool-owner", default=None)


@contextmanager
def _producing(service: object, run_id: str, task: asyncio.Task[None]) -> Iterator[None]:
    """仅实际注册的 producer 进入；共享撤销也作用于复制上下文的子 Task。"""
    record = _Producer(service, run_id, task)
    token = _PRODUCER.set(record)
    try:
        yield
    finally:
        record.active = False
        _PRODUCER.reset(token)


@contextmanager
def _calling(invocation: Invocation) -> Iterator[None]:
    producer = _PRODUCER.get()
    record = (
        _Call(producer, invocation.run_id, invocation.call_id, invocation)
        if producer is not None and producer.active
        and not producer.task.done() and producer.run_id == invocation.run_id
        else None
    )
    token = _CALL.set(record)
    try:
        yield
    finally:
        if record is not None:
            record.active = False
        _CALL.reset(token)


def _owns(service: object, tasks: Mapping[str, asyncio.Task[None]]) -> bool:
    producer, call = _PRODUCER.get(), _CALL.get()
    return (
        producer is not None and call is not None
        and producer.service is service and call.producer is producer
        and producer.active and call.active
        and call.run_id == producer.run_id
        and call.invocation.run_id == call.run_id and call.invocation.call_id == call.call_id
        and tasks.get(producer.run_id) is producer.task
        and not producer.task.done()
    )
