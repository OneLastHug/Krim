"""运行事实写口；不依赖 SDK、ORM 或具体工具后端。"""

import asyncio
from collections.abc import Awaitable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Protocol

from pydantic import JsonValue

from krim_agent.config import Settings
from krim_agent.context.types import SessionContext, WorkspaceSessionContext
from krim_agent.contracts import RunEvent, RunRequest, RunStatus, RunView
from krim_agent.workspace.types import WorkFrame


@dataclass(frozen=True)
class RunSnapshot:
    run: RunView
    request: RunRequest
    target: dict[str, JsonValue]
    events: tuple[RunEvent, ...]
    session_context: SessionContext | WorkspaceSessionContext | None = None
    work: WorkFrame | None = None


@dataclass(frozen=True)
class RunEventPage:
    events: tuple[RunEvent, ...]
    event_count: int
    status: RunStatus


class RunStore(Protocol):
    healthy: bool

    async def begin(
        self, request: RunRequest, target: dict[str, JsonValue]
    ) -> tuple[RunView, bool]: ...
    async def turn_request(self, turn_id: str) -> tuple[RunRequest, str]: ...
    async def begin_turn_run(
        self, turn_id: str, request: RunRequest, target: dict[str, JsonValue], settings: Settings
    ) -> tuple[RunView, bool]: ...
    async def append(
        self, run_id: str, kind: str, data: dict[str, JsonValue], *, event_id: str | None = None
    ) -> RunEvent: ...
    async def finish(self, run_id: str, status: RunStatus, code: str | None = None) -> RunView: ...
    async def set_running(self, run_id: str) -> None: ...
    async def set_awaiting(self, run_id: str) -> bool: ...  # False when stop won the transition
    async def write_checkpoint(
        self, run_id: str, checkpoint: dict[str, Any], fingerprint_value: str
    ) -> None: ...  # 写停靠恢复点
    async def read_checkpoint(self, run_id: str) -> tuple[dict[str, Any], str] | None: ...
    # 读停靠恢复点（内容, 指纹）；没有返回 None

    async def request_stop(self, run_id: str) -> None: ...
    async def snapshot(self, run_id: str) -> RunSnapshot: ...
    async def event_page(
        self, run_id: str, from_seq: int, limit: int
    ) -> RunEventPage: ...
    async def by_key(self, key: str) -> RunView | None: ...
    async def child_request(
        self, eval_id: str, case_id: str, repetition: int
    ) -> tuple[RunRequest, str]: ...


class ExecutionBudget:
    """当前run或工具的可暂停执行时间；仅持有单轮运行期状态。"""

    def __init__(self, deadline: float) -> None:
        self._loop = asyncio.get_running_loop()
        self._deadline = deadline
        self._timeout: asyncio.Timeout | None = None
        self._remaining: float | None = None
        self._pause_count = 0
        self._finished = False

    @property
    def deadline(self) -> float:
        if self._pause_count and self._remaining is not None:
            return self._loop.time() + self._remaining
        return self._deadline

    @property
    def remaining(self) -> float:
        if self._pause_count and self._remaining is not None:
            return self._remaining
        return max(0.0, self._deadline - self._loop.time())

    @property
    def paused(self) -> bool:
        return self._pause_count > 0

    def bind(self, timeout: asyncio.Timeout) -> None:
        if self._timeout is not None:
            raise RuntimeError("execution budget already has a timeout")
        self._timeout = timeout
        deadline = timeout.when()
        if deadline is not None:
            self._deadline = deadline
        if self._pause_count:
            timeout.reschedule(None)

    @contextmanager
    def pause(self) -> Iterator[None]:
        timeout = self._timeout
        counted = False
        if not self._finished and timeout is not None:
            if self._pause_count:
                self._pause_count += 1
                counted = True
            else:
                remaining = self._deadline - self._loop.time()
                if remaining > 0:
                    self._remaining = remaining
                    self._pause_count = 1
                    timeout.reschedule(None)
                    counted = True
        try:
            yield
        finally:
            if counted:
                self._pause_count -= 1
                if self._pause_count == 0:
                    remaining = self._remaining
                    self._remaining = None
                    if not self._finished and remaining is not None:
                        self._deadline = self._loop.time() + remaining
                        if not timeout.expired():
                            timeout.reschedule(self._deadline)

    def finish(self) -> None:
        self._finished = True


async def settled[T](operation: Awaitable[T]) -> T:
    """调用者反复取消也先等待原提交/清理完成，然后传播取消。"""
    task = asyncio.ensure_future(operation)
    cancelled = False
    while True:
        try:
            value = await asyncio.shield(task)
            break
        except asyncio.CancelledError:
            if task.cancelled():
                raise
            cancelled = True
    if cancelled:
        raise asyncio.CancelledError
    return value


class WorkRuntime(Protocol):
    """后段借用边界；本阶段无第二producer或业务装配。"""

    async def prepare(self, run_id: str) -> WorkFrame: ...

    async def close(self) -> None: ...
