"""运行事实写口；不依赖 SDK、ORM 或具体工具后端。"""

import asyncio
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Protocol

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
    async def request_stop(self, run_id: str) -> None: ...
    async def snapshot(self, run_id: str) -> RunSnapshot: ...
    async def by_key(self, key: str) -> RunView | None: ...
    async def child_request(
        self, eval_id: str, case_id: str, repetition: int
    ) -> tuple[RunRequest, str]: ...


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
