"""文件编排调用链标记，避免回调取消/关闭时等待正在等待自己的生产者。"""

from __future__ import annotations

import asyncio
from contextvars import ContextVar, Token

WorkspaceTasks = dict[str, tuple[str, asyncio.Task[None]]]
_current: ContextVar[tuple[WorkspaceTasks, str] | None] = ContextVar(
    "krim_workspace_execution", default=None
)


def enter_workspace_execution(
    tasks: WorkspaceTasks, identifier: str
) -> Token[tuple[WorkspaceTasks, str] | None]:
    return _current.set((tasks, identifier))


def leave_workspace_execution(token: Token[tuple[WorkspaceTasks, str] | None]) -> None:
    _current.reset(token)


def inside_workspace_execution(tasks: WorkspaceTasks | None = None) -> bool:
    current = _current.get()
    if current is None:
        return False
    if tasks is not None and current[0] is not tasks:
        return False
    entry = current[0].get(current[1])
    # Hook创建的后台Task可继承标记；原生产者已结束时不再视为内部调用。
    return entry is not None and not entry[1].done()
