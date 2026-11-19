"""The narrow host seams required by the SDK loop, without a second scheduler."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, cast

from ai import util as sdk_util
from pydantic import JsonValue

from krim_agent.development.contracts import OperationIdentity, OperationReceipt
from krim_agent.runtime.contracts import ExecutionBudget, settled
from krim_agent.tools.executor import ToolContext

CURRENT_OPERATION: ContextVar[OperationIdentity | None] = ContextVar(
    "development_operation", default=None,
)


@dataclass(frozen=True)
class HostExecution:
    """RunService owns state transitions; SDK consumes this already-claimed run."""

    context: ToolContext
    budget: ExecutionBudget
    read_receipt: Callable[[OperationReceipt], Awaitable[JsonValue]]
    model_step: Callable[[], int]
    checkpoint_state: Callable[[], dict[str, Any]]
    restore_state: Callable[[dict[str, Any]], None]


class _SDKLifetime:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.users = 0
        self.owned: asyncio.Task[None] | None = None


@asynccontextmanager
async def sdk_lifetime() -> AsyncIterator[None]:
    """Join ai 0.9.0's loop sentinel after our last native run is closed.

    The pinned SDK keeps a forever ``closing-dummy`` for decouple's shutdown
    detection. Borrow a pre-existing sentinel without touching it. Otherwise
    capture the exact task we create, share its lease across concurrent host
    runs, and join it only after all those SDK streams have closed. The lock and
    counter live on the loop itself, not in a global map retaining closed loops.
    """
    loop = asyncio.get_running_loop()
    state = getattr(loop, "_krim_ai_lifetime", None)
    if state is None:
        state = _SDKLifetime()
        loop.__dict__["_krim_ai_lifetime"] = state
    async with state.lock:
        if not state.users:
            existing = sdk_util._LOOP_CLOSING_MAP.get(loop)
            if existing is None:
                sdk_util.get_loop_closing_checker()
                state.owned = sdk_util._LOOP_CLOSING_MAP.get(loop)
        state.users += 1
    try:
        yield
    finally:
        async with state.lock:
            state.users -= 1
            if not state.users and state.owned is not None:
                task = cast(asyncio.Task[None], state.owned)
                state.owned = None
                if sdk_util._LOOP_CLOSING_MAP.get(loop) is task:
                    task.cancel()
                    await settled(asyncio.gather(task, return_exceptions=True))
