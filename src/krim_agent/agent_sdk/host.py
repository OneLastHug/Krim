"""The narrow host seams required by the SDK loop, without a second scheduler."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

from pydantic import JsonValue

from krim_agent.development.contracts import OperationIdentity, OperationReceipt
from krim_agent.runtime.contracts import ExecutionBudget
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
