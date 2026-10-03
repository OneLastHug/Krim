"""宿主注入的执行器声明；图配置不能授权代码或副作用重试。"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from pydantic import JsonValue

from .types import NodeOutput, WorkflowNode


@dataclass(frozen=True)
class ExecutionContext:
    run_id: str
    node_id: str
    attempt: int
    path: str = ""


Runner = Callable[
    [WorkflowNode, dict[str, JsonValue], ExecutionContext],
    NodeOutput | JsonValue | Awaitable[NodeOutput | JsonValue],
]


@dataclass(frozen=True)
class RunnerBinding:
    execute: Runner
    retry_safe: bool = False
    allow_code: bool = False
