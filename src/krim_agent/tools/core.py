"""不可变工具定义与 Hook 快照；没有旧工作区/命令 Backend。"""

from __future__ import annotations

import inspect
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel, JsonValue

from krim_agent.contracts import ToolObservation, fingerprint

from .output import OutputContract, WireOutputSchema

type Decision = Literal["allow", "deny", "ask"]


@dataclass(frozen=True)
class Invocation:
    run_id: str
    call_id: str
    span_id: str
    step_index: int
    name: str
    arguments_json: str
    tool_hash: str
    provider_call_id: str | None = None


type Policy = Callable[[Invocation], Awaitable[Decision]]
type PreHook = Callable[[Invocation], Awaitable[str | None]]
type PostHook = Callable[[Invocation, ToolObservation], Awaitable[None]]
type Executor = Callable[[BaseModel], Awaitable[JsonValue]]


@dataclass(frozen=True)
class ToolSpec:
    name: str
    parameters: type[BaseModel]
    executor: Executor
    output_schema: WireOutputSchema
    version: str
    implementation_digest: str
    risk: str = "read"
    scoped_executor: Callable[[Invocation, BaseModel, object], Awaitable[JsonValue]] | None = None
    capability: str | None = None

    def __post_init__(self) -> None:
        if (
            (self.risk != "read" and self.scoped_executor is None)
            or re.fullmatch(r"[a-z][a-z0-9_]{0,63}", self.name) is None
            or not inspect.iscoroutinefunction(self.executor)
        ):
            raise ValueError("仅接受显式可信异步只读工具")
        if self.scoped_executor is not None and (
            not inspect.iscoroutinefunction(self.scoped_executor)
            or self.capability is None
            or self.capability != {
                "workspace_read": "read", "workspace_search": "search",
                "workspace_write": "write", "command_run": "command",
                "artifact_read": "artifact", "plan_update": "plan",
            }.get(self.name)
            or self.risk != {"write": "write", "command": "exec",
            "plan": "write"}.get(self.capability, "read")
        ):
            raise ValueError("scoped工具须登记精确受控路由和风险")
        if self.scoped_executor is None and self.capability is not None:
            raise ValueError("legacy工具不能登记scoped能力")
        if re.fullmatch(r"[0-9a-f]{64}", self.implementation_digest) is None or not self.version:
            raise ValueError("工具须有宿主确认的实现摘要/版本")
        OutputContract(self.output_schema)

    def descriptor(self) -> dict[str, JsonValue]:
        value: dict[str, JsonValue] = {
            "name": self.name,
            "version": self.version,
            "implementation_digest": self.implementation_digest,
            "input_schema": self.parameters.model_json_schema(),
            "output_schema": OutputContract(self.output_schema).schema(),
        }
        if self.scoped_executor is not None:
            value.update(risk=self.risk, capability=self.capability, policy_version="workspace/1")
        return value

    @property
    def fingerprint(self) -> str:
        return fingerprint(self.descriptor())
