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

    def __post_init__(self) -> None:
        if (
            self.risk != "read"
            or re.fullmatch(r"[a-z][a-z0-9_]{0,63}", self.name) is None
            or not inspect.iscoroutinefunction(self.executor)
        ):
            raise ValueError("仅接受显式可信异步只读工具")
        if re.fullmatch(r"[0-9a-f]{64}", self.implementation_digest) is None or not self.version:
            raise ValueError("工具须有宿主确认的实现摘要/版本")
        OutputContract(self.output_schema)

    def descriptor(self) -> dict[str, JsonValue]:
        return {
            "name": self.name,
            "version": self.version,
            "implementation_digest": self.implementation_digest,
            "input_schema": self.parameters.model_json_schema(),
            "output_schema": OutputContract(self.output_schema).schema(),
        }

    @property
    def fingerprint(self) -> str:
        return fingerprint(self.descriptor())
