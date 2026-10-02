"""工具定义与 Python 适配；参考 SRC-DOC-001、SRC-OSS-003。"""

from __future__ import annotations

import inspect
import re
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, get_type_hints
from uuid import uuid4

from jsonschema import Draft202012Validator
from pydantic import BaseModel, ConfigDict, JsonValue, create_model

from krim_agent.protocol.models import StrictModel

if TYPE_CHECKING:
    from .registry import ToolRegistry

Risk = Literal["read", "write"]
Decision = Literal["allow", "deny", "ask", "audit"]
Policy = Callable[["ToolDefinition", "ToolContext"], Awaitable[Decision]]
Executor = Callable[[BaseModel, "ToolContext"], Awaitable[Any]]


class ToolError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class ShortCircuit:
    value: Any


@dataclass
class ToolContext:
    registry: ToolRegistry
    run_id: str = "local"
    topic_id: str = "local"
    depth: int = 0
    timeout: float = 10
    policy: Policy | None = None
    call_id: str = field(default_factory=lambda: str(uuid4()))
    arguments: dict[str, Any] = field(default_factory=dict)

    def child(self, arguments: dict[str, Any]) -> ToolContext:
        return ToolContext(
            registry=self.registry,
            run_id=self.run_id,
            topic_id=self.topic_id,
            depth=self.depth + 1,
            timeout=self.timeout,
            policy=self.policy,
            call_id=self.call_id if self.depth == 0 else str(uuid4()),
            arguments=arguments,
        )

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> Any:
        return await self.registry.call_tool(name, arguments, self)


PreHook = Callable[[ToolContext], Awaitable[ShortCircuit | None]]
PostHook = Callable[[Any, ToolContext], Awaitable[Any]]


@dataclass(frozen=True)
class ToolDefinition:
    name: str
    description: str
    parameters: type[BaseModel]
    executor: Executor
    risk: Risk = "write"
    pre_execute: PreHook | None = None
    post_execute: PostHook | None = None

    def __post_init__(self) -> None:
        if re.fullmatch(r"[a-z][a-z0-9_]{0,63}", self.name) is None:
            raise ValueError("工具名称必须是小写字母开头的 snake_case，最多 64 字符")
        if self.risk not in {"read", "write"}:
            raise ValueError("不支持的风险等级")
        Draft202012Validator.check_schema(self.parameters.model_json_schema())

    def schema(self) -> ToolInfo:
        return ToolInfo(
            name=self.name,
            description=self.description,
            risk=self.risk,
            input_schema=self.parameters.model_json_schema(),
        )


class ToolBase(ABC):
    name: str
    description: str
    parameters: type[BaseModel]
    risk: Risk = "write"

    @abstractmethod
    async def execute(self, arguments: BaseModel, context: ToolContext) -> Any:
        raise NotImplementedError

    def definition(self) -> ToolDefinition:
        return ToolDefinition(self.name, self.description, self.parameters, self.execute, self.risk)


def tool(
    *, name: str | None = None, description: str = "", risk: Risk = "write"
) -> Callable[[Callable[..., Awaitable[Any]]], ToolDefinition]:
    def decorate(function: Callable[..., Awaitable[Any]]) -> ToolDefinition:
        if not inspect.iscoroutinefunction(function):
            raise TypeError("M1 工具必须是 async 函数，不能在事件循环中执行同步阻塞代码")
        hints = get_type_hints(function, include_extras=True)
        fields: dict[str, Any] = {}
        needs_context = False
        for parameter in inspect.signature(function).parameters.values():
            if parameter.kind in {parameter.VAR_POSITIONAL, parameter.VAR_KEYWORD}:
                raise TypeError("工具不支持可变参数")
            if parameter.kind is parameter.POSITIONAL_ONLY:
                raise TypeError("工具不支持仅位置参数")
            if parameter.name == "context":
                if hints.get("context") is not ToolContext:
                    raise TypeError("context 必须声明为 ToolContext")
                needs_context = True
                continue
            if parameter.name not in hints:
                raise TypeError(f"缺少参数类型: {parameter.name}")
            default = ... if parameter.default is parameter.empty else parameter.default
            fields[parameter.name] = (hints[parameter.name], default)
        parameters = create_model(
            f"{function.__name__}Input",
            __config__=ConfigDict(
                extra="forbid", strict=True, allow_inf_nan=False, validate_default=True
            ),
            **fields,
        )

        async def executor(arguments: BaseModel, context: ToolContext) -> Any:
            values = {key: getattr(arguments, key) for key in fields}
            if needs_context:
                values["context"] = context
            return await function(**values)

        return ToolDefinition(
            name or function.__name__,
            description or inspect.getdoc(function) or function.__name__,
            parameters,
            executor,
            risk,
        )

    return decorate


class TextContent(StrictModel):
    type: Literal["text"] = "text"
    text: str


class ToolInfo(StrictModel):
    name: str
    description: str
    risk: Risk
    input_schema: dict[str, JsonValue]


class ToolResult(StrictModel):
    content: list[TextContent]
    is_error: bool = False
    data: JsonValue = None
    code: str | None = None


class ToolAudit(StrictModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    call_id: str
    run_id: str
    name: str
    risk: Risk
    decision: Decision
    depth: int
    ok: bool
    code: str | None = None


class ToolObservation(StrictModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    call_id: str
    run_id: str
    name: str
    result: ToolResult
