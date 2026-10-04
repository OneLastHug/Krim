"""工具定义与 Python 适配；参考 SRC-DOC-001、SRC-OSS-003。"""

from __future__ import annotations

import asyncio
import inspect
import re
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, get_type_hints
from uuid import uuid4

from jsonschema import Draft202012Validator
from pydantic import BaseModel, ConfigDict, JsonValue, create_model

from krim_agent.protocol.models import StrictModel

from .output import OutputContract, OutputSchema, WireOutputSchema

if TYPE_CHECKING:
    from .registry import ToolRegistry

Risk = Literal["read", "write"]
Decision = Literal["allow", "deny", "ask", "audit"]
Policy = Callable[["ToolDefinition", "ToolContext"], Awaitable[Decision]]
Executor = Callable[[BaseModel, "ToolContext"], Awaitable[Any]]
ApprovalHandler = Callable[["ToolDefinition", "ToolContext"], Awaitable[bool]]
QuestionHandler = Callable[[str, list[str], "ToolContext"], Awaitable[str]]
InteractionPublisher = Callable[[dict[str, JsonValue]], Awaitable[None]]


class ToolError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class ShortCircuit:
    value: Any


@dataclass
class ToolExecutionBudget:
    deadline: asyncio.Timeout
    active: bool = True
    waiters: int = 0
    remaining: float | None = None

    def pause(self) -> bool:
        if not self.active or self.deadline.expired():
            return False
        if self.waiters == 0:
            when = self.deadline.when()
            if when is None:
                return False
            self.remaining = max(0.0, when - asyncio.get_running_loop().time())
            self.deadline.reschedule(None)
        self.waiters += 1
        return True

    def resume(self) -> None:
        self.waiters -= 1
        if self.waiters == 0 and self.active and not self.deadline.expired():
            assert self.remaining is not None
            self.deadline.reschedule(asyncio.get_running_loop().time() + self.remaining)
            self.remaining = None


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
    agent_id: str | None = None
    approval_handler: ApprovalHandler | None = None
    question_handler: QuestionHandler | None = None
    interaction_publisher: InteractionPublisher | None = None
    execution_timeouts: tuple[ToolExecutionBudget, ...] = ()
    # 来源由宿主/模型运行时设置，不能从工具参数伪造。
    origin: Literal["model", "host"] = "model"
    model_tools: Mapping[str, ToolDefinition] | None = None

    def child(self, arguments: dict[str, Any]) -> ToolContext:
        return ToolContext(
            registry=self.registry,
            run_id=self.run_id,
            topic_id=self.topic_id,
            depth=self.depth + 1,
            timeout=self.timeout,
            policy=self.policy,
            call_id=self.call_id if self.depth == 0 else str(uuid4()),
            agent_id=self.agent_id,
            approval_handler=self.approval_handler,
            question_handler=self.question_handler,
            interaction_publisher=self.interaction_publisher,
            execution_timeouts=self.execution_timeouts,
            origin=self.origin,
            model_tools=self.model_tools,
            arguments=arguments,
        )

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> Any:
        return await self.registry.call_tool(name, arguments, self)

    @asynccontextmanager
    async def human_wait(self) -> AsyncIterator[None]:
        """只暂停借用的工具 deadline；运行/Agent 总预算和交互独立预算仍生效。"""
        budgets = [budget for budget in self.execution_timeouts if budget.pause()]
        try:
            yield
        finally:
            for budget in reversed(budgets):
                budget.resume()


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
    output_schema: OutputSchema | None = None
    should_defer: bool = False
    always_load: bool = False
    _output_contract: OutputContract | None = field(
        default=None, init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        if re.fullmatch(r"[a-z][a-z0-9_]{0,63}", self.name) is None:
            raise ValueError("工具名称必须是小写字母开头的 snake_case，最多 64 字符")
        if self.risk not in {"read", "write"}:
            raise ValueError("不支持的风险等级")
        if type(self.should_defer) is not bool or type(self.always_load) is not bool:
            raise ValueError("工具加载配置必须为布尔值")
        Draft202012Validator.check_schema(self.parameters.model_json_schema())
        if self.output_schema is not None:
            object.__setattr__(self, "_output_contract", OutputContract(self.output_schema))
            if not isinstance(self.output_schema, type):
                assert self._output_contract is not None
                object.__setattr__(self, "output_schema", self._output_contract.schema())

    def validate_output(self, value: JsonValue) -> None:
        if self._output_contract is not None:
            self._output_contract.validate(value)

    def schema(self) -> ToolInfo:
        return ToolInfo(
            name=self.name,
            description=self.description,
            risk=self.risk,
            input_schema=self.parameters.model_json_schema(),
            output_schema=self._output_contract.schema() if self._output_contract else None,
            should_defer=self.should_defer,
            always_load=self.always_load,
        )


class ToolBase(ABC):
    name: str
    description: str
    parameters: type[BaseModel]
    risk: Risk = "write"
    output_schema: OutputSchema | None = None
    should_defer: bool = False
    always_load: bool = False

    @abstractmethod
    async def execute(self, arguments: BaseModel, context: ToolContext) -> Any:
        raise NotImplementedError

    def definition(self) -> ToolDefinition:
        return ToolDefinition(
            self.name,
            self.description,
            self.parameters,
            self.execute,
            self.risk,
            output_schema=self.output_schema,
            should_defer=self.should_defer,
            always_load=self.always_load,
        )


def tool(
    *,
    name: str | None = None,
    description: str = "",
    risk: Risk = "write",
    output_schema: OutputSchema | None = None,
    should_defer: bool = False,
    always_load: bool = False,
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
            output_schema=output_schema,
            should_defer=should_defer,
            always_load=always_load,
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
    output_schema: WireOutputSchema | None = None
    should_defer: bool = False
    always_load: bool = False


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
