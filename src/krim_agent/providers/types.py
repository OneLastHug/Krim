"""Provider 只产模型事件，不持有工具实现或执行器。"""

from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from typing import Literal, Protocol

from pydantic import Field, JsonValue, model_validator

from krim_agent.contracts import Message, StrictModel, check_json
from krim_agent.usage import TokenUsage


class ModelText(StrictModel):
    kind: Literal["text"] = "text"
    text: str


class ModelToolCall(StrictModel):
    kind: Literal["tool_call"] = "tool_call"
    id: str = Field(min_length=1, max_length=128)
    name: str = Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")
    arguments: dict[str, JsonValue]

    @model_validator(mode="after")
    def finite(self) -> "ModelToolCall":
        check_json(self.arguments)
        return self


class ModelUsage(StrictModel):
    kind: Literal["usage"] = "usage"
    usage: TokenUsage
    final: bool = False


type ModelEvent = ModelText | ModelToolCall | ModelUsage


@dataclass(frozen=True, slots=True)
class ModelRequestBudget:
    """单次请求的剩余执行预算；不进入持久会话历史。"""

    remaining_steps: int
    remaining_tool_calls: int
    max_tool_calls_this_step: int

    def __post_init__(self) -> None:
        values = (self.remaining_steps, self.remaining_tool_calls, self.max_tool_calls_this_step)
        if any(type(value) is not int for value in values):
            raise TypeError("model request budgets must be integers")
        if (
            self.remaining_steps < 1
            or self.remaining_tool_calls < 0
            or not 0 <= self.max_tool_calls_this_step <= self.remaining_tool_calls
        ):
            raise ValueError("model request budget is out of range")


class ModelProvider(Protocol):
    def stream(
        self, messages: Sequence[Message], tools: list[dict[str, JsonValue]]
    ) -> AsyncIterator[ModelEvent]: ...
    async def close(self) -> None: ...
