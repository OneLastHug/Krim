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
    """单次请求的剩余执行预算；不进入持久会话历史。

    ``max_tool_calls_this_step`` 为 None 表示本步不裁剪工具批次（调度宽度
    上限未配置）：provider与执行层必须放行模型返回的完整批次，不得把
    None折叠成1或0。
    """

    remaining_steps: int | None
    remaining_tool_calls: int | None
    max_tool_calls_this_step: int | None

    def __post_init__(self) -> None:
        if any(value is not None and type(value) is not int
               for value in (self.remaining_steps, self.remaining_tool_calls)):
            raise TypeError("model run budgets must be integers or None")
        if self.max_tool_calls_this_step is not None and type(
                self.max_tool_calls_this_step) is not int:
            raise TypeError("per-request tool budget must be an integer or None")
        if (
            self.remaining_steps is not None and self.remaining_steps < 1
            or self.remaining_tool_calls is not None and self.remaining_tool_calls < 0
            or self.max_tool_calls_this_step is not None and self.max_tool_calls_this_step < 0
            or self.remaining_tool_calls is not None
            and self.max_tool_calls_this_step is not None
            and self.max_tool_calls_this_step > self.remaining_tool_calls
        ):
            raise ValueError("model request budget is out of range")


class ModelProvider(Protocol):
    def stream(
        self, messages: Sequence[Message], tools: list[dict[str, JsonValue]]
    ) -> AsyncIterator[ModelEvent]: ...
    async def close(self) -> None: ...
