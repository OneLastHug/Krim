"""Provider 只产模型事件，不持有工具实现或执行器。"""

from collections.abc import AsyncIterator, Sequence
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


class ModelProvider(Protocol):
    def stream(
        self, messages: Sequence[Message], tools: list[dict[str, JsonValue]]
    ) -> AsyncIterator[ModelEvent]: ...
    async def close(self) -> None: ...
