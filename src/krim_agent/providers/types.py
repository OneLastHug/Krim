"""模型事件与工具调用，不绑定商业供应商 SDK。"""

from collections.abc import AsyncIterator, Sequence
from typing import Literal, Protocol
from uuid import uuid4

from pydantic import Field, JsonValue

from krim_agent.protocol.domain import MessageView
from krim_agent.protocol.models import StrictModel
from krim_agent.tools.core import ToolDefinition
from krim_agent.usage import TokenUsage


class ModelText(StrictModel):
    kind: Literal["text"] = "text"
    text: str = Field(max_length=8192)


class ModelToolCall(StrictModel):
    kind: Literal["tool_call"] = "tool_call"
    id: str = Field(default_factory=lambda: str(uuid4()), max_length=128)
    name: str = Field(min_length=1, max_length=64)
    arguments: dict[str, JsonValue]


class ModelUsage(StrictModel):
    kind: Literal["usage"] = "usage"
    usage: TokenUsage
    final: bool = True


class ModelProvider(Protocol):
    name: str

    def stream(
        self, messages: Sequence[MessageView], tools: Sequence[ToolDefinition]
    ) -> AsyncIterator[ModelText | ModelToolCall | ModelUsage]: ...
