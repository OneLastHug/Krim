"""Python SDK 部件与逐步结果；语义参考 Vercel AI SDK（SRC-OSS-016）。"""

from typing import Literal

from pydantic import Field, JsonValue

from krim_agent.protocol.domain import MessageView, RunView
from krim_agent.protocol.models import StrictModel
from krim_agent.providers import ModelToolCall
from krim_agent.tools.core import ToolAudit, ToolResult

FinishReason = Literal["stop", "tool-calls", "length", "abort", "error", "interrupted"]


class StreamStart(StrictModel):
    type: Literal["start"] = "start"
    provider: str


class TextDelta(StrictModel):
    type: Literal["text-delta"] = "text-delta"
    text: str


class ToolCallPart(StrictModel):
    type: Literal["tool-call"] = "tool-call"
    tool_call_id: str
    tool_name: str
    input: dict[str, JsonValue]


class ToolResultPart(StrictModel):
    type: Literal["tool-result"] = "tool-result"
    tool_call_id: str
    tool_name: str
    result: ToolResult


class ContextCompactPart(StrictModel):
    type: Literal["context-compact"] = "context-compact"
    original_count: int = Field(ge=0)
    kept_count: int = Field(ge=0)
    omitted_count: int = Field(ge=0)
    strategy: str
    summary: str | None = None
    status: Literal["completed"] = "completed"
    persisted: bool = False
    source_count: int = Field(default=0, ge=0)
    source_until_message_id: str | None = None
    source_branch_key: str | None = None


class StepFinish(StrictModel):
    type: Literal["step-finish"] = "step-finish"
    step: int = Field(ge=0)
    finish_reason: Literal["stop", "tool-calls"]


class StreamFinish(StrictModel):
    type: Literal["finish"] = "finish"
    finish_reason: FinishReason
    status: str
    error: str | None = None
    code: str | None = None


class StreamError(StrictModel):
    type: Literal["error"] = "error"
    code: str
    message: str


SDKPart = (
    StreamStart
    | TextDelta
    | ToolCallPart
    | ToolResultPart
    | ContextCompactPart
    | StepFinish
    | StreamFinish
    | StreamError
)


class StepResult(StrictModel):
    step: int = Field(ge=0)
    text: str
    tool_calls: list[ModelToolCall] = Field(default_factory=list)
    tool_results: list[ToolResult] = Field(default_factory=list)
    finish_reason: Literal["stop", "tool-calls", "error"]
    partial: bool = False


class GenerateResult(StrictModel):
    text: str
    run: RunView
    finish_reason: FinishReason
    steps: list[StepResult]
    response_messages: list[MessageView]
    audits: list[ToolAudit]
    error: str | None = None
    code: str | None = None
    token_usage: None = None

    @property
    def all_text(self) -> str:
        return "".join(step.text for step in self.steps)
