"""Python SDK 部件与逐步结果；语义参考 Vercel AI SDK（SRC-OSS-016）。"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, JsonValue, model_validator

from krim_agent.budgets import BudgetRequestView
from krim_agent.context_engine import ContextProposalNotice
from krim_agent.interactions import InteractionView
from krim_agent.observability import TraceContext
from krim_agent.protocol.domain import MessageView, RunView
from krim_agent.protocol.models import StrictModel
from krim_agent.providers import ModelToolCall
from krim_agent.tools.core import ToolAudit, ToolResult
from krim_agent.usage import TokenUsage, UsageTotals

FinishReason = Literal["stop", "tool-calls", "length", "abort", "error", "interrupted"]


class StreamStart(StrictModel):
    type: Literal["start"] = "start"
    provider: str
    context_config: dict[str, JsonValue] | None = None
    trace_context: TraceContext | None = None


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


class UsagePart(StrictModel):
    type: Literal["usage"] = "usage"
    step: int = Field(ge=0)
    usage: TokenUsage
    final: bool = True


class UsageBudgetPart(StrictModel):
    type: Literal["usage-budget"] = "usage-budget"
    request: BudgetRequestView


class ModelStepPart(StrictModel):
    type: Literal["model-step"] = "model-step"
    step: int = Field(ge=0, le=31)
    phase: Literal["start", "end"]
    provider: str | None = None
    completed: bool | None = None

    @model_validator(mode="after")
    def validate_phase(self) -> ModelStepPart:
        if self.phase == "start" and (not self.provider or self.completed is not None):
            raise ValueError("模型开始事件需要 provider 且不能已完成")
        if self.phase == "end" and self.completed is None:
            raise ValueError("模型结束事件需要 completed")
        return self


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
    tokenizer: Literal["approx-v1", "cl100k_base", "o200k_base", "custom"] = "approx-v1"
    estimated_tokens: int | None = Field(default=None, ge=0)
    reserved_output_tokens: int = Field(default=0, ge=0)


class ContextProposalPart(StrictModel):
    type: Literal["context-proposal"] = "context-proposal"
    phase: Literal["pending", "resolved"]
    proposal: ContextProposalNotice


class ContextNoticePart(StrictModel):
    type: Literal["context-notice"] = "context-notice"
    step: int = Field(ge=0)
    message_count: int = Field(ge=0)
    threshold: int = Field(ge=2)


class ContextBudgetPart(StrictModel):
    type: Literal["context-budget"] = "context-budget"
    step: int = Field(ge=0)
    encoding: Literal["cl100k_base", "o200k_base"]
    input_tokens: int = Field(ge=0)
    tool_tokens: int = Field(ge=0)
    reserved_output_tokens: int = Field(gt=0)
    framing_reserve_tokens: int = Field(ge=0)
    window_tokens: int = Field(gt=0)
    required_tokens: int = Field(ge=0)
    within_limit: bool

    @model_validator(mode="after")
    def validate_total(self) -> ContextBudgetPart:
        total = (
            self.input_tokens
            + self.tool_tokens
            + self.reserved_output_tokens
            + self.framing_reserve_tokens
        )
        if self.required_tokens != total or self.within_limit != (total <= self.window_tokens):
            raise ValueError("上下文预算事件的总量/窗口状态不一致")
        return self


class StepFinish(StrictModel):
    type: Literal["step-finish"] = "step-finish"
    step: int = Field(ge=0)
    finish_reason: Literal["stop", "tool-calls"]


class InteractionPart(StrictModel):
    type: Literal["interaction-request"] = "interaction-request"
    interaction: InteractionView


class ToolDiscoveryPart(StrictModel):
    type: Literal["tool-discovery"] = "tool-discovery"
    tools: list[str]
    step: int = Field(ge=0)
    call_id: str


class SkillLoadedPart(StrictModel):
    type: Literal["skill-loaded"] = "skill-loaded"
    name: str
    resource: str | None = None
    step: int = Field(ge=0)
    call_id: str


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
    | UsagePart
    | UsageBudgetPart
    | ModelStepPart
    | ContextCompactPart
    | ContextProposalPart
    | ContextNoticePart
    | ContextBudgetPart
    | InteractionPart
    | ToolDiscoveryPart
    | SkillLoadedPart
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
    token_usage: TokenUsage | None = None
    usage_complete: bool = False


class GenerateResult(StrictModel):
    text: str
    run: RunView
    finish_reason: FinishReason
    steps: list[StepResult]
    response_messages: list[MessageView]
    audits: list[ToolAudit]
    error: str | None = None
    code: str | None = None
    token_usage: UsageTotals | None = None
    usage_complete: bool = False

    @property
    def all_text(self) -> str:
        return "".join(step.text for step in self.steps)
