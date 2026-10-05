"""上下文压缩的纯 Python 合约；不依赖数据库、网络或模型。"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from pydantic import ConfigDict, Field, JsonValue, model_validator

from krim_agent.protocol.domain import MessageView
from krim_agent.protocol.models import StrictModel
from krim_agent.tokenizer import TokenizerName

if TYPE_CHECKING:
    from krim_agent.config import Settings

CONTEXT_SETTINGS = {
    "strategy": "context_strategy",
    "policy": "context_policy",
    "max_messages": "context_max_messages",
    "max_chars": "context_max_chars",
    "max_tokens": "context_max_tokens",
    "reserved_output_tokens": "context_reserved_output_tokens",
    "tokenizer": "context_tokenizer",
    "model_context_window_tokens": "model_context_window_tokens",
    "model_framing_reserve_tokens": "model_framing_reserve_tokens",
}

ContextStrategy = Literal[
    "truncate", "sliding_window", "summarize_then_truncate", "annotated_compact"
]
ContextPolicy = Literal["never", "always", "auto"]
ContextCompactStatus = Literal["proposed", "completed", "unchanged"]


class ContextOptions(StrictModel):
    strategy: ContextStrategy = "sliding_window"
    policy: ContextPolicy = "auto"
    max_messages: int | None = Field(default=None, ge=2, le=10000)
    max_chars: int | None = Field(default=None, ge=128, le=10_000_000)
    max_tokens: int | None = Field(default=None, ge=1, le=2_500_000)
    reserved_output_tokens: int = Field(default=0, ge=0, le=1_000_000)


class ContextRunConfig(ContextOptions):
    """每次运行的公开不可变快照；不包含 Provider/Key/可调用对象。"""

    model_config = ConfigDict(extra="forbid", frozen=True)
    max_tokens: int | None = Field(default=None, ge=32, le=2_500_000)
    tokenizer: TokenizerName = "approx-v1"
    model_context_window_tokens: int | None = Field(default=None, ge=32, le=2_500_000)
    model_framing_reserve_tokens: int = Field(default=64, ge=0, le=100_000)

    @model_validator(mode="after")
    def validate_window(self) -> ContextRunConfig:
        if self.model_context_window_tokens is not None:
            if self.tokenizer == "approx-v1" or self.reserved_output_tokens <= 0:
                raise ValueError("硬模型窗口需要显式 BPE tokenizer 和正数输出预留")
            if (
                self.reserved_output_tokens + self.model_framing_reserve_tokens
                >= self.model_context_window_tokens
            ):
                raise ValueError("输出/封装预留必须小于模型窗口")
        return self

    @classmethod
    def from_settings(cls, settings: Settings) -> ContextRunConfig:
        return cls.model_validate(
            {name: getattr(settings, setting) for name, setting in CONTEXT_SETTINGS.items()}
        )

    def settings_values(self) -> dict[str, JsonValue]:
        values = self.model_dump(mode="json")
        return {setting: values[name] for name, setting in CONTEXT_SETTINGS.items()}

    def restricted_by(self, parent: ContextRunConfig) -> ContextRunConfig:
        child_bounded = self.max_tokens is not None or self.model_context_window_tokens is not None
        parent_bounded = (
            parent.max_tokens is not None or parent.model_context_window_tokens is not None
        )
        if child_bounded and parent_bounded and self.tokenizer != parent.tokenizer:
            raise ValueError("父子词元预算必须使用相同 tokenizer")
        values = self.model_dump()
        for name in ("max_messages", "max_chars", "max_tokens", "model_context_window_tokens"):
            limits = [
                value for value in (getattr(self, name), getattr(parent, name)) if value is not None
            ]
            values[name] = min(limits) if limits else None
        if parent_bounded and not child_bounded:
            values["tokenizer"] = parent.tokenizer
        values["reserved_output_tokens"] = max(
            self.reserved_output_tokens, parent.reserved_output_tokens
        )
        values["model_framing_reserve_tokens"] = max(
            self.model_framing_reserve_tokens, parent.model_framing_reserve_tokens
        )
        return self.model_validate(values)


class ContextCompactRequest(StrictModel):
    """HTTP/client 手动压缩请求；预览默认不产生持久副作用。"""

    strategy: ContextStrategy = "annotated_compact"
    policy: ContextPolicy = "auto"
    max_messages: int | None = Field(default=None, ge=2, le=10000)
    max_chars: int | None = Field(default=None, ge=128, le=10_000_000)
    max_tokens: int | None = Field(default=None, ge=1, le=2_500_000)
    reserved_output_tokens: int = Field(default=0, ge=0, le=1_000_000)
    confirm: bool = False
    expected_branch_key: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def require_summary_strategy(self) -> ContextCompactRequest:
        if self.strategy not in {"summarize_then_truncate", "annotated_compact"}:
            raise ValueError("手动 compact 需要 summarize_then_truncate 或 annotated_compact")
        return self


class ContextResult(StrictModel):
    messages: list[MessageView]
    summary: str | None = None
    compacted: bool = False
    original_count: int = Field(ge=0)
    omitted_count: int = Field(ge=0)
    estimated_chars: int = Field(ge=0)
    estimated_tokens: int = Field(ge=0)
    tokenizer: Literal["approx-v1", "cl100k_base", "o200k_base", "custom"] = "approx-v1"
    reserved_output_tokens: int = Field(default=0, ge=0)
    strategy: ContextStrategy
    policy: ContextPolicy
    processor_trace: list[str] = Field(default_factory=list)
    source_message_ids: list[str] = Field(default_factory=list)


class ContextCompactResponse(StrictModel):
    """手动压缩的可审计结果；messages 是本次模型输入预览。"""

    topic_id: str
    status: ContextCompactStatus
    branch_key: str
    source_branch_key: str | None = None
    source_until_message_id: str | None = None
    source_message_ids: list[str] = Field(default_factory=list)
    summary: str | None = None
    strategy: ContextStrategy
    policy: ContextPolicy
    compacted: bool = False
    original_count: int = Field(ge=0)
    omitted_count: int = Field(ge=0)
    estimated_chars: int = Field(ge=0)
    estimated_tokens: int = Field(ge=0)
    tokenizer: Literal["approx-v1", "cl100k_base", "o200k_base", "custom"] = "approx-v1"
    reserved_output_tokens: int = Field(default=0, ge=0)
    processor_trace: list[str] = Field(default_factory=list)
    messages: list[MessageView] = Field(default_factory=list)
    reason: str | None = None
    persisted: bool = False
    options: ContextOptions
