"""上下文压缩的纯 Python 合约；不依赖数据库、网络或模型。"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.domain import MessageView
from krim_agent.protocol.models import StrictModel

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
    max_tokens: int | None = Field(default=None, ge=32, le=2_500_000)


class ContextCompactRequest(StrictModel):
    """HTTP/client 手动压缩请求；预览默认不产生持久副作用。"""

    strategy: ContextStrategy = "annotated_compact"
    policy: ContextPolicy = "auto"
    max_messages: int | None = Field(default=None, ge=2, le=10000)
    max_chars: int | None = Field(default=None, ge=128, le=10_000_000)
    max_tokens: int | None = Field(default=None, ge=32, le=2_500_000)
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
    processor_trace: list[str] = Field(default_factory=list)
    messages: list[MessageView] = Field(default_factory=list)
    reason: str | None = None
    persisted: bool = False
    options: ContextOptions
