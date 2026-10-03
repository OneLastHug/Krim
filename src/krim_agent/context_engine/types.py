"""上下文压缩的纯 Python 合约；不依赖数据库、网络或模型。"""

from typing import Literal

from pydantic import Field

from krim_agent.protocol.domain import MessageView
from krim_agent.protocol.models import StrictModel

ContextStrategy = Literal[
    "truncate", "sliding_window", "summarize_then_truncate", "annotated_compact"
]
ContextPolicy = Literal["never", "always", "auto"]


class ContextOptions(StrictModel):
    strategy: ContextStrategy = "sliding_window"
    policy: ContextPolicy = "auto"
    max_messages: int | None = Field(default=None, ge=2, le=10000)
    max_chars: int | None = Field(default=None, ge=128, le=10_000_000)
    max_tokens: int | None = Field(default=None, ge=32, le=2_500_000)


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
