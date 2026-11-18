"""不可变上下文来源；完整历史与工作输入分别保存。"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, JsonValue, model_validator

from krim_agent.context.counter import (
    COMPACTION_TRIGGER_TOKENS,
    CONTEXT_WINDOW_TOKENS,
    TOKEN_WINDOW_VERSION,
)
from krim_agent.contracts import Message, StrictModel

# token窗口代的默认输出预留；升级时替换旧演示值，不是供应商输出能力上限。
DEFAULT_RESERVED_OUTPUT_TOKENS = 2048

CounterVersion = Literal["approx-utf8/1", "token-window/1"]


class ContextPolicy(StrictModel):
    """模型请求输入预算。

    ``counter_version="approx-utf8/1"`` 是旧冻结策略的兼容标记：按
    ``max_bytes`` 与 ``trigger_ratio`` 执行字节预算，已存储策略原样保留。
    ``counter_version="token-window/1"`` 是当前生成：按token计数执行
    ``max_tokens`` 窗口与 ``compaction_trigger_tokens`` 自动压缩触发点
    （计划第三节固定1,000,000/900,000）。``max_bytes`` 是可选字节预算：
    默认None表示不设应用层字节封顶，显式数值（旧冻结策略或宿主显式配置）
    仍执行。计数来源与精度见 ``context.counter``：默认分段估计器未做实测
    校准，``UsageCalibratedCounter`` 为待接入的响应usage校正。
    """

    # None=不设应用层字节预算（默认；真实输入规模由token窗口判定，供应商
    # 真实限制按其响应暴露）。显式数值（旧冻结策略或宿主显式配置）仍执行，
    # 作为字节合理界限而不是token窗口，也不承担任何"新默认cap"。
    max_bytes: int | None = Field(default=None, ge=256)
    max_messages: int | None = Field(default=None, ge=2)
    reserved_output_tokens: int = Field(default=DEFAULT_RESERVED_OUTPUT_TOKENS, ge=1)
    max_tokens: int = Field(default=CONTEXT_WINDOW_TOKENS, ge=1)
    compaction_trigger_tokens: int = Field(default=COMPACTION_TRIGGER_TOKENS, ge=1)
    counter_version: CounterVersion = "token-window/1"

    @model_validator(mode="after")
    def window_order(self) -> ContextPolicy:
        if self.counter_version == TOKEN_WINDOW_VERSION:
            # 触发点必须早于窗口扣留本次输出空间后耗尽，否则压缩永远不会
            # 在配额拒绝前发生。
            if self.compaction_trigger_tokens + self.reserved_output_tokens > self.max_tokens:
                raise ValueError("压缩触发点超过窗口可用输入空间")
        return self

    def counts_tokens(self) -> bool:
        """窗口判定是否按token计数（token窗口代）；旧代只走字节预算。"""
        return self.counter_version == TOKEN_WINDOW_VERSION


class HistorySource(StrictModel):
    turn_id: str
    run_id: str | None
    last_seq: int
    fingerprint: str


class ContextSnapshot(StrictModel):
    schema_version: int = 1
    messages: tuple[Message, ...]
    sources: tuple[HistorySource, ...]
    extensions: dict[str, JsonValue] = Field(default_factory=dict)
    policy: ContextPolicy
    tool_fingerprint: str
    builder_fingerprint: str
    input_bytes: int
    estimated_tokens: int
    source_fingerprint: str


class SessionContext(StrictModel):
    workspace_id: str
    session_id: str
    turn_id: str
    context: ContextSnapshot


class WorkspaceSessionContext(StrictModel):
    """schema2准备引用；不把完整历史复制进旧ContextSnapshot。"""

    schema_version: Literal[2] = 2
    workspace_id: str
    session_id: str
    turn_id: str
    effective_fingerprint: str
    source_fingerprint: str


class CompactionPolicy(StrictModel):
    """D3消费的冻结类型；D1只定义数据，不发摘要请求。

    触发判定随 ``ContextPolicy.counter_version`` 走：token窗口代按
    ``ContextPolicy.compaction_trigger_tokens``，旧代按字节 ``trigger_ratio``。
    本模型的 ``counter_version`` 仅作记录性标记随冻结策略保存。
    """

    version: Literal["compaction/1"] = "compaction/1"
    trigger_ratio: float = Field(default=0.8, gt=0, le=1)
    keep_turns: int = Field(default=2, ge=0)
    # None selects the real request/output capacity, not a product quota. Old
    # frozen policies retain their explicit values and checkpoint semantics.
    summary_bytes: int | None = Field(default=None, ge=1)
    max_passes: int | None = Field(default=None, ge=1)
    counter_version: CounterVersion = "approx-utf8/1"


def upgrade_to_token_window(policy: ContextPolicy) -> ContextPolicy:
    """旧approx-utf8/1策略升级到1M/900k token窗口（计划3.3：旧会话下一轮用新策略）。

    旧代随附的演示性限制一并解除：``max_bytes`` 字节封顶清空（计划3.1/4.2
    取消演示小封顶，不引入任何新默认字节cap），``max_messages`` 条数上限
    清空，``reserved_output_tokens`` 回到当前默认2048而不继承旧演示值；
    token窗口/触发点取计划固定值。已是token窗口代则原样返回。宿主在读回
    已存储策略准备冻结下一轮上下文时调用。
    """
    if policy.counter_version == TOKEN_WINDOW_VERSION:
        return policy
    return policy.model_copy(
        update={
            "max_bytes": None,
            "max_messages": None,
            "reserved_output_tokens": DEFAULT_RESERVED_OUTPUT_TOKENS,
            "max_tokens": CONTEXT_WINDOW_TOKENS,
            "compaction_trigger_tokens": COMPACTION_TRIGGER_TOKENS,
            "counter_version": TOKEN_WINDOW_VERSION,
        }
    )


class PreparedContext(StrictModel):
    schema_version: Literal[2] = 2
    messages: tuple[Message, ...]
    sources: tuple[HistorySource, ...]
    policy: CompactionPolicy
    source_fingerprint: str
    summary_hash: str | None
    input_bytes: int = Field(ge=0)
    estimated_tokens: int = Field(ge=0)
    counter_version: CounterVersion = "approx-utf8/1"


class DevelopmentContextFrozen(StrictModel):
    """V3 development history anchor; prior messages stay behind its source refs."""

    schema_version: Literal[3] = 3
    owner_id: str
    workspace_id: str
    project_id: str
    session_id: str
    turn_id: str
    run_id: str
    authorization_generation: int = Field(ge=1)
    sources: tuple[HistorySource, ...]
    current: Message
    system_prompt: str
    extensions: dict[str, JsonValue] = Field(default_factory=dict)
    policy: ContextPolicy
    compaction: CompactionPolicy
    tool_fingerprint: str
    projection_version: str
    source_fingerprint: str


class DevelopmentContextState(StrictModel):
    """Frozen V3 anchor and bounded prepared model input."""

    schema_version: Literal[3] = 3
    frozen: DevelopmentContextFrozen
    prepared: PreparedContext | None = None
    prepared_hash: str | None = None


class OrdinaryContextFrozen(StrictModel):
    """V4 ordinary-session anchor; full transcripts remain in turns."""

    schema_version: Literal[4] = 4
    owner_id: str
    workspace_id: str
    session_id: str
    turn_id: str
    run_id: str
    target_key: str
    # None is reserved for a host-authenticated static/legacy principal.
    authorization_generation: int | None = Field(default=None, ge=1)
    sources: tuple[HistorySource, ...]
    current: Message
    system_prompt: str
    extensions: dict[str, JsonValue] = Field(default_factory=dict)
    policy: ContextPolicy
    compaction: CompactionPolicy
    tool_fingerprint: str
    projection_version: str
    source_fingerprint: str


class OrdinaryContextState(StrictModel):
    schema_version: Literal[4] = 4
    frozen: OrdinaryContextFrozen
    prepared: PreparedContext | None = None
    prepared_hash: str | None = None


class OrdinarySessionContext(StrictModel):
    schema_version: Literal[4] = 4
    owner_id: str
    workspace_id: str
    session_id: str
    turn_id: str
    frozen: OrdinaryContextFrozen
    prepared: PreparedContext | None = None


class DevelopmentSessionContext(StrictModel):
    """Runtime view of a target-development V3 context, never a WorkFrame."""

    schema_version: Literal[3] = 3
    owner_id: str
    workspace_id: str
    project_id: str
    session_id: str
    turn_id: str
    authorization_generation: int = Field(ge=1)
    frozen: DevelopmentContextFrozen
    prepared: PreparedContext | None = None
