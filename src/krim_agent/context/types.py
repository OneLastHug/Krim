"""不可变上下文来源；完整历史与工作输入分别保存。"""

from typing import Literal

from pydantic import Field, JsonValue

from krim_agent.contracts import Message, StrictModel


class ContextPolicy(StrictModel):
    max_bytes: int = Field(default=131072, ge=256, le=1048576)
    max_messages: int = Field(default=256, ge=2, le=1024)
    reserved_output_tokens: int = Field(default=2048, ge=1, le=32768)
    counter_version: Literal["approx-utf8/1"] = "approx-utf8/1"


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
    """D3消费的冻结类型；D1只定义数据，不发摘要请求。"""

    version: Literal["compaction/1"] = "compaction/1"
    trigger_ratio: float = Field(default=0.8, gt=0, le=0.8)
    keep_turns: int = Field(default=2, ge=2, le=8)
    summary_bytes: int = Field(default=2048, ge=1, le=2048)
    max_passes: int = Field(default=2, ge=1, le=2)
    counter_version: Literal["approx-utf8/1"] = "approx-utf8/1"


class PreparedContext(StrictModel):
    schema_version: Literal[2] = 2
    messages: tuple[Message, ...]
    sources: tuple[HistorySource, ...]
    policy: CompactionPolicy
    source_fingerprint: str
    summary_hash: str | None
    input_bytes: int = Field(ge=0, le=1048576)
    estimated_tokens: int = Field(ge=0)
    counter_version: Literal["approx-utf8/1"] = "approx-utf8/1"
