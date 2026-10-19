"""不可变上下文来源；完整历史与工作输入分别保存。"""

from typing import Literal

from pydantic import Field

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
