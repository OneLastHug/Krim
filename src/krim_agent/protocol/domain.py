"""HTTP、仓储和运行时共享的类型化数据；M1 EXT 合约。"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator

from krim_agent.locale import LocaleChange, LocaleTag
from krim_agent.prompt_context import MentionOccurrence
from krim_agent.prompt_nodes import PromptNode
from krim_agent.skills import SkillName

from .models import StrictModel

RunStatus = Literal["queued", "running", "completed", "failed", "aborted", "interrupted"]
TERMINAL_STATUSES = {"completed", "failed", "aborted", "interrupted"}


class ServiceError(Exception):
    def __init__(self, code: str, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.status = status


class TopicCreate(LocaleChange):
    title: str = Field(default="新话题", min_length=1, max_length=120)
    locale: str = Field(default="zh-CN", min_length=2, max_length=64)


class TextRequest(StrictModel):
    text: str = Field(min_length=1, max_length=8192)
    idempotency_key: str = Field(min_length=1, max_length=128)

    @field_validator("text")
    @classmethod
    def nonempty(cls, text: str) -> str:
        if not text.strip():
            raise ValueError("消息不能只有空白")
        return text


class ChatRequest(TextRequest):
    parent_message_id: str | None = Field(default=None, min_length=1, max_length=128)
    mentions: list[MentionOccurrence] = Field(default_factory=list, max_length=32)
    skill_names: list[SkillName] = Field(default_factory=list, max_length=8)
    nodes: list[PromptNode] = Field(default_factory=list, max_length=128)


class EditRequest(TextRequest):
    mentions: list[MentionOccurrence] = Field(default_factory=list, max_length=32)
    skill_names: list[SkillName] = Field(default_factory=list, max_length=8)
    nodes: list[PromptNode] = Field(default_factory=list, max_length=128)


class RegenerateRequest(StrictModel):
    idempotency_key: str = Field(min_length=1, max_length=128)


class BranchRequest(StrictModel):
    message_id: str = Field(min_length=1, max_length=128)


class View(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra="forbid")

    @field_validator("created_at", "updated_at", "finished_at", check_fields=False)
    @classmethod
    def normalize_time(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value


class TopicView(View):
    id: str
    title: str
    created_at: datetime
    updated_at: datetime
    active_branches: dict[str, str | None] = Field(default_factory=dict)
    locale: LocaleTag = "zh-CN"
    agent_session_id: str | None = None


class MessageView(View):
    id: str
    topic_id: str
    run_id: str
    sequence: int
    role: Literal["system", "user", "assistant", "tool"]
    content: str
    details: dict[str, JsonValue]
    created_at: datetime
    parent_id: str | None = None


class MessageTreeView(StrictModel):
    topic: TopicView
    messages: list[MessageView]
    active_path: list[str]


class StreamEventView(StrictModel):
    run_id: str
    first_seq: int
    last_seq: int
    count: int
    durable: bool = True
    terminal: bool = False


class RunView(View):
    id: str
    topic_id: str
    input_message_id: str
    idempotency_key: str
    status: RunStatus
    stop_requested: bool
    error: str | None
    created_at: datetime
    updated_at: datetime
    finished_at: datetime | None
    action: Literal["send", "edit", "regenerate"] = "send"
    source_message_id: str | None = None
    error_code: str | None = None
