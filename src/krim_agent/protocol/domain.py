"""HTTP、仓储和运行时共享的类型化数据；M1 EXT 合约。"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator

from .models import StrictModel

RunStatus = Literal["queued", "running", "completed", "failed", "aborted", "interrupted"]
TERMINAL_STATUSES = {"completed", "failed", "aborted", "interrupted"}


class ServiceError(Exception):
    def __init__(self, code: str, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.status = status


class TopicCreate(StrictModel):
    title: str = Field(default="新话题", min_length=1, max_length=120)


class ChatRequest(StrictModel):
    text: str = Field(min_length=1, max_length=8192)
    idempotency_key: str = Field(min_length=1, max_length=128)

    @field_validator("text")
    @classmethod
    def nonempty(cls, text: str) -> str:
        if not text.strip():
            raise ValueError("消息不能只有空白")
        return text


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


class MessageView(View):
    id: str
    topic_id: str
    run_id: str
    sequence: int
    role: Literal["user", "assistant", "tool"]
    content: str
    details: dict[str, JsonValue]
    created_at: datetime


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
