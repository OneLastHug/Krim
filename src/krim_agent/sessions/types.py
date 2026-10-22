"""v2会话/Turn/控制合约；不改变v1 Run协议。"""

from typing import Literal

from pydantic import Field, JsonValue, field_validator

from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import Message, StrictModel

TargetKey = Literal[
    "demo", "target-v1", "target-v2", "target-bad", "target-http", "target-workspace"
]
Key = str


class SessionCreate(StrictModel):
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    title: str = Field(default="新对话", min_length=1, max_length=128)
    target_key: TargetKey = "demo"
    context_policy: ContextPolicy | None = None


class TurnRequest(StrictModel):
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    text: str = Field(min_length=1, max_length=32768)
    expected_revision: int = Field(ge=0)

    @field_validator("text")
    @classmethod
    def bounded(cls, text: str) -> str:
        if not text.strip() or len(text.encode()) > 32768:
            raise ValueError("输入为空或超限")
        return text


class ControlRequest(StrictModel):
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    expected_revision: int = Field(ge=0)
    title: str | None = Field(default=None, min_length=1, max_length=128)


class SessionView(StrictModel):
    id: str
    workspace_id: str
    owner_id: str
    create_key: str
    title: str
    target_key: TargetKey
    target_fingerprint: str
    context_policy: ContextPolicy
    revision: int
    status: Literal["open", "closed"]
    dispatch_paused: bool
    created_at: float
    closed_at: float | None


class TurnView(StrictModel):
    id: str
    session_id: str
    workspace_id: str
    owner_id: str
    ordinal: int
    request_key: str
    text: str
    accepted_revision: int
    status: Literal["queued", "running", "completed", "failed", "cancelled", "interrupted"]
    run_id: str | None
    context_fingerprint: str | None
    transcript: list[Message] | None
    code: str | None
    created_at: float
    started_at: float | None
    finished_at: float | None


class ControlView(StrictModel):
    id: str
    session_id: str
    request_key: str
    kind: str
    status: Literal["pending", "completed"]
    result: dict[str, JsonValue] | None
