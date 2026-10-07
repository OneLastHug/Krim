"""Agent 会话数据合约；独立于管理器和仓储，避免循环导入。"""

from datetime import datetime
from typing import Literal

from pydantic import Field, JsonValue

from krim_agent.protocol.models import StrictModel

SessionMode = Literal["agent", "ask", "plan", "auto"]
SessionStatus = Literal["open", "closed", "expired", "destroyed"]


class AgentSessionView(StrictModel):
    session_id: str
    agent_id: str
    topic_id: str
    name: str
    persistent: bool
    profile_revision: int | None = None
    definition_hash: str
    parent_session_id: str | None = None
    status: SessionStatus
    revision: int = Field(ge=0)
    owner_id: str | None = None
    expires_at: float | None = None
    created_at: datetime
    closed_at: datetime | None = None
    permissions: dict[str, JsonValue]
    turn: int = Field(ge=0)
    current_run_id: str | None = None


class AgentSessionControl(StrictModel):
    expected_revision: int = Field(ge=0)


class AgentTurnView(StrictModel):
    session_id: str
    agent_id: str
    run_id: str
    turn: int = Field(ge=1)
    status: str
    created_at: datetime
