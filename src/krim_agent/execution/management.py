"""命令管理HTTP/CLI共用的有限输入与视图模型。"""

from datetime import datetime

from pydantic import Field

from krim_agent.protocol.models import StrictModel

from .models import ExecutionStatus
from .records import CommandPhase


class CommandAccepted(StrictModel):
    command_id: str
    request_key: str


class CommandHistoryRequest(StrictModel):
    after: str | None = Field(default=None, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,35}$")
    limit: int = Field(default=32, ge=1, le=32)


class CommandHistoryEntry(StrictModel):
    command_id: str
    request_key: str
    command_preview: str
    phase: CommandPhase
    outcome: ExecutionStatus | None
    started: bool
    processes_reaped: bool
    error_code: str | None
    created_at: datetime


class CommandHistoryPage(StrictModel):
    entries: list[CommandHistoryEntry] = Field(max_length=32)
    next_after: str | None
