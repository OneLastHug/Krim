"""命令管理HTTP/CLI共用的有限输入与视图模型。"""

from __future__ import annotations

from datetime import datetime

from pydantic import Field, model_validator

from krim_agent.protocol.models import StrictModel

from .models import ExecutionStatus, SandboxLimits
from .records import CommandPhase, CommandResource
from .tree_models import SandboxTreeUsage


class CommandResourcesView(StrictModel):
    command_id: str
    revision: int = Field(ge=0, le=9007199254740991)
    scope_id: str
    topic_id: str
    request_key: str
    resource: CommandResource
    limits: SandboxLimits
    usage: SandboxTreeUsage | None
    run_id: str | None
    call_id: str | None
    phase: CommandPhase
    outcome: ExecutionStatus | None
    dispatched: bool
    started: bool
    processes_reaped: bool
    error_code: str | None

    @model_validator(mode="after")
    def original_tree(self) -> CommandResourcesView:
        if self.usage is not None:
            if self.limits.tree != self.usage.limits:
                raise ValueError("整树统计没有绑定原冻结限额")
            if self.started and not self.usage.membership_verified:
                raise ValueError("已启动命令缺少原进组资格")
            if self.processes_reaped and (
                self.usage.populated is not False or self.usage.leader_reaped is not True
            ):
                raise ValueError("进程树清空或leader回收尚未确认")
        elif self.limits.tree is not None and self.processes_reaped and self.dispatched:
            raise ValueError("已派发树命令不能伪造回收确认")
        return self


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
