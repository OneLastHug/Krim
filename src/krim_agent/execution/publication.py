"""命令私有输入/产物的差异与文件提案；公开请求不携带完整文件内容。"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.workspace import Workspace
from krim_agent.workspace_change import WorkspaceChangeView

from .agent_models import CommandLookup
from .models import ExecutionStatus

ChangeKind = Literal["added", "modified", "deleted", "unchanged"]


class CommandChangesRequest(CommandLookup):
    after: str | None = Field(default=None, max_length=512)
    limit: int = Field(default=32, ge=1, le=32)
    include_unchanged: bool = False


class CommandDelta(StrictModel):
    path: str
    kind: ChangeKind
    before_sha256: str | None
    after_sha256: str | None
    before_bytes: int | None
    after_bytes: int | None
    before_mode: int | None
    after_mode: int | None


class CommandChangesPage(StrictModel):
    command_id: str
    source_hash: str
    outcome: ExecutionStatus
    requires_failed_acceptance: bool
    entries: list[CommandDelta] = Field(max_length=32)
    next_after: str | None


class CommandProposalRequest(StrictModel):
    command_id: str = Field(min_length=1, max_length=36)
    path: str = Field(min_length=1, max_length=512)
    request_key: str = Field(min_length=1, max_length=128)
    ttl_seconds: float = Field(default=3600, gt=0, le=86400)
    accept_failed: bool = False

    @model_validator(mode="after")
    def file_path(self) -> CommandProposalRequest:
        try:
            if not Workspace._parts(self.path):
                raise ValueError("产物提案须指定文件路径")
        except ServiceError as error:
            raise ValueError(str(error)) from error
        return self


class CommandProposalView(StrictModel):
    command_id: str
    source_hash: str
    change: CommandDelta
    accept_failed: bool
    proposal: WorkspaceChangeView
