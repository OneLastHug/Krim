"""命令目录manifest到单目录容器提案；不将目录伪装为文件字节。"""

from __future__ import annotations

from pydantic import Field, model_validator

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.workspace import Workspace
from krim_agent.workspace_directory import DirectoryPreviewView

from .models import ExecutionStatus


class CommandDirectoryProposalRequest(StrictModel):
    command_id: str = Field(min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    path: str = Field(min_length=1, max_length=512)
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[^\x00-\x1f\x7f]+$")
    ttl_seconds: float = Field(default=3600, gt=0, le=86400)
    accept_failed: bool = False

    @model_validator(mode="after")
    def relative_directory(self) -> CommandDirectoryProposalRequest:
        try:
            if not Workspace._parts(self.path):
                raise ValueError("目录来源提案须指定相对目录路径")
        except ServiceError as error:
            raise ValueError(str(error)) from error
        return self


class CommandDirectoryEntry(StrictModel):
    path: str
    mode: int = Field(ge=0, le=0o777)
    empty: bool
    creatable: bool


class CommandDirectoryProposalView(StrictModel):
    command_id: str
    source_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    outcome: ExecutionStatus
    accept_failed: bool
    directory: CommandDirectoryEntry
    proposal: DirectoryPreviewView
