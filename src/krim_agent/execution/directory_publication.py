"""命令目录manifest到单目录容器提案；不将目录伪装为文件字节。"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.workspace import Workspace
from krim_agent.workspace_directory import DirectoryPreviewView

from .directory_dependencies import DirectoryDependencyPlan
from .models import ExecutionStatus
from .records import CommandFile, CommandResource


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


class CommandDirectoryChangesRequest(StrictModel):
    command_id: str = Field(min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    after: str | None = Field(default=None, min_length=1, max_length=512)
    limit: int = Field(default=32, ge=1, le=32)

    @model_validator(mode="after")
    def cursor(self) -> CommandDirectoryChangesRequest:
        if self.after is not None:
            try:
                if not Workspace._parts(self.after):
                    raise ValueError("目录来源游标须为原相对路径")
            except ServiceError as error:
                raise ValueError(str(error)) from error
        return self


class CommandDirectoryDelta(StrictModel):
    path: str
    mode: int = Field(ge=0, le=0o777)
    empty: bool
    creatable: bool
    proposal_id: str | None = Field(
        default=None, min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$"
    )


class CommandDirectoryChangesPage(StrictModel):
    command_id: str
    source_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    outcome: Literal["completed", "failed", "output_limit"]
    complete: Literal[True] = True
    entries: list[CommandDirectoryDelta] = Field(max_length=32)
    next_after: str | None = None

    @model_validator(mode="after")
    def ordered_page(self) -> CommandDirectoryChangesPage:
        paths = [item.path for item in self.entries]
        if paths != sorted(set(paths)):
            raise ValueError("目录来源页须为不重复的有序路径")
        if self.next_after is not None and (not paths or self.next_after != paths[-1]):
            raise ValueError("目录来源游标须为本页末项")
        return self


class CommandDirectoryDependencyRequest(StrictModel):
    """选择原完整成果生成依赖计划，来源和目录规格不由调用方提供。"""

    command_id: str = Field(min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    source_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    accept_failed: bool = False
    max_entries: int = Field(default=16384, ge=1, le=16384)


class CommandDirectoryDependencyView(StrictModel):
    command_id: str = Field(min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    scope_id: str
    source_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    workspace_key: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
    resource_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    execution_resource: CommandResource
    outcome: Literal["completed", "failed", "output_limit"]
    complete: Literal[True] = True
    accept_failed: bool
    plan: DirectoryDependencyPlan
    files: list[CommandFile] = Field(max_length=16384)

    @model_validator(mode="after")
    def source_facts(self) -> CommandDirectoryDependencyView:
        if self.plan.operation != "create":
            raise ValueError("命令目录来源不能推断宿主删除意图")
        if self.outcome != "completed" and not self.accept_failed:
            raise ValueError("失败命令成果须显式接受")
        paths = [item.path for item in self.files]
        expected = sorted(item.path for item in self.plan.steps if item.kind == "file")
        if paths != expected:
            raise ValueError("目录依赖计划与原文件清单不匹配")
        if any(
            item.kind == "directory" and (item.mode is None or item.mode & 0o700 != 0o700)
            for item in self.plan.steps
        ):
            raise ValueError("目录来源模式不支持受控创建")
        return self
