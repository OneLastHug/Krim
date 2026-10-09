"""原生文件检查点；公开元数据与可信内部捕获凭据分离。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.models import StrictModel, check_json
from krim_agent.workspace import Workspace
from krim_agent.workspace_change import SHA256, SnapshotMetadata


class WorkspaceCheckpointCreate(StrictModel):
    title: str = Field(default="文件检查点", min_length=1, max_length=120)
    paths: list[str] = Field(min_length=1, max_length=32)
    request_key: str = Field(min_length=1, max_length=128)

    @model_validator(mode="after")
    def bounded(self) -> WorkspaceCheckpointCreate:
        if not self.title.strip() or len(set(self.paths)) != len(self.paths):
            raise ValueError("检查点标题不能为空，路径不能重复")
        if any(ord(char) < 32 for char in self.request_key):
            raise ValueError("检查点请求键不能包含控制字符")
        for path in self.paths:
            if not 1 <= len(path) <= 512:
                raise ValueError("检查点路径长度无效")
            try:
                if not Workspace._parts(path):
                    raise ValueError("检查点必须指定文件相对路径")
            except Exception as error:
                raise ValueError("检查点路径不符合工作区边界") from error
        check_json(self.model_dump(mode="json"), max_bytes=24576)
        return self


class WorkspaceCheckpointFileView(StrictModel):
    path: str
    captured: bool
    exists: bool | None
    sha256: str | None = Field(pattern=SHA256)
    byte_size: int | None = Field(ge=0)
    mode: int | None = Field(ge=0, le=4095)


class WorkspaceCheckpointView(StrictModel):
    source: Literal["workspace"] = "workspace"
    checkpoint_id: str
    scope_id: str
    title: str
    request_key: str
    workspace_key: str
    resource_fingerprint: str
    status: Literal["capturing", "available", "failed", "cancelled", "interrupted"]
    files: list[WorkspaceCheckpointFileView] = Field(min_length=1, max_length=32)
    error_code: str | None
    created_at: datetime
    updated_at: datetime


class CapturedFileMetadata(StrictModel):
    path: str
    workspace_fingerprint: str = Field(pattern=SHA256)
    parent_device: int = Field(ge=0)
    parent_inode: int = Field(ge=0)
    snapshot: SnapshotMetadata | None
    content_sha256: str | None = Field(pattern=SHA256)
    byte_size: int = Field(ge=0, le=8388608)
    snapshot_hash: str = Field(pattern=SHA256)

    @model_validator(mode="after")
    def consistent_presence(self) -> CapturedFileMetadata:
        if (self.snapshot is None) != (self.content_sha256 is None):
            raise ValueError("文件身份与存在性摘要不匹配")
        if self.snapshot is None and self.byte_size != 0:
            raise ValueError("不存在的文件不能带内容长度")
        return self


@dataclass(frozen=True)
class CheckpointCaptureFence:
    checkpoint_id: str
    position: int
    token: str
