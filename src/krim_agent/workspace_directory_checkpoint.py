"""原生目录检查点的可信完整材料；生产捕获与恢复资格另行仲裁。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel, check_json
from krim_agent.workspace_change import SHA256
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshot, _path


class DirectoryCheckpointMaterialCreate(StrictModel):
    title: str = Field(default="目录检查点", min_length=1, max_length=120)
    root_path: str = Field(min_length=1, max_length=512)
    request_key: str = Field(min_length=1, max_length=128)

    @model_validator(mode="after")
    def canonical(self) -> DirectoryCheckpointMaterialCreate:
        if not self.title.strip() or any(ord(char) < 32 for char in self.request_key):
            raise ValueError("目录检查点标题或请求键无效")
        try:
            _path(self.root_path)
        except ServiceError:
            raise ValueError("目录检查点须指定授权根内的规范相对目录") from None
        check_json(self.model_dump(mode="json"), max_bytes=24576)
        return self


class DirectoryCheckpointCreate(DirectoryCheckpointMaterialCreate):
    """公开目录捕获请求；内部材料键由可信生产者另行生成。"""


class DirectoryCheckpointMaterialView(StrictModel):
    source: Literal["workspace_directory_material"] = "workspace_directory_material"
    status: Literal["stored"] = "stored"
    checkpoint_id: str
    scope_id: str
    request_key: str
    title: str
    root_path: str
    workspace_key: str
    resource_fingerprint: str = Field(pattern=SHA256)
    snapshot_fingerprint: str = Field(pattern=SHA256)
    entry_count: int = Field(ge=1, le=1024)
    file_count: int = Field(ge=0, le=1023)
    directory_count: int = Field(ge=1, le=1024)
    byte_size: int = Field(ge=0, le=33554432)
    created_at: datetime

    @model_validator(mode="after")
    def complete_counts(self) -> DirectoryCheckpointMaterialView:
        if self.file_count + self.directory_count != self.entry_count:
            raise ValueError("目录检查点节点计数不完整")
        return self


@dataclass(frozen=True)
class StoredDirectoryCheckpoint:
    view: DirectoryCheckpointMaterialView
    # 私有字节仅供可信层消费；默认repr不输出完整文件内容。
    snapshot: WorkspaceTreeSnapshot = field(repr=False)


class DirectoryCheckpointSummary(StrictModel):
    snapshot_fingerprint: str = Field(pattern=SHA256)
    entry_count: int = Field(ge=1, le=1024)
    file_count: int = Field(ge=0, le=1023)
    directory_count: int = Field(ge=1, le=1024)
    byte_size: int = Field(ge=0, le=33554432)

    @model_validator(mode="after")
    def complete_counts(self) -> DirectoryCheckpointSummary:
        if self.file_count + self.directory_count != self.entry_count:
            raise ValueError("目录捕获节点计数不完整")
        return self


DirectoryCheckpointStatus = Literal["capturing", "available", "failed", "cancelled", "interrupted"]


class DirectoryCheckpointView(StrictModel):
    source: Literal["workspace_directory"] = "workspace_directory"
    checkpoint_id: str
    scope_id: str
    title: str
    root_path: str
    request_key: str
    workspace_key: str
    resource_fingerprint: str = Field(pattern=SHA256)
    status: DirectoryCheckpointStatus
    snapshot: DirectoryCheckpointSummary | None
    error_code: str | None
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def actual_material(self) -> DirectoryCheckpointView:
        if self.status == "available" and (self.snapshot is None or self.error_code is not None):
            raise ValueError("可用目录检查点须有完整材料且无失败原因")
        if (self.status in {"failed", "cancelled", "interrupted"}) != (self.error_code is not None):
            raise ValueError("目录捕获终态与失败原因不一致")
        return self


class DirectoryCheckpointCaptureInput(StrictModel):
    root_path: str = Field(min_length=1, max_length=512)

    @model_validator(mode="after")
    def canonical(self) -> DirectoryCheckpointCaptureInput:
        try:
            _path(self.root_path)
        except ServiceError:
            raise ValueError("目录捕获须指定规范非空相对目录") from None
        return self


class DirectoryCheckpointCaptureView(DirectoryCheckpointSummary):
    checkpoint_id: str
    root_path: str
    captured: Literal[True] = True


@dataclass(frozen=True)
class DirectoryCheckpointCaptureFence:
    checkpoint_id: str
    token: str = field(repr=False)
    model_source: CheckpointModelSource | None = None


def directory_checkpoint_model_source(value: object) -> CheckpointModelSource | None:
    """内部来源先分类；假/falsey对象不能被解释成Host来源。"""
    if value is None:
        return None
    if (
        type(value) is not CheckpointModelSource
        or type(value.run_id) is not str
        or not 1 <= len(value.run_id) <= 36
        or value.run_id == "local"
        or type(value.call_id) is not str
        or not 1 <= len(value.call_id) <= 128
        or any(ord(char) < 32 or ord(char) == 127 for char in value.call_id)
        or value.restore_plan_id is not None
    ):
        raise ServiceError(
            "workspace_directory_capture_model_context", "目录捕获须保留原Run/call来源", 403
        )
    return value
