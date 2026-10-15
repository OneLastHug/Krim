"""原生目录检查点的可信完整材料；生产捕获与恢复资格另行仲裁。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel, check_json
from krim_agent.workspace_change import SHA256
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
