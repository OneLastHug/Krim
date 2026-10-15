"""原生文件检查点；公开元数据与可信内部捕获凭据分离。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from pydantic import Field, SerializerFunctionWrapHandler, model_serializer, model_validator

from krim_agent.protocol.models import StrictModel, check_json
from krim_agent.workspace import Workspace
from krim_agent.workspace_change import SHA256, SnapshotMetadata

CheckpointCaptureFormat = Literal["text", "bytes"]
CheckpointContentKind = Literal["text", "binary", "absent"]


def checkpoint_content_kind(content: bytes | None) -> CheckpointContentKind:
    if content is None:
        return "absent"
    if b"\0" in content:
        return "binary"
    try:
        content.decode("utf-8")
    except UnicodeDecodeError:
        return "binary"
    return "text"


class WorkspaceCheckpointCreate(StrictModel):
    title: str = Field(default="文件检查点", min_length=1, max_length=120)
    paths: list[str] = Field(min_length=1, max_length=32)
    request_key: str = Field(min_length=1, max_length=128)
    capture_format: CheckpointCaptureFormat = "text"

    @model_serializer(mode="wrap")
    def original_json(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        value: dict[str, Any] = handler(self)
        if self.capture_format == "text":
            value.pop("capture_format", None)
        return value

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


class CheckpointCaptureInput(StrictModel):
    path: str = Field(min_length=1, max_length=512)

    @model_validator(mode="after")
    def canonical(self) -> CheckpointCaptureInput:
        try:
            if not Workspace._parts(self.path):
                raise ValueError("检查点捕获需要规范文件路径")
        except Exception as error:
            raise ValueError("检查点捕获路径不符合工作区边界") from error
        return self


class CheckpointCaptureView(StrictModel):
    path: str
    captured: Literal[True] = True
    exists: bool
    sha256: str | None = Field(pattern=SHA256)
    byte_size: int = Field(ge=0, le=8388608)
    mode: int | None = Field(ge=0, le=511)
    content_kind: CheckpointContentKind

    @model_validator(mode="after")
    def actual_presence(self) -> CheckpointCaptureView:
        if self.exists:
            if self.sha256 is None or self.mode is None or self.content_kind == "absent":
                raise ValueError("真实捕获元数据缺少存在内容")
        elif (
            self.sha256 is not None
            or self.mode is not None
            or self.byte_size
            or self.content_kind != "absent"
        ):
            raise ValueError("缺失捕获不能伪造内容")
        return self


class WorkspaceCheckpointFileView(StrictModel):
    path: str
    captured: bool
    exists: bool | None
    sha256: str | None = Field(pattern=SHA256)
    byte_size: int | None = Field(ge=0)
    mode: int | None = Field(ge=0, le=4095)
    content_kind: CheckpointContentKind | None = None

    @model_serializer(mode="wrap")
    def original_json(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        value: dict[str, Any] = handler(self)
        if self.content_kind is None:
            value.pop("content_kind", None)
        return value


class WorkspaceCheckpointView(StrictModel):
    source: Literal["workspace"] = "workspace"
    checkpoint_id: str
    scope_id: str
    title: str
    request_key: str
    workspace_key: str
    resource_fingerprint: str
    capture_format: CheckpointCaptureFormat = "text"
    status: Literal["capturing", "available", "failed", "cancelled", "interrupted"]
    files: list[WorkspaceCheckpointFileView] = Field(min_length=1, max_length=32)
    error_code: str | None
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def actual_files(self) -> WorkspaceCheckpointView:
        if len({item.path for item in self.files}) != len(self.files):
            raise ValueError("原检查点不能有重复路径")
        for item in self.files:
            if not item.captured:
                if any(
                    value is not None
                    for value in (
                        item.exists,
                        item.sha256,
                        item.byte_size,
                        item.mode,
                        item.content_kind,
                    )
                ):
                    raise ValueError("未捕获文件不能伪造实际元数据")
            elif item.exists is True:
                if (
                    item.sha256 is None
                    or item.byte_size is None
                    or item.byte_size > 8388608
                    or item.mode is None
                    or item.content_kind == "absent"
                ):
                    raise ValueError("存在文件必须有完整捕获元数据")
            elif item.exists is False:
                if (
                    item.sha256 is not None
                    or item.mode is not None
                    or item.byte_size != 0
                    or item.content_kind not in {None, "absent"}
                ):
                    raise ValueError("缺失文件不能伪造字节或身份")
            else:
                raise ValueError("已捕获文件必须明确存在性")
            if item.captured and (
                (item.content_kind is not None) != (self.capture_format == "bytes")
            ):
                raise ValueError("原检查点格式与捕获种类不一致")
        if self.status == "available" and not all(item.captured for item in self.files):
            raise ValueError("可用检查点必须完整捕获原文件集")
        return self

    @model_serializer(mode="wrap")
    def original_json(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        value: dict[str, Any] = handler(self)
        if self.capture_format == "text":
            value.pop("capture_format", None)
        return value


class CapturedFileMetadata(StrictModel):
    path: str
    workspace_fingerprint: str = Field(pattern=SHA256)
    parent_device: int = Field(ge=0)
    parent_inode: int = Field(ge=0)
    snapshot: SnapshotMetadata | None
    content_sha256: str | None = Field(pattern=SHA256)
    byte_size: int = Field(ge=0, le=8388608)
    snapshot_hash: str = Field(pattern=SHA256)
    content_kind: CheckpointContentKind | None = None

    @model_serializer(mode="wrap")
    def original_json(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        value: dict[str, Any] = handler(self)
        if self.content_kind is None:
            value.pop("content_kind", None)
        return value

    @model_validator(mode="after")
    def consistent_presence(self) -> CapturedFileMetadata:
        if (self.snapshot is None) != (self.content_sha256 is None):
            raise ValueError("文件身份与存在性摘要不匹配")
        if self.snapshot is None and self.byte_size != 0:
            raise ValueError("不存在的文件不能带内容长度")
        if self.content_kind is not None and (self.content_kind == "absent") != (
            self.snapshot is None
        ):
            raise ValueError("原生字节种类与存在性不匹配")
        return self


@dataclass(frozen=True)
class CheckpointModelSource:
    run_id: str
    call_id: str


@dataclass(frozen=True)
class CheckpointCaptureFence:
    checkpoint_id: str
    position: int
    token: str
    capture_format: CheckpointCaptureFormat = "text"
    model_source: CheckpointModelSource | None = None
