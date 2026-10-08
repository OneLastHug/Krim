"""受控文件修改数据合约；持久回执与消费端沿ADR-0055分层接入。"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.models import StrictModel, check_json

SHA256 = r"^[0-9a-f]{64}$"


class TextReplacement(StrictModel):
    old_text: str = Field(min_length=1, max_length=8192)
    new_text: str = Field(max_length=8192)
    occurrences: int = Field(default=1, ge=1, le=1000)


class FileChangeSpec(StrictModel):
    path: str = Field(min_length=1, max_length=512)
    operation: Literal["write", "replace", "delete"]
    expected_sha256: str | None = Field(pattern=SHA256)
    content: str | None = Field(default=None, max_length=24000)
    replacements: list[TextReplacement] = Field(default_factory=list, max_length=16)

    @model_validator(mode="after")
    def valid_operation(self) -> FileChangeSpec:
        if self.operation == "write":
            if self.content is None or self.replacements:
                raise ValueError("写入须提供完整content，不同时提供replacements")
        elif self.operation == "replace":
            if self.expected_sha256 is None or self.content is not None or not self.replacements:
                raise ValueError("文本替换须提供原SHA与replacements")
        elif self.expected_sha256 is None or self.content is not None or self.replacements:
            raise ValueError("删除须提供原SHA，不提供新内容")
        try:
            check_json(self.model_dump(mode="json"), max_bytes=32768)
        except UnicodeEncodeError:
            raise ValueError("修改参数须为UTF-8") from None
        return self


class FileChangePreview(StrictModel):
    path: str
    operation: Literal["write", "replace", "delete"]
    before_sha256: str | None = Field(pattern=SHA256)
    after_sha256: str | None = Field(pattern=SHA256)
    before_bytes: int = Field(ge=0)
    after_bytes: int = Field(ge=0)
    diff: str
    truncated: bool


class FileChangeReceipt(StrictModel):
    path: str
    status: Literal["done", "rejected", "uncertain"]
    before_sha256: str | None = Field(pattern=SHA256)
    after_sha256: str | None = Field(pattern=SHA256)
    changed: bool
    error_code: str | None = None
    recovery_required: bool = False

    @model_validator(mode="after")
    def outcome(self) -> FileChangeReceipt:
        if (self.status == "done") != (self.error_code is None):
            raise ValueError("done不带错误码，rejected/uncertain须带稳定错误码")
        if self.recovery_required != (self.status == "uncertain"):
            raise ValueError("uncertain必须明确需要核实恢复")
        return self


class WorkspaceChangeView(StrictModel):
    proposal_id: str
    scope_id: str
    topic_id: str
    workspace_key: str
    resource_fingerprint: str
    revision: int = Field(ge=0, le=9007199254740991)
    status: Literal[
        "proposed",
        "applying",
        "applied",
        "undoing",
        "undone",
        "keeping",
        "kept",
        "rejected",
        "uncertain",
    ]
    preview: FileChangePreview
    active_operation_id: str | None = None
    expires_at: float
    created_at: datetime
    updated_at: datetime


class WorkspaceChangeAction(StrictModel):
    proposal_id: str = Field(min_length=1, max_length=36)
    action: Literal["apply", "undo", "keep"]
    expected_revision: int = Field(ge=0, le=9007199254740991)
    request_key: str = Field(min_length=1, max_length=128)


class WorkspaceAttemptView(StrictModel):
    operation_id: str
    proposal_id: str
    scope_id: str
    action: Literal["apply", "undo", "keep"]
    expected_revision: int
    status: Literal["running", "done", "rejected", "uncertain"]
    receipt: FileChangeReceipt | None
    cleanup_pending: bool
    run_id: str | None = None
    call_id: str | None = None
    created_at: datetime
    updated_at: datetime


class SnapshotMetadata(StrictModel):
    mode: int = Field(ge=0, le=4095)
    device: int = Field(ge=0)
    inode: int = Field(ge=0)
    mtime_ns: int
    ctime_ns: int
    uid: int = Field(ge=0)
    gid: int = Field(ge=0)


class PreparedChangeMetadata(StrictModel):
    spec_json: str
    workspace_fingerprint: str = Field(pattern=SHA256)
    parent_device: int = Field(ge=0)
    parent_inode: int = Field(ge=0)
    token: str = Field(pattern=r"^[0-9a-f]{32}$")
    before: SnapshotMetadata | None
    snapshot_hash: str = Field(pattern=SHA256)


class RecoveryMetadata(StrictModel):
    workspace_fingerprint: str
    path: str
    stage_name: str
    parent_device: int
    parent_inode: int
