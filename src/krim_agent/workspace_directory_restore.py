"""目录原子恢复的独立计划合约；不会将旧逐文件partial标为原子完成。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_directory_checkpoint import DirectoryCheckpointSummary
from krim_agent.workspace_directory_restore_writer import PreparedDirectoryRestore
from krim_agent.workspace_tree_snapshot import _path

IDENTIFIER = r"^[A-Za-z0-9][A-Za-z0-9_-]{0,35}$"
REQUEST_KEY = r"^[^\x00-\x1f\x7f]{1,128}$"
FINGERPRINT = r"^[a-f0-9]{64}$"


def directory_restore_model_source(value: object) -> CheckpointModelSource | None:
    """先分类原父来源，falsey对象不能降为Host；不接受旧文件恢复基线标记。"""
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
            "workspace_directory_restore_model_context", "目录恢复需要原模型父调用", 403
        )
    return value


class DirectoryRestoreTarget(StrictModel):
    kind: Literal["checkpoint", "baseline"] = "checkpoint"
    identifier: str = Field(pattern=IDENTIFIER)


class DirectoryRestorePreview(StrictModel):
    target: DirectoryRestoreTarget
    request_key: str = Field(pattern=REQUEST_KEY)
    ttl_seconds: float = Field(default=3600.0, gt=0, le=86400)


class DirectoryRestoreExecute(StrictModel):
    plan_id: str = Field(pattern=IDENTIFIER)
    expected_revision: int = Field(ge=0, le=9007199254740991)
    request_key: str = Field(pattern=REQUEST_KEY)


class DirectoryRestoreNodeView(StrictModel):
    kind: Literal["file", "directory"]
    mode: int = Field(ge=0, le=0o777)
    uid: int = Field(ge=0, lt=2**32 - 1)
    gid: int = Field(ge=0, lt=2**32 - 1)
    byte_size: int = Field(ge=0, le=8_388_608)
    sha256: str | None = Field(pattern=FINGERPRINT)

    @model_validator(mode="after")
    def content_summary(self) -> DirectoryRestoreNodeView:
        if self.kind == "directory" and (self.byte_size != 0 or self.sha256 is not None):
            raise ValueError("目录节点不能声称文件内容")
        if self.kind == "file" and self.sha256 is None:
            raise ValueError("文件节点必须有原字节摘要")
        return self


class DirectoryRestoreDiffEntry(StrictModel):
    path: str = Field(min_length=1, max_length=512)
    operation: Literal["create", "delete", "modify", "replace"]
    before: DirectoryRestoreNodeView | None
    after: DirectoryRestoreNodeView | None

    @model_validator(mode="after")
    def exact_change(self) -> DirectoryRestoreDiffEntry:
        _path(self.path)
        expected = (
            "create"
            if self.before is None and self.after is not None
            else "delete"
            if self.before is not None and self.after is None
            else "replace"
            if self.before is not None
            and self.after is not None
            and self.before.kind != self.after.kind
            else "modify"
        )
        if self.operation != expected or self.before == self.after:
            raise ValueError("目录差异必须反映原完整材料的真实变化")
        return self


class DirectoryRestoreReceiptView(StrictModel):
    root_path: str = Field(min_length=1, max_length=512)
    status: Literal["done", "rejected", "uncertain"]
    before_fingerprint: str = Field(pattern=FINGERPRINT)
    target_fingerprint: str = Field(pattern=FINGERPRINT)
    after_fingerprint: str | None = Field(pattern=FINGERPRINT)
    retained_fingerprint: str | None = Field(pattern=FINGERPRINT)
    changed: bool
    error_code: str | None = Field(min_length=1, max_length=128)
    recovery_required: bool

    @model_validator(mode="after")
    def original_effect(self) -> DirectoryRestoreReceiptView:
        _path(self.root_path)
        if self.status == "done":
            valid = (
                self.changed
                and self.error_code is None
                and not self.recovery_required
                and self.after_fingerprint is not None
                and self.retained_fingerprint is not None
            )
        else:
            valid = (
                self.error_code is not None
                and self.after_fingerprint is None
                and self.retained_fingerprint is None
                and self.changed == (self.status == "uncertain")
                and self.recovery_required == (self.status == "uncertain")
            )
        if not valid:
            raise ValueError("目录恢复回执必须反映原完整效果或原未知事实")
        return self


DirectoryRestorePhase = Literal[
    "preparing",
    "ready",
    "executing",
    "finished",
    "failed",
    "cancelled",
    "interrupted",
    "expired",
    "uncertain",
]
DirectoryRestoreStatus = Literal[
    "preparing",
    "ready",
    "running",
    "done",
    "failed",
    "cancelled",
    "interrupted",
    "expired",
    "uncertain",
]


class DirectoryRestoreView(StrictModel):
    source: Literal["workspace_directory_restore"] = "workspace_directory_restore"
    plan_id: str = Field(pattern=IDENTIFIER)
    scope_id: str = Field(min_length=1, max_length=80)
    target: DirectoryRestoreTarget
    root_path: str = Field(min_length=1, max_length=512)
    workspace_key: str = Field(min_length=1, max_length=64)
    resource_fingerprint: str = Field(pattern=FINGERPRINT)
    target_snapshot: DirectoryCheckpointSummary
    backup: DirectoryCheckpointSummary | None
    request_key: str = Field(pattern=REQUEST_KEY)
    execute_key: str | None = Field(pattern=REQUEST_KEY)
    revision: int = Field(ge=0, le=9007199254740991)
    phase: DirectoryRestorePhase
    status: DirectoryRestoreStatus
    operation_id: str = Field(pattern=IDENTIFIER)
    receipt: DirectoryRestoreReceiptView | None
    cleanup_pending: bool
    cancel_requested: bool
    expires_at: float = Field(gt=0, le=9007199254740991)
    error_code: str | None = Field(min_length=1, max_length=128)
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def plan_facts(self) -> DirectoryRestoreView:
        _path(self.root_path)
        for summary in (self.target_snapshot, self.backup):
            if summary is not None and summary.file_count == 0 and summary.byte_size != 0:
                raise ValueError("零文件的目录恢复材料不能包含文件字节")
        expected = {"executing": "running", "finished": "done"}.get(self.phase, self.phase)
        if self.status != expected:
            raise ValueError("目录恢复phase与status不一致")
        if self.execute_key is None:
            if (
                self.operation_id != self.plan_id
                or self.receipt is not None
                or self.cleanup_pending
            ):
                raise ValueError("未执行计划只能描述原准备，不能声称物理效果或清理")
        elif self.operation_id == self.plan_id or self.backup is None or self.revision < 2:
            raise ValueError("执行须绑定独立原操作、完整备份和审阅后的版本")
        if (
            self.phase == "preparing"
            and self.revision != 0
            or self.phase == "ready"
            and (self.revision < 1 or self.execute_key is not None)
        ):
            raise ValueError("目录恢复准备与审阅版本不一致")
        if self.phase == "cancelled" and not self.cancel_requested:
            raise ValueError("目录恢复取消必须保留原取消事实")
        if self.phase in {"ready", "executing", "finished", "uncertain"} and self.backup is None:
            raise ValueError("准备完成的目录恢复必须保留原完整备份")
        if self.phase in {"executing", "finished", "uncertain"} and self.execute_key is None:
            raise ValueError("目录恢复执行必须绑定原执行键")
        if self.phase in {"preparing", "ready", "executing", "finished"}:
            if self.error_code is not None:
                raise ValueError("非失败目录恢复不能包含错误")
        elif self.error_code is None:
            raise ValueError("目录恢复非成功终态必须保留原因")
        if self.phase == "finished" and (self.receipt is None or self.receipt.status != "done"):
            raise ValueError("目录恢复完成必须有原成功回执")
        if self.receipt is not None and (
            self.receipt.root_path != self.root_path
            or self.receipt.target_fingerprint != self.target_snapshot.snapshot_fingerprint
            or self.backup is None
            or self.receipt.before_fingerprint != self.backup.snapshot_fingerprint
            or self.receipt.status == "done"
            and self.phase != "finished"
            or self.receipt.status == "uncertain"
            and self.phase != "uncertain"
            or self.receipt.status == "rejected"
            and self.phase != "failed"
        ):
            raise ValueError("目录恢复回执与原计划不匹配")
        if self.created_at.utcoffset() is None or self.updated_at.utcoffset() is None:
            raise ValueError("目录恢复日期必须包含时区")
        if self.updated_at < self.created_at:
            raise ValueError("目录恢复时间不能倒退")
        return self


class DirectoryRestoreOperationView(StrictModel):
    operation_id: str = Field(pattern=IDENTIFIER)
    plan_id: str = Field(pattern=IDENTIFIER)
    scope_id: str = Field(min_length=1, max_length=80)
    kind: Literal["prepare", "execute"]
    request_key: str = Field(pattern=REQUEST_KEY)
    status: Literal[
        "running", "done", "rejected", "uncertain", "cancelled", "interrupted", "expired"
    ]
    intent_recorded: bool
    receipt: DirectoryRestoreReceiptView | None
    error_code: str | None = Field(min_length=1, max_length=128)
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def operation_facts(self) -> DirectoryRestoreOperationView:
        if (self.kind == "prepare") != (self.operation_id == self.plan_id):
            raise ValueError("准备与独立执行必须保留各自原操作身份")
        if self.kind == "prepare" and (self.intent_recorded or self.receipt is not None):
            raise ValueError("目录暂存操作不能冒充namespace执行")
        if (
            self.status == "done"
            and self.kind == "execute"
            and (not self.intent_recorded or self.receipt is None or self.receipt.status != "done")
        ):
            raise ValueError("目录执行完成必须保留原意图与成功回执")
        if self.status == "uncertain" and (self.kind != "execute" or not self.intent_recorded):
            raise ValueError("目录恢复未知须有原执行派发意图")
        if (self.status in {"running", "done"}) != (self.error_code is None):
            raise ValueError("目录恢复操作状态与错误不一致")
        if self.receipt is not None and self.status != self.receipt.status:
            raise ValueError("目录恢复操作必须保留同一物理回执状态")
        if self.created_at.utcoffset() is None or self.updated_at.utcoffset() is None:
            raise ValueError("目录恢复操作日期必须包含时区")
        if self.updated_at < self.created_at:
            raise ValueError("目录恢复操作日期不能倒退")
        return self


@dataclass(frozen=True)
class DirectoryRestoreFence:
    plan_id: str
    operation_id: str
    kind: Literal["prepare", "execute", "verify"]
    token: str = field(repr=False)
    model_source: CheckpointModelSource | None = None


@dataclass(frozen=True)
class StoredDirectoryRestore:
    view: DirectoryRestoreView
    prepared: PreparedDirectoryRestore | None = field(default=None, repr=False)
