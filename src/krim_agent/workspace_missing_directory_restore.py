"""缺失目录持久恢复的独立公共事实；不改变旧完整树View或私有资格边界。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.models import StrictModel
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_directory_checkpoint import DirectoryCheckpointSummary
from krim_agent.workspace_directory_restore import (
    FINGERPRINT,
    IDENTIFIER,
    REQUEST_KEY,
    DirectoryRestoreExecute,
    DirectoryRestorePhase,
    DirectoryRestorePreview,
    DirectoryRestoreStatus,
    DirectoryRestoreTarget,
)
from krim_agent.workspace_missing_directory_writer import PreparedMissingDirectoryRestore
from krim_agent.workspace_tree_snapshot import _path


class MissingDirectoryRestorePreview(DirectoryRestorePreview):
    """同原目标选择形状，独立类型防止被完整树生产入口冒领。"""


class MissingDirectoryRestoreExecute(DirectoryRestoreExecute):
    """独立原执行键与审阅版本。"""


class DirectoryAbsenceSummary(StrictModel):
    kind: Literal["absent"] = "absent"
    fingerprint: str = Field(pattern=FINGERPRINT)


class MissingDirectoryRestoreReceiptView(StrictModel):
    root_path: str = Field(min_length=1, max_length=512)
    status: Literal["done", "rejected", "uncertain"]
    before_absence_fingerprint: str = Field(pattern=FINGERPRINT)
    target_fingerprint: str = Field(pattern=FINGERPRINT)
    after_fingerprint: str | None = Field(pattern=FINGERPRINT)
    changed: bool
    error_code: str | None = Field(min_length=1, max_length=128)
    recovery_required: bool

    @model_validator(mode="after")
    def original_effect(self) -> MissingDirectoryRestoreReceiptView:
        _path(self.root_path)
        if self.status == "done":
            valid = (
                self.changed
                and self.after_fingerprint is not None
                and self.error_code is None
                and not self.recovery_required
            )
        else:
            valid = (
                self.after_fingerprint is None
                and self.error_code is not None
                and self.changed == (self.status == "uncertain")
                and self.recovery_required == (self.status == "uncertain")
            )
        if not valid:
            raise ValueError("缺失恢复回执必须保留原实际效果或未知")
        return self


class MissingDirectoryRestoreView(StrictModel):
    source: Literal["workspace_missing_directory_restore"] = "workspace_missing_directory_restore"
    plan_id: str = Field(pattern=IDENTIFIER)
    scope_id: str = Field(min_length=1, max_length=80)
    target: DirectoryRestoreTarget
    root_path: str = Field(min_length=1, max_length=512)
    workspace_key: str = Field(min_length=1, max_length=64)
    resource_fingerprint: str = Field(pattern=FINGERPRINT)
    target_snapshot: DirectoryCheckpointSummary
    baseline: DirectoryAbsenceSummary | None
    request_key: str = Field(pattern=REQUEST_KEY)
    execute_key: str | None = Field(pattern=REQUEST_KEY)
    revision: int = Field(ge=0, le=9007199254740991)
    phase: DirectoryRestorePhase
    status: DirectoryRestoreStatus
    operation_id: str = Field(pattern=IDENTIFIER)
    receipt: MissingDirectoryRestoreReceiptView | None
    cleanup_pending: bool
    cancel_requested: bool
    expires_at: float = Field(gt=0, le=9007199254740991)
    error_code: str | None = Field(min_length=1, max_length=128)
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def original_plan(self) -> MissingDirectoryRestoreView:
        _path(self.root_path)
        if self.target_snapshot.file_count == 0 and self.target_snapshot.byte_size != 0:
            raise ValueError("零文件目标不能包含文件字节")
        if self.status != {"executing": "running", "finished": "done"}.get(self.phase, self.phase):
            raise ValueError("缺失恢复phase/status不匹配")
        if self.execute_key is None:
            if (
                self.operation_id != self.plan_id
                or self.receipt is not None
                or self.cleanup_pending
            ):
                raise ValueError("未执行计划不能声称namespace效果")
        elif self.operation_id == self.plan_id or self.baseline is None or self.revision < 2:
            raise ValueError("执行必须有原独立操作和真实缺失基线")
        if (
            self.phase == "preparing"
            and self.revision != 0
            or self.phase == "ready"
            and (self.revision < 1 or self.execute_key is not None)
        ):
            raise ValueError("缺失恢复准备/审阅版本不匹配")
        if self.phase in {"ready", "executing", "finished", "uncertain"} and self.baseline is None:
            raise ValueError("准备完成必须保留真实缺失基线")
        if self.phase in {"executing", "finished", "uncertain"} and self.execute_key is None:
            raise ValueError("缺失恢复执行必须绑定原执行键")
        if self.phase in {"preparing", "ready", "executing", "finished"}:
            if self.error_code is not None:
                raise ValueError("非失败状态不能包含错误")
        elif self.error_code is None:
            raise ValueError("非成功终态必须保留原因")
        if self.phase == "cancelled" and not self.cancel_requested:
            raise ValueError("取消必须保留原请求事实")
        if self.phase == "finished" and (self.receipt is None or self.receipt.status != "done"):
            raise ValueError("完成必须有原实际回执")
        if self.receipt is not None and (
            self.baseline is None
            or self.receipt.root_path != self.root_path
            or self.receipt.before_absence_fingerprint != self.baseline.fingerprint
            or self.receipt.target_fingerprint != self.target_snapshot.snapshot_fingerprint
            or self.phase
            != {"done": "finished", "rejected": "failed", "uncertain": "uncertain"}[
                self.receipt.status
            ]
        ):
            raise ValueError("缺失恢复回执与原计划不匹配")
        if (
            self.created_at.utcoffset() is None
            or self.updated_at.utcoffset() is None
            or self.updated_at < self.created_at
        ):
            raise ValueError("缺失恢复时间必须带时区且不倒退")
        return self


class MissingDirectoryRestoreOperationView(StrictModel):
    operation_id: str = Field(pattern=IDENTIFIER)
    plan_id: str = Field(pattern=IDENTIFIER)
    scope_id: str = Field(min_length=1, max_length=80)
    request_key: str = Field(pattern=REQUEST_KEY)
    kind: Literal["execute"] = "execute"
    status: Literal[
        "running", "done", "rejected", "uncertain", "cancelled", "interrupted", "expired"
    ]
    intent_recorded: bool
    receipt: MissingDirectoryRestoreReceiptView | None
    error_code: str | None = Field(min_length=1, max_length=128)
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def original_operation(self) -> MissingDirectoryRestoreOperationView:
        if self.operation_id == self.plan_id:
            raise ValueError("缺失恢复执行必须是独立原操作")
        if (self.status in {"running", "done"}) != (self.error_code is None):
            raise ValueError("缺失恢复操作状态与错误不匹配")
        if self.status == "done" and (
            not self.intent_recorded or self.receipt is None or self.receipt.status != "done"
        ):
            raise ValueError("完成须有原意图和真实回执")
        if self.status == "uncertain" and not self.intent_recorded:
            raise ValueError("未知必须保留原派发意图")
        if self.receipt is not None and self.receipt.status != self.status:
            raise ValueError("原操作必须保留同一回执")
        if (
            self.created_at.utcoffset() is None
            or self.updated_at.utcoffset() is None
            or self.updated_at < self.created_at
        ):
            raise ValueError("原操作时间必须带时区且不倒退")
        return self


@dataclass(frozen=True)
class MissingDirectoryRestoreFence:
    plan_id: str
    operation_id: str
    kind: Literal["prepare", "execute", "verify"]
    token: str = field(repr=False)
    model_source: CheckpointModelSource | None = None


@dataclass(frozen=True)
class StoredMissingDirectoryRestore:
    view: MissingDirectoryRestoreView
    prepared: PreparedMissingDirectoryRestore | None = field(default=None, repr=False)
