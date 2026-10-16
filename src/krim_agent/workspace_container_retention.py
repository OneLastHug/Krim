"""独立容器回收事实；原恢复终态不随回收变化，私有Stage不进入公开请求。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.models import StrictModel, check_json
from krim_agent.workspace_directory_restore import FINGERPRINT, IDENTIFIER, REQUEST_KEY
from krim_agent.workspace_directory_restore_writer import DirectoryRestoreStage
from krim_agent.workspace_tree_snapshot import _path


class ContainerRetentionSource(StrictModel):
    kind: Literal["workspace_missing_directory_restore"] = "workspace_missing_directory_restore"
    source_plan_id: str = Field(pattern=IDENTIFIER, max_length=36)
    source_operation_id: str = Field(pattern=IDENTIFIER, max_length=36)
    scope_id: str = Field(min_length=1, max_length=80)
    topic_id: str = Field(pattern=IDENTIFIER, max_length=36)
    root_path: str = Field(min_length=1, max_length=512)
    workspace_key: str = Field(min_length=1, max_length=64)
    resource_fingerprint: str = Field(pattern=FINGERPRINT)
    source_hash: str = Field(pattern=FINGERPRINT)
    published_fingerprint: str = Field(pattern=FINGERPRINT)

    @model_validator(mode="after")
    def original_path(self) -> ContainerRetentionSource:
        _path(self.root_path)
        if self.source_plan_id == self.source_operation_id:
            raise ValueError("回收须绑定原独立执行操作")
        check_json(self.model_dump(mode="json"), max_bytes=8192)
        return self


@dataclass(frozen=True)
class StoredContainerRetentionSource:
    view: ContainerRetentionSource
    stage: DirectoryRestoreStage = field(repr=False)
    metadata: dict[str, object] = field(repr=False)


class ContainerRetentionRequest(StrictModel):
    source_plan_id: str = Field(pattern=IDENTIFIER, max_length=36)
    expected_source_hash: str = Field(pattern=FINGERPRINT)
    expected_container_fingerprint: str = Field(pattern=FINGERPRINT)
    request_key: str = Field(pattern=REQUEST_KEY)

    @model_validator(mode="after")
    def bounded(self) -> ContainerRetentionRequest:
        check_json(self.model_dump(mode="json"), max_bytes=8192)
        return self


class ContainerRetentionPreview(StrictModel):
    source: ContainerRetentionSource
    status: Literal["empty", "not_empty", "absent", "unverifiable"]
    fingerprint: str | None = Field(pattern=FINGERPRINT)
    error_code: str | None = Field(min_length=1, max_length=128)
    existing_operation_id: str | None = Field(pattern=IDENTIFIER, max_length=36)

    @model_validator(mode="after")
    def observed(self) -> ContainerRetentionPreview:
        if self.status == "unverifiable":
            if self.fingerprint is not None or self.error_code is None:
                raise ValueError("未核实的容器不能伪造可确认版本")
        elif self.fingerprint is None or self.error_code is not None:
            raise ValueError("原容器观察须有实际版本与无错误")
        return self


class ContainerRetentionReceipt(StrictModel):
    outcome: Literal["removed", "already_absent", "absence_verified", "rejected", "unknown"]
    expected_fingerprint: str = Field(pattern=FINGERPRINT)
    observed_fingerprint: str | None = Field(pattern=FINGERPRINT)
    changed: bool
    recovery_required: bool
    error_code: str | None = Field(min_length=1, max_length=128)

    @model_validator(mode="after")
    def actual_effect(self) -> ContainerRetentionReceipt:
        if self.outcome in {"removed", "already_absent", "absence_verified"}:
            if (
                self.observed_fingerprint is None
                or self.recovery_required
                or self.error_code is not None
            ):
                raise ValueError("回收终态须有原位置absence且无待核实错误")
            if self.changed != (self.outcome == "removed"):
                raise ValueError("只读absence不能伪造本次物理删除")
        elif self.outcome == "unknown":
            if not self.changed or not self.recovery_required or self.error_code is None:
                raise ValueError("可能已发生的回收必须保留unknown")
        elif self.changed or self.recovery_required or self.error_code is None:
            raise ValueError("拒绝回收不得声称物理变化")
        return self


RetentionStatus = Literal["running", "done", "rejected", "unknown", "cancelled", "interrupted"]


class ContainerRetentionView(StrictModel):
    kind: Literal["workspace_empty_container_retention"] = "workspace_empty_container_retention"
    operation_id: str = Field(pattern=IDENTIFIER, max_length=36)
    source: ContainerRetentionSource
    request: ContainerRetentionRequest
    status: RetentionStatus
    revision: int = Field(ge=0, le=9007199254740991)
    intent_recorded: bool
    cancelled: bool
    receipt: ContainerRetentionReceipt | None
    verification_count: int = Field(ge=0, le=128)
    error_code: str | None = Field(min_length=1, max_length=128)
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def complete_record(self) -> ContainerRetentionView:
        if (
            self.request.source_plan_id != self.source.source_plan_id
            or self.request.expected_source_hash != self.source.source_hash
        ):
            raise ValueError("回收请求必须保留原来源")
        if self.operation_id in {self.source.source_plan_id, self.source.source_operation_id}:
            raise ValueError("回收须有独立操作标识")
        if (
            self.created_at.utcoffset() is None
            or self.updated_at.utcoffset() is None
            or self.updated_at < self.created_at
        ):
            raise ValueError("回收时间必须规范且不后退")
        if (self.status in {"rejected", "unknown", "cancelled", "interrupted"}) != (
            self.error_code is not None
        ):
            raise ValueError("回收状态与原因不一致")
        if self.status in {"done", "unknown"} and self.receipt is None:
            raise ValueError("回收完成或未知必须保留原回执")
        if self.status == "unknown" and not self.intent_recorded:
            raise ValueError("物理未知必须保留原持久派发意图")
        if self.status == "cancelled" and not self.cancelled:
            raise ValueError("取消终态必须保留取消事实")
        if self.status in {"cancelled", "interrupted"} and self.intent_recorded:
            raise ValueError("派发后取消或失主须保持unknown")
        if self.receipt is not None:
            if self.receipt.expected_fingerprint != self.request.expected_container_fingerprint:
                raise ValueError("回收回执版本与原请求不符")
            if (
                self.status == "done"
                and self.receipt.outcome not in {"removed", "already_absent", "absence_verified"}
                or self.status == "unknown"
                and self.receipt.outcome != "unknown"
                or self.status in {"rejected", "cancelled", "interrupted"}
                and self.receipt.outcome != "rejected"
            ):
                raise ValueError("回收回执不能伪造完成或未知")
            if self.receipt.outcome in {"removed", "absence_verified"} and not self.intent_recorded:
                raise ValueError("真实删除或原未知核实须保留派发意图")
            if self.receipt.outcome == "absence_verified" and self.verification_count < 1:
                raise ValueError("冷absence核实必须保留核实次数")
            if self.status != "running" and self.receipt.error_code != self.error_code:
                raise ValueError("回收回执原因与当前终态不一致")
        check_json(self.model_dump(mode="json"), max_bytes=24576)
        return self


@dataclass(frozen=True)
class ContainerRetentionFence:
    operation_id: str
    epoch: int
    token: str = field(repr=False)
    kind: Literal["remove", "verify"] = "remove"
