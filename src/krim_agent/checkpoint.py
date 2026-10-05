"""宿主资源快照合约；外部引用只透传，实际保存/回滚由可信 Python Host 完成。"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Literal

from pydantic import Field, JsonValue, model_validator

from .change_list import ChangeID, ChangeRevision
from .protocol.models import StrictModel, check_json, utc_now


class CheckpointTransport(StrictModel):
    host_session_id: ChangeID | None = None
    timeout_seconds: float = Field(default=10, gt=0, le=300)


class CheckpointCreate(ChangeRevision, CheckpointTransport):
    title: str = Field(default="资源快照", min_length=1, max_length=120)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    idempotency_key: ChangeID

    @model_validator(mode="after")
    def bounded(self) -> CheckpointCreate:
        if not self.title.strip():
            raise ValueError("快照标题不能为空白")
        check_json(self.metadata, max_bytes=8192)
        return self


class CheckpointRollback(ChangeRevision, CheckpointTransport):
    checkpoint_id: ChangeID
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    idempotency_key: ChangeID

    @model_validator(mode="after")
    def bounded(self) -> CheckpointRollback:
        check_json(self.metadata, max_bytes=8192)
        return self


class CheckpointCanRollback(ChangeRevision, CheckpointTransport):
    checkpoint_id: ChangeID
    metadata: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def bounded(self) -> CheckpointCanRollback:
        check_json(self.metadata, max_bytes=8192)
        return self


class CheckpointInvoke(CheckpointCreate):
    action: Literal["create", "rollback"]
    checkpoint_id: ChangeID | None = None

    @model_validator(mode="after")
    def target(self) -> CheckpointInvoke:
        if (self.action == "rollback") != (self.checkpoint_id is not None):
            raise ValueError("回滚必须指定 checkpoint_id，创建由 Python 分配 ID")
        return self

    def fingerprint(self) -> str:
        payload = self.model_dump(exclude={"host_session_id", "timeout_seconds"})
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


class CheckpointExecution(ChangeRevision):
    operation_id: ChangeID
    topic_id: ChangeID
    checkpoint_id: ChangeID
    action: Literal["create", "rollback"]
    title: str = Field(min_length=1, max_length=120)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    snapshot_metadata: dict[str, JsonValue] = Field(default_factory=dict)
    snapshot_ref: JsonValue = None

    @model_validator(mode="after")
    def bounded(self) -> CheckpointExecution:
        check_json(self.metadata, max_bytes=8192)
        check_json(self.snapshot_metadata, max_bytes=8192)
        check_json(self.snapshot_ref, max_bytes=4096)
        return self


class CheckpointReceipt(ChangeRevision):
    operation_id: ChangeID
    topic_id: ChangeID
    checkpoint_id: ChangeID
    action: Literal["create", "rollback"]
    status: Literal["done", "error", "uncertain"]
    snapshot_ref: JsonValue = None
    error_code: ChangeID | None = Field(default=None, max_length=64)

    @model_validator(mode="after")
    def bounded(self) -> CheckpointReceipt:
        check_json(self.snapshot_ref, max_bytes=4096)
        if (self.status == "done") != (self.error_code is None):
            raise ValueError("done 不含错误码，error/uncertain 必须包含稳定错误码")
        if self.status == "done" and (
            self.snapshot_ref is None
            or self.snapshot_ref == ""
            or self.snapshot_ref == {}
            or self.snapshot_ref == []
            or (isinstance(self.snapshot_ref, str) and not self.snapshot_ref.strip())
        ):
            raise ValueError("成功创建/回滚必须关联真实外部快照引用")
        return self

    def matches(self, execution: CheckpointExecution) -> bool:
        return (
            self.operation_id == execution.operation_id
            and self.topic_id == execution.topic_id
            and self.checkpoint_id == execution.checkpoint_id
            and self.revision == execution.revision
            and self.action == execution.action
            and (
                self.action != "rollback"
                or self.status != "done"
                or self.snapshot_ref == execution.snapshot_ref
            )
        )

    @classmethod
    def uncertain(cls, execution: CheckpointExecution, code: str) -> CheckpointReceipt:
        return cls(
            **execution.model_dump(exclude={"title", "metadata", "snapshot_metadata"}),
            status="uncertain",
            error_code=code,
        )


class HostCheckpoint(StrictModel):
    id: ChangeID
    topic_id: ChangeID
    title: str
    metadata: dict[str, JsonValue]
    snapshot_ref: JsonValue = None
    status: Literal["creating", "available", "rolling_back", "rolled_back", "failed", "uncertain"]
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class CheckpointCatalog(ChangeRevision):
    topic_id: ChangeID
    active_operation_id: ChangeID | None = None
    checkpoints: list[HostCheckpoint] = Field(default_factory=list)


class CheckpointOperation(StrictModel):
    idempotency_key: ChangeID
    execution: CheckpointExecution
    status: Literal["executing", "done", "error", "uncertain"]
    receipt: CheckpointReceipt | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class CheckpointCheck(ChangeRevision):
    topic_id: ChangeID
    checkpoint_id: ChangeID
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    snapshot_metadata: dict[str, JsonValue]
    snapshot_ref: JsonValue


class CheckpointCheckResult(ChangeRevision):
    topic_id: ChangeID
    checkpoint_id: ChangeID
    can_rollback: bool
    reason: str = Field(default="", max_length=512)


class CheckpointTopic(StrictModel):
    topic_id: ChangeID


class CheckpointCreateRequest(CheckpointCreate, CheckpointTopic):
    pass


class CheckpointRollbackRequest(CheckpointRollback, CheckpointTopic):
    pass


class CheckpointCanRollbackRequest(CheckpointCanRollback, CheckpointTopic):
    pass
