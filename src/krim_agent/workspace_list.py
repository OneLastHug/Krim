"""原生工作区清单与批次；独立于通用Host清单和原文件工具合约。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.models import StrictModel, check_json
from krim_agent.workspace_change import SHA256, WorkspaceAttemptView, WorkspaceChangeAction

NativeListFilter = Literal["active", "history", "all"]


class NativeChangeItem(StrictModel):
    proposal_id: str
    revision: int = Field(ge=0, le=9007199254740991)
    path: str
    operation: Literal["write", "replace", "delete"]
    status: Literal["applying", "applied", "undoing", "undone", "keeping", "kept", "uncertain"]
    workspace_key: str
    resource_fingerprint: str
    apply_sequence: int | None = Field(ge=1, le=9007199254740991)
    order_known: bool
    active_operation_id: str | None


class NativeChangePage(StrictModel):
    source: Literal["workspace"] = "workspace"
    scope_id: str
    snapshot_revision: str = Field(pattern=SHA256)
    filter: NativeListFilter
    total: int = Field(ge=0)
    items: list[NativeChangeItem] = Field(max_length=32)
    next_cursor: str | None


class NativeBatchTarget(StrictModel):
    proposal_id: str = Field(min_length=1, max_length=36)
    expected_revision: int = Field(ge=0, le=9007199254740991)


class NativeBatchRequest(StrictModel):
    action: Literal["keep", "undo"]
    snapshot_revision: str = Field(pattern=SHA256)
    targets: list[NativeBatchTarget] = Field(min_length=1, max_length=32)
    request_key: str = Field(min_length=1, max_length=128)

    @model_validator(mode="after")
    def bounded_unique(self) -> NativeBatchRequest:
        if len({item.proposal_id for item in self.targets}) != len(self.targets):
            raise ValueError("批次提案不可重复")
        if any(ord(value) < 32 for value in self.request_key):
            raise ValueError("批次幂等键不能包含控制字符")
        check_json(self.model_dump(mode="json"), max_bytes=16384)
        return self


class NativeBatchItemView(StrictModel):
    position: int = Field(ge=0, le=31)
    proposal_id: str
    expected_revision: int
    path: str
    apply_sequence: int | None
    status: Literal["pending", "running", "done", "rejected", "uncertain", "skipped"]
    operation: WorkspaceAttemptView | None
    error_code: str | None


class NativeBatchView(StrictModel):
    source: Literal["workspace"] = "workspace"
    batch_id: str
    scope_id: str
    request_key: str
    snapshot_revision: str
    action: Literal["keep", "undo"]
    status: Literal["running", "done", "partial", "failed", "uncertain", "cancelled", "interrupted"]
    producer_status: Literal["running", "finished", "cancelled", "interrupted"]
    items: list[NativeBatchItemView] = Field(min_length=1, max_length=32)
    stop_reason: str | None
    created_at: datetime
    updated_at: datetime


class NativeBatchSummary(StrictModel):
    """批次历史的轻量摘要；不包含私有快照、子项或文件内容。"""

    batch_id: str
    request_key: str
    action: Literal["keep", "undo"]
    status: Literal["running", "done", "partial", "failed", "uncertain", "cancelled", "interrupted"]
    producer_status: Literal["running", "finished", "cancelled", "interrupted"]
    snapshot_revision: str = Field(pattern=SHA256)
    item_count: int = Field(ge=0, le=32)
    done_count: int = Field(ge=0, le=32)
    stop_reason: str | None
    created_at: datetime
    updated_at: datetime


class NativeBatchHistoryPage(StrictModel):
    source: Literal["workspace"] = "workspace"
    scope_id: str
    total: int = Field(ge=0, le=5000)
    items: list[NativeBatchSummary] = Field(max_length=32)
    next_cursor: str | None


@dataclass(frozen=True)
class NativeBatchFence:
    """可信编排私有凭据；不属于公开请求或工具Schema。"""

    batch_id: str
    position: int
    token: str


@dataclass(frozen=True)
class NativeBatchDispatch:
    fence: NativeBatchFence
    request: WorkspaceChangeAction
    file_key: str


@dataclass(frozen=True)
class NativeBatchExecution:
    view: NativeBatchView
    token: str
    children: tuple[NativeBatchDispatch, ...]
