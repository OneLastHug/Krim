"""显式文件/目录混合提案批次的公开模型；每个子项仍逐项复用原事务。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.models import StrictModel, check_json
from krim_agent.workspace_change import WorkspaceAttemptView, WorkspaceChangeAction
from krim_agent.workspace_directory import DirectoryAttemptView, DirectoryChangeAction

MixedBatchAction = Literal["apply", "keep", "undo"]
MixedBatchItemStatus = Literal["pending", "running", "done", "rejected", "uncertain", "skipped"]
MixedBatchStatus = Literal[
    "running", "done", "partial", "failed", "uncertain", "cancelled", "interrupted"
]


class MixedBatchTarget(StrictModel):
    source: Literal["file", "directory"]
    proposal_id: str = Field(
        min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,35}$"
    )
    expected_revision: int = Field(ge=0, le=9007199254740991)


class MixedBatchRequest(StrictModel):
    action: MixedBatchAction
    snapshot_revision: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    targets: list[MixedBatchTarget] = Field(min_length=1, max_length=32)
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[^\x00-\x1f\x7f]+$")

    @model_validator(mode="after")
    def bounded_unique(self) -> MixedBatchRequest:
        if len({(item.source, item.proposal_id) for item in self.targets}) != len(self.targets):
            raise ValueError("混合批次提案不可重复")
        check_json(self.model_dump(mode="json"), max_bytes=16384)
        return self


class MixedBatchItemView(StrictModel):
    source: Literal["file", "directory"]
    position: int = Field(ge=0, le=31)
    proposal_id: str
    expected_revision: int
    path: str
    target_path: str | None = None
    status: MixedBatchItemStatus
    operation: WorkspaceAttemptView | DirectoryAttemptView | None = None
    error_code: str | None

    @model_validator(mode="after")
    def original_operation(self) -> MixedBatchItemView:
        if self.source == "file" and self.target_path is not None:
            raise ValueError("文件批次项不能携带目录移动目标")
        if self.operation is None and self.status not in {"pending", "rejected", "skipped"}:
            raise ValueError("未关联原操作的批次项不能自报成功")
        if self.operation is not None and (
            isinstance(self.operation, DirectoryAttemptView) != (self.source == "directory")
            or self.operation.proposal_id != self.proposal_id
            or self.operation.expected_revision != self.expected_revision
            or self.operation.status != self.status
        ):
            raise ValueError("混合批次项未保留原来源/提案/修订号/状态")
        return self


class MixedBatchView(StrictModel):
    batch_id: str
    scope_id: str
    request_key: str
    snapshot_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    action: MixedBatchAction
    status: MixedBatchStatus
    producer_status: Literal["running", "finished", "cancelled", "interrupted"]
    items: list[MixedBatchItemView] = Field(min_length=1, max_length=32)
    stop_reason: str | None
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def original_items(self) -> MixedBatchView:
        if [item.position for item in self.items] != list(range(len(self.items))):
            raise ValueError("混合批次子项顺序无效")
        if len({(item.source, item.proposal_id) for item in self.items}) != len(self.items):
            raise ValueError("混合批次结果不能重复原提案")
        if any(
            item.operation is not None
            and (item.operation.scope_id != self.scope_id or item.operation.action != self.action)
            for item in self.items
        ):
            raise ValueError("混合批次子项不属于原作用域/动作")
        done = sum(item.status == "done" for item in self.items)
        expected = (
            "running"
            if self.producer_status == "running"
            else "uncertain"
            if any(item.status in {"running", "uncertain"} for item in self.items)
            else "done"
            if done == len(self.items)
            else self.producer_status
            if self.producer_status in {"cancelled", "interrupted"}
            else "partial"
            if done
            else "failed"
        )
        if self.status != expected:
            raise ValueError("混合批次汇总状态未保留原子项事实")
        return self


class MixedBatchSummary(StrictModel):
    batch_id: str
    request_key: str
    action: MixedBatchAction
    status: MixedBatchStatus
    producer_status: Literal["running", "finished", "cancelled", "interrupted"]
    snapshot_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    item_count: int = Field(ge=0, le=32)
    done_count: int = Field(ge=0, le=32)
    stop_reason: str | None
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def counts(self) -> MixedBatchSummary:
        if self.done_count > self.item_count:
            raise ValueError("混合批次成功计数不能超过原子项计数")
        return self


class MixedBatchHistoryPage(StrictModel):
    scope_id: str
    total: int = Field(ge=0, le=5000)
    items: list[MixedBatchSummary] = Field(max_length=32)
    next_cursor: str | None


class MixedBatchQuery(StrictModel):
    kind: Literal["batch", "by_key", "history"]
    batch_id: str | None = Field(
        default=None, min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,35}$"
    )
    request_key: str | None = Field(
        default=None, min_length=1, max_length=128, pattern=r"^[^\x00-\x1f\x7f]+$"
    )
    cursor: str | None = Field(default=None, max_length=1024)
    limit: int = Field(default=32, ge=1, le=32)

    @model_validator(mode="after")
    def selection(self) -> MixedBatchQuery:
        if self.kind == "batch" and (self.batch_id is None or self.request_key is not None):
            raise ValueError("批次查询须只提供batch_id")
        if self.kind == "by_key" and (self.request_key is None or self.batch_id is not None):
            raise ValueError("原键查询须只提供request_key")
        if self.kind == "history" and (self.batch_id is not None or self.request_key is not None):
            raise ValueError("历史查询只接受cursor/limit")
        if self.kind != "history" and (self.cursor is not None or self.limit != 32):
            raise ValueError("单项混合查询不接受分页参数")
        return self


class MixedBatchQueryResult(StrictModel):
    kind: Literal["batch", "by_key", "history"]
    batch: MixedBatchView | None = None
    history: MixedBatchHistoryPage | None = None

    @model_validator(mode="after")
    def one_result(self) -> MixedBatchQueryResult:
        if self.kind == "history" and (self.history is None or self.batch is not None):
            raise ValueError("历史回程须只提供原history")
        if self.kind != "history" and self.history is not None:
            raise ValueError("单批次回程不能携带history")
        if self.kind == "batch" and self.batch is None:
            raise ValueError("按ID批次回程缺少原batch")
        return self


@dataclass(frozen=True)
class MixedBatchFence:
    """可信宿主内部凭据；不属于HTTP、SDK请求或模型工具参数。"""

    batch_id: str
    position: int
    token: str
    source: Literal["file", "directory"]


@dataclass(frozen=True)
class MixedBatchDispatch:
    fence: MixedBatchFence
    request: WorkspaceChangeAction | DirectoryChangeAction


@dataclass(frozen=True)
class MixedBatchExecution:
    view: MixedBatchView
    token: str
    children: tuple[MixedBatchDispatch, ...]
