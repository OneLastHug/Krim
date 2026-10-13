"""非空树移动公开原事实与内部模型来源；私有快照不进入请求/回程。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.workspace_tree_snapshot import _path
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter

TreeMoveStatus = Literal[
    "proposed",
    "applying",
    "applied",
    "undoing",
    "undone",
    "keeping",
    "kept",
    "rejected",
    "uncertain",
    "cancelled",
]
TreeMoveActionKind = Literal["apply", "undo", "keep"]
TreeMoveModelKind = Literal["preview", "apply", "undo", "keep"]


class TreeMovePreviewRequest(StrictModel):
    source_path: str = Field(min_length=1, max_length=512)
    target_path: str = Field(min_length=1, max_length=512)
    expected_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[^\x00-\x1f\x7f]+$")
    ttl_seconds: float = Field(default=1800, gt=0, le=86400, allow_inf_nan=False)

    @model_validator(mode="after")
    def paths(self) -> TreeMovePreviewRequest:
        try:
            WorkspaceTreeWriter._paths(self.source_path, self.target_path)
        except ServiceError as error:
            raise ValueError(str(error)) from error
        return self


class TreeMoveAction(StrictModel):
    proposal_id: str = Field(min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    action: TreeMoveActionKind
    expected_revision: int = Field(ge=0, le=9007199254740989)
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[^\x00-\x1f\x7f]+$")


class TreeMoveInspectRequest(StrictModel):
    path: str = Field(min_length=1, max_length=512)

    @model_validator(mode="after")
    def canonical(self) -> TreeMoveInspectRequest:
        try:
            _path(self.path)
        except ServiceError as error:
            raise ValueError(str(error)) from error
        return self


class TreeMoveInspectView(StrictModel):
    path: str
    found: bool
    fingerprint: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    entry_count: int = Field(default=0, ge=0, le=16384)
    directory_count: int = Field(default=0, ge=0, le=16384)
    file_count: int = Field(default=0, ge=0, le=16384)
    byte_size: int = Field(default=0, ge=0, le=134217728)
    mode: int | None = Field(default=None, ge=0, le=511)


class TreeMoveOperationRequest(StrictModel):
    operation_id: str = Field(min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")


class TreeMovePreviewView(StrictModel):
    proposal_id: str
    scope_id: str
    workspace_key: str
    resource_fingerprint: str
    source_path: str
    target_path: str
    before_fingerprint: str
    entry_count: int = Field(ge=1, le=16384)
    byte_size: int = Field(ge=0, le=134217728)
    revision: int = Field(ge=0, le=9007199254740991)
    status: TreeMoveStatus
    expires_at: float = Field(allow_inf_nan=False)
    active_operation_id: str | None
    created_at: datetime
    updated_at: datetime


class TreeMoveReceiptView(StrictModel):
    source_path: str
    target_path: str
    status: Literal["done", "rejected", "uncertain"]
    before_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    after_fingerprint: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    changed: bool
    error_code: str | None
    recovery_required: bool


class TreeMoveAttemptView(StrictModel):
    operation_id: str
    proposal_id: str
    scope_id: str
    action: TreeMoveActionKind
    expected_revision: int = Field(ge=0, le=9007199254740989)
    status: Literal["running", "done", "rejected", "uncertain"]
    receipt: TreeMoveReceiptView | None
    cancelled: bool
    recovery_required: bool
    created_at: datetime
    updated_at: datetime


class TreeMovePage(StrictModel):
    items: list[TreeMovePreviewView] = Field(max_length=32)
    next_after: str | None


class TreeMoveAttemptPage(StrictModel):
    items: list[TreeMoveAttemptView] = Field(max_length=32)
    next_after: str | None


TreeMoveQueryKind = Literal[
    "proposal", "operation", "by_key", "operation_by_key", "history", "proposals"
]


class TreeMoveQueryRequest(StrictModel):
    kind: TreeMoveQueryKind
    identifier: str | None = Field(
        default=None, min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$"
    )
    request_key: str | None = Field(
        default=None, min_length=1, max_length=128, pattern=r"^[^\x00-\x1f\x7f]+$"
    )
    after: str | None = Field(
        default=None, min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$"
    )
    limit: int = Field(default=32, ge=1, le=32)

    @model_validator(mode="after")
    def selection(self) -> TreeMoveQueryRequest:
        if self.kind in {"proposal", "operation", "history"}:
            if self.identifier is None or self.request_key is not None:
                raise ValueError("原树查询须只提供原identifier")
        elif self.kind in {"by_key", "operation_by_key"}:
            if self.request_key is None or self.identifier is not None:
                raise ValueError("原键查询须只提供request_key")
        elif self.identifier is not None or self.request_key is not None:
            raise ValueError("原树列表只接受分页")
        if self.kind not in {"history", "proposals"} and (
            self.after is not None or self.limit != 32
        ):
            raise ValueError("原树单项查询不接受分页参数")
        return self


class TreeMoveQueryResult(StrictModel):
    kind: TreeMoveQueryKind
    proposal: TreeMovePreviewView | None = None
    operation: TreeMoveAttemptView | None = None
    history: TreeMoveAttemptPage | None = None
    proposals: TreeMovePage | None = None


@dataclass(frozen=True)
class TreeMoveModelSource:
    run_id: str
    call_id: str
