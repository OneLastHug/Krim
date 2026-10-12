"""非空树移动公开原事实与内部模型来源；私有快照不进入请求/回程。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
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


@dataclass(frozen=True)
class TreeMoveModelSource:
    run_id: str
    call_id: str
