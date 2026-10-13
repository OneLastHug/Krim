"""原完成生成树根撤销独立请求/来源；不扩旧Move公开Schema。"""

from dataclasses import dataclass
from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.workspace_tree_move import (
    TreeMoveAttemptView,
    TreeMovePreviewView,
    TreeMoveQueryKind,
)
from krim_agent.workspace_tree_snapshot import _path


@dataclass(frozen=True)
class GeneratedTreeUndoSource:
    plan_id: str
    previous_restore_operation_id: str | None = None


class GeneratedTreeUndoPreviewRequest(StrictModel):
    plan_id: str = Field(min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    preserve_path: str = Field(min_length=1, max_length=512)
    expected_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[^\x00-\x1f\x7f]+$")
    ttl_seconds: float = Field(default=1800, gt=0, le=86400, allow_inf_nan=False)
    previous_restore_operation_id: str | None = Field(
        default=None, min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$"
    )

    @model_validator(mode="after")
    def canonical(self) -> "GeneratedTreeUndoPreviewRequest":
        try:
            _path(self.preserve_path)
        except ServiceError as error:
            raise ValueError(str(error)) from error
        return self


GeneratedTreeUndoActionKind = Literal["remove_root", "restore_root", "keep"]
GeneratedTreeRootState = Literal[
    "awaiting_approval",
    "removing",
    "preserved",
    "restoring",
    "restored",
    "keeping",
    "kept",
    "rejected",
    "uncertain",
    "cancelled",
]


class GeneratedTreeUndoAction(StrictModel):
    proposal_id: str = Field(min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    action: GeneratedTreeUndoActionKind
    expected_revision: int = Field(ge=0, le=9007199254740989)
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[^\x00-\x1f\x7f]+$")


class GeneratedTreeUndoCancelRequest(StrictModel):
    proposal_id: str = Field(min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")


class GeneratedTreeUndoPreviewView(StrictModel):
    source_plan_id: str
    source_command_id: str
    source_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    command_outcome: Literal["completed", "failed", "output_limit"]
    accept_failed: bool
    snapshot_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    sealed_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    proof_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    previous_restore_operation_id: str | None
    generation_status: Literal["done"] = "done"
    root_state: GeneratedTreeRootState
    move: TreeMovePreviewView

    @model_validator(mode="after")
    def original_state(self) -> "GeneratedTreeUndoPreviewView":
        expected = {
            "proposed": "awaiting_approval",
            "applying": "removing",
            "applied": "preserved",
            "undoing": "restoring",
            "undone": "restored",
            "keeping": "keeping",
            "kept": "kept",
            "rejected": "rejected",
            "uncertain": "uncertain",
            "cancelled": "cancelled",
        }
        if self.root_state != expected[self.move.status]:
            raise ValueError("根状态必须保留原Move提案事实")
        return self


class GeneratedTreeUndoAttemptView(StrictModel):
    source_plan_id: str
    action: GeneratedTreeUndoActionKind
    operation: TreeMoveAttemptView

    @model_validator(mode="after")
    def original_action(self) -> "GeneratedTreeUndoAttemptView":
        if (
            self.operation.action
            != {"remove_root": "apply", "restore_root": "undo", "keep": "keep"}[self.action]
        ):
            raise ValueError("根动作必须保留原Move操作事实")
        return self


class GeneratedTreeUndoPage(StrictModel):
    items: list[GeneratedTreeUndoPreviewView] = Field(max_length=32)
    next_after: str | None


class GeneratedTreeUndoAttemptPage(StrictModel):
    items: list[GeneratedTreeUndoAttemptView] = Field(max_length=32)
    next_after: str | None


class GeneratedTreeUndoQueryResult(StrictModel):
    kind: TreeMoveQueryKind
    proposal: GeneratedTreeUndoPreviewView | None = None
    operation: GeneratedTreeUndoAttemptView | None = None
    history: GeneratedTreeUndoAttemptPage | None = None
    proposals: GeneratedTreeUndoPage | None = None
