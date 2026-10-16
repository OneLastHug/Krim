"""原缺失基线的完整保留回退；独立于生成根来源与普通Move请求。"""

from dataclasses import dataclass
from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.models import StrictModel
from krim_agent.workspace_directory_restore import FINGERPRINT, IDENTIFIER, REQUEST_KEY
from krim_agent.workspace_missing_directory_restore import DirectoryAbsenceSummary
from krim_agent.workspace_tree_move import TreeMoveAttemptView, TreeMovePreviewView
from krim_agent.workspace_tree_snapshot import _path


@dataclass(frozen=True)
class MissingDirectoryRollbackSource:
    plan_id: str


class MissingDirectoryRollbackPreview(StrictModel):
    plan_id: str = Field(pattern=IDENTIFIER, max_length=36)
    preserve_path: str = Field(min_length=1, max_length=512)
    expected_fingerprint: str = Field(pattern=FINGERPRINT)
    request_key: str = Field(pattern=REQUEST_KEY)
    ttl_seconds: float = Field(default=1800, gt=0, le=86400, allow_inf_nan=False)

    @model_validator(mode="after")
    def original_destination(self) -> "MissingDirectoryRollbackPreview":
        _path(self.preserve_path)
        return self


MissingDirectoryRollbackActionKind = Literal["return_to_absence", "restore_tree", "keep"]
MissingDirectoryRollbackState = Literal[
    "awaiting_approval",
    "preserving",
    "preserved",
    "restoring",
    "restored",
    "keeping",
    "kept",
    "rejected",
    "uncertain",
    "cancelled",
]


class MissingDirectoryRollbackAction(StrictModel):
    proposal_id: str = Field(pattern=IDENTIFIER, max_length=36)
    action: MissingDirectoryRollbackActionKind
    expected_revision: int = Field(ge=0, le=9007199254740989)
    request_key: str = Field(pattern=REQUEST_KEY)


class MissingDirectoryRollbackView(StrictModel):
    source: Literal["workspace_missing_directory_baseline_rollback"] = (
        "workspace_missing_directory_baseline_rollback"
    )
    source_plan_id: str = Field(pattern=IDENTIFIER, max_length=36)
    source_operation_id: str = Field(pattern=IDENTIFIER, max_length=36)
    source_hash: str = Field(pattern=FINGERPRINT)
    baseline: DirectoryAbsenceSummary
    published_fingerprint: str = Field(pattern=FINGERPRINT)
    approved_fingerprint: str = Field(pattern=FINGERPRINT)
    rollback_state: MissingDirectoryRollbackState
    move: TreeMovePreviewView

    @model_validator(mode="after")
    def actual_move_state(self) -> "MissingDirectoryRollbackView":
        expected = {
            "proposed": "awaiting_approval",
            "applying": "preserving",
            "applied": "preserved",
            "undoing": "restoring",
            "undone": "restored",
            "keeping": "keeping",
            "kept": "kept",
            "rejected": "rejected",
            "uncertain": "uncertain",
            "cancelled": "cancelled",
        }
        if (
            self.rollback_state != expected[self.move.status]
            or self.approved_fingerprint != self.move.before_fingerprint
        ):
            raise ValueError("缺失回退状态/当前批准版本必须保留原Move事实")
        return self


class MissingDirectoryRollbackAttemptView(StrictModel):
    source_plan_id: str = Field(pattern=IDENTIFIER, max_length=36)
    source_operation_id: str = Field(pattern=IDENTIFIER, max_length=36)
    action: MissingDirectoryRollbackActionKind
    operation: TreeMoveAttemptView

    @model_validator(mode="after")
    def actual_action(self) -> "MissingDirectoryRollbackAttemptView":
        if (
            self.operation.action
            != {"return_to_absence": "apply", "restore_tree": "undo", "keep": "keep"}[self.action]
        ):
            raise ValueError("缺失基线动作必须保留原Move操作")
        return self


class MissingDirectoryRollbackPage(StrictModel):
    items: list[MissingDirectoryRollbackView] = Field(max_length=32)
    next_after: str | None
