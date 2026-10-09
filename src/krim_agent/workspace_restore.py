"""检查点恢复计划合约；旧workspace动作和输出Schema保持原样。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pydantic import Field

from krim_agent.protocol.models import StrictModel
from krim_agent.workspace_change import (
    WorkspaceAttemptView,
    WorkspaceChangeAction,
    WorkspaceChangeView,
)


class CheckpointRestorePreview(StrictModel):
    checkpoint_id: str = Field(min_length=1, max_length=36)
    request_key: str = Field(min_length=1, max_length=128)
    ttl_seconds: float = Field(default=3600, gt=0, le=86400)


class CheckpointRestoreExecute(StrictModel):
    plan_id: str = Field(min_length=1, max_length=36)
    expected_revision: int = Field(ge=0, le=9007199254740991)
    request_key: str = Field(min_length=1, max_length=128)


class CheckpointRestoreItem(StrictModel):
    position: int
    path: str
    proposal: WorkspaceChangeView | None
    operation: WorkspaceAttemptView | None
    status: Literal["pending", "running", "done", "rejected", "uncertain", "skipped"]
    current_mode: int | None
    target_mode: int | None
    error_code: str | None


class CheckpointRestoreView(StrictModel):
    source: Literal["workspace"] = "workspace"
    plan_id: str
    scope_id: str
    checkpoint_id: str
    baseline_checkpoint_id: str | None
    request_key: str
    execute_key: str | None
    revision: int
    phase: Literal[
        "preparing",
        "ready",
        "executing",
        "finished",
        "failed",
        "cancelled",
        "interrupted",
        "expired",
    ]
    status: Literal[
        "preparing",
        "ready",
        "running",
        "done",
        "partial",
        "failed",
        "cancelled",
        "interrupted",
        "expired",
        "uncertain",
    ]
    items: list[CheckpointRestoreItem] = Field(min_length=1, max_length=32)
    expires_at: float
    error_code: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class CheckpointRestoreFence:
    plan_id: str
    position: int
    token: str


@dataclass(frozen=True)
class CheckpointRestoreDispatch:
    fence: CheckpointRestoreFence
    request: WorkspaceChangeAction


@dataclass(frozen=True)
class CheckpointRestorePreparation:
    view: CheckpointRestoreView
    token: str
    baseline_key: str
