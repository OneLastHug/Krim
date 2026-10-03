"""Workflow 的中立检查点接口；不依赖数据库或宿主运行时。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .types import WorkflowCheckpointState


@dataclass(frozen=True)
class StoredWorkflowCheckpoint:
    state: WorkflowCheckpointState
    revision: int


class WorkflowCheckpointBackend(Protocol):
    lease_seconds: float

    async def create(self, state: WorkflowCheckpointState, owner_id: str) -> int: ...
    async def load(self, run_id: str) -> StoredWorkflowCheckpoint: ...
    async def claim(self, run_id: str, owner_id: str) -> StoredWorkflowCheckpoint: ...
    async def save(
        self, state: WorkflowCheckpointState, owner_id: str, expected_revision: int
    ) -> int: ...
    async def renew(self, run_id: str, owner_id: str) -> None: ...
    async def release(self, run_id: str, owner_id: str) -> None: ...
