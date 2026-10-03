"""Workflow 的一致根快照与扁平子运行日志；只依赖中立 backend。"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from typing import TYPE_CHECKING
from uuid import uuid4

from .checkpoints import StoredWorkflowCheckpoint, WorkflowCheckpointBackend
from .types import WorkflowCheckpointState, WorkflowError, WorkflowRunCheckpoint

if TYPE_CHECKING:
    from .engine import WorkflowHandle


class WorkflowJournal:
    def __init__(
        self, backend: WorkflowCheckpointBackend, *, stored: StoredWorkflowCheckpoint | None = None
    ) -> None:
        self.backend = backend
        self.owner_id = str(uuid4())
        self.revision = stored.revision if stored else None
        self.saved = stored.state.model_copy(deep=True) if stored else None
        self.root: WorkflowHandle | None = None
        self.deadline = stored.state.deadline if stored else None
        self.active = False
        self.detaching = False
        self.error: WorkflowError | None = None
        self._lock = asyncio.Lock()
        self._heartbeat: asyncio.Task[None] | None = None

    def attach(self, handle: WorkflowHandle) -> None:
        self.root = handle

    async def acquire(self) -> None:
        if self.root is None:
            raise WorkflowError("checkpoint_not_ready", "Workflow journal 尚未装配")
        if self.active:
            return
        if self.deadline is None:
            self.deadline = time.time() + self.root.engine.timeout_seconds
        if self.saved is None:
            state = self._state(self.root.checkpoint_run())
            self.revision = await self._create_or_claim(state)
            self.saved = state
        else:
            claimed = await self._claim()
            if claimed.revision != self.revision:
                await self.backend.release(self.root.run_id, self.owner_id)
                raise WorkflowError("checkpoint_conflict", "Workflow 在读取后已发生变化")
            # 同 revision 的准备状态可包含经宿主校验的恢复裁决，不能被原快照覆盖。
        self.active = True
        self._heartbeat = asyncio.create_task(
            self._renew(), name=f"workflow-lease-{self.root.run_id}"
        )

    async def _create_or_claim(self, state: WorkflowCheckpointState) -> int:
        writing = asyncio.create_task(self.backend.create(state, self.owner_id))
        try:
            return await asyncio.shield(writing)
        except asyncio.CancelledError:
            await writing
            await asyncio.shield(self.backend.release(state.run_id, self.owner_id))
            raise

    async def _claim(self) -> StoredWorkflowCheckpoint:
        if self.root is None:
            raise WorkflowError("checkpoint_not_ready", "Workflow journal 尚未装配")
        acquiring = asyncio.create_task(self.backend.claim(self.root.run_id, self.owner_id))
        try:
            return await asyncio.shield(acquiring)
        except asyncio.CancelledError:
            await acquiring
            await asyncio.shield(self.backend.release(self.root.run_id, self.owner_id))
            raise

    async def _renew(self) -> None:
        if self.root is None:
            return
        try:
            while True:
                await asyncio.sleep(self.backend.lease_seconds / 3)
                await self.backend.renew(self.root.run_id, self.owner_id)
        except asyncio.CancelledError:
            raise
        except WorkflowError as error:
            self.error = error
            if not self.root.task.done():
                self.root.task.cancel()
        except Exception:
            self.error = WorkflowError("checkpoint_store_failed", "Workflow 续租失败")
            if not self.root.task.done():
                self.root.task.cancel()

    def _state(
        self, root: WorkflowRunCheckpoint, *, child: WorkflowRunCheckpoint | None = None
    ) -> WorkflowCheckpointState:
        if self.root is None:
            raise WorkflowError("checkpoint_not_ready", "Workflow journal 尚未装配")
        children = {
            path: state.model_copy(deep=True)
            for path, state in (self.saved.children.items() if self.saved else ())
        }
        if child is not None:
            children[child.path] = child
        fields = {name: getattr(root, name) for name in WorkflowRunCheckpoint.model_fields}
        encoded = json.dumps(
            root.graph.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        )
        return WorkflowCheckpointState(
            **fields,
            graph_hash=hashlib.sha256(encoded.encode()).hexdigest(),
            budget_count=self.root.budget.count,
            deadline=self.deadline,
            children=children,
            engine_options=self.root.engine.options(),
        )

    async def persist(self, handle: WorkflowHandle) -> None:
        async with self._lock:
            await self._persist_locked(handle)

    async def _persist_locked(self, handle: WorkflowHandle) -> None:
        if not self.active or self.root is None or self.revision is None or self.saved is None:
            raise WorkflowError("checkpoint_not_ready", "Workflow 没有有效的生产租约")
        if self.error is not None:
            raise self.error
        if handle is self.root:
            state = self._state(handle.checkpoint_run())
        else:
            state = self._state(self.saved, child=handle.checkpoint_run())
        writing = asyncio.create_task(self.backend.save(state, self.owner_id, self.revision))
        try:
            self.revision = await asyncio.shield(writing)
        except asyncio.CancelledError:
            self.revision = await writing
            self.saved = state
            raise
        self.saved = state

    async def control(self, handle: WorkflowHandle) -> None:
        async with self._lock:
            if self.saved is None or self.revision is None or not self.active:
                raise WorkflowError("checkpoint_not_ready", "Workflow 没有有效的生产租约")
            current = self.saved if handle is self.root else self.saved.children[handle.path]
            updated = current.model_copy(
                update={
                    "pause_requested": handle._pause_requested,
                    "input_overrides": {
                        key: dict(value) for key, value in handle._input_overrides.items()
                    },
                },
                deep=True,
            )
            state = (
                self._state(updated)
                if handle is self.root
                else self._state(self.saved, child=updated)
            )
            writing = asyncio.create_task(self.backend.save(state, self.owner_id, self.revision))
            try:
                self.revision = await asyncio.shield(writing)
            except asyncio.CancelledError:
                self.revision = await writing
                self.saved = state
                raise
            self.saved = state

    async def close(self) -> None:
        if self.root is None or not self.active:
            return
        self.active = False
        if self._heartbeat is not None:
            self._heartbeat.cancel()
            await asyncio.gather(self._heartbeat, return_exceptions=True)
        await self.backend.release(self.root.run_id, self.owner_id)

    def child(self, path: str) -> WorkflowRunCheckpoint | None:
        return self.saved.children.get(path) if self.saved else None
