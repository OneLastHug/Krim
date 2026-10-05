"""Workflow 持久运行组合层；图节点权限与 runner 由可信宿主注入。"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncGenerator
from uuid import uuid4

from krim_agent.persistence import Database, WorkflowCheckpointStore
from krim_agent.workflow import WorkflowEngine, WorkflowEvent, WorkflowHandle
from krim_agent.workflow.api import (
    WorkflowRerunRequest,
    WorkflowRestoreRequest,
    WorkflowResumeRequest,
    WorkflowRunView,
    WorkflowStartRequest,
    checkpoint_view,
)
from krim_agent.workflow.durable import WorkflowJournal
from krim_agent.workflow.types import WorkflowCheckpointState, WorkflowError, WorkflowGraph

from .workflow import WorkflowServices


def digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class WorkflowRuntime:
    def __init__(
        self,
        database: Database,
        *,
        scope: str = "api",
        services: WorkflowServices | None = None,
        max_active_runs: int = 16,
    ) -> None:
        self.database = database
        self.store = WorkflowCheckpointStore(database, scope=scope)
        self.services = services or WorkflowServices()
        self.engine = WorkflowEngine(tracer=self.services.tracer)
        self.max_active_runs = max_active_runs
        self._handles: dict[str, WorkflowHandle] = {}
        self._lock = asyncio.Lock()
        self._closed = False

    async def start(self, request: WorkflowStartRequest) -> WorkflowRunView:
        graph = (
            request.graph.production_projection()
            if request.production
            else request.graph.model_copy(deep=True)
        )
        graph.validate_graph()
        state = WorkflowCheckpointState(
            run_id=str(uuid4()),
            path="",
            graph=graph,
            input_value=request.input_value,
            production=request.production,
            status="queued",
            node_statuses={node.id: "pending" for node in graph.nodes},
            pause_before=list(request.pause_before),
            pause_after=list(request.pause_after),
            graph_hash=digest(graph.model_dump(mode="json")),
            engine_options=self.engine.options(),
        )
        async with self._lock:
            if self._closed:
                raise WorkflowError("workflow_closed", "WorkflowRuntime 已关闭")
            existing = await self.store.api_lookup(
                request.idempotency_key, digest(request.model_dump(mode="json"))
            )
            if existing is not None:
                return checkpoint_view(existing.state)
            bindings = self.services.bindings(graph)
            if (
                sum(not handle.task.done() for handle in self._handles.values())
                >= self.max_active_runs
            ):
                # 已存在的幂等请求仍可读取，容量只限制新增生产者。
                existing = await self.store.api_lookup(
                    request.idempotency_key, digest(request.model_dump(mode="json"))
                )
                if existing is not None:
                    return checkpoint_view(existing.state)
                raise WorkflowError("workflow_limit", "活跃 Workflow 数量超过限制")
            stored, created = await self.store.api_request(
                request.idempotency_key, digest(request.model_dump(mode="json")), state
            )
            if not created:
                return checkpoint_view(stored.state)
            journal = WorkflowJournal(self.store, stored=stored)
            handle = self.engine.execute(
                graph,
                request.input_value,
                bindings,
                production=request.production,
                pause_before=set(request.pause_before),
                pause_after=set(request.pause_after),
                journal=journal,
                run_id=state.run_id,
            )
            self._handles[handle.run_id] = handle
        await handle.wait_started()
        return await self.get(handle.run_id)

    async def state(self, run_id: str) -> WorkflowCheckpointState:
        return (await self.store.load(run_id)).state

    async def get(self, run_id: str) -> WorkflowRunView:
        return checkpoint_view(await self.state(run_id))

    async def graph(self, run_id: str) -> WorkflowGraph:
        return (await self.state(run_id)).graph.model_copy(deep=True)

    async def list(self) -> list[WorkflowRunView]:
        return [checkpoint_view(item.state) for item in await self.store.list()]

    async def restore(self, run_id: str, request: WorkflowRestoreRequest) -> WorkflowRunView:
        async with self._lock:
            if self._closed:
                raise WorkflowError("workflow_closed", "WorkflowRuntime 已关闭")
            existing = self._handles.get(run_id)
            if existing is not None:
                return await self.get(run_id)
            state = await self.state(run_id)
            handle = await self.engine.restore(
                self.store,
                run_id,
                self.services.bindings(state.graph),
                replay_side_effects=request.replay_side_effects,
                completed_outputs=request.completed_outputs,
            )
            self._handles[run_id] = handle
        if state.status in {"completed", "failed", "aborted"}:
            await handle.wait()
        else:
            await handle.wait_started()
        return await self.get(run_id)

    async def _handle(self, run_id: str) -> WorkflowHandle:
        async with self._lock:
            handle = self._handles.get(run_id)
        if handle is None:
            raise WorkflowError("workflow_restore_required", "运行不在本进程，请显式 restore")
        return handle

    async def pause(self, run_id: str) -> WorkflowRunView:
        handle = await self._handle(run_id)
        await handle.pause()
        return await self.get(run_id)

    async def resume(self, run_id: str, request: WorkflowResumeRequest) -> WorkflowRunView:
        handle = await self._handle(run_id)
        if request.has_input_override:
            await handle.resume(request.input_override)
        else:
            await handle.resume()
        return await self.get(run_id)

    async def stop(self, run_id: str) -> WorkflowRunView:
        async with self._lock:
            handle = self._handles.get(run_id)
            if handle is not None:
                await handle.stop()
                return await self.get(run_id)
            state = await self.state(run_id)
            if state.status in {"completed", "failed", "aborted"}:
                return checkpoint_view(state)
            await self.store.abort_cold(run_id)
        return await self.get(run_id)

    async def rerun(self, run_id: str, request: WorkflowRerunRequest) -> WorkflowRunView:
        handle = await self._handle(run_id)
        if handle.status in {"completed", "failed", "aborted"}:
            await handle.wait()
        if request.has_input_override:
            await handle.rerun(
                request.node_id,
                request.input_override,
                allow_side_effects=request.allow_side_effects,
            )
        else:
            await handle.rerun(request.node_id, allow_side_effects=request.allow_side_effects)
        return await self.get(run_id)

    async def validate_cursor(self, run_id: str, from_seq: int) -> WorkflowCheckpointState:
        state = await self.state(run_id)
        if from_seq < 0 or from_seq > len(state.events):
            raise WorkflowError("invalid_cursor", "Workflow 事件游标越界")
        return state

    async def events(
        self, run_id: str, from_seq: int = 0
    ) -> AsyncGenerator[WorkflowEvent | None, None]:
        state = await self.validate_cursor(run_id, from_seq)
        async with self._lock:
            handle = self._handles.get(run_id)
        if handle is not None:
            async for event in handle.stream_events(from_seq=from_seq, heartbeat=15):
                yield event
            return
        # 冷读取只重放，不恢复生产、工具或模型。
        for event in state.events[from_seq:]:
            yield event

    async def close(self) -> None:
        async with self._lock:
            self._closed = True
            handles = list(self._handles.values())
            self._handles.clear()
        for handle in handles:
            if not handle.task.done():
                await handle.suspend()
        if self.services.tools is not None:
            await self.services.tools.close()
