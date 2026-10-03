"""Workflow 有序调度、结构化嵌套执行、实时事件和可审计控制。"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import time
from collections.abc import AsyncGenerator, Mapping
from dataclasses import dataclass
from typing import Literal
from uuid import NAMESPACE_URL, uuid4, uuid5

from pydantic import JsonValue

from .builtins import NESTED_KINDS, PURE_KINDS, condition, input_value, run_builtin
from .checkpoints import WorkflowCheckpointBackend
from .durable import WorkflowJournal
from .runners import ExecutionContext, RunnerBinding
from .schema import schema_accepts
from .types import (
    EventType,
    NodeOutput,
    NodeStatus,
    WorkflowCheckpointState,
    WorkflowEdge,
    WorkflowError,
    WorkflowEvent,
    WorkflowGraph,
    WorkflowNode,
    WorkflowResult,
    WorkflowRunCheckpoint,
    WorkflowStatus,
)

Bindings = RunnerBinding | Mapping[str, RunnerBinding] | None
_UNSET = object()


@dataclass
class _Budget:
    count: int = 0


class WorkflowHandle:
    def __init__(
        self,
        engine: WorkflowEngine,
        graph: WorkflowGraph,
        value: JsonValue,
        runners: Bindings,
        *,
        production: bool,
        pause_before: set[str],
        pause_after: set[str],
        depth: int = 0,
        budget: _Budget | None = None,
        journal: WorkflowJournal | None = None,
        restored: WorkflowRunCheckpoint | None = None,
        path: str = "",
        run_id: str | None = None,
        replay_side_effects: bool = False,
    ) -> None:
        self.engine, self.graph, self.input_value, self.runners = engine, graph, value, runners
        self.production, self.depth = production, depth
        self.budget = budget or _Budget()
        self.run_id = restored.run_id if restored else (run_id or str(uuid4()))
        self.path = restored.path if restored else path
        self.epoch: int = (
            restored.epoch if restored else (journal.root.epoch if journal and journal.root else 0)
        )
        self.status: WorkflowStatus = restored.status if restored else "queued"
        self.events_log: list[WorkflowEvent] = list(restored.events) if restored else []
        self.node_statuses: dict[str, NodeStatus] = (
            dict(restored.node_statuses)
            if restored
            else {node.id: "pending" for node in graph.nodes}
        )
        self.node_outputs: dict[str, NodeOutput] = (
            {key: value.model_copy(deep=True) for key, value in restored.node_outputs.items()}
            if restored
            else {}
        )
        self.node_inputs: dict[str, dict[str, JsonValue]] = (
            dict(restored.node_inputs) if restored else {}
        )
        self.current_node: str | None = restored.current_node if restored else None
        self._pause_before = set(restored.pause_before) if restored else pause_before
        self._pause_after = set(restored.pause_after) if restored else pause_after
        self._pause_requested: bool = restored.pause_requested if restored else False
        self._condition = asyncio.Condition()
        self._resume_event = asyncio.Event()
        self._stop_requested = False
        self._input_overrides: dict[str, dict[str, JsonValue]] = (
            dict(restored.input_overrides) if restored else {}
        )
        self._paused_boundary: Literal["before", "after"] | None = (
            restored.paused_boundary if restored else None
        )
        self._attempts: dict[str, int] = dict(restored.attempts) if restored else {}
        self._children: set[WorkflowHandle] = set()
        self._control_lock = asyncio.Lock()
        self._result_value: WorkflowResult | None = (
            restored.result.model_copy(deep=True) if restored and restored.result else None
        )
        self._restored = restored
        self._replay_side_effects = replay_side_effects
        self._journal = journal
        self._started = asyncio.Event()
        self._group_ids = {
            node.id: [group.id for group in graph.groups if node.id in group.node_ids]
            for node in graph.nodes
        }
        if depth == 0 and journal is not None:
            journal.attach(self)
        self.task: asyncio.Task[WorkflowResult] = asyncio.create_task(
            engine._drive(self), name=f"krim-workflow-{self.run_id}"
        )

    def checkpoint_run(self) -> WorkflowRunCheckpoint:
        return WorkflowRunCheckpoint(
            run_id=self.run_id,
            path=self.path,
            graph=self.graph.model_copy(deep=True),
            input_value=self.input_value,
            production=self.production,
            status=self.status,
            epoch=self.epoch,
            node_statuses=dict(self.node_statuses),
            node_outputs={
                key: value.model_copy(deep=True) for key, value in self.node_outputs.items()
            },
            node_inputs={key: dict(value) for key, value in self.node_inputs.items()},
            current_node=self.current_node,
            paused_boundary=self._paused_boundary,
            pause_before=sorted(self._pause_before),
            pause_after=sorted(self._pause_after),
            pause_requested=self._pause_requested,
            input_overrides={key: dict(value) for key, value in self._input_overrides.items()},
            attempts=dict(self._attempts),
            events=[value.model_copy(deep=True) for value in self.events_log],
            result=self._result_value.model_copy(deep=True) if self._result_value else None,
        )

    async def suspend(self) -> WorkflowCheckpointState:
        if self.depth != 0 or self._journal is None:
            raise WorkflowError("checkpoint_required", "只有持久根运行可以移交")
        if not self.task.done():
            await self._started.wait()
            if self.task.done():
                await self.task
            self._journal.detaching = True
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
            await self._journal.close()
        stored = await self._journal.backend.load(self.run_id)
        return stored.state

    async def wait(self) -> WorkflowResult:
        try:
            return await asyncio.shield(self.task)
        except asyncio.CancelledError:
            if self.task.cancelled() and self._stop_requested:
                self.status = "aborted"
                return self.engine.snapshot(self, code="cancelled", error="Workflow 已中止")
            raise

    async def pause(self) -> None:
        if not self.task.done():
            self._pause_requested = True
            if self._journal is not None:
                await self._journal.control(self)

    async def resume(self, input_override: object = _UNSET) -> None:
        if self.status != "paused" or self.current_node is None:
            raise WorkflowError("not_paused", "Workflow 没有暂停")
        if input_override is not _UNSET:
            if self._paused_boundary != "before":
                raise WorkflowError(
                    "override_after_execution", "已经执行的节点不能覆盖输入，请使用 rerun"
                )
            node = self.graph.node_map()[self.current_node]
            replacement = self.engine.override_inputs(node, input_override)
            self.engine._validate_inputs(node, replacement)
            self._input_overrides[node.id] = replacement
        self._pause_requested = False
        if self._journal is not None:
            await self._journal.control(self)
        self._resume_event.set()

    async def stop(self) -> WorkflowResult:
        if self.task.done():
            return await self.wait()
        self._stop_requested = True
        self._resume_event.set()
        if self._started.is_set():
            self.task.cancel()
        return await self.wait()

    async def rerun(
        self,
        node_id: str,
        input_override: object = _UNSET,
        *,
        allow_side_effects: bool = False,
    ) -> WorkflowResult:
        if self._control_lock.locked():
            raise WorkflowError("workflow_busy", "已有 Workflow 控制操作")
        async with self._control_lock:
            return await self._rerun(node_id, input_override, allow_side_effects=allow_side_effects)

    async def _rerun(
        self,
        node_id: str,
        input_override: object = _UNSET,
        *,
        allow_side_effects: bool = False,
    ) -> WorkflowResult:
        if not self.task.done():
            raise WorkflowError("workflow_busy", "运行中的 Workflow 不能重跑")
        nodes = self.graph.node_map()
        if node_id not in nodes:
            raise WorkflowError("node_missing", "重跑节点不存在")
        replacement = None
        if input_override is not _UNSET:
            replacement = self.engine.override_inputs(nodes[node_id], input_override)
            self.engine._validate_inputs(nodes[node_id], replacement)
        descendants = {node_id}
        changed = True
        while changed:
            changed = False
            for edge in self.graph.edges:
                if edge.source in descendants and edge.target not in descendants:
                    descendants.add(edge.target)
                    changed = True
        if not allow_side_effects:
            for identifier in descendants:
                node = nodes[identifier]
                if node.kind in NESTED_KINDS:
                    self.engine._require_safe_graph(
                        self, WorkflowGraph.model_validate(node.config.get("graph"))
                    )
                    continue
                if node.kind not in PURE_KINDS:
                    binding = self.engine.binding(self, node)
                    if binding is None or not binding.retry_safe:
                        raise WorkflowError(
                            "rerun_requires_confirmation",
                            "重跑包含副作用或未知 runner，请显式确认",
                            node_id=identifier,
                        )
        if self._journal is not None:
            await self._journal.acquire()
            self._journal.deadline = time.time() + self.engine.timeout_seconds
        for identifier in descendants:
            self.node_outputs.pop(identifier, None)
            self.node_statuses[identifier] = "pending"
            self._attempts.pop(identifier, None)
        if input_override is not _UNSET:
            assert replacement is not None
            self._input_overrides[node_id] = replacement
        self.epoch += 1
        self.status = "queued"
        self._stop_requested = False
        self._pause_requested = False
        self._result_value = None
        self._restored = None
        invalidated: list[JsonValue] = [str(identifier) for identifier in sorted(descendants)]
        await self.engine.publish(self, "rerun", node_id, {"invalidated": invalidated})
        self.task = asyncio.create_task(
            self.engine._drive(self), name=f"krim-workflow-{self.run_id}-{self.epoch}"
        )
        return await self.wait()

    async def events(self) -> list[WorkflowEvent]:
        await self.wait()
        return [event.model_copy(deep=True) for event in self.events_log]

    async def stream_events(
        self, from_seq: int = 0, heartbeat: float = 15
    ) -> AsyncGenerator[WorkflowEvent | None, None]:
        if from_seq < 0 or from_seq > len(self.events_log):
            raise WorkflowError("invalid_cursor", "Workflow 事件游标越界")
        cursor = from_seq
        while True:
            async with self._condition:
                if (
                    cursor == len(self.events_log)
                    and not self.task.done()
                    and self.status not in {"completed", "failed", "aborted"}
                ):
                    try:
                        await asyncio.wait_for(self._condition.wait(), heartbeat)
                    except TimeoutError:
                        pass
                events = self.events_log[cursor:]
                done = self.status in {"completed", "failed", "aborted"} or self.task.done()
            for event in events:
                cursor = event.seq + 1
                yield event.model_copy(deep=True)
            if done:
                return
            if not events:
                yield None


class WorkflowEngine:
    def __init__(
        self,
        *,
        max_nodes: int = 2048,
        max_retries: int = 3,
        max_concurrency: int = 8,
        max_depth: int = 8,
        max_iterations: int = 100,
        timeout_seconds: float = 300,
    ) -> None:
        if (
            not 1 <= max_nodes <= 10000
            or not 0 <= max_retries <= 7
            or not 1 <= max_concurrency <= 64
        ):
            raise ValueError("Workflow 节点/重试/并发上限无效")
        if (
            not 1 <= max_depth <= 16
            or not 1 <= max_iterations <= 1000
            or not 0 < timeout_seconds <= 3600
        ):
            raise ValueError("Workflow 深度/迭代/超时上限无效")
        self.max_nodes, self.max_retries, self.max_concurrency = (
            max_nodes,
            max_retries,
            max_concurrency,
        )
        self.max_depth, self.max_iterations, self.timeout_seconds = (
            max_depth,
            max_iterations,
            timeout_seconds,
        )

    def options(self) -> dict[str, JsonValue]:
        return {
            "max_nodes": self.max_nodes,
            "max_retries": self.max_retries,
            "max_concurrency": self.max_concurrency,
            "max_depth": self.max_depth,
            "max_iterations": self.max_iterations,
            "timeout_seconds": self.timeout_seconds,
        }

    def execute(
        self,
        graph: WorkflowGraph,
        input_value: JsonValue,
        runner: Bindings = None,
        *,
        production: bool = True,
        pause_before: set[str] | None = None,
        pause_after: set[str] | None = None,
        checkpoint: WorkflowCheckpointBackend | None = None,
    ) -> WorkflowHandle:
        effective = graph.production_projection() if production else graph.model_copy(deep=True)
        effective.validate_graph()
        schema_accepts(input_value, effective.derived_input_schema())
        pauses = (pause_before or set()) | (pause_after or set())
        if not pauses <= set(effective.node_map()):
            raise WorkflowError("pause_node_missing", "暂停节点不存在")
        return WorkflowHandle(
            self,
            effective,
            input_value,
            runner,
            production=production,
            pause_before=set(pause_before or ()),
            pause_after=set(pause_after or ()),
            journal=WorkflowJournal(checkpoint) if checkpoint else None,
        )

    async def restore(
        self,
        backend: WorkflowCheckpointBackend,
        run_id: str,
        runner: Bindings = None,
        *,
        replay_side_effects: bool = False,
        completed_outputs: Mapping[str, NodeOutput] | None = None,
    ) -> WorkflowHandle:
        stored = await backend.load(run_id)
        state = stored.state
        if state.graph_hash != self._graph_hash(state.graph):
            raise WorkflowError("checkpoint_graph_mismatch", "Workflow 图哈希不匹配")
        if state.engine_options != self.options():
            raise WorkflowError("checkpoint_options_mismatch", "恢复引擎预算与已保存预算不一致")
        supplied = dict(completed_outputs or {})
        runs: list[WorkflowRunCheckpoint] = [state, *state.children.values()]
        for saved in runs:
            if saved.epoch != state.epoch or saved.status in {"completed", "failed", "aborted"}:
                continue
            for identifier, status in list(saved.node_statuses.items()):
                if status != "running":
                    continue
                node = saved.graph.node_map()[identifier]
                key = f"{saved.path}/{identifier}" if saved.path else identifier
                if key in supplied:
                    output = supplied.pop(key).model_copy(deep=True)
                    self._validate_outputs(node, output)
                    saved.node_outputs[identifier] = output
                    saved.node_statuses[identifier] = "completed"
                    saved.events.append(
                        WorkflowEvent(
                            run_id=saved.run_id,
                            seq=len(saved.events),
                            epoch=saved.epoch,
                            type="node_end",
                            node_id=identifier,
                            data=output.ports,
                            source="recovery",
                        )
                    )
                    continue
                binding = self._binding(runner, node, saved.path)
                if (
                    node.kind not in PURE_KINDS | NESTED_KINDS
                    and not replay_side_effects
                    and (binding is None or not binding.retry_safe)
                ):
                    raise WorkflowError(
                        "recovery_requires_confirmation",
                        "此节点的外部效果未确定，需要宿主确认重放或注入完成结果",
                        node_id=key,
                    )
                saved.node_statuses[identifier] = "pending"
                saved.input_overrides[identifier] = dict(saved.node_inputs.get(identifier, {}))
        if supplied:
            raise WorkflowError("recovery_node_missing", "完成结果只能注入当前未确定的节点")
        journal = WorkflowJournal(backend, stored=stored)
        journal.saved = state.model_copy(deep=True)
        graph = state.graph.model_copy(deep=True)
        handle = WorkflowHandle(
            self,
            graph,
            state.input_value,
            runner,
            production=state.production,
            pause_before=set(state.pause_before),
            pause_after=set(state.pause_after),
            journal=journal,
            restored=state,
            path=state.path,
            replay_side_effects=replay_side_effects,
            budget=_Budget(state.budget_count),
        )
        return handle

    @staticmethod
    def _graph_hash(graph: WorkflowGraph) -> str:
        value = json.dumps(graph.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(value.encode()).hexdigest()

    @staticmethod
    def binding(handle: WorkflowHandle, node: WorkflowNode) -> RunnerBinding | None:
        return WorkflowEngine._binding(handle.runners, node, handle.path)

    @staticmethod
    def _binding(runners: Bindings, node: WorkflowNode, path: str = "") -> RunnerBinding | None:
        if isinstance(runners, RunnerBinding):
            return runners
        if runners is None:
            return None
        return runners.get(f"{path}/{node.id}") or runners.get(node.id) or runners.get(node.kind)

    async def publish(
        self,
        handle: WorkflowHandle,
        kind: EventType,
        node_id: str | None = None,
        data: JsonValue = None,
    ) -> None:
        async with handle._condition:
            event = WorkflowEvent(
                run_id=handle.run_id,
                seq=len(handle.events_log),
                epoch=handle.epoch,
                type=kind,
                node_id=node_id,
                group_ids=handle._group_ids.get(node_id or "", []),
                data=data,
            )
            handle.events_log.append(event)
            try:
                if handle._journal is not None and handle._journal.active:
                    await handle._journal.persist(handle)
            except BaseException:
                saved = handle._journal.saved if handle._journal else None
                committed = (
                    saved.events
                    if saved is not None and handle.depth == 0
                    else saved.children[handle.path].events
                    if saved is not None and handle.path in saved.children
                    else []
                )
                if not committed or committed[-1].id != event.id:
                    handle.events_log.pop()
                raise
            handle._condition.notify_all()

    @staticmethod
    def output_value(output: NodeOutput) -> JsonValue:
        return output.ports["out"] if set(output.ports) == {"out"} else dict(output.ports)

    def snapshot(
        self, handle: WorkflowHandle, *, code: str | None = None, error: str | None = None
    ) -> WorkflowResult:
        results = {key: self.output_value(value) for key, value in handle.node_outputs.items()}
        return WorkflowResult(
            run_id=handle.run_id,
            status=handle.status,
            node_results=results,
            node_outputs={key: value.ports for key, value in handle.node_outputs.items()},
            node_statuses=handle.node_statuses,
            epoch=handle.epoch,
            error=error,
            code=code,
        ).model_copy(deep=True)

    @staticmethod
    def override_inputs(node: WorkflowNode, value: object) -> dict[str, JsonValue]:
        from pydantic import TypeAdapter

        data: JsonValue = TypeAdapter(JsonValue).validate_python(value)
        ports = node.inputs()
        if len(ports) == 1:
            return {ports[0].name: data}
        if not isinstance(data, dict):
            raise WorkflowError("input_override_type", "多端口节点覆盖输入必须为对象")
        return data

    async def _pause(
        self,
        handle: WorkflowHandle,
        node: WorkflowNode,
        boundary: Literal["before", "after"],
        inputs: dict[str, JsonValue],
    ) -> None:
        pauses = handle._pause_before if boundary == "before" else handle._pause_after
        if node.id not in pauses and not handle._pause_requested:
            return
        pauses.discard(node.id)
        handle.current_node = node.id
        handle._paused_boundary = boundary
        handle._resume_event.clear()
        handle.status = "paused"
        await self.publish(handle, "paused", node.id, {"boundary": boundary, "inputs": inputs})
        await handle._resume_event.wait()
        handle.status = "running"
        handle._paused_boundary = None
        await self.publish(handle, "resumed", node.id)

    def _is_active(self, handle: WorkflowHandle, edge: WorkflowEdge) -> bool:
        if handle.node_statuses[edge.source] != "completed":
            return False
        output = handle.node_outputs[edge.source]
        if edge.when is None:
            return True
        if edge.when == "default":
            return not any(
                candidate.source == edge.source and candidate.when == output.selection
                for candidate in handle.graph.edges
                if candidate.when not in {None, "default"}
            )
        return edge.when == output.selection

    def _inputs(
        self, handle: WorkflowHandle, node: WorkflowNode, incoming: list[WorkflowEdge]
    ) -> dict[str, JsonValue]:
        if node.id in handle._input_overrides:
            return handle._input_overrides.pop(node.id)
        ports = node.inputs()
        if not incoming:
            if not ports:
                return {}
            return (
                {ports[0].name: handle.input_value}
                if len(ports) == 1
                else dict(handle.input_value)
                if isinstance(handle.input_value, dict)
                else {}
            )
        values: dict[str, JsonValue] = {}
        for port in ports:
            active_edges = [
                edge
                for edge in incoming
                if edge.target_port == port.name and self._is_active(handle, edge)
            ]
            if not active_edges:
                if port.multiple:
                    values[port.name] = []
                continue
            items = [
                handle.node_outputs[edge.source].ports[edge.source_port] for edge in active_edges
            ]
            values[port.name] = items if port.multiple else items[0]
        return values

    async def _drive(self, handle: WorkflowHandle) -> WorkflowResult:
        if handle._restored is not None and handle._restored.status in {
            "completed",
            "failed",
            "aborted",
        }:
            if handle._result_value is None:
                raise WorkflowError("checkpoint_corrupt", "终态检查点缺少结果")
            return handle._result_value.model_copy(deep=True)
        incoming = {
            node.id: [edge for edge in handle.graph.edges if edge.target == node.id]
            for node in handle.graph.nodes
        }
        journal = handle._journal
        try:
            if journal is not None and handle.depth == 0:
                await journal.acquire()
            handle._started.set()
            if handle._stop_requested:
                raise asyncio.CancelledError
            remaining = self.timeout_seconds
            if journal is not None and journal.deadline is not None:
                remaining = max(0, journal.deadline - time.time())
            async with asyncio.timeout(remaining):
                if handle._restored is not None and handle.status == "paused":
                    handle._resume_event.clear()
                    await self.publish(
                        handle,
                        "paused",
                        handle.current_node,
                        {"boundary": handle._paused_boundary, "recovered": True},
                    )
                    await handle._resume_event.wait()
                    if handle._paused_boundary == "before" and handle.current_node is not None:
                        handle._input_overrides.setdefault(
                            handle.current_node,
                            dict(handle.node_inputs.get(handle.current_node, {})),
                        )
                    handle._paused_boundary = None
                    handle.status = "running"
                    await self.publish(handle, "resumed", handle.current_node, {"recovered": True})
                else:
                    handle.status = "running"
                    await self.publish(
                        handle,
                        "resumed" if handle._restored else "start",
                        data={"recovered": True} if handle._restored else handle.input_value,
                    )
                while any(value == "pending" for value in handle.node_statuses.values()):
                    progressed = False
                    for node in handle.graph.nodes:
                        if handle.node_statuses[node.id] != "pending":
                            continue
                        edges = incoming[node.id]
                        if any(
                            handle.node_statuses[edge.source] in {"pending", "running"}
                            for edge in edges
                        ):
                            continue
                        progressed = True
                        if edges and not any(self._is_active(handle, edge) for edge in edges):
                            handle.node_statuses[node.id] = "skipped"
                            await self.publish(
                                handle, "node_skip", node.id, {"reason": "inactive_edges"}
                            )
                            continue
                        inputs = self._inputs(handle, node, edges)
                        handle.node_inputs[node.id] = dict(inputs)
                        await self._pause(handle, node, "before", inputs)
                        if node.id in handle._input_overrides:
                            inputs = handle._input_overrides.pop(node.id)
                        self._validate_inputs(node, inputs)
                        handle.budget.count += 1
                        if handle.budget.count > self.max_nodes:
                            raise WorkflowError(
                                "node_limit", "Workflow 超过共享节点执行预算", node_id=node.id
                            )
                        handle.current_node = node.id
                        handle.node_statuses[node.id] = "running"
                        handle.node_inputs[node.id] = dict(inputs)
                        await self.publish(handle, "node_start", node.id, inputs)
                        async with asyncio.timeout(node.timeout_seconds):
                            output = await self._run_node(handle, node, inputs)
                        self._validate_outputs(node, output)
                        handle.node_outputs[node.id] = output
                        handle.node_statuses[node.id] = "completed"
                        await self.publish(handle, "node_end", node.id, output.ports)
                        await self._pause(handle, node, "after", inputs)
                    if not progressed:
                        raise WorkflowError("scheduler_stalled", "Workflow 依赖没有得到完整裁决")
                final_output = self._final_output(handle)
                schema_accepts(final_output, handle.graph.derived_output_schema())
                handle.status = "completed"
                result = self.snapshot(handle)
                result.output = final_output
                handle._result_value = result
                await self.publish(
                    handle, "end", data={"status": "completed", "output": final_output}
                )
                return result
        except asyncio.CancelledError:
            if journal is not None and (journal.detaching or journal.error is not None):
                if journal.error is not None:
                    raise journal.error from None
                raise
            handle.status = "aborted"
            if handle.current_node and handle.node_statuses[handle.current_node] == "running":
                handle.node_statuses[handle.current_node] = "aborted"
            result = self.snapshot(handle, code="cancelled", error="Workflow 已中止")
            handle._result_value = result
            await self.publish(handle, "end", data={"status": "aborted", "code": "cancelled"})
            return result
        except Exception as error:
            handle.status = "failed"
            if isinstance(error, WorkflowError) and error.code.startswith("checkpoint_"):
                raise
            code = (
                error.code
                if isinstance(error, WorkflowError)
                else "timeout"
                if isinstance(error, TimeoutError)
                else "runner_error"
            )
            if handle.current_node and handle.node_statuses[handle.current_node] == "running":
                handle.node_statuses[handle.current_node] = "failed"
            message = str(error) or "Workflow 超过时间预算"
            result = self.snapshot(handle, code=code, error=message)
            handle._result_value = result
            await self.publish(
                handle, "error", handle.current_node, {"code": code, "message": message}
            )
            await self.publish(handle, "end", data={"status": "failed", "code": code})
            return result
        finally:
            handle._started.set()
            for child in list(handle._children):
                try:
                    if not child.task.done():
                        if journal is not None and journal.detaching:
                            child.task.cancel()
                            await asyncio.gather(child.task, return_exceptions=True)
                        else:
                            await child.stop()
                finally:
                    handle._children.discard(child)
            if journal is not None and handle.depth == 0:
                await journal.close()

    @staticmethod
    def _validate_inputs(node: WorkflowNode, values: dict[str, JsonValue]) -> None:
        ports = {port.name: port for port in node.inputs()}
        if set(values) - set(ports):
            raise WorkflowError("input_port_unknown", "节点输入包含未知端口", node_id=node.id)
        for name, port in ports.items():
            if name not in values:
                if port.required:
                    raise WorkflowError(
                        "input_port_required", f"缺少输入端口: {name}", node_id=node.id
                    )
            else:
                schema_accepts(values[name], port.json_schema, node_id=node.id)

    @staticmethod
    def _validate_outputs(node: WorkflowNode, output: NodeOutput) -> None:
        ports = {port.name: port for port in node.outputs()}
        if set(output.ports) - set(ports):
            raise WorkflowError("output_port_unknown", "runner 返回未知输出端口", node_id=node.id)
        for name, port in ports.items():
            if name not in output.ports:
                if port.required:
                    raise WorkflowError(
                        "output_port_required", f"缺少输出端口: {name}", node_id=node.id
                    )
            else:
                schema_accepts(output.ports[name], port.json_schema, node_id=node.id)

    def _final_output(self, handle: WorkflowHandle) -> JsonValue:
        if handle.graph.output_node is not None:
            identifier = handle.graph.output_node
            if identifier not in handle.node_outputs:
                raise WorkflowError("output_skipped", "指定输出节点被跳过", node_id=identifier)
            return self.output_value(handle.node_outputs[identifier])
        sinks = [
            node
            for node in handle.graph.nodes
            if not any(edge.source == node.id for edge in handle.graph.edges)
            and node.kind not in {"image_preview", "model_preview"}
            and node.id in handle.node_outputs
        ]
        if len(sinks) == 1:
            return self.output_value(handle.node_outputs[sinks[0].id])
        return {node.id: self.output_value(handle.node_outputs[node.id]) for node in sinks}

    async def _run_node(
        self, handle: WorkflowHandle, node: WorkflowNode, inputs: dict[str, JsonValue]
    ) -> NodeOutput:
        if node.kind in PURE_KINDS:
            return run_builtin(node, inputs, handle.input_value)
        if node.kind in NESTED_KINDS:
            return await self._nested(handle, node, inputs)
        binding = self.binding(handle, node)
        if binding is None:
            raise WorkflowError("runner_required", "节点没有可用的可信 runner", node_id=node.id)
        if node.kind == "code" and not binding.allow_code:
            raise WorkflowError(
                "code_unavailable", "code 需要宿主明确注入受控执行器", node_id=node.id
            )
        if node.retry.max_attempts > self.max_retries + 1:
            raise WorkflowError("retry_limit", "配置重试次数超过引擎上限", node_id=node.id)
        if node.retry.max_attempts > 1 and not binding.retry_safe:
            raise WorkflowError("retry_unsafe", "runner 未声明可安全重试", node_id=node.id)
        for attempt in range(handle._attempts.get(node.id, 1), node.retry.max_attempts + 1):
            try:
                handle._attempts[node.id] = attempt
                if handle._journal is not None:
                    await handle._journal.persist(handle)
                context = ExecutionContext(
                    handle.run_id, node.id, attempt, handle.path, handle.epoch
                )
                value = binding.execute(node, inputs, context)
                resolved = await value if inspect.isawaitable(value) else value
                output = (
                    resolved
                    if isinstance(resolved, NodeOutput)
                    else NodeOutput(ports={"out": resolved})
                )
                if node.kind == "classifier" and output.selection is None:
                    classified = output.ports.get("out")
                    if not isinstance(classified, str):
                        raise WorkflowError(
                            "classifier_output", "classifier 必须返回字符串标签", node_id=node.id
                        )
                    output.selection = classified
                self._validate_outputs(node, output)
                return output
            except WorkflowError:
                raise
            except Exception:
                if attempt == node.retry.max_attempts:
                    raise
                handle._attempts[node.id] = attempt + 1
                await self.publish(handle, "node_retry", node.id, {"attempt": attempt})
                if node.retry.backoff_seconds:
                    await asyncio.sleep(node.retry.backoff_seconds)
        raise WorkflowError("runner_error", "没有成功的节点尝试", node_id=node.id)

    async def _nested(
        self, handle: WorkflowHandle, node: WorkflowNode, inputs: dict[str, JsonValue]
    ) -> NodeOutput:
        if handle.depth >= self.max_depth:
            raise WorkflowError("depth_limit", "嵌套图超过深度预算", node_id=node.id)
        graph = WorkflowGraph.model_validate(node.config.get("graph"))
        if handle.production:
            graph = graph.production_projection()
        graph.validate_graph()
        value = input_value(inputs)

        async def child_run(item: JsonValue, index: int) -> JsonValue:
            schema_accepts(item, graph.input_schema)
            segment = f"{handle.epoch}/{node.id}/{index}"
            path = f"{handle.path}/{segment}" if handle.path else segment
            saved = handle._journal.child(path) if handle._journal else None
            if saved is not None and (
                saved.graph != graph
                or saved.input_value != item
                or saved.production != handle.production
            ):
                raise WorkflowError(
                    "checkpoint_child_mismatch", "嵌套运行图/输入与已保存状态不一致"
                )
            child = WorkflowHandle(
                self,
                graph.model_copy(deep=True),
                item,
                handle.runners,
                production=handle.production,
                pause_before=set(),
                pause_after=set(),
                depth=handle.depth + 1,
                budget=handle.budget,
                journal=handle._journal,
                restored=saved,
                path=path,
                run_id=str(uuid5(NAMESPACE_URL, f"{handle.run_id}/{segment}")),
                replay_side_effects=handle._replay_side_effects,
            )
            handle._children.add(child)
            try:
                result = await child.wait()
                if result.status != "completed":
                    raise WorkflowError(
                        result.code or "nested_failed",
                        result.error or "嵌套图执行失败",
                        node_id=node.id,
                    )
                return result.output
            finally:
                try:
                    if not child.task.done():
                        if handle._journal and handle._journal.detaching:
                            child.task.cancel()
                            await asyncio.gather(child.task, return_exceptions=True)
                        else:
                            await child.stop()
                finally:
                    handle._children.discard(child)

        if node.kind == "sub_workflow":
            return NodeOutput(ports={"out": await child_run(value, 0)})
        if node.kind == "fanout":
            if not isinstance(value, list) or len(value) > self.max_iterations:
                raise WorkflowError(
                    "fanout_input", "fanout 输入必须是预算范围内数组", node_id=node.id
                )
            semaphore = asyncio.Semaphore(self.max_concurrency)

            async def bounded(item: JsonValue, index: int) -> JsonValue:
                async with semaphore:
                    return await child_run(item, index)

            tasks = [asyncio.create_task(bounded(item, index)) for index, item in enumerate(value)]
            try:
                results = await asyncio.gather(*tasks)
                return NodeOutput(ports={"out": list(results)})
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        if node.kind == "loop":
            iterations = node.config.get("max_iterations", self.max_iterations)
            if (
                not isinstance(iterations, int)
                or isinstance(iterations, bool)
                or not 1 <= iterations <= self.max_iterations
            ):
                raise WorkflowError("iteration_limit", "loop 迭代配置超过预算", node_id=node.id)
            until = node.config.get("until")
            if until is not None and not isinstance(until, dict):
                raise WorkflowError("invalid_config", "loop.until 必须为条件对象", node_id=node.id)
            for index in range(iterations):
                value = await child_run(value, index)
                if isinstance(until, dict) and condition(value, until):
                    return NodeOutput(ports={"out": value})
            if until is not None:
                raise WorkflowError(
                    "iteration_limit", "loop 未在预算内达到终止条件", node_id=node.id
                )
            return NodeOutput(ports={"out": value})
        attempts = node.config.get("max_attempts", node.retry.max_attempts)
        if (
            not isinstance(attempts, int)
            or isinstance(attempts, bool)
            or not 1 <= attempts <= self.max_retries + 1
        ):
            raise WorkflowError("retry_limit", "retry 图次数超过引擎预算", node_id=node.id)
        if attempts > 1:
            self._require_safe_graph(handle, graph)
        for attempt in range(1, attempts + 1):
            try:
                return NodeOutput(ports={"out": await child_run(value, attempt - 1)})
            except WorkflowError as error:
                if error.code not in {"runner_error", "timeout"} or attempt == attempts:
                    raise
                await self.publish(handle, "node_retry", node.id, {"attempt": attempt})
        raise WorkflowError("runner_error", "retry 图没有成功结果", node_id=node.id)

    def _require_safe_graph(self, handle: WorkflowHandle, graph: WorkflowGraph) -> None:
        for candidate in graph.nodes:
            if candidate.kind in PURE_KINDS:
                continue
            if candidate.kind in NESTED_KINDS:
                self._require_safe_graph(
                    handle, WorkflowGraph.model_validate(candidate.config.get("graph"))
                )
                continue
            binding = self.binding(handle, candidate)
            if binding is None or not binding.retry_safe:
                raise WorkflowError(
                    "retry_unsafe", "retry 子图含副作用或未声明安全的 runner", node_id=candidate.id
                )
