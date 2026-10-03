"""SQLite Workflow 一致快照、作用域与 owner/revision fencing；见 ADR-0018。"""

from __future__ import annotations

import hashlib
import json
import math
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import uuid4

from pydantic import JsonValue, TypeAdapter
from sqlalchemy import or_, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.models import check_json, utc_now
from krim_agent.workflow.checkpoints import StoredWorkflowCheckpoint
from krim_agent.workflow.schema import schema_accepts
from krim_agent.workflow.types import (
    WorkflowCheckpointState,
    WorkflowError,
    WorkflowEvent,
    WorkflowResult,
    WorkflowRunCheckpoint,
)

from .database import Database, WorkflowCheckpointRow, WorkflowRequestRow


class WorkflowCheckpointError(WorkflowError):
    pass


def graph_hash(state: WorkflowRunCheckpoint) -> str:
    value = json.dumps(state.graph.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(value.encode()).hexdigest()


def validate_run(state: WorkflowRunCheckpoint) -> None:
    state.graph.validate_graph()
    nodes = state.graph.node_map()
    if set(state.node_statuses) != set(nodes):
        raise ValueError("检查点节点状态不完整")
    if set(state.node_outputs) != {
        key for key, value in state.node_statuses.items() if value == "completed"
    }:
        raise ValueError("完成节点与输出不一致")
    if state.current_node is not None and state.current_node not in nodes:
        raise ValueError("当前节点不存在")
    if state.production and any(node.developer_only for node in nodes.values()):
        raise ValueError("生产检查点包含开发节点")
    if not set(state.pause_before + state.pause_after) <= set(nodes):
        raise ValueError("暂停节点不存在")
    if not set(state.node_inputs) <= set(nodes) or not set(state.input_overrides) <= set(nodes):
        raise ValueError("检查点输入包含未知节点")
    schema_accepts(state.input_value, state.graph.derived_input_schema())
    for identifier, output in state.node_outputs.items():
        declared = {port.name: port for port in nodes[identifier].outputs()}
        if set(output.ports) - set(declared) or any(
            port.required and port.name not in output.ports for port in declared.values()
        ):
            raise ValueError("检查点输出端口不一致")
        for name, value in output.ports.items():
            schema_accepts(value, declared[name].json_schema)
    for identifier, values in {**state.node_inputs, **state.input_overrides}.items():
        declared = {port.name: port for port in nodes[identifier].inputs()}
        if set(values) - set(declared) or any(
            port.required and port.name not in values for port in declared.values()
        ):
            raise ValueError("检查点输入端口不一致")
        for name, value in values.items():
            schema_accepts(value, declared[name].json_schema)
    if state.status == "paused" and (state.current_node is None or state.paused_boundary is None):
        raise ValueError("暂停位置缺失")
    if state.result is not None and (
        state.result.run_id != state.run_id or state.result.epoch != state.epoch
    ):
        raise ValueError("终态结果不属于当前运行")
    if state.status in {"completed", "failed", "aborted"}:
        if state.result is None or state.result.status != state.status:
            raise ValueError("终态检查点缺少匹配结果")
        if state.result.node_statuses != state.node_statuses or state.result.node_outputs != {
            key: output.ports for key, output in state.node_outputs.items()
        }:
            raise ValueError("结果与节点快照不一致")
        if state.status == "completed":
            schema_accepts(state.result.output, state.graph.derived_output_schema())
    for sequence, event in enumerate(state.events):
        if event.seq != sequence or event.run_id != state.run_id or event.epoch > state.epoch:
            raise ValueError("事件序列或归属无效")


def validate_state(state: WorkflowCheckpointState) -> None:
    validate_run(state)
    if state.graph_hash != graph_hash(state):
        raise ValueError("图哈希不一致")
    identifiers = {state.run_id}
    for path, child in state.children.items():
        if child.path != path or child.run_id in identifiers:
            raise ValueError("子运行路径或 ID 冲突")
        identifiers.add(child.run_id)
        validate_run(child)


class WorkflowCheckpointStore:
    def __init__(
        self,
        database: Database,
        *,
        scope: str = "local",
        lease_seconds: float = 30,
        max_snapshot_bytes: int = 67_108_864,
    ) -> None:
        if not scope or len(scope) > 128:
            raise ValueError("Workflow store scope 无效")
        if not math.isfinite(lease_seconds) or not 0.2 <= lease_seconds <= 600:
            raise ValueError("Workflow 租约必须在 0.2–600 秒之间")
        if not 1024 <= max_snapshot_bytes <= 67_108_864:
            raise ValueError("Workflow 快照大小预算无效")
        self.database, self.scope = database, scope
        self.lease_seconds, self.max_snapshot_bytes = lease_seconds, max_snapshot_bytes

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[AsyncSession]:
        try:
            async with self.database.write_lock, self.database.sessions.begin() as session:
                yield session
        except SQLAlchemyError:
            raise WorkflowCheckpointError(
                "checkpoint_store_failed", "Workflow 检查点存储失败"
            ) from None

    def _payload(self, state: WorkflowCheckpointState) -> dict[str, JsonValue]:
        try:
            validate_state(state)
            payload = TypeAdapter(dict[str, JsonValue]).validate_json(state.model_dump_json())
            check_json(payload, max_bytes=self.max_snapshot_bytes)
            return payload
        except (ValueError, RecursionError, WorkflowError):
            raise WorkflowCheckpointError(
                "checkpoint_invalid", "Workflow 快照无效或超过预算"
            ) from None

    def _decode(self, row: WorkflowCheckpointRow) -> StoredWorkflowCheckpoint:
        if row.scope != self.scope:
            raise WorkflowCheckpointError("checkpoint_missing", "Workflow 检查点不存在")
        try:
            state = WorkflowCheckpointState.model_validate_json(json.dumps(row.payload))
            validate_state(state)
            if (
                state.run_id != row.run_id
                or state.graph_hash != row.graph_hash
                or state.status != row.status
            ):
                raise ValueError
            return StoredWorkflowCheckpoint(state, row.revision)
        except (ValueError, RecursionError, WorkflowError):
            raise WorkflowCheckpointError("checkpoint_corrupt", "Workflow 检查点无法验证") from None

    async def create(self, state: WorkflowCheckpointState, owner_id: str) -> int:
        payload = self._payload(state)
        async with self._transaction() as session:
            if await session.get(WorkflowCheckpointRow, state.run_id) is not None:
                raise WorkflowCheckpointError("checkpoint_conflict", "Workflow run_id 已存在")
            session.add(
                WorkflowCheckpointRow(
                    run_id=state.run_id,
                    scope=self.scope,
                    graph_hash=state.graph_hash,
                    status=state.status,
                    payload=payload,
                    revision=0,
                    owner_id=owner_id,
                    lease_expires_at=time.time() + self.lease_seconds,
                )
            )
        return 0

    async def api_lookup(self, key: str, request_hash: str) -> StoredWorkflowCheckpoint | None:
        async with self._transaction() as session:
            request = await session.get(WorkflowRequestRow, (self.scope, key))
            if request is None:
                return None
            if request.request_hash != request_hash:
                raise WorkflowCheckpointError(
                    "idempotency_conflict", "Workflow 请求幂等键与正文不一致"
                )
            row = await session.get(WorkflowCheckpointRow, request.run_id)
            if row is None:
                raise WorkflowCheckpointError("checkpoint_corrupt", "Workflow 请求没有匹配检查点")
            return self._decode(row)

    async def api_request(
        self, key: str, request_hash: str, state: WorkflowCheckpointState
    ) -> tuple[StoredWorkflowCheckpoint, bool]:
        payload = self._payload(state)
        async with self._transaction() as session:
            existing = await session.get(WorkflowRequestRow, (self.scope, key))
            if existing is not None:
                if existing.request_hash != request_hash:
                    raise WorkflowCheckpointError(
                        "idempotency_conflict", "Workflow 请求幂等键与正文不一致"
                    )
                row = await session.get(WorkflowCheckpointRow, existing.run_id)
                if row is None:
                    raise WorkflowCheckpointError(
                        "checkpoint_corrupt", "Workflow 请求没有匹配检查点"
                    )
                return self._decode(row), False
            row = WorkflowCheckpointRow(
                run_id=state.run_id,
                scope=self.scope,
                graph_hash=state.graph_hash,
                status=state.status,
                payload=payload,
                revision=0,
                owner_id=None,
                lease_expires_at=None,
            )
            session.add(row)
            await session.flush()
            session.add(
                WorkflowRequestRow(
                    scope=self.scope,
                    idempotency_key=key,
                    request_hash=request_hash,
                    run_id=state.run_id,
                )
            )
            return StoredWorkflowCheckpoint(state, 0), True

    async def load(self, run_id: str) -> StoredWorkflowCheckpoint:
        async with self._transaction() as session:
            row = await session.get(WorkflowCheckpointRow, run_id)
            if row is None:
                raise WorkflowCheckpointError("checkpoint_missing", "Workflow 检查点不存在")
            return self._decode(row)

    async def claim(self, run_id: str, owner_id: str) -> StoredWorkflowCheckpoint:
        async with self._transaction() as session:
            result = await session.execute(
                update(WorkflowCheckpointRow)
                .where(
                    WorkflowCheckpointRow.run_id == run_id,
                    WorkflowCheckpointRow.scope == self.scope,
                    or_(
                        WorkflowCheckpointRow.owner_id.is_(None),
                        WorkflowCheckpointRow.lease_expires_at <= time.time(),
                    ),
                )
                .values(owner_id=owner_id, lease_expires_at=time.time() + self.lease_seconds)
                .returning(WorkflowCheckpointRow.run_id)
            )
            acquired = result.scalar_one_or_none()
            row = await session.get(WorkflowCheckpointRow, run_id)
            if row is None or row.scope != self.scope:
                raise WorkflowCheckpointError("checkpoint_missing", "Workflow 检查点不存在")
            if acquired is None:
                raise WorkflowCheckpointError("checkpoint_busy", "Workflow 仍由其他有效 owner 持有")
            return self._decode(row)

    async def save(
        self, state: WorkflowCheckpointState, owner_id: str, expected_revision: int
    ) -> int:
        payload = self._payload(state)
        async with self._transaction() as session:
            # UPDATE 先取得 SQLite 写入锁，避免多个连接先读后写造成丢失更新。
            result = await session.execute(
                update(WorkflowCheckpointRow)
                .where(
                    WorkflowCheckpointRow.run_id == state.run_id,
                    WorkflowCheckpointRow.scope == self.scope,
                    WorkflowCheckpointRow.graph_hash == state.graph_hash,
                    WorkflowCheckpointRow.revision == expected_revision,
                    WorkflowCheckpointRow.owner_id == owner_id,
                    WorkflowCheckpointRow.lease_expires_at > time.time(),
                )
                .values(revision=expected_revision + 1)
                .returning(WorkflowCheckpointRow.run_id)
            )
            if result.scalar_one_or_none() is None:
                raise WorkflowCheckpointError(
                    "checkpoint_owner_lost", "Workflow owner/revision 已变化"
                )
            row = await session.get(WorkflowCheckpointRow, state.run_id)
            if row is None:
                raise WorkflowCheckpointError("checkpoint_missing", "Workflow 检查点不存在")
            previous = self._decode(row).state
            if state.epoch < previous.epoch or state.epoch > previous.epoch + 1:
                raise WorkflowCheckpointError("checkpoint_conflict", "Workflow epoch 非连续")
            if state.events[: len(previous.events)] != previous.events:
                raise WorkflowCheckpointError("checkpoint_conflict", "Workflow 已保存事件不能改写")
            for path, child in previous.children.items():
                updated = state.children.get(path)
                if updated is None or updated.events[: len(child.events)] != child.events:
                    raise WorkflowCheckpointError(
                        "checkpoint_conflict", "Workflow 子运行历史不能回退"
                    )
            row.payload = payload
            row.status = state.status
            row.updated_at = utc_now()
        return expected_revision + 1

    async def renew(self, run_id: str, owner_id: str) -> None:
        async with self._transaction() as session:
            result = await session.execute(
                update(WorkflowCheckpointRow)
                .where(
                    WorkflowCheckpointRow.run_id == run_id,
                    WorkflowCheckpointRow.scope == self.scope,
                    WorkflowCheckpointRow.owner_id == owner_id,
                    WorkflowCheckpointRow.lease_expires_at > time.time(),
                )
                .values(lease_expires_at=time.time() + self.lease_seconds)
                .returning(WorkflowCheckpointRow.run_id)
            )
            if result.scalar_one_or_none() is None:
                raise WorkflowCheckpointError("checkpoint_owner_lost", "Workflow owner 租约已失效")

    async def release(self, run_id: str, owner_id: str) -> None:
        async with self._transaction() as session:
            await session.execute(
                update(WorkflowCheckpointRow)
                .where(
                    WorkflowCheckpointRow.run_id == run_id,
                    WorkflowCheckpointRow.scope == self.scope,
                    WorkflowCheckpointRow.owner_id == owner_id,
                )
                .values(owner_id=None, lease_expires_at=None)
            )

    async def abort_cold(self, run_id: str) -> None:
        owner = str(uuid4())
        claimed = await self.claim(run_id, owner)
        state = claimed.state.model_copy(deep=True)
        state.status = "aborted"
        state.node_statuses = {
            key: "aborted" if value == "running" else value
            for key, value in state.node_statuses.items()
        }
        state.result = WorkflowResult(
            run_id=state.run_id,
            status="aborted",
            node_results={
                key: value.ports.get("out", value.ports)
                if set(value.ports) == {"out"}
                else value.ports
                for key, value in state.node_outputs.items()
            },
            node_outputs={key: value.ports for key, value in state.node_outputs.items()},
            node_statuses=state.node_statuses,
            epoch=state.epoch,
            code="cancelled",
            error="Workflow 已中止",
        )
        state.events.append(
            WorkflowEvent(
                run_id=state.run_id,
                seq=len(state.events),
                epoch=state.epoch,
                type="end",
                data={"status": "aborted", "code": "cancelled"},
            )
        )
        try:
            await self.save(state, owner, claimed.revision)
        finally:
            await self.release(run_id, owner)

    async def list(self) -> list[StoredWorkflowCheckpoint]:
        async with self._transaction() as session:
            rows = await session.scalars(
                select(WorkflowCheckpointRow)
                .where(WorkflowCheckpointRow.scope == self.scope)
                .order_by(WorkflowCheckpointRow.created_at, WorkflowCheckpointRow.run_id)
            )
            return [self._decode(row) for row in rows]
