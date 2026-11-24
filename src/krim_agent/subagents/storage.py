"""子代理持久化仓库：SubagentStore(store) 以 JSON 兼容 dict 暴露树/成员/任务操作。

所有写路径走 store.work_write（写异常经 confirm 核实，未确认则全 store 不健康并
返回 storage_unavailable）；读路径走 store.work_read。事务内不等待模型、沙箱或
网络。幂等约定：同 request_key 相同内容返回原回执（created=False），同键不同
内容拒绝 key_conflict；CAS 失败一律 revision_conflict。
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from sqlalchemy import delete, exists, func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import TERMINAL as TERMINAL_RUN_STATUS
from krim_agent.contracts import RunView, ServiceError, fingerprint
from krim_agent.storage.models import park_checkpoints, runs
from krim_agent.storage.sqlite import SCHEMA_VERSION, SQLiteStore
from krim_agent.storage.subagent_models import (
    subagent_activations,
    subagent_child_runs,
    subagent_history,
    subagent_input_checkpoints,
    subagent_members,
    subagent_messages,
    subagent_notifications,
    subagent_results,
    subagent_tasks,
    subagent_trees,
)
from krim_agent.subagents.errors import SubagentError
from krim_agent.subagents.types import (
    ACTIVE_TASK_STATUS,
    LIVE_ACTIVATION_STATUS,
    TERMINAL_TASK_STATUS,
    AcceptSpawn,
    CommitInput,
    ReserveAgent,
    SendMessage,
    SettleResult,
    StartTask,
    TreeScope,
)

TREE_OPEN = "open"
TREE_CLOSED = "closed"


def _digest(value: object) -> str:
    return fingerprint(value)


def _now() -> float:
    return time.time()


def _tree_view(row: Any) -> dict[str, Any]:
    return {
        "tree_id": row["id"],
        "session_id": row["session_id"],
        "root_turn_id": row["root_turn_id"],
        "root_run_id": row["root_run_id"],
        "workspace_id": row["workspace_id"],
        "owner_id": row["owner_id"],
        "project_id": row["project_id"],
        "input_version": row["input_version"],
        "revision": row["revision"],
        "status": row["status"],
        "close_reason": row["close_reason"],
        "notify_cursor": row["notify_cursor"],
        "created_at": row["created_at"],
        "closed_at": row["closed_at"],
    }


def _member_view(row: Any) -> dict[str, Any]:
    return {
        "agent_id": row["id"],
        "tree_id": row["tree_id"],
        "parent_id": row["parent_id"],
        "path": row["path"],
        "role": row["role"],
        "provider": row["provider"],
        "profile": row["profile"],
        "provider_version": row["provider_version"],
        "profile_version": row["profile_version"],
        "workdir": row["workdir"],
        "depth": row["depth"],
        "status": row["status"],
        "current_task_id": row["current_task_id"],
        "inbox_cursor": row["inbox_cursor"],
        "notify_cursor": row["notify_cursor"],
        "revision": row["revision"],
        "created_at": row["created_at"],
        "closed_at": row["closed_at"],
    }


def _activation_view(row: Any) -> dict[str, Any]:
    return {
        "activation_id": row["id"],
        "agent_id": row["agent_id"],
        "tree_id": row["tree_id"],
        "instance": row["instance"],
        "status": row["status"],
        "provider_version": row["provider_version"],
        "cleanup_code": row["cleanup_code"],
        "created_at": row["created_at"],
        "resident_at": row["resident_at"],
        "released_at": row["released_at"],
    }


def _task_view(row: Any, *, with_input: bool = False) -> dict[str, Any]:
    body = {
        "task_id": row["id"],
        "agent_id": row["agent_id"],
        "tree_id": row["tree_id"],
        "ordinal": row["ordinal"],
        "status": row["status"],
        "run_id": row["run_id"],
        "parent_run_id": row["parent_run_id"],
        "source_version": row["source_version"],
        "provider_version": row["provider_version"],
        "profile_version": row["profile_version"],
        "input_hash": row["input_hash"],
        "revision": row["revision"],
        "input_closed": bool(row["input_closed"]),
        "created_at": row["created_at"],
        "finished_at": row["finished_at"],
    }
    if with_input:
        body["input"] = row["input"]
    return body


def _message_view(row: Any) -> dict[str, Any]:
    return {
        "message_id": row["id"],
        "tree_id": row["tree_id"],
        "target_id": row["target_id"],
        "seq": row["seq"],
        "sender_id": row["sender_id"],
        "task_id": row["task_id"],
        "kind": row["kind"],
        "wake": row["wake"],
        "body": row["body"],
        "body_hash": row["body_hash"],
        "checkpoint_id": row["checkpoint_id"],
        "accepted_at": row["accepted_at"],
    }


def _result_view(row: Any) -> dict[str, Any]:
    return {
        "result_id": row["id"],
        "task_id": row["task_id"],
        "run_id": row["run_id"],
        "stop_reason": row["stop_reason"],
        "code": row["code"],
        "output": row["output"],
        "diagnostics": row["diagnostics"],
        "content_hash": row["content_hash"],
        "settled_at": row["settled_at"],
    }


def _notification_view(row: Any, result: Any = None) -> dict[str, Any]:
    body = {
        "notification_id": row["id"],
        "tree_id": row["tree_id"],
        "result_id": row["result_id"],
        "task_id": row["task_id"],
        "producer_id": row["producer_id"],
        "consumer_id": row["consumer_id"],
        "seq": row["seq"],
        "status": row["status"],
        "checkpoint_id": row["checkpoint_id"],
        "suppress_reason": row["suppress_reason"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }
    if result is not None:
        body["result"] = _result_view(result)
    return body


def _checkpoint_view(row: Any) -> dict[str, Any]:
    return {
        "checkpoint_id": row["id"],
        "agent_id": row["agent_id"],
        "seq": row["seq"],
        "run_id": row["run_id"],
        "payload": row["payload"],
        "fingerprint": row["fingerprint"],
        "created_at": row["created_at"],
    }


async def _member_row(conn: AsyncConnection, agent_id: str) -> Any:
    row = (
        (await conn.execute(select(subagent_members).where(subagent_members.c.id == agent_id)))
        .mappings()
        .first()
    )
    if row is None:
        raise ServiceError("not_found", 404)
    return row


async def _tree_row(conn: AsyncConnection, tree_id: str) -> Any:
    row = (
        (await conn.execute(select(subagent_trees).where(subagent_trees.c.id == tree_id)))
        .mappings()
        .first()
    )
    if row is None:
        raise ServiceError("not_found", 404)
    return row


async def _latest_activation(conn: AsyncConnection, agent_id: str) -> Any | None:
    return (
        (
            await conn.execute(
                select(subagent_activations)
                .where(subagent_activations.c.agent_id == agent_id)
                .order_by(subagent_activations.c.instance.desc())
                .limit(1)
            )
        )
        .mappings()
        .first()
    )


async def _activation_row(conn: AsyncConnection, activation_id: str) -> Any:
    row = (
        (
            await conn.execute(
                select(subagent_activations).where(subagent_activations.c.id == activation_id)
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise ServiceError("not_found", 404)
    return row


async def _resolve_consumer(conn: AsyncConnection, member: Any) -> str | None:
    """完成通知消费方解析：父成员可消费仅当它是活跃根成员（role=root，绑定
    用户 run、无 current_task_id，离线也持久挂根，唤醒由域层决定），或其当前
    任务处于活跃态且输入未关闭；否则继续上溯。无可消费祖先时交宿主（None）。
    生产者本身是根成员时结果直接交宿主。中间成员可能自己没有 run，pending
    通知不能永远挂在它名下。"""

    if member["parent_id"] is None:
        return None
    current = member
    while current["parent_id"] is not None:
        parent = await _member_row(conn, current["parent_id"])
        if parent["status"] == "active":
            if parent["role"] == "root":
                return parent["id"]
            if parent["current_task_id"] is not None:
                task = (
                    (
                        await conn.execute(
                            select(subagent_tasks).where(
                                subagent_tasks.c.id == parent["current_task_id"]
                            )
                        )
                    )
                    .mappings()
                    .first()
                )
                if (
                    task is not None
                    and task["status"] in ACTIVE_TASK_STATUS
                    and not task["input_closed"]
                ):
                    return parent["id"]
        current = parent
    return None


async def _resident_count(conn: AsyncConnection, tree_id: str) -> int:
    return int(
        await conn.scalar(
            select(func.count())
            .select_from(subagent_activations)
            .join(subagent_members, subagent_activations.c.agent_id == subagent_members.c.id)
            .where(
                subagent_members.c.tree_id == tree_id,
                subagent_members.c.role == "agent",
                subagent_activations.c.status.in_(LIVE_ACTIVATION_STATUS),
            )
        )
    )


def _target_state(member: Any, activation: Any | None, task: Any | None) -> dict[str, Any]:
    return {
        "agent_id": member["id"],
        "status": member["status"],
        "activation_status": activation["status"] if activation is not None else None,
        "task_status": task["status"] if task is not None else None,
        "resident": activation is not None and activation["status"] in LIVE_ACTIVATION_STATUS,
    }


class SubagentStore:
    """Repository over SQLiteStore；方法返回 JSON 兼容 dict，可直接进工具输出。"""

    def __init__(self, store: SQLiteStore) -> None:
        if store.schema_version < SCHEMA_VERSION:
            raise ServiceError("schema_upgrade_required")
        self._store = store

    async def _submit(
        self,
        operation: Callable[[AsyncConnection], Awaitable[Any]],
        confirm: Callable[[], Awaitable[Any | None]],
    ) -> Any:
        store = self._store
        if store.closed:
            raise ServiceError("closed")
        if store.readonly:
            raise ServiceError("readonly")
        if not store.healthy:
            raise ServiceError("storage_unavailable", 503)
        return await store.work_write(operation, confirm)

    async def _query(self, operation: Callable[[AsyncConnection], Awaitable[Any]]) -> Any:
        return await self._store.work_read(operation)

    # ---------- 树 ----------

    async def ensure_tree(self, scope: TreeScope) -> dict[str, Any]:
        """宿主范围内幂等建树；同键同内容返回原树，同键异内容 key_conflict。"""
        scope = TreeScope.model_validate(scope.model_dump(mode="json"))
        digest = _digest(scope.model_dump(mode="json"))
        tree_id = str(uuid.uuid4())

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            row = (
                (
                    await conn.execute(
                        select(subagent_trees).where(
                            subagent_trees.c.session_id == scope.session_id,
                            subagent_trees.c.request_key == scope.request_key,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if row is not None:
                if row["request_hash"] != digest:
                    raise ServiceError("key_conflict")
                return _tree_view(row) | {"created": False}
            values = {
                "id": tree_id,
                "session_id": scope.session_id,
                "root_turn_id": scope.turn_id,
                "root_run_id": scope.run_id,
                "workspace_id": scope.workspace_id,
                "owner_id": scope.owner_id,
                "project_id": scope.project_id,
                "request_key": scope.request_key,
                "request_hash": digest,
                "input_version": 1,
                "revision": 1,
                "status": TREE_OPEN,
                "close_reason": None,
                "notify_cursor": 0,
                "created_at": _now(),
                "closed_at": None,
            }
            await conn.execute(insert(subagent_trees).values(**values))
            row = (
                (
                    await conn.execute(
                        select(subagent_trees).where(subagent_trees.c.id == tree_id)
                    )
                )
                .mappings()
                .one()
            )
            return _tree_view(row) | {"created": True}

        async def confirm() -> dict[str, Any] | None:
            found = await self.tree_by_key(scope.session_id, scope.request_key)
            if found is not None and found["revision"] >= 1:
                # 内容一致性由 request_hash 承担；这里只回放已提交事实。
                return found | {"created": found["tree_id"] == tree_id}
            return None

        return await self._submit(operation, confirm)

    async def tree(self, tree_id: str) -> dict[str, Any]:
        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            return _tree_view(await _tree_row(conn, tree_id))

        return await self._query(operation)

    async def tree_by_key(self, session_id: str, request_key: str) -> dict[str, Any] | None:
        async def operation(conn: AsyncConnection) -> dict[str, Any] | None:
            row = (
                (
                    await conn.execute(
                        select(subagent_trees).where(
                            subagent_trees.c.session_id == session_id,
                            subagent_trees.c.request_key == request_key,
                        )
                    )
                )
                .mappings()
                .first()
            )
            return _tree_view(row) if row is not None else None

        return await self._query(operation)

    async def require_open_tree(
        self, tree_id: str, *, expected_input_version: int | None = None
    ) -> dict[str, Any]:
        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            row = await _tree_row(conn, tree_id)
            if expected_input_version is not None and row["input_version"] != (
                expected_input_version
            ):
                raise ServiceError("revision_conflict")
            if row["status"] != TREE_OPEN:
                raise SubagentError(
                    "subagent_tree_closed",
                    accepted=False,
                    ids={"tree_id": tree_id},
                    action="树已关闭；不再接受新启动、消息或唤醒",
                )
            return _tree_view(row)

        return await self._query(operation)

    async def advance_input(self, tree_id: str, *, expected_input_version: int) -> dict[str, Any]:
        """新用户输入到达时推进根输入版本，旧版本的启动与唤醒随之失效。"""

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            row = await _tree_row(conn, tree_id)
            if row["input_version"] != expected_input_version:
                raise ServiceError("revision_conflict")
            await conn.execute(
                update(subagent_trees)
                .where(subagent_trees.c.id == tree_id)
                .values(
                    input_version=subagent_trees.c.input_version + 1,
                    revision=subagent_trees.c.revision + 1,
                )
            )
            return _tree_view(await _tree_row(conn, tree_id))

        async def confirm() -> dict[str, Any] | None:
            found = await self.tree(tree_id)
            return found if found["input_version"] > expected_input_version else None

        return await self._submit(operation, confirm)

    async def close_root_input(
        self,
        tree_id: str,
        *,
        consumer_id: str | None = None,
        expected_input_version: int | None = None,
    ) -> dict[str, Any]:
        """根完成事务检查：同事务核对无在途工作后关闭树准入。

        阻塞条件（plan §6.3）：starting 启动、执行中/待批准/已接受未跑的后代任务、
        未消费的 auto 唤醒消息（queue_only 不阻止收尾）、未完成交付的通知
        （pending/attached）。单纯驻留但空闲的成员与已关闭成员的遗留消息不阻止。
        检查与关闭在同一事务，不依赖调用方领域锁；无阻塞时同时推进根输入版本。
        """

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            tree = await _tree_row(conn, tree_id)
            if expected_input_version is not None and (
                tree["input_version"] != expected_input_version
            ):
                raise ServiceError("revision_conflict")
            if consumer_id is not None:
                consumer = await _member_row(conn, consumer_id)
                if consumer["tree_id"] != tree_id:
                    raise ServiceError("invalid_input", 422)
            if tree["status"] == TREE_CLOSED:
                return {"closed": True, "already_closed": True, "blockers": {}}
            starting = int(
                await conn.scalar(
                    select(func.count())
                    .select_from(subagent_activations)
                    .join(
                        subagent_members,
                        subagent_members.c.id == subagent_activations.c.agent_id,
                    )
                    .where(
                        subagent_members.c.tree_id == tree_id,
                        subagent_members.c.role == "agent",
                        subagent_activations.c.status == "starting",
                    )
                )
            )
            executing = int(
                await conn.scalar(
                    select(func.count())
                    .select_from(subagent_tasks)
                    .where(
                        subagent_tasks.c.tree_id == tree_id,
                        subagent_tasks.c.status.in_(
                            ("running", "waiting_input", "awaiting_approval")
                        ),
                    )
                )
            ) + int(
                await conn.scalar(
                    # accepted 只在还有未消费 auto 消息时阻止收尾；只有
                    # queue_only 排队的任务不延长根任务（plan §6.3）。
                    select(func.count())
                    .select_from(subagent_tasks)
                    .where(
                        subagent_tasks.c.tree_id == tree_id,
                        subagent_tasks.c.status == "accepted",
                        exists(
                            select(subagent_messages.c.id).where(
                                subagent_messages.c.task_id == subagent_tasks.c.id,
                                subagent_messages.c.checkpoint_id.is_(None),
                                subagent_messages.c.wake == "auto",
                            )
                        ),
                    )
                )
            )
            pending_auto = int(
                await conn.scalar(
                    select(func.count())
                    .select_from(subagent_messages)
                    .join(
                        subagent_members,
                        subagent_members.c.id == subagent_messages.c.target_id,
                    )
                    .outerjoin(
                        subagent_tasks,
                        subagent_tasks.c.id == subagent_messages.c.task_id,
                    )
                    .where(
                        subagent_messages.c.tree_id == tree_id,
                        subagent_messages.c.checkpoint_id.is_(None),
                        subagent_messages.c.wake == "auto",
                        subagent_members.c.status == "active",
                        # 绑定已结算任务的消息不再投递，不阻止收尾。
                        subagent_messages.c.task_id.is_(None)
                        | subagent_tasks.c.status.in_(ACTIVE_TASK_STATUS),
                    )
                )
            )
            unconsumed = int(
                await conn.scalar(
                    select(func.count())
                    .select_from(subagent_notifications)
                    .outerjoin(
                        subagent_members,
                        subagent_members.c.id == subagent_notifications.c.consumer_id,
                    )
                    .where(
                        subagent_notifications.c.tree_id == tree_id,
                        subagent_notifications.c.status.in_(("pending", "attached")),
                        subagent_notifications.c.consumer_id.is_(None)
                        | (subagent_members.c.status == "active"),
                    )
                )
            )
            blockers = {
                "starting_spawns": starting,
                "executing_tasks": executing,
                "pending_auto_messages": pending_auto,
                "unconsumed_notifications": unconsumed,
            }
            if any(blockers.values()):
                return {"closed": False, "already_closed": False, "blockers": blockers}
            await conn.execute(
                update(subagent_trees)
                .where(subagent_trees.c.id == tree_id, subagent_trees.c.status == TREE_OPEN)
                .values(
                    status=TREE_CLOSED,
                    close_reason="root-input-closed",
                    input_version=subagent_trees.c.input_version + 1,
                    revision=subagent_trees.c.revision + 1,
                    closed_at=_now(),
                )
            )
            return {"closed": True, "already_closed": False, "blockers": {}}

        async def confirm() -> dict[str, Any] | None:
            found = await self.tree(tree_id)
            if found["status"] == TREE_CLOSED:
                return {"closed": True, "already_closed": False, "blockers": {}}
            return None

        return await self._submit(operation, confirm)

    async def reopen_for_host(
        self, tree_id: str, *, session_id: str, workspace_id: str, owner_id: str,
        expected_input_version: int,
    ) -> dict[str, Any]:
        """Reopen natural completion for explicit owner input, without rebinding a run."""
        from krim_agent.storage.models import sessions

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            tree = await _tree_row(conn, tree_id)
            if (tree["session_id"], tree["workspace_id"], tree["owner_id"]) != (
                session_id, workspace_id, owner_id,
            ):
                raise ServiceError("not_found", 404)
            session = (await conn.execute(select(sessions).where(
                sessions.c.id == session_id,
            ))).mappings().first()
            if session is None or session["status"] != "open":
                raise ServiceError("session_closed", 409)
            if tree["input_version"] != expected_input_version:
                raise ServiceError("revision_conflict")
            if tree["status"] == TREE_OPEN:
                return _tree_view(tree)
            if tree["close_reason"] != "root-input-closed" or tree["root_run_id"] is None:
                raise SubagentError("subagent_tree_closed", accepted=False)
            root = (await conn.execute(select(runs).where(
                runs.c.id == tree["root_run_id"],
            ))).mappings().first()
            if root is None or root["status"] != "completed" or root["stop_requested"]:
                raise SubagentError("subagent_tree_closed", accepted=False)
            await conn.execute(update(subagent_trees).where(
                subagent_trees.c.id == tree_id,
                subagent_trees.c.input_version == expected_input_version,
            ).values(status=TREE_OPEN, close_reason="host-followup", closed_at=None,
                     input_version=subagent_trees.c.input_version + 1,
                     revision=subagent_trees.c.revision + 1))
            return _tree_view(await _tree_row(conn, tree_id))

        async def confirm() -> dict[str, Any] | None:
            found = await self.tree(tree_id)
            return found if (found["status"] == TREE_OPEN
                             and found["input_version"] == expected_input_version + 1
                             and found["close_reason"] == "host-followup") else None

        return await self._submit(operation, confirm)

    async def bind_root(
        self, scope: TreeScope, tree_id: str, *, expected_input_version: int
    ) -> dict[str, Any]:
        """宿主显式新用户 turn 续用已有树（plan 阶段5显式恢复入口）。

        仅当前宿主 producer 调用，模型不接触 tree_id。同一事务逐项核对树归属
        （session/workspace/owner/project 全等）与根输入版本 CAS；旧 root 任务
        必须已终态（root_run 非终态同样拒绝），不得在旧 root 活跃时抢占。通过后
        重绑 root_turn/root_run、status=open、input_version+1/revision+1。稳定
        root 成员、子成员与历史一律保留；旧 pending 通知按来源版本原样保留并
        在返回值中给出计数，不自动成为新授权，由宿主显式消费或抑制。
        """
        scope = TreeScope.model_validate(scope.model_dump(mode="json"))

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            tree = await _tree_row(conn, tree_id)
            if tree["session_id"] != scope.session_id:
                raise ServiceError("invalid_input", 422)
            if (
                tree["workspace_id"] != scope.workspace_id
                or tree["owner_id"] != scope.owner_id
                or tree["project_id"] != scope.project_id
            ):
                raise ServiceError("invalid_input", 422)
            if tree["input_version"] != expected_input_version:
                raise ServiceError("revision_conflict")
            root_row = (
                (
                    await conn.execute(
                        select(subagent_members).where(
                            subagent_members.c.tree_id == tree_id,
                            subagent_members.c.role == "root",
                        )
                    )
                )
                .mappings()
                .first()
            )
            if root_row is not None and root_row["current_task_id"] is not None:
                task = (
                    (
                        await conn.execute(
                            select(subagent_tasks).where(
                                subagent_tasks.c.id == root_row["current_task_id"]
                            )
                        )
                    )
                    .mappings()
                    .first()
                )
                if task is not None and task["status"] not in TERMINAL_TASK_STATUS:
                    raise ServiceError("revision_conflict")
            if tree["root_run_id"] is not None:
                run_status = await conn.scalar(
                    select(runs.c.status).where(runs.c.id == tree["root_run_id"])
                )
                if run_status is not None and run_status not in TERMINAL_RUN_STATUS:
                    raise ServiceError("revision_conflict")
            pending = int(
                await conn.scalar(
                    select(func.count())
                    .select_from(subagent_notifications)
                    .where(
                        subagent_notifications.c.tree_id == tree_id,
                        subagent_notifications.c.status.in_(("pending", "attached")),
                    )
                )
            )
            await conn.execute(
                update(subagent_trees)
                .where(
                    subagent_trees.c.id == tree_id,
                    subagent_trees.c.input_version == expected_input_version,
                )
                .values(
                    root_turn_id=scope.turn_id,
                    root_run_id=scope.run_id,
                    status=TREE_OPEN,
                    input_version=subagent_trees.c.input_version + 1,
                    revision=subagent_trees.c.revision + 1,
                )
            )
            updated = await _tree_row(conn, tree_id)
            return _tree_view(updated) | {"pending_notifications": pending, "rebound": True}

        async def confirm() -> dict[str, Any] | None:
            found = await self.tree(tree_id)
            if found["input_version"] > expected_input_version:
                return found | {"pending_notifications": -1, "rebound": True}
            return None

        return await self._submit(operation, confirm)

    async def close_tree(
        self, tree_id: str, *, reason: str, expected_revision: int | None = None
    ) -> dict[str, Any]:
        """关闭整棵树的准入；成员与历史保留，关闭是幂等的。"""

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            row = await _tree_row(conn, tree_id)
            if expected_revision is not None and row["revision"] != expected_revision:
                raise ServiceError("revision_conflict")
            if row["status"] == TREE_CLOSED:
                return _tree_view(row)
            await conn.execute(
                update(subagent_trees)
                .where(subagent_trees.c.id == tree_id)
                .values(
                    status=TREE_CLOSED,
                    close_reason=reason[:64],
                    revision=subagent_trees.c.revision + 1,
                    closed_at=_now(),
                )
            )
            return _tree_view(await _tree_row(conn, tree_id))

        async def confirm() -> dict[str, Any] | None:
            found = await self.tree(tree_id)
            return found if found["status"] == TREE_CLOSED else None

        return await self._submit(operation, confirm)

    # ---------- 成员与驻留 ----------

    async def reserve_agent(self, request: ReserveAgent) -> dict[str, Any]:
        """预留名字与驻留容量，建立 starting 实例；接受前可整体回滚。"""
        identity = request.model_dump(mode="json")
        identity.pop("max_depth")
        identity.pop("resident_limit")
        digest = _digest(identity)
        agent_id = str(uuid.uuid4())
        activation_id = str(uuid.uuid4())

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            tree = await _tree_row(conn, request.tree_id)
            if tree["status"] != TREE_OPEN:
                raise SubagentError(
                    "subagent_tree_closed",
                    accepted=False,
                    ids={"tree_id": request.tree_id},
                    action="树已关闭；不得新建成员",
                )
            row = (
                (
                    await conn.execute(
                        select(subagent_members).where(
                            subagent_members.c.tree_id == request.tree_id,
                            subagent_members.c.request_key == request.request_key,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if row is not None:
                if row["request_hash"] != digest:
                    raise ServiceError("key_conflict")
                activation = await _latest_activation(conn, row["id"])
                return {
                    "agent": _member_view(row),
                    "activation": _activation_view(activation) if activation else None,
                    "created": False,
                }
            named = (
                (
                    await conn.execute(
                        select(subagent_members.c.id).where(
                            subagent_members.c.tree_id == request.tree_id,
                            subagent_members.c.path == request.path,
                        )
                    )
                )
                .scalars()
                .first()
            )
            if named is not None:
                raise SubagentError(
                    "subagent_name_conflict",
                    accepted=False,
                    ids={"tree_id": request.tree_id, "path": request.path, "agent_id": named},
                    action="换一个名字或用原 request_key 找回已接受的成员",
                )
            depth = 0
            if request.role == "agent":
                if request.parent_id is None:
                    raise ServiceError("invalid_input", 422)
                parent = await _member_row(conn, request.parent_id)
                if parent["tree_id"] != request.tree_id:
                    raise ServiceError("invalid_input", 422)
                if parent["status"] != "active":
                    raise SubagentError(
                        "subagent_activation_closed",
                        accepted=False,
                        ids={"agent_id": request.parent_id},
                        action="父成员已关闭；不得在其下新建成员",
                    )
                depth = parent["depth"] + 1
                if request.max_depth is not None and depth > request.max_depth:
                    raise SubagentError(
                        "subagent_depth_exceeded",
                        accepted=False,
                        ids={"tree_id": request.tree_id, "path": request.path},
                        action=f"当前深度 {depth} 超过允许的 {request.max_depth}",
                    )
                if request.resident_limit is not None:
                    residents = await _resident_count(conn, request.tree_id)
                    if residents >= request.resident_limit:
                        raise SubagentError(
                            "capacity",
                            accepted=False,
                            ids={"tree_id": request.tree_id},
                            action="驻留容量已满；先释放或等待清理完成",
                            status=429,
                        )
            member_values = {
                "id": agent_id,
                "tree_id": request.tree_id,
                "parent_id": request.parent_id,
                "path": request.path,
                "role": request.role,
                "provider": request.provider,
                "profile": request.profile,
                "provider_version": request.provider_version,
                "profile_version": request.profile_version,
                "workdir": request.workdir,
                "depth": depth,
                "status": "active",
                "current_task_id": None,
                "inbox_cursor": 0,
                "notify_cursor": 0,
                "revision": 1,
                "request_key": request.request_key,
                "request_hash": digest,
                "created_at": _now(),
                "closed_at": None,
            }
            await conn.execute(insert(subagent_members).values(**member_values))
            activation_values = {
                "id": activation_id,
                "agent_id": agent_id,
                "tree_id": request.tree_id,
                "instance": 1,
                "status": "starting",
                "provider_version": request.provider_version,
                "cleanup_code": None,
                "created_at": _now(),
                "resident_at": None,
                "released_at": None,
            }
            await conn.execute(insert(subagent_activations).values(**activation_values))
            return {
                "agent": _member_view(await _member_row(conn, agent_id)),
                "activation": _activation_view(await _activation_row(conn, activation_id)),
                "created": True,
            }

        async def confirm() -> dict[str, Any] | None:
            found = await self._find_member_by_key(request.tree_id, request.request_key)
            if found is None or found["request_hash"] != digest:
                return None
            return found | {"created": found["agent"]["agent_id"] == agent_id}

        return await self._submit(operation, confirm)

    async def _find_member_by_key(self, tree_id: str, request_key: str) -> dict[str, Any] | None:
        async def operation(conn: AsyncConnection) -> dict[str, Any] | None:
            row = (
                (
                    await conn.execute(
                        select(subagent_members).where(
                            subagent_members.c.tree_id == tree_id,
                            subagent_members.c.request_key == request_key,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                return None
            activation = await _latest_activation(conn, row["id"])
            return {
                "agent": _member_view(row),
                "activation": _activation_view(activation) if activation else None,
                "request_hash": row["request_hash"],
            }

        return await self._query(operation)

    async def accept_spawn(self, request: AcceptSpawn) -> dict[str, Any]:
        """启动接受点：同一事务建初始任务、发首条消息并绑定当前任务。"""
        input_hash = _digest(request.input)

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            member = await _member_row(conn, request.agent_id)
            activation = await _activation_row(conn, request.activation_id)
            if activation["agent_id"] != member["id"]:
                raise ServiceError("invalid_input", 422)
            if activation["status"] not in LIVE_ACTIVATION_STATUS:
                raise SubagentError(
                    "subagent_activation_closed",
                    accepted=False,
                    ids={
                        "agent_id": member["id"],
                        "activation_id": activation["id"],
                    },
                    action="实例已释放；先按成员冷恢复取得新 activation",
                )
            tree = await _tree_row(conn, member["tree_id"])
            if tree["status"] != TREE_OPEN:
                raise SubagentError(
                    "subagent_tree_closed",
                    accepted=False,
                    ids={"tree_id": member["tree_id"], "agent_id": member["id"]},
                )
            first = (
                (
                    await conn.execute(
                        select(subagent_tasks).where(
                            subagent_tasks.c.agent_id == member["id"],
                            subagent_tasks.c.ordinal == 1,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if first is not None:
                if first["input_hash"] != input_hash:
                    raise ServiceError("key_conflict")
                message = (
                    (
                        await conn.execute(
                            select(subagent_messages).where(
                                subagent_messages.c.target_id == member["id"],
                                subagent_messages.c.seq == 1,
                            )
                        )
                    )
                    .mappings()
                    .one()
                )
                return self._spawn_receipt(member, activation, first, message, created=False)
            task_id = str(uuid.uuid4())
            message_id = str(uuid.uuid4())
            provider_version = activation["provider_version"]
            task_values = {
                "id": task_id,
                "agent_id": member["id"],
                "tree_id": member["tree_id"],
                "ordinal": 1,
                "status": "accepted",
                "run_id": None,
                "parent_run_id": request.parent_run_id,
                "source_version": tree["input_version"],
                "provider_version": provider_version,
                "profile_version": member["profile_version"],
                "input": request.input,
                "input_hash": input_hash,
                "request_key": member["request_key"],
                "request_hash": member["request_hash"],
                "revision": 1,
                "input_closed": False,
                "created_at": _now(),
                "finished_at": None,
            }
            await conn.execute(insert(subagent_tasks).values(**task_values))
            send_key = "spawn:" + member["request_key"]
            message_values = {
                "id": message_id,
                "tree_id": member["tree_id"],
                "target_id": member["id"],
                "seq": 1,
                "sender_id": None,
                "task_id": task_id,
                "kind": "task",
                "wake": "auto",
                "body": request.input,
                "body_hash": input_hash,
                "send_key": send_key,
                "send_key_hash": _digest(send_key),
                "checkpoint_id": None,
                "accepted_at": _now(),
            }
            await conn.execute(insert(subagent_messages).values(**message_values))
            await conn.execute(
                update(subagent_members)
                .where(subagent_members.c.id == member["id"])
                .values(current_task_id=task_id, revision=subagent_members.c.revision + 1)
            )
            member = await _member_row(conn, member["id"])
            task = (
                (
                    await conn.execute(
                        select(subagent_tasks).where(subagent_tasks.c.id == task_id)
                    )
                )
                .mappings()
                .one()
            )
            message = (
                (
                    await conn.execute(
                        select(subagent_messages).where(subagent_messages.c.id == message_id)
                    )
                )
                .mappings()
                .one()
            )
            return self._spawn_receipt(member, activation, task, message, created=True)

        async def confirm() -> dict[str, Any] | None:
            found = await self._spawn_task(request.agent_id)
            if found is None or found["task"]["input_hash"] != input_hash:
                return None
            return found | {"created": found["task_id"] is not None and False}

        return await self._submit(operation, confirm)

    def _spawn_receipt(
        self, member: Any, activation: Any, task: Any, message: Any, *, created: bool
    ) -> dict[str, Any]:
        return {
            "agent_id": member["id"],
            "activation_id": activation["id"],
            "task_id": task["id"],
            "message_id": message["id"],
            "seq": message["seq"],
            "task": _task_view(task, with_input=True),
            "target": _target_state(member, activation, task),
            "created": created,
        }

    async def _spawn_task(self, agent_id: str) -> dict[str, Any] | None:
        async def operation(conn: AsyncConnection) -> dict[str, Any] | None:
            task = (
                (
                    await conn.execute(
                        select(subagent_tasks).where(
                            subagent_tasks.c.agent_id == agent_id,
                            subagent_tasks.c.ordinal == 1,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if task is None:
                return None
            member = await _member_row(conn, agent_id)
            activation = await _latest_activation(conn, agent_id)
            message = (
                (
                    await conn.execute(
                        select(subagent_messages).where(
                            subagent_messages.c.target_id == agent_id,
                            subagent_messages.c.seq == 1,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if message is None:
                raise ServiceError("facts_corrupt")
            receipt = self._spawn_receipt(member, activation, task, message, created=False)
            receipt["task_id"] = task["id"]
            return receipt

        return await self._query(operation)

    async def rollback_reservation(self, agent_id: str, activation_id: str) -> bool:
        """回执前失败回滚：仅当尚未接受任务时删除预留的成员与实例。"""

        async def operation(conn: AsyncConnection) -> bool:
            activation = await _activation_row(conn, activation_id)
            member = await _member_row(conn, agent_id)
            if activation["agent_id"] != member["id"]:
                raise ServiceError("invalid_input", 422)
            if member["current_task_id"] is not None:
                return False
            started = (
                await conn.scalar(
                    select(func.count())
                    .select_from(subagent_tasks)
                    .where(subagent_tasks.c.agent_id == agent_id)
                )
            )
            if started:
                return False
            messaged = (
                await conn.scalar(
                    select(func.count())
                    .select_from(subagent_messages)
                    .where(subagent_messages.c.target_id == agent_id)
                )
            )
            if messaged:
                return False
            if activation["status"] not in ("starting",):
                return False
            await conn.execute(
                delete(subagent_activations).where(subagent_activations.c.id == activation_id)
            )
            await conn.execute(
                delete(subagent_members).where(subagent_members.c.id == agent_id)
            )
            return True

        async def confirm() -> bool | None:
            found = await self.get_agent(agent_id)
            if found["current_task_id"] is not None:
                return False
            return None

        return await self._submit(operation, confirm)

    async def begin_activation(
        self,
        agent_id: str,
        *,
        provider_version: str,
        expected_instance: int | None = None,
        resident_limit: int | None = None,
    ) -> dict[str, Any]:
        """冷恢复：带实例版本检查建立新驻留；并发恢复只有一个胜出。"""
        activation_id = str(uuid.uuid4())

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            member = await _member_row(conn, agent_id)
            if member["status"] != "active":
                raise SubagentError(
                    "subagent_not_recoverable",
                    ids={"agent_id": agent_id},
                    action="成员已随子树关闭；不得复活",
                )
            tree = await _tree_row(conn, member["tree_id"])
            if tree["status"] != TREE_OPEN:
                raise SubagentError(
                    "subagent_tree_closed",
                    ids={"tree_id": member["tree_id"], "agent_id": agent_id},
                )
            latest = await _latest_activation(conn, agent_id)
            prior = latest["instance"] if latest is not None else 0
            if expected_instance is not None and prior != expected_instance:
                raise ServiceError("revision_conflict")
            if latest is not None and latest["status"] in ("starting", "resident"):
                raise ServiceError("revision_conflict")
            if latest is not None and latest["status"] == "closing":
                raise SubagentError(
                    "subagent_not_recoverable",
                    ids={"agent_id": agent_id, "activation_id": latest["id"]},
                    action="上一实例清理未完成且仍占容量；先完成回收再恢复",
                )
            if resident_limit is not None and member["role"] == "agent":
                residents = await _resident_count(conn, member["tree_id"])
                if residents >= resident_limit:
                    raise SubagentError(
                        "capacity",
                        ids={"tree_id": member["tree_id"]},
                        action="驻留容量已满；先释放或等待清理完成",
                        status=429,
                    )
            values = {
                "id": activation_id,
                "agent_id": agent_id,
                "tree_id": member["tree_id"],
                "instance": prior + 1,
                "status": "starting",
                "provider_version": provider_version,
                "cleanup_code": None,
                "created_at": _now(),
                "resident_at": None,
                "released_at": None,
            }
            await conn.execute(insert(subagent_activations).values(**values))
            await conn.execute(
                update(subagent_members)
                .where(subagent_members.c.id == agent_id)
                .values(
                    provider_version=provider_version,
                    revision=subagent_members.c.revision + 1,
                )
            )
            return {
                "activation": _activation_view(await _activation_row(conn, activation_id)),
                "prior_instance": prior,
                "created": True,
            }

        async def confirm() -> dict[str, Any] | None:
            async def operation(conn: AsyncConnection) -> Any:
                return await _latest_activation(conn, agent_id)

            latest = await self._query(operation)
            if latest is None:
                return None
            if expected_instance is not None and latest["instance"] <= expected_instance:
                return None
            return {
                "activation": _activation_view(latest),
                "prior_instance": expected_instance or latest["instance"] - 1,
                "created": latest["id"] == activation_id,
            }

        return await self._submit(operation, confirm)

    async def transition_activation(
        self,
        activation_id: str,
        *,
        expected_status: str,
        status: str,
        cleanup_code: str | None = None,
    ) -> dict[str, Any]:
        """驻留状态机 CAS：starting→resident→closing→released；closing→closing 记清理失败。"""

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            row = await _activation_row(conn, activation_id)
            if row["status"] == "released":
                raise SubagentError(
                    "subagent_activation_closed",
                    ids={"agent_id": row["agent_id"], "activation_id": activation_id},
                    action="该实例已释放；旧句柄不能操作新实例",
                )
            if row["status"] != expected_status:
                raise ServiceError("revision_conflict")
            allowed = {
                ("starting", "resident"),
                ("starting", "closing"),
                ("resident", "closing"),
                ("closing", "released"),
                ("closing", "closing"),
            }
            if (row["status"], status) not in allowed:
                raise ServiceError("invalid_input", 422)
            values: dict[str, Any] = {"status": status}
            if status == "resident":
                values["resident_at"] = _now()
            elif status == "released":
                values["released_at"] = _now()
            if cleanup_code is not None:
                values["cleanup_code"] = cleanup_code[:64]
            if (row["status"], status) == ("closing", "closing") and cleanup_code is None:
                raise ServiceError("invalid_input", 422)
            await conn.execute(
                update(subagent_activations)
                .where(
                    subagent_activations.c.id == activation_id,
                    subagent_activations.c.status == expected_status,
                )
                .values(**values)
            )
            return _activation_view(await _activation_row(conn, activation_id))

        async def confirm() -> dict[str, Any] | None:
            async def operation(conn: AsyncConnection) -> Any:
                return await _activation_row(conn, activation_id)

            found = await self._query(operation)
            if found is None:
                return None
            if found["status"] == status and (
                cleanup_code is None or found["cleanup_code"] == cleanup_code
            ):
                return _activation_view(found)
            if found["status"] == "released":
                return _activation_view(found)
            return None

        return await self._submit(operation, confirm)

    async def get_agent(self, agent_id: str) -> dict[str, Any]:
        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            member = await _member_row(conn, agent_id)
            activation = await _latest_activation(conn, agent_id)
            task = (
                (
                    await conn.execute(
                        select(subagent_tasks).where(
                            subagent_tasks.c.id == member["current_task_id"]
                        )
                    )
                )
                .mappings()
                .first()
                if member["current_task_id"] is not None
                else None
            )
            version = (
                await conn.scalar(
                    select(subagent_history.c.version).where(
                        subagent_history.c.agent_id == agent_id
                    )
                )
            )
            body = _member_view(member)
            body |= {
                "activation": _activation_view(activation) if activation else None,
                "task": _task_view(task) if task is not None else None,
                "history_version": version or 0,
                "resident": activation is not None
                and activation["status"] in LIVE_ACTIVATION_STATUS,
            }
            return body

        return await self._query(operation)

    async def list_agents(
        self, tree_id: str, *, after_path: str | None = None, limit: int = 64
    ) -> list[dict[str, Any]]:
        if not 1 <= limit <= 256:
            raise ServiceError("invalid_input", 422)

        async def operation(conn: AsyncConnection) -> list[dict[str, Any]]:
            statement = (
                select(subagent_members)
                .where(subagent_members.c.tree_id == tree_id)
                .order_by(subagent_members.c.path)
                .limit(limit)
            )
            if after_path is not None:
                statement = statement.where(subagent_members.c.path > after_path)
            rows = (await conn.execute(statement)).mappings().all()
            agents: list[dict[str, Any]] = []
            for row in rows:
                activation = await _latest_activation(conn, row["id"])
                task = (
                    (
                        await conn.execute(
                            select(subagent_tasks).where(
                                subagent_tasks.c.id == row["current_task_id"]
                            )
                        )
                    )
                    .mappings()
                    .first()
                    if row["current_task_id"] is not None
                    else None
                )
                agents.append(
                    _member_view(row)
                    | {
                        "activation": _activation_view(activation) if activation else None,
                        "task": _task_view(task) if task is not None else None,
                        "resident": activation is not None
                        and activation["status"] in LIVE_ACTIVATION_STATUS,
                    }
                )
            return agents

        return await self._query(operation)

    async def list_descendants(self, agent_id: str, *, limit: int = 256) -> list[dict[str, Any]]:
        """按路径前缀列出子树成员；名字只用于寻址，权限仍由服务核对。"""
        if not 1 <= limit <= 1024:
            raise ServiceError("invalid_input", 422)

        async def operation(conn: AsyncConnection) -> list[dict[str, Any]]:
            member = await _member_row(conn, agent_id)
            escaped = member["path"].replace("\\", "\\\\").replace("%", r"\%").replace("_", r"\_")
            rows = (
                (
                    await conn.execute(
                        select(subagent_members)
                        .where(
                            subagent_members.c.tree_id == member["tree_id"],
                            subagent_members.c.path.like(escaped + r"/%", escape="\\"),
                        )
                        .order_by(subagent_members.c.path)
                        .limit(limit)
                    )
                )
                .mappings()
                .all()
            )
            agents: list[dict[str, Any]] = []
            for row in rows:
                activation = await _latest_activation(conn, row["id"])
                agents.append(
                    _member_view(row)
                    | {
                        "activation": _activation_view(activation) if activation else None,
                        "resident": activation is not None
                        and activation["status"] in LIVE_ACTIVATION_STATUS,
                    }
                )
            return agents

        return await self._query(operation)

    async def close_member(self, agent_id: str, *, expected_revision: int) -> dict[str, Any]:
        """关闭单个成员的准入；active→closed 幂等，实例释放由服务另行 dispose。"""

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            member = await _member_row(conn, agent_id)
            if member["status"] == "closed":
                return _member_view(member)
            if member["revision"] != expected_revision:
                raise ServiceError("revision_conflict")
            await conn.execute(
                update(subagent_members)
                .where(
                    subagent_members.c.id == agent_id,
                    subagent_members.c.revision == expected_revision,
                )
                .values(status="closed", closed_at=_now(), revision=expected_revision + 1)
            )
            return _member_view(await _member_row(conn, agent_id))

        async def confirm() -> dict[str, Any] | None:
            found = await self.get_agent(agent_id)
            return found if found["status"] == "closed" else None

        return await self._submit(operation, confirm)

    async def resident_count(self, tree_id: str) -> int:
        """树内非根成员的驻留占用：starting/resident/closing 都计入。"""

        async def operation(conn: AsyncConnection) -> int:
            return await _resident_count(conn, tree_id)

        return await self._query(operation)

    # ---------- 消息 ----------

    async def accept_message(self, request: SendMessage) -> dict[str, Any]:
        """接受消息并分配目标序号；send_key 重放返回原回执，不重复入队。"""
        body_hash = _digest(request.body)
        send_key_hash = _digest(request.send_key)
        message_id = str(uuid.uuid4())

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            tree = await _tree_row(conn, request.tree_id)
            if tree["status"] != TREE_OPEN:
                raise SubagentError(
                    "subagent_tree_closed",
                    accepted=False,
                    ids={"tree_id": request.tree_id, "agent_id": request.target_id},
                    action="树已关闭；消息不再接受",
                )
            member = await _member_row(conn, request.target_id)
            if member["tree_id"] != request.tree_id:
                raise ServiceError("invalid_input", 422)
            activation = await _latest_activation(conn, member["id"])
            if member["status"] == "closed" or (
                activation is not None and activation["status"] == "closing"
            ):
                raise SubagentError(
                    "subagent_activation_closed",
                    accepted=False,
                    ids={"agent_id": member["id"], "tree_id": request.tree_id},
                    action="目标正在关闭或已随子树关闭；本次发送拒绝",
                )
            if request.sender_id is not None:
                sender = await _member_row(conn, request.sender_id)
                if sender["tree_id"] != request.tree_id:
                    raise ServiceError("invalid_input", 422)
            replay = (
                (
                    await conn.execute(
                        select(subagent_messages).where(
                            subagent_messages.c.target_id == member["id"],
                            subagent_messages.c.send_key_hash == send_key_hash,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if replay is not None:
                mismatch = (
                    replay["body_hash"] != body_hash
                    or replay["kind"] != request.kind
                    or replay["wake"] != request.wake
                )
                if not mismatch and replay["task_id"] != request.task_id:
                    # 绑定差异需按原发送意图判断：显式 task_id 必须一致；
                    # 自动产物（kind=task 建任务 / kind=message 绑当前任务）
                    # 在任务输入与本次消息体一致时视为同一发送。
                    if request.task_id is not None or replay["task_id"] is None:
                        mismatch = True
                    elif request.kind == "task":
                        bound = (
                            (
                                await conn.execute(
                                    select(subagent_tasks).where(
                                        subagent_tasks.c.id == replay["task_id"]
                                    )
                                )
                            )
                            .mappings()
                            .first()
                        )
                        if bound is None or bound["input_hash"] != body_hash:
                            mismatch = True
                if mismatch:
                    raise ServiceError("key_conflict")
                current = (
                    (
                        await conn.execute(
                            select(subagent_tasks).where(
                                subagent_tasks.c.id == member["current_task_id"]
                            )
                        )
                    )
                    .mappings()
                    .first()
                    if member["current_task_id"] is not None
                    else None
                )
                return {
                    "message_id": replay["id"],
                    "seq": replay["seq"],
                    "task_id": replay["task_id"],
                    "created": False,
                    "target": _target_state(member, activation, current),
                }
            task = None
            binding_task_id = request.task_id
            if request.task_id is not None:
                task = (
                    (
                        await conn.execute(
                            select(subagent_tasks).where(subagent_tasks.c.id == request.task_id)
                        )
                    )
                    .mappings()
                    .first()
                )
                if task is None or task["agent_id"] != member["id"]:
                    raise ServiceError("invalid_input", 422)
                if task["status"] in TERMINAL_TASK_STATUS:
                    raise SubagentError(
                        "subagent_task_closed",
                        accepted=False,
                        ids={"agent_id": member["id"], "task_id": task["id"]},
                        action="旧任务已结算；如需继续请用显式后续任务（kind=task）",
                    )
                if task["input_closed"]:
                    raise SubagentError(
                        "subagent_task_closed",
                        accepted=False,
                        ids={"agent_id": member["id"], "task_id": task["id"]},
                        action="任务输入已持久关闭；不再接受补充输入",
                    )
            elif request.kind == "task":
                current = (
                    (
                        await conn.execute(
                            select(subagent_tasks).where(
                                subagent_tasks.c.id == member["current_task_id"]
                            )
                        )
                    )
                    .mappings()
                    .first()
                    if member["current_task_id"] is not None
                    else None
                )
                if current is not None and current["status"] in ACTIVE_TASK_STATUS:
                    raise SubagentError(
                        "subagent_task_closed",
                        accepted=False,
                        ids={"agent_id": member["id"], "task_id": current["id"]},
                        action="当前任务未结束；先 interrupt 或等结果结算再开新任务",
                    )
                seq = (
                    await conn.scalar(
                        select(func.coalesce(func.max(subagent_tasks.c.ordinal), 0)).where(
                            subagent_tasks.c.agent_id == member["id"]
                        )
                    )
                ) + 1
                provider_version = (
                    activation["provider_version"] if activation is not None else
                    member["provider_version"]
                )
                task_id = str(uuid.uuid4())
                task_values = {
                    "id": task_id,
                    "agent_id": member["id"],
                    "tree_id": member["tree_id"],
                    "ordinal": seq,
                    "status": "accepted",
                    "run_id": None,
                    "parent_run_id": None,
                    "source_version": tree["input_version"],
                    "provider_version": provider_version,
                    "profile_version": member["profile_version"],
                    "input": request.body,
                    "input_hash": body_hash,
                    # send_key 只在目标内唯一；任务键按树唯一，需带成员限定。
                    "request_key": f"{request.send_key}@{member['id']}",
                    "request_hash": send_key_hash,
                    "revision": 1,
                    "input_closed": False,
                    "created_at": _now(),
                    "finished_at": None,
                }
                await conn.execute(insert(subagent_tasks).values(**task_values))
                binding_task_id = task_id
                task = (
                    (
                        await conn.execute(
                            select(subagent_tasks).where(subagent_tasks.c.id == task_id)
                        )
                    )
                    .mappings()
                    .one()
                )
            else:
                # 无 task_id 的普通消息绑定当前活跃任务，不漂移到未来的任务。
                current = (
                    (
                        await conn.execute(
                            select(subagent_tasks).where(
                                subagent_tasks.c.id == member["current_task_id"]
                            )
                        )
                    )
                    .mappings()
                    .first()
                    if member["current_task_id"] is not None
                    else None
                )
                if current is not None and current["status"] in ACTIVE_TASK_STATUS:
                    if current["input_closed"]:
                        raise SubagentError(
                            "subagent_task_closed",
                            accepted=False,
                            ids={"agent_id": member["id"], "task_id": current["id"]},
                            action="当前任务输入已持久关闭；不再接受补充输入",
                        )
                    task = current
                    binding_task_id = current["id"]
            seq = (
                await conn.scalar(
                    select(func.coalesce(func.max(subagent_messages.c.seq), 0)).where(
                        subagent_messages.c.target_id == member["id"]
                    )
                )
            ) + 1
            values = {
                "id": message_id,
                "tree_id": request.tree_id,
                "target_id": member["id"],
                "seq": seq,
                "sender_id": request.sender_id,
                "task_id": binding_task_id,
                "kind": request.kind,
                "wake": request.wake,
                "body": request.body,
                "body_hash": body_hash,
                "send_key": request.send_key,
                "send_key_hash": send_key_hash,
                "checkpoint_id": None,
                "accepted_at": _now(),
            }
            await conn.execute(insert(subagent_messages).values(**values))
            if binding_task_id is not None and request.kind == "task":
                await conn.execute(
                    update(subagent_members)
                    .where(subagent_members.c.id == member["id"])
                    .values(
                        current_task_id=binding_task_id,
                        revision=subagent_members.c.revision + 1,
                    )
                )
                member = await _member_row(conn, member["id"])
            message = (
                (
                    await conn.execute(
                        select(subagent_messages).where(subagent_messages.c.id == message_id)
                    )
                )
                .mappings()
                .one()
            )
            current = (
                (
                    await conn.execute(
                        select(subagent_tasks).where(
                            subagent_tasks.c.id == member["current_task_id"]
                        )
                    )
                )
                .mappings()
                .first()
                if member["current_task_id"] is not None
                else None
            )
            return {
                "message_id": message["id"],
                "seq": message["seq"],
                "task_id": message["task_id"],
                "created": True,
                "target": _target_state(member, activation, current),
            }

        async def confirm() -> dict[str, Any] | None:
            async def operation(conn: AsyncConnection) -> Any:
                return (
                    (
                        await conn.execute(
                            select(subagent_messages).where(
                                subagent_messages.c.target_id == request.target_id,
                                subagent_messages.c.send_key_hash == send_key_hash,
                            )
                        )
                    )
                    .mappings()
                    .first()
                )

            replay = await self._query(operation)
            if replay is None or replay["body_hash"] != body_hash:
                return None
            return {
                "message_id": replay["id"],
                "seq": replay["seq"],
                "task_id": replay["task_id"],
                "created": replay["id"] == message_id,
                "target": None,
            }

        return await self._submit(operation, confirm)

    async def inbox(
        self, agent_id: str, *, after_seq: int = 0, limit: int = 64
    ) -> list[dict[str, Any]]:
        """按游标增量读取收件箱；不重读全量历史。"""
        if after_seq < 0 or not 1 <= limit <= 256:
            raise ServiceError("invalid_cursor", 416)

        async def operation(conn: AsyncConnection) -> list[dict[str, Any]]:
            rows = (
                (
                    await conn.execute(
                        select(subagent_messages)
                        .where(
                            subagent_messages.c.target_id == agent_id,
                            subagent_messages.c.seq > after_seq,
                        )
                        .order_by(subagent_messages.c.seq)
                        .limit(limit)
                    )
                )
                .mappings()
                .all()
            )
            return [_message_view(row) for row in rows]

        return await self._query(operation)

    async def message_by_key(self, target_id: str, send_key: str) -> dict[str, Any] | None:
        """按（目标, 发送键哈希）找回已接受消息的原回执；供重试路径先查后判。

        返回 message_id/seq/task_id/body/body_hash/send_key_hash/sender_id/
        wake/kind 与绑定任务摘要；调用方必须自行核对原请求意图指纹，不得
        用找到的消息改写参数或重建任务。
        """
        send_key_hash = _digest(send_key)

        async def operation(conn: AsyncConnection) -> dict[str, Any] | None:
            row = (
                (
                    await conn.execute(
                        select(subagent_messages).where(
                            subagent_messages.c.target_id == target_id,
                            subagent_messages.c.send_key_hash == send_key_hash,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                return None
            receipt = _message_view(row)
            receipt["send_key_hash"] = row["send_key_hash"]
            if row["task_id"] is not None:
                task = (
                    (
                        await conn.execute(
                            select(subagent_tasks).where(subagent_tasks.c.id == row["task_id"])
                        )
                    )
                    .mappings()
                    .first()
                )
                receipt["task"] = _task_view(task, with_input=True) if task else None
            return receipt

        return await self._query(operation)

    # ---------- 任务与执行 ----------

    async def start_task(self, request: StartTask) -> dict[str, Any]:
        """为已接受任务创建生产 run：同事务插入 run、child_run ownership 映射，
        并在 run.target 写入子上下文描述符（schema 5，值与 DB 行一致）。

        幂等：按 request.request.request_key 查 runs，hash 匹配返回原 run
        （created=False），不匹配 key_conflict。重放优先于准入校验。
        """
        run_id = str(uuid.uuid4())

        async def descriptor(
            tree: Any, member: Any, activation: Any, task: Any
        ) -> dict[str, Any]:
            return {
                "schema_version": 5,
                "tree_id": tree["id"],
                "agent_id": member["id"],
                "activation_id": activation["id"],
                "owner_session_id": tree["session_id"],
                "owner_turn_id": request.owner_turn_id,
                "owner_id": tree["owner_id"],
                "workspace_id": tree["workspace_id"],
                "depth": member["depth"],
                "parent_run_id": task["parent_run_id"],
                "profile": member["profile"],
                "path": member["path"],
                "task_id": task["id"],
            }

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            existing = (
                (
                    await conn.execute(
                        select(runs).where(runs.c.request_key == request.request.request_key)
                    )
                )
                .mappings()
                .first()
            )
            if existing is not None:
                # 重放：需要行重建 effective_target 才能核对 request_hash；行值
                # 全部为不可变事实，重试携带同一 spec 即得同一描述符。
                member = await _member_row(conn, request.agent_id)
                activation = await _activation_row(conn, request.activation_id)
                tree = await _tree_row(conn, member["tree_id"])
                task = (
                    (
                        await conn.execute(
                            select(subagent_tasks).where(subagent_tasks.c.id == request.task_id)
                        )
                    )
                    .mappings()
                    .first()
                )
                digest = _digest(
                    {
                        "request": request.request.model_dump(mode="json"),
                        "target": request.target
                        | {"context": await descriptor(tree, member, activation, task)},
                    }
                )
                if existing["request_hash"] != digest:
                    raise ServiceError("key_conflict")
                mapping = (
                    (
                        await conn.execute(
                            select(subagent_child_runs).where(
                                subagent_child_runs.c.run_id == existing["id"]
                            )
                        )
                    )
                    .mappings()
                    .first()
                )
                if mapping is None or mapping["task_id"] != request.task_id:
                    raise ServiceError("key_conflict")
                view = RunView.model_validate(
                    {key: existing[key] for key in RunView.model_fields}
                )
                return {
                    "run": view.model_dump(mode="json"),
                    "run_id": existing["id"],
                    "task": _task_view(task),
                    "created": False,
                }
            member = await _member_row(conn, request.agent_id)
            if member["status"] != "active":
                raise SubagentError(
                    "subagent_activation_closed",
                    ids={"agent_id": member["id"]},
                    action="成员已关闭；不得开跑",
                )
            activation = await _activation_row(conn, request.activation_id)
            if activation["agent_id"] != member["id"] or activation["status"] not in (
                "starting",
                "resident",
            ):
                raise SubagentError(
                    "subagent_activation_closed",
                    ids={"agent_id": member["id"], "activation_id": request.activation_id},
                    action="驻留实例不在位；先恢复实例再开跑",
                )
            tree = await _tree_row(conn, member["tree_id"])
            if tree["status"] != TREE_OPEN:
                raise SubagentError(
                    "subagent_tree_closed",
                    ids={"tree_id": member["tree_id"], "agent_id": member["id"]},
                )
            task = (
                (
                    await conn.execute(
                        select(subagent_tasks).where(subagent_tasks.c.id == request.task_id)
                    )
                )
                .mappings()
                .first()
            )
            if task is None or task["agent_id"] != member["id"]:
                raise ServiceError("invalid_input", 422)
            if task["status"] not in ("accepted", "waiting_input"):
                raise SubagentError(
                    "subagent_task_closed",
                    ids={"agent_id": member["id"], "task_id": task["id"]},
                    action="任务不在可开跑状态；先等当前执行结束或恢复任务",
                )
            effective_target = dict(request.target)
            if "context" in effective_target:
                raise ServiceError("invalid_input", 422)
            effective_target["context"] = await descriptor(tree, member, activation, task)
            digest = _digest(
                {
                    "request": request.request.model_dump(mode="json"),
                    "target": effective_target,
                }
            )
            view = await self._store._insert_run(
                conn, request.request, effective_target, run_id, digest
            )
            await conn.execute(
                insert(subagent_child_runs).values(
                    run_id=run_id,
                    task_id=task["id"],
                    agent_id=member["id"],
                    activation_id=activation["id"],
                    tree_id=member["tree_id"],
                    owner_session_id=tree["session_id"],
                    owner_turn_id=request.owner_turn_id,
                    source_version=task["source_version"],
                    created_at=_now(),
                )
            )
            await conn.execute(
                update(subagent_tasks)
                .where(
                    subagent_tasks.c.id == task["id"],
                    subagent_tasks.c.status == task["status"],
                )
                .values(
                    run_id=run_id,
                    status="running",
                    revision=subagent_tasks.c.revision + 1,
                )
            )
            updated = (
                (
                    await conn.execute(
                        select(subagent_tasks).where(subagent_tasks.c.id == task["id"])
                    )
                )
                .mappings()
                .one()
            )
            return {
                "run": view.model_dump(mode="json"),
                "run_id": run_id,
                "task": _task_view(updated),
                "created": True,
            }

        async def confirm() -> dict[str, Any] | None:
            found = await self._store.by_key(request.request.request_key)
            if found is None:
                return None

            async def operation(conn: AsyncConnection) -> Any:
                return (
                    (
                        await conn.execute(
                            select(subagent_child_runs).where(
                                subagent_child_runs.c.run_id == found.id
                            )
                        )
                    )
                    .mappings()
                    .first()
                )

            mapping = await self._query(operation)
            if mapping is None or mapping["task_id"] != request.task_id:
                return None
            return {
                "run": found.model_dump(mode="json"),
                "run_id": found.id,
                "task": await self.task(request.task_id),
                "created": found.id == run_id,
            }

        return await self._submit(operation, confirm)

    async def set_task_status(
        self, task_id: str, *, status: str, expected_revision: int | None = None
    ) -> dict[str, Any]:
        """活跃态之间迁移（accepted→running、running↔waiting_input/awaiting_approval）；
        终态只能经 settle_result 同事务落结果与通知。"""
        if status not in ACTIVE_TASK_STATUS:
            raise ServiceError("invalid_input", 422)

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            task = (
                (
                    await conn.execute(
                        select(subagent_tasks).where(subagent_tasks.c.id == task_id)
                    )
                )
                .mappings()
                .first()
            )
            if task is None:
                raise ServiceError("not_found", 404)
            if expected_revision is not None and task["revision"] != expected_revision:
                raise ServiceError("revision_conflict")
            if task["status"] not in ACTIVE_TASK_STATUS:
                raise SubagentError(
                    "subagent_task_closed",
                    ids={"agent_id": task["agent_id"], "task_id": task_id},
                    action="任务已结算；状态不再变化",
                )
            await conn.execute(
                update(subagent_tasks)
                .where(
                    subagent_tasks.c.id == task_id,
                    subagent_tasks.c.status == task["status"],
                )
                .values(
                    status=status,
                    run_id=task["run_id"],
                    revision=subagent_tasks.c.revision + 1,
                )
            )
            row = (
                (
                    await conn.execute(
                        select(subagent_tasks).where(subagent_tasks.c.id == task_id)
                    )
                )
                .mappings()
                .one()
            )
            return _task_view(row)

        async def confirm() -> dict[str, Any] | None:
            found = await self.task(task_id)
            return found if found["status"] == status else None

        return await self._submit(operation, confirm)

    async def close_task_input(
        self, task_id: str, *, expected_revision: int | None = None
    ) -> dict[str, Any]:
        """持久关闭任务输入（最终响应已到、settle 尚未发生的间隙）。

        事务内核对该任务没有未消费（未附入 checkpoint）的消息后才 CAS 关闭；
        仍有消息时返回 closed=False，调用方继续消费后重试。关闭后
        accept_message 对该任务的绑定一律 subagent_task_closed。
        """

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            task = (
                (
                    await conn.execute(
                        select(subagent_tasks).where(subagent_tasks.c.id == task_id)
                    )
                )
                .mappings()
                .first()
            )
            if task is None:
                raise ServiceError("not_found", 404)
            if task["input_closed"]:
                return {"task": _task_view(task), "closed": True}
            if expected_revision is not None and task["revision"] != expected_revision:
                raise ServiceError("revision_conflict")
            unconsumed = (
                await conn.scalar(
                    select(func.count())
                    .select_from(subagent_messages)
                    .where(
                        subagent_messages.c.task_id == task_id,
                        subagent_messages.c.checkpoint_id.is_(None),
                        # queue_only 不阻止关闭：空闲排队的低优先消息随任务
                        # 一起终态；正在执行时域层会全部消费入本轮边界。
                        subagent_messages.c.wake == "auto",
                    )
                )
            )
            if unconsumed:
                return {
                    "task": _task_view(task),
                    "closed": False,
                    "unconsumed_messages": int(unconsumed),
                }
            await conn.execute(
                update(subagent_tasks)
                .where(
                    subagent_tasks.c.id == task_id,
                    subagent_tasks.c.input_closed.is_(False),
                )
                .values(
                    input_closed=True,
                    revision=subagent_tasks.c.revision + 1,
                )
            )
            row = (
                (
                    await conn.execute(
                        select(subagent_tasks).where(subagent_tasks.c.id == task_id)
                    )
                )
                .mappings()
                .one()
            )
            return {"task": _task_view(row), "closed": True}

        async def confirm() -> dict[str, Any] | None:
            found = await self.task(task_id)
            if found["input_closed"]:
                return {"task": found, "closed": True}
            return None

        return await self._submit(operation, confirm)

    async def task(self, task_id: str) -> dict[str, Any]:
        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            row = (
                (
                    await conn.execute(
                        select(subagent_tasks).where(subagent_tasks.c.id == task_id)
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                raise ServiceError("not_found", 404)
            return _task_view(row, with_input=True)

        return await self._query(operation)

    async def tasks(
        self, agent_id: str, *, after_ordinal: int = 0, limit: int = 64
    ) -> list[dict[str, Any]]:
        if after_ordinal < 0 or not 1 <= limit <= 256:
            raise ServiceError("invalid_cursor", 416)

        async def operation(conn: AsyncConnection) -> list[dict[str, Any]]:
            rows = (
                (
                    await conn.execute(
                        select(subagent_tasks)
                        .where(
                            subagent_tasks.c.agent_id == agent_id,
                            subagent_tasks.c.ordinal > after_ordinal,
                        )
                        .order_by(subagent_tasks.c.ordinal)
                        .limit(limit)
                    )
                )
                .mappings()
                .all()
            )
            return [_task_view(row, with_input=True) for row in rows]

        return await self._query(operation)

    async def settle_result(self, request: SettleResult) -> dict[str, Any]:
        """单次结算：同事务写结果与待通知记录；重放同内容返回原结果。"""
        body = {
            "stop_reason": request.stop_reason,
            "code": request.code,
            "output": request.output,
            "diagnostics": request.diagnostics,
        }
        content_hash = _digest(body)
        result_id = str(uuid.uuid4())
        notification_id = str(uuid.uuid4())

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            task = (
                (
                    await conn.execute(
                        select(subagent_tasks).where(subagent_tasks.c.id == request.task_id)
                    )
                )
                .mappings()
                .first()
            )
            if task is None:
                raise ServiceError("not_found", 404)
            settled = (
                (
                    await conn.execute(
                        select(subagent_results).where(
                            subagent_results.c.task_id == request.task_id
                        )
                    )
                )
                .mappings()
                .first()
            )
            if settled is not None:
                if settled["content_hash"] != content_hash:
                    raise ServiceError("revision_conflict")
                notification = (
                    (
                        await conn.execute(
                            select(subagent_notifications).where(
                                subagent_notifications.c.result_id == settled["id"]
                            )
                        )
                    )
                    .mappings()
                    .first()
                )
                if notification is None:
                    raise ServiceError("facts_corrupt")
                member = await _member_row(conn, task["agent_id"])
                return {
                    "result": _result_view(settled),
                    "notification": _notification_view(notification, settled),
                    "created": False,
                    "agent_id": member["id"],
                }
            if request.expected_task_revision is not None and (
                task["revision"] != request.expected_task_revision
            ):
                raise ServiceError("revision_conflict")
            if task["status"] in TERMINAL_TASK_STATUS:
                raise ServiceError("facts_corrupt")
            member = await _member_row(conn, task["agent_id"])
            seq = (
                await conn.scalar(
                    select(func.coalesce(func.max(subagent_notifications.c.seq), 0)).where(
                        subagent_notifications.c.tree_id == member["tree_id"]
                    )
                )
            ) + 1
            await conn.execute(
                insert(subagent_results).values(
                    id=result_id,
                    task_id=task["id"],
                    run_id=request.run_id or task["run_id"],
                    stop_reason=request.stop_reason,
                    code=request.code,
                    output=request.output,
                    diagnostics=request.diagnostics,
                    content_hash=content_hash,
                    settled_at=_now(),
                )
            )
            await conn.execute(
                insert(subagent_notifications).values(
                    id=notification_id,
                    tree_id=member["tree_id"],
                    result_id=result_id,
                    task_id=task["id"],
                    producer_id=member["id"],
                    # 父已死时上溯最近活跃祖先（至树根），避免通知挂在
                    # 无 run 的中间成员名下永不消费。
                    consumer_id=await _resolve_consumer(conn, member),
                    seq=seq,
                    status="pending",
                    checkpoint_id=None,
                    suppress_reason=None,
                    created_at=_now(),
                    updated_at=_now(),
                )
            )
            await conn.execute(
                update(subagent_tasks)
                .where(
                    subagent_tasks.c.id == task["id"],
                    subagent_tasks.c.status == task["status"],
                )
                .values(
                    status=request.stop_reason,
                    run_id=request.run_id or task["run_id"],
                    revision=subagent_tasks.c.revision + 1,
                    finished_at=_now(),
                )
            )
            result = (
                (
                    await conn.execute(
                        select(subagent_results).where(subagent_results.c.id == result_id)
                    )
                )
                .mappings()
                .one()
            )
            notification = (
                (
                    await conn.execute(
                        select(subagent_notifications).where(
                            subagent_notifications.c.id == notification_id
                        )
                    )
                )
                .mappings()
                .one()
            )
            return {
                "result": _result_view(result),
                "notification": _notification_view(notification, result),
                "created": True,
                "agent_id": member["id"],
            }

        async def confirm() -> dict[str, Any] | None:
            async def operation(conn: AsyncConnection) -> Any:
                return (
                    (
                        await conn.execute(
                            select(subagent_results).where(
                                subagent_results.c.task_id == request.task_id
                            )
                        )
                    )
                    .mappings()
                    .first()
                )

            settled = await self._query(operation)
            if settled is None or settled["content_hash"] != content_hash:
                return None
            found = await self.result(request.task_id)
            return {
                "result": found,
                "notification": None,
                "created": found["result_id"] == result_id,
                "agent_id": None,
            }

        return await self._submit(operation, confirm)

    async def result(self, task_id: str) -> dict[str, Any] | None:
        async def operation(conn: AsyncConnection) -> dict[str, Any] | None:
            row = (
                (
                    await conn.execute(
                        select(subagent_results).where(subagent_results.c.task_id == task_id)
                    )
                )
                .mappings()
                .first()
            )
            return _result_view(row) if row is not None else None

        return await self._query(operation)

    # ---------- 通知与父输入 checkpoint ----------

    async def notifications(
        self,
        tree_id: str,
        *,
        consumer_id: str | None,
        after_seq: int = 0,
        limit: int = 64,
        include_attached: bool = False,
    ) -> list[dict[str, Any]]:
        """按游标增量读取完成通知；consumer_id=None 为宿主消费（根成员结果）。"""
        if after_seq < 0 or not 1 <= limit <= 256:
            raise ServiceError("invalid_cursor", 416)
        statuses = ("pending", "attached") if include_attached else ("pending",)

        async def operation(conn: AsyncConnection) -> list[dict[str, Any]]:
            rows = (
                (
                    await conn.execute(
                        select(
                            *[
                                column.label(f"n_{column.name}")
                                for column in subagent_notifications.c
                            ],
                            *[
                                column.label(f"r_{column.name}") for column in subagent_results.c
                            ],
                        )
                        .join(
                            subagent_results,
                            subagent_results.c.id == subagent_notifications.c.result_id,
                        )
                        .where(
                            subagent_notifications.c.tree_id == tree_id,
                            subagent_notifications.c.seq > after_seq,
                            subagent_notifications.c.status.in_(statuses),
                            (
                                subagent_notifications.c.consumer_id == consumer_id
                                if consumer_id is not None
                                else subagent_notifications.c.consumer_id.is_(None)
                            ),
                        )
                        .order_by(subagent_notifications.c.seq)
                        .limit(limit)
                    )
                )
                .mappings()
                .all()
            )
            view: list[dict[str, Any]] = []
            for row in rows:
                notification_row = {key[2:]: row[key] for key in row.keys() if key.startswith("n_")}
                result_row = {key[2:]: row[key] for key in row.keys() if key.startswith("r_")}
                view.append(_notification_view(notification_row, result_row))
            return view

        return await self._query(operation)

    async def commit_input(self, request: CommitInput) -> dict[str, Any]:
        """输入边界：checkpoint、收件箱与通知消费游标同一事务推进，防重复附入。"""
        payload_hash = _digest(request.payload)
        checkpoint_id = str(uuid.uuid4())

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            member = await _member_row(conn, request.agent_id)
            if request.run_id is not None:
                run_exists = (
                    await conn.scalar(select(runs.c.id).where(runs.c.id == request.run_id))
                )
                if run_exists is None:
                    raise ServiceError("not_found", 404)
            if request.expected_inbox_cursor is not None and (
                member["inbox_cursor"] != request.expected_inbox_cursor
            ):
                raise ServiceError("revision_conflict")
            if request.expected_notify_cursor is not None and (
                member["notify_cursor"] != request.expected_notify_cursor
            ):
                raise ServiceError("revision_conflict")
            messages: list[Any] = []
            for message_id in request.message_ids:
                row = (
                    (
                        await conn.execute(
                            select(subagent_messages).where(
                                subagent_messages.c.id == message_id
                            )
                        )
                    )
                    .mappings()
                    .first()
                )
                if row is None or row["target_id"] != member["id"]:
                    raise ServiceError("invalid_input", 422)
                if row["checkpoint_id"] is not None:
                    raise ServiceError("revision_conflict")
                messages.append(row)
            notifications: list[Any] = []
            for item_id in request.notification_ids:
                row = (
                    (
                        await conn.execute(
                            select(subagent_notifications).where(
                                subagent_notifications.c.id == item_id
                            )
                        )
                    )
                    .mappings()
                    .first()
                )
                if (
                    row is None
                    or row["consumer_id"] != member["id"]
                    or row["status"] != "pending"
                ):
                    raise ServiceError("revision_conflict")
                notifications.append(row)
            checkpoint_seq = (
                await conn.scalar(
                    select(func.coalesce(func.max(subagent_input_checkpoints.c.seq), 0)).where(
                        subagent_input_checkpoints.c.agent_id == member["id"]
                    )
                )
            ) + 1
            await conn.execute(
                insert(subagent_input_checkpoints).values(
                    id=checkpoint_id,
                    agent_id=member["id"],
                    seq=checkpoint_seq,
                    run_id=request.run_id,
                    payload=request.payload,
                    fingerprint=payload_hash,
                    created_at=_now(),
                )
            )
            if request.run_id is not None:
                # SDK 停靠恢复读的是真实 checkpoint：消费后的真实 history 必须
                # 与领域 checkpoint、游标同事务落库，不能只写自定义快照。
                await conn.execute(
                    delete(park_checkpoints).where(
                        park_checkpoints.c.run_id == request.run_id
                    )
                )
                await conn.execute(
                    insert(park_checkpoints).values(
                        run_id=request.run_id,
                        checkpoint=request.payload,
                        fingerprint=payload_hash,
                        created_at=_now(),
                    )
                )
            if request.message_ids:
                await conn.execute(
                    update(subagent_messages)
                    .where(subagent_messages.c.id.in_(request.message_ids))
                    .values(checkpoint_id=checkpoint_id)
                )
            if request.notification_ids:
                await conn.execute(
                    update(subagent_notifications)
                    .where(subagent_notifications.c.id.in_(request.notification_ids))
                    .values(status="attached", checkpoint_id=checkpoint_id, updated_at=_now())
                )
            inbox_cursor = max(
                [member["inbox_cursor"]] + [row["seq"] for row in messages]
            )
            notify_cursor = max(
                [member["notify_cursor"]] + [row["seq"] for row in notifications]
            )
            await conn.execute(
                update(subagent_members)
                .where(subagent_members.c.id == member["id"])
                .values(
                    inbox_cursor=inbox_cursor,
                    notify_cursor=notify_cursor,
                    revision=subagent_members.c.revision + 1,
                )
            )
            return {
                "checkpoint_id": checkpoint_id,
                "message_count": len(messages),
                "notification_count": len(notifications),
                "inbox_cursor": inbox_cursor,
                "notify_cursor": notify_cursor,
                "park_checkpoint": request.run_id is not None,
                "created": True,
            }

        async def confirm() -> dict[str, Any] | None:
            found = await self.latest_checkpoint(request.agent_id)
            if found is None or found["fingerprint"] != payload_hash:
                return None
            if request.run_id is not None:

                async def operation(conn: AsyncConnection) -> Any:
                    return (
                        (
                            await conn.execute(
                                select(park_checkpoints).where(
                                    park_checkpoints.c.run_id == request.run_id
                                )
                            )
                        )
                        .mappings()
                        .first()
                    )

                parked = await self._query(operation)
                if parked is None or parked["fingerprint"] != payload_hash:
                    return None
            member = await self.get_agent(request.agent_id)
            return {
                "checkpoint_id": found["checkpoint_id"],
                "message_count": -1,
                "notification_count": -1,
                "inbox_cursor": member["inbox_cursor"],
                "notify_cursor": member["notify_cursor"],
                "park_checkpoint": request.run_id is not None,
                "created": found["checkpoint_id"] == checkpoint_id,
            }

        return await self._submit(operation, confirm)

    async def commit_step(
        self, agent_id: str, run_id: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        """step 提交边界（SDK on_step_commit）：同一事务更新真实 park checkpoint，
        并把该 run 输入 checkpoint 上已 attached 的通知推进到 consumed。

        仅附入采样输入不写本方法；payload 是完整 step checkpoint（messages + 已校验
        响应），即使没有 attached 通知也落库，保留响应中的工具调用关联供恢复。
        """
        payload_hash = _digest(payload)

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            member = await _member_row(conn, agent_id)
            mapping = (
                (
                    await conn.execute(
                        select(subagent_child_runs).where(
                            subagent_child_runs.c.run_id == run_id,
                            subagent_child_runs.c.agent_id == member["id"],
                        )
                    )
                )
                .mappings()
                .first()
            )
            if mapping is None:
                # 根成员的生产 run 走用户会话链（无 child_run 映射）：仅当该 run
                # 正是树当前绑定的 root_run 时接受；其余任意 run 一律拒绝。
                root_run = None
                if member["role"] == "root":
                    root_run = await conn.scalar(
                        select(subagent_trees.c.root_run_id).where(
                            subagent_trees.c.id == member["tree_id"]
                        )
                    )
                if member["role"] != "root" or root_run != run_id:
                    raise ServiceError("not_found", 404)
            checkpoint_ids = (
                (
                    await conn.execute(
                        select(subagent_input_checkpoints.c.id).where(
                            subagent_input_checkpoints.c.agent_id == member["id"],
                            subagent_input_checkpoints.c.run_id == run_id,
                        )
                    )
                )
                .scalars()
                .all()
            )
            attached = (
                (
                    await conn.execute(
                        select(subagent_notifications.c.id).where(
                            subagent_notifications.c.checkpoint_id.in_(
                                list(checkpoint_ids)
                            ),
                            subagent_notifications.c.status == "attached",
                        )
                    )
                )
                .scalars()
                .all()
            )
            if checkpoint_ids:
                await conn.execute(
                    update(subagent_notifications)
                    .where(
                        subagent_notifications.c.checkpoint_id.in_(list(checkpoint_ids)),
                        subagent_notifications.c.status == "attached",
                    )
                    .values(status="consumed", updated_at=_now())
                )
            await conn.execute(
                delete(park_checkpoints).where(park_checkpoints.c.run_id == run_id)
            )
            await conn.execute(
                insert(park_checkpoints).values(
                    run_id=run_id,
                    checkpoint=payload,
                    fingerprint=payload_hash,
                    created_at=_now(),
                )
            )
            return {
                "run_id": run_id,
                "agent_id": member["id"],
                "consumed": list(attached),
                "created": True,
            }

        async def confirm() -> dict[str, Any] | None:
            async def operation(conn: AsyncConnection) -> Any:
                checkpoint_ids = (
                    (
                        await conn.execute(
                            select(subagent_input_checkpoints.c.id).where(
                                subagent_input_checkpoints.c.agent_id == agent_id,
                                subagent_input_checkpoints.c.run_id == run_id,
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                consumed = (
                    (
                        await conn.execute(
                            select(subagent_notifications.c.id).where(
                                subagent_notifications.c.checkpoint_id.in_(
                                    list(checkpoint_ids)
                                ),
                                subagent_notifications.c.status == "consumed",
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                parked = (
                    (
                        await conn.execute(
                            select(park_checkpoints).where(park_checkpoints.c.run_id == run_id)
                        )
                    )
                    .mappings()
                    .first()
                )
                return parked, list(consumed)

            parked, consumed = await self._query(operation)
            if parked is None or parked["fingerprint"] != payload_hash:
                return None
            return {
                "run_id": run_id,
                "agent_id": agent_id,
                "consumed": consumed,
                "created": False,
            }

        return await self._submit(operation, confirm)

    async def commit_step_checkpoint(self, checkpoint_id: str) -> dict[str, Any]:
        """按 checkpoint 提交：仅推进该 checkpoint 上 attached 通知到 consumed。"""

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            checkpoint = (
                (
                    await conn.execute(
                        select(subagent_input_checkpoints).where(
                            subagent_input_checkpoints.c.id == checkpoint_id
                        )
                    )
                )
                .mappings()
                .first()
            )
            if checkpoint is None:
                raise ServiceError("not_found", 404)
            rows = (
                (
                    await conn.execute(
                        select(subagent_notifications.c.id).where(
                            subagent_notifications.c.checkpoint_id == checkpoint_id,
                            subagent_notifications.c.status == "attached",
                        )
                    )
                )
                .scalars()
                .all()
            )
            if rows:
                await conn.execute(
                    update(subagent_notifications)
                    .where(
                        subagent_notifications.c.id.in_(list(rows)),
                        subagent_notifications.c.status == "attached",
                    )
                    .values(status="consumed", updated_at=_now())
                )
            return {"checkpoint_id": checkpoint_id, "committed": list(rows)}

        async def confirm() -> dict[str, Any] | None:
            async def operation(conn: AsyncConnection) -> Any:
                return (
                    (
                        await conn.execute(
                            select(subagent_notifications.c.id).where(
                                subagent_notifications.c.checkpoint_id == checkpoint_id,
                                subagent_notifications.c.status == "consumed",
                            )
                        )
                    )
                    .scalars()
                    .all()
                )

            rows = await self._query(operation)
            if rows:
                return {"checkpoint_id": checkpoint_id, "committed": list(rows)}
            return None

        return await self._submit(operation, confirm)

    async def consume_notifications(
        self, notification_ids: list[str], *, expected_notify_cursor: int | None = None
    ) -> dict[str, Any]:
        """宿主直接消费（consumer_id 为空的通知）；成员通知必须走 commit_input。"""
        if not notification_ids:
            raise ServiceError("invalid_input", 422)

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            first = (
                (
                    await conn.execute(
                        select(subagent_notifications).where(
                            subagent_notifications.c.id == notification_ids[0]
                        )
                    )
                )
                .mappings()
                .first()
            )
            if first is None or first["consumer_id"] is not None:
                raise ServiceError("invalid_input", 422)
            tree_id = first["tree_id"]
            if expected_notify_cursor is not None:
                tree = await _tree_row(conn, tree_id)
                if tree["notify_cursor"] != expected_notify_cursor:
                    raise ServiceError("revision_conflict")
            rows = (
                (
                    await conn.execute(
                        select(subagent_notifications).where(
                            subagent_notifications.c.id.in_(notification_ids)
                        )
                    )
                )
                .mappings()
                .all()
            )
            for row in rows:
                if (
                    row["tree_id"] != tree_id
                    or row["consumer_id"] is not None
                    or row["status"] not in ("pending", "attached")
                ):
                    raise ServiceError("revision_conflict")
            await conn.execute(
                update(subagent_notifications)
                .where(
                    subagent_notifications.c.id.in_(notification_ids),
                    subagent_notifications.c.status.in_(("pending", "attached")),
                )
                .values(status="consumed", updated_at=_now())
            )
            cursor = await conn.scalar(
                select(func.coalesce(func.max(subagent_notifications.c.seq), 0)).where(
                    subagent_notifications.c.tree_id == tree_id
                )
            )
            if expected_notify_cursor is not None:
                await conn.execute(
                    update(subagent_trees)
                    .where(subagent_trees.c.id == tree_id)
                    .values(notify_cursor=cursor, revision=subagent_trees.c.revision + 1)
                )
            return {"consumed": notification_ids, "notify_cursor": cursor}

        async def confirm() -> dict[str, Any] | None:
            async def operation(conn: AsyncConnection) -> Any:
                return (
                    (
                        await conn.execute(
                            select(subagent_notifications).where(
                                subagent_notifications.c.id.in_(notification_ids),
                                subagent_notifications.c.status == "consumed",
                            )
                        )
                    )
                    .mappings()
                    .all()
                )

            rows = await self._query(operation)
            if len(rows) == len(notification_ids):
                return {"consumed": notification_ids, "notify_cursor": -1}
            return None

        return await self._submit(operation, confirm)

    async def suppress_notifications(
        self, notification_ids: list[str], *, reason: str
    ) -> dict[str, Any]:
        """过期或主动停止：标记 suppressed，结果与原因保留。"""
        if not notification_ids:
            raise ServiceError("invalid_input", 422)

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            rows = (
                (
                    await conn.execute(
                        select(subagent_notifications).where(
                            subagent_notifications.c.id.in_(notification_ids)
                        )
                    )
                )
                .mappings()
                .all()
            )
            if len(rows) != len(notification_ids):
                raise ServiceError("not_found", 404)
            affected = [
                row["id"]
                for row in rows
                if row["status"] in ("pending", "attached")
            ]
            if affected:
                await conn.execute(
                    update(subagent_notifications)
                    .where(
                        subagent_notifications.c.id.in_(affected),
                        subagent_notifications.c.status.in_(("pending", "attached")),
                    )
                    .values(
                        status="suppressed",
                        suppress_reason=reason[:64],
                        updated_at=_now(),
                    )
                )
            return {"suppressed": affected, "unchanged": [
                row["id"] for row in rows if row["id"] not in affected
            ]}

        async def confirm() -> dict[str, Any] | None:
            async def operation(conn: AsyncConnection) -> Any:
                return (
                    (
                        await conn.execute(
                            select(subagent_notifications).where(
                                subagent_notifications.c.id.in_(notification_ids),
                                subagent_notifications.c.status == "suppressed",
                            )
                        )
                    )
                    .mappings()
                    .all()
                )

            rows = await self._query(operation)
            if rows:
                return {"suppressed": [row["id"] for row in rows], "unchanged": []}
            return None

        return await self._submit(operation, confirm)

    async def latest_checkpoint(self, agent_id: str) -> dict[str, Any] | None:
        async def operation(conn: AsyncConnection) -> dict[str, Any] | None:
            row = (
                (
                    await conn.execute(
                        select(subagent_input_checkpoints)
                        .where(subagent_input_checkpoints.c.agent_id == agent_id)
                        .order_by(subagent_input_checkpoints.c.seq.desc())
                        .limit(1)
                    )
                )
                .mappings()
                .first()
            )
            return _checkpoint_view(row) if row is not None else None

        return await self._query(operation)

    # ---------- 独立历史 ----------

    async def save_history(
        self, agent_id: str, payload: dict[str, Any], *, expected_version: int | None = None
    ) -> dict[str, Any]:
        """成员独立历史快照按版本 CAS 覆盖；成员存在即可写（关闭前最后一次落盘）。"""
        payload_hash = _digest(payload)

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            # 成员存在即可写；不引用其余字段。
            await _member_row(conn, agent_id)
            row = (
                (
                    await conn.execute(
                        select(subagent_history).where(
                            subagent_history.c.agent_id == agent_id
                        )
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                if expected_version not in (None, 0):
                    raise ServiceError("revision_conflict")
                await conn.execute(
                    insert(subagent_history).values(
                        agent_id=agent_id,
                        version=1,
                        payload=payload,
                        fingerprint=payload_hash,
                        updated_at=_now(),
                    )
                )
                return {"agent_id": agent_id, "version": 1, "created": True}
            if expected_version is not None and row["version"] != expected_version:
                raise ServiceError("revision_conflict")
            await conn.execute(
                update(subagent_history)
                .where(
                    subagent_history.c.agent_id == agent_id,
                    subagent_history.c.version == row["version"],
                )
                .values(
                    version=row["version"] + 1,
                    payload=payload,
                    fingerprint=payload_hash,
                    updated_at=_now(),
                )
            )
            return {"agent_id": agent_id, "version": row["version"] + 1, "created": False}

        async def confirm() -> dict[str, Any] | None:
            found = await self.history(agent_id)
            if found is not None and found["fingerprint"] == payload_hash:
                return {
                    "agent_id": agent_id,
                    "version": found["version"],
                    "created": False,
                }
            return None

        return await self._submit(operation, confirm)

    async def history(self, agent_id: str) -> dict[str, Any] | None:
        async def operation(conn: AsyncConnection) -> dict[str, Any] | None:
            row = (
                (
                    await conn.execute(
                        select(subagent_history).where(
                            subagent_history.c.agent_id == agent_id
                        )
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                return None
            return {
                "agent_id": row["agent_id"],
                "version": row["version"],
                "payload": row["payload"],
                "fingerprint": row["fingerprint"],
                "updated_at": row["updated_at"],
            }

        return await self._query(operation)

    # ---------- 恢复 ----------

    async def recover(self, tree_id: str | None = None) -> dict[str, Any]:
        """重启恢复：旧驻留与未确认执行不视为存活；不自动重放任何任务。

        对全部无结果且处于 accepted/running/waiting_input 的任务同事务原子结算
        interrupted 并建立 pending 完成通知（消费方按最近活跃祖先解析），父恢复后
        能看到子任务死亡而不是永远等待。awaiting_approval 仅在 run 真实处于
        awaiting 且带停靠 checkpoint 时保留审批关联；否则同样结算。历史结果、
        未知副作用与旧 pending 通知一律不动，不重放。预算 lease 释放由域层
        recover 遍历本次结算任务调用 budgets.release 完成，存储不越权。
        """
        task_filter = (
            subagent_tasks.c.tree_id == tree_id if tree_id is not None else True
        )
        activation_filter = (
            subagent_activations.c.tree_id == tree_id if tree_id is not None else True
        )

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            pending_rows = (
                (
                    await conn.execute(
                        select(
                            subagent_tasks.c.id,
                            subagent_tasks.c.agent_id,
                            subagent_tasks.c.tree_id,
                            subagent_tasks.c.run_id,
                            subagent_tasks.c.status,
                        ).where(
                            task_filter,
                            subagent_tasks.c.status.in_(
                                ("accepted", "running", "waiting_input", "awaiting_approval")
                            ),
                        )
                    )
                )
                .mappings()
                .all()
            )
            seq = int(
                await conn.scalar(
                    select(func.coalesce(func.max(subagent_notifications.c.seq), 0)).where(
                        *(
                            [subagent_notifications.c.tree_id == tree_id]
                            if tree_id is not None
                            else []
                        )
                    )
                )
            )
            interrupted = 0
            created = 0
            preserved = 0
            for task in pending_rows:
                if task["status"] == "awaiting_approval" and task["run_id"] is not None:
                    run_status = await conn.scalar(
                        select(runs.c.status).where(runs.c.id == task["run_id"])
                    )
                    checkpointed = await conn.scalar(
                        select(func.count())
                        .select_from(park_checkpoints)
                        .where(park_checkpoints.c.run_id == task["run_id"])
                    )
                    if run_status == "awaiting" and checkpointed:
                        # 待批准执行保留原操作与停靠恢复点，等待显式答复。
                        preserved += 1
                        continue
                member = await _member_row(conn, task["agent_id"])
                settled = (
                    (
                        await conn.execute(
                            select(subagent_results.c.id).where(
                                subagent_results.c.task_id == task["id"]
                            )
                        )
                    )
                    .scalars()
                    .first()
                )
                if settled is None:
                    result_id = str(uuid.uuid4())
                    body = {
                        "stop_reason": "interrupted",
                        "code": "interrupted",
                        "output": {},
                        "diagnostics": None,
                    }
                    await conn.execute(
                        insert(subagent_results).values(
                            id=result_id,
                            task_id=task["id"],
                            run_id=task["run_id"],
                            stop_reason="interrupted",
                            code="interrupted",
                            output={},
                            diagnostics=None,
                            content_hash=_digest(body),
                            settled_at=_now(),
                        )
                    )
                    seq += 1
                    await conn.execute(
                        insert(subagent_notifications).values(
                            id=str(uuid.uuid4()),
                            tree_id=task["tree_id"],
                            result_id=result_id,
                            task_id=task["id"],
                            producer_id=member["id"],
                            consumer_id=await _resolve_consumer(conn, member),
                            seq=seq,
                            status="pending",
                            checkpoint_id=None,
                            suppress_reason=None,
                            created_at=_now(),
                            updated_at=_now(),
                        )
                    )
                    created += 1
                await conn.execute(
                    update(subagent_tasks)
                    .where(
                        subagent_tasks.c.id == task["id"],
                        subagent_tasks.c.status == task["status"],
                    )
                    .values(status="interrupted", finished_at=_now())
                )
                interrupted += 1
            live_rows = (
                (
                    await conn.execute(
                        select(subagent_activations.c.id).where(
                            activation_filter,
                            subagent_activations.c.status.in_(("starting", "resident")),
                        )
                    )
                )
                .scalars()
                .all()
            )
            if live_rows:
                await conn.execute(
                    update(subagent_activations)
                    .where(
                        subagent_activations.c.id.in_(list(live_rows)),
                        subagent_activations.c.status.in_(("starting", "resident")),
                    )
                    .values(
                        status="released",
                        cleanup_code="interrupted",
                        released_at=_now(),
                    )
                )
            closing_rows = (
                (
                    await conn.execute(
                        select(subagent_activations.c.id).where(
                            activation_filter, subagent_activations.c.status == "closing"
                        )
                    )
                )
                .scalars()
                .all()
            )
            return {
                "tasks_interrupted": interrupted,
                "notifications_created": created,
                "approvals_preserved": preserved,
                "activations_released": len(live_rows),
                "activations_pending_cleanup": len(closing_rows),
            }

        async def confirm() -> dict[str, Any] | None:
            # recover 幂等：核实残余未结算任务与存活实例归零即视为已提交；
            # 提交丢失前的具体计数不可知，以 -1 表示。
            async def read(conn: AsyncConnection) -> dict[str, Any]:
                unfinished = await conn.scalar(
                    select(func.count())
                    .select_from(subagent_tasks)
                    .where(
                        task_filter,
                        subagent_tasks.c.status.in_(("accepted", "running", "waiting_input")),
                    )
                )
                live = await conn.scalar(
                    select(func.count())
                    .select_from(subagent_activations)
                    .where(
                        activation_filter,
                        subagent_activations.c.status.in_(("starting", "resident")),
                    )
                )
                closing = await conn.scalar(
                    select(func.count())
                    .select_from(subagent_activations)
                    .where(activation_filter, subagent_activations.c.status == "closing")
                )
                return {
                    "tasks_interrupted": -1,
                    "notifications_created": -1,
                    "approvals_preserved": -1,
                    "activations_released": -1,
                    "activations_pending_cleanup": int(closing or 0),
                    "unrecovered_tasks": int(unfinished or 0),
                    "unrecovered_live": int(live or 0),
                }

            found = await self._query(read)
            if found["unrecovered_tasks"] == 0 and found["unrecovered_live"] == 0:
                return found
            return None

        return await self._submit(operation, confirm)
