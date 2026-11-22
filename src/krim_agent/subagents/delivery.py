"""通知改投与未启动任务结算的独立修复助手（interrupt/close/drain 后调用）。

`reparent_pending`：把一个成员名下无法再被消费的完成通知改投给"最近的
仍活跃且有活跃任务的祖先，root 兜底"。pending 恒迁移；attached 只有在该
consumer 的 run 已终态时才翻转回 pending 并清除 checkpoint 关联（父 run
仍存活时它还可能随 step 提交被消费，翻转会造成双重投递）；consumed 永不
复制。迁移重分配树内全局递增的新 seq，目标 consumer 的游标不可能跳过。

`settle_unstarted`：结算成员所有 `status=accepted 且 run_id is None` 的
任务（从未开跑，不存在未知副作用，不取消任何已执行操作），释放固化在
task.input 的预算租约，并把新通知的 consumer 按同一条改投规则落到合格
祖先/root。新通知 seq 本就是树内最大值 +1，目标游标不会跳过，只换
consumer 不重排 seq。

两个函数都只在 close/drain 已停止该子树准入之后调用；纯存储事务，不运行
模型、不发送消息、不唤醒代理。
"""

from __future__ import annotations

import time
from typing import Any

from sqlalchemy import func, select, update

from krim_agent.contracts import TERMINAL, ServiceError
from krim_agent.storage.models import runs
from krim_agent.storage.subagent_models import (
    subagent_child_runs,
    subagent_members,
    subagent_notifications,
    subagent_tasks,
    subagent_trees,
)

from .types import ACTIVE_TASK_STATUS, SettleResult

MAX_ANCESTOR_DEPTH = 64


async def _member_row(conn: Any, agent_id: str) -> Any:
    row = (
        (await conn.execute(select(subagent_members).where(subagent_members.c.id == agent_id)))
        .mappings()
        .first()
    )
    if row is None:
        raise ServiceError("not_found", 404)
    return row


async def _tree_row(conn: Any, tree_id: str) -> Any:
    row = (
        (await conn.execute(select(subagent_trees).where(subagent_trees.c.id == tree_id)))
        .mappings()
        .first()
    )
    if row is None:
        raise ServiceError("not_found", 404)
    return row


async def _has_active_task(conn: Any, agent_id: str) -> bool:
    found = (
        await conn.scalar(
            select(subagent_tasks.c.id)
            .where(
                subagent_tasks.c.agent_id == agent_id,
                subagent_tasks.c.status.in_(ACTIVE_TASK_STATUS),
            )
            .limit(1)
        )
    )
    return found is not None


async def _rule_target(conn: Any, member: Any) -> str:
    """改投规则的 consumer：最近"活跃且有活跃任务"的祖先，root 兜底。"""
    current: Any = member["parent_id"]
    for _ in range(MAX_ANCESTOR_DEPTH):
        if current is None:
            break
        row = await _member_row(conn, current)
        if row["status"] == "active" and await _has_active_task(conn, row["id"]):
            return str(row["id"])
        current = row["parent_id"]
    root = (
        await conn.scalar(
            select(subagent_members.c.id).where(
                subagent_members.c.tree_id == member["tree_id"],
                subagent_members.c.role == "root",
            )
        )
    )
    if root is None:
        raise ServiceError("facts_corrupt")
    return str(root)


async def _rule_target_id(store: Any, agent_id: str) -> str | None:
    async def read(conn: Any) -> str | None:
        member = (
            (await conn.execute(select(subagent_members).where(subagent_members.c.id == agent_id)))
            .mappings()
            .first()
        )
        if member is None:
            return None
        return await _rule_target(conn, member)

    return await store.work_read(read)


async def _consumer_run_id(conn: Any, member: Any) -> str | None:
    """consumer 当前输入所属的 run：root 看树根 run，成员看当前任务 run，
    再兜底最近一次 child_run。无 run 视为已终态。"""
    if member["role"] == "root":
        tree = await _tree_row(conn, member["tree_id"])
        run_id = tree["root_run_id"]
    else:
        run_id = None
        if member["current_task_id"] is not None:
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
            )
            if task is not None:
                run_id = task["run_id"]
    if run_id is None:
        run_id = (
            await conn.scalar(
                select(subagent_child_runs.c.run_id)
                .where(subagent_child_runs.c.agent_id == member["id"])
                .order_by(subagent_child_runs.c.created_at.desc())
                .limit(1)
            )
        )
    return str(run_id) if run_id is not None else None


async def _consumer_run_settled(conn: Any, member: Any) -> bool:
    run_id = await _consumer_run_id(conn, member)
    if run_id is None:
        return True
    status = await conn.scalar(select(runs.c.status).where(runs.c.id == run_id))
    return status is None or status in TERMINAL


async def reparent_pending(store: Any, agent_id: str) -> dict[str, Any]:
    """迁移 agent 名下未交付通知到合格祖先/root；返回 consumer 与通知 id。"""
    rule_target = await _rule_target_id(store, agent_id)
    if rule_target is None:
        raise ServiceError("not_found", 404)
    migrated: list[str] = []

    async def operation(conn: Any) -> dict[str, Any]:
        member = await _member_row(conn, agent_id)
        if rule_target == agent_id:
            return {"consumer_id": rule_target, "notification_ids": []}
        run_settled = await _consumer_run_settled(conn, member)
        statuses = ("pending",) if not run_settled else ("pending", "attached")
        rows = (
            (
                await conn.execute(
                    select(subagent_notifications)
                    .where(
                        subagent_notifications.c.consumer_id == agent_id,
                        subagent_notifications.c.status.in_(statuses),
                    )
                    .order_by(subagent_notifications.c.seq)
                )
            )
            .mappings()
            .all()
        )
        if not rows:
            return {"consumer_id": rule_target, "notification_ids": []}
        seq = (
            await conn.scalar(
                select(func.max(subagent_notifications.c.seq)).where(
                    subagent_notifications.c.tree_id == member["tree_id"]
                )
            )
        ) or 0
        now = time.time()
        for row in rows:
            seq += 1
            # CAS：consumer/状态与读取时一致才迁移；域边界并发改动即拒。
            changed = await conn.execute(
                update(subagent_notifications)
                .where(
                    subagent_notifications.c.id == row["id"],
                    subagent_notifications.c.consumer_id == agent_id,
                    subagent_notifications.c.status == row["status"],
                )
                .values(
                    consumer_id=rule_target,
                    seq=seq,
                    status="pending",
                    checkpoint_id=None,
                    updated_at=now,
                )
            )
            if changed.rowcount != 1:
                raise ServiceError("revision_conflict")
            migrated.append(str(row["id"]))
        return {"consumer_id": rule_target, "notification_ids": migrated}

    async def confirm() -> dict[str, Any] | None:
        if not migrated:
            return {"consumer_id": rule_target, "notification_ids": []}

        async def probe(conn: Any) -> dict[str, Any] | None:
            values = (
                await conn.execute(
                    select(subagent_notifications.c.id, subagent_notifications.c.consumer_id).where(
                        subagent_notifications.c.id.in_(migrated)
                    )
                )
            ).all()
            if len(values) != len(migrated):
                return None
            if any(row[1] != rule_target for row in values):
                return None
            return {"consumer_id": rule_target, "notification_ids": migrated}

        return await store.work_read(probe)

    return await store.work_write(operation, confirm)


async def settle_unstarted(
    store: Any,
    repository: Any,
    budgets: Any,
    agent_id: str,
    reason: str,
) -> dict[str, Any]:
    """结算成员从未开跑（accepted 且 run_id 为空）的任务并释放其租约。

    reason 作为结算 code 与 output.reason 记录（stop_reason 固定 aborted：
    成员停止准入后的主动放弃）。仅处理无 run 的任务，不存在未知副作用，
    不取消任何已执行操作；只在 close/drain 停准入后调用。
    """
    if not isinstance(reason, str) or not reason.strip():
        raise ServiceError("invalid_input", 422)
    member = await repository.get_agent(agent_id)
    tree_id = member["tree_id"]
    tasks = await repository.tasks(agent_id)
    settled: list[str] = []
    released: list[str] = []
    reparented: list[str] = []
    skipped: list[str] = []

    rule_target = await _rule_target_id(store, agent_id)
    for task in tasks:
        if task["status"] != "accepted" or task["run_id"] is not None:
            continue
        try:
            await repository.settle_result(
                SettleResult(
                    task_id=task["task_id"],
                    stop_reason="aborted",
                    code=reason,
                    output={"text": "", "reason": reason},
                )
            )
        except ServiceError as error:
            # 他人已按不同内容结算：保留原结果，不覆盖。
            if error.code == "revision_conflict":
                skipped.append(task["task_id"])
                continue
            raise
        settled.append(task["task_id"])
        budget_key = (task.get("input") or {}).get("budget_key")
        if isinstance(budget_key, str):
            await budgets.release(tree_id, budget_key)
            released.append(budget_key)
        if rule_target is not None and rule_target != member["parent_id"]:
            notification_id = await _reassign_consumer(store, task["task_id"], rule_target)
            if notification_id is not None:
                reparented.append(notification_id)
    return {
        "settled_task_ids": settled,
        "released_budget_keys": released,
        "reparented_notification_ids": reparented,
        "skipped_task_ids": skipped,
    }


async def _reassign_consumer(store: Any, task_id: str, target_id: str) -> str | None:
    """把刚结算任务的通知 consumer 改到合格目标；seq 已是全局最大 +1，不重排。"""

    async def operation(conn: Any) -> str | None:
        row = (
            (
                await conn.execute(
                    select(
                        subagent_notifications.c.id,
                        subagent_notifications.c.consumer_id,
                    ).where(
                        subagent_notifications.c.task_id == task_id,
                        subagent_notifications.c.status == "pending",
                    )
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            return None
        # CAS：consumer/状态与读取时一致才改投。
        changed = await conn.execute(
            update(subagent_notifications)
            .where(
                subagent_notifications.c.id == row["id"],
                subagent_notifications.c.consumer_id == row["consumer_id"],
                subagent_notifications.c.status == "pending",
            )
            .values(consumer_id=target_id, updated_at=time.time())
        )
        if changed.rowcount != 1:
            raise ServiceError("revision_conflict")
        return str(row["id"])

    async def confirm() -> str | None:
        async def probe(conn: Any) -> str | None:
            return (
                await conn.scalar(
                    select(subagent_notifications.c.id).where(
                        subagent_notifications.c.task_id == task_id,
                        subagent_notifications.c.consumer_id == target_id,
                    )
                )
            )

        found = await store.work_read(probe)
        return str(found) if found is not None else None

    return await store.work_write(operation, confirm)


__all__ = ["reparent_pending", "settle_unstarted"]
