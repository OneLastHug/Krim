"""delivery 助手验收：通知改投规则与未启动任务结算（真实 Store/Repository）。

树/成员行沿用 test_subagent_team 的夹具（表约束级准备）；结算/通知走真实
SubagentStore.settle_result，预算走真实 RequestBudgets。
"""

from __future__ import annotations

import time
import uuid
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import func, insert, select, update
from test_subagent_team import seed_tree, write

from krim_agent.contracts import RunRequest, ServiceError, fingerprint
from krim_agent.storage.models import runs
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.storage.subagent_models import (
    subagent_input_checkpoints,
    subagent_members,
    subagent_notifications,
    subagent_tasks,
)
from krim_agent.subagents.budgets import RequestBudgets
from krim_agent.subagents.delivery import reparent_pending, settle_unstarted
from krim_agent.subagents.storage import SubagentStore
from krim_agent.subagents.types import SettleResult

NESTED_SPEC = (
    ("root", None, "root"),
    ("a", "root", "agent"),
    ("b", "a", "agent"),
    ("c", "b", "agent"),
)


async def seeded(tmp_path: Path, name: str = "delivery.sqlite", spec: Any = NESTED_SPEC):
    store = await SQLiteStore.open(tmp_path / name)
    ids = await seed_tree(store, "dl", members_spec=spec)
    return store, ids, SubagentStore(store), RequestBudgets(store)


async def seed_task(
    store: SQLiteStore,
    ids: dict[str, Any],
    member: str,
    *,
    status: str = "accepted",
    run_id: str | None = None,
    body: dict[str, Any] | None = None,
) -> str:
    task_id = str(uuid.uuid4())
    agent_id = ids["ids"][member]
    payload = body or {"goal": "seeded " + member}

    async def operation(conn: Any) -> None:
        ordinal = (
            await conn.scalar(
                select(func.max(subagent_tasks.c.ordinal)).where(
                    subagent_tasks.c.agent_id == agent_id
                )
            )
            or 0
        ) + 1
        await conn.execute(
            insert(subagent_tasks).values(
                id=task_id,
                agent_id=agent_id,
                tree_id=ids["tree_id"],
                ordinal=ordinal,
                status=status,
                run_id=run_id,
                parent_run_id=None,
                source_version=1,
                provider_version="v1",
                profile_version="v1",
                input=payload,
                input_hash=fingerprint(payload),
                request_key=f"seed:{task_id}@{agent_id}",
                request_hash=fingerprint({"task": task_id}),
                revision=1,
                input_closed=False,
                created_at=time.time(),
                finished_at=None,
            )
        )

    await write(store, operation)
    return task_id


async def settle_member_task(
    store: SQLiteStore,
    repository: SubagentStore,
    ids: dict[str, Any],
    member: str,
    *,
    text: str,
) -> dict[str, Any]:
    """给 member 建一个 accepted 任务并按真实路径结算 → 通知 consumer=其父。"""
    task_id = await seed_task(store, ids, member, body={"goal": text})
    return await repository.settle_result(
        SettleResult(
            task_id=task_id,
            stop_reason="completed",
            code=None,
            output={"text": text},
        )
    )


async def strand_notification(
    store: SQLiteStore, notification_id: str, *, to_consumer: str
) -> None:
    """把通知挂回中间成员，构造"路由规则上线前的历史遗留"场景。

    真实 UPDATE 且 CAS：仅当仍为 pending 且 consumer 不是目标时生效，
    rowcount != 1 即失败，不静默。
    """
    async def operation(conn: Any) -> None:
        changed = await conn.execute(
            update(subagent_notifications)
            .where(
                subagent_notifications.c.id == notification_id,
                subagent_notifications.c.status == "pending",
                subagent_notifications.c.consumer_id != to_consumer,
            )
            .values(consumer_id=to_consumer, updated_at=time.time())
        )
        assert changed.rowcount == 1

    await write(store, operation)


async def member_closed(store: SQLiteStore, ids: dict[str, Any], member: str) -> None:
    async def operation(conn: Any) -> None:
        await conn.execute(
            update(subagent_members)
            .where(subagent_members.c.id == ids["ids"][member])
            .values(
                status="closed",
                closed_at=time.time(),
                revision=subagent_members.c.revision + 1,
            )
        )

    await write(store, operation)


async def notification_row(store: SQLiteStore, notification_id: str) -> dict[str, Any] | None:
    async def read(conn: Any) -> dict[str, Any] | None:
        row = (
            (
                await conn.execute(
                    select(subagent_notifications).where(
                        subagent_notifications.c.id == notification_id
                    )
                )
            )
            .mappings()
            .first()
        )
        return dict(row) if row is not None else None

    return await store.work_read(read)


async def seed_run(store: SQLiteStore, key: str, *, status: str) -> str:
    run, _ = await store.begin(
        RunRequest(request_key=key, text="run", target="demo"), {"key": "demo"}
    )

    async def operation(conn: Any) -> None:
        await conn.execute(update(runs).where(runs.c.id == run.id).values(status=status))

    await write(store, operation)
    return run.id


async def test_reparent_moves_pending_to_root_when_no_qualifying_ancestor(tmp_path: Path) -> None:
    store, ids, repository, _budgets = await seeded(tmp_path)
    try:
        settled = await settle_member_task(store, repository, ids, "c", text="孤儿通知")
        notification_id = settled["notification"]["notification_id"]
        before_seq = settled["notification"]["seq"]
        # 存储路由会把空闲中间成员的通知直接发 root；历史遗留场景显式挂回 b。
        await strand_notification(store, notification_id, to_consumer=ids["ids"]["b"])
        await member_closed(store, ids, "b")
        result = await reparent_pending(store, ids["ids"]["b"])
        assert result["consumer_id"] == ids["ids"]["root"]
        assert result["notification_ids"] == [notification_id]
        row = await notification_row(store, notification_id)
        assert row is not None
        assert row["consumer_id"] == ids["ids"]["root"]
        assert row["status"] == "pending"
        assert row["seq"] > before_seq
    finally:
        await store.close()


async def test_reparent_prefers_active_ancestor_with_active_task(tmp_path: Path) -> None:
    store, ids, repository, _budgets = await seeded(tmp_path)
    try:
        settled = await settle_member_task(store, repository, ids, "c", text="给中间层")
        notification_id = settled["notification"]["notification_id"]
        # a 活跃且持有活跃任务 → 最近合格祖先；root 兜底不生效。
        await seed_task(store, ids, "a", status="accepted")
        await strand_notification(store, notification_id, to_consumer=ids["ids"]["b"])
        await member_closed(store, ids, "b")
        result = await reparent_pending(store, ids["ids"]["b"])
        assert result["consumer_id"] == ids["ids"]["a"]
        row = await notification_row(store, notification_id)
        assert row is not None
        assert row["consumer_id"] == ids["ids"]["a"]
    finally:
        await store.close()


async def test_reparent_keeps_attached_while_consumer_run_is_alive(tmp_path: Path) -> None:
    store, ids, repository, _budgets = await seeded(tmp_path)
    try:
        settled = await settle_member_task(store, repository, ids, "c", text="已附入输入")
        notification_id = settled["notification"]["notification_id"]
        run_id = await seed_run(store, "dl-consumer-run", status="running")
        task_id = await seed_task(store, ids, "b", status="running", run_id=run_id)
        checkpoint_id = str(uuid.uuid4())
        await strand_notification(store, notification_id, to_consumer=ids["ids"]["b"])

        async def attach(conn: Any) -> None:
            await conn.execute(
                update(subagent_members)
                .where(subagent_members.c.id == ids["ids"]["b"])
                .values(current_task_id=task_id)
            )
            await conn.execute(
                insert(subagent_input_checkpoints).values(
                    id=checkpoint_id,
                    agent_id=ids["ids"]["b"],
                    seq=1,
                    payload={},
                    fingerprint=fingerprint({"checkpoint": 1}),
                    created_at=time.time(),
                )
            )
            await conn.execute(
                update(subagent_notifications)
                .where(subagent_notifications.c.id == notification_id)
                .values(status="attached", checkpoint_id=checkpoint_id)
            )

        await write(store, attach)
        await member_closed(store, ids, "b")
        result = await reparent_pending(store, ids["ids"]["b"])
        # consumer 的 run 仍存活：attached 通知不得翻转（避免双重投递）。
        assert result["notification_ids"] == []
        row = await notification_row(store, notification_id)
        assert row is not None
        assert row["consumer_id"] == ids["ids"]["b"]
        assert row["status"] == "attached"
        assert row["checkpoint_id"] == checkpoint_id

        async def fail_run(conn: Any) -> None:
            await conn.execute(update(runs).where(runs.c.id == run_id).values(status="failed"))

        await write(store, fail_run)
        migrated = await reparent_pending(store, ids["ids"]["b"])
        assert migrated["consumer_id"] == ids["ids"]["root"]
        assert migrated["notification_ids"] == [notification_id]
        row = await notification_row(store, notification_id)
        assert row is not None
        assert row["status"] == "pending"
        assert row["checkpoint_id"] is None
        assert row["consumer_id"] == ids["ids"]["root"]
    finally:
        await store.close()


async def test_reparent_never_touches_consumed_notifications(tmp_path: Path) -> None:
    store, ids, repository, _budgets = await seeded(tmp_path)
    try:
        settled = await settle_member_task(store, repository, ids, "c", text="已消费")
        notification_id = settled["notification"]["notification_id"]
        await strand_notification(store, notification_id, to_consumer=ids["ids"]["b"])

        async def consume(conn: Any) -> None:
            await conn.execute(
                update(subagent_notifications)
                .where(subagent_notifications.c.id == notification_id)
                .values(status="consumed", updated_at=time.time())
            )

        await write(store, consume)
        await member_closed(store, ids, "b")
        result = await reparent_pending(store, ids["ids"]["b"])
        assert result["notification_ids"] == []
        row = await notification_row(store, notification_id)
        assert row is not None
        assert row["status"] == "consumed"
        assert row["consumer_id"] == ids["ids"]["b"]
    finally:
        await store.close()


async def test_reparent_on_the_root_itself_is_a_noop(tmp_path: Path) -> None:
    store, ids, repository, _budgets = await seeded(
        tmp_path, spec=(("root", None, "root"), ("w1", "root", "agent"))
    )
    try:
        settled = await settle_member_task(store, repository, ids, "w1", text="根名下通知")
        notification_id = settled["notification"]["notification_id"]
        assert settled["notification"]["consumer_id"] == ids["ids"]["root"]
        result = await reparent_pending(store, ids["ids"]["root"])
        assert result["consumer_id"] == ids["ids"]["root"]
        assert result["notification_ids"] == []
        row = await notification_row(store, notification_id)
        assert row is not None
        assert row["consumer_id"] == ids["ids"]["root"]
    finally:
        await store.close()


async def test_reparent_unknown_agent_is_not_found(tmp_path: Path) -> None:
    store, _ids, _repository, _budgets = await seeded(tmp_path)
    try:
        with pytest.raises(ServiceError, match="not_found"):
            await reparent_pending(store, str(uuid.uuid4()))
    finally:
        await store.close()


async def test_settle_unstarted_settles_accepted_tasks_and_releases_lease(tmp_path: Path) -> None:
    store, ids, repository, budgets = await seeded(
        tmp_path, spec=(("root", None, "root"), ("w1", "root", "agent"))
    )
    try:
        await budgets.ensure(ids["tree_id"], 4)
        await budgets.reserve(ids["tree_id"], "lease-1", 2)
        task_id = await seed_task(
            store, ids, "w1", body={"goal": "排队任务", "budget_key": "lease-1"}
        )
        result = await settle_unstarted(
            store, repository, budgets, ids["ids"]["w1"], "member_closed"
        )
        assert result["settled_task_ids"] == [task_id]
        assert result["released_budget_keys"] == ["lease-1"]
        assert result["reparented_notification_ids"] == []
        settled = await repository.result(task_id)
        assert settled is not None
        assert settled["stop_reason"] == "aborted"
        assert settled["code"] == "member_closed"
        state = await budgets.state(ids["tree_id"])
        assert state["active_leases"] == []
        # w1 的父就是 root 且 root 即兜底目标：通知落在 root 名下待消费。
        notices = await repository.notifications(ids["tree_id"], consumer_id=ids["ids"]["root"])
        assert [item["task_id"] for item in notices] == [task_id]
    finally:
        await store.close()


async def test_settle_unstarted_reparents_notification_past_closed_ancestor(tmp_path: Path) -> None:
    store, ids, repository, budgets = await seeded(tmp_path)
    try:
        task_id = await seed_task(store, ids, "c", body={"goal": "孙子任务"})
        await member_closed(store, ids, "b")
        result = await settle_unstarted(store, repository, budgets, ids["ids"]["c"], "drain")
        assert result["settled_task_ids"] == [task_id]
        assert result["reparented_notification_ids"]
        notification_id = result["reparented_notification_ids"][0]
        row = await notification_row(store, notification_id)
        assert row is not None
        # 通知原 consumer=b（已关闭），按规则落到 root，seq 保持全局递增。
        assert row["consumer_id"] == ids["ids"]["root"]
        assert row["status"] == "pending"
    finally:
        await store.close()


async def test_settle_unstarted_skips_started_and_non_accepted_tasks(tmp_path: Path) -> None:
    store, ids, repository, budgets = await seeded(
        tmp_path, spec=(("root", None, "root"), ("w1", "root", "agent"))
    )
    try:
        run_id = await seed_run(store, "dl-started-run", status="running")
        started = await seed_task(
            store, ids, "w1", status="accepted", run_id=run_id, body={"goal": "已有 run"}
        )
        running = await seed_task(store, ids, "w1", status="running", body={"goal": "执行中"})
        pending = await seed_task(store, ids, "w1", body={"goal": "排队中"})
        result = await settle_unstarted(store, repository, budgets, ids["ids"]["w1"], "drain")
        assert result["settled_task_ids"] == [pending]

        async def read_status(conn: Any, task_id: str) -> str | None:
            return await conn.scalar(
                select(subagent_tasks.c.status).where(subagent_tasks.c.id == task_id)
            )

        for task_id, expected in ((started, "accepted"), (running, "running")):
            status = await store.work_read(lambda conn, task_id=task_id: read_status(conn, task_id))
            assert status == expected
    finally:
        await store.close()


async def test_settle_unstarted_requires_a_reason(tmp_path: Path) -> None:
    store, ids, repository, budgets = await seeded(
        tmp_path, spec=(("root", None, "root"), ("w1", "root", "agent"))
    )
    try:
        with pytest.raises(ServiceError, match="invalid_input"):
            await settle_unstarted(store, repository, budgets, ids["ids"]["w1"], "  ")
    finally:
        await store.close()


async def test_settle_unstarted_unknown_agent_is_not_found(tmp_path: Path) -> None:
    store, _ids, repository, budgets = await seeded(tmp_path)
    try:
        with pytest.raises(ServiceError, match="not_found"):
            await settle_unstarted(store, repository, budgets, str(uuid.uuid4()), "drain")
    finally:
        await store.close()
