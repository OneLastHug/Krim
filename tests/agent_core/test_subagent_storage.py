"""阶段1子代理存储：schema20迁移、树/成员/驻留/任务/消息/结果/通知与恢复。"""

import asyncio
import json
import sqlite3

import pytest
from sqlalchemy import insert
from sqlalchemy.sql.ddl import sort_tables

from krim_agent.contracts import RunRequest, ServiceError
from krim_agent.storage import sessions as session_sql
from krim_agent.storage import work as work_sql
from krim_agent.storage.models import metadata, sessions
from krim_agent.storage.sqlite import SCHEMA_VERSION, SQLiteStore
from krim_agent.storage.subagent_budget_models import BUDGET_TABLE_NAMES
from krim_agent.storage.subagent_models import SUBAGENT_TABLE_NAMES
from krim_agent.storage.subagent_team_models import TEAM_TABLE_NAMES
from krim_agent.subagents import (
    AcceptSpawn,
    CommitInput,
    ReserveAgent,
    SendMessage,
    SettleResult,
    StartTask,
    SubagentError,
    SubagentStore,
    TreeScope,
)


def start_request(task_id: str, ordinal: int = 1) -> RunRequest:
    return RunRequest(
        text="执行子任务", request_key=f"ac-child.{task_id}.{ordinal}", target="target-http"
    )


async def run_task(sub, reserved, spawn) -> str:
    started = await sub.start_task(
        StartTask(
            agent_id=reserved["agent"]["agent_id"],
            task_id=spawn["task_id"],
            activation_id=reserved["activation"]["activation_id"],
            request=start_request(spawn["task_id"]),
            target={"k": 1},
        )
    )
    return started["run_id"]


def raw(path, names):
    conn = sqlite3.connect(path)
    try:
        return {name: conn.execute(f'SELECT * FROM "{name}"').fetchall() for name in names}
    finally:
        conn.close()


def table_names(path):
    conn = sqlite3.connect(path)
    try:
        return {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    finally:
        conn.close()


def downgrade_to_19(path):
    """从当前完整表集逐层删除到 schema19 白名单；只读门与升级测试共用。"""
    conn = sqlite3.connect(path)
    try:
        schema20_tables = SUBAGENT_TABLE_NAMES | TEAM_TABLE_NAMES | BUDGET_TABLE_NAMES
        ordered = sort_tables([metadata.tables[name] for name in schema20_tables])
        for table in reversed(ordered):
            conn.execute(f'DROP TABLE "{table.name}"')
        conn.execute("PRAGMA user_version=19")
        conn.commit()
    finally:
        conn.close()


async def make_session(store, session_id="s-1"):
    async with store.engine.begin() as conn:
        await conn.execute(
            insert(sessions).values(
                id=session_id,
                workspace_id="ws-1",
                owner_id="owner-1",
                create_key="k-" + session_id,
                request_hash="hash",
                title="宿主会话",
                target_key="target",
                target={},
                context_policy={},
                revision=1,
                status="open",
                dispatch_paused=False,
                created_at=1.0,
            )
        )


async def spawn_child(sub, tree_id, parent_id, path, key, *, input_body=None):
    reserved = await sub.reserve_agent(
        ReserveAgent(
            tree_id=tree_id,
            parent_id=parent_id,
            path=path,
            role="agent",
            provider="local-sdk",
            profile="worker",
            provider_version="prov-1",
            profile_version="prof-1",
            request_key=key,
        )
    )
    assert reserved["created"] is True
    spawn = await sub.accept_spawn(
        AcceptSpawn(
            agent_id=reserved["agent"]["agent_id"],
            activation_id=reserved["activation"]["activation_id"],
            input=input_body or {"text": "任务"},
        )
    )
    return reserved, spawn


async def build_tree(store):
    """宿主会话 + 根成员 + 一名子成员；返回常用句柄。"""
    await make_session(store)
    sub = SubagentStore(store)
    tree = await sub.ensure_tree(
        TreeScope(session_id="s-1", workspace_id="ws-1", owner_id="owner-1", request_key="tree-1")
    )
    assert tree["created"] is True
    reserved = await sub.reserve_agent(
        ReserveAgent(
            tree_id=tree["tree_id"],
            path="lead",
            role="root",
            provider="local-sdk",
            profile="lead",
            provider_version="prov-1",
            profile_version="prof-1",
            request_key="root-key",
        )
    )
    root_id = reserved["agent"]["agent_id"]
    root_spawn = await sub.accept_spawn(
        AcceptSpawn(
            agent_id=root_id,
            activation_id=reserved["activation"]["activation_id"],
            input={"text": "协调"},
        )
    )
    return sub, tree, root_id, reserved["activation"]["activation_id"], root_spawn


async def test_fresh_store_has_current_schema_with_subagent_tables(tmp_path):
    database = tmp_path / "fresh.sqlite"
    store = await SQLiteStore.open(database)
    try:
        assert SCHEMA_VERSION == 21
        assert store.schema_version == SCHEMA_VERSION
        assert SUBAGENT_TABLE_NAMES <= set(metadata.tables)
        assert SUBAGENT_TABLE_NAMES <= table_names(database)
        assert TEAM_TABLE_NAMES <= table_names(database)
        assert BUDGET_TABLE_NAMES <= table_names(database)
        session_sql.require(store)
        work_sql.require(store)
        SubagentStore(store)
    finally:
        await store.close()


async def test_schema19_upgrade_preserves_rows_and_adds_tables(tmp_path):
    database = tmp_path / "upgrade.sqlite"
    store = await SQLiteStore.open(database)
    view, created = await store.begin(RunRequest(text="保留", request_key="keep-1"), {"v": 1})
    assert created is True
    await store.close()
    downgrade_to_19(database)
    assert "subagent_trees" not in table_names(database)
    store = await SQLiteStore.open(database)
    try:
        assert store.schema_version == SCHEMA_VERSION
        assert SUBAGENT_TABLE_NAMES <= table_names(database)
        snapshot = await store.snapshot(view.id)
        assert snapshot.run.request_key == "keep-1"
        assert snapshot.run.status == "queued"
        assert raw(database, ["subagent_trees"])["subagent_trees"] == []
    finally:
        await store.close()


async def test_readonly_schema19_store_blocks_subagent_access(tmp_path):
    database = tmp_path / "legacy.sqlite"
    store = await SQLiteStore.open(database)
    view, _ = await store.begin(RunRequest(text="旧", request_key="old-1"), {})
    await store.close()
    downgrade_to_19(database)
    store = await SQLiteStore.open(database, readonly=True)
    try:
        assert store.schema_version == 19
        snapshot = await store.snapshot(view.id)
        assert snapshot.run.request_key == "old-1"
        with pytest.raises(ServiceError) as exc:
            SubagentStore(store)
        assert exc.value.code == "schema_upgrade_required"
    finally:
        await store.close()


async def test_ensure_tree_idempotent_with_hash_conflict(tmp_path):
    store = await SQLiteStore.open(tmp_path / "trees.sqlite")
    try:
        await make_session(store)
        sub = SubagentStore(store)
        scope = TreeScope(
            session_id="s-1", workspace_id="ws-1", owner_id="owner-1", request_key="t1"
        )
        first = await sub.ensure_tree(scope)
        assert first["created"] is True
        assert first["status"] == "open" and first["input_version"] == 1
        again = await sub.ensure_tree(scope)
        assert again["created"] is False and again["tree_id"] == first["tree_id"]
        shifted = TreeScope(
            session_id="s-1", workspace_id="ws-2", owner_id="owner-1", request_key="t1"
        )
        with pytest.raises(ServiceError) as exc:
            await sub.ensure_tree(shifted)
        assert exc.value.code == "key_conflict"
        with pytest.raises(ServiceError) as exc:
            await sub.tree("missing")
        assert exc.value.status == 404
    finally:
        await store.close()


async def test_tree_input_version_and_close_gates(tmp_path):
    store = await SQLiteStore.open(tmp_path / "gates.sqlite")
    try:
        sub, tree, root_id, _, _ = await build_tree(store)
        with pytest.raises(ServiceError) as exc:
            await sub.advance_input(tree["tree_id"], expected_input_version=2)
        assert exc.value.code == "revision_conflict"
        bumped = await sub.advance_input(tree["tree_id"], expected_input_version=1)
        assert bumped["input_version"] == 2 and bumped["revision"] > tree["revision"]
        opened = await sub.require_open_tree(tree["tree_id"], expected_input_version=2)
        assert opened["input_version"] == 2
        closed = await sub.close_tree(tree["tree_id"], reason="user-stop")
        assert closed["status"] == "closed" and closed["close_reason"] == "user-stop"
        again = await sub.close_tree(tree["tree_id"], reason="user-stop")
        assert again["status"] == "closed"
        with pytest.raises(SubagentError) as exc:
            await sub.require_open_tree(tree["tree_id"])
        assert exc.value.code == "subagent_tree_closed"
        assert exc.value.payload()["accepted"] is False
        with pytest.raises(SubagentError) as exc:
            await sub.reserve_agent(
                ReserveAgent(
                    tree_id=tree["tree_id"],
                    parent_id=root_id,
                    path="lead/late",
                    provider="local-sdk",
                    profile="worker",
                    provider_version="prov-1",
                    profile_version="prof-1",
                    request_key="late-key",
                )
            )
        assert exc.value.code == "subagent_tree_closed"
    finally:
        await store.close()


async def test_close_root_input_transactional_race_check(tmp_path):
    store = await SQLiteStore.open(tmp_path / "root-close.sqlite")
    try:
        sub, tree, root_id, _, root_spawn = await build_tree(store)
        reserved, spawn = await spawn_child(
            sub, tree["tree_id"], root_id, "lead/worker-a", "spawn-a"
        )
        await sub.transition_activation(
            reserved["activation"]["activation_id"],
            expected_status="starting",
            status="resident",
        )
        await run_task(sub, reserved, spawn)
        # 执行中后代与在途 auto 消息阻塞收尾。
        blocked = await sub.close_root_input(
            tree["tree_id"], consumer_id=root_id, expected_input_version=1
        )
        assert blocked["closed"] is False
        assert blocked["blockers"]["executing_tasks"] >= 1
        assert blocked["blockers"]["pending_auto_messages"] >= 1
        assert (await sub.tree(tree["tree_id"]))["status"] == "open"
        # 双方消费各自收件箱后，消息不再阻塞。
        child_inbox = await sub.inbox(reserved["agent"]["agent_id"], after_seq=0)
        await sub.commit_input(
            CommitInput(
                agent_id=reserved["agent"]["agent_id"],
                payload={"step": 0},
                message_ids=[m["message_id"] for m in child_inbox],
            )
        )
        settled = await sub.settle_result(
            SettleResult(
                task_id=spawn["task_id"], stop_reason="completed", output={"answer": "done"}
            )
        )
        root_inbox = await sub.inbox(root_id, after_seq=0)
        await sub.commit_input(
            CommitInput(
                agent_id=root_id,
                payload={"step": 0},
                message_ids=[m["message_id"] for m in root_inbox],
            )
        )
        root_settled = await sub.settle_result(
            SettleResult(
                task_id=root_spawn["task_id"],
                stop_reason="completed",
                output={"final": "root-done"},
            )
        )
        blocked = await sub.close_root_input(
            tree["tree_id"], consumer_id=root_id, expected_input_version=1
        )
        assert blocked["blockers"] == {
            "starting_spawns": 0,
            "executing_tasks": 0,
            "pending_auto_messages": 0,
            "unconsumed_notifications": 2,
        }
        # queue_only 消息不延长根任务。
        await sub.accept_message(
            SendMessage(
                tree_id=tree["tree_id"],
                target_id=root_id,
                body={"note": "仅排队"},
                send_key="q-1",
                wake="queue_only",
            )
        )
        blocked = await sub.close_root_input(
            tree["tree_id"], consumer_id=root_id, expected_input_version=1
        )
        assert blocked["blockers"]["pending_auto_messages"] == 0
        # attached（已入父输入 checkpoint、未提交）仍算未完成交付。
        parent_commit = await sub.commit_input(
            CommitInput(
                agent_id=root_id,
                payload={"parent_step": 1},
                notification_ids=[settled["notification"]["notification_id"]],
            )
        )
        blocked = await sub.close_root_input(
            tree["tree_id"], consumer_id=root_id, expected_input_version=1
        )
        assert blocked["closed"] is False
        assert blocked["blockers"]["unconsumed_notifications"] == 2
        step = await sub.commit_step_checkpoint(parent_commit["checkpoint_id"])
        assert step["committed"] == [settled["notification"]["notification_id"]]
        await sub.consume_notifications(
            [root_settled["notification"]["notification_id"]], expected_notify_cursor=0
        )
        closed = await sub.close_root_input(
            tree["tree_id"], consumer_id=root_id, expected_input_version=1
        )
        assert closed["closed"] is True and closed["blockers"] == {}
        tree_row = await sub.tree(tree["tree_id"])
        assert tree_row["status"] == "closed" and tree_row["input_version"] == 2
        already = await sub.close_root_input(tree["tree_id"], expected_input_version=None)
        assert already["closed"] is True and already["already_closed"] is True
        with pytest.raises(ServiceError) as exc:
            await sub.close_root_input(tree["tree_id"], expected_input_version=1)
        assert exc.value.code == "revision_conflict"
    finally:
        await store.close()


async def seed_turn(store, turn_id, session_id="s-1"):
    from krim_agent.storage.models import turns

    async with store.engine.begin() as conn:
        await conn.execute(
            insert(turns).values(
                id=turn_id,
                session_id=session_id,
                workspace_id="ws-1",
                owner_id="owner-1",
                request_key="rk-" + turn_id,
                request_hash="hash",
                text="新用户输入",
                status="queued",
                ordinal=2 if turn_id.endswith("2") else 3,
                accepted_revision=1,
                run_id=None,
                projection_version="v1",
                created_at=2.0,
            )
        )


async def test_commit_step_accepts_root_session_run(tmp_path):
    database = tmp_path / "root-step.sqlite"
    store = await SQLiteStore.open(database)
    try:
        await make_session(store)
        sub = SubagentStore(store)
        root_run, _ = await store.begin(
            RunRequest(text="根任务", request_key="root:run:1"), {"v": 1}
        )
        tree = await sub.ensure_tree(
            TreeScope(
                session_id="s-1",
                run_id=root_run.id,
                workspace_id="ws-1",
                owner_id="owner-1",
                request_key="tree-root",
            )
        )
        reserved = await sub.reserve_agent(
            ReserveAgent(
                tree_id=tree["tree_id"],
                path="lead",
                role="root",
                provider="local-sdk",
                profile="lead",
                provider_version="prov-1",
                profile_version="prof-1",
                request_key="root-key",
            )
        )
        root_id = reserved["agent"]["agent_id"]
        committed = await sub.commit_input(
            CommitInput(agent_id=root_id, payload={"step": 1}, run_id=root_run.id)
        )
        assert committed["park_checkpoint"] is True
        step = await sub.commit_step(root_id, root_run.id, {"step": 1, "response": "ok"})
        assert step["run_id"] == root_run.id and step["consumed"] == []
        park = raw(database, ["park_checkpoints"])["park_checkpoints"]
        assert park[0][0] == root_run.id
        assert json.loads(park[0][1])["response"] == "ok"
        # 任意非本树 root_run 的 run 不可用于 step 提交。
        other_run, _ = await store.begin(
            RunRequest(text="别的运行", request_key="root:run:2"), {}
        )
        with pytest.raises(ServiceError) as exc:
            await sub.commit_step(root_id, other_run.id, {"step": 2})
        assert exc.value.code == "not_found"
    finally:
        await store.close()


async def test_bind_root_explicit_resume(tmp_path):
    store = await SQLiteStore.open(tmp_path / "bind.sqlite")
    try:
        sub, tree, root_id, _, root_spawn = await build_tree(store)
        reserved, spawn = await spawn_child(
            sub, tree["tree_id"], root_id, "lead/worker-a", "spawn-a"
        )
        await sub.transition_activation(
            reserved["activation"]["activation_id"],
            expected_status="starting",
            status="resident",
        )
        await run_task(sub, reserved, spawn)
        settled = await sub.settle_result(
            SettleResult(
                task_id=spawn["task_id"], stop_reason="completed", output={"answer": "done"}
            )
        )
        await seed_turn(store, "turn-2")
        resumed_run, _ = await store.begin(
            RunRequest(text="继续这棵树", request_key="bind:run-1"), {"resume": True}
        )
        scope = TreeScope(
            session_id="s-1",
            turn_id="turn-2",
            run_id=resumed_run.id,
            workspace_id="ws-1",
            owner_id="owner-1",
            request_key="tree-1",
        )
        # 旧 root 未终态：不能抢占。
        with pytest.raises(ServiceError) as exc:
            await sub.bind_root(scope, tree["tree_id"], expected_input_version=1)
        assert exc.value.code == "revision_conflict"
        await sub.settle_result(
            SettleResult(
                task_id=root_spawn["task_id"],
                stop_reason="completed",
                output={"final": "root-done"},
            )
        )
        # 归属不符拒绝。
        shifted = scope.model_copy(update={"workspace_id": "ws-9"})
        with pytest.raises(ServiceError) as exc:
            await sub.bind_root(shifted, tree["tree_id"], expected_input_version=1)
        assert exc.value.code == "invalid_input"
        wrong_session = scope.model_copy(update={"session_id": "s-9"})
        with pytest.raises(ServiceError) as exc:
            await sub.bind_root(wrong_session, tree["tree_id"], expected_input_version=1)
        assert exc.value.code == "invalid_input"
        stale = scope.model_copy(update={"turn_id": "turn-2"})
        with pytest.raises(ServiceError) as exc:
            await sub.bind_root(stale, tree["tree_id"], expected_input_version=2)
        assert exc.value.code == "revision_conflict"
        bound = await sub.bind_root(scope, tree["tree_id"], expected_input_version=1)
        assert bound["rebound"] is True
        assert bound["status"] == "open"
        assert bound["root_turn_id"] == "turn-2" and bound["root_run_id"] == resumed_run.id
        assert bound["input_version"] == 2
        # 子结果通知（consumer=root）与 root 自身结果通知（consumer=NULL）都在。
        assert bound["pending_notifications"] == 2
        # 成员/历史/结果保留；旧 pending 通知保留但不是新授权。
        agents = await sub.list_agents(tree["tree_id"])
        assert [a["path"] for a in agents] == ["lead", "lead/worker-a"]
        pending = await sub.notifications(tree["tree_id"], consumer_id=root_id, after_seq=0)
        assert [n["status"] for n in pending] == ["pending"]
        assert pending[0]["result"]["result_id"] == settled["result"]["result_id"]
        # 旧成员仍可被显式唤醒（树已重新打开）。
        wake = await sub.accept_message(
            SendMessage(
                tree_id=tree["tree_id"],
                target_id=reserved["agent"]["agent_id"],
                body={"text": "继续"},
                send_key="resume-1",
            )
        )
        assert wake["created"] is True
        # 再绑一次走 CAS；上一轮绑定的 root run 须先终态（不能抢占活跃根执行）。
        await store.finish(resumed_run.id, "completed")
        await seed_turn(store, "turn-3")
        rebound = await sub.bind_root(
            scope.model_copy(update={"turn_id": "turn-3"}),
            tree["tree_id"],
            expected_input_version=2,
        )
        assert rebound["input_version"] == 3 and rebound["root_turn_id"] == "turn-3"
    finally:
        await store.close()


async def test_spawn_reservation_idempotent_with_rollback(tmp_path):
    store = await SQLiteStore.open(tmp_path / "spawn.sqlite")
    try:
        sub, tree, root_id, _, _ = await build_tree(store)
        request = ReserveAgent(
            tree_id=tree["tree_id"],
            parent_id=root_id,
            path="lead/worker-a",
            provider="local-sdk",
            profile="worker",
            provider_version="prov-1",
            profile_version="prof-1",
            request_key="spawn-1",
        )
        reserved = await sub.reserve_agent(request)
        assert reserved["created"] is True
        assert reserved["agent"]["depth"] == 1
        assert reserved["activation"]["status"] == "starting"
        assert reserved["activation"]["instance"] == 1
        replay = await sub.reserve_agent(request)
        assert replay["created"] is False
        assert replay["agent"]["agent_id"] == reserved["agent"]["agent_id"]
        shifted = request.model_copy(update={"profile": "analyst"})
        with pytest.raises(ServiceError) as exc:
            await sub.reserve_agent(shifted)
        assert exc.value.code == "key_conflict"
        renamed = request.model_copy(update={"request_key": "spawn-2"})
        with pytest.raises(SubagentError) as exc:
            await sub.reserve_agent(renamed)
        assert exc.value.code == "subagent_name_conflict"
        deep = ReserveAgent(
            tree_id=tree["tree_id"],
            parent_id=reserved["agent"]["agent_id"],
            path="lead/worker-a/deeper",
            provider="local-sdk",
            profile="worker",
            provider_version="prov-1",
            profile_version="prof-1",
            request_key="spawn-3",
            max_depth=1,
        )
        with pytest.raises(SubagentError) as exc:
            await sub.reserve_agent(deep)
        assert exc.value.code == "subagent_depth_exceeded"
        # 回执前失败可整体回滚预留。
        assert await sub.rollback_reservation(
            reserved["agent"]["agent_id"], reserved["activation"]["activation_id"]
        ) is True
        with pytest.raises(ServiceError):
            await sub.get_agent(reserved["agent"]["agent_id"])
        assert await sub.resident_count(tree["tree_id"]) == 0
        # 已接受任务的成员不能回滚。
        reserved, spawn = await spawn_child(
            sub, tree["tree_id"], root_id, "lead/worker-b", "spawn-b"
        )
        assert await sub.rollback_reservation(
            reserved["agent"]["agent_id"], reserved["activation"]["activation_id"]
        ) is False
    finally:
        await store.close()


async def test_accept_spawn_receipt_is_idempotent(tmp_path):
    store = await SQLiteStore.open(tmp_path / "receipt.sqlite")
    try:
        sub, tree, root_id, _, _ = await build_tree(store)
        reserved = await sub.reserve_agent(
            ReserveAgent(
                tree_id=tree["tree_id"],
                parent_id=root_id,
                path="lead/worker-a",
                provider="local-sdk",
                profile="worker",
                provider_version="prov-1",
                profile_version="prof-1",
                request_key="spawn-1",
            )
        )
        spawn = await sub.accept_spawn(
            AcceptSpawn(
                agent_id=reserved["agent"]["agent_id"],
                activation_id=reserved["activation"]["activation_id"],
                input={"text": "首任务"},
            )
        )
        assert spawn["created"] is True and spawn["seq"] == 1
        assert spawn["task"]["status"] == "accepted"
        assert spawn["target"]["task_status"] == "accepted"
        replay = await sub.accept_spawn(
            AcceptSpawn(
                agent_id=reserved["agent"]["agent_id"],
                activation_id=reserved["activation"]["activation_id"],
                input={"text": "首任务"},
            )
        )
        assert replay["created"] is False
        assert replay["task_id"] == spawn["task_id"]
        assert replay["message_id"] == spawn["message_id"]
        with pytest.raises(ServiceError) as exc:
            await sub.accept_spawn(
                AcceptSpawn(
                    agent_id=reserved["agent"]["agent_id"],
                    activation_id=reserved["activation"]["activation_id"],
                    input={"text": "不同的任务"},
                )
            )
        assert exc.value.code == "key_conflict"
        agent = await sub.get_agent(reserved["agent"]["agent_id"])
        assert agent["current_task_id"] == spawn["task_id"]
        assert agent["task"]["input_hash"]
    finally:
        await store.close()


async def test_message_seq_dense_replay_and_current_task_binding(tmp_path):
    store = await SQLiteStore.open(tmp_path / "messages.sqlite")
    try:
        sub, tree, root_id, _, root_spawn = await build_tree(store)
        reserved, spawn = await spawn_child(
            sub, tree["tree_id"], root_id, "lead/worker-a", "spawn-a"
        )
        child_id = reserved["agent"]["agent_id"]
        first = await sub.accept_message(
            SendMessage(
                tree_id=tree["tree_id"],
                target_id=child_id,
                sender_id=root_id,
                body={"note": "补充"},
                send_key="m-1",
            )
        )
        assert first["created"] is True and first["seq"] == 2
        assert first["task_id"] == spawn["task_id"]
        replay = await sub.accept_message(
            SendMessage(
                tree_id=tree["tree_id"],
                target_id=child_id,
                sender_id=root_id,
                body={"note": "补充"},
                send_key="m-1",
            )
        )
        assert replay["created"] is False and replay["message_id"] == first["message_id"]
        with pytest.raises(ServiceError) as exc:
            await sub.accept_message(
                SendMessage(
                    tree_id=tree["tree_id"],
                    target_id=child_id,
                    body={"note": "不同内容"},
                    send_key="m-1",
                )
            )
        assert exc.value.code == "key_conflict"
        batch = await asyncio.gather(
            *[
                sub.accept_message(
                    SendMessage(
                        tree_id=tree["tree_id"],
                        target_id=child_id,
                        body={"n": index},
                        send_key=f"c-{index}",
                    )
                )
                for index in range(5)
            ]
        )
        assert sorted(item["seq"] for item in batch) == [3, 4, 5, 6, 7]
        inbox = await sub.inbox(child_id, after_seq=2)
        assert [m["seq"] for m in inbox] == [3, 4, 5, 6, 7]
        agent = await sub.get_agent(child_id)
        assert agent["inbox_cursor"] == 0
    finally:
        await store.close()


async def test_task_input_gates_and_followup(tmp_path):
    store = await SQLiteStore.open(tmp_path / "tasks.sqlite")
    try:
        sub, tree, root_id, _, _ = await build_tree(store)
        reserved, spawn = await spawn_child(
            sub, tree["tree_id"], root_id, "lead/worker-a", "spawn-a"
        )
        child_id = reserved["agent"]["agent_id"]
        task_id = spawn["task_id"]
        # 显式绑定旧任务的消息在活跃期被接受，任务结算后被拒绝。
        busy = await sub.accept_message(
            SendMessage(
                tree_id=tree["tree_id"],
                target_id=child_id,
                body={"n": 1},
                send_key="bind-1",
                task_id=task_id,
            )
        )
        assert busy["task_id"] == task_id
        settled = await sub.settle_result(
            SettleResult(task_id=task_id, stop_reason="completed", output={"answer": "ok"})
        )
        assert settled["created"] is True
        with pytest.raises(SubagentError) as exc:
            await sub.accept_message(
                SendMessage(
                    tree_id=tree["tree_id"],
                    target_id=child_id,
                    body={"n": 2},
                    send_key="bind-2",
                    task_id=task_id,
                )
            )
        assert exc.value.code == "subagent_task_closed"
        assert exc.value.payload()["accepted"] is False
        # 活跃任务未结束时不能开显式后续任务。
        fresh = await spawn_child(sub, tree["tree_id"], root_id, "lead/worker-b", "spawn-b")
        with pytest.raises(SubagentError) as exc:
            await sub.accept_message(
                SendMessage(
                    tree_id=tree["tree_id"],
                    target_id=fresh[1]["task"]["agent_id"],
                    kind="task",
                    body={"text": "新任务"},
                    send_key="new-task-1",
                )
            )
        assert exc.value.code == "subagent_task_closed"
        # 结算后 kind=task 开显式后续任务。
        follow = await sub.accept_message(
            SendMessage(
                tree_id=tree["tree_id"],
                target_id=child_id,
                kind="task",
                body={"text": "继续调查"},
                send_key="follow-1",
            )
        )
        assert follow["created"] is True and follow["task_id"] != task_id
        assert follow["task_id"] == (await sub.get_agent(child_id))["current_task_id"]
        follow_task = await sub.task(follow["task_id"])
        assert follow_task["ordinal"] == 2
        assert follow_task["status"] == "accepted"
        assert follow_task["input"] == {"text": "继续调查"}
        replay = await sub.accept_message(
            SendMessage(
                tree_id=tree["tree_id"],
                target_id=child_id,
                kind="task",
                body={"text": "继续调查"},
                send_key="follow-1",
            )
        )
        assert replay["created"] is False and replay["task_id"] == follow["task_id"]
        # 同键不同内容拒绝。
        with pytest.raises(ServiceError) as exc:
            await sub.accept_message(
                SendMessage(
                    tree_id=tree["tree_id"],
                    target_id=child_id,
                    kind="task",
                    body={"text": "另一个"},
                    send_key="follow-1",
                )
            )
        assert exc.value.code == "key_conflict"
    finally:
        await store.close()


async def test_close_task_input_blocks_late_binding(tmp_path):
    store = await SQLiteStore.open(tmp_path / "close-input.sqlite")
    try:
        sub, tree, root_id, _, _ = await build_tree(store)
        reserved, spawn = await spawn_child(
            sub, tree["tree_id"], root_id, "lead/worker-a", "spawn-a"
        )
        child_id = reserved["agent"]["agent_id"]
        task_id = spawn["task_id"]
        late = await sub.accept_message(
            SendMessage(
                tree_id=tree["tree_id"],
                target_id=child_id,
                body={"n": 1},
                send_key="late-1",
                task_id=task_id,
            )
        )
        assert late["created"] is True
        closed = await sub.close_task_input(task_id)
        # 初始任务消息与补充输入都未消费，暂不能关闭。
        assert closed["closed"] is False and closed["unconsumed_messages"] == 2
        # 消费全部未决消息后才能关闭。
        inbox = await sub.inbox(child_id, after_seq=0)
        commit = await sub.commit_input(
            CommitInput(
                agent_id=child_id,
                payload={"step": 1},
                message_ids=[m["message_id"] for m in inbox],
            )
        )
        assert commit["message_count"] == 2
        task_revision = (await sub.task(task_id))["revision"]
        closed = await sub.close_task_input(task_id, expected_revision=task_revision)
        assert closed["closed"] is True
        again = await sub.close_task_input(task_id)
        assert again["closed"] is True
        with pytest.raises(SubagentError) as exc:
            await sub.accept_message(
                SendMessage(
                    tree_id=tree["tree_id"],
                    target_id=child_id,
                    body={"n": 2},
                    send_key="late-2",
                    task_id=task_id,
                )
            )
        assert exc.value.code == "subagent_task_closed"
        with pytest.raises(SubagentError) as exc:
            await sub.accept_message(
                SendMessage(
                    tree_id=tree["tree_id"],
                    target_id=child_id,
                    body={"n": 3},
                    send_key="late-3",
                )
            )
        assert exc.value.code == "subagent_task_closed"
    finally:
        await store.close()


async def test_start_task_run_mapping_descriptor_and_replay(tmp_path):
    database = tmp_path / "start.sqlite"
    store = await SQLiteStore.open(database)
    try:
        sub, tree, root_id, _, _ = await build_tree(store)
        reserved, spawn = await spawn_child(
            sub, tree["tree_id"], root_id, "lead/worker-a", "spawn-a"
        )
        child_id = reserved["agent"]["agent_id"]
        task_id = spawn["task_id"]
        request = RunRequest(
            text="执行子任务",
            request_key=f"ac-child.{task_id}.1",
            target="target-http",
        )
        started = await sub.start_task(
            StartTask(
                agent_id=child_id,
                task_id=task_id,
                activation_id=reserved["activation"]["activation_id"],
                request=request,
                target={"k": 1},
            )
        )
        assert started["created"] is True
        assert started["run"]["status"] == "queued"
        assert started["run"]["trace_id"]
        found = await store.by_key(request.request_key)
        assert found is not None and found.id == started["run_id"]
        snapshot = await store.snapshot(started["run_id"])
        context = snapshot.target["context"]
        assert context["schema_version"] == 5
        assert context["task_id"] == task_id
        assert context["tree_id"] == tree["tree_id"]
        assert context["agent_id"] == child_id
        assert context["owner_session_id"] == "s-1"
        assert context["owner_id"] == "owner-1"
        assert context["workspace_id"] == "ws-1"
        assert context["depth"] == 1
        assert context["profile"] == "worker"
        assert context["path"] == "lead/worker-a"
        rows = raw(database, ["subagent_child_runs"])["subagent_child_runs"]
        assert len(rows) == 1 and rows[0][1] == task_id and rows[0][5] == "s-1"
        task = await sub.task(task_id)
        assert task["status"] == "running" and task["run_id"] == started["run_id"]
        replay = await sub.start_task(
            StartTask(
                agent_id=child_id,
                task_id=task_id,
                activation_id=reserved["activation"]["activation_id"],
                request=request,
                target={"k": 1},
            )
        )
        assert replay["created"] is False and replay["run_id"] == started["run_id"]
        with pytest.raises(ServiceError) as exc:
            await sub.start_task(
                StartTask(
                    agent_id=child_id,
                    task_id=task_id,
                    activation_id=reserved["activation"]["activation_id"],
                    request=request.model_copy(update={"text": "篡改"}),
                    target={"k": 1},
                )
            )
        assert exc.value.code == "key_conflict"
        with pytest.raises(SubagentError) as exc:
            await sub.start_task(
                StartTask(
                    agent_id=child_id,
                    task_id=task_id,
                    activation_id=reserved["activation"]["activation_id"],
                    request=request.model_copy(update={"request_key": f"ac-child.{task_id}.2"}),
                    target={"k": 1},
                )
            )
        assert exc.value.code == "subagent_task_closed"
    finally:
        await store.close()


async def test_settle_result_notification_and_replay(tmp_path):
    store = await SQLiteStore.open(tmp_path / "settle.sqlite")
    try:
        sub, tree, root_id, _, _ = await build_tree(store)
        reserved, spawn = await spawn_child(
            sub, tree["tree_id"], root_id, "lead/worker-a", "spawn-a"
        )
        child_id = reserved["agent"]["agent_id"]
        task_id = spawn["task_id"]
        settled = await sub.settle_result(
            SettleResult(task_id=task_id, stop_reason="completed", output={"answer": "done"})
        )
        assert settled["created"] is True
        assert settled["result"]["stop_reason"] == "completed"
        notification = settled["notification"]
        assert notification["status"] == "pending"
        assert notification["consumer_id"] == root_id
        assert notification["producer_id"] == child_id
        assert notification["result"]["output"] == {"answer": "done"}
        replay = await sub.settle_result(
            SettleResult(task_id=task_id, stop_reason="completed", output={"answer": "done"})
        )
        assert replay["created"] is False
        assert replay["result"]["result_id"] == settled["result"]["result_id"]
        with pytest.raises(ServiceError) as exc:
            await sub.settle_result(
                SettleResult(task_id=task_id, stop_reason="error", output={"answer": "冲突"})
            )
        assert exc.value.code == "revision_conflict"
        task = await sub.task(task_id)
        assert task["status"] == "completed" and task["finished_at"] is not None
        with pytest.raises(SubagentError) as exc:
            await sub.set_task_status(task_id, status="running")
        assert exc.value.code == "subagent_task_closed"
        # 通知增量读取：父消费视角。
        pending = await sub.notifications(
            tree["tree_id"], consumer_id=root_id, after_seq=0
        )
        assert [n["notification_id"] for n in pending] == [notification["notification_id"]]
        assert pending[0]["result"]["result_id"] == settled["result"]["result_id"]
    finally:
        await store.close()


async def test_commit_input_cursors_checkpoints_and_park(tmp_path):
    database = tmp_path / "commit.sqlite"
    store = await SQLiteStore.open(database)
    try:
        sub, tree, root_id, root_activation_id, root_spawn = await build_tree(store)
        reserved, spawn = await spawn_child(
            sub, tree["tree_id"], root_id, "lead/worker-a", "spawn-a"
        )
        child_id = reserved["agent"]["agent_id"]
        child_run = await run_task(sub, reserved, spawn)
        settled = await sub.settle_result(
            SettleResult(
                task_id=spawn["task_id"], stop_reason="completed", output={"answer": "done"}
            )
        )
        notification_id = settled["notification"]["notification_id"]
        inbox = await sub.inbox(child_id, after_seq=0)
        message_id = inbox[0]["message_id"]
        committed = await sub.commit_input(
            CommitInput(
                agent_id=child_id,
                payload={"step": 1, "messages": ["m"]},
                message_ids=[message_id],
                run_id=child_run,
                expected_inbox_cursor=0,
                expected_notify_cursor=0,
            )
        )
        assert committed["created"] is True
        assert committed["message_count"] == 1 and committed["notification_count"] == 0
        assert committed["inbox_cursor"] == 1
        assert committed["park_checkpoint"] is True
        rows = raw(database, ["subagent_notifications"])["subagent_notifications"]
        assert rows[0][7] == "pending"
        checkpoint = await sub.latest_checkpoint(child_id)
        assert checkpoint["payload"]["step"] == 1
        park = raw(database, ["park_checkpoints"])["park_checkpoints"]
        assert len(park) == 1 and park[0][0] == child_run
        assert json.loads(park[0][1]) == {"step": 1, "messages": ["m"]}
        # 重复附入拒绝；游标 CAS 失效拒绝。
        with pytest.raises(ServiceError) as exc:
            await sub.commit_input(
                CommitInput(
                    agent_id=child_id,
                    payload={"step": 2},
                    message_ids=[message_id],
                )
            )
        assert exc.value.code == "revision_conflict"
        with pytest.raises(ServiceError) as exc:
            await sub.commit_input(
                CommitInput(
                    agent_id=child_id,
                    payload={"step": 2},
                    expected_inbox_cursor=0,
                )
            )
        assert exc.value.code == "revision_conflict"
        # 子的 step 提交更新 park checkpoint，无 attached 通知也落库。
        step = await sub.commit_step(
            child_id, child_run, {"step": 1, "response": "ok", "toolcalls": []}
        )
        assert step["consumed"] == []
        park = raw(database, ["park_checkpoints"])["park_checkpoints"]
        assert len(park) == 1
        assert json.loads(park[0][1])["response"] == "ok"
        # 父（根成员）消费子完成通知：commit_input 附入 + commit_step 消费。
        root_start = await sub.start_task(
            StartTask(
                agent_id=root_id,
                task_id=root_spawn["task_id"],
                activation_id=root_activation_id,
                request=start_request(root_spawn["task_id"]),
                target={"k": 1},
            )
        )
        parent_commit = await sub.commit_input(
            CommitInput(
                agent_id=root_id,
                payload={"parent_step": 1},
                notification_ids=[notification_id],
                run_id=root_start["run_id"],
            )
        )
        assert parent_commit["notification_count"] == 1
        rows = raw(database, ["subagent_notifications"])["subagent_notifications"]
        assert rows[0][7] == "attached"
        parent_step = await sub.commit_step(
            root_id, root_start["run_id"], {"parent_step": 1, "response": "ack"}
        )
        assert parent_step["consumed"] == [notification_id]
        rows = raw(database, ["subagent_notifications"])["subagent_notifications"]
        assert rows[0][7] == "consumed"
        park = raw(database, ["park_checkpoints"])["park_checkpoints"]
        assert len(park) == 2
        # 带新结果的通知可被抑制，结果保留。
        fresh = await spawn_child(sub, tree["tree_id"], root_id, "lead/worker-b", "spawn-b")
        settled_b = await sub.settle_result(
            SettleResult(
                task_id=fresh[1]["task_id"], stop_reason="aborted", output={"why": "stop"}
            )
        )
        suppressed = await sub.suppress_notifications(
            [settled_b["notification"]["notification_id"]], reason="tree-closed"
        )
        assert suppressed["suppressed"] == [settled_b["notification"]["notification_id"]]
        assert (await sub.result(fresh[1]["task_id"]))["stop_reason"] == "aborted"
    finally:
        await store.close()


async def test_commit_input_writes_real_park_checkpoint_with_run(tmp_path):
    database = tmp_path / "park.sqlite"
    store = await SQLiteStore.open(database)
    try:
        sub, tree, root_id, _, _ = await build_tree(store)
        reserved, spawn = await spawn_child(
            sub, tree["tree_id"], root_id, "lead/worker-a", "spawn-a"
        )
        child_id = reserved["agent"]["agent_id"]
        run = await run_task(sub, reserved, spawn)
        committed = await sub.commit_input(
            CommitInput(
                agent_id=child_id,
                payload={"history": ["h1"], "budget": 5},
                run_id=run,
            )
        )
        assert committed["park_checkpoint"] is True
        park = raw(database, ["park_checkpoints"])["park_checkpoints"]
        assert park[0][0] == run
        assert json.loads(park[0][1]) == {"history": ["h1"], "budget": 5}
        with pytest.raises(ServiceError) as exc:
            await sub.commit_input(
                CommitInput(agent_id=child_id, payload={}, run_id="missing-run")
            )
        assert exc.value.code == "not_found"
        assert store.healthy is True
    finally:
        await store.close()


async def test_member_close_and_subtree_gates(tmp_path):
    store = await SQLiteStore.open(tmp_path / "close.sqlite")
    try:
        sub, tree, root_id, _, _ = await build_tree(store)
        reserved, spawn = await spawn_child(
            sub, tree["tree_id"], root_id, "lead/worker-a", "spawn-a"
        )
        child_id = reserved["agent"]["agent_id"]
        revision = (await sub.get_agent(child_id))["revision"]
        with pytest.raises(ServiceError) as exc:
            await sub.close_member(child_id, expected_revision=revision + 3)
        assert exc.value.code == "revision_conflict"
        closed = await sub.close_member(child_id, expected_revision=revision)
        assert closed["status"] == "closed" and closed["closed_at"] is not None
        again = await sub.close_member(child_id, expected_revision=revision)
        assert again["status"] == "closed"
        with pytest.raises(SubagentError) as exc:
            await sub.accept_message(
                SendMessage(
                    tree_id=tree["tree_id"], target_id=child_id, body={}, send_key="x-1"
                )
            )
        assert exc.value.code == "subagent_activation_closed"
        with pytest.raises(SubagentError) as exc:
            await sub.reserve_agent(
                ReserveAgent(
                    tree_id=tree["tree_id"],
                    parent_id=child_id,
                    path="lead/worker-a/deep",
                    provider="local-sdk",
                    profile="worker",
                    provider_version="prov-1",
                    profile_version="prof-1",
                    request_key="deep-key",
                )
            )
        assert exc.value.code == "subagent_activation_closed"
        with pytest.raises(SubagentError) as exc:
            await sub.begin_activation(child_id, provider_version="prov-2")
        assert exc.value.code == "subagent_not_recoverable"
        descendants = await sub.list_descendants(root_id)
        assert [d["path"] for d in descendants] == ["lead/worker-a"]
    finally:
        await store.close()


async def test_activation_state_machine_and_instance_versions(tmp_path):
    store = await SQLiteStore.open(tmp_path / "activation.sqlite")
    try:
        sub, tree, root_id, _, _ = await build_tree(store)
        reserved, _ = await spawn_child(sub, tree["tree_id"], root_id, "lead/worker-a", "spawn-a")
        agent_id = reserved["agent"]["agent_id"]
        activation_id = reserved["activation"]["activation_id"]
        assert await sub.resident_count(tree["tree_id"]) == 1
        resident = await sub.transition_activation(
            activation_id, expected_status="starting", status="resident"
        )
        assert resident["status"] == "resident" and resident["resident_at"] is not None
        with pytest.raises(ServiceError) as exc:
            await sub.transition_activation(
                activation_id, expected_status="starting", status="resident"
            )
        assert exc.value.code == "revision_conflict"
        closing = await sub.transition_activation(
            activation_id, expected_status="resident", status="closing"
        )
        assert closing["status"] == "closing"
        assert await sub.resident_count(tree["tree_id"]) == 1
        with pytest.raises(SubagentError) as exc:
            await sub.begin_activation(agent_id, provider_version="prov-2")
        assert exc.value.code == "subagent_not_recoverable"
        failed = await sub.transition_activation(
            activation_id, expected_status="closing", status="closing",
            cleanup_code="cleanup_failed",
        )
        assert failed["cleanup_code"] == "cleanup_failed"
        released = await sub.transition_activation(
            activation_id, expected_status="closing", status="released"
        )
        assert released["status"] == "released" and released["released_at"] is not None
        assert await sub.resident_count(tree["tree_id"]) == 0
        # 旧句柄不能操作已释放实例。
        with pytest.raises(SubagentError) as exc:
            await sub.transition_activation(
                activation_id, expected_status="closing", status="released"
            )
        assert exc.value.code == "subagent_activation_closed"
        # 冷恢复带实例版本检查；并发恢复只有一个胜出。
        recovered = await sub.begin_activation(
            agent_id, provider_version="prov-2", expected_instance=1
        )
        assert recovered["activation"]["instance"] == 2
        with pytest.raises(ServiceError) as exc:
            await sub.begin_activation(
                agent_id, provider_version="prov-2", expected_instance=1
            )
        assert exc.value.code == "revision_conflict"
        with pytest.raises(ServiceError) as exc:
            await sub.begin_activation(
                agent_id, provider_version="prov-2", expected_instance=2
            )
        assert exc.value.code == "revision_conflict"
        newest = await sub.get_agent(agent_id)
        assert newest["activation"]["activation_id"] != activation_id
        assert newest["provider_version"] == "prov-2"
    finally:
        await store.close()


async def test_capacity_counts_live_only_and_excludes_root(tmp_path):
    store = await SQLiteStore.open(tmp_path / "capacity.sqlite")
    try:
        await make_session(store)
        sub = SubagentStore(store)
        tree = await sub.ensure_tree(
            TreeScope(session_id="s-1", workspace_id="ws-1", owner_id="owner-1", request_key="t1")
        )
        root = await sub.reserve_agent(
            ReserveAgent(
                tree_id=tree["tree_id"],
                path="lead",
                role="root",
                provider="local-sdk",
                profile="lead",
                provider_version="prov-1",
                profile_version="prof-1",
                request_key="root-key",
            )
        )
        root_id = root["agent"]["agent_id"]
        first = await sub.reserve_agent(
            ReserveAgent(
                tree_id=tree["tree_id"],
                parent_id=root_id,
                path="lead/a",
                provider="local-sdk",
                profile="worker",
                provider_version="prov-1",
                profile_version="prof-1",
                request_key="a",
                resident_limit=1,
            )
        )
        assert await sub.resident_count(tree["tree_id"]) == 1
        with pytest.raises(SubagentError) as exc:
            await sub.reserve_agent(
                ReserveAgent(
                    tree_id=tree["tree_id"],
                    parent_id=root_id,
                    path="lead/b",
                    provider="local-sdk",
                    profile="worker",
                    provider_version="prov-1",
                    profile_version="prof-1",
                    request_key="b",
                    resident_limit=1,
                )
            )
        assert exc.value.code == "capacity"
        assert exc.value.status == 429
        # 释放后容量归还。
        await sub.transition_activation(
            first["activation"]["activation_id"],
            expected_status="starting",
            status="closing",
        )
        assert await sub.resident_count(tree["tree_id"]) == 1
        await sub.transition_activation(
            first["activation"]["activation_id"],
            expected_status="closing",
            status="released",
        )
        assert await sub.resident_count(tree["tree_id"]) == 0
        rebound = await sub.reserve_agent(
            ReserveAgent(
                tree_id=tree["tree_id"],
                parent_id=root_id,
                path="lead/b",
                provider="local-sdk",
                profile="worker",
                provider_version="prov-1",
                profile_version="prof-1",
                request_key="b",
                resident_limit=1,
            )
        )
        assert rebound["created"] is True
    finally:
        await store.close()


async def test_host_consumes_root_results_and_advances_tree_cursor(tmp_path):
    store = await SQLiteStore.open(tmp_path / "host.sqlite")
    try:
        sub, tree, root_id, _, root_spawn = await build_tree(store)
        settled = await sub.settle_result(
            SettleResult(
                task_id=root_spawn["task_id"],
                stop_reason="completed",
                output={"final": "root-done"},
            )
        )
        assert settled["notification"]["consumer_id"] is None
        assert (await sub.tree(tree["tree_id"]))["notify_cursor"] == 0
        pending = await sub.notifications(tree["tree_id"], consumer_id=None, after_seq=0)
        assert [n["notification_id"] for n in pending] == [
            settled["notification"]["notification_id"]
        ]
        consumed = await sub.consume_notifications(
            [settled["notification"]["notification_id"]], expected_notify_cursor=0
        )
        assert consumed["consumed"] == [settled["notification"]["notification_id"]]
        assert (await sub.tree(tree["tree_id"]))["notify_cursor"] == (
            settled["notification"]["seq"]
        )
        with pytest.raises(ServiceError) as exc:
            await sub.consume_notifications(
                [settled["notification"]["notification_id"]], expected_notify_cursor=0
            )
        assert exc.value.code == "revision_conflict"
        # 成员通知不能走宿主直消费通道。
        reserved, spawn = await spawn_child(sub, tree["tree_id"], root_id, "lead/w", "spawn-w")
        child_settled = await sub.settle_result(
            SettleResult(task_id=spawn["task_id"], stop_reason="completed", output={"a": 1})
        )
        with pytest.raises(ServiceError) as exc:
            await sub.consume_notifications(
                [child_settled["notification"]["notification_id"]]
            )
        assert exc.value.code == "invalid_input"
    finally:
        await store.close()


async def test_recover_interrupts_unknown_and_keeps_cleanup_debt(tmp_path):
    database = tmp_path / "recover.sqlite"
    store = await SQLiteStore.open(database)
    try:
        sub, tree, root_id, _, _ = await build_tree(store)
        runner, run_spawn = await spawn_child(
            sub, tree["tree_id"], root_id, "lead/worker-a", "spawn-a"
        )
        await run_task(sub, runner, run_spawn)
        # 另一个成员结算完成：通知待父消费，结果已持久。
        notifier, note_spawn = await spawn_child(
            sub, tree["tree_id"], root_id, "lead/worker-b", "spawn-b"
        )
        await sub.settle_result(
            SettleResult(
                task_id=note_spawn["task_id"], stop_reason="completed", output={"answer": "x"}
            )
        )
        # 第三个成员清理失败停在 closing，继续占容量。
        closer, closer_spawn = await spawn_child(
            sub, tree["tree_id"], root_id, "lead/worker-c", "spawn-c"
        )
        await sub.transition_activation(
            closer["activation"]["activation_id"],
            expected_status="starting",
            status="closing",
            cleanup_code="cleanup_failed",
        )
        pending = await sub.notifications(
            tree["tree_id"], consumer_id=root_id, after_seq=0
        )
        assert len(pending) == 1
    finally:
        await store.close()
    store = await SQLiteStore.open(database)
    try:
        sub = SubagentStore(store)
        report = await sub.recover()
        # runner 执行不可确认、closer 任务从未开跑、root 自身任务也无结果：
        # 都结算 interrupted 并建立待通知，父/宿主恢复后能看到死亡而非永远等待。
        assert report["tasks_interrupted"] == 3
        assert report["notifications_created"] == 3
        assert report["approvals_preserved"] == 0
        # starting/resident 驻留不视为存活；closing 清理债保留。
        assert report["activations_released"] >= 3
        assert report["activations_pending_cleanup"] == 1
        task = await sub.task(run_spawn["task_id"])
        assert task["status"] == "interrupted"
        result = await sub.result(run_spawn["task_id"])
        assert result is not None and result["stop_reason"] == "interrupted"
        agents = await sub.list_agents(tree["tree_id"])
        statuses = {a["path"]: a["activation"]["status"] for a in agents}
        assert statuses["lead/worker-c"] == "closing"
        assert statuses["lead/worker-a"] == "released"
        assert statuses["lead/worker-b"] == "released"
        # 已持久结果的通知 + 两份新的 interrupted 通知都在等待显式消费。
        still_pending = await sub.notifications(
            tree["tree_id"], consumer_id=root_id, after_seq=0
        )
        assert len(still_pending) == 3
        assert {n["task_id"] for n in still_pending} == {
            note_spawn["task_id"],
            run_spawn["task_id"],
            closer_spawn["task_id"],
        }
        recover_twice = await sub.recover()
        assert recover_twice["tasks_interrupted"] == 0
        assert recover_twice["notifications_created"] == 0
        assert recover_twice["activations_pending_cleanup"] == 1
    finally:
        await store.close()


async def test_recover_preserves_genuine_awaiting_approval(tmp_path):
    store = await SQLiteStore.open(tmp_path / "approval.sqlite")
    try:
        sub, tree, root_id, _, _ = await build_tree(store)
        reserved, spawn = await spawn_child(
            sub, tree["tree_id"], root_id, "lead/worker-a", "spawn-a"
        )
        child_run = await run_task(sub, reserved, spawn)
        await store.set_running(child_run)
        await sub.set_task_status(spawn["task_id"], status="awaiting_approval")
        await store.set_awaiting(child_run)
        await store.write_checkpoint(child_run, {"parked": True}, "fp-1")
    finally:
        await store.close()
    store = await SQLiteStore.open(tmp_path / "approval.sqlite")
    try:
        sub = SubagentStore(store)
        report = await sub.recover()
        # run 真实 awaiting 且带停靠恢复点：审批关联保留，等显式答复。
        assert report["approvals_preserved"] == 1
        # build_tree 的 root 自身任务（accepted、无结果）按同一规则结算，
        # 通知交宿主消费。
        assert report["tasks_interrupted"] == 1
        assert report["notifications_created"] == 1
        task = await sub.task(spawn["task_id"])
        assert task["status"] == "awaiting_approval"
        host_pending = await sub.notifications(tree["tree_id"], consumer_id=None, after_seq=0)
        assert len(host_pending) == 1
    finally:
        await store.close()


async def test_history_version_cas(tmp_path):
    store = await SQLiteStore.open(tmp_path / "history.sqlite")
    try:
        sub, tree, root_id, _, _ = await build_tree(store)
        saved = await sub.save_history(root_id, {"messages": ["a"]})
        assert saved == {"agent_id": root_id, "version": 1, "created": True}
        bumped = await sub.save_history(root_id, {"messages": ["a", "b"]}, expected_version=1)
        assert bumped["version"] == 2 and bumped["created"] is False
        with pytest.raises(ServiceError) as exc:
            await sub.save_history(root_id, {"messages": ["c"]}, expected_version=1)
        assert exc.value.code == "revision_conflict"
        found = await sub.history(root_id)
        assert found["version"] == 2 and found["payload"] == {"messages": ["a", "b"]}
        assert (await sub.get_agent(root_id))["history_version"] == 2
    finally:
        await store.close()


async def test_write_failure_marks_store_unhealthy(tmp_path):
    store = await SQLiteStore.open(tmp_path / "health.sqlite")
    try:
        await make_session(store)
        sub = SubagentStore(store)
        healthy_engine = store.engine

        class BrokenEngine:
            def begin(self):
                raise RuntimeError("disk gone")

        store.engine = BrokenEngine()
        with pytest.raises(ServiceError) as exc:
            await sub.ensure_tree(
                TreeScope(
                    session_id="s-1", workspace_id="ws-1", owner_id="owner-1", request_key="t1"
                )
            )
        assert exc.value.code == "storage_unavailable"
        assert exc.value.status == 503
        assert store.healthy is False
        with pytest.raises(ServiceError) as exc:
            await sub.ensure_tree(
                TreeScope(
                    session_id="s-1", workspace_id="ws-1", owner_id="owner-1", request_key="t2"
                )
            )
        assert exc.value.code == "storage_unavailable"
        store.engine = healthy_engine
    finally:
        store.engine = healthy_engine
        await store.close()


async def test_listing_is_incremental_and_bounded(tmp_path):
    store = await SQLiteStore.open(tmp_path / "list.sqlite")
    try:
        sub, tree, root_id, _, _ = await build_tree(store)
        for index in range(3):
            await spawn_child(
                sub, tree["tree_id"], root_id, f"lead/w-{index}", f"spawn-{index}"
            )
        page = await sub.list_agents(tree["tree_id"], limit=2)
        assert [a["path"] for a in page] == ["lead", "lead/w-0"]
        page = await sub.list_agents(tree["tree_id"], after_path="lead/w-0", limit=2)
        assert [a["path"] for a in page] == ["lead/w-1", "lead/w-2"]
        with pytest.raises(ServiceError) as exc:
            await sub.list_agents(tree["tree_id"], limit=0)
        assert exc.value.code == "invalid_input"
        with pytest.raises(ServiceError) as exc:
            await sub.inbox(root_id, after_seq=-1)
        assert exc.value.code == "invalid_cursor"
    finally:
        await store.close()
