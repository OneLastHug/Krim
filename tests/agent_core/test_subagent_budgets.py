"""计划 8.3 子代理共享请求预算：ensure/reserve/charge/release 与并发、提交回程。

测试用真 SQLiteStore；预算表由测试显式 create_all 补齐（全局 schema 阶梯由
主会话统一接入），不改动 sqlite.py。只测请求次数记账，不接付费模型。
"""

import asyncio
import sqlite3
from pathlib import Path

import pytest
from sqlalchemy import insert

from krim_agent.contracts import ServiceError
from krim_agent.storage.models import metadata, runs, sessions
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.storage.subagent_budget_models import BUDGET_TABLE_NAMES, BUDGET_TABLES
from krim_agent.subagents import SubagentStore, TreeScope
from krim_agent.subagents.budgets import RequestBudgets


def raw_rows(path: Path, names: list[str]) -> dict[str, list[tuple]]:
    conn = sqlite3.connect(path)
    try:
        return {name: conn.execute(f'SELECT * FROM "{name}"').fetchall() for name in names}
    finally:
        conn.close()


def table_names(path: Path) -> set[str]:
    conn = sqlite3.connect(path)
    try:
        return {
            row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    finally:
        conn.close()


async def open_budget_store(tmp_path: Path, name: str = "budgets.sqlite") -> SQLiteStore:
    """打开全新 store 并只补预算表；不改全局 schema 阶梯与 user_version。"""
    store = await SQLiteStore.open(tmp_path / name)
    async with store.engine.begin() as conn:
        await conn.run_sync(lambda sync: metadata.create_all(sync, tables=list(BUDGET_TABLES)))
    return store


async def make_session(store: SQLiteStore, session_id: str = "s-b") -> None:
    async with store.engine.begin() as conn:
        await conn.execute(
            insert(sessions).values(
                id=session_id,
                workspace_id="ws-1",
                owner_id="owner-1",
                create_key="k-" + session_id,
                request_hash="hash",
                title="预算宿主会话",
                target_key="target",
                target={},
                context_policy={},
                revision=1,
                status="open",
                dispatch_paused=False,
                created_at=1.0,
            )
        )


async def make_run(store: SQLiteStore, run_id: str) -> None:
    async with store.engine.begin() as conn:
        await conn.execute(
            insert(runs).values(
                id=run_id,
                request_key="rk-" + run_id,
                request_hash="hash",
                request={"text": "预算测试"},
                target={},
                status="running",
                trace_id="0" * 32,
                root_span_id="0" * 16,
                created_at=1.0,
                stop_requested=False,
            )
        )


async def make_tree(store: SQLiteStore, key: str = "tree-b", session_id: str = "s-b") -> str:
    await make_session(store, session_id)
    sub = SubagentStore(store)
    tree = await sub.ensure_tree(
        TreeScope(session_id=session_id, workspace_id="ws-1", owner_id="owner-1", request_key=key)
    )
    assert tree["created"] is True
    return tree["tree_id"]


async def test_budget_tables_exported_and_created(tmp_path):
    assert BUDGET_TABLE_NAMES == {
        "subagent_budgets",
        "subagent_budget_leases",
        "subagent_budget_charges",
    }
    assert BUDGET_TABLE_NAMES <= set(metadata.tables)
    store = await open_budget_store(tmp_path)
    try:
        assert BUDGET_TABLE_NAMES <= table_names(tmp_path / "budgets.sqlite")
        RequestBudgets(store)
    finally:
        await store.close()


async def test_ensure_idempotent_and_counts_initial_spent(tmp_path):
    store = await open_budget_store(tmp_path)
    try:
        tree_id = await make_tree(store)
        budgets = RequestBudgets(store)
        first = await budgets.ensure(tree_id, 10, initial_spent=3)
        assert first == {
            "tree_id": tree_id,
            "limit": 10,
            "spent": 3,
            "remaining": 7,
            "created": True,
        }
        again = await budgets.ensure(tree_id, 5, initial_spent=0)
        assert again["created"] is False
        assert again["limit"] == 10 and again["spent"] == 3
        fresh = await budgets.state(tree_id)
        assert fresh["limit"] == 10 and fresh["spent"] == 3 and fresh["remaining"] == 7
        with pytest.raises(ServiceError) as exc:
            await budgets.ensure("missing-tree", 10)
        assert exc.value.code == "not_found"
    finally:
        await store.close()


async def test_root_charge_counts_and_exhausts_with_step_limit(tmp_path):
    store = await open_budget_store(tmp_path)
    try:
        tree_id = await make_tree(store)
        await make_run(store, "root-run")
        budgets = RequestBudgets(store)
        await budgets.ensure(tree_id, 2)
        first = await budgets.charge(tree_id, "root-run", 0)
        assert first["attempt_id"] and first["created"] is True
        assert first["limit"] == 2 and first["remaining"] == 1
        assert first["tree_spent"] == 1 and first["tree_remaining"] == 1
        second = await budgets.charge(tree_id, "root-run", 1)
        assert second["remaining"] == 0 and second["tree_spent"] == 2
        with pytest.raises(ServiceError) as exc:
            await budgets.charge(tree_id, "root-run", 2)
        assert exc.value.code == "step_limit"
        state = await budgets.state(tree_id)
        assert state["spent"] == 2 and state["remaining"] == 0
        with pytest.raises(ServiceError) as exc:
            await budgets.charge(tree_id, "unknown-run", 0)
        assert exc.value.code == "not_found"
        with pytest.raises(ServiceError) as exc:
            await budgets.charge(tree_id, "root-run", -1)
        assert exc.value.code == "invalid_input"
    finally:
        await store.close()


async def test_unlimited_budget_inherits_none_end_to_end(tmp_path):
    store = await open_budget_store(tmp_path)
    try:
        tree_id = await make_tree(store)
        await make_run(store, "root-run")
        budgets = RequestBudgets(store)
        view = await budgets.ensure(tree_id, None)
        assert view["limit"] is None and view["remaining"] is None
        lease = await budgets.reserve(tree_id, "leaf", None)
        assert lease["limit"] is None and lease["remaining"] is None
        for step in range(4):
            receipt = await budgets.charge(tree_id, "root-run", step, lease_key="leaf")
            assert receipt["remaining"] is None and receipt["tree_remaining"] is None
        state = await budgets.state(tree_id)
        assert state["remaining"] is None and len(state["active_leases"]) == 1
        assert state["active_leases"][0]["remaining"] is None
    finally:
        await store.close()


async def test_reserve_default_half_keeps_one_and_admits_explicit_full_pool(tmp_path):
    store = await open_budget_store(tmp_path)
    try:
        tree_id = await make_tree(store)
        budgets = RequestBudgets(store)
        await budgets.ensure(tree_id, 10)
        c1 = await budgets.reserve(tree_id, "c1", None)
        assert c1["limit"] == 5 and c1["remaining"] == 5 and c1["tree_remaining"] == 5
        c2 = await budgets.reserve(tree_id, "c2", None)
        assert c2["limit"] == 2 and c2["tree_remaining"] == 3
        with pytest.raises(ServiceError) as exc:
            await budgets.reserve(tree_id, "c3", 99)
        assert exc.value.code == "capacity"
        c3 = await budgets.reserve(tree_id, "c3", None)
        assert c3["limit"] == 1 and c3["tree_remaining"] == 2
        c4 = await budgets.reserve(tree_id, "c4", 2)
        assert c4["limit"] == 2 and c4["tree_remaining"] == 0
        await make_run(store, "late-root")
        with pytest.raises(ServiceError) as exc:
            await budgets.charge(tree_id, "late-root", 0)
        assert exc.value.code == "step_limit"
    finally:
        await store.close()


async def test_reserve_default_rejects_when_cannot_keep_one(tmp_path):
    store = await open_budget_store(tmp_path)
    try:
        tree_id = await make_tree(store)
        budgets = RequestBudgets(store)
        await budgets.ensure(tree_id, 4)
        assert (await budgets.reserve(tree_id, "c1", None))["limit"] == 2
        assert (await budgets.reserve(tree_id, "c2", None))["limit"] == 1
        with pytest.raises(ServiceError) as exc:
            await budgets.reserve(tree_id, "c3", None)
        assert exc.value.code == "capacity"
    finally:
        await store.close()


async def test_reserve_replay_and_key_conflict(tmp_path):
    store = await open_budget_store(tmp_path)
    try:
        tree_id = await make_tree(store)
        budgets = RequestBudgets(store)
        await budgets.ensure(tree_id, 10)
        first = await budgets.reserve(tree_id, "lease-1", 3)
        replay = await budgets.reserve(tree_id, "lease-1", 3)
        assert replay["created"] is False and replay["key"] == first["key"]
        assert replay["limit"] == 3 and replay["remaining"] == 3
        with pytest.raises(ServiceError) as exc:
            await budgets.reserve(tree_id, "lease-1", 4)
        assert exc.value.code == "key_conflict"
        with pytest.raises(ServiceError) as exc:
            await budgets.reserve(tree_id, "lease-1", 3, parent_key="lease-1")
        assert exc.value.code == "key_conflict"
        state = await budgets.state(tree_id)
        assert state["outstanding"] == 3 and len(state["active_leases"]) == 1
    finally:
        await store.close()


async def test_nested_reserve_charges_from_own_lease_not_root(tmp_path):
    store = await open_budget_store(tmp_path)
    try:
        tree_id = await make_tree(store)
        await make_run(store, "child-run")
        budgets = RequestBudgets(store)
        await budgets.ensure(tree_id, 10)
        await budgets.reserve(tree_id, "c1", None)
        grant = await budgets.reserve(tree_id, "c1:g1", None, parent_key="c1")
        assert grant["limit"] == 2 and grant["parent_key"] == "c1"
        first = await budgets.charge(tree_id, "child-run", 0, lease_key="c1:g1")
        assert first["remaining"] == 1 and first["tree_spent"] == 1
        # 孙租约的划拨已含在 c1 的 5 次占位内：10 - 1(已花) - 5(c1 活跃) = 4
        assert first["tree_remaining"] == 4
        state = await budgets.state(tree_id)
        by_key = {item["key"]: item for item in state["active_leases"]}
        assert by_key["c1"]["remaining"] == 3
        assert by_key["c1:g1"]["remaining"] == 1
        with pytest.raises(ServiceError) as exc:
            await budgets.charge(tree_id, "child-run", 1, lease_key="missing")
        assert exc.value.code == "not_found"
        with pytest.raises(ServiceError) as exc:
            await budgets.reserve(tree_id, "c2", 2, parent_key="other-tree-lease")
        assert exc.value.code == "not_found"
        # c1 自身只剩 grant 5 - 子租约占位 2 = 3 次；g1 未用完前其占位不释放
        for step in range(1, 4):
            await budgets.charge(tree_id, "child-run", step, lease_key="c1")
        with pytest.raises(ServiceError) as exc:
            await budgets.charge(tree_id, "child-run", 4, lease_key="c1")
        assert exc.value.code == "step_limit"
        state = await budgets.state(tree_id)
        assert state["spent"] == 4
    finally:
        await store.close()


async def test_release_returns_unused_and_is_idempotent(tmp_path):
    store = await open_budget_store(tmp_path)
    try:
        tree_id = await make_tree(store)
        await make_run(store, "child-run")
        budgets = RequestBudgets(store)
        await budgets.ensure(tree_id, 10)
        await budgets.reserve(tree_id, "c1", None)
        await budgets.charge(tree_id, "child-run", 0, lease_key="c1")
        await budgets.charge(tree_id, "child-run", 1, lease_key="c1")
        released = await budgets.release(tree_id, "c1")
        assert released["released"] is True and released["returned"] == 3
        assert released["tree_remaining"] == 8
        repeat = await budgets.release(tree_id, "c1")
        assert repeat["released"] is False and repeat["returned"] == 0
        state = await budgets.state(tree_id)
        assert state["active_leases"] == [] and state["outstanding"] == 0
        assert state["remaining"] == 8 and state["spent"] == 2
        again = await budgets.reserve(tree_id, "c2", None)
        assert again["limit"] == 4 and again["tree_remaining"] == 4
        with pytest.raises(ServiceError) as exc:
            await budgets.release(tree_id, "missing-lease")
        assert exc.value.code == "not_found"
    finally:
        await store.close()


async def test_release_cascades_descendants_and_keeps_spent(tmp_path):
    store = await open_budget_store(tmp_path)
    try:
        tree_id = await make_tree(store)
        await make_run(store, "child-run")
        budgets = RequestBudgets(store)
        await budgets.ensure(tree_id, 12)
        await budgets.reserve(tree_id, "c1", None)
        await budgets.reserve(tree_id, "c1:g", None, parent_key="c1")
        await budgets.reserve(tree_id, "c1:g:gg", None, parent_key="c1:g")
        for key, step in (("c1", 0), ("c1:g", 1), ("c1:g:gg", 2)):
            await budgets.charge(tree_id, "child-run", step, lease_key=key)
        released = await budgets.release(tree_id, "c1")
        assert released["released"] is True
        assert released["returned"] == (6 - 1) + (3 - 1) + (1 - 1)
        assert released["tree_remaining"] == 9
        state = await budgets.state(tree_id)
        assert state["active_leases"] == [] and state["spent"] == 3
    finally:
        await store.close()


async def test_released_parent_and_lease_refuse_new_work(tmp_path):
    store = await open_budget_store(tmp_path)
    try:
        tree_id = await make_tree(store)
        await make_run(store, "child-run")
        budgets = RequestBudgets(store)
        await budgets.ensure(tree_id, 10)
        await budgets.reserve(tree_id, "c1", None)
        await budgets.release(tree_id, "c1")
        with pytest.raises(ServiceError) as exc:
            await budgets.reserve(tree_id, "c1:kid", None, parent_key="c1")
        assert exc.value.code == "capacity"
        with pytest.raises(ServiceError) as exc:
            await budgets.charge(tree_id, "child-run", 0, lease_key="c1")
        assert exc.value.code == "capacity"
    finally:
        await store.close()


async def test_charge_same_step_rereasoning_gets_independent_attempts(tmp_path):
    store = await open_budget_store(tmp_path)
    try:
        tree_id = await make_tree(store)
        await make_run(store, "root-run")
        budgets = RequestBudgets(store)
        await budgets.ensure(tree_id, None)
        first = await budgets.charge(tree_id, "root-run", 3)
        second = await budgets.charge(tree_id, "root-run", 3)
        assert first["attempt_id"] != second["attempt_id"]
        assert first["created"] is True and second["created"] is True
        state = await budgets.state(tree_id)
        assert state["spent"] == 2
        rows = raw_rows(tmp_path / "budgets.sqlite", ["subagent_budget_charges"])
        assert len(rows["subagent_budget_charges"]) == 2
    finally:
        await store.close()


async def test_concurrent_charges_serialize_within_limit(tmp_path):
    store = await open_budget_store(tmp_path)
    try:
        tree_id = await make_tree(store)
        await make_run(store, "root-run")
        budgets = RequestBudgets(store)
        await budgets.ensure(tree_id, 5)

        async def attempt(index: int):
            try:
                return await budgets.charge(tree_id, "root-run", index)
            except ServiceError as error:
                return error

        results = await asyncio.gather(*(attempt(i) for i in range(8)))
        receipts = [item for item in results if isinstance(item, dict)]
        failures = [item for item in results if isinstance(item, ServiceError)]
        assert len(receipts) == 5 and len(failures) == 3
        assert all(item.code == "step_limit" for item in failures)
        assert len({item["attempt_id"] for item in receipts}) == 5
        state = await budgets.state(tree_id)
        assert state["spent"] == 5 and state["remaining"] == 0
        rows = raw_rows(tmp_path / "budgets.sqlite", ["subagent_budget_charges"])
        assert len(rows["subagent_budget_charges"]) == 5
    finally:
        await store.close()


async def test_concurrent_default_reserves_drain_pool_deterministically(tmp_path):
    store = await open_budget_store(tmp_path)
    try:
        tree_id = await make_tree(store)
        budgets = RequestBudgets(store)
        await budgets.ensure(tree_id, 8)

        async def claim(key: str):
            try:
                return await budgets.reserve(tree_id, key, None)
            except ServiceError as error:
                return error

        results = await asyncio.gather(*(claim(f"c{i}") for i in range(4)))
        grants = sorted(item["limit"] for item in results if isinstance(item, dict))
        failures = [item for item in results if isinstance(item, ServiceError)]
        assert grants == [1, 2, 4]
        assert len(failures) == 1 and failures[0].code == "capacity"
        state = await budgets.state(tree_id)
        assert state["outstanding"] == 7 and state["remaining"] == 1
    finally:
        await store.close()


async def test_commit_backtrack_charge_confirmed_by_attempt_id(tmp_path, monkeypatch):
    store = await open_budget_store(tmp_path)
    try:
        tree_id = await make_tree(store)
        await make_run(store, "root-run")
        budgets = RequestBudgets(store)
        await budgets.ensure(tree_id, 5)
        original = SQLiteStore._write

        async def commit_then_lose(self, operation):
            await original(self, operation)  # 事务已提交，随后丢失回程
            raise ServiceError("storage_unavailable", 503)

        monkeypatch.setattr(SQLiteStore, "_write", commit_then_lose)
        receipt = await budgets.charge(tree_id, "root-run", 0)
        assert receipt["created"] is False and receipt["tree_spent"] == 1
        assert store.healthy is True
        monkeypatch.setattr(SQLiteStore, "_write", original)
        state = await budgets.state(tree_id)
        assert state["spent"] == 1 and state["remaining"] == 4
        rows = raw_rows(tmp_path / "budgets.sqlite", ["subagent_budget_charges"])
        assert len(rows["subagent_budget_charges"]) == 1
    finally:
        await store.close()


async def test_unknown_storage_failure_poisons_whole_store(tmp_path, monkeypatch):
    store = await open_budget_store(tmp_path)
    try:
        tree_id = await make_tree(store)
        await make_run(store, "root-run")
        budgets = RequestBudgets(store)
        await budgets.ensure(tree_id, 5)

        async def dead_write(self, operation):
            raise RuntimeError("disk on fire")

        monkeypatch.setattr(SQLiteStore, "_write", dead_write)
        with pytest.raises(ServiceError) as exc:
            await budgets.charge(tree_id, "root-run", 0)
        assert exc.value.code == "storage_unavailable"
        assert store.healthy is False
        with pytest.raises(ServiceError) as exc:
            await budgets.charge(tree_id, "root-run", 1)
        assert exc.value.code == "storage_unavailable"
        rows = raw_rows(tmp_path / "budgets.sqlite", ["subagent_budget_charges"])
        assert rows["subagent_budget_charges"] == []
    finally:
        await store.close()


async def test_cold_recovery_keeps_spent_budget(tmp_path):
    store = await open_budget_store(tmp_path)
    database = tmp_path / "budgets.sqlite"
    try:
        tree_id = await make_tree(store)
        await make_run(store, "child-run")
        budgets = RequestBudgets(store)
        await budgets.ensure(tree_id, 10)
        await budgets.reserve(tree_id, "c1", None)
        await budgets.charge(tree_id, "child-run", 0, lease_key="c1")
        await budgets.charge(tree_id, "child-run", 1, lease_key="c1")
    finally:
        await store.close()
    rows = raw_rows(database, ["subagent_budgets", "subagent_budget_leases"])
    assert rows["subagent_budgets"][0][2] == 2  # budgets.spent
    assert rows["subagent_budget_leases"][0][5] == 2  # leases.spent
    revived = await SQLiteStore.open(database)
    try:
        async with revived.engine.begin() as conn:
            await conn.run_sync(
                lambda sync: metadata.create_all(sync, tables=list(BUDGET_TABLES))
            )
        fresh = RequestBudgets(revived)
        state = await fresh.state(tree_id)
        assert state["spent"] == 2 and state["remaining"] == 3
        assert state["active_leases"][0]["key"] == "c1"
        again = await fresh.ensure(tree_id, 10)
        assert again["created"] is False and again["spent"] == 2
        replay = await fresh.reserve(tree_id, "c1", None)
        assert replay["created"] is False and replay["spent"] == 2
    finally:
        await revived.close()


async def test_initial_spent_exceeding_limit_blocks_charges(tmp_path):
    store = await open_budget_store(tmp_path)
    try:
        tree_id = await make_tree(store)
        await make_run(store, "root-run")
        budgets = RequestBudgets(store)
        view = await budgets.ensure(tree_id, 2, initial_spent=5)
        assert view["spent"] == 5 and view["remaining"] == -3
        with pytest.raises(ServiceError) as exc:
            await budgets.charge(tree_id, "root-run", 0)
        assert exc.value.code == "step_limit"
        with pytest.raises(ServiceError) as exc:
            await budgets.reserve(tree_id, "c1", None)
        assert exc.value.code == "capacity"
    finally:
        await store.close()


async def test_invalid_inputs_and_unensured_tree(tmp_path):
    store = await open_budget_store(tmp_path)
    try:
        tree_id = await make_tree(store)
        budgets = RequestBudgets(store)
        with pytest.raises(ServiceError) as exc:
            await budgets.state(tree_id)
        assert exc.value.code == "not_found"
        with pytest.raises(ServiceError) as exc:
            await budgets.charge(tree_id, "run-x", 0)
        assert exc.value.code == "not_found"
        await budgets.ensure(tree_id, 10)
        with pytest.raises(ServiceError) as exc:
            await budgets.ensure(tree_id, -1)
        assert exc.value.code == "invalid_input"
        with pytest.raises(ServiceError) as exc:
            await budgets.reserve(tree_id, "bad", 0)
        assert exc.value.code == "invalid_input"
        with pytest.raises(ServiceError) as exc:
            await budgets.reserve(tree_id, "x" * 129, None)
        assert exc.value.code == "invalid_input"
        with pytest.raises(ServiceError) as exc:
            await budgets.release("other-tree", "k")
        assert exc.value.code == "not_found"
    finally:
        await store.close()


async def test_readonly_store_blocks_budget_writes_but_reads(tmp_path):
    database = tmp_path / "readonly.sqlite"
    store = await open_budget_store(tmp_path, "readonly.sqlite")
    try:
        tree_id = await make_tree(store)
        await make_run(store, "root-run")
        budgets = RequestBudgets(store)
        await budgets.ensure(tree_id, 5)
        await budgets.charge(tree_id, "root-run", 0)
    finally:
        await store.close()
    reopened = await SQLiteStore.open(database, readonly=True)
    try:
        async with reopened.engine.begin() as conn:
            await conn.run_sync(
                lambda sync: metadata.create_all(sync, tables=list(BUDGET_TABLES))
            )
        frozen = RequestBudgets(reopened)
        state = await frozen.state(tree_id)
        assert state["spent"] == 1 and state["remaining"] == 4
        with pytest.raises(ServiceError) as exc:
            await frozen.ensure(tree_id, 9)
        assert exc.value.code == "readonly"
        with pytest.raises(ServiceError) as exc:
            await frozen.charge(tree_id, "root-run", 1)
        assert exc.value.code == "readonly"
    finally:
        await reopened.close()
