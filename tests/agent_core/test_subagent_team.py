"""TeamService 阶段 7 验收：真实 SQLiteStore 上的依赖、归属、CAS 与证据。

树/成员行按 subagent_models 的约束直接落库（SubagentStore API 尚未定稿，
这里只做行准备，不复制任何业务规则）；宿主会话走生产 create_session，
委派来源 run 走生产 store.begin，不伪造用户 turn。
"""

from __future__ import annotations

import itertools
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import insert, update

from krim_agent.config import Settings
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import RunRequest, ServiceError, fingerprint
from krim_agent.sessions.types import SessionCreate
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.sqlite import SCHEMA_VERSION, SQLiteStore
from krim_agent.storage.subagent_models import subagent_members, subagent_tasks, subagent_trees
from krim_agent.storage.subagent_team_models import TEAM_TABLE_NAMES
from krim_agent.subagents.team import (
    TeamEvidence,
    TeamService,
    TeamTaskClaim,
    TeamTaskCreate,
    TeamTaskQuery,
    TeamTaskReassign,
    TeamTaskUpdate,
    _confirm_claim,
    _confirm_create,
    _confirm_reassign,
    _confirm_update,
)

WORKSPACE = "local"
OWNER = "local"


def artifact(key: str, body: dict[str, Any] | None = None) -> TeamEvidence:
    return TeamEvidence(kind="artifact", body=body or {"path": "out/report.md"}, request_key=key)


def check(key: str, body: dict[str, Any] | None = None) -> TeamEvidence:
    return TeamEvidence(kind="check", body=body or {"passed": True}, request_key=key)


async def write(store: SQLiteStore, operation: Any) -> None:
    """测试脚手架直写；行准备无副作用重放，confirm 恒 None 保持失败诚实。"""

    async def confirm() -> None:
        return None

    await store.work_write(operation, confirm)


async def seed_tree(
    store: SQLiteStore,
    tree_key: str,
    members_spec: tuple[tuple[str, str | None, str], ...] = (
        ("root", None, "root"),
        ("w1", "root", "agent"),
        ("w2", "root", "agent"),
        ("w3", "root", "agent"),
    ),
) -> dict[str, Any]:
    """经生产 create_session 建宿主会话后，按表约束准备树与成员行。"""
    session = await session_sql.create_session(
        store,
        WORKSPACE,
        OWNER,
        SessionCreate(request_key=f"session-{tree_key}"),
        {"key": "demo"},
        ContextPolicy(),
        Settings(),
    )
    tree_id = str(uuid.uuid4())
    now = time.time()
    paths: dict[str, str] = {}
    depths: dict[str, int] = {}
    for name, parent, _ in members_spec:
        paths[name] = name if parent is None else f"{paths[parent]}/{name}"
        depths[name] = 0 if parent is None else depths[parent] + 1
    ids: dict[str, str] = {name: str(uuid.uuid4()) for name, _, _ in members_spec}

    async def seed(conn: Any) -> None:
        await conn.execute(
            insert(subagent_trees).values(
                id=tree_id,
                session_id=session.id,
                root_turn_id=None,
                root_run_id=None,
                workspace_id=WORKSPACE,
                owner_id=OWNER,
                project_id=None,
                request_key=tree_key,
                request_hash=fingerprint({"tree": tree_key}),
                input_version=1,
                revision=1,
                status="open",
                close_reason=None,
                notify_cursor=0,
                created_at=now,
                closed_at=None,
            )
        )
        rows = []
        for name, parent, role in members_spec:
            rows.append(
                dict(
                    id=ids[name],
                    tree_id=tree_id,
                    parent_id=None if parent is None else ids[parent],
                    path=paths[name],
                    role=role,
                    provider="sdk_session",
                    profile="default",
                    provider_version="v1",
                    profile_version="v1",
                    workdir=None,
                    depth=depths[name],
                    status="active",
                    current_task_id=None,
                    inbox_cursor=0,
                    notify_cursor=0,
                    revision=1,
                    request_key=f"{tree_key}:{name}",
                    request_hash=fingerprint({"member": name}),
                    created_at=now,
                    closed_at=None,
                )
            )
        await conn.execute(insert(subagent_members).values(rows))

    await write(store, seed)
    return {"tree_id": tree_id, "session_id": session.id, "ids": ids}


async def seed_delegation_task(store: SQLiteStore, tree: dict[str, Any], member: str) -> str:
    """在指定树准备一行真实委派任务（Team 任务来源引用的跨树反例）。"""
    task_id = str(uuid.uuid4())
    body = {"goal": "foreign delegation"}

    async def seed(conn: Any) -> None:
        await conn.execute(
            insert(subagent_tasks).values(
                id=task_id,
                agent_id=tree["ids"][member],
                tree_id=tree["tree_id"],
                ordinal=1,
                status="accepted",
                run_id=None,
                parent_run_id=None,
                source_version=1,
                provider_version="v1",
                profile_version="v1",
                input=body,
                input_hash=fingerprint(body),
                request_key=f"foreign:{task_id}",
                request_hash=fingerprint({"task": task_id}),
                revision=1,
                created_at=time.time(),
                finished_at=None,
            )
        )

    await write(store, seed)
    return task_id


class Board:
    """围绕一棵已准备树的 TeamService 便捷封装；caller 用成员名。"""

    def __init__(self, store: SQLiteStore, ids: dict[str, Any]) -> None:
        self.store = store
        self.ids = ids
        self.service = TeamService(store)
        self._keys = itertools.count(1)

    def key(self, prefix: str = "k") -> str:
        return f"{prefix}:{next(self._keys)}"

    def member(self, name: str) -> str:
        return self.ids["ids"][name]

    async def create(
        self, goal: str, *, caller: str = "root", deps: tuple[str, ...] = (), **kw: Any
    ) -> dict[str, Any]:
        return await self.service.create(
            TeamTaskCreate(
                tree_id=self.ids["tree_id"],
                caller_id=self.member(caller),
                goal=goal,
                dependencies=deps,
                request_key=self.key("create"),
                **kw,
            )
        )

    async def claim(self, task_id: str, caller: str, expected_revision: int) -> dict[str, Any]:
        return await self.service.claim(
            TeamTaskClaim(
                tree_id=self.ids["tree_id"],
                task_id=task_id,
                caller_id=self.member(caller),
                expected_revision=expected_revision,
            )
        )

    async def update(
        self, task_id: str, caller: str, expected_revision: int, **kw: Any
    ) -> dict[str, Any]:
        return await self.service.update(
            TeamTaskUpdate(
                tree_id=self.ids["tree_id"],
                task_id=task_id,
                caller_id=self.member(caller),
                expected_revision=expected_revision,
                **kw,
            )
        )

    async def reassign(
        self, task_id: str, caller: str, expected_revision: int, **kw: Any
    ) -> dict[str, Any]:
        return await self.service.reassign(
            TeamTaskReassign(
                tree_id=self.ids["tree_id"],
                task_id=task_id,
                caller_id=self.member(caller),
                expected_revision=expected_revision,
                **kw,
            )
        )

    async def list(self, caller: str = "root", **filters: Any) -> list[dict[str, Any]]:
        return await self.service.list(
            TeamTaskQuery(
                tree_id=self.ids["tree_id"],
                caller_id=self.member(caller),
                owner_id=self.member(filters["owner"]) if "owner" in filters else None,
                status=filters.get("status"),
            )
        )

    async def started(
        self, goal: str, caller: str = "w1", deps: tuple[str, ...] = ()
    ) -> dict[str, Any]:
        task = await self.create(goal, deps=deps)
        claimed = await self.claim(task["id"], caller, task["revision"])
        return await self.update(
            task["id"],
            caller,
            claimed["revision"],
            expected_ownership_version=claimed["ownership_version"],
            status="in_progress",
        )

    async def close_member(self, name: str) -> None:
        async def operation(conn: Any) -> None:
            await conn.execute(
                update(subagent_members)
                .where(subagent_members.c.id == self.member(name))
                .values(status="closed", closed_at=time.time())
            )

        await write(self.store, operation)

    async def close_tree(self) -> None:
        async def operation(conn: Any) -> None:
            await conn.execute(
                update(subagent_trees)
                .where(subagent_trees.c.id == self.ids["tree_id"])
                .values(status="closed", closed_at=time.time())
            )

        await write(self.store, operation)


async def test_fresh_store_contains_team_tables(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        assert store.schema_version == SCHEMA_VERSION
        with sqlite3.connect(tmp_path / "team.sqlite") as conn:
            tables = {
                row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
        assert TEAM_TABLE_NAMES <= tables
    finally:
        await store.close()


async def test_create_is_idempotent_per_request_key_and_orders_ordinally(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t1")
        board = Board(store, ids)
        request = TeamTaskCreate(
            tree_id=ids["tree_id"],
            caller_id=board.member("root"),
            goal="调研依赖",
            dependencies=(),
            request_key="board:1",
        )
        first = await board.service.create(request)
        replay = await board.service.create(request)
        assert replay["id"] == first["id"]
        assert replay["ordinal"] == 1
        second = await board.create("另一项工作")
        assert second["ordinal"] == 2
        with pytest.raises(ServiceError, match="key_conflict"):
            await board.service.create(request.model_copy(update={"goal": "不同的目标"}))
    finally:
        await store.close()


async def test_create_validates_the_dependency_graph(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t2")
        other = await seed_tree(store, "t2-other")
        board = Board(store, ids)
        t_a = await board.create("先做")
        t_b = await board.create("后做", deps=(t_a["id"],))
        t_c = await board.create("并行", deps=(t_a["id"],))
        t_d = await board.create("汇聚", deps=(t_b["id"], t_c["id"]))
        assert set(dep["task_id"] for dep in t_d["dependencies"]) == {t_b["id"], t_c["id"]}
        assert all(dep["status"] == "open" for dep in t_d["dependencies"])
        with pytest.raises(ServiceError, match="invalid_input"):
            await board.create("未知依赖", deps=(str(uuid.uuid4()),))
        foreign = await Board(store, other).create("别树任务")
        with pytest.raises(ServiceError, match="invalid_input"):
            await board.create("跨树依赖", deps=(foreign["id"],))
        with pytest.raises(ServiceError, match="invalid_input"):
            await board.create("重复依赖", deps=(t_a["id"], t_a["id"]))
        with pytest.raises(ServiceError, match="invalid_input"):
            await board.update(t_a["id"], "root", t_a["revision"], dependencies=(t_a["id"],))
        with pytest.raises(ServiceError, match="invalid_input"):
            await board.update(t_a["id"], "root", t_a["revision"], dependencies=(t_b["id"],))
    finally:
        await store.close()


async def test_caller_must_be_real_active_member_of_the_tree(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t3")
        other = await seed_tree(store, "t3-other")
        board = Board(store, ids)
        with pytest.raises(ServiceError, match="permission_denied"):
            await board.service.create(
                TeamTaskCreate(
                    tree_id=ids["tree_id"],
                    caller_id=str(uuid.uuid4()),
                    goal="伪造身份",
                    request_key=board.key(),
                )
            )
        outsider = Board(store, other)
        with pytest.raises(ServiceError, match="permission_denied"):
            await outsider.service.create(
                TeamTaskCreate(
                    tree_id=ids["tree_id"],
                    caller_id=outsider.member("root"),
                    goal="别树成员",
                    request_key=outsider.key(),
                )
            )
        await board.close_member("w2")
        with pytest.raises(ServiceError, match="closed"):
            await board.create("关闭成员调用", caller="w2")
        with pytest.raises(ServiceError, match="not_found"):
            await board.service.create(
                TeamTaskCreate(
                    tree_id=str(uuid.uuid4()),
                    caller_id=board.member("root"),
                    goal="树不存在",
                    request_key=board.key(),
                )
            )
    finally:
        await store.close()


async def test_goal_and_dependency_edits_are_coordinator_only(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t4")
        board = Board(store, ids)
        task = await board.create("原目标")
        with pytest.raises(ServiceError, match="permission_denied"):
            await board.update(task["id"], "w1", task["revision"], goal="worker 改目标")
        with pytest.raises(ServiceError, match="permission_denied"):
            await board.update(task["id"], "w1", task["revision"], dependencies=())
        edited = await board.update(task["id"], "root", task["revision"], goal="协调者修订目标")
        assert edited["goal"] == "协调者修订目标"
        assert edited["revision"] == task["revision"] + 1
    finally:
        await store.close()


async def test_claim_sets_owner_and_rejects_races(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t5")
        board = Board(store, ids)
        task = await board.create("领取我")
        claimed = await board.claim(task["id"], "w1", task["revision"])
        assert claimed["owner_id"] == board.member("w1")
        assert claimed["status"] == "claimed"
        assert claimed["revision"] == task["revision"] + 1
        assert claimed["ownership_version"] == task["ownership_version"] + 1
        with pytest.raises(ServiceError, match="revision_conflict"):
            await board.claim(task["id"], "w2", task["revision"])
        with pytest.raises(ServiceError, match="subagent_task_closed"):
            await board.claim(task["id"], "w2", claimed["revision"])
        await board.close_member("w2")
        with pytest.raises(ServiceError, match="closed"):
            await board.claim(task["id"], "w2", claimed["revision"])
    finally:
        await store.close()


async def test_claim_requires_completed_dependencies(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t6")
        board = Board(store, ids)
        first = await board.create("前置")
        second = await board.create("后置", deps=(first["id"],))
        with pytest.raises(ServiceError, match="invalid_input") as unmet:
            await board.claim(second["id"], "w1", second["revision"])
        assert unmet.value.action == "wait_dependencies"
        claimed = await board.claim(first["id"], "w1", first["revision"])
        await board.update(
            first["id"],
            "w1",
            claimed["revision"],
            expected_ownership_version=claimed["ownership_version"],
            status="completed",
            evidence=(artifact("done"),),
        )
        ok = await board.claim(second["id"], "w2", second["revision"])
        assert ok["owner_id"] == board.member("w2")
    finally:
        await store.close()


async def test_failed_dependency_blocks_dependents_and_keeps_ownership(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t7")
        board = Board(store, ids)
        first = await board.started("会失败的工作", caller="w1")
        second = await board.create("被阻塞的工作", deps=(first["id"],))
        third = await board.create("传递依赖", deps=(second["id"],))
        # 依赖未完成时第二个任务不可领取。
        with pytest.raises(ServiceError, match="invalid_input") as premature:
            await board.claim(second["id"], "w2", second["revision"])
        assert premature.value.action == "wait_dependencies"
        failed = await board.update(
            first["id"],
            "w1",
            first["revision"],
            expected_ownership_version=first["ownership_version"],
            status="failed",
        )
        assert failed["status"] == "failed"
        rows = {row["id"]: row for row in await board.list()}
        blocked = rows[second["id"]]
        assert blocked["status"] == "blocked"
        assert blocked["revision"] == second["revision"] + 1
        assert rows[third["id"]]["status"] == "open"
        with pytest.raises(ServiceError, match="subagent_task_closed"):
            await board.claim(second["id"], "w2", blocked["revision"])
        with pytest.raises(ServiceError, match="subagent_task_closed"):
            await board.update(
                second["id"],
                "root",
                blocked["revision"],
                expected_ownership_version=blocked["ownership_version"],
                status="completed",
                evidence=(check("late"),),
            )
        with pytest.raises(ServiceError, match="invalid_input") as unmet:
            await board.claim(third["id"], "w3", third["revision"])
        assert unmet.value.action == "wait_dependencies"
    finally:
        await store.close()


async def test_blocked_owner_keeps_task_and_cannot_finish_it(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t8")
        board = Board(store, ids)
        doomed = await board.started("将失败", caller="w1")
        await board.update(
            doomed["id"],
            "w1",
            doomed["revision"],
            expected_ownership_version=doomed["ownership_version"],
            status="failed",
        )
        task = await board.create("认领后受阻")
        claimed = await board.claim(task["id"], "w2", task["revision"])
        blocked = await board.update(
            task["id"], "root", claimed["revision"], dependencies=(doomed["id"],)
        )
        assert blocked["status"] == "blocked"
        assert blocked["owner_id"] == board.member("w2")
        assert blocked["ownership_version"] == claimed["ownership_version"]
        with pytest.raises(ServiceError, match="subagent_task_closed"):
            await board.update(
                task["id"],
                "w2",
                blocked["revision"],
                expected_ownership_version=blocked["ownership_version"],
                status="in_progress",
            )
        with pytest.raises(ServiceError, match="subagent_task_closed"):
            await board.update(
                task["id"],
                "w2",
                blocked["revision"],
                expected_ownership_version=blocked["ownership_version"],
                status="completed",
                evidence=(artifact("blocked"),),
            )
    finally:
        await store.close()


async def test_reopening_blocked_tasks_is_coordinator_only(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t9")
        board = Board(store, ids)
        first = await board.started("失败源", caller="w1")
        second = await board.create("受阻", deps=(first["id"],))
        await board.update(
            first["id"],
            "w1",
            first["revision"],
            expected_ownership_version=first["ownership_version"],
            status="failed",
        )
        rows = {row["id"]: row for row in await board.list()}
        blocked = rows[second["id"]]
        assert blocked["status"] == "blocked"
        with pytest.raises(ServiceError, match="permission_denied"):
            await board.update(second["id"], "w2", blocked["revision"], dependencies=())
        with pytest.raises(ServiceError, match="invalid_input"):
            await board.update(second["id"], "root", blocked["revision"], status="open")
        dirty = await board.update(
            second["id"], "root", blocked["revision"], dependencies=(first["id"],)
        )
        assert dirty["status"] == "blocked"
        reopened = await board.update(second["id"], "root", dirty["revision"], dependencies=())
        assert reopened["status"] == "open"
        claimed = await board.claim(reopened["id"], "w2", reopened["revision"])
        assert claimed["owner_id"] == board.member("w2")
    finally:
        await store.close()


async def test_replacing_dependencies_with_failed_ones_blocks_claimed_tasks(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t10")
        board = Board(store, ids)
        doomed = await board.started("将失败", caller="w1")
        await board.update(
            doomed["id"],
            "w1",
            doomed["revision"],
            expected_ownership_version=doomed["ownership_version"],
            status="failed",
        )
        task = await board.create("进行中")
        claimed = await board.claim(task["id"], "w2", task["revision"])
        edited = await board.update(
            task["id"], "root", claimed["revision"], dependencies=(doomed["id"],)
        )
        assert edited["status"] == "blocked"
        assert edited["owner_id"] == board.member("w2")
    finally:
        await store.close()


async def test_create_with_failed_dependency_starts_blocked(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t11")
        board = Board(store, ids)
        doomed = await board.started("失败源", caller="w1")
        await board.update(
            doomed["id"],
            "w1",
            doomed["revision"],
            expected_ownership_version=doomed["ownership_version"],
            status="failed",
        )
        created = await board.create("直接受阻", deps=(doomed["id"],))
        assert created["status"] == "blocked"
        with pytest.raises(ServiceError, match="subagent_task_closed"):
            await board.claim(created["id"], "w2", created["revision"])
    finally:
        await store.close()


async def test_owner_starts_with_ownership_version_cas(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t12")
        board = Board(store, ids)
        task = await board.create("开始我")
        claimed = await board.claim(task["id"], "w1", task["revision"])
        with pytest.raises(ServiceError, match="invalid_input"):
            await board.update(task["id"], "w1", claimed["revision"], status="in_progress")
        with pytest.raises(ServiceError, match="revision_conflict"):
            await board.update(
                task["id"],
                "w1",
                claimed["revision"],
                expected_ownership_version=claimed["ownership_version"] + 3,
                status="in_progress",
            )
        with pytest.raises(ServiceError, match="revision_conflict"):
            await board.update(
                task["id"],
                "w1",
                claimed["revision"] + 5,
                expected_ownership_version=claimed["ownership_version"],
                status="in_progress",
            )
        started = await board.update(
            task["id"],
            "w1",
            claimed["revision"],
            expected_ownership_version=claimed["ownership_version"],
            status="in_progress",
        )
        assert started["status"] == "in_progress"
        assert started["ownership_version"] == claimed["ownership_version"]
        # 同状态重放也必须由 owner 或协调者发起，其他成员不能借幂等请求越权。
        with pytest.raises(ServiceError, match="subagent_task_closed"):
            await board.claim(task["id"], "w2", started["revision"])
        with pytest.raises(ServiceError, match="permission_denied"):
            await board.update(
                task["id"],
                "w2",
                started["revision"],
                expected_ownership_version=started["ownership_version"],
                status="in_progress",
            )
    finally:
        await store.close()


async def test_start_rechecks_replaced_dependencies(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t13")
        board = Board(store, ids)
        pending = await board.create("未完成前置")
        task = await board.create("主体")
        claimed = await board.claim(task["id"], "w1", task["revision"])
        await board.update(task["id"], "root", claimed["revision"], dependencies=(pending["id"],))
        with pytest.raises(ServiceError, match="invalid_input"):
            await board.update(
                task["id"],
                "w1",
                claimed["revision"] + 1,
                expected_ownership_version=claimed["ownership_version"],
                status="in_progress",
            )
    finally:
        await store.close()


async def test_completion_requires_attached_evidence(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t14")
        board = Board(store, ids)
        task = await board.create("要有证据")
        claimed = await board.claim(task["id"], "w1", task["revision"])
        with pytest.raises(ServiceError, match="invalid_input") as lacking:
            await board.update(
                task["id"],
                "w1",
                claimed["revision"],
                expected_ownership_version=claimed["ownership_version"],
                status="completed",
            )
        assert lacking.value.action == "attach an artifact or check result to complete"
        completed = await board.update(
            task["id"],
            "w1",
            claimed["revision"],
            expected_ownership_version=claimed["ownership_version"],
            status="completed",
            evidence=(artifact(board.key("ev")),),
        )
        assert completed["status"] == "completed"
        assert completed["completed_at"] is not None
        assert len(completed["evidence"]) == 1
    finally:
        await store.close()


async def test_pre_attached_evidence_satisfies_completion(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t15")
        board = Board(store, ids)
        task = await board.create("先附证据")
        claimed = await board.claim(task["id"], "w1", task["revision"])
        attached = await board.update(
            task["id"],
            "w1",
            claimed["revision"],
            expected_ownership_version=claimed["ownership_version"],
            evidence=(artifact(board.key("ev")),),
        )
        assert attached["status"] == "claimed"
        assert attached["revision"] == claimed["revision"] + 1
        completed = await board.update(
            task["id"],
            "w1",
            attached["revision"],
            expected_ownership_version=attached["ownership_version"],
            status="completed",
        )
        assert completed["status"] == "completed"
    finally:
        await store.close()


async def test_evidence_replay_is_idempotent_per_request_key(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t16")
        board = Board(store, ids)
        task = await board.create("证据重放")
        claimed = await board.claim(task["id"], "w1", task["revision"])
        evidence = artifact("ev:replay", {"path": "same.md"})
        first = await board.update(
            task["id"],
            "w1",
            claimed["revision"],
            expected_ownership_version=claimed["ownership_version"],
            evidence=(evidence,),
        )
        second = await board.update(
            task["id"],
            "w1",
            first["revision"],
            expected_ownership_version=first["ownership_version"],
            evidence=(evidence,),
        )
        assert len(second["evidence"]) == 1
        assert second["revision"] == first["revision"] + 1
        with pytest.raises(ServiceError, match="key_conflict"):
            await board.update(
                task["id"],
                "w1",
                second["revision"],
                expected_ownership_version=second["ownership_version"],
                evidence=(artifact("ev:replay", {"path": "other.md"}),),
            )
    finally:
        await store.close()


async def test_board_changes_never_complete_the_task_themselves(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t17")
        board = Board(store, ids)
        task = await board.started("执行成功不等于验收", caller="w1")
        rows = {row["id"]: row for row in await board.list()}
        assert rows[task["id"]]["status"] == "in_progress"
        await board.update(
            task["id"],
            "w1",
            task["revision"],
            expected_ownership_version=task["ownership_version"],
            evidence=(artifact(board.key("ev")),),
        )
        rows = {row["id"]: row for row in await board.list()}
        assert rows[task["id"]]["status"] == "in_progress"
    finally:
        await store.close()


async def test_root_completes_and_fails_member_tasks_without_releasing_owner(
    tmp_path: Path,
) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t18")
        board = Board(store, ids)
        done = await board.started("成员做协调者收", caller="w1")
        completed = await board.update(
            done["id"],
            "root",
            done["revision"],
            expected_ownership_version=done["ownership_version"],
            status="completed",
            evidence=(check(board.key("accept")),),
        )
        assert completed["status"] == "completed"
        assert completed["owner_id"] == board.member("w1")
        lost = await board.started("成员做协调者判失败", caller="w2")
        failed = await board.update(
            lost["id"],
            "root",
            lost["revision"],
            expected_ownership_version=lost["ownership_version"],
            status="failed",
        )
        assert failed["status"] == "failed"
        assert failed["owner_id"] == board.member("w2")
    finally:
        await store.close()


async def test_cancel_is_coordinator_only_and_blocks_dependents(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t19")
        board = Board(store, ids)
        root_task = await board.started("被取消", caller="w1")
        child = await board.create("随之阻塞", deps=(root_task["id"],))
        with pytest.raises(ServiceError, match="permission_denied"):
            await board.update(
                root_task["id"],
                "w1",
                root_task["revision"],
                expected_ownership_version=root_task["ownership_version"],
                status="cancelled",
            )
        cancelled = await board.update(
            root_task["id"],
            "root",
            root_task["revision"],
            expected_ownership_version=root_task["ownership_version"],
            status="cancelled",
        )
        assert cancelled["status"] == "cancelled"
        rows = {row["id"]: row for row in await board.list()}
        assert rows[child["id"]]["status"] == "blocked"
    finally:
        await store.close()


async def test_reassign_invalidates_the_old_owner_completions(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t20")
        board = Board(store, ids)
        task = await board.started("转交后旧归属失效", caller="w1")
        attached = await board.update(
            task["id"],
            "w1",
            task["revision"],
            expected_ownership_version=task["ownership_version"],
            evidence=(artifact("old-owner-artifact", {"path": "old.md"}),),
        )
        transferred = await board.reassign(
            task["id"],
            "root",
            attached["revision"],
            expected_ownership_version=attached["ownership_version"],
            new_owner_id=board.member("w2"),
        )
        assert transferred["owner_id"] == board.member("w2")
        assert transferred["status"] == "claimed"
        assert transferred["ownership_version"] == attached["ownership_version"] + 1
        with pytest.raises(ServiceError, match="revision_conflict"):
            await board.update(
                task["id"],
                "w1",
                attached["revision"],
                expected_ownership_version=attached["ownership_version"],
                status="completed",
                evidence=(check("stale"),),
            )
        with pytest.raises(ServiceError, match="revision_conflict"):
            await board.update(
                task["id"],
                "w1",
                attached["revision"],
                expected_ownership_version=attached["ownership_version"],
                evidence=(artifact("late"),),
            )
        rows = {row["id"]: row for row in await board.list()}
        old_evidence = [
            item
            for item in rows[task["id"]]["evidence"]
            if item["request_key"] == "old-owner-artifact"
        ]
        assert old_evidence
        assert old_evidence[0]["agent_id"] == board.member("w1")
        assert old_evidence[0]["ownership_version"] == attached["ownership_version"]
        started = await board.update(
            task["id"],
            "w2",
            transferred["revision"],
            expected_ownership_version=transferred["ownership_version"],
            status="in_progress",
        )
        completed = await board.update(
            task["id"],
            "w2",
            started["revision"],
            expected_ownership_version=started["ownership_version"],
            status="completed",
            evidence=(check("new-owner"),),
        )
        assert completed["status"] == "completed"
    finally:
        await store.close()


async def test_reassign_authority_is_root_or_owner_ancestor(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(
            store,
            "t21",
            members_spec=(
                ("root", None, "root"),
                ("coord", "root", "agent"),
                ("worker", "coord", "agent"),
                ("peer", "root", "agent"),
            ),
        )
        board = Board(store, ids)
        task = await board.started("祖先协调", caller="worker")
        with pytest.raises(ServiceError, match="permission_denied"):
            await board.reassign(
                task["id"],
                "peer",
                task["revision"],
                expected_ownership_version=task["ownership_version"],
                new_owner_id=board.member("coord"),
            )
        transferred = await board.reassign(
            task["id"],
            "coord",
            task["revision"],
            expected_ownership_version=task["ownership_version"],
            new_owner_id=board.member("peer"),
        )
        assert transferred["owner_id"] == board.member("peer")
        unowned = await board.create("无人领取")
        with pytest.raises(ServiceError, match="permission_denied"):
            await board.reassign(
                unowned["id"],
                "worker",
                unowned["revision"],
                new_owner_id=board.member("worker"),
            )
        assigned = await board.reassign(
            unowned["id"], "root", unowned["revision"], new_owner_id=board.member("worker")
        )
        assert assigned["status"] == "claimed"
        assert assigned["owner_id"] == board.member("worker")
    finally:
        await store.close()


async def test_reassign_validates_target_member_and_versions(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t22")
        other = await seed_tree(store, "t22-other")
        board = Board(store, ids)
        task = await board.started("目标校验", caller="w1")
        with pytest.raises(ServiceError, match="invalid_input"):
            await board.reassign(
                task["id"], "root", task["revision"], new_owner_id=board.member("w2")
            )
        with pytest.raises(ServiceError, match="revision_conflict"):
            await board.reassign(
                task["id"],
                "root",
                task["revision"],
                expected_ownership_version=task["ownership_version"] + 1,
                new_owner_id=board.member("w2"),
            )
        with pytest.raises(ServiceError, match="not_found"):
            await board.reassign(
                task["id"],
                "root",
                task["revision"],
                expected_ownership_version=task["ownership_version"],
                new_owner_id=str(uuid.uuid4()),
            )
        cross = Board(store, other)
        with pytest.raises(ServiceError, match="invalid_input"):
            await board.reassign(
                task["id"],
                "root",
                task["revision"],
                expected_ownership_version=task["ownership_version"],
                new_owner_id=cross.member("w1"),
            )
        with pytest.raises(ServiceError, match="invalid_input"):
            await board.reassign(
                task["id"],
                "root",
                task["revision"],
                expected_ownership_version=task["ownership_version"],
                new_owner_id=board.member("w1"),
            )
        await board.close_member("w2")
        with pytest.raises(ServiceError, match="closed"):
            await board.reassign(
                task["id"],
                "root",
                task["revision"],
                expected_ownership_version=task["ownership_version"],
                new_owner_id=board.member("w2"),
            )
    finally:
        await store.close()


async def test_reassign_back_to_the_board_reopens_the_task(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t23")
        board = Board(store, ids)
        task = await board.started("退回任务板", caller="w1")
        released = await board.reassign(
            task["id"],
            "root",
            task["revision"],
            expected_ownership_version=task["ownership_version"],
            new_owner_id=None,
        )
        assert released["status"] == "open"
        assert released["owner_id"] is None
        with pytest.raises(ServiceError, match="invalid_input"):
            await board.reassign(
                task["id"], "root", released["revision"], new_owner_id=None
            )
        with pytest.raises(ServiceError, match="permission_denied"):
            await board.update(
                task["id"],
                "w1",
                released["revision"],
                expected_ownership_version=released["ownership_version"],
                status="completed",
                evidence=(check("old-owner"),),
            )
        reclaimed = await board.claim(task["id"], "w2", released["revision"])
        assert reclaimed["owner_id"] == board.member("w2")
    finally:
        await store.close()


async def test_terminal_tasks_accept_only_coordinator_evidence(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t24")
        board = Board(store, ids)
        task = await board.started("终态封板", caller="w1")
        completed = await board.update(
            task["id"],
            "w1",
            task["revision"],
            expected_ownership_version=task["ownership_version"],
            status="completed",
            evidence=(artifact("final"),),
        )
        with pytest.raises(ServiceError, match="subagent_task_closed"):
            await board.update(
                task["id"],
                "w1",
                completed["revision"],
                expected_ownership_version=completed["ownership_version"],
                evidence=(artifact("late-worker"),),
            )
        with pytest.raises(ServiceError, match="subagent_task_closed"):
            await board.update(task["id"], "root", completed["revision"], goal="重新规划")
        with pytest.raises(ServiceError, match="subagent_task_closed"):
            await board.reassign(
                task["id"],
                "root",
                completed["revision"],
                expected_ownership_version=completed["ownership_version"],
                new_owner_id=board.member("w2"),
            )
        accepted = await board.update(
            task["id"],
            "root",
            completed["revision"],
            expected_ownership_version=completed["ownership_version"],
            evidence=(check("acceptance"),),
        )
        assert accepted["status"] == "completed"
        assert accepted["revision"] == completed["revision"] + 1
        assert len(accepted["evidence"]) == 2
    finally:
        await store.close()


async def test_list_filters_and_orders_the_board(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t25")
        board = Board(store, ids)
        first = await board.create("一")
        second = await board.create("二", deps=(first["id"],))
        third = await board.create("三")
        claimed = await board.claim(first["id"], "w1", first["revision"])
        everything = await board.list()
        assert [row["ordinal"] for row in everything] == [1, 2, 3]
        assert everything[1]["dependencies"] == [
            {"task_id": first["id"], "status": claimed["status"]}
        ]
        open_tasks = await board.list(status="open")
        assert {row["id"] for row in open_tasks} == {second["id"], third["id"]}
        mine = await board.list(owner="w1")
        assert [row["id"] for row in mine] == [first["id"]]
        assert mine[0]["owner_id"] == board.member("w1")
    finally:
        await store.close()


async def test_closing_the_tree_freezes_mutations_but_keeps_reads(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t26")
        board = Board(store, ids)
        task = await board.create("树要关了")
        await board.close_tree()
        with pytest.raises(ServiceError, match="subagent_tree_closed"):
            await board.claim(task["id"], "w1", task["revision"])
        with pytest.raises(ServiceError, match="subagent_tree_closed"):
            await board.reassign(
                task["id"], "root", task["revision"], new_owner_id=board.member("w1")
            )
        rows = await board.list()
        assert [row["id"] for row in rows] == [task["id"]]
    finally:
        await store.close()


async def test_source_references_must_be_real_and_same_tree(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t27")
        other = await seed_tree(store, "t27-other")
        board = Board(store, ids)
        run, _ = await store.begin(
            RunRequest(request_key="team-source-run", text="委派执行", target="demo"),
            {"key": "demo"},
        )
        linked = await board.create("带来源", source_run_id=run.id)
        assert linked["source_run_id"] == run.id
        with pytest.raises(ServiceError, match="invalid_input"):
            await board.create("来源run不存在", source_run_id=str(uuid.uuid4()))
        with pytest.raises(ServiceError, match="invalid_input"):
            await board.create("来源任务不存在", source_task_id=str(uuid.uuid4()))
        foreign = await seed_delegation_task(store, other, "w1")
        with pytest.raises(ServiceError, match="invalid_input"):
            await board.create("跨树来源", source_task_id=foreign)
    finally:
        await store.close()


async def test_confirm_probes_refuse_mismatched_commit_outcomes(tmp_path: Path) -> None:
    store = await SQLiteStore.open(tmp_path / "team.sqlite")
    try:
        ids = await seed_tree(store, "t28")
        board = Board(store, ids)
        request = TeamTaskCreate(
            tree_id=ids["tree_id"],
            caller_id=board.member("root"),
            goal="核对回执",
            dependencies=(),
            request_key="board:confirm",
        )
        digest = fingerprint(
            {
                "goal": request.goal,
                "dependencies": list(request.dependencies),
                "source_task_id": request.source_task_id,
                "source_run_id": request.source_run_id,
            }
        )
        assert await _confirm_create(store, request, digest) is None
        created = await board.service.create(request)
        found = await _confirm_create(store, request, digest)
        assert found is not None
        assert found["id"] == created["id"]
        assert await _confirm_create(store, request, "0" * 64) is None

        claim_request = TeamTaskClaim(
            tree_id=ids["tree_id"],
            task_id=created["id"],
            caller_id=board.member("w1"),
            expected_revision=created["revision"],
        )
        assert await _confirm_claim(store, claim_request) is None
        claimed = await board.claim(created["id"], "w1", created["revision"])
        confirmed = await _confirm_claim(store, claim_request)
        assert confirmed is not None
        assert confirmed["owner_id"] == board.member("w1")
        stale = claim_request.model_copy(update={"expected_revision": created["revision"] + 9})
        assert await _confirm_claim(store, stale) is None

        update_request = TeamTaskUpdate(
            tree_id=ids["tree_id"],
            task_id=created["id"],
            caller_id=board.member("w1"),
            expected_revision=claimed["revision"],
            expected_ownership_version=claimed["ownership_version"],
            status="in_progress",
        )
        assert (
            await _confirm_update(
                store, update_request, {"revision": 99, "status": "in_progress"}
            )
            is None
        )
        started = await board.update(
            created["id"],
            "w1",
            claimed["revision"],
            expected_ownership_version=claimed["ownership_version"],
            status="in_progress",
        )
        confirmed_update = await _confirm_update(
            store, update_request, {"revision": started["revision"], "status": "in_progress"}
        )
        assert confirmed_update is not None
        assert confirmed_update["status"] == "in_progress"

        reassign_request = TeamTaskReassign(
            tree_id=ids["tree_id"],
            task_id=created["id"],
            caller_id=board.member("root"),
            expected_revision=started["revision"],
            expected_ownership_version=started["ownership_version"],
            new_owner_id=board.member("w2"),
        )
        assert await _confirm_reassign(store, reassign_request) is None
        transferred = await board.reassign(
            created["id"],
            "root",
            started["revision"],
            expected_ownership_version=started["ownership_version"],
            new_owner_id=board.member("w2"),
        )
        confirmed_reassign = await _confirm_reassign(store, reassign_request)
        assert confirmed_reassign is not None
        assert confirmed_reassign["owner_id"] == board.member("w2")
        assert transferred["revision"] == started["revision"] + 1
    finally:
        await store.close()
