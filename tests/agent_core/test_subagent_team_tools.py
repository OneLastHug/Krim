"""Team 工具层验收：固定名、宿主 Caller 绑定、键派生与错误映射。

工具下的 TeamService 与 SQLiteStore 全真实；仅宿主 Caller/_target 解析边界
按 service.py 的契约用桩替换（真实实现属运行服务整合范围）。
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import ValidationError
from sqlalchemy import select
from test_subagent_team import seed_tree

from krim_agent.contracts import ServiceError
from krim_agent.security import Membership, PrincipalContext
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.storage.subagent_models import subagent_members
from krim_agent.subagents.service import Caller
from krim_agent.subagents.team import TeamService
from krim_agent.subagents.team_tools import (
    TeamClaimTask,
    TeamCreateTask,
    TeamEvidenceItem,
    TeamListTasks,
    TeamReassignTask,
    TeamUpdateTask,
    _invocation_key,
    _unavailable,
    bundle,
)
from krim_agent.tools.core import Invocation, ToolSpec
from krim_agent.tools.session import SESSION_CAPABILITIES, SessionToolScope, ToolBundle

TEAM_TOOL_NAMES = frozenset(
    {
        "team_create_task",
        "team_list_tasks",
        "team_claim_task",
        "team_update_task",
        "team_reassign_task",
    }
)

ACTOR = PrincipalContext("local", 1, (Membership(workspace_id="local", role="admin"),))


class StubSubagents:
    """按 service.py 契约模拟宿主解析边界：_caller 恒返回同一 Caller。"""

    def __init__(self, store: SQLiteStore, caller: Caller) -> None:
        self.store = store
        self.caller = caller

    async def _caller(self, invocation: Invocation) -> Caller:
        return self.caller

    async def _target(self, caller: Caller, target: str) -> dict[str, Any]:
        async def read(conn: Any) -> dict[str, Any] | None:
            row = (
                (
                    await conn.execute(
                        select(subagent_members).where(
                            subagent_members.c.tree_id == caller.tree["tree_id"],
                            (subagent_members.c.id == target)
                            | (subagent_members.c.path == target),
                        )
                    )
                )
                .mappings()
                .first()
            )
            return dict(row) if row is not None else None

        found = await self.store.work_read(read)
        if found is None:
            raise ServiceError("not_found", 404)
        # repository.get_agent 的成员视图形状：工具取 target["agent_id"]。
        return {**found, "agent_id": found["id"]}


class FailingTeam:
    def __init__(self, error: Exception) -> None:
        self.error = error

    async def create(self, request: Any) -> dict[str, Any]:
        raise self.error


def invocation(name: str, *, run_id: str = "run-1", call_id: str = "call-1") -> Invocation:
    return Invocation(
        run_id=run_id,
        call_id=call_id,
        span_id="span",
        step_index=0,
        name=name,
        arguments_json="{}",
        tool_hash="tool-hash",
    )


def resolver(ids: dict[str, Any]):
    async def resolve(target: Invocation) -> SessionToolScope:
        return SessionToolScope(
            actor=ACTOR,
            workspace_id="local",
            session_id=ids["session_id"],
            turn_id="turn-1",
            run_id=target.run_id,
        )

    return resolve


async def tooling(tmp_path: Path, name: str = "team-tools.sqlite", caller: str = "root"):
    store = await SQLiteStore.open(tmp_path / name)
    ids = await seed_tree(store, "tt")
    host = Caller(
        member={"agent_id": ids["ids"][caller]},
        tree={"tree_id": ids["tree_id"]},
        scope=None,
    )
    subagents = StubSubagents(store, host)
    board = SimpleNamespace(
        store=store,
        ids=ids,
        team=TeamService(store),
        subagents=subagents,
        bundle=bundle(TeamService(store), subagents, resolver(ids)),
        host=host,
    )
    return board


async def call(board: Any, name: str, parsed: Any, **kwargs: Any) -> dict[str, Any]:
    spec = next(spec for spec in board.bundle.specs if spec.name == name)
    assert spec.scoped_executor is not None
    return await spec.scoped_executor(invocation(name, **kwargs), parsed, None)


async def test_five_tools_are_session_read_capabilities(tmp_path: Path) -> None:
    board = await tooling(tmp_path)
    try:
        assert TEAM_TOOL_NAMES <= set(SESSION_CAPABILITIES)
        assert all(SESSION_CAPABILITIES[name] == "session_read" for name in TEAM_TOOL_NAMES)
        assert isinstance(board.bundle, ToolBundle)
        assert {spec.name for spec in board.bundle.specs} == TEAM_TOOL_NAMES
        for spec in board.bundle.specs:
            assert isinstance(spec, ToolSpec)
            assert spec.capability == "session_read"
            assert spec.risk == "read"
            assert spec.version == "team/1"
            assert spec.concurrency == "parallel"
            assert spec.scoped_executor is not None
            assert spec.implementation_digest
    finally:
        await board.store.close()


async def test_implementation_digest_covers_service_and_model_files(tmp_path: Path) -> None:
    board = await tooling(tmp_path)
    try:
        root = Path("src/krim_agent/subagents")
        digest = hashlib.sha256()
        for path in (
            root / "team.py",
            root / "team_tools.py",
            root.parent / "storage" / "subagent_team_models.py",
            root / "types.py",
        ):
            digest.update(path.read_bytes())
        expected = digest.hexdigest()
        for spec in board.bundle.specs:
            assert spec.implementation_digest == expected
    finally:
        await board.store.close()


def test_model_inputs_carry_no_authority_fields() -> None:
    with pytest.raises(ValidationError):
        TeamCreateTask(goal="x", tree_id="tree", caller_id="agent", request_key="k")
    with pytest.raises(ValidationError):
        TeamUpdateTask(task_id="t", expected_revision=1, caller_id="agent")
    with pytest.raises(ValidationError):
        TeamReassignTask(task_id="t", expected_revision=1, tree_id="tree")
    with pytest.raises(ValidationError):
        TeamListTasks(owner="me", caller_id="agent")
    # 领取走 team_claim_task；update 不接受 claimed/blocked 作为目标状态。
    with pytest.raises(ValidationError):
        TeamUpdateTask(task_id="t", expected_revision=1, status="claimed")
    with pytest.raises(ValidationError):
        TeamUpdateTask(task_id="t", expected_revision=1, status="blocked")


async def test_create_replays_share_the_invocation_key(tmp_path: Path) -> None:
    board = await tooling(tmp_path)
    try:
        first = await call(board, "team_create_task", TeamCreateTask(goal="同一目标"))
        replay = await call(board, "team_create_task", TeamCreateTask(goal="同一目标"))
        assert first["ok"] is True and replay["ok"] is True
        assert replay["id"] == first["id"]
        assert replay["revision"] == first["revision"]
        other = await call(
            board, "team_create_task", TeamCreateTask(goal="同一目标"), call_id="call-2"
        )
        assert other["id"] != first["id"]
        key = _invocation_key(invocation("team_create_task"), "create")
        assert key.startswith("team:") and len(key) <= 128
    finally:
        await board.store.close()


async def test_claim_and_update_flow_return_board_views(tmp_path: Path) -> None:
    board = await tooling(tmp_path)
    try:
        created = await call(board, "team_create_task", TeamCreateTask(goal="走全流程"))
        claimed = await call(
            board,
            "team_claim_task",
            TeamClaimTask(task_id=created["id"], expected_revision=created["revision"]),
        )
        assert claimed["ok"] is True
        assert claimed["status"] == "claimed"
        started = await call(
            board,
            "team_update_task",
            TeamUpdateTask(
                task_id=created["id"],
                expected_revision=claimed["revision"],
                expected_ownership_version=claimed["ownership_version"],
                status="in_progress",
            ),
        )
        assert started["status"] == "in_progress"
        completed = await call(
            board,
            "team_update_task",
            TeamUpdateTask(
                task_id=created["id"],
                expected_revision=started["revision"],
                expected_ownership_version=started["ownership_version"],
                status="completed",
                evidence=(
                    TeamEvidenceItem(kind="artifact", body={"path": "out.md"}),
                ),
            ),
        )
        assert completed["ok"] is True
        assert completed["status"] == "completed"
        assert completed["completed_at"] is not None
        assert completed["evidence"][0]["request_key"] == _invocation_key(
            invocation("team_update_task"), "evidence", 0
        )
    finally:
        await board.store.close()


async def test_cas_and_dependency_errors_return_actionable_payloads(tmp_path: Path) -> None:
    board = await tooling(tmp_path)
    try:
        created = await call(board, "team_create_task", TeamCreateTask(goal="竞态目标"))
        stale = await call(
            board,
            "team_claim_task",
            TeamClaimTask(task_id=created["id"], expected_revision=created["revision"]),
        )
        assert stale["ok"] is True
        late = await call(
            board,
            "team_claim_task",
            TeamClaimTask(task_id=created["id"], expected_revision=created["revision"]),
        )
        assert late["ok"] is False
        assert late["code"] == "revision_conflict"
        # accepted 恒在：CAS 冲突没有接受语义，null 表示未知。
        assert late["accepted"] is None
        assert "reload" in late["action"]
        first = await call(
            board, "team_create_task", TeamCreateTask(goal="前置"), call_id="call-3"
        )
        gated = await call(
            board,
            "team_create_task",
            TeamCreateTask(goal="等依赖", dependencies=(first["id"],)),
            call_id="call-4",
        )
        assert gated["ok"] is True
        attempt = await call(
            board,
            "team_claim_task",
            TeamClaimTask(task_id=gated["id"], expected_revision=gated["revision"]),
            call_id="call-5",
        )
        assert attempt["ok"] is False
        assert attempt["code"] == "invalid_input"
        assert attempt["action"] == "wait_dependencies"
    finally:
        await board.store.close()


async def test_storage_unavailable_and_cancelled_propagate(tmp_path: Path) -> None:
    board = await tooling(tmp_path)
    try:
        for code, status in (("storage_unavailable", 503), ("cancelled", 409)):
            failing = SimpleNamespace(
                bundle=bundle(
                    FailingTeam(ServiceError(code, status)),
                    board.subagents,
                    resolver(board.ids),
                )
            )
            with pytest.raises(ServiceError, match=code):
                await call(failing, "team_create_task", TeamCreateTask(goal="传播"))
    finally:
        await board.store.close()


async def test_completion_without_evidence_is_refused_with_action(tmp_path: Path) -> None:
    board = await tooling(tmp_path)
    try:
        created = await call(board, "team_create_task", TeamCreateTask(goal="证据门控"))
        claimed = await call(
            board,
            "team_claim_task",
            TeamClaimTask(task_id=created["id"], expected_revision=created["revision"]),
        )
        lacking = await call(
            board,
            "team_update_task",
            TeamUpdateTask(
                task_id=created["id"],
                expected_revision=claimed["revision"],
                expected_ownership_version=claimed["ownership_version"],
                status="completed",
            ),
        )
        assert lacking["ok"] is False
        assert lacking["code"] == "invalid_input"
        assert "artifact" in lacking["action"]
    finally:
        await board.store.close()


async def test_evidence_request_keys_derive_stably_per_index(tmp_path: Path) -> None:
    board = await tooling(tmp_path)
    try:
        created = await call(board, "team_create_task", TeamCreateTask(goal="证据重放"))
        claimed = await call(
            board,
            "team_claim_task",
            TeamClaimTask(task_id=created["id"], expected_revision=created["revision"]),
        )
        items = (
            TeamEvidenceItem(kind="artifact", body={"path": "a.md"}),
            TeamEvidenceItem(kind="check", body={"passed": True}),
        )
        first = await call(
            board,
            "team_update_task",
            TeamUpdateTask(
                task_id=created["id"],
                expected_revision=claimed["revision"],
                expected_ownership_version=claimed["ownership_version"],
                evidence=items,
            ),
        )
        assert first["ok"] is True
        assert len(first["evidence"]) == 2
        replay = await call(
            board,
            "team_update_task",
            TeamUpdateTask(
                task_id=created["id"],
                expected_revision=first["revision"],
                expected_ownership_version=first["ownership_version"],
                evidence=items,
            ),
        )
        assert replay["ok"] is True
        assert len(replay["evidence"]) == 2
        keyed = await call(
            board,
            "team_update_task",
            TeamUpdateTask(
                task_id=created["id"],
                expected_revision=replay["revision"],
                expected_ownership_version=replay["ownership_version"],
                evidence=(
                    TeamEvidenceItem(
                        kind="check", body={"passed": True}, request_key="ev:explicit"
                    ),
                ),
            ),
        )
        assert keyed["evidence"][-1]["request_key"] == "ev:explicit"
    finally:
        await board.store.close()


async def test_reassign_resolves_target_and_enforces_authority(tmp_path: Path) -> None:
    board = await tooling(tmp_path)
    try:
        created = await call(board, "team_create_task", TeamCreateTask(goal="转交目标"))
        worker_bundle = SimpleNamespace(
            store=board.store,
            ids=board.ids,
            subagents=StubSubagents(
                board.store,
                Caller(
                    member={"agent_id": board.ids["ids"]["w1"]},
                    tree={"tree_id": board.ids["tree_id"]},
                    scope=None,
                ),
            ),
            bundle=bundle(
                TeamService(board.store),
                StubSubagents(
                    board.store,
                    Caller(
                        member={"agent_id": board.ids["ids"]["w1"]},
                        tree={"tree_id": board.ids["tree_id"]},
                        scope=None,
                    ),
                ),
                resolver(board.ids),
            ),
        )
        claimed = await call(
            worker_bundle,
            "team_claim_task",
            TeamClaimTask(task_id=created["id"], expected_revision=created["revision"]),
        )
        assert claimed["ok"] is True
        denied = await call(
            worker_bundle,
            "team_reassign_task",
            TeamReassignTask(
                task_id=created["id"],
                expected_revision=claimed["revision"],
                expected_ownership_version=claimed["ownership_version"],
                target="root/w2",
            ),
        )
        assert denied["ok"] is False
        assert denied["code"] == "permission_denied"
        missing_ov = await call(
            board,
            "team_reassign_task",
            TeamReassignTask(
                task_id=created["id"], expected_revision=claimed["revision"], target="root/w2"
            ),
        )
        assert missing_ov["ok"] is False
        assert missing_ov["code"] == "invalid_input"
        unknown = await call(
            board,
            "team_reassign_task",
            TeamReassignTask(
                task_id=created["id"],
                expected_revision=claimed["revision"],
                expected_ownership_version=claimed["ownership_version"],
                target="ghost",
            ),
        )
        assert unknown["ok"] is False
        assert unknown["code"] == "not_found"
        transferred = await call(
            board,
            "team_reassign_task",
            TeamReassignTask(
                task_id=created["id"],
                expected_revision=claimed["revision"],
                expected_ownership_version=claimed["ownership_version"],
                target="root/w2",
            ),
        )
        assert transferred["ok"] is True
        assert transferred["owner_id"] == board.ids["ids"]["w2"]
        assert transferred["ownership_version"] == claimed["ownership_version"] + 1
    finally:
        await board.store.close()


async def test_list_supports_me_and_status_filters(tmp_path: Path) -> None:
    board = await tooling(tmp_path)
    try:
        await call(board, "team_create_task", TeamCreateTask(goal="其一"))
        second = await call(
            board, "team_create_task", TeamCreateTask(goal="其二"), call_id="call-2"
        )
        worker = SimpleNamespace(
            store=board.store,
            ids=board.ids,
            subagents=StubSubagents(
                board.store,
                Caller(
                    member={"agent_id": board.ids["ids"]["w1"]},
                    tree={"tree_id": board.ids["tree_id"]},
                    scope=None,
                ),
            ),
            bundle=bundle(
                TeamService(board.store),
                StubSubagents(
                    board.store,
                    Caller(
                        member={"agent_id": board.ids["ids"]["w1"]},
                        tree={"tree_id": board.ids["tree_id"]},
                        scope=None,
                    ),
                ),
                resolver(board.ids),
            ),
        )
        claimed = await call(
            worker,
            "team_claim_task",
            TeamClaimTask(task_id=second["id"], expected_revision=second["revision"]),
        )
        assert claimed["ok"] is True
        mine = await call(worker, "team_list_tasks", TeamListTasks(owner="me"))
        assert mine["ok"] is True
        assert len(mine["tasks"]) == 1
        assert mine["tasks"][0]["id"] == second["id"]
        open_tasks = await call(board, "team_list_tasks", TeamListTasks(status="open"))
        assert len(open_tasks["tasks"]) == 1
    finally:
        await board.store.close()


async def test_tree_closure_returns_ok_false_payload(tmp_path: Path) -> None:
    board = await tooling(tmp_path)
    try:
        from sqlalchemy import update as sql_update
        from test_subagent_team import write

        from krim_agent.storage.subagent_models import subagent_trees

        async def close(conn: Any) -> None:
            await conn.execute(
                sql_update(subagent_trees)
                .where(subagent_trees.c.id == board.ids["tree_id"])
                .values(status="closed")
            )

        await write(board.store, close)
        refused = await call(board, "team_create_task", TeamCreateTask(goal="关树后创建"))
        assert refused["ok"] is False
        assert refused["code"] == "subagent_tree_closed"
    finally:
        await board.store.close()


async def test_policy_admits_known_names_and_denies_the_rest(tmp_path: Path) -> None:
    board = await tooling(tmp_path)
    try:
        policy = board.bundle.policy
        assert await policy(invocation("team_create_task")) == "allow"
        assert await policy(invocation("plan_read")) == "deny"
        assert await policy(invocation("team_claim_task", run_id="run-1")) == "allow"

        async def foreign(target: Invocation) -> SessionToolScope:
            return SessionToolScope(
                actor=ACTOR,
                workspace_id="local",
                session_id=board.ids["session_id"],
                turn_id="turn-1",
                run_id="other-run",
            )

        guarded = bundle(board.team, board.subagents, foreign)
        assert await guarded.policy(invocation("team_claim_task")) == "deny"
    finally:
        await board.store.close()


async def test_unavailable_executor_is_a_placeholder(tmp_path: Path) -> None:
    with pytest.raises(ServiceError, match="workspace_contract_required"):
        await _unavailable(TeamCreateTask(goal="x"))


async def test_invocation_keys_differ_by_call_and_part(tmp_path: Path) -> None:
    base = invocation("team_update_task")
    assert _invocation_key(base, "create") != _invocation_key(base, "evidence", 0)
    assert _invocation_key(base, "evidence", 0) != _invocation_key(base, "evidence", 1)
    assert _invocation_key(base, "evidence", 0) == _invocation_key(
        invocation("team_update_task"), "evidence", 0
    )
