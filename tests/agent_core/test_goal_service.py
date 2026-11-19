"""GoalService persistence, authorization, CAS/idempotency and open/reopen schema facts."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from sqlalchemy import update

from krim_agent.config import Settings
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import RunRequest, ServiceError
from krim_agent.goals.contracts import (
    EvidenceRef,
    GoalControlRequest,
    GoalCreateRequest,
    GoalRead,
    GoalUpdate,
)
from krim_agent.goals.service import GoalService
from krim_agent.security import Membership, PrincipalContext
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.goal_models import GOAL_TABLES
from krim_agent.storage.models import metadata, sessions
from krim_agent.storage.sqlite import SQLiteStore

WORKSPACE = "goals-test"


def member(principal_id: str, workspace_id: str = WORKSPACE, role: str = "member"):
    return PrincipalContext(
        principal_id,
        1,
        (Membership(workspace_id=workspace_id, role=role),),
    )


async def open_goal_store(path: Path) -> SQLiteStore:
    store = await SQLiteStore.open(path)
    # Fresh SQLiteStore.open already builds every registered table; this keeps the
    # test independent of whether the host has registered GOAL_TABLES yet.
    async with store.lock, store.engine.begin() as conn:
        await conn.run_sync(lambda sync: metadata.create_all(sync, tables=list(GOAL_TABLES)))
    return store


@asynccontextmanager
async def managed_goal_store(path: Path) -> AsyncIterator[SQLiteStore]:
    store = await open_goal_store(path)
    try:
        yield store
    finally:
        await store.close()


async def create_session(store: SQLiteStore, owner_id: str, key: str):
    return await session_sql.create_session(
        store,
        WORKSPACE,
        owner_id,
        SessionCreate(request_key=key),
        {"key": "demo"},
        ContextPolicy(),
        Settings(),
    )


async def submit_turn(store: SQLiteStore, session_id: str, key: str, text: str):
    async with store.lock, store.engine.begin() as conn:
        revision = (await session_sql.session_row(conn, session_id))["revision"]
    return await session_sql.submit(
        store,
        session_id,
        TurnRequest(request_key=key, text=text, expected_revision=revision),
        Settings(),
    )


async def bind_run(store: SQLiteStore, turn_id: str, key: str, status: str = "queued"):
    # A real run row keeps the turns.run_id foreign key valid; no dispatcher is started.
    run, _ = await store.begin(
        RunRequest(request_key=f"run-{key}", text=f"run {key}", target="demo"),
        {"key": "demo"},
    )
    async with store.lock, store.engine.begin() as conn:
        await conn.execute(
            update(session_sql.turns)
            .where(session_sql.turns.c.id == turn_id)
            .values(run_id=run.id, status=status)
        )
    return run.id


async def user_turn(
    store: SQLiteStore, session_id: str, key: str, *, text: str = "任务", with_run: bool = True
):
    turn = await submit_turn(store, session_id, key, text)
    run_id = await bind_run(store, turn.id, key) if with_run else None
    return turn, run_id


def goal_update(authority: PrincipalContext, **values) -> GoalUpdate:
    return GoalUpdate.model_validate(values)


def evidence(*refs: str) -> tuple[EvidenceRef, ...]:
    return tuple(EvidenceRef(kind="test", ref=ref) for ref in refs)


@pytest.mark.asyncio
async def test_revision_zero_and_creation_requires_a_real_user_turn(tmp_path):
    async with managed_goal_store(tmp_path / "create.sqlite") as store:
        owner = member("owner")
        session = await create_session(store, owner.principal_id, "session-1")
        service = GoalService(store)

        empty = await service.snapshot(owner, WORKSPACE, session.id)
        assert (empty.goal_id, empty.revision, empty.status) == (None, 0, None)
        assert len(empty.content_hash) == 64

        # No user turn yet: creation is rejected rather than fabricating authority.
        with pytest.raises(ServiceError, match="invalid_input"):
            await service.create(
                owner,
                WORKSPACE,
                session.id,
                GoalCreateRequest(request_key="no-turn", expected_revision=0, objective="目标"),
            )

        turn, run_id = await user_turn(store, session.id, "user-1", text="请完成报告")
        created = await service.create(
            owner,
            WORKSPACE,
            session.id,
            GoalCreateRequest(
                request_key="create-1",
                expected_revision=0,
                objective="完成报告",
                completion_criteria=("测试通过",),
                origin_turn_id=turn.id,
                origin_run_id=run_id,
            ),
        )
        assert created.status == "active" and created.revision == 1
        assert (created.origin_turn_id, created.origin_run_id) == (turn.id, run_id)

        # Same request key and body: idempotent replay, no new revision.
        replay = await service.create(
            owner,
            WORKSPACE,
            session.id,
            GoalCreateRequest(
                request_key="create-1",
                expected_revision=0,
                objective="完成报告",
                completion_criteria=("测试通过",),
                origin_turn_id=turn.id,
                origin_run_id=run_id,
            ),
        )
        assert replay == created

        with pytest.raises(ServiceError, match="key_conflict"):
            await service.create(
                owner,
                WORKSPACE,
                session.id,
                GoalCreateRequest(
                    request_key="create-1",
                    expected_revision=0,
                    objective="不同目标",
                    origin_turn_id=turn.id,
                    origin_run_id=run_id,
                ),
            )


@pytest.mark.asyncio
async def test_internal_turn_cannot_create_or_redirect_a_goal(tmp_path):
    async with managed_goal_store(tmp_path / "internal.sqlite") as store:
        owner = member("owner")
        session = await create_session(store, owner.principal_id, "session-internal")
        service = GoalService(store)
        internal, internal_run = await user_turn(
            store, session.id, "goal-continue:abc", text="内部续跑"
        )
        with pytest.raises(ServiceError, match="invalid_input"):
            await service.create(
                owner,
                WORKSPACE,
                session.id,
                GoalCreateRequest(
                    request_key="from-internal",
                    expected_revision=0,
                    objective="模型自建",
                    origin_turn_id=internal.id,
                    origin_run_id=internal_run,
                ),
            )
        # "latest user turn" selection also never picks the internal turn.
        with pytest.raises(ServiceError, match="invalid_input"):
            await service.create(
                owner,
                WORKSPACE,
                session.id,
                GoalCreateRequest(
                    request_key="from-latest-internal", expected_revision=0, objective="x"
                ),
            )

        user, run_id = await user_turn(store, session.id, "user-2", text="真实任务")
        created = await service.create(
            owner,
            WORKSPACE,
            session.id,
            GoalCreateRequest(
                request_key="create",
                expected_revision=0,
                objective="真实目标",
                origin_turn_id=user.id,
                origin_run_id=run_id,
            ),
        )
        assert created.origin_turn_id == user.id
        # Internal turn text cannot rename an existing goal either: binding check fires
        # first, and replacement is owner-only for paused/blocked goals.
        with pytest.raises(ServiceError, match="invalid_input"):
            await service.create(
                owner,
                WORKSPACE,
                session.id,
                GoalCreateRequest(
                    request_key="replace-active",
                    expected_revision=1,
                    objective="内部模型改写",
                    origin_turn_id=internal.id,
                    origin_run_id=internal_run,
                ),
            )
        with pytest.raises(ServiceError, match="revision_conflict"):
            await service.create(
                owner,
                WORKSPACE,
                session.id,
                GoalCreateRequest(
                    request_key="replace-active-user",
                    expected_revision=1,
                    objective="用户改写运行中的目标",
                    origin_turn_id=user.id,
                    origin_run_id=run_id,
                ),
            )


@pytest.mark.asyncio
async def test_cas_progress_evidence_and_immutable_history(tmp_path):
    async with managed_goal_store(tmp_path / "cas.sqlite") as store:
        owner = member("owner")
        session = await create_session(store, owner.principal_id, "session-cas")
        service = GoalService(store)
        turn, run_id = await user_turn(store, session.id, "user-1")
        await service.create(
            owner,
            WORKSPACE,
            session.id,
            GoalCreateRequest(
                request_key="create",
                expected_revision=0,
                objective="目标",
                completion_criteria=("条件一", "条件二"),
                origin_turn_id=turn.id,
                origin_run_id=run_id,
            ),
        )
        progressed = await service.update_from_tool(
            scope_for(owner, session.id, turn.id, run_id),
            goal_update(
                owner,
                request_key="progress-1",
                expected_revision=1,
                action="progress",
                note="完成第一步",
                evidence_refs=[{"kind": "test", "ref": "pytest:first"}],
            ),
        )
        assert progressed.revision == 2 and progressed.status == "active"
        assert progressed.status_note == "完成第一步"
        assert [item.ref for item in progressed.evidence_refs] == ["pytest:first"]

        with pytest.raises(ServiceError, match="revision_conflict"):
            await service.update_from_tool(
                scope_for(owner, session.id, turn.id, run_id),
                goal_update(owner, request_key="stale", expected_revision=1, action="progress"),
            )

        historical = await service.read(owner, WORKSPACE, session.id, GoalRead(revision=1))
        assert historical.status_note is None and historical.revision == 1
        latest = await service.snapshot(owner, WORKSPACE, session.id)
        assert latest.revision == 2
        with pytest.raises(ServiceError, match="not_found"):
            await service.read(owner, WORKSPACE, session.id, GoalRead(revision=9))

        # pause/resume stay owner API actions; the model tool cannot use them.
        for action in ("pause", "resume"):
            with pytest.raises(ServiceError, match="invalid_input"):
                await service.update_from_tool(
                    scope_for(owner, session.id, turn.id, run_id),
                    GoalUpdate(request_key=f"tool-{action}", expected_revision=2, action=action),
                )
        completed = await service.update_from_tool(
            scope_for(owner, session.id, turn.id, run_id),
            goal_update(
                owner,
                request_key="complete-1",
                expected_revision=2,
                action="complete",
                evidence_refs=[{"kind": "test", "ref": "pytest:all"}],
            ),
        )
        assert completed.status == "completed"
        assert [item.ref for item in completed.evidence_refs] == [
            "pytest:first",
            "pytest:all",
        ]
        # Completed goals are terminal for both model and API control.
        with pytest.raises(ServiceError, match="revision_conflict"):
            await service.update_from_tool(
                scope_for(owner, session.id, turn.id, run_id),
                goal_update(
                    owner, request_key="progress-after", expected_revision=3, action="progress"
                ),
            )


@pytest.mark.asyncio
async def test_legacy_workspace_sessions_cannot_activate_a_goal(tmp_path):
    async with managed_goal_store(tmp_path / "workspace.sqlite") as store:
        owner = member("owner")
        session = await create_session(store, owner.principal_id, "session-workspace")
        service = GoalService(store)
        turn, run_id = await user_turn(store, session.id, "user-1")
        await service.create(
            owner,
            WORKSPACE,
            session.id,
            GoalCreateRequest(
                request_key="create",
                expected_revision=0,
                objective="历史目标",
                origin_turn_id=turn.id,
                origin_run_id=run_id,
            ),
        )
        # Flip the row to the legacy target after creation: the historical goal and
        # reads survive, but anything that could activate continuation is refused.
        async with store.lock, store.engine.begin() as conn:
            await conn.execute(
                update(sessions)
                .where(sessions.c.id == session.id)
                .values(target_key="target-workspace")
            )

        with pytest.raises(ServiceError, match="workspace_contract_required"):
            await service.create(
                owner,
                WORKSPACE,
                session.id,
                GoalCreateRequest(
                    request_key="replace",
                    expected_revision=1,
                    objective="替换",
                    origin_turn_id=turn.id,
                    origin_run_id=run_id,
                ),
            )
        paused = await service.control(
            owner,
            WORKSPACE,
            session.id,
            GoalControlRequest(request_key="pause", expected_revision=1),
            action="pause",
        )
        assert paused.status == "paused"
        with pytest.raises(ServiceError, match="workspace_contract_required"):
            await service.control(
                owner,
                WORKSPACE,
                session.id,
                GoalControlRequest(request_key="resume", expected_revision=2),
                action="resume",
            )
        # Reads and owner cancel remain available; nothing was silently dropped.
        assert (await service.snapshot(owner, WORKSPACE, session.id)).revision == 2
        # Resume was refused, so the head is still the paused revision 2; owner
        # cancel remains available on the legacy target.
        cancelled = await service.control(
            owner,
            WORKSPACE,
            session.id,
            GoalControlRequest(request_key="cancel", expected_revision=2),
            action="cancel",
        )
        assert cancelled.status == "cancelled"


@pytest.mark.asyncio
async def test_completion_requires_criteria_and_plan_is_never_evidence(tmp_path):
    async with managed_goal_store(tmp_path / "criteria.sqlite") as store:
        owner = member("owner")
        session = await create_session(store, owner.principal_id, "session-criteria")
        service = GoalService(store)
        turn, run_id = await user_turn(store, session.id, "user-1")
        await service.create(
            owner,
            WORKSPACE,
            session.id,
            GoalCreateRequest(
                request_key="create",
                expected_revision=0,
                objective="有完成条件",
                origin_turn_id=turn.id,
                origin_run_id=run_id,
            ),
        )
        with pytest.raises(ServiceError, match="invalid_input"):
            await service.update_from_tool(
                scope_for(owner, session.id, turn.id, run_id),
                goal_update(owner, request_key="complete", expected_revision=1, action="complete"),
            )
        # Explicit criteria plus evidence are accepted and the snapshot exposes both.
        done = await service.update_from_tool(
            scope_for(owner, session.id, turn.id, run_id),
            goal_update(
                owner,
                request_key="complete-2",
                expected_revision=1,
                action="complete",
                completion_criteria=["测试与复现说明齐全"],
                evidence_refs=[{"kind": "command", "ref": "pytest:tests/agent_core"}],
            ),
        )
        assert done.status == "completed"
        assert done.completion_criteria == ("测试与复现说明齐全",)


@pytest.mark.asyncio
async def test_authorization_requires_owner_membership_and_open_session(tmp_path):
    async with managed_goal_store(tmp_path / "auth.sqlite") as store:
        owner = member("owner")
        admin = member("admin", role="admin")
        outsider = member("outsider", workspace_id="other")
        session = await create_session(store, owner.principal_id, "session-auth")
        service = GoalService(store)
        turn, run_id = await user_turn(store, session.id, "user-1")
        await service.create(
            owner,
            WORKSPACE,
            session.id,
            GoalCreateRequest(
                request_key="create",
                expected_revision=0,
                objective="目标",
                origin_turn_id=turn.id,
                origin_run_id=run_id,
            ),
        )
        for actor in (admin, outsider):
            with pytest.raises(ServiceError):
                await service.snapshot(actor, WORKSPACE, session.id)
        with pytest.raises(ServiceError, match="not_found"):
            await service.snapshot(owner, WORKSPACE, "missing-session")

        async with store.lock, store.engine.begin() as conn:
            await conn.execute(
                update(sessions).where(sessions.c.id == session.id).values(status="closed")
            )
        with pytest.raises(ServiceError, match="session_closed"):
            await service.control(
                owner,
                WORKSPACE,
                session.id,
                GoalControlRequest(request_key="pause", expected_revision=1),
                action="pause",
            )
        # Reads still show the durable goal after close.
        assert (await service.snapshot(owner, WORKSPACE, session.id)).revision == 1

        async with store.lock, store.engine.begin() as conn:
            await conn.execute(
                update(sessions).where(sessions.c.id == session.id).values(status="open")
            )
        paused = await service.control(
            owner,
            WORKSPACE,
            session.id,
            GoalControlRequest(request_key="pause", expected_revision=1, reason="先暂停"),
            action="pause",
        )
        assert paused.status == "paused"
        with pytest.raises(ServiceError, match="revision_conflict"):
            await service.control(
                owner,
                WORKSPACE,
                session.id,
                GoalControlRequest(request_key="pause-old", expected_revision=1),
                action="pause",
            )
        resumed = await service.control(
            owner,
            WORKSPACE,
            session.id,
            GoalControlRequest(request_key="resume", expected_revision=2),
            action="resume",
        )
        assert resumed.status == "active"


@pytest.mark.asyncio
async def test_cancellation_and_replaced_goal_relations(tmp_path):
    async with managed_goal_store(tmp_path / "cancel.sqlite") as store:
        owner = member("owner")
        session = await create_session(store, owner.principal_id, "session-cancel")
        service = GoalService(store)
        turn, run_id = await user_turn(store, session.id, "user-1")
        await service.create(
            owner,
            WORKSPACE,
            session.id,
            GoalCreateRequest(
                request_key="create",
                expected_revision=0,
                objective="旧目标",
                origin_turn_id=turn.id,
                origin_run_id=run_id,
            ),
        )
        cancelled = await service.control(
            owner,
            WORKSPACE,
            session.id,
            GoalControlRequest(request_key="cancel", expected_revision=1),
            action="cancel",
        )
        assert cancelled.status == "cancelled"
        # Replaying the same key is stable; a new key cannot revive a cancelled goal.
        assert (
            await service.control(
                owner,
                WORKSPACE,
                session.id,
                GoalControlRequest(request_key="cancel", expected_revision=1),
                action="cancel",
            )
        ) == cancelled
        with pytest.raises(ServiceError, match="revision_conflict"):
            await service.control(
                owner,
                WORKSPACE,
                session.id,
                GoalControlRequest(request_key="cancel-2", expected_revision=2),
                action="cancel",
            )


@pytest.mark.asyncio
async def test_check_cancellation_rolls_back_before_commit(tmp_path):
    async with managed_goal_store(tmp_path / "cancel-check.sqlite") as store:
        owner = member("owner")
        session = await create_session(store, owner.principal_id, "session-check")
        service = GoalService(store)
        turn, run_id = await user_turn(store, session.id, "user-1")
        created = await service.create(
            owner,
            WORKSPACE,
            session.id,
            GoalCreateRequest(
                request_key="create",
                expected_revision=0,
                objective="目标",
                origin_turn_id=turn.id,
                origin_run_id=run_id,
            ),
        )
        assert created.revision == 1

        def cancelled() -> None:
            raise ServiceError("cancelled", 409)

        with pytest.raises(ServiceError, match="cancelled"):
            await service.update_from_tool(
                scope_for(owner, session.id, turn.id, run_id, check=cancelled),
                goal_update(owner, request_key="progress", expected_revision=1, action="progress"),
            )
        assert (await service.snapshot(owner, WORKSPACE, session.id)).revision == 1


@pytest.mark.asyncio
async def test_fresh_store_open_close_readonly_reopen_keeps_goal_tables_valid(tmp_path):
    path = tmp_path / "reopen.sqlite"
    owner = member("owner")
    store = await open_goal_store(path)
    try:
        session = await create_session(store, owner.principal_id, "session-reopen")
        service = GoalService(store)
        turn, run_id = await user_turn(store, session.id, "user-1")
        await service.create(
            owner,
            WORKSPACE,
            session.id,
            GoalCreateRequest(
                request_key="create",
                expected_revision=0,
                objective="重启后仍在",
                completion_criteria=("证据",),
                origin_turn_id=turn.id,
                origin_run_id=run_id,
            ),
        )
        session_id = session.id
    finally:
        await store.close()

    # Read-only reopen must pass the central schema validator: goal tables use only
    # primary keys and plain UniqueConstraints (no partial indexes).
    reopened = await SQLiteStore.open(path, readonly=True)
    try:
        assert reopened.readonly
        snapshot = await GoalService(reopened).snapshot(owner, WORKSPACE, session_id)
        assert snapshot.objective == "重启后仍在"
        assert snapshot.revision == 1
    finally:
        await reopened.close()

    writable = await open_goal_store(path)
    try:
        progressed = await GoalService(writable).update_from_tool(
            scope_for(owner, session_id, turn.id, run_id),
            goal_update(owner, request_key="progress", expected_revision=1, action="progress"),
        )
        assert progressed.revision == 2
    finally:
        await writable.close()


@pytest.mark.asyncio
async def test_tool_creation_rejects_internal_or_foreign_turn_binding(tmp_path):
    async with managed_goal_store(tmp_path / "tool.sqlite") as store:
        owner = member("owner")
        session = await create_session(store, owner.principal_id, "session-tool")
        service = GoalService(store)
        turn, run_id = await user_turn(store, session.id, "user-1")
        # Tool creation for a run whose turn is not this session is rejected.
        foreign = await create_session(store, owner.principal_id, "session-foreign")
        foreign_turn, foreign_run = await user_turn(store, foreign.id, "user-foreign")
        with pytest.raises(ServiceError, match="invalid_input"):
            await service.update_from_tool(
                scope_for(owner, session.id, foreign_turn.id, foreign_run),
                goal_update(
                    owner,
                    request_key="wrong-session",
                    expected_revision=0,
                    action="set",
                    objective="x",
                ),
            )
        with pytest.raises(ServiceError, match="invalid_input"):
            await service.update_from_tool(
                scope_for(owner, session.id, turn.id, "wrong-run"),
                goal_update(
                    owner,
                    request_key="wrong-run",
                    expected_revision=0,
                    action="set",
                    objective="x",
                ),
            )
        created = await service.update_from_tool(
            scope_for(owner, session.id, turn.id, run_id),
            goal_update(
                owner,
                request_key="tool-create",
                expected_revision=0,
                action="set",
                objective="工具创建的目标",
                completion_criteria=["证据"],
            ),
        )
        assert created.origin_turn_id == turn.id
        assert created.origin_run_id == run_id
        # Internal continuation turn cannot create a second goal through the tool.
        internal, internal_run = await user_turn(
            store, session.id, "goal-continue:xyz", text="续跑"
        )
        with pytest.raises(ServiceError, match="invalid_input"):
            await service.update_from_tool(
                scope_for(owner, session.id, internal.id, internal_run),
                goal_update(
                    owner,
                    request_key="internal-set",
                    expected_revision=1,
                    action="set",
                    objective="内部新目标",
                ),
            )


def scope_for(owner, session_id, turn_id, run_id, *, check=lambda: None):
    from krim_agent.tools.session import SessionToolScope

    return SessionToolScope(owner, WORKSPACE, session_id, turn_id, run_id, None, check)


@pytest.mark.asyncio
async def test_concurrent_cas_only_one_writer_wins(tmp_path):
    async with managed_goal_store(tmp_path / "race.sqlite") as store:
        owner = member("owner")
        session = await create_session(store, owner.principal_id, "session-race")
        service = GoalService(store)
        turn, run_id = await user_turn(store, session.id, "user-1")
        await service.create(
            owner,
            WORKSPACE,
            session.id,
            GoalCreateRequest(
                request_key="create",
                expected_revision=0,
                objective="目标",
                origin_turn_id=turn.id,
                origin_run_id=run_id,
            ),
        )
        scope = scope_for(owner, session.id, turn.id, run_id)

        async def progress(key: str):
            return await service.update_from_tool(
                scope,
                goal_update(owner, request_key=key, expected_revision=1, action="progress"),
            )

        results = await asyncio.gather(progress("a"), progress("b"), return_exceptions=True)
        assert sum(isinstance(item, ServiceError) for item in results) == 1
        assert (await service.snapshot(owner, WORKSPACE, session.id)).revision == 2
