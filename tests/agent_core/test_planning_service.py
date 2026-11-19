"""PlanningService persistence, authorization, and retry contracts."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from pydantic import ValidationError
from sqlalchemy import update

from krim_agent.config import Settings
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import ServiceError
from krim_agent.planning.contracts import PlanRead, PlanStep, PlanUpdate
from krim_agent.planning.service import PlanningService
from krim_agent.security import Membership, PrincipalContext
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.models import metadata, sessions
from krim_agent.storage.planning_models import PLANNING_TABLES
from krim_agent.storage.sqlite import SQLiteStore

WORKSPACE = "planning-test"


def member(principal_id: str, workspace_id: str = WORKSPACE, role: str = "member"):
    return PrincipalContext(
        principal_id,
        1,
        (Membership(workspace_id=workspace_id, role=role),),
    )


def plan(request_key: str, expected_revision: int, *steps: PlanStep) -> PlanUpdate:
    return PlanUpdate(
        request_key=request_key,
        expected_revision=expected_revision,
        steps=tuple(steps),
    )


async def open_planning_store(path: Path) -> SQLiteStore:
    store = await SQLiteStore.open(path)
    async with store.lock, store.engine.begin() as conn:
        await conn.run_sync(lambda sync: metadata.create_all(sync, tables=list(PLANNING_TABLES)))
    return store


@asynccontextmanager
async def managed_planning_store(path: Path) -> AsyncIterator[SQLiteStore]:
    store = await open_planning_store(path)
    try:
        yield store
    finally:
        await store.close()


async def create_session(
    store: SQLiteStore, owner_id: str, key: str, *, workspace: str = WORKSPACE
):
    return await session_sql.create_session(
        store,
        workspace,
        owner_id,
        SessionCreate(request_key=key),
        {"key": "demo"},
        ContextPolicy(),
        Settings(),
    )


@pytest.mark.asyncio
async def test_revision_zero_and_idempotent_historical_replay(tmp_path):
    async with managed_planning_store(tmp_path / "revision.sqlite") as store:
        owner = member("owner")
        session = await create_session(store, owner.principal_id, "new-session")
        service = PlanningService(store)

        empty = await service.read(owner, WORKSPACE, session.id)
        assert empty.revision == 0
        assert empty.steps == ()
        assert empty.total == empty.matched_total == 0
        assert empty.counts.model_dump() == {
            "pending": 0,
            "in_progress": 0,
            "completed": 0,
            "blocked": 0,
        }
        assert empty.updated_at is None
        assert len(empty.content_hash) == 64

        first_body = plan(
            "first-write",
            0,
            PlanStep(id="one", text="第一步"),
            PlanStep(id="two", text="第二步", status="in_progress"),
        )
        first = await service.update(owner, WORKSPACE, session.id, first_body)
        assert first.revision == 1
        assert first.total == 2
        assert (await service.update(owner, WORKSPACE, session.id, first_body)) == first

        second = await service.update(
            owner,
            WORKSPACE,
            session.id,
            plan("second-write", 1, PlanStep(id="new", text="新版计划")),
        )
        assert second.revision == 2
        assert (await service.read(owner, WORKSPACE, session.id)).revision == 2
        historical = await service.read(owner, WORKSPACE, session.id, PlanRead(revision=1))
        assert historical == first

        replay = await service.update(owner, WORKSPACE, session.id, first_body)
        assert replay == first
        with pytest.raises(ServiceError, match="key_conflict"):
            await service.update(
                owner,
                WORKSPACE,
                session.id,
                plan("first-write", 2, PlanStep(id="one", text="不同内容")),
            )
        with pytest.raises(ServiceError, match="not_found"):
            await service.read(owner, WORKSPACE, session.id, PlanRead(revision=3))


@pytest.mark.asyncio
async def test_pagination_is_revision_pinned_and_status_filtered(tmp_path):
    async with managed_planning_store(tmp_path / "pages.sqlite") as store:
        owner = member("owner")
        session = await create_session(store, owner.principal_id, "page-session")
        service = PlanningService(store)
        original_steps = (
            PlanStep(id="a", text="A", status="pending"),
            PlanStep(id="b", text="B", status="completed"),
            PlanStep(id="c", text="C", status="in_progress"),
            PlanStep(id="d", text="D", status="blocked"),
            PlanStep(id="e", text="E", status="pending"),
            PlanStep(id="f", text="F", status="completed"),
        )
        first = await service.update(
            owner, WORKSPACE, session.id, plan("page-v1", 0, *original_steps)
        )

        first_page = await service.read(owner, WORKSPACE, session.id, PlanRead(limit=2))
        assert first_page.revision == 1
        assert [step.id for step in first_page.steps] == ["a", "b"]
        assert first_page.total == first_page.matched_total == 6
        assert first_page.next_offset == 2

        await service.update(
            owner,
            WORKSPACE,
            session.id,
            plan("page-v2", 1, PlanStep(id="replacement", text="替换版本")),
        )
        pinned_page = await service.read(
            owner,
            WORKSPACE,
            session.id,
            PlanRead(revision=first_page.revision, offset=2, limit=2),
        )
        assert pinned_page.revision == 1
        assert [step.id for step in pinned_page.steps] == ["c", "d"]
        assert pinned_page.total == 6
        assert pinned_page.next_offset == 4

        filtered = await service.read(
            owner,
            WORKSPACE,
            session.id,
            PlanRead(revision=1, limit=2, statuses=("completed", "blocked")),
        )
        assert [step.id for step in filtered.steps] == ["b", "d"]
        assert filtered.total == 6
        assert filtered.matched_total == 3
        assert filtered.counts.pending == 2
        assert filtered.next_offset == 2
        filtered_tail = await service.read(
            owner,
            WORKSPACE,
            session.id,
            PlanRead(
                revision=1,
                offset=2,
                limit=2,
                statuses=("completed", "blocked"),
            ),
        )
        assert [step.id for step in filtered_tail.steps] == ["f"]
        assert filtered_tail.next_offset is None
        assert first.revision == 1

        with pytest.raises(ValidationError):
            PlanRead(offset=1)


@pytest.mark.asyncio
async def test_plan_accepts_more_than_200_steps_and_over_one_mib(tmp_path):
    async with managed_planning_store(tmp_path / "large-plan.sqlite") as store:
        owner = member("owner")
        session = await create_session(store, owner.principal_id, "large-session")
        service = PlanningService(store)
        steps = (
            PlanStep(id="large-text", text="x" * (1024 * 1024 + 1)),
            *(PlanStep(id=f"step-{index}", text=f"Step {index}") for index in range(200)),
        )
        assert len(steps) > 200
        assert len(steps[0].text) > 1024
        assert sum(len(step.text.encode("utf-8")) for step in steps) > 1024 * 1024

        saved = await service.update(owner, WORKSPACE, session.id, plan("large-write", 0, *steps))
        assert saved.revision == 1
        assert saved.total == 201
        assert len(saved.steps) == 50
        assert saved.steps[0].id == "large-text"
        assert len(saved.steps[0].text) > 1024
        assert saved.next_offset == 50
        first_step = await service.read(owner, WORKSPACE, session.id, PlanRead(limit=1))
        assert len(first_step.steps[0].text) == 1024 * 1024 + 1


@pytest.mark.parametrize("legacy_status", ["running", "failed"])
def test_plan_steps_reject_legacy_statuses(legacy_status):
    with pytest.raises(ValidationError):
        PlanStep(id="legacy", text="not a supported plan status", status=legacy_status)


@pytest.mark.asyncio
async def test_owner_workspace_session_and_role_isolation(tmp_path):
    async with managed_planning_store(tmp_path / "isolation.sqlite") as store:
        owner = member("owner")
        other = member("other")
        admin = member("workspace-admin", role="admin")
        viewer = member("viewer", role="viewer")
        owner_session = await create_session(store, owner.principal_id, "owner-session")
        owner_second_session = await create_session(
            store, owner.principal_id, "owner-second-session"
        )
        other_session = await create_session(store, other.principal_id, "other-session")
        viewer_session = await create_session(store, viewer.principal_id, "viewer-session")
        service = PlanningService(store)
        await service.update(
            owner,
            WORKSPACE,
            owner_session.id,
            plan("owner-plan", 0, PlanStep(id="private", text="仅属原所有者")),
        )

        assert (await service.read(owner, WORKSPACE, owner_session.id)).revision == 1
        assert (await service.read(owner, WORKSPACE, owner_second_session.id)).revision == 0
        with pytest.raises(ServiceError, match="not_found"):
            await service.read(owner, WORKSPACE, other_session.id)
        with pytest.raises(ServiceError, match="not_found"):
            await service.read(other, WORKSPACE, owner_session.id)
        with pytest.raises(ServiceError, match="not_found"):
            await service.read(owner, "another-workspace", owner_session.id)

        assert (await service.read(admin, WORKSPACE, owner_session.id)).revision == 1
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.update(
                admin,
                WORKSPACE,
                owner_session.id,
                plan("admin-write", 1, PlanStep(id="admin", text="不得代写")),
            )
        assert (await service.read(viewer, WORKSPACE, viewer_session.id)).revision == 0
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.update(
                viewer,
                WORKSPACE,
                viewer_session.id,
                plan("viewer-write", 0, PlanStep(id="viewer", text="不得写入")),
            )


@pytest.mark.asyncio
async def test_closed_session_rejects_new_writes_but_allows_authorized_replay(tmp_path):
    async with managed_planning_store(tmp_path / "closed.sqlite") as store:
        owner = member("owner")
        session = await create_session(store, owner.principal_id, "closed-session")
        service = PlanningService(store)
        original = plan("accepted-before-close", 0, PlanStep(id="one", text="已提交"))
        committed = await service.update(owner, WORKSPACE, session.id, original)
        async with store.lock, store.engine.begin() as conn:
            await conn.execute(
                update(sessions)
                .where(sessions.c.id == session.id)
                .values(status="closed", closed_at=1.0)
            )

        with pytest.raises(ServiceError, match="session_closed"):
            await service.update(
                owner,
                WORKSPACE,
                session.id,
                plan("after-close", 1, PlanStep(id="two", text="不得保存")),
            )
        assert await service.update(owner, WORKSPACE, session.id, original) == committed
        assert (await service.read(owner, WORKSPACE, session.id)).revision == 1


@pytest.mark.asyncio
async def test_competing_compare_and_swap_updates_only_commit_once(tmp_path):
    async with managed_planning_store(tmp_path / "cas.sqlite") as store:
        owner = member("owner")
        session = await create_session(store, owner.principal_id, "cas-session")
        service = PlanningService(store)
        candidates = (
            plan("cas-left", 0, PlanStep(id="left", text="左侧")),
            plan("cas-right", 0, PlanStep(id="right", text="右侧")),
        )

        results = await asyncio.gather(
            *(service.update(owner, WORKSPACE, session.id, body) for body in candidates),
            return_exceptions=True,
        )
        successes = [result for result in results if not isinstance(result, BaseException)]
        failures = [result for result in results if isinstance(result, ServiceError)]
        assert len(successes) == 1
        assert successes[0].revision == 1
        assert len(failures) == 1
        assert failures[0].code == "revision_conflict"
        current = await service.read(owner, WORKSPACE, session.id)
        assert current.revision == 1
        assert [step.id for step in current.steps] in (["left"], ["right"])


class RevocableIdentities:
    def __init__(self, *, fail_at: int | None = None) -> None:
        self.fail_at = fail_at
        self.calls = 0

    def validate(self, actor: PrincipalContext) -> PrincipalContext:
        self.calls += 1
        if self.calls == self.fail_at:
            raise ServiceError("unauthorized", 401)
        return actor


@pytest.mark.asyncio
async def test_cancellation_and_identity_revocation_roll_back_mutation(tmp_path):
    async with managed_planning_store(tmp_path / "rollback.sqlite") as store:
        owner = member("owner")
        session = await create_session(store, owner.principal_id, "rollback-session")
        service = PlanningService(store)
        body = plan("cancelled-write", 0, PlanStep(id="cancel", text="取消后不应保留"))
        checks = 0

        def cancel_after_write() -> None:
            nonlocal checks
            checks += 1
            if checks == 4:
                raise ServiceError("cancelled")

        with pytest.raises(ServiceError, match="cancelled"):
            await service.update(owner, WORKSPACE, session.id, body, check=cancel_after_write)
        assert checks == 4
        assert (await service.read(owner, WORKSPACE, session.id)).revision == 0

        identities = RevocableIdentities(fail_at=5)
        revocable = PlanningService(store, identities)
        with pytest.raises(ServiceError, match="unauthorized"):
            await revocable.update(
                owner,
                WORKSPACE,
                session.id,
                plan("revoked-write", 0, PlanStep(id="revoked", text="撤权后不应保留")),
            )
        assert identities.calls == 5
        identities.fail_at = None
        assert (await revocable.read(owner, WORKSPACE, session.id)).revision == 0


@pytest.mark.asyncio
async def test_write_confirms_committed_plan_after_commit_transport_error(tmp_path, monkeypatch):
    async with managed_planning_store(tmp_path / "commit-confirm.sqlite") as store:
        owner = member("owner")
        session = await create_session(store, owner.principal_id, "commit-session")
        service = PlanningService(store)
        original_write = store._write

        async def commit_then_error(operation):
            await original_write(operation)
            raise RuntimeError("simulated error after SQLite commit")

        monkeypatch.setattr(store, "_write", commit_then_error)
        result = await service.update(
            owner,
            WORKSPACE,
            session.id,
            plan("committed-write", 0, PlanStep(id="saved", text="提交后确认")),
        )
        assert result.revision == 1
        assert [step.id for step in result.steps] == ["saved"]
        assert (await service.read(owner, WORKSPACE, session.id)) == result


@pytest.mark.asyncio
async def test_plan_survives_turns_and_store_restart(tmp_path):
    db = tmp_path / "restart.sqlite"
    store = await open_planning_store(db)
    try:
        owner = member("owner")
        session = await create_session(store, owner.principal_id, "restart-session")
        service = PlanningService(store)
        saved = await service.update(
            owner,
            WORKSPACE,
            session.id,
            plan("restart-plan", 0, PlanStep(id="persistent", text="跨轮次保留")),
        )

        await session_sql.submit(
            store,
            session.id,
            TurnRequest(request_key="turn-one", text="第一轮", expected_revision=0),
            Settings(),
        )
        await session_sql.submit(
            store,
            session.id,
            TurnRequest(request_key="turn-two", text="第二轮", expected_revision=1),
            Settings(),
        )
        assert await service.read(owner, WORKSPACE, session.id) == saved

        await store.close()
        store = await SQLiteStore.open(db)
        reopened = PlanningService(store)
        assert await reopened.read(owner, WORKSPACE, session.id) == saved
        revision_zero = await reopened.read(owner, WORKSPACE, session.id, PlanRead(revision=0))
        assert revision_zero.revision == 0
        assert revision_zero.steps == ()
        assert revision_zero.total == 0
    finally:
        await store.close()
