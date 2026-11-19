"""Durable compact_context scheduling, boundary consumption and the snapshot port.

Real SQLiteStore on an isolated tmp database: persistence across close/reopen,
idempotent request keys, owner authorization, run-boundary guards,
failure-preserving pending requests, and the host-injected snapshot
projection. No model, run loop or frontend is involved.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path

import pytest
from sqlalchemy import func, insert, select, update

from krim_agent.context.serialization import fingerprint as canonical_fingerprint
from krim_agent.context.service import (
    CompactionBoundary,
    ContextService,
    ContextSnapshotItem,
    snapshot_fingerprint,
)
from krim_agent.context.tools import CompactContextInput, compact_context_bundle
from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.security import Membership, PrincipalContext
from krim_agent.storage.account_models import accounts
from krim_agent.storage.context_models import CONTEXT_TABLE_NAMES, CONTEXT_TABLES
from krim_agent.storage.models import runs, sessions, turns
from krim_agent.storage.sandbox_models import (
    sandbox_environments,
    sandbox_projects,
    sandbox_session_bindings,
)
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.tools.core import Invocation
from krim_agent.tools.session import SessionToolScope

WORKSPACE = "local"
OWNER = PrincipalContext("alice", 1, (Membership(workspace_id=WORKSPACE, role="admin"),), True)
STRANGER = PrincipalContext("bob", 1, (Membership(workspace_id=WORKSPACE, role="admin"),), True)
OUTSIDER = PrincipalContext("carol", 1, (), True)
RECEIPT = {"source_fingerprint": "a" * 64, "covered": 2, "passes": 1, "summary_hash": None}


def invocation_of(name: str, run_id: str = "run-tool", call_id: str = "call-1") -> Invocation:
    return Invocation(
        run_id=run_id,
        call_id=call_id,
        span_id="span-1",
        step_index=0,
        name=name,
        arguments_json="{}",
        tool_hash="0" * 64,
    )


def scope_of(actor: PrincipalContext, session_id: str, run_id: str = "run-1") -> SessionToolScope:
    return SessionToolScope(
        actor=actor,
        workspace_id=WORKSPACE,
        session_id=session_id,
        turn_id="turn-1",
        run_id=run_id,
    )


async def make_session(store: SQLiteStore, session_id: str, owner: str, status: str = "open"):
    async with store.lock, store.engine.begin() as conn:
        await conn.execute(
            insert(sessions).values(
                id=session_id,
                workspace_id=WORKSPACE,
                owner_id=owner,
                create_key=f"key-{session_id}",
                request_hash="0" * 64,
                title="context tests",
                target_key="target-v2",
                target={"target_key": "target-v2"},
                context_policy={
                    "max_bytes": 65536,
                    "reserved_output_tokens": 1024,
                    "counter_version": "approx-utf8/1",
                },
                revision=0,
                status=status,
                dispatch_paused=False,
                created_at=1.0,
            )
        )


async def bind_run(
    store: SQLiteStore,
    run_id: str,
    session_id: str,
    owner: str,
    *,
    status: str = "running",
    stop_requested: bool = False,
) -> None:
    async with store.lock, store.engine.begin() as conn:
        ordinal = (
            await conn.scalar(
                select(func.count()).select_from(turns).where(turns.c.session_id == session_id)
            )
            or 0
        )
        await conn.execute(
            insert(runs).values(
                id=run_id,
                request_key=f"rk-{run_id}",
                request_hash="0" * 64,
                request={},
                target={},
                status=status,
                trace_id="t" * 32,
                root_span_id="r" * 16,
                created_at=1.0,
                stop_requested=stop_requested,
            )
        )
        await conn.execute(
            insert(turns).values(
                id=f"turn-{run_id}",
                session_id=session_id,
                workspace_id=WORKSPACE,
                owner_id=owner,
                request_key=f"tk-{run_id}",
                request_hash="0" * 64,
                text="go",
                status="running",
                ordinal=ordinal,
                accepted_revision=0,
                run_id=run_id,
                projection_version="test/1",
                created_at=1.0,
            )
        )


async def bind_project(store, project_id="project"):
    async with store.engine.begin() as conn:
        if (
            await conn.scalar(
                select(accounts.c.principal_id).where(
                    accounts.c.principal_id == OWNER.principal_id,
                )
            )
            is None
        ):
            await conn.execute(
                insert(accounts).values(
                    principal_id=OWNER.principal_id,
                    display_name="owner",
                    status="active",
                    platform_role="admin",
                    authorization_generation=1,
                    revision=1,
                    legacy=True,
                    created_at=1.0,
                    updated_at=1.0,
                )
            )
            await conn.execute(
                insert(sandbox_environments).values(
                    environment_id="environment",
                    principal_id=OWNER.principal_id,
                    quota_override={},
                    revision=1,
                    status="ready",
                    created_at=1.0,
                    updated_at=1.0,
                )
            )
        await conn.execute(
            insert(sandbox_projects).values(
                project_id=project_id,
                environment_id="environment",
                principal_id=OWNER.principal_id,
                workspace_id=WORKSPACE,
                title="project",
                generation=1,
                status="ready",
                created_at=1.0,
                updated_at=1.0,
            )
        )
        await conn.execute(
            insert(sandbox_session_bindings).values(
                session_id="session-a",
                principal_id=OWNER.principal_id,
                project_id=project_id,
                revision=1,
                created_at=1.0,
                updated_at=1.0,
            )
        )


@pytest.fixture
async def store(tmp_path: Path):
    path = tmp_path / "context-service.sqlite"
    service_store = await SQLiteStore.open(path)
    service_store.test_path = path
    await make_session(service_store, "session-a", OWNER.principal_id)
    await make_session(service_store, "session-b", STRANGER.principal_id)
    await make_session(service_store, "session-closed", OWNER.principal_id, "closed")
    await bind_run(service_store, "run-1", "session-a", OWNER.principal_id)
    await bind_run(service_store, "run-b", "session-b", STRANGER.principal_id)
    try:
        yield service_store
    finally:
        await service_store.close()


async def test_combined_prepare_reads_pending_and_rechecks_each_call(store, monkeypatch):
    await bind_project(store)
    service = ContextService(
        store,
        snapshots=lambda scope: [
            ContextSnapshotItem(name="plan", revision=1, content="current plan")
        ],
    )
    await service.schedule_compaction(OWNER, WORKSPACE, "session-a", "manual")
    original = store.work_read
    calls = 0

    async def counted(operation):
        nonlocal calls
        calls += 1
        return await original(operation)

    monkeypatch.setattr(store, "work_read", counted)
    boundary = service.boundary(OWNER, WORKSPACE, "session-a", "run-1")
    token, items = await boundary.prepare_context()
    assert token == "manual" and [item.name for item in items] == ["plan"]
    assert calls == 2
    async with store.engine.begin() as conn:
        await conn.execute(update(runs).where(runs.c.id == "run-1").values(stop_requested=True))
    with pytest.raises(ServiceError, match="run_closed"):
        await boundary.prepare_context()
    assert calls == 3


async def test_boundary_optional_scope_preserves_host_guard_and_project(store):
    await bind_project(store)
    cancelled = False
    seen = []

    def check():
        if cancelled:
            raise asyncio.CancelledError

    async def snapshots(scope):
        seen.append(scope)
        return []

    scope = SessionToolScope(
        OWNER, WORKSPACE, "session-a", "turn-run-1", "run-1", project_id="project", check=check
    )
    service = ContextService(store, snapshots=snapshots)
    await service.schedule_compaction(OWNER, WORKSPACE, "session-a", "manual")
    boundary = service.boundary(OWNER, WORKSPACE, "session-a", "run-1", scope=scope)
    assert await boundary.prepare() == []
    assert seen[0].turn_id == scope.turn_id and seen[0].project_id == "project"
    assert seen[0].check is check
    cancelled = True
    for operation in (
        boundary.pending,
        boundary.prepare,
        lambda: boundary.complete("manual", RECEIPT),
    ):
        with pytest.raises(asyncio.CancelledError):
            await operation()
    assert len(seen) == 1
    assert (await service.pending(OWNER, WORKSPACE, "session-a"))[0]["request_key"] == "manual"
    cancelled = False
    with pytest.raises(ServiceError, match="permission_denied"):
        service.boundary(
            OWNER, WORKSPACE, "session-a", "run-1", scope=replace(scope, session_id="session-b")
        )
    wrong_turn = service.boundary(
        OWNER, WORKSPACE, "session-a", "run-1", scope=replace(scope, turn_id="wrong-turn")
    )
    with pytest.raises(ServiceError, match="permission_denied"):
        await wrong_turn.pending()


async def test_boundary_resolves_current_project_and_rejects_rebinding_during_snapshot(store):
    await bind_project(store)
    seen = []

    async def snapshots(scope):
        seen.append(scope.project_id)
        return []

    service = ContextService(store, snapshots=snapshots)
    boundary = service.boundary(OWNER, WORKSPACE, "session-a", "run-1")
    await boundary.prepare()
    assert seen == ["project"]
    scope = SessionToolScope(
        OWNER, WORKSPACE, "session-a", "turn-run-1", "run-1", project_id="project"
    )
    bound = service.boundary(OWNER, WORKSPACE, "session-a", "run-1", scope=scope)

    async def rebind(scope):
        async with store.engine.begin() as conn:
            await conn.execute(
                update(sandbox_session_bindings)
                .where(
                    sandbox_session_bindings.c.session_id == "session-a",
                )
                .values(project_id=None, revision=2)
            )
        return [ContextSnapshotItem(name="memory", revision=1, content="stale project memory")]

    service.snapshots = rebind
    with pytest.raises(ServiceError, match="permission_denied"):
        await bound.prepare()
    for operation in (bound.pending, bound.prepare):
        with pytest.raises(ServiceError, match="permission_denied"):
            await operation()


async def test_scope_cancelled_during_snapshot_prevents_returning_projection(store):
    cancelled = False

    def check():
        if cancelled:
            raise asyncio.CancelledError

    async def snapshots(scope):
        nonlocal cancelled
        cancelled = True
        return [ContextSnapshotItem(name="plan", revision=1, content="do not inject")]

    scope = SessionToolScope(OWNER, WORKSPACE, "session-a", "turn-run-1", "run-1", check=check)
    service = ContextService(store, snapshots=snapshots)
    boundary = service.boundary(OWNER, WORKSPACE, "session-a", "run-1", scope=scope)
    with pytest.raises(asyncio.CancelledError):
        await boundary.prepare()


async def test_schedule_is_idempotent_per_request_key(store):
    service = ContextService(store)
    first = await service.schedule_compaction(
        OWNER, WORKSPACE, "session-a", "compact-context.1", reason="long history"
    )
    assert first["status"] == "pending" and first["revision"] == 1
    retry = await service.schedule_compaction(
        OWNER, WORKSPACE, "session-a", "compact-context.1", reason="long history"
    )
    assert retry == first | {"created": False}
    second = await service.schedule_compaction(OWNER, WORKSPACE, "session-a", "compact-context.2")
    assert second["revision"] == 2
    pending = await service.pending(OWNER, WORKSPACE, "session-a")
    assert [item["request_key"] for item in pending] == [
        "compact-context.1",
        "compact-context.2",
    ]


async def test_requests_and_fulfillment_survive_restart(store):
    service = ContextService(store)
    await service.schedule_compaction(OWNER, WORKSPACE, "session-a", "durable-1")
    await service.schedule_compaction(OWNER, WORKSPACE, "session-a", "durable-2")
    await bind_run(store, "run-2", "session-a", OWNER.principal_id)
    database = Path(store.test_path)
    await store.close()

    reopened = await SQLiteStore.open(database)
    try:
        after_restart = ContextService(reopened)
        pending = await after_restart.pending(OWNER, WORKSPACE, "session-a")
        assert [item["request_key"] for item in pending] == ["durable-1", "durable-2"]
        boundary = after_restart.boundary(OWNER, WORKSPACE, "session-a", "run-2")
        assert await boundary.pending() == "durable-1"
        await boundary.complete("durable-1", RECEIPT)
        assert await boundary.pending() == "durable-2"
    finally:
        await reopened.close()

    again = await SQLiteStore.open(database)
    try:
        pending = await ContextService(again).pending(OWNER, WORKSPACE, "session-a")
        assert [item["request_key"] for item in pending] == ["durable-2"]
    finally:
        await again.close()


async def test_same_key_with_different_body_conflicts(store):
    service = ContextService(store)
    await service.schedule_compaction(OWNER, WORKSPACE, "session-a", "key-x")
    with pytest.raises(ServiceError, match="key_conflict"):
        await service.schedule_compaction(
            OWNER, WORKSPACE, "session-a", "key-x", reason="different"
        )
    with pytest.raises(ServiceError, match="key_conflict"):
        await service.schedule_compaction(OWNER, WORKSPACE, "session-a", "key-x", expected=3)


async def test_only_the_session_owner_can_schedule_or_read(store):
    service = ContextService(store)
    with pytest.raises(ServiceError, match="not_found"):
        await service.schedule_compaction(STRANGER, WORKSPACE, "session-a", "other-key")
    with pytest.raises(ServiceError, match="not_found"):
        await service.pending(STRANGER, WORKSPACE, "session-a")
    with pytest.raises(ServiceError, match="not_found"):
        await service.pending(OUTSIDER, WORKSPACE, "session-a")


async def test_scheduling_requires_an_open_session(store):
    service = ContextService(store)
    with pytest.raises(ServiceError, match="session_closed"):
        await service.schedule_compaction(OWNER, WORKSPACE, "session-closed", "late-key")


async def test_boundary_returns_oldest_token_and_consumes_only_after_success(store):
    service = ContextService(store)
    await service.schedule_compaction(OWNER, WORKSPACE, "session-a", "first")
    await service.schedule_compaction(OWNER, WORKSPACE, "session-a", "second")
    await bind_run(store, "run-9", "session-a", OWNER.principal_id)
    await bind_run(store, "run-10", "session-a", OWNER.principal_id)
    boundary = service.boundary(OWNER, WORKSPACE, "session-a", "run-9")
    assert await boundary.pending() == "first"
    # A failed compaction never completes: every run still sees the same token.
    assert await service.boundary(OWNER, WORKSPACE, "session-a", "run-10").pending() == "first"
    await boundary.complete("first", RECEIPT)
    assert await boundary.pending() == "second"
    pending = await service.pending(OWNER, WORKSPACE, "session-a")
    assert [item["request_key"] for item in pending] == ["second"]


async def test_completion_is_idempotent_and_receipt_shape_is_fixed(store):
    service = ContextService(store)
    await service.schedule_compaction(OWNER, WORKSPACE, "session-a", "token")
    boundary = service.boundary(OWNER, WORKSPACE, "session-a", "run-1")
    await boundary.complete("token", RECEIPT)
    await boundary.complete("token", RECEIPT)
    with pytest.raises(ServiceError, match="key_conflict"):
        await boundary.complete("token", {"source_fingerprint": "a" * 64, "covered": 3})
    with pytest.raises(ServiceError, match="invalid_input"):
        await boundary.complete("token", {"pass_index": 0})
    with pytest.raises(ServiceError, match="invalid_input"):
        await boundary.complete("token", {"source_fingerprint": "not-hex"})
    with pytest.raises(ServiceError, match="not_found"):
        await boundary.complete("missing", RECEIPT)
    assert await service.pending(OWNER, WORKSPACE, "session-a") == []


async def test_boundary_verifies_the_run_belongs_to_the_session_and_is_unstopped(store):
    service = ContextService(store)
    await service.schedule_compaction(OWNER, WORKSPACE, "session-a", "owned")
    with pytest.raises(ServiceError, match="not_found"):
        await service.boundary(OWNER, WORKSPACE, "session-a", "run-unbound").pending()
    await bind_run(store, "run-done", "session-a", OWNER.principal_id, status="completed")
    with pytest.raises(ServiceError, match="run_closed"):
        await service.boundary(OWNER, WORKSPACE, "session-a", "run-done").pending()
    await bind_run(store, "run-stop", "session-a", OWNER.principal_id, stop_requested=True)
    with pytest.raises(ServiceError, match="run_closed"):
        await service.boundary(OWNER, WORKSPACE, "session-a", "run-stop").pending()
    # A run bound to another owner's session never passes this session's guard.
    await bind_run(store, "run-x", "session-b", STRANGER.principal_id)
    with pytest.raises(ServiceError, match="not_found"):
        await service.boundary(OWNER, WORKSPACE, "session-a", "run-x").pending()
    other = service.boundary(STRANGER, WORKSPACE, "session-a", "run-1")
    with pytest.raises(ServiceError, match="not_found"):
        await other.pending()
    with pytest.raises(ServiceError, match="not_found"):
        await other.complete("owned", RECEIPT)
    own = service.boundary(STRANGER, WORKSPACE, "session-b", "run-b")
    assert await own.pending() is None


async def test_snapshot_prepare_freezes_injected_provider_items(store):
    items = (
        ContextSnapshotItem(
            name="plan", revision=3, content="- step one", source_refs=({"plan": "a"},)
        ),
        {"name": "goal", "revision": "r7", "content": "ship the release"},
    )
    seen: list[SessionToolScope] = []

    async def provider(scope: SessionToolScope):
        seen.append(scope)
        return items

    service = ContextService(store, snapshots=provider)
    await bind_run(store, "run-5", "session-a", OWNER.principal_id)
    await bind_run(store, "run-6", "session-a", OWNER.principal_id)
    boundary = service.boundary(OWNER, WORKSPACE, "session-a", "run-5")
    prepared = await boundary.prepare()
    assert [item.name for item in prepared] == ["plan", "goal"]
    assert prepared[0].revision == 3 and prepared[1].revision == "r7"
    assert prepared[0].source_refs == ({"plan": "a"},)
    assert isinstance(prepared[1], ContextSnapshotItem)
    # The provider receives the run's real scope, including its turn id.
    assert seen[0].session_id == "session-a"
    assert seen[0].run_id == "run-5" and seen[0].turn_id == "turn-run-5"
    again = await service.boundary(OWNER, WORKSPACE, "session-a", "run-6").prepare()
    assert [item.model_dump(mode="json") for item in again] == [
        item.model_dump(mode="json") for item in prepared
    ]
    assert snapshot_fingerprint(prepared[0]) == fingerprint(prepared[0].model_dump(mode="json"))


async def test_snapshot_content_has_no_application_size_ceiling(store):
    large = "x" * 1_100_000

    def provider(scope: SessionToolScope):
        return [ContextSnapshotItem(name="plan", revision=1, content=large)]

    service = ContextService(store, snapshots=provider)
    prepared = await service.boundary(OWNER, WORKSPACE, "session-a", "run-1").prepare()
    assert prepared[0].content == large
    assert snapshot_fingerprint(prepared[0]) == canonical_fingerprint(
        prepared[0].model_dump(mode="json")
    )


async def test_snapshot_prepare_is_empty_without_a_provider(store):
    service = ContextService(store)
    prepared = await service.boundary(OWNER, WORKSPACE, "session-a", "run-1").prepare()
    assert prepared == []


async def test_snapshot_provider_output_is_validated(store):
    async def broken(scope):
        return [{"name": "plan", "revision": "", "content": "x"}]

    service = ContextService(store, snapshots=broken)
    with pytest.raises(ServiceError, match="invalid_output"):
        await service.boundary(OWNER, WORKSPACE, "session-a", "run-1").prepare()
    with pytest.raises(ServiceError, match="invalid_input"):
        ContextService(store, snapshots="not-callable")


async def test_snapshot_provider_never_runs_without_authorization(store):
    denied = ContextService(
        store,
        snapshots=lambda scope: pytest.fail("provider must not run without authorization"),
    )
    with pytest.raises(ServiceError, match="not_found"):
        await denied.boundary(OUTSIDER, WORKSPACE, "session-a", "run-1").prepare()


async def test_context_tables_are_exported_for_host_migration(store):
    assert CONTEXT_TABLE_NAMES == {"context_requests"}
    assert [table.name for table in CONTEXT_TABLES] == ["context_requests"]


async def test_tool_bundle_enqueues_without_compacting_inline(store):
    service = ContextService(store)
    calls: list[str] = []

    async def resolve(invocation):
        calls.append(invocation.name)
        return scope_of(OWNER, "session-a", invocation.run_id)

    bundle = compact_context_bundle(service, resolve)
    assert [spec.name for spec in bundle.specs] == ["compact_context"]
    spec = bundle.specs[0]
    assert spec.capability == "session_state" and spec.risk == "write"
    with pytest.raises(ServiceError):
        await spec.executor(CompactContextInput())

    invocation = invocation_of("compact_context")
    assert await bundle.policy(invocation) == "allow"
    assert calls == ["compact_context"]
    result = await spec.scoped_executor(invocation, CompactContextInput(reason="long"), None)
    assert result["status"] == "pending"
    assert result["request_key"] == "compact-context.run-tool.call-1"
    pending = await service.pending(OWNER, WORKSPACE, "session-a")
    assert [item["request_key"] for item in pending] == [result["request_key"]]
    # The tool only enqueues; consumption still goes through the boundary.
    boundary = service.boundary(OWNER, WORKSPACE, "session-a", "run-1")
    assert await boundary.pending() == result["request_key"]
    await boundary.complete(result["request_key"], RECEIPT)
    assert await service.pending(OWNER, WORKSPACE, "session-a") == []


async def test_tool_bundle_retries_are_idempotent(store):
    service = ContextService(store)

    async def resolve(invocation):
        return scope_of(OWNER, "session-a", invocation.run_id)

    bundle = compact_context_bundle(service, resolve)
    invocation = invocation_of("compact_context", run_id="run-1", call_id="call-2")
    assert await bundle.policy(invocation) == "allow"
    first = await bundle.specs[0].scoped_executor(invocation, CompactContextInput(), None)
    second = await bundle.specs[0].scoped_executor(invocation, CompactContextInput(), None)
    assert second == first | {"created": False}
    pending = await service.pending(OWNER, WORKSPACE, "session-a")
    assert len(pending) == 1


async def test_tool_policy_denies_binding_drift_and_unknown_names(store):
    service = ContextService(store)

    async def wrong_run(invocation):
        return scope_of(OWNER, "session-a", "run-other")

    drifted = compact_context_bundle(service, wrong_run)
    assert await drifted.policy(invocation_of("compact_context")) == "deny"
    with pytest.raises(ServiceError, match="tool_binding_changed"):
        await drifted.specs[0].scoped_executor(
            invocation_of("compact_context"), CompactContextInput(), None
        )

    async def plain_scope(invocation):
        return scope_of(OWNER, "session-a")

    plain = compact_context_bundle(service, plain_scope)
    assert await plain.policy(invocation_of("plan_update")) == "deny"

    async def stranger_scope(invocation):
        return scope_of(STRANGER, "session-a")

    stranger = compact_context_bundle(service, stranger_scope)
    assert await stranger.policy(invocation_of("compact_context")) == "deny"


async def test_boundary_exposes_run_scoped_projection_for_the_host(store):
    boundary = CompactionBoundary(ContextService(store), OWNER, WORKSPACE, "session-a", "run-1")
    turn_id, project_id = await boundary._guard()
    assert turn_id == "turn-run-1" and project_id is None
