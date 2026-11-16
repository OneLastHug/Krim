"""V3 development history admission and frozen-source integrity tests."""

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select, update

from krim_agent.accounts.service import IdentityService
from krim_agent.config import Settings
from krim_agent.context.types import (
    CompactionPolicy,
    ContextPolicy,
    DevelopmentContextState,
    DevelopmentSessionContext,
    PreparedContext,
    SessionContext,
)
from krim_agent.contracts import Message, RunRequest, ServiceError, fingerprint
from krim_agent.sandbox.bindings import bind_project
from krim_agent.sandbox.configuration import SandboxConfiguration
from krim_agent.sandbox.models import BindProjectBody
from krim_agent.sandbox.service import SandboxService
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.account_models import account_workspaces, accounts
from krim_agent.storage.development_context import DevelopmentCompactionSource, state_from_raw
from krim_agent.storage.models import turns
from krim_agent.storage.sandbox_models import sandbox_session_bindings
from krim_agent.storage.sqlite import SQLiteStore


@pytest.fixture
async def development_session(tmp_path: Path):
    store = await SQLiteStore.open(tmp_path / "development-context.sqlite")
    identities = await IdentityService.open(store)
    bootstrap = await identities.bootstrap("context test", request_key="bootstrap")
    actor = identities.authenticate("Bearer " + str(bootstrap.token))
    sandbox = SandboxService(store, identities, SandboxConfiguration())
    workspace = actor.memberships[0].workspace_id
    project = await sandbox.create_project(
        actor,
        {"workspace_id": workspace, "title": "context source"},
        request_key="project",
    )
    settings = Settings(
        development_history_compaction=True,
        max_request_bytes=8192,
        context_max_bytes=16384,
        context_reserved_output_tokens=1,
        context_max_messages=16,
    )
    policy = ContextPolicy(max_bytes=16384, max_messages=16, reserved_output_tokens=1)
    target = {
        "schema_version": 2,
        "provider": {"name": "frozen-test-provider"},
        "tools": [],
    }
    session = await session_sql.create_session(
        store,
        workspace,
        actor.principal_id,
        SessionCreate(request_key="session", target_key="target-development"),
        target,
        policy,
        settings,
    )
    await bind_project(
        sandbox,
        actor,
        session.id,
        BindProjectBody(
            expected_revision=0,
            expected_session_revision=0,
            project_id=project["project_id"],
        ),
        request_key="bind-project",
    )
    import time

    from krim_agent.project_access.types import ProjectGrant
    from krim_agent.storage.project_access import SQLiteAccessRepository
    from krim_agent.storage.sandbox_models import sandbox_projects

    async with store.engine.begin() as conn:
        await conn.execute(
            update(sandbox_projects)
            .where(
                sandbox_projects.c.project_id == project["project_id"],
            )
            .values(status="ready")
        )
    await SQLiteAccessRepository(store).put(
        ProjectGrant(
            owner_id=actor.principal_id,
            workspace_id=workspace,
            project_id=project["project_id"],
            actor_id=actor.principal_id,
            actor_generation=actor.generation,
            revision=1,
            actions=("shell.execute",),
            requires_approval=False,
            expires_at=time.time() + 600,
        ),
        expected_revision=0,
    )
    yield store, identities, sandbox, actor, project, session, settings, policy, target
    await sandbox.close()
    await identities.close()
    await store.close()


async def _start_turn(fixture: Any, text: str, request_key: str, revision: int):
    store, _identities, _sandbox, _actor, _project, session, settings, _policy, target = fixture
    turn = await session_sql.submit(
        store,
        session.id,
        TurnRequest(text=text, request_key=request_key, expected_revision=revision),
        settings,
    )
    request = RunRequest(text=text, request_key="ac-turn." + turn.id, target="target-development")
    run, created = await store.begin_turn_run(turn.id, request, target, settings)
    assert created
    await store.set_running(run.id)
    return turn, run


async def _store_transcript(store: SQLiteStore, turn_id: str, content: str) -> None:
    async with store.lock, store.engine.begin() as conn:
        await conn.execute(
            update(turns)
            .where(turns.c.id == turn_id)
            .values(
                transcript=[{"role": "user", "content": content, "call_id": None, "tool_calls": []}]
            )
        )


async def test_v3_disabled_preserves_legacy_v1_snapshot(development_session) -> None:
    fixture = development_session
    store = fixture[0]
    settings = fixture[6].model_copy(update={"development_history_compaction": False})
    session = fixture[5]
    target = fixture[8]
    turn = await session_sql.submit(
        store,
        session.id,
        TurnRequest(text="legacy current", request_key="legacy", expected_revision=1),
        settings,
    )
    request = RunRequest(
        text="legacy current", request_key="ac-turn." + turn.id, target="target-development"
    )
    run, created = await store.begin_turn_run(turn.id, request, target, settings)
    assert created
    snapshot = await store.snapshot(run.id)
    assert type(snapshot.session_context) is SessionContext
    assert snapshot.session_context.context.schema_version == 1
    raw = await store.work_read(
        lambda conn: conn.scalar(select(turns.c.context_snapshot).where(turns.c.id == turn.id))
    )
    assert raw["schema_version"] == 1


async def test_v3_admits_long_history_without_copying_it_into_run_snapshot(
    development_session,
) -> None:
    fixture = development_session
    store, *_ = fixture
    for index in range(3):
        turn, run = await _start_turn(fixture, f"prior {index}", f"prior-{index}", index + 1)
        await store.finish(run.id, "completed")
        await _store_transcript(store, turn.id, "history-" + str(index) + "-" + "x" * 9000)

    turn, run = await _start_turn(fixture, "current task", "current", 4)
    snapshot = await store.snapshot(run.id)
    assert isinstance(snapshot.session_context, DevelopmentSessionContext)
    assert len(snapshot.session_context.frozen.sources) == 3
    assert snapshot.session_context.prepared is None
    raw = await store.work_read(
        lambda conn: conn.scalar(select(turns.c.context_snapshot).where(turns.c.id == turn.id))
    )
    assert raw["schema_version"] == 3
    assert "messages" not in raw["frozen"]
    assert len(str(raw).encode()) < 8192


async def test_v3_rejects_current_input_over_budget_without_reserving_run(
    development_session,
) -> None:
    fixture = development_session
    store, *_ = fixture
    turn = await session_sql.submit(
        store,
        fixture[5].id,
        TurnRequest(text="z" * 9000, request_key="oversized", expected_revision=1),
        fixture[6],
    )
    with pytest.raises(ServiceError, match="context_budget"):
        await store.begin_turn_run(
            turn.id,
            RunRequest(
                text="z" * 9000,
                request_key="ac-turn." + turn.id,
                target="target-development",
            ),
            fixture[8],
            fixture[6],
        )
    current = await session_sql.get_turn(store, turn.id)
    assert current.run_id is None and current.status == "queued"


async def test_v3_empty_source_paging_and_prepared_corruption_are_detected(
    development_session,
) -> None:
    fixture = development_session
    store = fixture[0]
    turn, run = await _start_turn(fixture, "current", "empty", 1)
    async with store.lock, store.engine.connect() as conn:
        row = (
            (
                await conn.execute(
                    select(turns.c.context_snapshot, turns.c.context_fingerprint).where(
                        turns.c.id == turn.id
                    )
                )
            )
            .mappings()
            .one()
        )
    raw_state = DevelopmentContextState.model_validate_json(json.dumps(row["context_snapshot"]))
    frozen = raw_state.frozen
    assert row["context_fingerprint"] == fingerprint(frozen.model_dump(mode="json"))
    snapshot = await store.snapshot(run.id)
    assert isinstance(snapshot.session_context, DevelopmentSessionContext)
    source = DevelopmentCompactionSource(snapshot.session_context.frozen)
    page = await store.work_read(lambda conn: source.page(conn, run.id, 0, 16))
    assert page == []

    state = state_from_raw(row["context_snapshot"], row["context_fingerprint"])
    frozen = state.frozen
    prepared = PreparedContext(
        messages=(
            Message(role="system", content=frozen.system_prompt),
            Message(role="user", content="current"),
        ),
        sources=frozen.sources,
        policy=CompactionPolicy(),
        source_fingerprint=frozen.source_fingerprint,
        summary_hash=None,
        input_bytes=64,
        estimated_tokens=16,
    )
    await store.work_write(
        lambda conn: DevelopmentCompactionSource(frozen).save_prepared(conn, run.id, prepared),
        lambda: asyncio.sleep(0, result=None),
    )
    saved_raw = await store.work_read(
        lambda conn: conn.scalar(select(turns.c.context_snapshot).where(turns.c.id == turn.id))
    )
    saved = state_from_raw(saved_raw, fingerprint(saved_raw["frozen"]))
    assert saved.prepared == prepared
    corrupted = saved.model_copy(update={"prepared_hash": "0" * 64}).model_dump(mode="json")
    async with store.lock, store.engine.begin() as conn:
        await conn.execute(
            update(turns).where(turns.c.id == turn.id).values(context_snapshot=corrupted)
        )
    with pytest.raises(ServiceError, match="facts_corrupt"):
        await store.snapshot(run.id)


@pytest.mark.parametrize("raw", [[], "invalid", 1, None])
async def test_v3_non_object_context_is_facts_corrupt(development_session, raw):
    fixture, store = development_session, development_session[0]
    turn, run = await _start_turn(fixture, "current", "bad-snapshot", 1)
    async with store.lock, store.engine.begin() as conn:
        await conn.execute(update(turns).where(turns.c.id == turn.id).values(context_snapshot=raw))
    with pytest.raises(ServiceError, match="facts_corrupt"):
        await store.snapshot(run.id)


@pytest.mark.parametrize("drift", ["owner", "project", "membership", "key", "grant"])
async def test_v3_source_rechecks_authorization_and_project_binding(
    development_session, drift: str
) -> None:
    fixture = development_session
    store, _identities, _sandbox, actor, project, _session, _settings, _policy, _target = fixture
    _turn, run = await _start_turn(fixture, "current", "drift", 1)
    snapshot = await store.snapshot(run.id)
    assert isinstance(snapshot.session_context, DevelopmentSessionContext)
    source = DevelopmentCompactionSource(snapshot.session_context.frozen)
    async with store.lock, store.engine.begin() as conn:
        if drift == "owner":
            await conn.execute(
                update(accounts)
                .where(accounts.c.principal_id == actor.principal_id)
                .values(authorization_generation=actor.generation + 1)
            )
        elif drift == "project":
            await conn.execute(
                update(sandbox_session_bindings)
                .where(sandbox_session_bindings.c.session_id == snapshot.session_context.session_id)
                .values(project_id=None)
            )
        elif drift == "key":
            from krim_agent.storage.account_models import access_keys

            await conn.execute(
                update(access_keys)
                .where(access_keys.c.principal_id == actor.principal_id)
                .values(status="revoked")
            )
        elif drift == "grant":
            from krim_agent.storage.development_models import project_access_records

            await conn.execute(
                project_access_records.delete().where(
                    project_access_records.c.project_id == project["project_id"],
                    project_access_records.c.kind == "grant",
                )
            )
        else:
            await conn.execute(
                update(account_workspaces)
                .where(
                    account_workspaces.c.principal_id == actor.principal_id,
                    account_workspaces.c.workspace_id == actor.memberships[0].workspace_id,
                )
                .values(role="viewer")
            )
    with pytest.raises(ServiceError):
        await store.work_read(lambda conn: source.verify(conn, run.id))
    persisted = await store.snapshot(run.id)
    assert isinstance(persisted.session_context, DevelopmentSessionContext)
    await store.finish(run.id, "failed", "permission_denied")
    assert (await session_sql.get_turn(store, snapshot.session_context.turn_id)).status == "failed"


async def test_v3_paging_rejects_changed_transcript(development_session):
    fixture, store = development_session, development_session[0]
    turn, prior = await _start_turn(fixture, "prior", "prior", 1)
    await store.finish(prior.id, "completed")
    _, current = await _start_turn(fixture, "go", "current", 2)
    snapshot = await store.snapshot(current.id)
    source = DevelopmentCompactionSource(snapshot.session_context.frozen)
    await _store_transcript(store, turn.id, "changed after the anchor was frozen")
    with pytest.raises(ServiceError, match="facts_corrupt"):
        await store.work_read(lambda conn: source.page(conn, current.id, 0, 16))


async def _history_service(fixture, tmp_path, *, approval=False):
    """Real RunService/SDK/SQLite; only model responses and tools are fixtures."""
    from krim_agent.contracts import parse_json
    from krim_agent.development.contracts import EnvironmentHandle, RunOwnership
    from krim_agent.development.ports import EnvironmentRecord
    from krim_agent.runtime.sdk_loop import DevelopmentRunScope, DevelopmentSDKLoop
    from krim_agent.runtime.service import RunBinding, RunService
    from krim_agent.storage.development import DevelopmentStore
    from krim_agent.storage.models import sessions
    from tests.coding_agent.sdk_fixtures import setup
    from tests.coding_agent.test_sdk_context_compaction import Script

    store, _, _, actor, project, session, settings, _, _ = fixture
    for index in range(8):
        turn, prior = await _start_turn(fixture, f"prior {index}", f"prior-{index}", index + 1)
        await store.finish(prior.id, "completed")
        async with store.lock, store.engine.begin() as conn:
            await conn.execute(
                update(turns)
                .where(turns.c.id == turn.id)
                .values(
                    transcript=[
                        Message(role="user", content=f"SENTINEL={index} " + "x" * 600).model_dump(
                            mode="json"
                        ),
                        Message(role="assistant", content="y" * 600).model_dump(mode="json"),
                    ]
                )
            )
    environment = EnvironmentHandle(
        owner_id=actor.principal_id,
        project_id=project["project_id"],
        environment_id=project["environment_id"],
        generation=1,
        state="ready",
    )
    content = tmp_path / "history-project"
    content.mkdir()
    info = content.stat()
    repositories = DevelopmentStore(store)
    await repositories.environments.register(
        EnvironmentRecord(environment, "sha256:" + "a" * 64, content, info.st_dev, info.st_ino)
    )
    harness, provider, cleanup = setup(approval=approval), Script(write=True), []
    executor = harness.adapter().executor

    async def scope(run_id, check, budget):
        check()
        return DevelopmentRunScope(
            environment,
            RunOwnership(owner_id=actor.principal_id, session_id=session.id, run_id=run_id),
            None,
        )

    async def close_run(run_id):
        cleanup.append(run_id)

    loop = DevelopmentSDKLoop(
        store,
        provider,
        executor,
        settings,
        resolve_scope=scope,
        repositories=repositories,
        cleanup_run=close_run,
        descriptor={"version": "v3-test/1"},
    )
    binding = RunBinding.freeze("target-development", loop, settings)
    target = parse_json(binding.descriptor_json)
    async with store.lock, store.engine.begin() as conn:
        await conn.execute(
            update(sessions).where(sessions.c.id == session.id).values(target=target)
        )
    service = RunService(store, loop, settings, target, {"target-development": binding})
    turn = await session_sql.submit(
        store,
        session.id,
        TurnRequest(text="current task", request_key="sdk-current", expected_revision=9),
        settings,
    )
    return service, harness, provider, turn, cleanup


async def test_v3_real_service_compacts_and_reuses_summary_on_next_turn(
    development_session, tmp_path
):
    from krim_agent.storage.models import compactions

    fixture, store = development_session, development_session[0]
    service, harness, provider, turn, cleanup = await _history_service(fixture, tmp_path)
    try:
        run = await service.start_turn(turn.id)
        result = await service.wait(run.id)
        assert result.status == "completed", result.code
        assert harness.counts["write"] == 1
        snapshot = await store.snapshot(run.id)
        starts = [event for event in snapshot.events if event.kind == "model_start"]
        assert [event.data.get("purpose") for event in starts] == [
            "summary",
            "summary",
            "task",
            "task",
        ]
        assert [event.data["step_index"] for event in starts] == [0, 1, 2, 3]
        assert len(provider.requests) == 4
        assert snapshot.session_context.prepared is not None
        transcript = await store.work_read(
            lambda conn: conn.scalar(select(turns.c.transcript).where(turns.c.id == turn.id))
        )
        assert "SENTINEL=42" not in json.dumps(transcript)
        assert "finished" in json.dumps(transcript)
        rows = await store.work_read(lambda conn: conn.execute(select(compactions)))
        assert len(rows.mappings().all()) == 2
        next_turn = await session_sql.submit(
            store,
            fixture[5].id,
            TurnRequest(text="continue", request_key="sdk-next", expected_revision=10),
            fixture[6],
        )
        next_run = await service.start_turn(next_turn.id)
        next_result = await service.wait(next_run.id)
        assert next_result.status == "completed", next_result.code
        after = await store.snapshot(next_run.id)
        assert all(
            event.data.get("purpose") != "summary"
            for event in after.events
            if event.kind == "model_start"
        )
        assert any(
            message.content.startswith("[不可信历史摘要]") for message in provider.requests[-1][0]
        )
        assert len(provider.requests) == 5
        assert cleanup == [run.id, next_run.id]
    finally:
        await service.close()


@pytest.mark.parametrize("revoke", [False, True])
async def test_v3_real_approval_resume_never_recharges_or_dispatches_after_revocation(
    development_session,
    revoke,
    tmp_path,
):
    from krim_agent.contracts import ApprovalReply
    from krim_agent.storage.development_models import project_access_records

    fixture, store = development_session, development_session[0]
    service, harness, provider, turn, cleanup = await _history_service(
        fixture, tmp_path, approval=True
    )
    try:
        run = await service.start_turn(turn.id)
        parked = await service.wait(run.id)
        assert parked.status == "awaiting", parked.code
        assert harness.counts["write"] == 0 and len(provider.requests) == 3
        request = (await service.approvals(run.id))[0]
        if revoke:
            async with store.lock, store.engine.begin() as conn:
                await conn.execute(
                    project_access_records.delete().where(
                        project_access_records.c.project_id == fixture[4]["project_id"],
                        project_access_records.c.kind == "grant",
                    )
                )
        await service.reply_approval(
            run.id,
            request["request_id"],
            ApprovalReply(
                request_key="approve",
                call_id=request["call_id"],
                tool_hash=request["tool_hash"],
                arguments_hash=request["arguments_hash"],
                allow=True,
            ),
        )
        result = await service.wait(run.id)
        assert result.status == ("failed" if revoke else "completed"), result.code
        assert harness.counts["write"] == (0 if revoke else 1)
        assert len(provider.requests) == (3 if revoke else 4)
        snapshot = await store.snapshot(run.id)
        assert (
            sum(
                event.kind == "model_start" and event.data.get("purpose") == "summary"
                for event in snapshot.events
            )
            == 2
        )
        assert sum(event.kind == "terminal" for event in snapshot.events) == 1
        assert cleanup == [run.id]
    finally:
        await service.close()


async def test_v3_revocation_at_model_start_prevents_provider_call(
    development_session,
    tmp_path,
    monkeypatch,
):
    from krim_agent.storage.development_models import project_access_records
    from krim_agent.storage.models import compactions

    fixture, store = development_session, development_session[0]
    service, harness, provider, turn, cleanup = await _history_service(fixture, tmp_path)
    original = type(store).append

    async def append_and_revoke(self, run_id, kind, data, **kwargs):
        event = await original(self, run_id, kind, data, **kwargs)
        if self is store and kind == "model_start":
            async with store.lock, store.engine.begin() as conn:
                await conn.execute(
                    project_access_records.delete().where(
                        project_access_records.c.project_id == fixture[4]["project_id"],
                        project_access_records.c.kind == "grant",
                    )
                )
        return event

    monkeypatch.setattr(type(store), "append", append_and_revoke)
    try:
        run = await service.start_turn(turn.id)
        result = await service.wait(run.id)
        assert result.status == "failed" and result.code == "permission_denied"
        assert not provider.requests and harness.counts["write"] == 0
        statuses = await store.work_read(
            lambda conn: conn.execute(
                select(compactions.c.status).where(compactions.c.run_id == run.id)
            )
        )
        assert list(statuses.scalars()) == ["failed"]
        assert cleanup == [run.id]
        assert (await store.snapshot(run.id)).session_context is not None
    finally:
        await service.close()
