import asyncio
from dataclasses import replace
from uuid import uuid4

import pytest
from alembic import command
from sqlalchemy import select, text, update

from krim_agent.execution import CommandSpec, ExecutionArtifact, ExecutionResult, SandboxLimits
from krim_agent.execution.records import CommandRequest, CommandResource
from krim_agent.persistence import Database
from krim_agent.persistence.command_models import CommandFileRow, CommandRow
from krim_agent.persistence.commands import CommandStore
from krim_agent.persistence.database import AgentSessionRow
from krim_agent.persistence.migration import migration_config
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.protocol.models import utc_now

RESOURCE = CommandResource(key="test", fingerprint="a" * 64)
LIMITS = SandboxLimits(file_bytes=1024, artifact_bytes=1024, output_bytes=1024)


def request(key="one", code="pass"):
    return CommandRequest(
        request_key=key, command=CommandSpec(argv=["/usr/bin/python3", "-c", code])
    )


def receipt(identifier, *, dispatched=True, started=True, stdout=b"ok", artifacts=()):
    return ExecutionResult(
        identifier,
        "completed",
        dispatched,
        started,
        0,
        stdout,
        b"",
        False,
        artifacts,
        True,
        0,
        None,
        0.1,
    )


@pytest.fixture
async def ledger(repository):
    topic = await repository.create_topic("commands")
    return CommandStore(repository.database), "topic:" + topic.id


@pytest.mark.parametrize(
    "change", ["request", "input", "mode", "resource", "version", "directory", "limits"]
)
async def test_original_key_freezes_every_execution_input(ledger, change):
    from krim_agent.execution import SandboxFile

    store, scope = ledger
    original = request()
    first = await store.reserve(scope, original, RESOURCE, LIMITS, {"a": b"original"})
    original.command.argv[-1] = "changed by caller"
    again = await store.reserve(scope, request(), RESOURCE, LIMITS, {"a": b"original"})
    assert again.token is None and first.view.command_id == again.view.command_id
    assert again.view.request.command.argv[-1] == "pass"
    inputs = {"a": b"changed" if change == "input" else b"original"}
    if change == "mode":
        inputs["a"] = SandboxFile(b"original", 0o600)
    with pytest.raises(ServiceError) as conflict:
        await store.reserve(
            scope,
            request(code="other" if change == "request" else "pass"),
            RESOURCE.model_copy(update={"fingerprint": "b" * 64})
            if change == "resource"
            else RESOURCE.model_copy(update={"version": "2"})
            if change == "version"
            else RESOURCE,
            LIMITS.model_copy(update={"wall_seconds": 10}) if change == "limits" else LIMITS,
            inputs,
            directories=("new",) if change == "directory" else (),
        )
    assert conflict.value.code == "execution_key_conflict"
    assert (await store.files(scope, first.view.command_id, "input"))["a"].data == b"original"
    assert "original" not in first.view.model_dump_json()


async def test_two_connections_admit_one_producer_and_preserve_scope(ledger, repository):
    store, scope = ledger
    second_db = Database(str(repository.database.engine.url))
    second = CommandStore(second_db)
    try:
        results = await asyncio.gather(
            *[selected.reserve(scope, request(), RESOURCE, LIMITS) for selected in (store, second)]
        )
        assert len({item.view.command_id for item in results}) == 1
        assert sum(item.token is not None for item in results) == 1
        other = await repository.create_topic("other")
        identifier = results[0].view.command_id
        for operation in (second.get, second.cancel, second.output):
            with pytest.raises(ServiceError) as hidden:
                await operation("topic:" + other.id, identifier)
            assert hidden.value.code == "execution_not_found"
    finally:
        await second_db.close()


async def test_run_call_binding_rejects_other_runs_duplicate_calls_and_stopped_runs(
    ledger, repository
):
    store, scope = ledger
    topic = scope.removeprefix("topic:")
    run, _ = await repository.begin_run(topic, ChatRequest(text="test", idempotency_key="run"))
    first = await store.reserve(scope, request(), RESOURCE, LIMITS, run_id=run.id, call_id="call")
    with pytest.raises(ServiceError) as duplicate:
        await store.reserve(scope, request("two"), RESOURCE, LIMITS, run_id=run.id, call_id="call")
    assert duplicate.value.code == "execution_call_conflict"
    other = await repository.create_topic("other")
    with pytest.raises(ServiceError) as wrong:
        await store.reserve(
            "topic:" + other.id, request(), RESOURCE, LIMITS, run_id=run.id, call_id="different"
        )
    assert wrong.value.code == "execution_run_mismatch"
    await repository.finish(run.id, "completed", None, [])
    with pytest.raises(ServiceError) as closed:
        await store.authorize(scope, first.view.command_id, first.token)
    assert closed.value.code == "execution_run_closed"
    assert (
        await store.reserve(scope, request(), RESOURCE, LIMITS, run_id=run.id, call_id="call")
    ).token is None


@pytest.mark.parametrize("dispatched", [False, True])
async def test_expired_work_never_dispatches_again_and_late_facts_keep_interruption(
    ledger, dispatched
):
    store, scope = ledger
    first = await store.reserve(scope, request(), RESOURCE, LIMITS)
    identifier, token, sandbox = first.view.command_id, first.token, str(uuid4())
    await store.authorize(scope, identifier, token, sandbox_id=sandbox, dispatch=dispatched)
    async with store.database.sessions.begin() as session:
        await session.execute(
            update(CommandRow).where(CommandRow.command_id == identifier).values(lease_expires_at=0)
        )
    view = await store.get(scope, identifier)
    assert view.phase == "interrupted" and not view.processes_reaped
    assert view.outcome == ("uncertain" if dispatched else "setup_failed")
    with pytest.raises(ServiceError) as stale:
        await store.authorize(scope, identifier, token, dispatch=True)
    assert stale.value.code == "execution_lease_lost"
    assert (await store.reserve(scope, request(), RESOURCE, LIMITS)).token is None
    result = receipt(sandbox, dispatched=dispatched, started=dispatched)
    if not dispatched:
        result = replace(
            result, status="setup_failed", exit_code=None, stdout=b"", artifacts_complete=False
        )
    done = await store.finish(scope, identifier, token, result)
    assert done.phase == "finished" and done.processes_reaped and done.interrupted_at
    events = await store.events(scope, identifier)
    assert [entry["kind"] for entry in events][-2:] == ["interrupted", "finished"]
    assert (await store.finish(scope, identifier, token, result)).revision == done.revision
    with pytest.raises(ServiceError) as changed:
        await store.finish(scope, identifier, token, replace(result, stdout=b"changed"))
    assert changed.value.code == "execution_receipt_conflict"


async def test_output_prefix_storage_reservation_and_file_integrity(ledger):
    store, scope = ledger
    # 额外预留1024字节目录清单及256字节格式开销，不能漏记目录元数据。
    store.max_reserved_bytes = 3330
    first = await store.reserve(scope, request(), RESOURCE, LIMITS, {"a": b"x"})
    with pytest.raises(ServiceError) as full:
        await store.reserve(scope, request("two"), RESOURCE, LIMITS)
    assert full.value.code == "execution_storage_limit"
    identifier, token, sandbox = first.view.command_id, first.token, str(uuid4())
    await store.authorize(scope, identifier, token, sandbox_id=sandbox, dispatch=True)
    await store.progress(scope, identifier, token, b"a", b"", started=True)
    for output in (b"b", b"a" * 1025):
        with pytest.raises(ServiceError) as invalid:
            await store.progress(scope, identifier, token, output, b"", started=True)
        assert invalid.value.code == "execution_output_conflict"
    await store.finish(scope, identifier, token, receipt(sandbox, stdout=b"a"))
    assert (await store.reserve(scope, request("two"), RESOURCE, LIMITS)).token
    async with store.database.sessions.begin() as session:
        await session.execute(
            update(CommandFileRow)
            .where(CommandFileRow.command_id == identifier)
            .values(content=b"changed")
        )
    with pytest.raises(ServiceError) as corrupt:
        await store.files(scope, identifier, "input")
    assert corrupt.value.code == "execution_record_corrupt"


async def test_agent_scope_requires_frozen_resource_owner_and_open_session(ledger, repository):
    store, topic_scope = ledger
    identifier = str(uuid4())
    async with store.database.sessions.begin() as session:
        session.add(
            AgentSessionRow(
                session_id=identifier,
                agent_id=str(uuid4()),
                topic_id=topic_scope.removeprefix("topic:"),
                name="test",
                persistent=False,
                profile_revision=None,
                definition_hash="a" * 64,
                parent_session_id=None,
                status="open",
                revision=0,
                owner_id=store.owner_id,
                expires_at=None,
                created_at=utc_now(),
                closed_at=None,
                permissions={},
                turn=0,
                current_run_id=None,
            )
        )
    scope = "session:" + identifier
    with pytest.raises(ServiceError) as managed:
        await store.reserve(topic_scope, request(), RESOURCE, LIMITS)
    assert managed.value.code == "execution_scope_managed"
    with pytest.raises(ServiceError) as denied:
        await store.reserve(scope, request(), RESOURCE, LIMITS)
    assert denied.value.code == "execution_binding_changed"
    async with store.database.sessions.begin() as session:
        await session.execute(
            update(AgentSessionRow)
            .where(AgentSessionRow.session_id == identifier)
            .values(permissions={"execution": RESOURCE.model_dump()})
        )
    first = await store.reserve(scope, request(), RESOURCE, LIMITS)
    other = CommandStore(store.database)
    with pytest.raises(ServiceError) as owner:
        await other.authorize(scope, first.view.command_id, first.token)
    assert owner.value.code == "execution_lease_lost"
    async with store.database.sessions.begin() as session:
        await session.execute(
            update(AgentSessionRow)
            .where(AgentSessionRow.session_id == identifier)
            .values(status="closed")
        )
    with pytest.raises(ServiceError) as closed:
        await store.authorize(scope, first.view.command_id, first.token)
    assert closed.value.code == "agent_session_closed"
    assert (await other.by_key(scope, "one")).command_id == first.view.command_id


async def test_migration_refuses_loss_and_events_and_listing_are_bounded(ledger):
    store, scope = ledger
    identifiers = []
    for key in ("one", "two", "three"):
        identifiers.append(
            (await store.reserve(scope, request(key), RESOURCE, LIMITS)).view.command_id
        )
    assert [item.command_id for item in await store.list_commands(scope, limit=1)] == identifiers[
        :1
    ]
    assert [
        item.command_id for item in await store.list_commands(scope, after=identifiers[0], limit=1)
    ] == identifiers[1:2]
    events = await store.events(scope, identifiers[0], limit=1)
    assert len(events) == 1 and not await store.events(
        scope, identifiers[0], after=events[0]["sequence"]
    )
    with pytest.raises(RuntimeError, match="命令记录非空"):
        async with store.database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(
                    migration_config(sync), "0029_workspace_restore_plans"
                )
            )
    async with store.database.engine.connect() as connection:
        assert (
            await connection.execute(text("SELECT version_num FROM alembic_version"))
        ).scalar_one() == "0037_command_directory_sources"
        assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
    assert len(await store.list_commands(scope)) == 3


async def test_wrong_producer_and_artifact_rewrites_cannot_change_original_receipt(ledger):
    store, scope = ledger
    first = await store.reserve(scope, request(), RESOURCE, LIMITS)
    sandbox = str(uuid4())
    await store.authorize(
        scope, first.view.command_id, first.token, sandbox_id=sandbox, dispatch=True
    )
    result = receipt(sandbox, artifacts=(ExecutionArtifact("file", b"original", 0o600),))
    with pytest.raises(ServiceError):
        await store.finish(scope, first.view.command_id, "wrong", result)
    await store.finish(scope, first.view.command_id, first.token, result)
    assert (await store.files(scope, first.view.command_id, "artifact"))["file"].data == b"original"
    async with store.database.sessions.begin() as session:
        row = await session.scalar(
            select(CommandRow).where(CommandRow.command_id == first.view.command_id)
        )
        row.stdout = b"corrupt"
    with pytest.raises(ServiceError) as output:
        await store.output(scope, first.view.command_id)
    assert output.value.code == "execution_record_corrupt"
