"""Real SQLite admission, ownership fencing and schema-14 preservation."""

import asyncio
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from krim_agent.accounts.service import IdentityService
from krim_agent.config import Settings
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import RunRequest, ServiceError
from krim_agent.development.contracts import (
    AuthorizationRef,
    CommandRequest,
    EnvironmentHandle,
    OperationIdentity,
    RunOwnership,
)
from krim_agent.development.ports import EnvironmentRecord, command_digest
from krim_agent.sandbox.configuration import SandboxConfiguration
from krim_agent.sandbox.service import SandboxService
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.development import DevelopmentStore
from krim_agent.storage.development_models import DEVELOPMENT_TABLES
from krim_agent.storage.preview_models import PREVIEW_TABLES
from krim_agent.storage.sidebar_models import SIDEBAR_TABLES
from krim_agent.storage.sqlite import SCHEMA_VERSION, SQLiteStore


async def prepare(
    path: Path, *, max_processes: int = 4, image_id: str | None = None,
    creating: bool = False, content_path: Path | None = None,
):
    store = await SQLiteStore.open(path)
    identities = await IdentityService.open(store)
    boot = await identities.bootstrap('storage test', request_key='boot')
    actor = identities.authenticate('Bearer ' + str(boot.token))
    sandbox = SandboxService(store, identities, SandboxConfiguration())
    project = await sandbox.create_project(actor, {
        'workspace_id': actor.memberships[0].workspace_id, 'title': 'development',
    }, request_key='project')
    settings = Settings()
    target = {'fixture': True, 'tools': []}
    session = await session_sql.create_session(
        store, actor.memberships[0].workspace_id, actor.principal_id,
        SessionCreate(request_key='session'), target, ContextPolicy(), settings,
    )
    turn = await session_sql.submit(store, session.id,
                                   TurnRequest(text='run', request_key='turn', expected_revision=0),
                                   settings)
    run, _ = await store.begin_turn_run(
        turn.id, RunRequest(text='run', request_key='ac-turn.' + turn.id), target, settings,
    )
    await store.set_running(run.id)
    assert (await store.snapshot(run.id)).session_context is not None
    environment = EnvironmentHandle(owner_id=actor.principal_id, project_id=project['project_id'],
                                    environment_id='dev-env', generation=1,
                                    state='creating' if creating else 'ready')
    ownership = RunOwnership(owner_id=actor.principal_id, session_id=session.id, run_id=run.id)
    content = content_path or path.parent / 'project-content'
    content.mkdir(exist_ok=True)
    info = content.stat()
    repositories = DevelopmentStore(store)
    record = EnvironmentRecord(environment, image_id or 'sha256:' + 'a' * 64, content,
                               info.st_dev, info.st_ino, max_processes=max_processes)
    await repositories.environments.register(record)
    request = CommandRequest(
        operation_id='command-1', environment=environment, ownership=ownership,
        authorization=AuthorizationRef(
            actor_id=actor.principal_id, actor_generation=actor.generation,
            project_id=project['project_id'], arguments_digest='a' * 64),
        command='printf ready',
    )
    await sandbox.close()
    await identities.close()
    return store, repositories, record, request


async def test_atomic_first_reservation_and_reopen_never_redispatch(tmp_path: Path) -> None:
    path = tmp_path / 'dev.sqlite'
    store, repositories, _, request = await prepare(path)
    results = await asyncio.gather(*[
        repositories.processes.reserve(request, command_digest(request)) for _ in range(6)
    ])
    assert sum(result.execute for result in results) == 1
    assert len({result.record.handle.process_id for result in results}) == 1
    await store.close()
    reopened = await SQLiteStore.open(path)
    try:
        again = await DevelopmentStore(reopened).processes.reserve(request, command_digest(request))
        assert again.execute is False
        assert again.record.handle.state == 'accepted'
    finally:
        await reopened.close()


async def test_request_drift_and_cross_session_run_are_rejected(tmp_path: Path) -> None:
    store, repositories, _, request = await prepare(tmp_path / 'dev.sqlite')
    try:
        await repositories.processes.reserve(request, command_digest(request))
        changed = request.model_copy(update={'command': 'printf changed'})
        with pytest.raises(ServiceError, match='key_conflict'):
            await repositories.processes.reserve(changed, command_digest(changed))
        changed = request.model_copy(update={
            'operation_id': 'command-2',
            'ownership': request.ownership.model_copy(update={'session_id': 'unrelated-session'}),
        })
        with pytest.raises(ServiceError, match='permission_denied'):
            await repositories.processes.reserve(changed, command_digest(changed))
    finally:
        await store.close()


async def test_environment_cas_and_stale_generation(tmp_path: Path) -> None:
    store, repositories, record, _ = await prepare(tmp_path / 'dev.sqlite')
    try:
        updated = replace(record, revision=2, container_id='container-1')
        assert await repositories.environments.compare_and_swap(record, updated) == updated
        with pytest.raises(ServiceError, match='revision_conflict'):
            await repositories.environments.compare_and_swap(record, updated)
        with pytest.raises(ServiceError, match='tool_binding_changed'):
            await repositories.environments.resolve(
                record.environment.model_copy(update={'generation': 2}))
    finally:
        await store.close()


async def test_sdk_operation_and_process_share_one_ledger(tmp_path: Path) -> None:
    store, repositories, _, request = await prepare(tmp_path / 'dev.sqlite')
    try:
        identity = OperationIdentity(operation_id=request.operation_id, call_id='host-call',
                                     environment=request.environment, ownership=request.ownership,
                                     request_digest=request.authorization.arguments_digest)
        assert (await repositories.operations.admit(identity)).execute
        await repositories.operations.bind_command(identity, request)
        reserved = await repositories.processes.reserve(request, command_digest(request))
        assert reserved.execute
        assert not (await repositories.operations.admit(identity)).execute
        assert not (await repositories.processes.reserve(request, command_digest(request))).execute
        assert await repositories.operations.lookup(identity) is not None
    finally:
        await store.close()


async def test_sdk_admission_cannot_be_reused_with_different_authorized_arguments(
    tmp_path: Path,
) -> None:
    store, repositories, _, request = await prepare(tmp_path / 'dev.sqlite')
    try:
        identity = OperationIdentity(
            operation_id=request.operation_id, call_id='host-call', environment=request.environment,
            ownership=request.ownership, request_digest='b' * 64,
        )
        await repositories.operations.admit(identity)
        with pytest.raises(ServiceError, match='key_conflict'):
            await repositories.processes.reserve(request, command_digest(request))
        await repositories.operations.bind_command(identity, request)
        changed = request.model_copy(update={'command': 'printf unauthorized'})
        with pytest.raises(ServiceError, match='key_conflict'):
            await repositories.operations.bind_command(identity, changed)
        with pytest.raises(ServiceError, match='key_conflict'):
            await repositories.processes.reserve(changed, command_digest(changed))
    finally:
        await store.close()


async def test_stopped_run_never_accepts_new_process(tmp_path: Path) -> None:
    store, repositories, _, request = await prepare(tmp_path / 'dev.sqlite')
    try:
        await store.request_stop(request.ownership.run_id)
        with pytest.raises(ServiceError, match='cancelled'):
            await repositories.processes.reserve(request, command_digest(request))
    finally:
        await store.close()


async def test_recreate_preserves_retired_binding_only_for_trusted_cleanup(tmp_path: Path) -> None:
    store, repositories, record, request = await prepare(tmp_path / 'dev.sqlite')
    try:
        reserved = (await repositories.processes.reserve(request, command_digest(request))).record
        stopped = replace(record, revision=2, container_id='old-container',
                          environment=record.environment.model_copy(update={'state': 'stopping'}))
        await repositories.environments.compare_and_swap(record, stopped)
        next_generation = await repositories.environments.begin_recreate(stopped)
        assert next_generation.environment.generation == 2
        assert await repositories.environments.previous(next_generation.environment) == stopped
        with pytest.raises(ServiceError, match='tool_binding_changed'):
            await repositories.environments.resolve(stopped.environment)
        old_rows = await repositories.processes.list_environment_for_cleanup(stopped.environment)
        assert old_rows == (reserved,)
        assert await repositories.processes.list_environment(next_generation.environment) == ()
        ready = replace(
            next_generation, revision=next_generation.revision + 1, container_id='new-container',
            environment=next_generation.environment.model_copy(update={'state': 'ready'}),
        )
        await repositories.environments.compare_and_swap(next_generation, ready)
        with pytest.raises(ServiceError, match='sandbox_cleanup_required'):
            await repositories.environments.finish_recreate(ready.environment)
        cancelled = replace(reserved, revision=2,
                            handle=reserved.handle.model_copy(update={'state': 'cancelled'}))
        await repositories.processes.save(reserved, cancelled)
        await repositories.environments.finish_recreate(ready.environment)
        with pytest.raises(ServiceError, match='not_found'):
            await repositories.processes.list_environment_for_cleanup(stopped.environment)
    finally:
        await store.close()


async def test_environment_stop_fence_blocks_new_process_admission(tmp_path: Path) -> None:
    store, repositories, record, request = await prepare(tmp_path / 'dev.sqlite')
    try:
        stopping = replace(record, revision=2,
                           environment=record.environment.model_copy(update={'state': 'stopping'}))
        await repositories.environments.compare_and_swap(record, stopping)
        with pytest.raises(ServiceError, match='workspace_busy'):
            await repositories.processes.reserve(request, command_digest(request))
    finally:
        await store.close()


async def test_process_capacity_is_atomic_and_unknown_keeps_its_slot(tmp_path: Path) -> None:
    store, repositories, _, request = await prepare(tmp_path / 'dev.sqlite', max_processes=1)
    try:
        second = request.model_copy(update={'operation_id': 'command-2'})
        results = await asyncio.gather(
            repositories.processes.reserve(request, command_digest(request)),
            repositories.processes.reserve(second, command_digest(second)),
            return_exceptions=True,
        )
        admitted = [r for r in results if not isinstance(r, BaseException)]
        denied = [r for r in results if isinstance(r, ServiceError)]
        assert len(admitted) == len(denied) == 1
        assert denied[0].code == 'capacity'
        record = admitted[0].record
        unknown = replace(record, revision=2,
                          handle=record.handle.model_copy(update={'state': 'unknown'}))
        await repositories.processes.save(record, unknown)
        third = request.model_copy(update={'operation_id': 'command-3'})
        with pytest.raises(ServiceError, match='capacity'):
            await repositories.processes.reserve(third, command_digest(third))
        cancelled = replace(unknown, revision=3,
                            handle=unknown.handle.model_copy(update={'state': 'cancelled'}))
        await repositories.processes.save(unknown, cancelled)
        assert (await repositories.processes.reserve(third, command_digest(third))).execute
    finally:
        await store.close()


async def test_unknown_process_cannot_be_reset_to_accepted(tmp_path: Path) -> None:
    store, repositories, _, request = await prepare(tmp_path / 'dev.sqlite')
    try:
        initial = (await repositories.processes.reserve(request, command_digest(request))).record
        unknown = replace(initial, revision=2,
                          handle=initial.handle.model_copy(update={'state': 'unknown'}))
        await repositories.processes.save(initial, unknown)
        reset = replace(unknown, revision=3,
                        handle=unknown.handle.model_copy(update={'state': 'accepted'}))
        with pytest.raises(ServiceError, match='key_conflict'):
            await repositories.processes.save(unknown, reset)
        saved = await repositories.processes.get(initial.handle.process_id)
        assert saved.handle.state == 'unknown'
        assert not (await repositories.processes.reserve(request, command_digest(request))).execute
    finally:
        await store.close()


async def test_corrupt_indexed_ownership_is_not_used_for_cleanup(tmp_path: Path) -> None:
    from sqlalchemy import update

    from krim_agent.storage.development_models import development_processes

    store, repositories, _, request = await prepare(tmp_path / 'dev.sqlite')
    try:
        initial = (await repositories.processes.reserve(request, command_digest(request))).record
        async with store.lock, store.engine.begin() as connection:
            await connection.execute(update(development_processes).where(
                development_processes.c.process_id == initial.handle.process_id,
            ).values(lifecycle='project_service'))
        with pytest.raises(ServiceError, match='facts_corrupt'):
            await repositories.processes.list_environment(request.environment)
    finally:
        await store.close()


@pytest.mark.parametrize('kind', ['operation', 'process'])
async def test_lost_commit_response_returns_existing_without_execution_permit(
    tmp_path: Path, monkeypatch, kind: str,
) -> None:
    store, repositories, _, request = await prepare(tmp_path / 'dev.sqlite')
    original = store._write
    writes = 0

    async def commit_then_disconnect(operation):
        nonlocal writes
        writes += 1
        await original(operation)
        raise OSError('commit response lost')

    monkeypatch.setattr(store, '_write', commit_then_disconnect)
    try:
        if kind == 'operation':
            identity = OperationIdentity(
                operation_id=request.operation_id, call_id='host-call',
                environment=request.environment, ownership=request.ownership,
                request_digest=request.authorization.arguments_digest,
            )
            result = await repositories.operations.admit(identity)
            assert result.receipt.state == 'accepted'
        else:
            result = await repositories.processes.reserve(request, command_digest(request))
            assert result.record.handle.state == 'accepted'
        assert result.execute is False
        assert writes == 1
        assert store.healthy
    finally:
        await store.close()


async def test_schema14_readonly_and_upgrade_preserve_original_rows(tmp_path: Path) -> None:
    path = tmp_path / 'legacy.sqlite'
    store = await SQLiteStore.open(path)
    run, _ = await store.begin(RunRequest(text='legacy', request_key='legacy'), {})
    await store.close()
    with sqlite3.connect(path) as connection:
        for table in reversed((*DEVELOPMENT_TABLES, *PREVIEW_TABLES, *SIDEBAR_TABLES)):
            connection.execute(f'DROP TABLE "{table.name}"')
        connection.execute('PRAGMA user_version=14')
    original = path.read_bytes()
    reader = await SQLiteStore.open(path, readonly=True)
    try:
        assert reader.schema_version == 14
        assert (await reader.snapshot(run.id)).request.text == 'legacy'
        with pytest.raises(ServiceError, match='schema_upgrade_required'):
            DevelopmentStore(reader).require()
    finally:
        await reader.close()
    assert path.read_bytes() == original
    writer = await SQLiteStore.open(path)
    try:
        assert writer.schema_version == SCHEMA_VERSION == 17
        assert (await writer.snapshot(run.id)).request.text == 'legacy'
    finally:
        await writer.close()
