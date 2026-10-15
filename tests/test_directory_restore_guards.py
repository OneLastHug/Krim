"""最后SQL资格/旧token/原命名空间锁和原Source标记的拒绝证据。"""

import asyncio
import time
from dataclasses import replace

import pytest
from sqlalchemy import delete, select
from test_directory_checkpoint_capture_store import capture_context as capture_context
from test_directory_restore_execution import execute_request, prepared, ready
from test_directory_restore_plans import request
from test_model_directory_checkpoint_store import context as context

from krim_agent.persistence.database import (
    WorkspaceDirectoryRestoreAttemptRow,
    WorkspaceDirectoryRestoreLockRow,
    WorkspaceDirectoryRestoreModelSourceRow,
    WorkspaceDirectoryRestorePlanRow,
)
from krim_agent.persistence.workspace_directories import WorkspaceDirectoryStore
from krim_agent.persistence.workspace_directory_restores import WorkspaceDirectoryRestoreStore
from krim_agent.persistence.workspace_restore_sources import admit_restore_source
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_change import FileChangeSpec, WorkspaceChangeAction
from krim_agent.workspace_checkpoint import WorkspaceCheckpointCreate
from krim_agent.workspace_directory import (
    DirectoryChangeAction,
    DirectoryChangeSpec,
    WorkspaceDirectoryWriter,
)
from krim_agent.workspace_directory_checkpoint import DirectoryCheckpointMaterialCreate
from krim_agent.workspace_writer import WorkspaceWriter


@pytest.mark.parametrize("first", ["restore", "neighbor"])
@pytest.mark.parametrize("neighbor", ["file", "directory"])
async def test_physical_namespace_both_admission_orders_and_resource_aliases(
    capture_context, first, neighbor
):
    store, scope, binding, _, planned = await ready(capture_context)
    if neighbor == "file":
        adjacent = store.edits
        change = WorkspaceWriter(binding.workspace).prepare(
            FileChangeSpec(
                path="tree/nested/new.txt", operation="write", expected_sha256=None, content="new"
            )
        )
        view, _ = await adjacent.preview(
            scope, binding.key, binding.fingerprint, "neighbor", change
        )
        action = WorkspaceChangeAction(
            proposal_id=view.view.proposal_id,
            action="apply",
            expected_revision=0,
            request_key="neighbor",
        )
    else:
        adjacent = WorkspaceDirectoryStore(store.database, owner_id=store.owner_id)
        change = WorkspaceDirectoryWriter(binding.workspace).prepare(
            DirectoryChangeSpec(path="tree/nested/newdir", operation="create", mode=0o750)
        )
        view, _ = await adjacent.preview(
            scope, binding.key, binding.fingerprint, "neighbor", change
        )
        action = DirectoryChangeAction(
            proposal_id=view.view.proposal_id,
            action="apply",
            expected_revision=0,
            request_key="neighbor",
        )
    if first == "restore":
        await store.start(scope, binding, execute_request(planned), qualify=lambda: None)
        with pytest.raises(ServiceError) as denied:
            await adjacent.reserve(scope, action, binding.fingerprint, change)
        assert denied.value.code == "workspace_directory_restore_busy"
    else:
        await adjacent.reserve(scope, action, binding.fingerprint, change)
        with pytest.raises(ServiceError) as denied:
            await store.start(scope, binding, execute_request(planned), qualify=lambda: None)
        assert (
            denied.value.code
            == {"file": "workspace_file_busy", "directory": "workspace_directory_busy"}[neighbor]
        )
    assert not (binding.workspace.root / "tree/nested/new.txt").exists()
    assert not (binding.workspace.root / "tree/nested/newdir").exists()


async def test_lost_namespace_lock_is_not_silently_reacquired(capture_context):
    store, scope, binding, _, planned = await ready(capture_context)
    op, fence, _writer, change, _authorize = await prepared(store, scope, binding, planned)
    async with store.database.sessions() as sql, sql.begin():
        row = await sql.scalar(
            select(WorkspaceDirectoryRestoreLockRow).where(
                WorkspaceDirectoryRestoreLockRow.operation_id == op.operation_id
            )
        )
        await sql.delete(row)
    with pytest.raises(ServiceError) as denied:
        await store.record_intent(scope, fence, binding, change, qualify=lambda: None)
    assert denied.value.code == "workspace_directory_restore_lock_lost"
    assert not (await store.operation(scope, op.operation_id)).intent_recorded


@pytest.mark.parametrize("phase", ["prepare", "execute"])
async def test_renew_wait_crosses_original_lease_rolls_back_without_extension(
    capture_context, monkeypatch, phase
):
    store, scope, binding, _, planned = await ready(capture_context)
    if phase == "prepare":
        second, _ = await store.reserve(
            scope,
            binding,
            request(planned.target.identifier, "second-preview"),
            qualify=lambda: None,
        )
        fence = await store.preparation(scope, second.plan_id)
        model, identifier = WorkspaceDirectoryRestorePlanRow, second.plan_id
    else:
        op, _ = await store.start(scope, binding, execute_request(planned), qualify=lambda: None)
        fence = await store.execution(scope, op.operation_id)
        model, identifier = WorkspaceDirectoryRestoreAttemptRow, op.operation_id
    async with store.database.sessions() as sql, sql.begin():
        row = await sql.get(model, identifier)
        row.lease_expires_at = time.time() + 1
    flush = None
    # 只在实际最后flush进入后注入临近到期，避免前置SQL未到目标窗口就过期。
    from sqlalchemy.ext.asyncio import AsyncSession

    flush = AsyncSession.flush
    entered, original_lease = [], []

    async def delayed(session, *args, **kwargs):
        candidates = [
            row
            for row in session.identity_map.values()
            if isinstance(row, model) and (getattr(row, "plan_id", None) == fence.plan_id)
        ]
        if candidates and not entered:
            entered.append(True)
            # 到期时钟指向原已提交租约；不修改已提交DB中的租约。
            async with store.database.engine.connect() as connection:
                table = model.__table__
                key = table.c.plan_id if phase == "prepare" else table.c.operation_id
                original_lease.append(
                    (
                        await connection.execute(
                            select(table.c.lease_expires_at).where(key == identifier)
                        )
                    ).scalar_one()
                )
            await asyncio.sleep(max(0, original_lease[0] - time.time()) + 0.02)
        return await flush(session, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "flush", delayed)
    with pytest.raises(ServiceError) as denied:
        await store.renew(scope, fence, binding, qualify=lambda: None)
    assert denied.value.code == "workspace_directory_restore_fenced" and entered
    monkeypatch.setattr(AsyncSession, "flush", flush)
    async with store.database.sessions() as sql:
        row = await sql.get(model, identifier)
        assert row.lease_expires_at == original_lease[0] <= time.time()


async def model_target(context):
    service, _agent, binding, scope, _source, _request = context
    captures = service.directory_checkpoint_store
    original, _ = await captures.reserve(
        scope,
        binding,
        DirectoryCheckpointMaterialCreate(root_path="tree", request_key="host-target"),
    )
    fence = await captures.execution(scope, original.checkpoint_id)
    from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

    snapshot = WorkspaceTreeSnapshotReader(binding.workspace).capture("tree")
    result = await captures.record(scope, fence, binding, snapshot, qualify=lambda: None)
    await captures.finish(scope, fence, binding, result, qualify=lambda: None)
    return WorkspaceDirectoryRestoreStore(captures), original.checkpoint_id


async def test_model_preview_and_execute_original_call_cannot_be_lent_to_other_producers(
    context,
):
    service, _agent, binding, scope, source, _ = context
    store, identifier = await model_target(context)
    planned, _ = await store.reserve(
        scope, binding, request(identifier), model_source=source, qualify=lambda: None
    )
    fence = await store.preparation(scope, planned.plan_id)
    assert fence.model_source == source
    from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

    snapshot = WorkspaceTreeSnapshotReader(binding.workspace).capture("tree")
    with pytest.raises(ServiceError):
        await store.record_baseline(
            scope, replace(fence, model_source=None), binding, snapshot, qualify=lambda: None
        )
    await store.record_baseline(scope, fence, binding, snapshot, qualify=lambda: None)
    planned = await store.ready(scope, fence, binding, qualify=lambda: None)
    with pytest.raises(ServiceError):
        await store.start(
            scope, binding, execute_request(planned), model_source=source, qualify=lambda: None
        )
    for old in ("directory", "file", "restore"):
        with pytest.raises(ServiceError) as denied:
            if old == "directory":
                await service.directory_checkpoint_store.reserve(
                    scope,
                    binding,
                    DirectoryCheckpointMaterialCreate(root_path="tree", request_key="conflict"),
                    model_source=source,
                )
            elif old == "file":
                await service.workspace_checkpoint_store.reserve(
                    scope,
                    WorkspaceCheckpointCreate(paths=["tree/a.txt"], request_key="conflict"),
                    binding.key,
                    binding.fingerprint,
                    binding.workspace.fingerprint,
                    model_source=source,
                )
            else:
                async with store._transaction() as sql:
                    await admit_restore_source(sql, store.edits, scope, source)
        assert denied.value.code == "workspace_directory_restore_model_call_conflict"
    execute_source = replace(source, call_id="new-directory-execute")
    op, _ = await store.start(
        scope, binding, execute_request(planned), model_source=execute_source, qualify=lambda: None
    )
    assert (await store.execution(scope, op.operation_id)).model_source == execute_source
    with pytest.raises(ServiceError):
        await store.start(scope, binding, execute_request(planned), qualify=lambda: None)


@pytest.mark.parametrize("fault", ["missing", "flag_and_source", "changed_call"])
async def test_original_model_source_corruption_never_downgrades_to_host(context, fault):
    _service, _agent, binding, scope, source, _ = context
    store, identifier = await model_target(context)
    planned, _ = await store.reserve(
        scope, binding, request(identifier), model_source=source, qualify=lambda: None
    )
    async with store.database.sessions() as sql, sql.begin():
        if fault == "changed_call":
            row = await sql.get(
                WorkspaceDirectoryRestoreModelSourceRow, (source.run_id, source.call_id)
            )
            row.call_id = "tampered"
        else:
            await sql.execute(
                delete(WorkspaceDirectoryRestoreModelSourceRow).where(
                    WorkspaceDirectoryRestoreModelSourceRow.plan_id == planned.plan_id
                )
            )
            if fault == "flag_and_source":
                row = await sql.get(WorkspaceDirectoryRestorePlanRow, planned.plan_id)
                row.preview_model_bound = False
    with pytest.raises(ServiceError) as denied:
        await store.get(scope, planned.plan_id)
    assert denied.value.code == "workspace_directory_restore_corrupt"
