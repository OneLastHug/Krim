"""真实原子Writer经原SQL Fence完成暂存/意图/双after，unknown同操作只核实。"""

import asyncio
import time
from dataclasses import replace

import pytest
from sqlalchemy import func, select
from test_directory_checkpoint_capture_store import capture_context as capture_context
from test_directory_restore_plans import baseline, request, target

import krim_agent.workspace_directory_restore_writer as physical
from krim_agent.persistence.database import (
    WorkspaceDirectoryRestoreAttemptRow,
    WorkspaceDirectoryRestoreLockRow,
)
from krim_agent.persistence.workspace_directory_captures import WorkspaceDirectoryCaptureStore
from krim_agent.persistence.workspace_directory_checkpoints import WorkspaceDirectoryCheckpointStore
from krim_agent.persistence.workspace_directory_restores import WorkspaceDirectoryRestoreStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_directory_restore import DirectoryRestoreExecute
from krim_agent.workspace_directory_restore_writer import WorkspaceDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


async def ready(context, **options):
    captures, scope, binding, _, _ = context
    identifier = await target(context)
    (binding.workspace.root / "tree/nested/nul").write_bytes(b"current\x80\0")
    store = WorkspaceDirectoryRestoreStore(captures, **options)
    started, _ = await store.reserve(scope, binding, request(identifier), qualify=lambda: None)
    before, planned = await baseline(context, store, started.plan_id)
    return store, scope, binding, before, planned


def execute_request(plan, key="execute"):
    return DirectoryRestoreExecute(
        plan_id=plan.plan_id, expected_revision=plan.revision, request_key=key
    )


def bridge(loop, operation):
    def call(*args):
        return asyncio.run_coroutine_threadsafe(operation(*args), loop).result(timeout=5)

    return call


async def prepared(store, scope, binding, planned, *, model_source=None, key="execute"):
    wanted = execute_request(planned, key)
    operation, created = await store.start(
        scope, binding, wanted, model_source=model_source, qualify=lambda: None
    )
    assert created and operation.status == "running" and not operation.intent_recorded
    fence = await store.execution(scope, operation.operation_id)
    wanted_tree, before, token = await store.inputs(scope, fence, binding, qualify=lambda: None)
    writer = WorkspaceDirectoryRestoreWriter(binding.workspace)
    loop = asyncio.get_running_loop()
    authorize = bridge(
        loop, lambda: store.assert_execution(scope, fence, binding, qualify=lambda: None)
    )
    record = bridge(
        loop, lambda value: store.record_stage(scope, fence, binding, value, qualify=lambda: None)
    )
    change = await asyncio.to_thread(
        writer.prepare,
        wanted_tree,
        expected_before=before.fingerprint,
        token=token,
        authorize=authorize,
        record_stage=record,
    )
    await store.record_prepared(scope, fence, binding, change, qualify=lambda: None)
    return operation, fence, writer, change, authorize


async def apply(store, scope, binding, fence, writer, change, authorize):
    loop = asyncio.get_running_loop()
    record = bridge(
        loop, lambda value: store.record_intent(scope, fence, binding, value, qualify=lambda: None)
    )
    return await asyncio.to_thread(writer.apply, change, authorize=authorize, record_intent=record)


async def lock_count(store):
    async with store.database.sessions() as sql:
        return await sql.scalar(select(func.count()).select_from(WorkspaceDirectoryRestoreLockRow))


async def test_same_ledger_real_complete_atomic_restore_backup_and_all_cold_queries_zero_replay(
    capture_context, monkeypatch
):
    store, scope, binding, before, planned = await ready(capture_context)
    op, fence, writer, change, authorize = await prepared(store, scope, binding, planned)
    assert await lock_count(store) > 0
    assert WorkspaceTreeSnapshotReader(binding.workspace).capture("tree") == before
    effect = await apply(store, scope, binding, fence, writer, change, authorize)
    assert effect.receipt.status == "done", effect
    finished = await store.settle(scope, fence, binding, effect, qualify=lambda: None)
    assert finished.phase == "finished" and finished.receipt.status == "done"
    assert finished.operation_id == op.operation_id and await lock_count(store) == 0
    assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"\0\xff"
    assert effect.retained_before.entries[-1].file is not None
    before_queries = await store.operation(scope, op.operation_id)

    def forbidden(*_args, **_kwargs):
        pytest.fail("冷事实不能重新调用原生产者")

    monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
    monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "prepare", forbidden)
    monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "apply", forbidden)
    binding.revoked.set()
    assert await store.get(scope, planned.plan_id) == finished
    assert await store.by_key(scope, planned.request_key) == finished
    assert await store.operation_by_key(scope, "execute") == before_queries
    assert await store.list(scope) == [finished]
    assert await store.start(scope, binding, execute_request(planned), qualify=forbidden) == (
        before_queries,
        False,
    )
    assert await store.cancel(scope, planned.plan_id) == finished
    with pytest.raises(ServiceError):
        await store.execution(scope, op.operation_id)


async def test_namespace_lost_reply_new_owner_verifies_same_operation_without_second_writer(
    capture_context, monkeypatch
):
    store, scope, binding, _, planned = await ready(capture_context)
    op, fence, writer, change, authorize = await prepared(store, scope, binding, planned)
    rename, calls = physical.rename_at, []

    def lost(*args, **kwargs):
        rename(*args, **kwargs)
        calls.append(1)
        raise OSError("原namespace后丢回程")

    monkeypatch.setattr(physical, "rename_at", lost)
    effect = await apply(store, scope, binding, fence, writer, change, authorize)
    assert effect.receipt.status == "uncertain" and len(calls) == 1
    unknown = await store.settle(scope, fence, binding, effect, qualify=lambda: None)
    assert unknown.phase == "uncertain" and await lock_count(store) > 0
    cold = WorkspaceDirectoryRestoreStore(
        WorkspaceDirectoryCaptureStore(
            WorkspaceDirectoryCheckpointStore(WorkspaceEditStore(store.database))
        )
    )
    current, verify = await cold.claim_verify(scope, op.operation_id, binding, qualify=lambda: None)
    assert current.operation_id == op.operation_id and verify.kind == "verify"
    with pytest.raises(ServiceError):
        await store.record_intent(scope, fence, binding, change, qualify=lambda: None)
    with pytest.raises(ServiceError):
        await cold.record_intent(scope, verify, binding, change, qualify=lambda: None)
    actual = await cold.verification_input(scope, verify, binding, qualify=lambda: None)

    def forbidden(*_args, **_kwargs):
        pytest.fail("核实不能重新prepare/apply/namespace")

    monkeypatch.setattr(physical, "rename_at", forbidden)
    monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "prepare", forbidden)
    monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "apply", forbidden)
    result = await asyncio.to_thread(
        WorkspaceDirectoryRestoreWriter(binding.workspace).reconcile, actual
    )
    assert result.receipt.status == "done"
    finished = await cold.settle(scope, verify, binding, result, qualify=lambda: None)
    assert finished.phase == "finished" and finished.operation_id == op.operation_id
    assert await lock_count(cold) == 0 and len(calls) == 1


@pytest.mark.parametrize("after_intent", [False, True])
async def test_cancel_and_expired_owner_close_original_admission_without_replay(
    capture_context, after_intent
):
    store, scope, binding, before, planned = await ready(capture_context)
    op, fence, writer, change, _authorize = await prepared(store, scope, binding, planned)
    if after_intent:
        await store.record_intent(scope, fence, binding, change, qualify=lambda: None)
    cancelled = await store.cancel(scope, planned.plan_id)
    assert cancelled.phase == ("uncertain" if after_intent else "cancelled")
    assert cancelled.cancel_requested
    assert bool(await lock_count(store)) is after_intent
    with pytest.raises(ServiceError):
        await store.assert_execution(scope, fence, binding, qualify=lambda: None)
    assert WorkspaceTreeSnapshotReader(binding.workspace).capture("tree") == before


@pytest.mark.parametrize("part", ["before", "stage", "token"])
async def test_prepared_material_cannot_switch_original_backup_target_or_operation(
    capture_context, part
):
    store, scope, binding, _, planned = await ready(capture_context)
    op, fence, writer, change, _authorize = await prepared(store, scope, binding, planned)
    if part == "before":
        bad = replace(change, before=change.staged)
    elif part == "stage":
        bad = replace(change, stage=replace(change.stage, token="0" * 32))
    else:
        bad = replace(change, target_fingerprint="a" * 64)
    with pytest.raises(ServiceError):
        await store.record_prepared(scope, fence, binding, bad, qualify=lambda: None)
    assert not (await store.operation(scope, op.operation_id)).intent_recorded


@pytest.mark.parametrize("quota", ["bytes", "entries", "material"])
async def test_quota_is_decided_before_namespace_and_reservation_is_persistent(
    capture_context, quota
):
    options = (
        {"max_staging_bytes": 0}
        if quota == "bytes"
        else {"max_staging_entries": 1}
        if quota == "entries"
        else {"max_snapshot_bytes": 1024}
    )
    store, scope, binding, before, planned = await ready(capture_context, **options)
    if quota != "material":
        with pytest.raises(ServiceError) as denied:
            await store.start(scope, binding, execute_request(planned), qualify=lambda: None)
        assert denied.value.status == 429
        assert (await store.get(scope, planned.plan_id)).phase == "ready"
    else:
        with pytest.raises(ServiceError) as denied:
            await prepared(store, scope, binding, planned)
        assert denied.value.code == "workspace_directory_restore_capacity"
    assert WorkspaceTreeSnapshotReader(binding.workspace).capture("tree") == before


async def test_original_execute_revision_key_does_not_rebind_second_dispatch(capture_context):
    store, scope, binding, _, planned = await ready(capture_context)
    first, made = await store.start(scope, binding, execute_request(planned), qualify=lambda: None)
    assert made
    assert await store.start(scope, binding, execute_request(planned), qualify=lambda: None) == (
        first,
        False,
    )
    with pytest.raises(ServiceError):
        await store.start(
            scope, binding, execute_request(planned, "different"), qualify=lambda: None
        )
    with pytest.raises(ServiceError):
        await store.start(
            scope,
            binding,
            execute_request(planned).model_copy(update={"expected_revision": planned.revision + 1}),
            qualify=lambda: None,
        )


@pytest.mark.parametrize("intent", [False, True])
async def test_restart_projects_expired_execution_same_operation_and_zero_producers(
    capture_context, monkeypatch, intent
):
    store, scope, binding, before, planned = await ready(capture_context)
    op, fence, writer, change, _authorize = await prepared(store, scope, binding, planned)
    if intent:
        await store.record_intent(scope, fence, binding, change, qualify=lambda: None)
    async with store.database.sessions() as sql, sql.begin():
        row = await sql.get(WorkspaceDirectoryRestoreAttemptRow, op.operation_id)
        row.lease_expires_at = time.time() - 1

    def forbidden(*_args, **_kwargs):
        pytest.fail("过期投影不能重放Reader/Writer")

    monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "prepare", forbidden)
    monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "apply", forbidden)
    cold = WorkspaceDirectoryRestoreStore(store.captures)
    assert await cold.recover_interrupted() == 1
    state = await cold.get(scope, planned.plan_id)
    assert (
        state.phase == ("uncertain" if intent else "interrupted")
        and state.operation_id == op.operation_id
    )
    assert await cold.recover_interrupted() == 0
    with pytest.raises(ServiceError):
        await store.renew(scope, fence, binding, qualify=lambda: None)
