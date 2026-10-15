"""原生产Run结束后，显式核实仍保留原来源，只授予新调用只读核实资格。"""

import asyncio
from dataclasses import replace

import pytest
from sqlalchemy import delete, select
from test_directory_restore_execution import apply, prepared
from test_directory_restore_guards import model_target
from test_directory_restore_plans import request
from test_model_directory_checkpoint_store import context as context

import krim_agent.workspace_directory_restore_writer as physical
from krim_agent.persistence.database import (
    RunRow,
    WorkspaceDirectoryRestoreAttemptRow,
    WorkspaceDirectoryRestoreModelSourceRow,
)
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.workspace_checkpoint import CheckpointModelSource, WorkspaceCheckpointCreate
from krim_agent.workspace_directory_checkpoint import DirectoryCheckpointMaterialCreate
from krim_agent.workspace_directory_restore_writer import WorkspaceDirectoryRestoreWriter
from krim_agent.workspace_restore import CheckpointRestorePreview
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader
from krim_agent.workspace_writer import WorkspaceWriter


async def model_ready(context):
    _, _, binding, scope, source, _ = context
    store, identifier = await model_target(context)
    planned, _ = await store.reserve(
        scope, binding, request(identifier), model_source=source, qualify=lambda: None
    )
    fence = await store.preparation(scope, planned.plan_id)
    snapshot = WorkspaceTreeSnapshotReader(binding.workspace).capture("tree")
    await store.record_baseline(scope, fence, binding, snapshot, qualify=lambda: None)
    return store, await store.ready(scope, fence, binding, qualify=lambda: None)


async def test_new_run_verifies_same_unknown_operation_after_original_run_closes(
    context, runtime, monkeypatch
):
    service, agent, binding, scope, preview_source, _ = context
    store, planned = await model_ready(context)
    execute_source = replace(preview_source, call_id="execute-original")
    op, fence, writer, change, authorize = await prepared(
        store, scope, binding, planned, model_source=execute_source
    )
    rename = physical.rename_at

    def lost(*args, **kwargs):
        rename(*args, **kwargs)
        raise OSError("原发布已发生，尚无可信回执")

    monkeypatch.setattr(physical, "rename_at", lost)
    effect = await apply(store, scope, binding, fence, writer, change, authorize)
    await store.settle(scope, fence, binding, effect, qualify=lambda: None)
    async with store.database.sessions() as sql, sql.begin():
        run = await sql.get(RunRow, preview_source.run_id)
        run.status, run.stop_requested = "aborted", True
    assert (await store.get(scope, planned.plan_id)).phase == "uncertain"
    run, _ = await runtime.repository.begin_run(
        service._handle(agent.agent_id).topic_id,
        ChatRequest(text="新调用只核实原操作", idempotency_key="verify-new-run"),
        agent_session_id=agent.session_id,
        agent_id=agent.agent_id,
        agent_owner_id=service.manager.owner_id,
    )
    source = CheckpointModelSource(run.id, "verify-original-operation")
    _, verification = await store.claim_verify(
        scope, op.operation_id, binding, model_source=source, qualify=lambda: None
    )
    assert verification.model_source == source and verification.kind == "verify"
    with pytest.raises(ServiceError):
        await store.record_intent(scope, verification, binding, change, qualify=lambda: None)
    actual = await store.verification_input(scope, verification, binding, qualify=lambda: None)

    def forbidden(*_args, **_kwargs):
        pytest.fail("新的核实调用不能重新准备或发布")

    monkeypatch.setattr(physical, "rename_at", forbidden)
    monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "prepare", forbidden)
    monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "apply", forbidden)
    actual_effect = await asyncio.to_thread(writer.reconcile, actual)
    result = await store.settle(scope, verification, binding, actual_effect, qualify=lambda: None)
    assert result.phase == "finished" and result.operation_id == op.operation_id
    async with store.database.sessions() as sql:
        origins = list(await sql.scalars(select(WorkspaceDirectoryRestoreModelSourceRow)))
        assert {(item.phase, item.run_id, item.call_id) for item in origins} == {
            ("preview", preview_source.run_id, preview_source.call_id),
            ("execute", execute_source.run_id, execute_source.call_id),
            ("verify", source.run_id, source.call_id),
        }
        attempt = await sql.get(WorkspaceDirectoryRestoreAttemptRow, op.operation_id)
        assert attempt.verification_count == attempt.verification_model_count == 1
    assert (await store.claim_verify(scope, op.operation_id, binding, qualify=forbidden))[1] is None


@pytest.mark.parametrize("fault", ["source", "source_and_marker", "counter"])
async def test_verify_source_corruption_never_downgrades_to_host(context, fault):
    _, _, binding, scope, source, _ = context
    store, planned = await model_ready(context)
    op, fence, _, change, _ = await prepared(
        store, scope, binding, planned, model_source=replace(source, call_id="execute")
    )
    await store.record_intent(scope, fence, binding, change, qualify=lambda: None)
    await store.fail(scope, fence, "unknown_original_dispatch")
    verify_source = replace(source, call_id="verify")
    await store.claim_verify(
        scope, op.operation_id, binding, model_source=verify_source, qualify=lambda: None
    )
    async with store.database.sessions() as sql, sql.begin():
        attempt = await sql.get(WorkspaceDirectoryRestoreAttemptRow, op.operation_id)
        if fault == "counter":
            attempt.verification_count += 1
        else:
            await sql.execute(
                delete(WorkspaceDirectoryRestoreModelSourceRow).where(
                    WorkspaceDirectoryRestoreModelSourceRow.phase == "verify"
                )
            )
            if fault == "source_and_marker":
                attempt.verify_model_bound = False
                attempt.verification_run_id = attempt.verification_call_id = None
    with pytest.raises(ServiceError) as denied:
        await store.get(scope, planned.plan_id)
    assert denied.value.code == "workspace_directory_restore_corrupt"


@pytest.mark.parametrize("producer", ["directory", "file", "restore"])
async def test_old_producer_original_call_cannot_admit_new_directory_restore(context, producer):
    service, _, binding, scope, source, _ = context
    store, identifier = await model_target(context)
    if producer == "directory":
        await store.captures.reserve(
            scope,
            binding,
            DirectoryCheckpointMaterialCreate(root_path="tree", request_key="older-directory"),
            model_source=source,
        )
    else:
        files = service.workspace_checkpoint_store
        file, _ = await files.reserve(
            scope,
            WorkspaceCheckpointCreate(paths=["tree/a.txt"], request_key="older-file"),
            binding.key,
            binding.fingerprint,
            binding.workspace.fingerprint,
            model_source=source if producer == "file" else None,
        )
        if producer == "restore":
            fence = (await files.execution(scope, file.checkpoint_id))[0]
            await files.record(
                scope, fence, WorkspaceWriter(binding.workspace).capture_file("tree/a.txt")
            )
            await files.finish(scope, fence)
            await service.workspace_restore_store.reserve(
                scope,
                CheckpointRestorePreview(
                    checkpoint_id=file.checkpoint_id, request_key="older-restore"
                ),
                binding.key,
                binding.fingerprint,
                binding.workspace.fingerprint,
                model_source=source,
            )
    with pytest.raises(ServiceError) as denied:
        await store.reserve(
            scope, binding, request(identifier), model_source=source, qualify=lambda: None
        )
    assert denied.value.code == "workspace_directory_restore_model_call_conflict"
