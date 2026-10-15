"""原模型目录来源与请求的同事务绑定/同调用互斥/损坏与丢回执。"""

import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import delete, event, func, select
from sqlalchemy.orm import Session
from test_directory_checkpoint_capture import make_tree
from test_model_directory_checkpoints import OneShotProvider, setup

from krim_agent.persistence.database import (
    NativeCheckpointModelSourceRow,
    RunRow,
    WorkspaceDirectoryCaptureModelSourceRow,
    WorkspaceDirectoryCaptureRow,
    WorkspaceDirectoryCheckpointRow,
    WorkspaceRestoreModelSourceRow,
)
from krim_agent.persistence.workspace_restore_sources import admit_restore_source
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.workspace_checkpoint import CheckpointModelSource, WorkspaceCheckpointCreate
from krim_agent.workspace_directory_checkpoint import DirectoryCheckpointMaterialCreate
from krim_agent.workspace_restore import CheckpointRestorePreview
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader
from krim_agent.workspace_writer import WorkspaceWriter


@pytest.fixture
async def context(runtime, tmp_path):
    make_tree(tmp_path)
    service, agent = await setup(runtime, tmp_path, OneShotProvider())
    handle = service._handle(agent.agent_id)
    run, _ = await runtime.repository.begin_run(
        handle.topic_id,
        ChatRequest(text="可信目录来源Store", idempotency_key="store"),
        agent_session_id=agent.session_id,
        agent_id=agent.agent_id,
        agent_owner_id=service.manager.owner_id,
    )
    binding = handle.definition.workspace
    source = CheckpointModelSource(run.id, "original-directory-source")
    request = DirectoryCheckpointMaterialCreate(root_path="tree", request_key="model-source")
    try:
        yield service, agent, binding, "session:" + agent.session_id, source, request
    finally:
        await service.close()


async def counts(service):
    async with service.directory_checkpoint_store.database.sessions() as sql:
        return [
            await sql.scalar(select(func.count()).select_from(table))
            for table in (
                WorkspaceDirectoryCaptureRow,
                WorkspaceDirectoryCaptureModelSourceRow,
                WorkspaceDirectoryCheckpointRow,
            )
        ]


async def test_source_insert_failure_rolls_back_original_job_and_source_together(context):
    service, _agent, binding, scope, source, request = context
    store = service.directory_checkpoint_store

    def fail(_conn, _cursor, statement, _parameters, _context, _many):
        if statement.startswith("INSERT INTO workspace_directory_capture_model_sources"):
            raise OSError("synthetic model source insert failure")

    event.listen(store.database.engine.sync_engine, "before_cursor_execute", fail)
    try:
        with pytest.raises(OSError, match="synthetic model source"):
            await store.reserve(scope, binding, request, model_source=source)
    finally:
        event.remove(store.database.engine.sync_engine, "before_cursor_execute", fail)
    assert await counts(service) == [0, 0, 0]
    assert store.database.engine.sync_engine.pool.checkedout() == 0


async def test_same_key_parent_source_conflicts_and_fence_cannot_drop_origin(context):
    service, _agent, binding, scope, source, request = context
    store = service.directory_checkpoint_store
    original, _ = await store.reserve(scope, binding, request, model_source=source)
    assert await store.source(scope, original.checkpoint_id) == source
    assert await store.reserve(scope, binding, request, model_source=source) == (original, False)
    with pytest.raises(ServiceError) as other_key:
        await store.reserve(
            scope,
            binding,
            request.model_copy(update={"request_key": "second"}),
            model_source=source,
        )
    assert other_key.value.code == "workspace_directory_capture_model_call_conflict"
    for selected in (None, replace(source, call_id="other")):
        with pytest.raises(ServiceError) as conflict:
            await store.reserve(scope, binding, request, model_source=selected)
        assert conflict.value.code == "workspace_directory_capture_model_conflict"
    fence = await store.execution(scope, original.checkpoint_id)
    assert fence.model_source == source
    with pytest.raises(ServiceError) as dropped:
        await store.assert_capture(scope, replace(fence, model_source=None), "tree")
    assert dropped.value.code == "workspace_directory_capture_model_context"
    await store.cancel(scope, original.checkpoint_id)


@pytest.mark.parametrize("fault", ["missing-source", "flag-and-source", "changed-call"])
async def test_source_or_flag_corruption_cannot_downgrade_to_host_or_change_parent(context, fault):
    service, _agent, binding, scope, source, request = context
    store = service.directory_checkpoint_store
    view, _ = await store.reserve(scope, binding, request, model_source=source)
    async with store.database.sessions() as sql, sql.begin():
        if fault in {"missing-source", "flag-and-source"}:
            await sql.execute(
                delete(WorkspaceDirectoryCaptureModelSourceRow).where(
                    WorkspaceDirectoryCaptureModelSourceRow.checkpoint_id == view.checkpoint_id
                )
            )
            if fault == "flag-and-source":
                row = await sql.get(WorkspaceDirectoryCaptureRow, view.checkpoint_id)
                row.model_bound = False
        else:
            row = await sql.get(WorkspaceDirectoryCaptureModelSourceRow, view.checkpoint_id)
            row.call_id = "changed-original"
    with pytest.raises(ServiceError) as corrupt:
        await store.get(scope, view.checkpoint_id)
    assert corrupt.value.code == "workspace_directory_capture_corrupt"


async def test_directory_file_and_restore_sources_cannot_claim_same_run_call(context):
    service, _agent, binding, scope, source, request = context
    directories, files = service.directory_checkpoint_store, service.workspace_checkpoint_store
    directory, _ = await directories.reserve(scope, binding, request, model_source=source)
    file_request = WorkspaceCheckpointCreate(
        paths=["tree/a.txt"], request_key="file-after-directory", capture_format="bytes"
    )
    with pytest.raises(ServiceError) as file_conflict:
        await files.reserve(
            scope,
            file_request,
            binding.key,
            binding.fingerprint,
            binding.workspace.fingerprint,
            model_source=source,
        )
    assert file_conflict.value.code == "workspace_checkpoint_model_call_conflict"
    with pytest.raises(ServiceError) as restore_conflict:
        async with (
            directories.database.write_lock,
            directories.database.sessions() as sql,
            sql.begin(),
        ):
            await directories.edits._writer(sql)
            await admit_restore_source(sql, directories.edits, scope, source)
    assert restore_conflict.value.code == "workspace_restore_model_call_conflict"

    file_source = replace(source, call_id="file-first")
    file, _ = await files.reserve(
        scope,
        file_request,
        binding.key,
        binding.fingerprint,
        binding.workspace.fingerprint,
        model_source=file_source,
    )
    with pytest.raises(ServiceError) as directory_conflict:
        await directories.reserve(
            scope,
            binding,
            request.model_copy(update={"request_key": "after-file"}),
            model_source=file_source,
        )
    assert directory_conflict.value.code == "workspace_directory_capture_model_call_conflict"
    file_fence = (await files.execution(scope, file.checkpoint_id))[0]
    capture = WorkspaceWriter(binding.workspace).capture_file("tree/a.txt", allow_binary=True)
    await files.record(scope, file_fence, capture)
    await files.finish(scope, file_fence)
    restore_source = replace(source, call_id="restore-first")
    plan, _ = await service.workspace_restore_store.reserve(
        scope,
        CheckpointRestorePreview(checkpoint_id=file.checkpoint_id, request_key="restore-first"),
        binding.key,
        binding.fingerprint,
        binding.workspace.fingerprint,
        model_source=restore_source,
    )
    with pytest.raises(ServiceError) as after_restore:
        await directories.reserve(
            scope,
            binding,
            request.model_copy(update={"request_key": "after-restore"}),
            model_source=restore_source,
        )
    assert after_restore.value.code == "workspace_directory_capture_model_call_conflict"
    async with directories.database.sessions() as sql:
        assert (
            await sql.scalar(select(func.count()).select_from(NativeCheckpointModelSourceRow)) == 1
        )
        assert (
            await sql.scalar(select(func.count()).select_from(WorkspaceRestoreModelSourceRow)) == 1
        )
    await directories.cancel(scope, directory.checkpoint_id)
    await service.workspace_restore_store.cancel(scope, plan.plan_id)


async def test_complete_material_and_private_source_link_have_same_original_parent(context):
    service, _agent, binding, scope, source, request = context
    store = service.directory_checkpoint_store
    view, _ = await store.reserve(scope, binding, request, model_source=source)
    fence = await store.execution(scope, view.checkpoint_id)
    snapshot = WorkspaceTreeSnapshotReader(binding.workspace).capture("tree")
    value = await store.record(scope, fence, binding, snapshot, qualify=lambda: None)
    await store.finish(scope, fence, binding, value, qualify=lambda: None)
    assert (await store.get(scope, view.checkpoint_id)).status == "available"
    assert await store.source(scope, view.checkpoint_id) == source
    assert await counts(service) == [1, 1, 1]


async def test_model_reserve_commit_lost_receipt_is_original_key_and_source_without_pool_leak(
    context,
):
    service, _agent, binding, scope, source, request = context
    store = service.directory_checkpoint_store

    def lost(_sql):
        raise RuntimeError("synthetic committed source receipt loss")

    event.listen(Session, "after_commit", lost)
    try:
        with pytest.raises(RuntimeError, match="synthetic committed source"):
            await store.reserve(scope, binding, request, model_source=source)
    finally:
        event.remove(Session, "after_commit", lost)
    assert store.database.engine.sync_engine.pool.checkedout() == 0
    original = await store.by_key(scope, request.request_key)
    assert (
        original.status == "capturing"
        and await store.source(scope, original.checkpoint_id) == source
    )
    assert await store.reserve(scope, binding, request, model_source=source) == (original, False)
    assert await counts(service) == [1, 1, 0]
    await store.cancel(scope, original.checkpoint_id)


def test_previous_published_host_request_hash_vector_is_unchanged():
    from krim_agent.persistence.workspace_directory_captures import WorkspaceDirectoryCaptureStore

    vector = json.loads(
        (
            Path(__file__).parent / "fixtures/directory_checkpoint_host_legacy_vectors.json"
        ).read_text()
    )
    assert vector["source_commit"] == "c09a54b3d751f3c061adff55bce7bb217feb3f91"
    binding = SimpleNamespace(
        key=vector["workspace_key"],
        fingerprint=vector["resource_fingerprint"],
        workspace=SimpleNamespace(fingerprint=vector["workspace_fingerprint"]),
    )
    assert (
        WorkspaceDirectoryCaptureStore._hash(
            DirectoryCheckpointMaterialCreate.model_validate(vector["request"]), binding
        )
        == vector["request_hash"]
    )


@pytest.mark.parametrize("captured", [False, True])
async def test_terminal_original_run_fences_io_finish_and_renew_but_allows_failure_cleanup(
    context, captured
):
    service, _agent, binding, scope, source, request = context
    store = service.directory_checkpoint_store
    view, _ = await store.reserve(scope, binding, request, model_source=source)
    fence = await store.execution(scope, view.checkpoint_id)
    snapshot = WorkspaceTreeSnapshotReader(binding.workspace).capture("tree")
    value = None
    if captured:
        value = await store.record(scope, fence, binding, snapshot, qualify=lambda: None)
    async with store.database.sessions() as sql, sql.begin():
        run = await sql.get(RunRow, source.run_id)
        run.status, run.stop_requested = "aborted", True
    with pytest.raises(ServiceError) as stopped:
        if captured:
            await store.finish(scope, fence, binding, value, qualify=lambda: None)
        else:
            await store.record(scope, fence, binding, snapshot, qualify=lambda: None)
    assert stopped.value.code == "workspace_run_closed"
    with pytest.raises(ServiceError) as renew:
        await store.renew(scope, fence)
    assert renew.value.code == "workspace_run_closed"
    await store.fail(scope, fence, "workspace_run_closed")
    final = await store.get(scope, view.checkpoint_id)
    assert final.status == "failed" and (final.snapshot is not None) == captured
    assert await store.source(scope, final.checkpoint_id) == source
