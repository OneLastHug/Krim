"""当前GC来源的双向call互斥、活跃Run、容量与同事务回滚。"""

from dataclasses import replace

import pytest
from sqlalchemy import event, func, select, update
from test_container_retention_current_model_sources import actor as actor
from test_container_retention_current_model_sources import reserved
from test_container_retention_host import controller as controller
from test_container_retention_host import request
from test_container_retention_store import remove

from krim_agent.persistence.database import RunRow
from krim_agent.persistence.database import WorkspaceContainerRetentionLockRow as Lock
from krim_agent.persistence.database import WorkspaceContainerRetentionModelSourceRow as Source
from krim_agent.persistence.database import WorkspaceContainerRetentionRow as Row
from krim_agent.persistence.directory_restore_capacity import material_usage
from krim_agent.persistence.workspace_restore_sources import admit_restore_source
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_checkpoint import WorkspaceCheckpointCreate
from krim_agent.workspace_directory_checkpoint import DirectoryCheckpointMaterialCreate


@pytest.mark.parametrize("family", ["directory-capture", "file-capture", "file-restore"])
async def test_current_gc_claim_blocks_other_original_producer_using_same_run_call(actor, family):
    service, _, binding, _, _, scope, source = actor
    store, _, _, _, _, _, _, view, fence = await reserved(actor)
    await store.fail(scope, fence, "synthetic_no_intent_release")
    with pytest.raises(ServiceError) as denied:
        if family == "directory-capture":
            await service.directory_checkpoint_store.reserve(
                scope,
                binding,
                DirectoryCheckpointMaterialCreate(root_path="tree", request_key="other-directory"),
                model_source=source,
            )
        elif family == "file-capture":
            await service.workspace_checkpoint_store.reserve(
                scope,
                WorkspaceCheckpointCreate(paths=["tree/a.txt"], request_key="other-file"),
                binding.key,
                binding.fingerprint,
                binding.workspace.fingerprint,
                model_source=source,
            )
        else:
            async with store._transaction() as sql:
                await admit_restore_source(sql, store.edits, scope, source)
    assert denied.value.code == "workspace_container_retention_model_call_conflict"
    assert (await store.model_sources(scope, view.operation_id)).remove == source


async def test_original_directory_capture_claim_blocks_gc_and_rolls_back_gc_row_and_locks(actor):
    service, _, binding, _, preview, scope, source = actor
    checkpoint, _ = await service.directory_checkpoint_store.reserve(
        scope,
        binding,
        DirectoryCheckpointMaterialCreate(root_path="tree", request_key="claimed-call"),
        model_source=source,
    )
    with pytest.raises(ServiceError) as denied:
        await service.container_retention_store.reserve(
            scope, binding, request(preview), qualify=lambda: None, model_source=source
        )
    assert denied.value.code == "workspace_directory_restore_model_call_conflict"
    async with service.container_retention_store.database.sessions() as sql:
        for model in (Row, Source, Lock):
            assert await sql.scalar(select(func.count()).select_from(model)) == 0
    assert (
        await service.directory_checkpoint_store.get(scope, checkpoint.checkpoint_id)
    ).status == "capturing"


@pytest.mark.parametrize("limit", ["sources", "bytes"])
async def test_new_model_verify_capacity_failure_restores_original_epoch_key_marker_and_fences(
    actor, limit
):
    context = await reserved(actor)
    store, scope, binding, origin, _, core, _, view, fence = context
    observed = core.inspect(origin.stage, authorize=lambda: None)
    await store.record_intent(scope, fence, binding, observed, qualify=lambda: None)
    unknown = await store.fail(scope, fence, "synthetic_intent_only_unknown")
    async with store.database.sessions() as sql:
        original = await sql.get(Row, view.operation_id)
        metadata = original.model_sources_metadata
        if limit == "bytes":
            used = await material_usage(sql, sqlite=True)
    if limit == "sources":
        store.max_model_sources = 1
    else:
        store.restores.max_snapshot_bytes = used + 20
    with pytest.raises(ServiceError) as denied:
        await store.claim_verify(
            scope,
            view.operation_id,
            binding,
            expected_revision=unknown.revision,
            request_key="must-roll-back",
            qualify=lambda: None,
            model_source=replace(actor[-1], call_id="over-capacity-verify"),
        )
    assert denied.value.code == "workspace_container_retention_capacity"
    assert await store.get(scope, view.operation_id) == unknown
    async with store.database.sessions() as sql:
        row = await sql.get(Row, view.operation_id)
        assert row.epoch == 0 and row.model_sources_metadata == metadata
        assert await sql.scalar(select(func.count()).select_from(Source)) == 1
        assert await sql.scalar(select(func.count()).select_from(Lock)) == 3
    assert store.database.engine.sync_engine.pool.checkedout() == 0


@pytest.mark.parametrize("when", ["admit", "intent", "settle", "verify"])
async def test_stopped_original_run_cannot_produce_but_original_history_and_failure_cleanup_remain(
    actor, when
):
    service, _, binding, _, preview, scope, source = actor
    store = service.container_retention_store
    context = None
    if when != "admit":
        context = await reserved(actor)
        _, _, _, origin, _, core, _, view, fence = context
        if when == "settle":
            effect = await remove(context)
            assert effect.status == "removed"
        elif when == "verify":
            observed = core.inspect(origin.stage, authorize=lambda: None)
            await store.record_intent(scope, fence, binding, observed, qualify=lambda: None)
            previous = await store.fail(scope, fence, "synthetic_intent_only_unknown")
    async with store.database.sessions() as sql, sql.begin():
        await sql.execute(
            update(RunRow).where(RunRow.id == source.run_id).values(stop_requested=True)
        )
    with pytest.raises(ServiceError) as denied:
        if when == "admit":
            await store.reserve(
                scope, binding, request(preview), qualify=lambda: None, model_source=source
            )
        elif when == "intent":
            observed = core.inspect(origin.stage, authorize=lambda: None)
            await store.record_intent(scope, fence, binding, observed, qualify=lambda: None)
        elif when == "settle":
            await store.settle(scope, fence, binding, effect, qualify=lambda: None)
        else:
            await store.claim_verify(
                scope,
                view.operation_id,
                binding,
                expected_revision=previous.revision,
                request_key="after-stop",
                qualify=lambda: None,
                model_source=replace(source, call_id="stopped-verify"),
            )
    assert denied.value.code == "workspace_run_closed"
    if context is not None:
        final = await store.fail(scope, fence, "original_run_stopped")
        assert final.status == ("interrupted" if when == "intent" else "unknown")
        assert await store.by_key(scope, final.request.request_key) == final


async def test_source_index_insert_failure_rolls_back_new_gc_marker_namespace_and_key(actor):
    service, _, binding, _, preview, scope, source = actor
    store = service.container_retention_store

    def fail(_connection, _cursor, statement, _parameters, _context, _many):
        if statement.startswith("INSERT INTO workspace_container_retention_model_sources"):
            raise OSError("synthetic GC source index write failure")

    event.listen(store.database.engine.sync_engine, "before_cursor_execute", fail)
    try:
        with pytest.raises(OSError, match="source index write failure"):
            await store.reserve(
                scope, binding, request(preview), qualify=lambda: None, model_source=source
            )
    finally:
        event.remove(store.database.engine.sync_engine, "before_cursor_execute", fail)
    assert await store.by_key(scope, request(preview).request_key) is None
    async with store.database.sessions() as sql:
        for model in (Row, Source, Lock):
            assert await sql.scalar(select(func.count()).select_from(model)) == 0
    assert store.database.engine.sync_engine.pool.checkedout() == 0
