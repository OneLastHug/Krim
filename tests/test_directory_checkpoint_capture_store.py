"""原目录任务的SQL原子关联、跨连接、旧owner/租约、配额和丢回执。"""

import asyncio
import time
from dataclasses import replace

import pytest
from sqlalchemy import event, func, select
from sqlalchemy.orm import Session
from test_directory_checkpoint_capture import make_tree

from krim_agent.persistence import Database
from krim_agent.persistence.database import (
    WorkspaceDirectoryCaptureRow,
    WorkspaceDirectoryCheckpointRow,
)
from krim_agent.persistence.workspace_directory_captures import WorkspaceDirectoryCaptureStore
from krim_agent.persistence.workspace_directory_checkpoints import WorkspaceDirectoryCheckpointStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace, WorkspaceBinding
from krim_agent.workspace_directory_checkpoint import DirectoryCheckpointMaterialCreate
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


@pytest.fixture
async def capture_context(repository, tmp_path):
    make_tree(tmp_path)
    workspace = Workspace(tmp_path, writable=True)
    binding = WorkspaceBinding("files", "1", workspace, "a" * 64)
    snapshot = WorkspaceTreeSnapshotReader(workspace).capture("tree")
    materials = WorkspaceDirectoryCheckpointStore(WorkspaceEditStore(repository.database))
    store = WorkspaceDirectoryCaptureStore(materials)
    topic = await repository.create_topic("完整目录生产捕获")
    request = DirectoryCheckpointMaterialCreate(root_path="tree", request_key="original")
    try:
        yield store, "topic:" + topic.id, binding, snapshot, request
    finally:
        workspace.close()


async def material_count(store):
    async with store.database.sessions() as sql:
        return await sql.scalar(select(func.count()).select_from(WorkspaceDirectoryCheckpointRow))


async def reserved(context):
    store, scope, binding, snapshot, request = context
    view, created = await store.reserve(scope, binding, request)
    assert created
    fence = await store.execution(scope, view.checkpoint_id)
    return store, scope, binding, snapshot, request, view, fence


async def test_two_real_connections_same_key_have_one_job_and_one_capturing_owner(capture_context):
    store, scope, binding, _snapshot, request = capture_context
    other_database = Database(str(store.database.engine.url))
    other = WorkspaceDirectoryCaptureStore(
        WorkspaceDirectoryCheckpointStore(WorkspaceEditStore(other_database))
    )
    try:
        results = await asyncio.gather(
            store.reserve(scope, binding, request), other.reserve(scope, binding, request)
        )
        assert results[0][0] == results[1][0]
        assert sorted(created for _view, created in results) == [False, True]
        async with store.database.sessions() as sql:
            assert (
                await sql.scalar(select(func.count()).select_from(WorkspaceDirectoryCaptureRow))
                == 1
            )
        assert await material_count(store) == 0
    finally:
        await other_database.close()


async def test_cancel_and_foreign_token_owner_cannot_publish_or_renew(capture_context):
    store, scope, binding, snapshot, request, view, fence = await reserved(capture_context)
    other = WorkspaceDirectoryCaptureStore(
        WorkspaceDirectoryCheckpointStore(WorkspaceEditStore(store.database))
    )
    for selected, token in ((other, fence), (store, replace(fence, token="0" * 64))):
        with pytest.raises(ServiceError) as denied:
            await selected.record(scope, token, binding, snapshot, qualify=lambda: None)
        assert denied.value.code == "workspace_directory_capture_fenced"
        with pytest.raises(ServiceError):
            await selected.renew(scope, token)
    cancelled = await store.cancel(scope, view.checkpoint_id)
    assert cancelled.status == "cancelled"
    with pytest.raises(ServiceError):
        await store.record(scope, fence, binding, snapshot, qualify=lambda: None)
    assert (await store.reserve(scope, binding, request)) == (cancelled, False)
    assert await material_count(store) == 0


async def test_expired_job_is_interrupted_without_restart_and_old_token_cannot_finish(
    capture_context,
):
    store, scope, binding, snapshot, request, view, fence = await reserved(capture_context)
    value = await store.record(scope, fence, binding, snapshot, qualify=lambda: None)
    async with store.database.sessions() as sql, sql.begin():
        row = await sql.get(WorkspaceDirectoryCaptureRow, view.checkpoint_id)
        row.lease_expires_at = time.time() - 1
    cold = WorkspaceDirectoryCaptureStore(
        WorkspaceDirectoryCheckpointStore(WorkspaceEditStore(store.database))
    )
    assert await cold.recover_interrupted() == 1
    assert await cold.recover_interrupted() == 0
    result = await store.get(scope, view.checkpoint_id)
    assert result.status == "interrupted" and result.snapshot is not None
    assert await store.reserve(scope, binding, request) == (result, False)
    with pytest.raises(ServiceError):
        await store.finish(scope, fence, binding, value, qualify=lambda: None)
    assert await material_count(store) == 1


async def test_complete_material_and_source_link_rollback_together(capture_context, monkeypatch):
    store, scope, binding, snapshot, _request, view, fence = await reserved(capture_context)
    save = store.materials._save_captured

    async def lost_link(*args, **kwargs):
        await save(*args, **kwargs)
        raise OSError("synthetic link failure after material flush")

    monkeypatch.setattr(store.materials, "_save_captured", lost_link)
    with pytest.raises(OSError, match="synthetic link failure"):
        await store.record(scope, fence, binding, snapshot, qualify=lambda: None)
    assert await material_count(store) == 0
    assert (await store.get(scope, view.checkpoint_id)).snapshot is None
    assert store.database.engine.sync_engine.pool.checkedout() == 0


@pytest.mark.parametrize("phase", ["reserve", "record", "finish"])
async def test_each_sql_commit_lost_receipt_is_read_back_without_replay_or_pool_leak(
    capture_context, phase
):
    store, scope, binding, snapshot, request = capture_context
    view = fence = value = None
    if phase != "reserve":
        view, _created = await store.reserve(scope, binding, request)
        fence = await store.execution(scope, view.checkpoint_id)
    if phase == "finish":
        value = await store.record(scope, fence, binding, snapshot, qualify=lambda: None)

    def lost(_sql):
        raise RuntimeError("synthetic committed receipt loss")

    event.listen(Session, "after_commit", lost)
    try:
        with pytest.raises(RuntimeError, match="synthetic committed receipt loss"):
            if phase == "reserve":
                await store.reserve(scope, binding, request)
            elif phase == "record":
                await store.record(scope, fence, binding, snapshot, qualify=lambda: None)
            else:
                await store.finish(scope, fence, binding, value, qualify=lambda: None)
    finally:
        event.remove(Session, "after_commit", lost)
    assert store.database.engine.sync_engine.pool.checkedout() == 0
    result = await store.by_key(scope, request.request_key)
    assert result.status == ("available" if phase == "finish" else "capturing")
    assert (result.snapshot is not None) == (phase != "reserve")
    assert await material_count(store) == (0 if phase == "reserve" else 1)
    assert await store.reserve(scope, binding, request) == (result, False)


async def test_global_job_and_actual_material_quota_do_not_partially_publish(capture_context):
    store, scope, binding, snapshot, request, view, fence = await reserved(capture_context)
    limited = WorkspaceDirectoryCaptureStore(store.materials, max_captures=1)
    with pytest.raises(ServiceError) as full:
        await limited.reserve(scope, binding, request.model_copy(update={"request_key": "second"}))
    assert full.value.code == "workspace_directory_capture_capacity"
    assert await limited.reserve(scope, binding, request) == (view, False)
    store.materials.max_snapshot_bytes = 1024
    with pytest.raises(ServiceError) as no_bytes:
        await store.record(scope, fence, binding, snapshot, qualify=lambda: None)
    assert no_bytes.value.code == "workspace_directory_checkpoint_capacity"
    assert await material_count(store) == 0
    assert (await store.get(scope, view.checkpoint_id)).snapshot is None


async def test_last_sql_flush_cannot_publish_after_original_deadline(capture_context):
    store, scope, binding, snapshot, _request, view, fence = await reserved(capture_context)
    value = await store.record(scope, fence, binding, snapshot, qualify=lambda: None)
    cutoff = time.monotonic() + 0.05

    def qualify():
        if time.monotonic() >= cutoff:
            raise ServiceError("tool_timeout", "原预算终止", 408)

    def delayed(_sql, _flush, _instances):
        if any(
            isinstance(row, WorkspaceDirectoryCaptureRow) and row.status == "available"
            for row in _sql.dirty
        ):
            time.sleep(0.08)

    event.listen(Session, "before_flush", delayed)
    try:
        with pytest.raises(ServiceError) as late:
            await store.finish(scope, fence, binding, value, qualify=qualify)
        assert late.value.code == "tool_timeout"
    finally:
        event.remove(Session, "before_flush", delayed)
    result = await store.get(scope, view.checkpoint_id)
    assert result.status == "capturing" and result.snapshot is not None
    assert await material_count(store) == 1


async def test_cold_link_to_unrelated_material_is_rejected(capture_context):
    store, scope, binding, snapshot, request, view, _fence = await reserved(capture_context)
    unrelated, _created = await store.materials.save_captured(
        scope, binding, request.model_copy(update={"request_key": "foreign"}), snapshot
    )
    async with store.database.sessions() as sql, sql.begin():
        row = await sql.get(WorkspaceDirectoryCaptureRow, view.checkpoint_id)
        row.material_id = unrelated.view.checkpoint_id
    with pytest.raises(ServiceError) as corrupt:
        await store.get(scope, view.checkpoint_id)
    assert corrupt.value.code == "workspace_directory_capture_corrupt"


async def test_renew_flush_crossing_original_lease_cannot_revive_producer(
    capture_context, monkeypatch
):
    store, scope, _binding, _snapshot, _request, view, fence = await reserved(capture_context)
    original_live = store._live
    committed_lease = []

    async def expiring(sql, actual_scope, actual_fence):
        row = await original_live(sql, actual_scope, actual_fence)
        committed_lease.append(row.lease_expires_at)
        # 固定最后资格后的注入窗口；前置SQL耗时不充当本节点的过期触发。
        row.lease_expires_at = time.time() + 0.1
        return row

    monkeypatch.setattr(store, "_live", expiring)
    delayed = []

    def delay_renew(sql, _flush, _instances):
        if any(
            isinstance(row, WorkspaceDirectoryCaptureRow) and row.status == "capturing"
            for row in sql.dirty
        ):
            delayed.append(True)
            time.sleep(0.15)

    event.listen(Session, "before_flush", delay_renew)
    try:
        with pytest.raises(ServiceError) as expired:
            await store.renew(scope, fence)
        assert expired.value.code == "workspace_directory_capture_fenced"
    finally:
        event.remove(Session, "before_flush", delay_renew)
    assert delayed == [True]
    async with store.database.sessions() as sql:
        row = await sql.get(WorkspaceDirectoryCaptureRow, view.checkpoint_id)
        assert committed_lease == [row.lease_expires_at]
        assert row.status == "capturing" and row.owner_id == store.owner_id
    assert await material_count(store) == 0
