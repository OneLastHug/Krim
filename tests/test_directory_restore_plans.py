"""新持久目录预览只读工作区，完整备份和原键事实不重放生产者。"""

import asyncio
from dataclasses import replace

import pytest
from sqlalchemy import func, select
from test_directory_checkpoint_capture_store import capture_context as capture_context

from krim_agent.persistence import Database
from krim_agent.persistence.database import (
    WorkspaceDirectoryCaptureRow,
    WorkspaceDirectoryCheckpointRow,
    WorkspaceDirectoryRestorePlanRow,
)
from krim_agent.persistence.workspace_directory_captures import WorkspaceDirectoryCaptureStore
from krim_agent.persistence.workspace_directory_checkpoints import WorkspaceDirectoryCheckpointStore
from krim_agent.persistence.workspace_directory_restores import WorkspaceDirectoryRestoreStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_directory_restore import DirectoryRestorePreview, DirectoryRestoreTarget
from krim_agent.workspace_directory_restore_writer import WorkspaceDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


async def target(context):
    captures, scope, binding, snapshot, request = context
    checkpoint, _ = await captures.reserve(scope, binding, request)
    fence = await captures.execution(scope, checkpoint.checkpoint_id)
    value = await captures.record(scope, fence, binding, snapshot, qualify=lambda: None)
    await captures.finish(scope, fence, binding, value, qualify=lambda: None)
    return checkpoint.checkpoint_id


def request(identifier, key="preview", *, kind="checkpoint"):
    return DirectoryRestorePreview(
        target=DirectoryRestoreTarget(kind=kind, identifier=identifier), request_key=key
    )


async def baseline(context, store, identifier):
    _captures, scope, binding, _snapshot, _request = context
    before = WorkspaceTreeSnapshotReader(binding.workspace).capture("tree")
    fence = await store.preparation(scope, identifier)
    await store.record_baseline(scope, fence, binding, before, qualify=lambda: None)
    return before, await store.ready(scope, fence, binding, qualify=lambda: None)


async def test_real_read_only_preview_complete_backup_original_key_and_cold_zero_producers(
    capture_context, monkeypatch
):
    captures, scope, binding, original, _ = capture_context
    identifier = await target(capture_context)
    (binding.workspace.root / "tree/nested/nul").write_bytes(b"current\0\x80")
    store = WorkspaceDirectoryRestoreStore(captures)
    wanted = request(identifier)
    started, created = await store.reserve(scope, binding, wanted, qualify=lambda: None)
    assert created and started.phase == "preparing" and started.backup is None
    before, ready = await baseline(capture_context, store, started.plan_id)
    assert ready.phase == ready.status == "ready" and ready.revision == 1
    assert ready.backup.snapshot_fingerprint == before.fingerprint != original.fingerprint
    assert ready.target_snapshot.snapshot_fingerprint == original.fingerprint
    assert not ready.cleanup_pending and not list(binding.workspace.root.glob("._krim-tx-*"))
    assert WorkspaceTreeSnapshotReader(binding.workspace).capture("tree") == before
    assert all(
        value not in ready.model_dump_json()
        for value in ("current", "token", "material_id", str(binding.workspace.root))
    )

    def forbidden(*_args, **_kwargs):
        pytest.fail("冷读/同原键不得再次执行Reader/Writer或生产者")

    monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
    monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "prepare", forbidden)
    monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "apply", forbidden)
    cold = WorkspaceDirectoryRestoreStore(captures)
    binding.revoked.set()
    assert await cold.get(scope, ready.plan_id) == ready
    assert await cold.by_key(scope, wanted.request_key) == ready
    assert await cold.reserve(scope, binding, wanted, qualify=forbidden) == (ready, False)
    assert await cold.by_key(scope, "missing") is None


async def test_baseline_reference_survives_original_target_blob_corruption(capture_context):
    captures, scope, binding, _original, _ = capture_context
    identifier = await target(capture_context)
    (binding.workspace.root / "tree/nested/nul").write_bytes(b"new\x80backup")
    store = WorkspaceDirectoryRestoreStore(captures)
    started, _ = await store.reserve(scope, binding, request(identifier), qualify=lambda: None)
    before, ready = await baseline(capture_context, store, started.plan_id)
    async with store.database.sessions() as sql, sql.begin():
        captured = await sql.get(WorkspaceDirectoryCaptureRow, identifier)
        material = await sql.get(WorkspaceDirectoryCheckpointRow, captured.material_id)
        material.payload = b"corrupt original target"
    backup, created = await store.reserve(
        scope,
        binding,
        request(ready.plan_id, "restore-backup", kind="baseline"),
        qualify=lambda: None,
    )
    assert created and backup.target.kind == "baseline" and backup.root_path == "tree"
    assert backup.target_snapshot.snapshot_fingerprint == before.fingerprint
    with pytest.raises(ServiceError):
        await store.reserve(
            scope, binding, request(identifier, "broken-target"), qualify=lambda: None
        )


@pytest.mark.parametrize(
    "fault",
    ["wrong_key", "wrong_scope", "wrong_root", "foreign_token", "early_ready", "baseline_change"],
)
async def test_preview_original_source_scope_and_backup_version_refuse(capture_context, fault):
    captures, scope, binding, original, _ = capture_context
    identifier = await target(capture_context)
    store = WorkspaceDirectoryRestoreStore(captures)
    started, _ = await store.reserve(scope, binding, request(identifier), qualify=lambda: None)
    fence = await store.preparation(scope, started.plan_id)
    with pytest.raises(ServiceError):
        if fault == "wrong_key":
            await store.reserve(
                scope, binding, request(identifier, kind="baseline"), qualify=lambda: None
            )
        elif fault == "wrong_scope":
            await store.get("topic:not-original", started.plan_id)
        elif fault == "foreign_token":
            await store.record_baseline(
                scope, replace(fence, token="0" * 64), binding, original, qualify=lambda: None
            )
        elif fault == "early_ready":
            await store.ready(scope, fence, binding, qualify=lambda: None)
        elif fault == "wrong_root":
            object.__setattr__(original, "root_path", "different")
            await store.record_baseline(scope, fence, binding, original, qualify=lambda: None)
        else:
            await store.record_baseline(scope, fence, binding, original, qualify=lambda: None)
            (binding.workspace.root / "tree/nested/nul").write_bytes(b"different")
            changed = WorkspaceTreeSnapshotReader(binding.workspace).capture("tree")
            await store.record_baseline(scope, fence, binding, changed, qualify=lambda: None)
    assert not list(binding.workspace.root.glob("._krim-tx-*"))


async def test_two_connections_reserve_one_original_preview_and_one_owner(capture_context):
    captures, scope, binding, _, _ = capture_context
    identifier = await target(capture_context)
    first = WorkspaceDirectoryRestoreStore(captures)
    database = Database(str(first.database.engine.url))
    other = WorkspaceDirectoryRestoreStore(
        WorkspaceDirectoryCaptureStore(
            WorkspaceDirectoryCheckpointStore(WorkspaceEditStore(database))
        )
    )
    try:
        results = await asyncio.gather(
            first.reserve(scope, binding, request(identifier), qualify=lambda: None),
            other.reserve(scope, binding, request(identifier), qualify=lambda: None),
        )
        assert results[0][0] == results[1][0] and sorted(created for _, created in results) == [
            False,
            True,
        ]
        async with first.database.sessions() as sql:
            assert (
                await sql.scalar(select(func.count()).select_from(WorkspaceDirectoryRestorePlanRow))
                == 1
            )
        creator, refused = (first, other) if results[0][1] else (other, first)
        fence = await creator.preparation(scope, results[0][0].plan_id)
        assert fence.operation_id == fence.plan_id
        with pytest.raises(ServiceError):
            await refused.preparation(scope, fence.plan_id)
    finally:
        await database.close()
