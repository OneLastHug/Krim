"""完整原材料的真实差异和独立备份整树回退，冷读不再观察工作区。"""

import hashlib

import pytest
from test_directory_checkpoint_capture_store import capture_context as capture_context
from test_directory_restore_execution import apply, prepared, ready
from test_directory_restore_plans import baseline, request, target

from krim_agent.persistence.database import (
    WorkspaceDirectoryCaptureRow,
    WorkspaceDirectoryCheckpointRow,
)
from krim_agent.persistence.workspace_directory_restores import WorkspaceDirectoryRestoreStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_directory_restore_writer import WorkspaceDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


async def test_complete_original_diff_covers_bytes_modes_types_create_delete_and_cold_pages(
    capture_context, monkeypatch
):
    captures, scope, binding, _, _ = capture_context
    identifier = await target(capture_context)
    tree = binding.workspace.root / "tree"
    (tree / "a.txt").unlink()
    (tree / "a.txt").mkdir()
    (tree / "a.txt/new").write_bytes(b"must remove")
    (tree / "empty").unlink()
    (tree / "nested/nul").write_bytes(b"current\x80\0")
    tree.chmod(0o700)
    store = WorkspaceDirectoryRestoreStore(captures)
    planned, _ = await store.reserve(scope, binding, request(identifier), qualify=lambda: None)
    with pytest.raises(ServiceError) as denied:
        await store.diff(scope, planned.plan_id)
    assert denied.value.code == "workspace_directory_restore_not_ready"
    _, planned = await baseline(capture_context, store, planned.plan_id)

    def forbidden(*_args, **_kwargs):
        pytest.fail("差异必须从原完整SQL材料冷读，不得重新观察工作区")

    monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
    monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "prepare", forbidden)
    entries = await store.diff(scope, planned.plan_id)
    assert {item.path: item.operation for item in entries} == {
        "tree": "modify",
        "tree/a.txt": "replace",
        "tree/a.txt/new": "delete",
        "tree/empty": "create",
        "tree/nested/nul": "modify",
    }
    binary = next(item for item in entries if item.path == "tree/nested/nul")
    assert binary.before.sha256 == hashlib.sha256(b"current\x80\0").hexdigest()
    assert binary.after.sha256 == hashlib.sha256(b"\0\xff").hexdigest()
    assert binary.after.mode == 0o640 and binary.after.byte_size == 2
    assert entries[:2] == await store.diff(scope, planned.plan_id, limit=2)
    assert entries[2:] == await store.diff(scope, planned.plan_id, offset=2)
    assert await store.diff(scope, planned.plan_id, offset=999) == []
    assert all("must remove" not in item.model_dump_json() for item in entries)


async def test_real_full_baseline_rollback_works_after_original_target_blob_is_corrupt(
    capture_context,
):
    store, scope, binding, before, planned = await ready(capture_context)
    _, fence, writer, change, authorize = await prepared(store, scope, binding, planned)
    effect = await apply(store, scope, binding, fence, writer, change, authorize)
    restored = await store.settle(scope, fence, binding, effect, qualify=lambda: None)
    assert restored.status == "done"
    async with store.database.sessions() as sql, sql.begin():
        original = await sql.get(WorkspaceDirectoryCaptureRow, planned.target.identifier)
        material = await sql.get(WorkspaceDirectoryCheckpointRow, original.material_id)
        material.payload = b"original target corrupted after complete restore"
    fallback, made = await store.reserve(
        scope, binding, request(planned.plan_id, "rollback", kind="baseline"), qualify=lambda: None
    )
    assert made and fallback.target_snapshot.snapshot_fingerprint == before.fingerprint
    _, fallback = await baseline(capture_context, store, fallback.plan_id)
    op, fence, writer, change, authorize = await prepared(
        store, scope, binding, fallback, key="rollback-execute"
    )
    effect = await apply(store, scope, binding, fence, writer, change, authorize)
    result = await store.settle(scope, fence, binding, effect, qualify=lambda: None)
    assert result.status == "done" and result.operation_id == op.operation_id
    actual = WorkspaceTreeSnapshotReader(binding.workspace).capture("tree")
    assert WorkspaceDirectoryRestoreWriter._same_content(before, actual)
    assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"current\x80\0"
