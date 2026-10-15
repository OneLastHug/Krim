"""真实完整目录材料的SQL保真、冷读、拒绝和幂等，不接公开生产者。"""

import asyncio

import pytest
from sqlalchemy import event, func, select, text
from sqlalchemy.orm import Session

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentService
from krim_agent.persistence import Database
from krim_agent.persistence.database import WorkspaceDirectoryCheckpointRow
from krim_agent.persistence.workspace_directory_checkpoints import WorkspaceDirectoryCheckpointStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace, WorkspaceBinding
from krim_agent.workspace_directory_checkpoint import DirectoryCheckpointMaterialCreate
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


def directory(root):
    (root / "tree/sub").mkdir(parents=True)
    (root / "tree/empty").mkdir()
    (root / "tree/sub/text.txt").write_bytes(b"text\n")
    (root / "tree/raw.bin").write_bytes(b"\0\xff")
    (root / "tree/raw.bin").chmod(0o640)
    (root / "tree/zero.bin").write_bytes(b"")
    (root / "tree").chmod(0o750)


@pytest.fixture
async def material_context(repository, tmp_path):
    root = tmp_path / "files"
    directory(root)
    workspace = Workspace(root)
    binding = WorkspaceBinding("files", "1", workspace, "a" * 64)
    snapshot = WorkspaceTreeSnapshotReader(workspace).capture("tree")
    topic = await repository.create_topic("目录材料")
    request = DirectoryCheckpointMaterialCreate(root_path="tree", request_key="original")
    store = WorkspaceDirectoryCheckpointStore(WorkspaceEditStore(repository.database))
    try:
        yield store, "topic:" + topic.id, binding, snapshot, request
    finally:
        workspace.close()


async def count(store):
    async with store.database.sessions() as sql:
        return await sql.scalar(select(func.count()).select_from(WorkspaceDirectoryCheckpointRow))


async def test_real_tree_bytes_empty_directories_modes_and_cold_read_without_filesystem(
    material_context, monkeypatch
):
    store, scope, binding, snapshot, request = material_context
    result, created = await store.save_captured(scope, binding, request, snapshot)
    assert created and result.snapshot == snapshot
    assert result.view.status == "stored" and result.view.source == "workspace_directory_material"
    assert result.view.entry_count == 6 and result.view.file_count == 3
    assert result.view.directory_count == 3 and result.view.byte_size == 7
    assert result.view.snapshot_fingerprint == snapshot.fingerprint
    raw = next(entry.file for entry in result.snapshot.entries if entry.path == "tree/raw.bin")
    assert raw.data == b"\0\xff" and raw.mode == 0o640
    (binding.workspace.root / "tree/raw.bin").write_bytes(b"changed")
    binding.workspace.close()
    binding.revoked.set()

    def forbidden(*_args, **_kwargs):
        pytest.fail("历史目录材料不得重新观察或验证工作区")

    monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
    monkeypatch.setattr(WorkspaceTreeSnapshotReader, "verify", forbidden)
    cold = WorkspaceDirectoryCheckpointStore(WorkspaceEditStore(store.database))
    queried = await cold.get_material(
        scope,
        result.view.checkpoint_id,
        workspace_key=binding.key,
        resource_fingerprint=binding.fingerprint,
    )
    assert queried == result
    assert (
        await cold.by_key(
            scope,
            request.request_key,
            workspace_key=binding.key,
            resource_fingerprint=binding.fingerprint,
        )
        == result
    )
    repeated, created = await cold.save_captured(scope, binding, request)
    assert repeated == result and not created and await count(cold) == 1
    assert "text\\n" not in repr(result)
    with pytest.raises(ServiceError) as unavailable:
        await cold.save_captured(
            scope, binding, request.model_copy(update={"request_key": "new"}), snapshot
        )
    assert unavailable.value.code == "workspace_unavailable"


async def test_key_change_other_scope_resource_and_wrong_root_are_rejected(
    material_context, repository
):
    store, scope, binding, snapshot, request = material_context
    result, _ = await store.save_captured(scope, binding, request, snapshot)
    with pytest.raises(ServiceError) as conflict:
        await store.save_captured(scope, binding, request.model_copy(update={"title": "changed"}))
    assert conflict.value.code == "workspace_directory_checkpoint_key_conflict"
    other = "topic:" + (await repository.create_topic("另一个作用域")).id
    for selected_scope, fingerprint in ((other, binding.fingerprint), (scope, "b" * 64)):
        with pytest.raises(ServiceError) as missing:
            await store.get_material(
                selected_scope,
                result.view.checkpoint_id,
                workspace_key=binding.key,
                resource_fingerprint=fingerprint,
            )
        assert missing.value.code == "workspace_directory_checkpoint_not_found"
    assert (
        await store.by_key(
            other,
            request.request_key,
            workspace_key=binding.key,
            resource_fingerprint=binding.fingerprint,
        )
        is None
    )
    with pytest.raises(ServiceError) as wrong_root:
        await store.save_captured(
            scope,
            binding,
            request.model_copy(update={"request_key": "different", "root_path": "elsewhere"}),
            snapshot,
        )
    assert wrong_root.value.code == "workspace_directory_checkpoint_binding"
    assert await count(store) == 1


@pytest.mark.parametrize("snapshot", [None, object(), False])
async def test_malformed_private_snapshot_is_controlled_before_attributes(
    material_context, snapshot
):
    store, scope, binding, _snapshot, request = material_context
    with pytest.raises(ServiceError) as invalid:
        await store.save_captured(scope, binding, request, snapshot)
    assert invalid.value.code == "workspace_directory_checkpoint_material"
    assert await count(store) == 0


@pytest.mark.parametrize("field", ["payload", "metadata", "request_hash", "snapshot_fingerprint"])
async def test_corrupt_complete_material_never_returns_a_partial_success(material_context, field):
    store, scope, binding, snapshot, request = material_context
    original, _ = await store.save_captured(scope, binding, request, snapshot)
    async with store.database.sessions.begin() as sql:
        row = await sql.get(WorkspaceDirectoryCheckpointRow, original.view.checkpoint_id)
        if field == "payload":
            row.payload += b"extra"
        elif field == "metadata":
            row.metadata_value = {**row.metadata_value, "kind": "forged"}
        else:
            setattr(row, field, "0" * 64)
    with pytest.raises(ServiceError) as corrupted:
        await store.get_material(
            scope,
            original.view.checkpoint_id,
            workspace_key=binding.key,
            resource_fingerprint=binding.fingerprint,
        )
    assert corrupted.value.code == "workspace_directory_checkpoint_corrupt"


async def test_history_count_and_actual_json_plus_blob_capacity_are_global(
    material_context, repository
):
    store, scope, binding, snapshot, request = material_context
    original, _ = await store.save_captured(scope, binding, request, snapshot)
    other = "topic:" + (await repository.create_topic("全局配额")).id
    limited = WorkspaceDirectoryCheckpointStore(store.edits, max_checkpoints=1)
    with pytest.raises(ServiceError) as full:
        await limited.save_captured(other, binding, request, snapshot)
    assert full.value.code == "workspace_directory_checkpoint_capacity"
    async with store.database.sessions() as sql:
        used = (
            await sql.execute(
                text(
                    "SELECT length(payload) + length(CAST(metadata AS BLOB)) "
                    "FROM workspace_directory_checkpoints"
                )
            )
        ).scalar_one()
    assert used > snapshot.total_bytes
    byte_limited = WorkspaceDirectoryCheckpointStore(
        store.edits, max_snapshot_bytes=used + snapshot.total_bytes + 1
    )
    with pytest.raises(ServiceError) as no_bytes:
        await byte_limited.save_captured(
            scope, binding, request.model_copy(update={"request_key": "second"}), snapshot
        )
    assert no_bytes.value.code == "workspace_directory_checkpoint_capacity"
    assert (await byte_limited.save_captured(scope, binding, request))[0] == original
    assert await count(store) == 1


async def test_two_independent_connections_reserve_one_immutable_material(material_context):
    store, scope, binding, snapshot, request = material_context
    other_database = Database(str(store.database.engine.url))
    other = WorkspaceDirectoryCheckpointStore(WorkspaceEditStore(other_database))
    try:
        results = await asyncio.gather(
            store.save_captured(scope, binding, request, snapshot),
            other.save_captured(scope, binding, request, snapshot),
        )
        assert results[0][0] == results[1][0]
        assert sorted(created for _record, created in results) == [False, True]
        assert await count(store) == 1
    finally:
        await other_database.close()


async def test_cancel_waiting_for_original_lock_does_not_publish_material(material_context):
    store, scope, binding, snapshot, request = material_context
    async with store.database.write_lock:
        waiting = asyncio.create_task(store.save_captured(scope, binding, request, snapshot))
        await asyncio.sleep(0)
        assert not waiting.done()
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
    assert await count(store) == 0


async def test_committed_material_lost_receipt_is_read_by_original_key_without_recapture(
    material_context,
):
    store, scope, binding, snapshot, request = material_context

    def lost(_session):
        raise RuntimeError("lost directory material receipt")

    event.listen(Session, "after_commit", lost)
    try:
        with pytest.raises(RuntimeError, match="lost directory material receipt"):
            await store.save_captured(scope, binding, request, snapshot)
    finally:
        event.remove(Session, "after_commit", lost)
    assert store.database.engine.sync_engine.pool.checkedout() == 0
    cold = WorkspaceDirectoryCheckpointStore(WorkspaceEditStore(store.database))
    saved = await cold.by_key(
        scope,
        request.request_key,
        workspace_key=binding.key,
        resource_fingerprint=binding.fingerprint,
    )
    assert saved is not None and saved.snapshot == snapshot
    duplicate, created = await cold.save_captured(scope, binding, request)
    assert duplicate == saved and not created and await count(store) == 1


async def test_session_owner_closed_history_and_original_resource_fence(runtime, tmp_path):
    root = tmp_path / "session-files"
    directory(root)
    service = AgentService(runtime, runtime.settings)
    service.register_workspace("files", root, version="1")
    try:
        await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name="dirhost", description="可信目录材料", workspace_key="files"
                ),
            )
        )
        actor = await service.spawn("dirhost")
        scope = "session:" + actor.agent_id
        binding = service.manager._handles[actor.agent_id].definition.workspace
        snapshot = WorkspaceTreeSnapshotReader(binding.require()).capture("tree")
        request = DirectoryCheckpointMaterialCreate(root_path="tree", request_key="session")
        foreign = WorkspaceDirectoryCheckpointStore(WorkspaceEditStore(runtime.repository.database))
        with pytest.raises(ServiceError):
            await foreign.save_captured(scope, binding, request, snapshot)
        assert await count(foreign) == 0
        store = WorkspaceDirectoryCheckpointStore(service.workspace_edits)
        saved, _ = await store.save_captured(scope, binding, request, snapshot)
        session = await service.session(actor.agent_id)
        await service.close_session(actor.agent_id, session.revision)
        assert (
            await store.get_material(
                scope,
                saved.view.checkpoint_id,
                workspace_key=binding.key,
                resource_fingerprint=binding.fingerprint,
            )
            == saved
        )
        with pytest.raises(ServiceError):
            await store.save_captured(
                scope, binding, request.model_copy(update={"request_key": "closed"}), snapshot
            )
        assert await count(store) == 1
    finally:
        await service.close()
