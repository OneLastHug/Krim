"""混合批次Store真实SQLite冻结、幂等和物理命名空间门禁。"""

import asyncio
import time

import pytest
from alembic import command
from sqlalchemy import select, text, update

from krim_agent.persistence.database import (
    Database,
    MixedWorkspaceBatchChildRow,
    MixedWorkspaceBatchRow,
    WorkspaceEditProposalRow,
)
from krim_agent.persistence.migration import migration_config
from krim_agent.persistence.workspace_directories import WorkspaceDirectoryStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.persistence.workspace_mixed_batches import MixedBatchStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_change import FileChangeSpec
from krim_agent.workspace_directory import DirectoryChangeSpec, WorkspaceDirectoryWriter
from krim_agent.workspace_mixed_batch import MixedBatchRequest, MixedBatchTarget
from krim_agent.workspace_writer import WorkspaceWriter


async def setup(repository, tmp_path, *, file_path="file.txt", directory_path="empty"):
    topic = await repository.create_topic("mixed store")
    scope = "topic:" + topic.id
    workspace = Workspace(tmp_path, writable=True)
    files = WorkspaceEditStore(repository.database)
    directories = WorkspaceDirectoryStore(repository.database, owner_id=files.owner_id)
    file, _ = await files.preview(
        scope,
        "files",
        workspace.fingerprint,
        "file-preview",
        WorkspaceWriter(workspace).prepare(
            FileChangeSpec(path=file_path, operation="write", content="data", expected_sha256=None)
        ),
    )
    directory, _ = await directories.preview(
        scope,
        "files",
        workspace.fingerprint,
        "directory-preview",
        WorkspaceDirectoryWriter(workspace).prepare(
            DirectoryChangeSpec(path=directory_path, operation="create", mode=0o750)
        ),
    )
    request = MixedBatchRequest(
        action="apply",
        targets=[
            MixedBatchTarget(source="file", proposal_id=file.view.proposal_id, expected_revision=0),
            MixedBatchTarget(
                source="directory", proposal_id=directory.view.proposal_id, expected_revision=0
            ),
        ],
        request_key="mixed",
    )
    return MixedBatchStore(files, directories), scope, workspace, request


async def test_same_physical_file_and_directory_target_is_rejected_before_acceptance(
    repository, tmp_path
):
    store, scope, workspace, request = await setup(
        repository, tmp_path, file_path="same", directory_path="same"
    )
    try:
        with pytest.raises(ServiceError) as rejected:
            await store.reserve(scope, "files", workspace.fingerprint, request)
        assert rejected.value.code == "workspace_mixed_batch_dependency"
        assert await store.by_key(scope, "mixed") is None
        assert not (tmp_path / "same").exists()
    finally:
        workspace.close()


async def test_reserve_persists_frozen_source_and_is_idempotent(repository, tmp_path):
    store, scope, workspace, request = await setup(repository, tmp_path)
    try:
        first, created = await store.reserve(scope, "files", workspace.fingerprint, request)
        assert created and first.status == "running" and len(first.items) == 2
        assert {item.source for item in first.items} == {"file", "directory"}
        repeated, created = await store.reserve(scope, "files", workspace.fingerprint, request)
        assert not created and repeated == first
        conflict = request.model_copy(update={"action": "undo"})
        with pytest.raises(ServiceError) as mismatch:
            await store.reserve(scope, "files", workspace.fingerprint, conflict)
        assert mismatch.value.code == "workspace_mixed_batch_idempotency_conflict"
        cold = await store.get(scope, first.batch_id)
        assert cold == first and all(item.operation is None for item in cold.items)
    finally:
        workspace.close()


async def test_cancel_closes_every_child_and_same_key_never_reopens(repository, tmp_path):
    store, scope, workspace, request = await setup(repository, tmp_path)
    try:
        accepted, _ = await store.reserve(scope, "files", workspace.fingerprint, request)
        cancelled = await store.cancel(scope, accepted.batch_id)
        assert cancelled.status == "cancelled"
        assert all(item.status == "skipped" and item.operation is None for item in cancelled.items)
        repeated, created = await store.reserve(scope, "files", workspace.fingerprint, request)
        assert not created and repeated == cancelled
        async with store.database.sessions() as sql:
            parent = await sql.get(MixedWorkspaceBatchRow, accepted.batch_id)
            children = list(
                await sql.scalars(
                    select(MixedWorkspaceBatchChildRow).where(
                        MixedWorkspaceBatchChildRow.batch_id == accepted.batch_id
                    )
                )
            )
            assert parent.owner_id is None and parent.lease_expires_at is None
            assert all(not child.admission_open for child in children)
        assert not (tmp_path / "file.txt").exists() and not (tmp_path / "empty").exists()
    finally:
        workspace.close()


async def test_expired_lease_is_interrupted_without_replaying_or_reopening(repository, tmp_path):
    store, scope, workspace, request = await setup(repository, tmp_path)
    try:
        accepted, _ = await store.reserve(scope, "files", workspace.fingerprint, request)
        async with store.database.sessions.begin() as sql:
            await sql.execute(
                update(MixedWorkspaceBatchRow)
                .where(MixedWorkspaceBatchRow.batch_id == accepted.batch_id)
                .values(lease_expires_at=time.time() - 1)
            )
        read = await store.get(scope, accepted.batch_id)
        assert (
            read.status == "interrupted"
            and read.stop_reason == "workspace_mixed_batch_lease_expired"
        )
        assert all(item.status == "skipped" and item.operation is None for item in read.items)
        assert (await store.by_key(scope, "mixed")) == read
        assert not (tmp_path / "file.txt").exists()
    finally:
        workspace.close()


@pytest.mark.parametrize("case", ["revision", "resource", "snapshot", "status", "expired"])
async def test_rejected_admission_preserves_zero_parent_rows(repository, tmp_path, case):
    store, scope, workspace, request = await setup(repository, tmp_path)
    try:
        resource = workspace.fingerprint
        if case == "revision":
            request.targets[0].expected_revision = 1
        elif case == "resource":
            resource = "0" * 64
        elif case == "snapshot":
            request.snapshot_revision = "0" * 64
        else:
            async with store.database.sessions.begin() as sql:
                values = {"status": "uncertain"} if case == "status" else {"expires_at": 0.0}
                await sql.execute(
                    update(WorkspaceEditProposalRow)
                    .where(WorkspaceEditProposalRow.proposal_id == request.targets[0].proposal_id)
                    .values(**values)
                )
        with pytest.raises(ServiceError):
            await store.reserve(scope, "files", resource, request)
        assert await store.by_key(scope, "mixed") is None
        assert not (tmp_path / "file.txt").exists()
    finally:
        workspace.close()


async def test_two_connections_have_single_acceptance_and_cold_scope_fences(repository, tmp_path):
    store, scope, workspace, request = await setup(repository, tmp_path)
    other_db = Database(str(repository.database.engine.url))
    other_files = WorkspaceEditStore(other_db, owner_id=store.owner_id)
    other_directories = WorkspaceDirectoryStore(other_db, owner_id=store.owner_id)
    other_store = MixedBatchStore(other_files, other_directories)
    try:
        results = await asyncio.gather(
            store.reserve(scope, "files", workspace.fingerprint, request),
            other_store.reserve(scope, "files", workspace.fingerprint, request),
        )
        assert len({item[0].batch_id for item in results}) == 1
        assert sum(item[1] for item in results) == 1
        different = "topic:" + (await repository.create_topic("foreign")).id
        assert await store.by_key(different, "mixed") is None
        with pytest.raises(ServiceError) as denied:
            await store.get(different, results[0][0].batch_id)
        assert denied.value.code == "workspace_mixed_batch_not_found"
        assert await other_store.get(scope, results[0][0].batch_id) == results[0][0]
    finally:
        await other_db.close()
        workspace.close()


async def test_nonempty_batch_downgrade_refuses_loss_and_empty_roundtrip(repository, tmp_path):
    store, scope, workspace, request = await setup(repository, tmp_path)
    try:
        async with store.database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(
                    migration_config(sync), "0038_directory_move_target_lock"
                )
            )
            await connection.run_sync(lambda sync: command.upgrade(migration_config(sync), "head"))
        accepted, _ = await store.reserve(scope, "files", workspace.fingerprint, request)
        with pytest.raises(RuntimeError, match="混合批次账本非空"):
            async with store.database.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(
                        migration_config(sync), "0038_directory_move_target_lock"
                    )
                )
        assert (await store.get(scope, accepted.batch_id)) == accepted
        async with store.database.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0053_directory_captures"
            assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
    finally:
        workspace.close()


async def test_move_target_cannot_overlap_selected_file_entry(repository, tmp_path):
    store, scope, workspace, request = await setup(repository, tmp_path)
    (tmp_path / "source").mkdir()
    try:
        writer = WorkspaceDirectoryWriter(workspace)
        snapshot = writer.capture("source")
        moved, _ = await store.directories.preview(
            scope,
            "files",
            workspace.fingerprint,
            "move-preview",
            writer.prepare(
                DirectoryChangeSpec(
                    path="source",
                    operation="move",
                    target_path="file.txt",
                    expected_fingerprint=snapshot.fingerprint,
                )
            ),
        )
        request.targets[1] = MixedBatchTarget(
            source="directory", proposal_id=moved.view.proposal_id, expected_revision=0
        )
        with pytest.raises(ServiceError) as denied:
            await store.reserve(scope, "files", workspace.fingerprint, request)
        assert denied.value.code == "workspace_mixed_batch_dependency"
    finally:
        workspace.close()
