"""0036目录账本真实CAS/幂等/材料/命名空间门禁，全部使用临时数据库。"""

import asyncio
import hashlib
import json
import time

import pytest
from alembic import command
from sqlalchemy import text, update

from krim_agent.persistence.database import (
    Database,
    WorkspaceDirectoryAttemptRow,
)
from krim_agent.persistence.migration import migration_config
from krim_agent.persistence.workspace_directories import WorkspaceDirectoryStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_change import FileChangeSpec, WorkspaceChangeAction
from krim_agent.workspace_directory import (
    DirectoryChangeAction,
    DirectoryChangeSpec,
    WorkspaceDirectoryWriter,
)
from krim_agent.workspace_writer import WorkspaceWriter


async def setup(repository, tmp_path):
    topic = await repository.create_topic("directory ledger")
    workspace = Workspace(tmp_path, writable=True)
    return WorkspaceDirectoryStore(repository.database), "topic:" + topic.id, workspace


def request(proposal, action="apply", key="apply"):
    return DirectoryChangeAction(
        proposal_id=proposal.proposal_id,
        action=action,
        expected_revision=proposal.revision,
        request_key=key,
    )


async def test_real_preview_reserve_apply_settle_and_original_key_never_repeats(
    repository, tmp_path
):
    store, scope, workspace = await setup(repository, tmp_path)
    try:
        writer = WorkspaceDirectoryWriter(workspace)
        plan = writer.prepare(DirectoryChangeSpec(path="empty", operation="create", mode=0o750))
        proposal, created = await store.preview(
            scope, "files", workspace.fingerprint, "preview", plan
        )
        assert created and not (tmp_path / "empty").exists()
        again, created = await store.preview(scope, "files", workspace.fingerprint, "preview", plan)
        assert not created and again.view.proposal_id == proposal.view.proposal_id
        attempt, created = await store.reserve(
            scope, request(proposal.view), workspace.fingerprint, plan
        )
        assert created
        reference = []
        effect = writer.apply(plan, record_intent=reference.append)
        await store.record_intent(scope, attempt.view.operation_id, reference[0])
        done = await store.settle(scope, attempt.view.operation_id, effect)
        assert done.view.status == "done" and (tmp_path / "empty").is_dir()
        duplicated, created = await store.reserve(
            scope, request(proposal.view), workspace.fingerprint, plan
        )
        assert not created and duplicated.view.operation_id == done.view.operation_id
    finally:
        workspace.close()


async def test_two_connections_same_proposal_and_operation_have_single_acceptance(
    repository, tmp_path
):
    store, scope, workspace = await setup(repository, tmp_path)
    database = Database(str(repository.database.engine.url))
    second = WorkspaceDirectoryStore(database, owner_id=store.owner_id)
    try:
        plan = WorkspaceDirectoryWriter(workspace).prepare(
            DirectoryChangeSpec(path="empty", operation="create", mode=0o750)
        )
        values = await asyncio.gather(
            *[
                chosen.preview(scope, "files", workspace.fingerprint, "one", plan)
                for chosen in [store, second]
            ]
        )
        assert (
            len({item[0].view.proposal_id for item in values}) == 1
            and sum(item[1] for item in values) == 1
        )
        view = values[0][0].view
        operations = await asyncio.gather(
            *[
                chosen.reserve(scope, request(view), workspace.fingerprint, plan)
                for chosen in [store, second]
            ]
        )
        assert (
            len({item[0].view.operation_id for item in operations}) == 1
            and sum(item[1] for item in operations) == 1
        )
    finally:
        await database.close()
        workspace.close()


async def test_expired_owner_is_unknown_and_cannot_execute_or_renew(repository, tmp_path):
    store, scope, workspace = await setup(repository, tmp_path)
    try:
        plan = WorkspaceDirectoryWriter(workspace).prepare(
            DirectoryChangeSpec(path="empty", operation="create", mode=0o750)
        )
        proposal, _ = await store.preview(scope, "files", workspace.fingerprint, "one", plan)
        operation, _ = await store.reserve(
            scope, request(proposal.view), workspace.fingerprint, plan
        )
        async with store.database.sessions.begin() as sql:
            await sql.execute(
                update(WorkspaceDirectoryAttemptRow)
                .where(WorkspaceDirectoryAttemptRow.operation_id == operation.view.operation_id)
                .values(lease_expires_at=time.time() - 1)
            )
        expired = await store.operation(scope, operation.view.operation_id)
        assert expired.view.status == "uncertain"
        for action in [store.assert_execution, store.renew]:
            with pytest.raises(ServiceError) as rejected:
                await action(scope, operation.view.operation_id)
            assert rejected.value.code == "workspace_directory_lease_lost"
        assert not (tmp_path / "empty").exists()
    finally:
        workspace.close()


async def test_corrupt_snapshot_or_changed_key_does_not_change_original_proposal(
    repository, tmp_path
):
    store, scope, workspace = await setup(repository, tmp_path)
    try:
        writer = WorkspaceDirectoryWriter(workspace)
        first = writer.prepare(DirectoryChangeSpec(path="a", operation="create", mode=0o750))
        second = writer.prepare(DirectoryChangeSpec(path="b", operation="create", mode=0o750))
        proposal, _ = await store.preview(scope, "files", workspace.fingerprint, "same", first)
        with pytest.raises(ServiceError) as conflict:
            await store.preview(scope, "files", workspace.fingerprint, "same", second)
        assert conflict.value.code == "workspace_idempotency_conflict"
        assert (await store.get(scope, proposal.view.proposal_id)).view.path == "a"
    finally:
        workspace.close()


async def test_create_and_delete_undo_must_bind_original_saved_effect(repository, tmp_path):
    store, scope, workspace = await setup(repository, tmp_path)
    try:
        writer = WorkspaceDirectoryWriter(workspace)
        for index, mode in enumerate(["create", "delete"]):
            path = "dir-" + str(index)
            if mode == "delete":
                (tmp_path / path).mkdir()
                spec = DirectoryChangeSpec(
                    path=path,
                    operation="delete",
                    expected_fingerprint=writer.capture(path).fingerprint,
                )
            else:
                spec = DirectoryChangeSpec(path=path, operation="create", mode=0o750)
            prepared = writer.prepare(spec)
            proposal, _ = await store.preview(
                scope, "files", workspace.fingerprint, "preview-" + str(index), prepared
            )
            attempt, _ = await store.reserve(
                scope,
                request(proposal.view, key="apply-" + str(index)),
                workspace.fingerprint,
                prepared,
            )
            references = []
            effect = writer.apply(prepared, record_intent=references.append)
            await store.record_intent(scope, attempt.view.operation_id, references[0])
            await store.settle(scope, attempt.view.operation_id, effect)
            saved = await store.effect(scope, attempt.view.operation_id)
            reverse = writer.prepare_undo(prepared, saved)
            current = (await store.get(scope, proposal.view.proposal_id)).view
            undone, _ = await store.reserve(
                scope,
                request(current, "undo", "undo-" + str(index)),
                workspace.fingerprint,
                reverse,
            )
            refs = []
            restored = writer.apply(reverse, record_intent=refs.append)
            await store.record_intent(scope, undone.view.operation_id, refs[0])
            result = await store.settle(scope, undone.view.operation_id, restored)
            assert result.view.status == "done"
            assert (tmp_path / path).exists() == (mode == "delete")
            assert (await store.get(scope, current.proposal_id)).view.status == "undone"
    finally:
        workspace.close()


async def test_directory_lock_blocks_same_name_and_child_file_reservations(repository, tmp_path):
    store, scope, workspace = await setup(repository, tmp_path)
    try:
        directory = WorkspaceDirectoryWriter(workspace)
        plan = directory.prepare(DirectoryChangeSpec(path="new", operation="create", mode=0o750))
        proposed, _ = await store.preview(scope, "files", workspace.fingerprint, "dir", plan)
        attempt, _ = await store.reserve(scope, request(proposed.view), workspace.fingerprint, plan)
        files = WorkspaceEditStore(store.database, owner_id=store.owner_id)
        writer = WorkspaceWriter(workspace)
        fileplan = writer.prepare(
            FileChangeSpec(path="new", operation="write", expected_sha256=None, content="file")
        )
        fileproposal, _ = await files.preview(
            scope, "files", workspace.fingerprint, "file", fileplan
        )
        with pytest.raises(ServiceError) as busy:
            await files.reserve(
                scope,
                WorkspaceChangeAction(
                    proposal_id=fileproposal.view.proposal_id,
                    action="apply",
                    expected_revision=0,
                    request_key="file-apply",
                ),
                workspace.fingerprint,
                fileplan,
            )
        assert busy.value.code == "workspace_directory_busy"
        refs = []
        effect = directory.apply(plan, record_intent=refs.append)
        await store.record_intent(scope, attempt.view.operation_id, refs[0])
        childplan = writer.prepare(
            FileChangeSpec(
                path="new/child", operation="write", expected_sha256=None, content="file"
            )
        )
        child, _ = await files.preview(scope, "files", workspace.fingerprint, "child", childplan)
        with pytest.raises(ServiceError) as parent_busy:
            await files.reserve(
                scope,
                WorkspaceChangeAction(
                    proposal_id=child.view.proposal_id,
                    action="apply",
                    expected_revision=0,
                    request_key="child-apply",
                ),
                workspace.fingerprint,
                childplan,
            )
        assert parent_busy.value.code == "workspace_directory_busy"
        await store.settle(scope, attempt.view.operation_id, effect)
    finally:
        workspace.close()


async def test_file_in_directory_blocks_directory_deletion_until_original_operation_settles(
    repository, tmp_path
):
    store, scope, workspace = await setup(repository, tmp_path)
    (tmp_path / "empty").mkdir()
    try:
        files = WorkspaceEditStore(store.database, owner_id=store.owner_id)
        fileplan = WorkspaceWriter(workspace).prepare(
            FileChangeSpec(
                path="empty/child", operation="write", expected_sha256=None, content="data"
            )
        )
        file, _ = await files.preview(scope, "files", workspace.fingerprint, "file", fileplan)
        await files.reserve(
            scope,
            WorkspaceChangeAction(
                proposal_id=file.view.proposal_id,
                action="apply",
                expected_revision=0,
                request_key="file-apply",
            ),
            workspace.fingerprint,
            fileplan,
        )
        directory = WorkspaceDirectoryWriter(workspace)
        spec = DirectoryChangeSpec(
            path="empty",
            operation="delete",
            expected_fingerprint=directory.capture("empty").fingerprint,
        )
        plan = directory.prepare(spec)
        proposed, _ = await store.preview(scope, "files", workspace.fingerprint, "delete", plan)
        with pytest.raises(ServiceError) as busy:
            await store.reserve(scope, request(proposed.view), workspace.fingerprint, plan)
        assert busy.value.code == "workspace_file_busy" and (tmp_path / "empty").exists()
    finally:
        workspace.close()


async def test_same_key_lookup_after_target_change_reads_original_without_reprepare(
    repository, tmp_path
):
    store, scope, workspace = await setup(repository, tmp_path)
    try:
        writer = WorkspaceDirectoryWriter(workspace)
        spec = DirectoryChangeSpec(path="empty", operation="create", mode=0o750)
        plan = writer.prepare(spec)
        original, _ = await store.preview(scope, "files", workspace.fingerprint, "same", plan)
        (tmp_path / "empty").mkdir()
        found = await store.find_preview(scope, "files", workspace.fingerprint, "same", spec)
        assert found.view.proposal_id == original.view.proposal_id and found.prepared == plan
        with pytest.raises(ServiceError) as conflict:
            await store.find_preview(
                scope,
                "files",
                workspace.fingerprint,
                "same",
                DirectoryChangeSpec(path="other", operation="create", mode=0o750),
            )
        assert conflict.value.code == "workspace_idempotency_conflict"
    finally:
        workspace.close()


async def test_migration_empty_roundtrip_and_nonempty_ledger_refuses_loss(repository, tmp_path):
    store, scope, workspace = await setup(repository, tmp_path)
    try:
        async with store.database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(
                    migration_config(sync), "0035_oauth_browser_callbacks"
                )
            )
            await connection.run_sync(lambda sync: command.upgrade(migration_config(sync), "head"))
        plan = WorkspaceDirectoryWriter(workspace).prepare(
            DirectoryChangeSpec(path="empty", operation="create", mode=0o750)
        )
        await store.preview(scope, "files", workspace.fingerprint, "one", plan)
        with pytest.raises(RuntimeError, match="目录条件事务账本非空"):
            async with store.database.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(
                        migration_config(sync), "0035_oauth_browser_callbacks"
                    )
                )
        async with store.database.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0041_workspace_tree_plans"
            assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
    finally:
        workspace.close()


async def test_public_registered_resource_hash_is_distinct_from_private_filesystem_hash(
    repository, tmp_path
):
    store, scope, workspace = await setup(repository, tmp_path)
    try:
        resource = hashlib.sha256(
            json.dumps(
                {"key": "files", "version": "registered-v1", "workspace": workspace.fingerprint},
                sort_keys=True,
            ).encode()
        ).hexdigest()
        assert resource != workspace.fingerprint
        plan = WorkspaceDirectoryWriter(workspace).prepare(
            DirectoryChangeSpec(path="empty", operation="create", mode=0o750)
        )
        proposal, _ = await store.preview(scope, "files", resource, "registered", plan)
        assert (
            proposal.view.resource_fingerprint == resource
            and proposal.prepared.workspace_fingerprint == workspace.fingerprint
        )
        operation, _ = await store.reserve(scope, request(proposal.view), resource, plan)
        assert operation.view.status == "running"
    finally:
        workspace.close()
