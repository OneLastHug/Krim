"""真实提案快照、作用域、SQL CAS/幂等、租约、未核实锁与降级保护。"""

import asyncio
import time
from dataclasses import replace

import pytest
from sqlalchemy import func, select, text, update

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentService
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.database import (
    WorkspaceEditAttemptRow,
    WorkspaceEditProposalRow,
    WorkspaceFileLockRow,
)
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_change import FileChangeReceipt, FileChangeSpec, WorkspaceChangeAction
from krim_agent.workspace_writer import FileRecovery, WorkspaceWriter


async def setup_store(tmp_path, *, lease=60, max_changes=500, snapshot_limit=67_108_864):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'edits.sqlite'}")
    await database.initialize()
    topic = await KrimRepository(database).create_topic("文件任务")
    root = tmp_path / "files"
    root.mkdir()
    (root / "file.txt").write_text("old\n", encoding="utf-8")
    workspace = Workspace(root, writable=True)
    store = WorkspaceEditStore(
        database, lease_seconds=lease, max_changes=max_changes, max_snapshot_bytes=snapshot_limit
    )
    return database, store, workspace, "topic:" + topic.id


def plan(workspace, value="new\n", path="file.txt"):
    return WorkspaceWriter(workspace).prepare(
        FileChangeSpec(
            path=path,
            operation="write",
            expected_sha256=workspace.read(path).sha256,
            content=value,
        )
    )


async def preview(store, workspace, scope, key="preview", value="new\n"):
    prepared = plan(workspace, value)
    change, created = await store.preview(scope, "files", workspace.fingerprint, key, prepared)
    return prepared, change, created


def action(change, key="apply", mode="apply", revision=None):
    return WorkspaceChangeAction(
        proposal_id=change.view.proposal_id,
        action=mode,
        expected_revision=change.view.revision if revision is None else revision,
        request_key=key,
    )


def done(prepared, *, status="done", code=None):
    return FileChangeReceipt(
        path=prepared.spec.path,
        status=status,
        before_sha256=prepared.before_sha256,
        after_sha256=prepared.after_sha256,
        changed=True,
        error_code=code,
        recovery_required=status == "uncertain",
    )


async def test_snapshot_freezing_replay_and_content_is_private(tmp_path):
    database, store, workspace, scope = await setup_store(tmp_path)
    try:
        prepared, change, created = await preview(store, workspace, scope)
        assert created and change.view.status == "proposed" and change.view.revision == 0
        assert change.prepared == prepared
        public = change.view.model_dump_json()
        assert (
            "before_content" not in public
            and "spec_json" not in public
            and "stage_name" not in public
        )
        (workspace.root / "file.txt").write_text("external", encoding="utf-8")
        replay = await store.find_preview(scope, "preview", prepared.spec, workspace.fingerprint)
        assert replay.prepared.before.data == b"old\n" and replay.prepared.after == b"new\n"
        second, created = await store.preview(
            scope, "files", workspace.fingerprint, "preview", prepared
        )
        assert not created and second.view.proposal_id == change.view.proposal_id
        with pytest.raises(ServiceError) as conflict:
            await store.find_preview(
                scope,
                "preview",
                prepared.spec.model_copy(update={"content": "other"}),
                workspace.fingerprint,
            )
        assert conflict.value.code == "workspace_idempotency_conflict"
        assert (await store.list(scope))[0].proposal_id == change.view.proposal_id
    finally:
        workspace.close()
        await database.close()


async def test_reserve_settle_replay_and_real_file_result(tmp_path):
    database, store, workspace, scope = await setup_store(tmp_path)
    try:
        prepared, change, _ = await preview(store, workspace, scope)
        request = action(change)
        attempted, created = await store.reserve(scope, request, workspace.fingerprint, prepared)
        assert created and attempted.view.status == "running" and attempted.recovery is not None
        assert (await store.get(scope, change.view.proposal_id)).view.status == "applying"
        replay, created = await store.reserve(scope, request, workspace.fingerprint, prepared)
        assert not created and replay.view.operation_id == attempted.view.operation_id
        writer = WorkspaceWriter(workspace)
        effect = writer.apply(prepared)
        settled = await store.settle(
            scope, attempted.view.operation_id, effect.receipt, effect.recovery
        )
        assert settled.view.status == "done" and settled.view.cleanup_pending
        assert (workspace.root / "file.txt").read_text() == "new\n"
        assert (await store.get(scope, change.view.proposal_id)).view.status == "applied"
        writer.finalize(prepared, settled.recovery)
        assert not (
            await store.mark_cleaned(scope, attempted.view.operation_id)
        ).view.cleanup_pending
        again = await store.find_action(scope, request, workspace.fingerprint)
        assert again.view.status == "done"
        assert [item.status for item in await store.history(scope, change.view.proposal_id)] == [
            "done"
        ]
        with pytest.raises(ServiceError):
            await store.reserve(
                scope,
                request.model_copy(update={"expected_revision": 2}),
                workspace.fingerprint,
                prepared,
            )
    finally:
        workspace.close()
        await database.close()


async def test_two_connections_accept_only_one_revision_and_same_key_replays(tmp_path):
    database, store, workspace, scope = await setup_store(tmp_path)
    remote_db = Database(str(database.engine.url))
    await remote_db.initialize()
    remote = WorkspaceEditStore(remote_db)
    try:
        prepared, change, _ = await preview(store, workspace, scope)
        results = await asyncio.gather(
            store.reserve(scope, action(change, "one"), workspace.fingerprint, prepared),
            remote.reserve(scope, action(change, "two"), workspace.fingerprint, prepared),
            return_exceptions=True,
        )
        assert sum(not isinstance(value, Exception) for value in results) == 1
        assert sum(isinstance(value, ServiceError) for value in results) == 1
        async with database.sessions() as session:
            assert (
                await session.scalar(select(func.count()).select_from(WorkspaceEditAttemptRow)) == 1
            )
            assert await session.scalar(select(func.count()).select_from(WorkspaceFileLockRow)) == 1
    finally:
        workspace.close()
        await remote_db.close()
        await database.close()


async def test_expired_lease_becomes_uncertain_live_producer_is_untouched(tmp_path):
    database, store, workspace, scope = await setup_store(tmp_path)
    try:
        prepared, change, _ = await preview(store, workspace, scope)
        attempt, _ = await store.reserve(scope, action(change), workspace.fingerprint, prepared)
        assert await store.recover_interrupted() == 0
        await store.renew(attempt.view.operation_id)
        async with database.sessions.begin() as session:
            await session.execute(
                update(WorkspaceEditAttemptRow)
                .where(WorkspaceEditAttemptRow.operation_id == attempt.view.operation_id)
                .values(lease_expires_at=time.time() - 1)
            )
        assert await store.recover_interrupted() == 1
        assert await store.recover_interrupted() == 0
        recovered = await store.operation(scope, attempt.view.operation_id)
        assert recovered.view.status == "uncertain"
        assert recovered.view.receipt.path == "file.txt"
        assert recovered.view.receipt.before_sha256 == prepared.before_sha256
        assert recovered.recovery.stage_name == "._krim-tx-" + prepared.token
        other_prepared, other_change, _ = await preview(store, workspace, scope, "other", "another")
        with pytest.raises(ServiceError) as blocked:
            await store.reserve(
                scope, action(other_change, "second"), workspace.fingerprint, other_prepared
            )
        assert blocked.value.code == "workspace_file_busy"
        receipt = done(prepared, status="rejected", code="confirmed_no_effect")
        await store.settle(scope, attempt.view.operation_id, receipt, resolve=True)
        assert (await store.get(scope, change.view.proposal_id)).view.status == "rejected"
        assert (
            await store.reserve(
                scope, action(other_change, "second"), workspace.fingerprint, other_prepared
            )
        )[1]
    finally:
        workspace.close()
        await database.close()


async def test_wrong_owner_receipt_target_resource_and_corrupt_snapshot_are_rejected(tmp_path):
    database, store, workspace, scope = await setup_store(tmp_path)
    remote = WorkspaceEditStore(database)
    try:
        prepared, change, _ = await preview(store, workspace, scope)
        with pytest.raises(ServiceError) as resource:
            await store.reserve(scope, action(change), "0" * 64, prepared)
        assert resource.value.code == "workspace_binding_changed"
        attempt, _ = await store.reserve(scope, action(change), workspace.fingerprint, prepared)
        with pytest.raises(ServiceError) as owner:
            await remote.settle(scope, attempt.view.operation_id, done(prepared))
        assert owner.value.code == "workspace_operation_owner"
        with pytest.raises(ServiceError) as receipt:
            await store.settle(
                scope,
                attempt.view.operation_id,
                done(prepared).model_copy(update={"path": "other"}),
            )
        assert receipt.value.code == "workspace_receipt_mismatch"
        with pytest.raises(ServiceError):
            await store.settle(
                scope,
                attempt.view.operation_id,
                done(prepared),
                FileRecovery(
                    workspace.fingerprint,
                    "file.txt",
                    "._krim-tx-" + "0" * 32,
                    prepared.parent_identity,
                ),
            )
        async with database.sessions.begin() as session:
            row = await session.get(WorkspaceEditProposalRow, change.view.proposal_id)
            row.before_content = b"tampered"
        with pytest.raises(ServiceError) as corrupt:
            await store.get(scope, change.view.proposal_id)
        assert corrupt.value.code == "workspace_snapshot_corrupt"
    finally:
        workspace.close()
        await database.close()


async def test_invalid_prepared_new_content_and_preview_ttl_and_capacity(tmp_path):
    database, store, workspace, scope = await setup_store(tmp_path, max_changes=1)
    try:
        prepared = plan(workspace)
        with pytest.raises(ServiceError) as forged:
            await store.preview(
                scope,
                "files",
                workspace.fingerprint,
                "forged",
                replace(prepared, after=b"arbitrary"),
            )
        assert forged.value.code == "workspace_snapshot_corrupt"
        change, _ = await store.preview(
            scope, "files", workspace.fingerprint, "first", prepared, ttl_seconds=0.01
        )
        async with database.sessions.begin() as session:
            row = await session.get(WorkspaceEditProposalRow, change.view.proposal_id)
            row.expires_at = 1
        with pytest.raises(ServiceError) as expired:
            await store.reserve(scope, action(change), workspace.fingerprint, prepared)
        assert expired.value.code == "workspace_proposal_expired"
        with pytest.raises(ServiceError) as capacity:
            await store.preview(scope, "files", workspace.fingerprint, "second", prepared)
        assert capacity.value.code == "workspace_proposal_limit"
        tiny = WorkspaceEditStore(database, max_snapshot_bytes=1024)
        large = plan(workspace, "x" * 2000)
        with pytest.raises(ServiceError) as bytes_limit:
            await tiny.preview(scope, "files", workspace.fingerprint, "large", large)
        assert bytes_limit.value.code == "workspace_snapshot_limit"
    finally:
        workspace.close()
        await database.close()


async def test_topic_run_and_session_permission_boundaries(runtime, tmp_path):
    root = tmp_path / "files"
    root.mkdir()
    (root / "file.txt").write_text("old\n", encoding="utf-8")
    service = AgentService(runtime, runtime.settings)
    try:
        binding = service.register_workspace("files", root, version="v1", writable=True)
        await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name="writer",
                    description="作用域",
                    workspace_key="files",
                    tool_names=["workspace_read"],
                ),
            )
        )
        agent = await service.spawn("writer")
        scope = "session:" + agent.session_id
        store = WorkspaceEditStore(runtime.repository.database, owner_id=service.manager.owner_id)
        prepared = plan(binding.workspace)
        change, _ = await store.preview(scope, "files", binding.fingerprint, "preview", prepared)
        with pytest.raises(ServiceError) as managed:
            await store.preview(
                "topic:" + agent.topic_id, "files", binding.fingerprint, "invalid", prepared
            )
        assert managed.value.code == "workspace_scope_managed"
        with pytest.raises(ServiceError) as root_version:
            await store.preview(scope, "files", "0" * 64, "different-root", prepared)
        assert root_version.value.code == "workspace_binding_changed"
        foreign = WorkspaceEditStore(runtime.repository.database)
        with pytest.raises(ServiceError) as owner:
            await foreign.preview(scope, "files", binding.fingerprint, "foreign", prepared)
        assert owner.value.code == "agent_session_owner"
        normal_topic = await runtime.repository.create_topic("其他任务")
        run, _ = await runtime.repository.begin_run(
            normal_topic.id, ChatRequest(text="x", idempotency_key="x")
        )
        with pytest.raises(ServiceError) as wrong_run:
            await store.preview(scope, "files", binding.fingerprint, "run", prepared, run_id=run.id)
        assert wrong_run.value.code == "workspace_run_mismatch"
        await service.close_session(agent.agent_id, 0)
        assert (await store.get(scope, change.view.proposal_id)).view.status == "proposed"
        with pytest.raises(ServiceError) as closed:
            await store.reserve(scope, action(change), binding.fingerprint, prepared)
        assert closed.value.code == "agent_session_closed"
    finally:
        await service.close()


async def test_database_reopen_retains_exact_private_snapshot_and_migration_protection(tmp_path):
    from alembic import command

    from krim_agent.persistence.migration import migration_config

    database, store, workspace, scope = await setup_store(tmp_path)
    prepared, change, _ = await preview(store, workspace, scope)
    attempt, _ = await store.reserve(scope, action(change), workspace.fingerprint, prepared)
    await database.close()
    cold_db = Database(f"sqlite+aiosqlite:///{tmp_path / 'edits.sqlite'}")
    await cold_db.initialize()
    cold = WorkspaceEditStore(cold_db)
    try:
        assert (await cold.get(scope, change.view.proposal_id)).prepared == prepared
        assert (
            await cold.operation(scope, attempt.view.operation_id)
        ).recovery.stage_name == "._krim-tx-" + prepared.token
        async with cold_db.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar() == "0055_directory_restore_plans"
            assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
        with pytest.raises(RuntimeError, match="文件修改"):
            async with cold_db.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(migration_config(sync), "0023_agent_tasks")
                )
    finally:
        workspace.close()
        await cold_db.close()


async def test_real_apply_keep_and_version_checked_undo_are_distinct_facts(tmp_path):
    database, store, workspace, scope = await setup_store(tmp_path)
    try:
        writer = WorkspaceWriter(workspace)
        prepared, change, _ = await preview(store, workspace, scope)
        attempted, _ = await store.reserve(scope, action(change), workspace.fingerprint, prepared)
        applied = writer.apply(prepared)
        await store.settle(scope, attempted.view.operation_id, applied.receipt, applied.recovery)
        current = await store.get(scope, change.view.proposal_id)
        keep, _ = await store.reserve(
            scope, action(current, "keep", "keep"), workspace.fingerprint, prepared
        )
        await store.settle(
            scope, keep.view.operation_id, done(prepared).model_copy(update={"changed": False})
        )
        kept = await store.get(scope, change.view.proposal_id)
        assert kept.view.status == "kept"
        reverse = writer.prepare(
            FileChangeSpec(
                path="file.txt",
                operation="write",
                expected_sha256=prepared.after_sha256,
                content="old\n",
            )
        )
        undo, _ = await store.reserve(
            scope, action(kept, "undo", "undo"), workspace.fingerprint, reverse
        )
        undone = writer.apply(reverse)
        await store.settle(scope, undo.view.operation_id, undone.receipt, undone.recovery)
        assert (workspace.root / "file.txt").read_text() == "old\n"
        assert (await store.get(scope, change.view.proposal_id)).view.status == "undone"
        assert [item.action for item in await store.history(scope, change.view.proposal_id)] == [
            "apply",
            "keep",
            "undo",
        ]
    finally:
        workspace.close()
        await database.close()


async def test_undo_cannot_substitute_another_version_and_cleanup_requires_scope_owner(
    runtime, tmp_path
):
    database, store, workspace, scope = await setup_store(tmp_path)
    try:
        prepared, change, _ = await preview(store, workspace, scope)
        attempt, _ = await store.reserve(scope, action(change), workspace.fingerprint, prepared)
        effect = WorkspaceWriter(workspace).apply(prepared)
        await store.settle(scope, attempt.view.operation_id, effect.receipt, effect.recovery)
        current = await store.get(scope, change.view.proposal_id)
        reverse = WorkspaceWriter(workspace).prepare(
            FileChangeSpec(
                path="file.txt",
                operation="write",
                expected_sha256=prepared.after_sha256,
                content="old\n",
            )
        )
        with pytest.raises(ServiceError) as invalid:
            await store.reserve(
                scope,
                action(current, "bad-undo", "undo"),
                workspace.fingerprint,
                replace(reverse, after=b"different"),
            )
        assert invalid.value.code == "workspace_proposal_changed"
        topic = await KrimRepository(database).create_topic("不同作用域")
        with pytest.raises(ServiceError):
            await store.mark_cleaned("topic:" + topic.id, attempt.view.operation_id)
        assert (await store.operation(scope, attempt.view.operation_id)).view.cleanup_pending
    finally:
        workspace.close()
        await database.close()
