"""恢复账本的事务绑定、并发幂等、配额、作用域与迁移。"""

import asyncio
import time
from dataclasses import replace

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import event, func, inspect, select, text, update
from test_workspace_changes import setup
from test_workspace_checkpoint_capture import captured
from test_workspace_checkpoint_restore import restore_execute, restore_preview

from krim_agent.persistence.database import (
    Base,
    NativeWorkspaceCheckpointFileRow,
    WorkspaceEditAttemptRow,
    WorkspaceEditProposalRow,
    WorkspaceRestoreItemRow,
    WorkspaceRestorePlanRow,
)
from krim_agent.persistence.migration import migration_config
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import AgentSession, CheckpointRestoreExecute, CheckpointRestorePreview


async def test_preview_and_execute_cas_scope_keys_and_closed_history(runtime, tmp_path):
    file = tmp_path / "file.txt"
    file.write_text("A")
    service, session, _ = await setup(runtime, tmp_path)
    try:
        checkpoint = await captured(session, [file.name])
        file.write_text("B")
        plans = await asyncio.gather(*(restore_preview(session, checkpoint) for _ in range(4)))
        assert len({plan.plan_id for plan in plans}) == 1
        plan = plans[0]
        assert len(await session.file_checkpoints()) == 2
        assert len(await session.file_changes()) == 1
        other = AgentSession(service, (await service.spawn("default")).agent_id)
        for read in (
            other.file_checkpoint_restore(plan.plan_id),
            other.preview_file_checkpoint_restore(
                CheckpointRestorePreview(
                    checkpoint_id=checkpoint.checkpoint_id, request_key="hidden"
                )
            ),
        ):
            with pytest.raises(ServiceError) as error:
                await read
            assert error.value.status == 404
        with pytest.raises(ServiceError) as conflict:
            await restore_preview(session, checkpoint, ttl_seconds=1)
        assert conflict.value.code == "workspace_restore_key_conflict"
        with pytest.raises(ServiceError) as version:
            await session.execute_file_checkpoint_restore(
                CheckpointRestoreExecute(
                    plan_id=plan.plan_id, request_key="bad-revision", expected_revision=0
                )
            )
        assert version.value.code == "workspace_restore_revision"
        done = await asyncio.gather(*(restore_execute(session, plan) for _ in range(4)))
        assert all(item.status == "done" for item in done), done
        assert len({item.items[0].operation.operation_id for item in done}) == 1
        state = await session.state()
        await service.close_session(session.agent_id, state.revision)
        assert (await session.file_checkpoint_restore(plan.plan_id)).status == "done"
        assert (await session.file_checkpoint_restores())[0].status == "done"
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["item", "proposal"])
async def test_plan_items_and_derived_proposals_are_atomic(runtime, tmp_path, fault):
    file = tmp_path / "file.txt"
    file.write_text("A")
    service, session, binding = await setup(runtime, tmp_path)
    store = service.workspace_restore_store
    checkpoint = await captured(session, [file.name])
    file.write_text("B")

    def fail(_conn, _cursor, statement, _parameters, _context, _many):
        if statement.startswith(
            "INSERT INTO "
            + ("workspace_restore_items" if fault == "item" else "workspace_edit_proposals")
        ):
            raise RuntimeError("simulated insert failure")

    event.listen(store.database.engine.sync_engine, "before_cursor_execute", fail)
    try:
        if fault == "item":
            with pytest.raises(RuntimeError, match="insert failure"):
                await store.reserve(
                    checkpoint.scope_id,
                    CheckpointRestorePreview(
                        checkpoint_id=checkpoint.checkpoint_id, request_key="atomic"
                    ),
                    binding.key,
                    binding.fingerprint,
                    binding.workspace.fingerprint,
                )
        else:
            plan = await restore_preview(session, checkpoint)
            assert plan.phase == "failed"
            assert plan.baseline_checkpoint_id is not None
        async with store.database.write_lock, store.database.sessions() as sql:
            assert await sql.scalar(select(func.count()).select_from(WorkspaceEditProposalRow)) == 0
            assert await sql.scalar(select(func.count()).select_from(WorkspaceRestorePlanRow)) == (
                0 if fault == "item" else 1
            )
            assert not list(
                await sql.scalars(
                    select(WorkspaceRestoreItemRow.proposal_id).where(
                        WorkspaceRestoreItemRow.proposal_id.is_not(None)
                    )
                )
            )
        assert file.read_text() == "B"
    finally:
        event.remove(store.database.engine.sync_engine, "before_cursor_execute", fail)
        await service.close()


async def test_restore_reserve_and_child_link_are_same_transaction(runtime, tmp_path, monkeypatch):
    file = tmp_path / "file.txt"
    file.write_text("A")
    service, session, binding = await setup(runtime, tmp_path)
    store = service.workspace_restore_store
    try:
        checkpoint = await captured(session, [file.name])
        file.write_text("B")
        plan = await restore_preview(session, checkpoint)
        await store.start(
            plan.scope_id,
            CheckpointRestoreExecute(
                plan_id=plan.plan_id, expected_revision=plan.revision, request_key="execute"
            ),
            binding.key,
            binding.fingerprint,
            binding.workspace.fingerprint,
        )
        dispatch = (await store.execution(plan.scope_id, plan.plan_id))[0]
        original = await service.workspace_edits.get(plan.scope_id, dispatch.request.proposal_id)
        with pytest.raises(ServiceError) as forged:
            await service.workspace_edits.reserve(
                plan.scope_id,
                dispatch.request,
                binding.fingerprint,
                original.prepared,
                restore_fence=replace(dispatch.fence, token="x" * 64),
            )
        assert forged.value.code == "workspace_restore_target"

        def fail(_conn, _cursor, statement, _parameters, _context, _many):
            if statement.startswith("UPDATE workspace_restore_items SET operation_id"):
                raise RuntimeError("link write failed")

        event.listen(store.database.engine.sync_engine, "before_cursor_execute", fail)
        try:
            with pytest.raises(RuntimeError, match="link write failed"):
                await service.workspace_edits.reserve(
                    plan.scope_id,
                    dispatch.request,
                    binding.fingerprint,
                    original.prepared,
                    restore_fence=dispatch.fence,
                )
        finally:
            event.remove(store.database.engine.sync_engine, "before_cursor_execute", fail)
        async with store.database.write_lock, store.database.sessions() as sql:
            assert await sql.scalar(select(func.count()).select_from(WorkspaceEditAttemptRow)) == 0
        assert (await session.file_change(dispatch.request.proposal_id)).status == "proposed"
        attempt, created = await service.workspace_edits.reserve(
            plan.scope_id,
            dispatch.request,
            binding.fingerprint,
            original.prepared,
            restore_fence=dispatch.fence,
        )
        assert created
        assert (await session.file_checkpoint_restore(plan.plan_id)).items[
            0
        ].operation.operation_id == attempt.view.operation_id
        # 原键只能读取原操作，即使父计划随后被取消也不能认领另一个操作。
        await store.cancel(plan.scope_id, plan.plan_id)
        repeated, created = await service.workspace_edits.reserve(
            plan.scope_id,
            dispatch.request,
            binding.fingerprint,
            original.prepared,
            restore_fence=dispatch.fence,
        )
        assert not created and repeated.view.operation_id == attempt.view.operation_id
        assert file.read_text() == "B"
    finally:
        await service.close()


@pytest.mark.parametrize("limit", ["plans", "proposals", "bytes"])
async def test_restore_respects_plan_and_private_snapshot_capacity(runtime, tmp_path, limit):
    file = tmp_path / "file.txt"
    file.write_text("A" * 1500)
    service, session, _ = await setup(runtime, tmp_path)
    try:
        checkpoint = await captured(session, [file.name])
        file.write_text("B" * 1500)
        if limit == "plans":
            service.workspace_restore_store.max_plans = 0
            with pytest.raises(ServiceError) as error:
                await restore_preview(session, checkpoint)
            assert error.value.code == "workspace_restore_limit"
        else:
            if limit == "proposals":
                service.workspace_edits.max_changes = 0
            else:
                service.workspace_edits.max_snapshot_bytes = 1024
            plan = await restore_preview(session, checkpoint)
            assert plan.phase == "failed"
            assert plan.error_code == (
                "workspace_proposal_limit" if limit == "proposals" else "workspace_snapshot_limit"
            )
        assert not await session.file_changes()
        assert file.read_text() == "B" * 1500
    finally:
        await service.close()


async def test_ready_plan_expiry_corrupt_source_and_resource_version(runtime, tmp_path):
    file = tmp_path / "file.txt"
    file.write_text("A")
    service, session, binding = await setup(runtime, tmp_path)
    store = service.workspace_restore_store
    try:
        checkpoint = await captured(session, [file.name])
        file.write_text("B")
        with pytest.raises(ServiceError) as binding_error:
            await store.reserve(
                checkpoint.scope_id,
                CheckpointRestorePreview(
                    checkpoint_id=checkpoint.checkpoint_id, request_key="wrong-resource"
                ),
                binding.key,
                binding.fingerprint,
                "0" * 64,
            )
        assert binding_error.value.code == "workspace_binding_changed"
        plan = await restore_preview(session, checkpoint)
        async with store.database.write_lock, store.database.sessions.begin() as sql:
            await sql.execute(
                update(WorkspaceRestorePlanRow)
                .where(WorkspaceRestorePlanRow.plan_id == plan.plan_id)
                .values(expires_at=time.time() - 1)
            )
        assert (await session.file_checkpoint_restore(plan.plan_id)).phase == "expired"
        with pytest.raises(ServiceError) as expired:
            await restore_execute(session, plan)
        assert expired.value.code == "workspace_restore_state"
        async with store.database.write_lock, store.database.sessions.begin() as sql:
            await sql.execute(
                update(NativeWorkspaceCheckpointFileRow)
                .where(NativeWorkspaceCheckpointFileRow.checkpoint_id == checkpoint.checkpoint_id)
                .values(content=b"corruption")
            )
        corrupt = await restore_preview(session, checkpoint, key="corrupt")
        assert corrupt.phase == "failed" and corrupt.error_code == "workspace_checkpoint_corrupt"
        assert file.read_text() == "B"
    finally:
        await service.close()


async def test_restore_migration_0028_roundtrip_metadata_and_nonempty_guard(runtime, tmp_path):
    file = tmp_path / "file.txt"
    file.write_text("A")
    service, session, _ = await setup(runtime, tmp_path)
    database = runtime.repository.database
    try:
        checkpoint = await captured(session, [file.name])
        async with database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(
                    migration_config(sync), "0028_native_workspace_checkpoints"
                )
            )
            assert "workspace_restore_plans" not in await connection.run_sync(
                lambda sync: inspect(sync).get_table_names()
            )
        await database.initialize()
        file.write_text("B")
        plan = await restore_preview(session, checkpoint)
        assert (await restore_execute(session, plan)).status == "done"
        with pytest.raises(RuntimeError, match="恢复计划非空"):
            async with database.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(
                        migration_config(sync), "0028_native_workspace_checkpoints"
                    )
                )
        async with database.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0046_tree_exchange_origins"
            assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()

            def compare(sync):
                def included(obj, _name, kind, _reflected, _compare_to):
                    table = obj if kind == "table" else getattr(obj, "table", None)
                    return table is not None and table.name.startswith("workspace_restore_")

                return compare_metadata(
                    MigrationContext.configure(sync, opts={"include_object": included}),
                    Base.metadata,
                )

            assert await connection.run_sync(compare) == []
        assert (await session.file_checkpoint_restore(plan.plan_id)).status == "done"
    finally:
        await service.close()
