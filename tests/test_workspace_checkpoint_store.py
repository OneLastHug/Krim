"""检查点存储的原子性、完整性与真实旧schema升级。"""

import time

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import event, func, inspect, select, text, update
from test_workspace_changes import setup
from test_workspace_checkpoint_capture import captured

from krim_agent.persistence.database import (
    Base,
    NativeWorkspaceCheckpointFileRow,
    NativeWorkspaceCheckpointRow,
)
from krim_agent.persistence.migration import migration_config
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import WorkspaceCheckpointCreate
from krim_agent.workspace_writer import WorkspaceWriter


async def test_capture_row_and_entries_are_atomic_and_capacity_bound(runtime, tmp_path):
    (tmp_path / "file.txt").write_text("hello")
    service, session, binding = await setup(runtime, tmp_path)
    store = service.workspace_checkpoint_store
    scope = f"session:{session.session_id}"

    def fail_entry(_conn, _cursor, statement, _parameters, _context, _many):
        if statement.startswith("INSERT INTO native_workspace_checkpoint_files"):
            raise RuntimeError("entry write unavailable")

    try:
        event.listen(store.database.engine.sync_engine, "before_cursor_execute", fail_entry)
        with pytest.raises(RuntimeError, match="entry write"):
            await store.reserve(
                scope,
                WorkspaceCheckpointCreate(paths=["file.txt"], request_key="atomic"),
                binding.key,
                binding.fingerprint,
                binding.workspace.fingerprint,
            )
        event.remove(store.database.engine.sync_engine, "before_cursor_execute", fail_entry)
        async with store.database.write_lock, store.database.sessions() as sql:
            assert (
                await sql.scalar(select(func.count()).select_from(NativeWorkspaceCheckpointRow))
                == 0
            )
        store.max_checkpoints = 1
        ready = await captured(session, ["file.txt"])
        assert ready.status == "available"
        with pytest.raises(ServiceError) as limited:
            await captured(session, ["file.txt"], key="extra")
        assert limited.value.code == "workspace_checkpoint_limit"
        assert (await captured(session, ["file.txt"])).checkpoint_id == ready.checkpoint_id
    finally:
        if event.contains(store.database.engine.sync_engine, "before_cursor_execute", fail_entry):
            event.remove(store.database.engine.sync_engine, "before_cursor_execute", fail_entry)
        await service.close()


async def test_private_content_corruption_is_not_usable_and_view_never_loads_blob(
    runtime, tmp_path
):
    (tmp_path / "file.txt").write_text("private-content")
    service, session, _ = await setup(runtime, tmp_path)
    store = service.workspace_checkpoint_store
    try:
        ready = await captured(session, ["file.txt"])
        statements = []

        def observe(_conn, _cursor, statement, _parameters, _context, _many):
            statements.append(statement)

        event.listen(store.database.engine.sync_engine, "before_cursor_execute", observe)
        try:
            assert (await session.file_checkpoint(ready.checkpoint_id)).status == "available"
        finally:
            event.remove(store.database.engine.sync_engine, "before_cursor_execute", observe)
        assert not any("native_workspace_checkpoint_files.content" in value for value in statements)
        async with store.database.write_lock, store.database.sessions.begin() as sql:
            await sql.execute(
                update(NativeWorkspaceCheckpointFileRow)
                .where(NativeWorkspaceCheckpointFileRow.checkpoint_id == ready.checkpoint_id)
                .values(content=b"corrupted")
            )
        with pytest.raises(ServiceError) as corrupt:
            await store.captures(ready.scope_id, ready.checkpoint_id)
        assert corrupt.value.code == "workspace_checkpoint_corrupt"
        assert (tmp_path / "file.txt").read_text() == "private-content"
    finally:
        await service.close()


async def test_incomplete_and_lost_owner_never_publish_available(runtime, tmp_path):
    (tmp_path / "file.txt").write_text("hello")
    service, session, binding = await setup(runtime, tmp_path)
    store = service.workspace_checkpoint_store
    scope = f"session:{session.session_id}"
    try:
        view, _ = await store.reserve(
            scope,
            WorkspaceCheckpointCreate(paths=["file.txt"], request_key="raw"),
            binding.key,
            binding.fingerprint,
            binding.workspace.fingerprint,
        )
        fence = (await store.execution(scope, view.checkpoint_id))[0]
        with pytest.raises(ServiceError) as unfinished:
            await store.finish(scope, fence)
        assert unfinished.value.code == "workspace_checkpoint_incomplete"
        snap = WorkspaceWriter(binding.workspace).capture_file("file.txt")
        async with store.database.write_lock, store.database.sessions.begin() as sql:
            await sql.execute(
                update(NativeWorkspaceCheckpointRow)
                .where(NativeWorkspaceCheckpointRow.checkpoint_id == view.checkpoint_id)
                .values(lease_expires_at=time.time() - 1)
            )
        with pytest.raises(ServiceError) as expired:
            await store.record(scope, fence, snap)
        assert expired.value.code == "workspace_checkpoint_fenced"
        assert (await store.get(scope, view.checkpoint_id)).status == "interrupted"
        assert not (await store.get(scope, view.checkpoint_id)).files[0].captured
    finally:
        await service.close()


async def test_checkpoint_migration_roundtrip_preserves_old_workspaces_and_refuses_loss(
    runtime, tmp_path
):
    (tmp_path / "file.txt").write_text("hello")
    service, session, _ = await setup(runtime, tmp_path)
    database = runtime.repository.database
    try:
        async with database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(
                    migration_config(sync), "0027_native_workspace_batches"
                )
            )
            assert "native_workspace_checkpoints" not in await connection.run_sync(
                lambda sync: inspect(sync).get_table_names()
            )
        await database.initialize()
        checkpoint = await captured(session, ["file.txt"])
        assert checkpoint.status == "available"
        with pytest.raises(RuntimeError, match="原生文件检查点非空"):
            async with database.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(
                        migration_config(sync), "0027_native_workspace_batches"
                    )
                )
        async with database.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0029_workspace_restore_plans"
            assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()

            def compare(sync):
                def included(obj, _name, kind, _reflected, _compare_to):
                    table = obj if kind == "table" else getattr(obj, "table", None)
                    return table is not None and table.name.startswith(
                        "native_workspace_checkpoint"
                    )

                return compare_metadata(
                    MigrationContext.configure(sync, opts={"include_object": included}),
                    Base.metadata,
                )

            assert await connection.run_sync(compare) == []
        assert (await session.file_checkpoint(checkpoint.checkpoint_id)).status == "available"
    finally:
        await service.close()
