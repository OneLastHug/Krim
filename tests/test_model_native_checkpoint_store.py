"""模型检查点来源原子绑定、原Run最后资格与真实0050旧材料迁移。"""

import asyncio
from dataclasses import replace
from uuid import uuid4

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import delete, event, func, inspect, select, text, update
from sqlalchemy.exc import IntegrityError
from test_model_native_checkpoints import OneShotProvider, setup

from krim_agent.persistence.database import (
    Base,
    NativeCheckpointModelSourceRow,
    NativeWorkspaceCheckpointFileRow,
    NativeWorkspaceCheckpointRow,
    RunRow,
)
from krim_agent.persistence.migration import migration_config
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.workspace_checkpoint import CheckpointModelSource, WorkspaceCheckpointCreate
from krim_agent.workspace_writer import WorkspaceWriter


async def prepared(runtime, root):
    service, agent = await setup(runtime, root, OneShotProvider())
    store = service.workspace_checkpoint_store
    binding = service.resources.workspace("files")
    scope = "session:" + agent.session_id
    run, _ = await runtime.repository.begin_run(
        service._handle(agent.agent_id).topic_id,
        ChatRequest(text="可信Store来源专项", idempotency_key="store-model-source"),
        agent_session_id=agent.session_id,
        agent_id=agent.agent_id,
        agent_owner_id=store.owner_id,
    )
    return service, agent, store, binding, scope, CheckpointModelSource(run.id, "store-root-call")


async def reserve(store, binding, scope, source, key="source", paths=None):
    return await store.reserve(
        scope,
        WorkspaceCheckpointCreate(
            paths=paths or ["plain.txt"], request_key=key, capture_format="bytes"
        ),
        binding.key,
        binding.fingerprint,
        binding.workspace.fingerprint,
        model_source=source,
    )


async def test_parent_source_and_all_capture_rows_rollback_atomically(runtime, tmp_path):
    service, agent, store, binding, scope, source = await prepared(runtime, tmp_path)

    def unavailable(_conn, _cursor, statement, _parameters, _context, _many):
        if statement.startswith("INSERT INTO native_checkpoint_model_sources"):
            raise RuntimeError("source insert unavailable")

    try:
        event.listen(store.database.engine.sync_engine, "before_cursor_execute", unavailable)
        with pytest.raises(RuntimeError, match="source insert unavailable"):
            await reserve(store, binding, scope, source)
        event.remove(store.database.engine.sync_engine, "before_cursor_execute", unavailable)
        async with store.database.sessions() as sql:
            for table in (
                NativeWorkspaceCheckpointRow,
                NativeWorkspaceCheckpointFileRow,
                NativeCheckpointModelSourceRow,
            ):
                assert await sql.scalar(select(func.count()).select_from(table)) == 0
        view, created = await reserve(store, binding, scope, source)
        assert created and await store.source(scope, view.checkpoint_id) == source
        await store.cancel(scope, view.checkpoint_id)
    finally:
        if event.contains(store.database.engine.sync_engine, "before_cursor_execute", unavailable):
            event.remove(store.database.engine.sync_engine, "before_cursor_execute", unavailable)
        await service.close()


async def test_same_original_call_cannot_change_key_or_lose_parent_fence(runtime, tmp_path):
    service, agent, store, binding, scope, source = await prepared(runtime, tmp_path)
    try:
        original, _ = await reserve(store, binding, scope, source)
        assert (await reserve(store, binding, scope, source))[0] == original
        with pytest.raises(ServiceError) as duplicate:
            await reserve(store, binding, scope, source, key="second")
        assert duplicate.value.code == "workspace_checkpoint_model_call_conflict"
        with pytest.raises(ServiceError) as host_claim:
            await reserve(store, binding, scope, None)
        assert host_claim.value.code == "workspace_checkpoint_model_conflict"
        fence = (await store.execution(scope, original.checkpoint_id))[0]
        assert fence.model_source == source
        with pytest.raises(ServiceError) as dropped:
            await store.assert_capture(scope, replace(fence, model_source=None), "plain.txt")
        assert dropped.value.code == "workspace_checkpoint_model_context"
        await store.cancel(scope, original.checkpoint_id)
    finally:
        await service.close()


async def test_original_model_run_stop_blocks_private_record_and_available_finish(
    runtime, tmp_path
):
    (tmp_path / "plain.txt").write_text("last-authority")
    service, agent, store, binding, scope, source = await prepared(runtime, tmp_path)
    try:
        view, _ = await reserve(store, binding, scope, source)
        fence = (await store.execution(scope, view.checkpoint_id))[0]
        capture = await asyncio.to_thread(
            WorkspaceWriter(binding.workspace).capture_file, "plain.txt", allow_binary=True
        )
        async with store.database.write_lock, store.database.sessions.begin() as sql:
            await store.edits._writer(sql)
            await sql.execute(
                update(RunRow).where(RunRow.id == source.run_id).values(stop_requested=True)
            )
        for operation in (
            lambda: store.record(scope, fence, capture),
            lambda: store.finish(scope, fence),
        ):
            with pytest.raises(ServiceError) as closed:
                await operation()
            assert closed.value.code == "workspace_run_closed"
        await store.fail(scope, fence, "original_run_stopped")
        final = await store.get(scope, view.checkpoint_id)
        assert final.status == "failed" and all(not item.captured for item in final.files)
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["delete", "marker"])
async def test_missing_or_inconsistent_model_source_never_downgrades_to_host_history(
    runtime, tmp_path, fault
):
    (tmp_path / "plain.txt").write_text("persistent-original")
    service, agent = await setup(runtime, tmp_path, OneShotProvider())
    try:
        result = await agent.execute("真实模型来源损坏测试", idempotency_key="source-corrupt")
        view = await agent.file_checkpoint_by_key("one-root")
        assert result.status == "completed" and view.status == "available"
        database = runtime.repository.database
        async with database.write_lock, database.sessions.begin() as sql:
            if fault == "delete":
                await sql.execute(
                    delete(NativeCheckpointModelSourceRow).where(
                        NativeCheckpointModelSourceRow.checkpoint_id == view.checkpoint_id
                    )
                )
            else:
                await sql.execute(
                    update(NativeWorkspaceCheckpointRow)
                    .where(NativeWorkspaceCheckpointRow.checkpoint_id == view.checkpoint_id)
                    .values(model_bound=False)
                )
        with pytest.raises(ServiceError) as corrupt:
            await agent.file_checkpoint(view.checkpoint_id)
        assert corrupt.value.code == "workspace_checkpoint_corrupt"
    finally:
        await service.close()


async def test_0050_empty_source_roundtrip_preserves_original_host_hash_blob_json_and_fk(
    runtime, tmp_path
):
    (tmp_path / "plain.txt").write_bytes(b"old-host\0\xff")
    service, agent = await setup(runtime, tmp_path, OneShotProvider())
    database = runtime.repository.database
    try:
        start = await agent.create_file_checkpoint(
            WorkspaceCheckpointCreate(
                paths=["plain.txt"], request_key="host-original", capture_format="bytes"
            )
        )
        original = await agent.wait_file_checkpoint(start.checkpoint_id)
        assert original.status == "available"
        async with database.engine.connect() as sql:
            before = (
                await sql.execute(text("SELECT request_hash FROM native_workspace_checkpoints"))
            ).all()
            blobs = (
                await sql.execute(
                    text("SELECT capture_metadata,content FROM native_workspace_checkpoint_files")
                )
            ).all()
        async with database.engine.begin() as sql:
            await sql.run_sync(
                lambda sync: command.downgrade(
                    migration_config(sync), "0049_checkpoint_restore_bytes"
                )
            )
            assert "model_bound" not in await sql.run_sync(
                lambda sync: {
                    item["name"]
                    for item in inspect(sync).get_columns("native_workspace_checkpoints")
                }
            )
        await database.initialize()
        assert await agent.file_checkpoint(original.checkpoint_id) == original
        async with database.engine.connect() as sql:
            assert (
                await sql.execute(text("SELECT request_hash FROM native_workspace_checkpoints"))
            ).all() == before
            assert (
                await sql.execute(
                    text("SELECT capture_metadata,content FROM native_workspace_checkpoint_files")
                )
            ).all() == blobs
            assert (await sql.execute(text("PRAGMA foreign_key_check"))).all() == []

            def compare(sync):
                def include(obj, _name, kind, _reflected, _other):
                    table = obj if kind == "table" else getattr(obj, "table", None)
                    return table is not None and table.name in {
                        "native_workspace_checkpoints",
                        "native_workspace_checkpoint_files",
                        "native_checkpoint_model_sources",
                    }

                return compare_metadata(
                    MigrationContext.configure(sync, opts={"include_object": include}),
                    Base.metadata,
                )

            assert await sql.run_sync(compare) == []
    finally:
        await service.close()


async def test_0050_model_sources_refuse_lossy_downgrade_with_original_material_unchanged(
    runtime, tmp_path
):
    (tmp_path / "plain.txt").write_text("do-not-downgrade")
    service, agent = await setup(runtime, tmp_path, OneShotProvider())
    database = runtime.repository.database
    try:
        result = await agent.execute("模型来源不可降级丢失", idempotency_key="downgrade")
        view = await agent.file_checkpoint_by_key("one-root")
        assert result.status == "completed" and view.status == "available"
        with pytest.raises(RuntimeError, match="原Run/call来源"):
            async with database.engine.begin() as sql:
                await sql.run_sync(
                    lambda sync: command.downgrade(
                        migration_config(sync), "0049_checkpoint_restore_bytes"
                    )
                )
        async with database.engine.connect() as sql:
            assert (
                await sql.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0055_directory_restore_plans"
            assert (await sql.execute(text("PRAGMA foreign_key_check"))).all() == []
        assert await agent.file_checkpoint(view.checkpoint_id) == view
    finally:
        await service.close()


async def test_actual_sql_source_unique_fk_and_model_marker_constraints(runtime, tmp_path):
    service, agent, store, binding, scope, source = await prepared(runtime, tmp_path)
    try:
        view, _ = await reserve(store, binding, scope, source)
        host, _ = await reserve(store, binding, scope, None, key="host-target")
        for statement, parameters in [
            (
                "INSERT INTO native_checkpoint_model_sources(checkpoint_id,run_id,call_id) "
                "VALUES(:checkpoint,:run,:call)",
                {"checkpoint": host.checkpoint_id, "run": source.run_id, "call": source.call_id},
            ),
            (
                "INSERT INTO native_checkpoint_model_sources(checkpoint_id,run_id,call_id) "
                "VALUES(:checkpoint,:run,:call)",
                {"checkpoint": str(uuid4()), "run": source.run_id, "call": "orphan"},
            ),
            (
                "UPDATE native_workspace_checkpoints SET model_bound=2 "
                "WHERE checkpoint_id=:checkpoint",
                {"checkpoint": view.checkpoint_id},
            ),
        ]:
            with pytest.raises(IntegrityError):
                async with store.database.engine.begin() as sql:
                    await sql.execute(text(statement), parameters)
        await store.cancel(scope, view.checkpoint_id)
        await store.cancel(scope, host.checkpoint_id)
    finally:
        await service.close()


async def test_last_sql_budget_guard_rolls_back_admission_record_and_available_finish(
    runtime, tmp_path
):
    (tmp_path / "plain.txt").write_text("sql-budget")
    service, agent, store, binding, scope, source = await prepared(runtime, tmp_path)

    def expired():
        # 该回调必须纯同步；不能重入持有中的Database锁。
        assert store.database.write_lock.locked()
        raise ServiceError("tool_timeout", "原模型预算已结束", 408)

    try:
        request = WorkspaceCheckpointCreate(
            paths=["plain.txt"], request_key="sql-budget", capture_format="bytes"
        )
        with pytest.raises(ServiceError) as rejected:
            await store.reserve(
                scope,
                request,
                binding.key,
                binding.fingerprint,
                binding.workspace.fingerprint,
                model_source=source,
                authorize=expired,
            )
        assert rejected.value.code == "tool_timeout"
        assert await store.by_key(scope, request.request_key) is None
        view, _ = await store.reserve(
            scope,
            request,
            binding.key,
            binding.fingerprint,
            binding.workspace.fingerprint,
            model_source=source,
        )
        fence = (await store.execution(scope, view.checkpoint_id))[0]
        capture = await asyncio.to_thread(
            WorkspaceWriter(binding.workspace).capture_file, "plain.txt", allow_binary=True
        )
        with pytest.raises(ServiceError) as late_record:
            await store.record(scope, fence, capture, authorize=expired)
        assert (
            late_record.value.code == "tool_timeout" and await store.captured(scope, fence) is None
        )
        await store.record(scope, fence, capture)
        with pytest.raises(ServiceError) as late_finish:
            await store.finish(scope, fence, authorize=expired)
        assert late_finish.value.code == "tool_timeout"
        assert (await store.get(scope, view.checkpoint_id)).status == "capturing"
        await store.cancel(scope, view.checkpoint_id)
    finally:
        await service.close()
