"""0040原模型Run/call来源、单次调用准入、两连接竞争与原操作执行围栏。"""

import asyncio

import pytest
from alembic import command
from sqlalchemy import event, func, select, text, update
from test_mixed_workspace_batch_store import setup

from krim_agent.persistence.database import (
    Database,
    MixedWorkspaceBatchModelSourceRow,
    MixedWorkspaceBatchRow,
    RunRow,
    WorkspaceDirectoryAttemptRow,
    WorkspaceEditAttemptRow,
)
from krim_agent.persistence.migration import migration_config
from krim_agent.persistence.workspace_directories import WorkspaceDirectoryStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore, digest
from krim_agent.persistence.workspace_mixed_batches import MixedBatchStore
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.workspace_directory import WorkspaceDirectoryWriter
from krim_agent.workspace_mixed_batch import MixedBatchModelSource
from krim_agent.workspace_writer import WorkspaceWriter


async def model_setup(repository, tmp_path):
    store, scope, workspace, request = await setup(repository, tmp_path)
    run, _ = await repository.begin_run(
        scope.removeprefix("topic:"), ChatRequest(text="model mixed", idempotency_key="run")
    )
    return store, scope, workspace, request, run, MixedBatchModelSource(run.id, "model-call")


async def reserve_child(store, scope, workspace, child, run_id):
    selected = store.files if child.fence.source == "file" else store.directories
    original = await selected.get(scope, child.request.proposal_id)
    return await selected.reserve(
        scope,
        child.request,
        workspace.fingerprint,
        original.prepared,
        mixed_fence=child.fence,
        run_id=run_id,
        call_id="child-" + child.fence.source,
    )


async def counts(database):
    async with database.sessions() as sql:
        return [
            await sql.scalar(select(func.count()).select_from(table))
            for table in (
                MixedWorkspaceBatchRow,
                MixedWorkspaceBatchModelSourceRow,
                WorkspaceEditAttemptRow,
                WorkspaceDirectoryAttemptRow,
            )
        ]


async def test_run_call_source_same_transaction_real_children_and_immutable_cold_facts(
    repository, tmp_path
):
    store, scope, workspace, request, run, source = await model_setup(repository, tmp_path)
    try:
        view, created = await store.reserve(
            scope, "files", workspace.fingerprint, request, model_source=source
        )
        assert created
        execution = await store.execution(scope, view.batch_id)
        assert execution.model_source == source
        assert (execution.run_id, execution.call_id) == (source.run_id, source.call_id)
        async with store.database.sessions() as sql:
            row = await sql.get(MixedWorkspaceBatchModelSourceRow, view.batch_id)
            assert (row.run_id, row.call_id) == (run.id, "model-call")
        for child in execution.children:
            attempt, created = await reserve_child(store, scope, workspace, child, run.id)
            assert created and attempt.view.status == "running"
            selected = store.files if child.fence.source == "file" else store.directories
            await selected.assert_execution(scope, attempt.view.operation_id)
            if child.fence.source == "file":
                effect = WorkspaceWriter(workspace).apply(attempt.prepared)
                await selected.settle(
                    scope,
                    attempt.view.operation_id,
                    effect.receipt,
                    effect.recovery,
                    observed_after=effect.observed_after,
                )
            else:
                effect = WorkspaceDirectoryWriter(workspace).apply(attempt.prepared)
                await selected.settle(scope, attempt.view.operation_id, effect)
            await store.finish_child(scope, child.fence)
        done = await store.finish(scope, view.batch_id, execution.token)
        assert done.status == "done" and await counts(store.database) == [1, 1, 1, 1]
        await repository.finish(run.id, "completed", None, [])
        repeated, created = await store.reserve(
            scope, "files", workspace.fingerprint, request, model_source=source
        )
        assert not created and repeated == done
        assert (await store.history(scope)).items[0].done_count == 2
        assert (
            "model_source" not in done.model_dump_json()
            and "model-call" not in done.model_dump_json()
        )
    finally:
        workspace.close()


@pytest.mark.parametrize("source_kind", ["file", "directory"])
@pytest.mark.parametrize("case", ["missing", "foreign"])
async def test_child_must_preserve_frozen_run_before_attempt_admission(
    repository, tmp_path, source_kind, case
):
    store, scope, workspace, request, _run, origin = await model_setup(repository, tmp_path)
    try:
        view, _ = await store.reserve(
            scope, "files", workspace.fingerprint, request, model_source=origin
        )
        child = (await store.execution(scope, view.batch_id)).children[
            0 if source_kind == "file" else 1
        ]
        wanted = None
        if case == "foreign":
            await repository.finish(_run.id, "completed", None, [])
            other, _ = await repository.begin_run(
                scope.removeprefix("topic:"), ChatRequest(text="other", idempotency_key="other")
            )
            wanted = other.id
        with pytest.raises(ServiceError) as denied:
            await reserve_child(store, scope, workspace, child, wanted)
        assert denied.value.code == "workspace_mixed_batch_run_mismatch"
        assert await counts(store.database) == [1, 1, 0, 0]
    finally:
        workspace.close()


@pytest.mark.parametrize("source_kind", ["file", "directory"])
@pytest.mark.parametrize("stop", ["stop_requested", "completed"])
async def test_run_closed_rejects_late_execution_renew_finish_and_preserves_existing_attempt(
    repository, tmp_path, source_kind, stop
):
    store, scope, workspace, request, run, origin = await model_setup(repository, tmp_path)
    try:
        view, _ = await store.reserve(
            scope, "files", workspace.fingerprint, request, model_source=origin
        )
        execution = await store.execution(scope, view.batch_id)
        child = execution.children[0 if source_kind == "file" else 1]
        attempt, _ = await reserve_child(store, scope, workspace, child, run.id)
        async with store.database.sessions.begin() as sql:
            await sql.execute(
                update(RunRow)
                .where(RunRow.id == run.id)
                .values(
                    **(
                        {"stop_requested": True}
                        if stop == "stop_requested"
                        else {"status": "completed"}
                    )
                )
            )
        selected = store.files if source_kind == "file" else store.directories
        for method in ("execute", "renew", "dispatch", "finish_child", "finish"):
            with pytest.raises(ServiceError) as denied:
                if method == "execute":
                    await selected.assert_execution(scope, attempt.view.operation_id)
                elif method == "renew":
                    await store.renew(view.batch_id, execution.token)
                elif method == "dispatch":
                    await store.execution(scope, view.batch_id)
                elif method == "finish_child":
                    await store.finish_child(scope, child.fence)
                else:
                    await store.finish(scope, view.batch_id, execution.token)
            assert denied.value.code in {"workspace_run_closed", "workspace_mixed_batch_run_closed"}
        cancelled = await store.cancel(scope, view.batch_id)
        assert (
            cancelled.status == "uncertain"
            and cancelled.items[child.fence.position].operation.operation_id
            == attempt.view.operation_id
        )
        assert not (tmp_path / "file.txt").exists() and not (tmp_path / "empty").exists()
    finally:
        workspace.close()


async def test_same_model_call_cannot_change_key_or_scope_and_two_connections_accept_once(
    repository, tmp_path
):
    store, scope, workspace, request, run, origin = await model_setup(repository, tmp_path)
    second_db = Database(str(store.database.engine.url))
    second = MixedBatchStore(
        WorkspaceEditStore(second_db, owner_id=store.owner_id),
        WorkspaceDirectoryStore(second_db, owner_id=store.owner_id),
    )
    try:
        values = await asyncio.gather(
            *(
                selected.reserve(
                    scope, "files", workspace.fingerprint, request, model_source=origin
                )
                for selected in (store, second)
            )
        )
        assert sum(created for _view, created in values) == 1
        assert len({view.batch_id for view, _created in values}) == 1
        with pytest.raises(ServiceError) as repeated:
            await store.reserve(
                scope,
                "files",
                workspace.fingerprint,
                request.model_copy(update={"request_key": "other-key"}),
                model_source=origin,
            )
        assert repeated.value.code == "workspace_mixed_batch_call_conflict"
        foreign = "topic:" + (await repository.create_topic("foreign")).id
        with pytest.raises(ServiceError) as denied:
            await second.reserve(
                foreign,
                "files",
                workspace.fingerprint,
                request.model_copy(update={"request_key": "foreign"}),
                model_source=origin,
            )
        assert denied.value.code == "workspace_run_mismatch"
        assert await counts(store.database) == [1, 1, 0, 0]
    finally:
        await second_db.close()
        workspace.close()


async def test_origin_insert_failure_rolls_back_parent_children_and_source(repository, tmp_path):
    store, scope, workspace, request, _run, origin = await model_setup(repository, tmp_path)

    def fail(_conn, _cursor, statement, _params, _context, _many):
        if statement.startswith("INSERT INTO mixed_workspace_batch_model_sources"):
            raise RuntimeError("test source write failed")

    event.listen(store.database.engine.sync_engine, "before_cursor_execute", fail)
    try:
        with pytest.raises(RuntimeError, match="source write"):
            await store.reserve(scope, "files", workspace.fingerprint, request, model_source=origin)
        assert await counts(store.database) == [0, 0, 0, 0]
    finally:
        event.remove(store.database.engine.sync_engine, "before_cursor_execute", fail)
        workspace.close()


@pytest.mark.parametrize("source_kind", ["file", "directory"])
@pytest.mark.parametrize("field", ["run_id", "call_id"])
async def test_cold_model_projection_rejects_corrupt_attempt_source(
    repository, tmp_path, source_kind, field
):
    store, scope, workspace, request, run, origin = await model_setup(repository, tmp_path)
    try:
        view, _ = await store.reserve(
            scope, "files", workspace.fingerprint, request, model_source=origin
        )
        child = (await store.execution(scope, view.batch_id)).children[
            0 if source_kind == "file" else 1
        ]
        attempt, _ = await reserve_child(store, scope, workspace, child, run.id)
        table = WorkspaceEditAttemptRow if source_kind == "file" else WorkspaceDirectoryAttemptRow
        async with store.database.sessions.begin() as sql:
            await sql.execute(
                update(table)
                .where(table.operation_id == attempt.view.operation_id)
                .values(**{field: None})
            )
        selected = store.files if source_kind == "file" else store.directories
        for action in (
            lambda: store.get(scope, view.batch_id),
            lambda: store.history(scope),
            lambda: selected.assert_execution(scope, attempt.view.operation_id),
        ):
            with pytest.raises(ServiceError) as denied:
                await action()
            assert denied.value.code == "workspace_mixed_batch_corrupt"
        assert await counts(store.database) == (
            [1, 1, 1, 0] if source_kind == "file" else [1, 1, 0, 1]
        )
        assert not (tmp_path / "file.txt").exists() and not (tmp_path / "empty").exists()
    finally:
        workspace.close()


async def test_0040_host_hash_nonempty_0039_upgrade_and_model_source_downgrade_guard(
    repository, tmp_path
):
    store, scope, workspace, request, _run, origin = await model_setup(repository, tmp_path)
    try:
        async with store.database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(
                    migration_config(sync), "0039_mixed_workspace_batches"
                )
            )
            # 在真实0039 schema直接插入一个host-only父，验证升级不重建父表/影响外键。
            await connection.execute(
                text(
                    "INSERT INTO mixed_workspace_batches "
                    "(batch_id,scope_id,workspace_key,resource_fingerprint,request_key,request_hash,"
                    "snapshot_revision,action,producer_status,token,created_at,updated_at) "
                    "VALUES ('legacy-host',:scope,'files',:resource,'legacy',:hash,:hash,"
                    "'apply','cancelled',:hash,'2026-01-01','2026-01-01')"
                ),
                {"scope": scope, "resource": workspace.fingerprint, "hash": "a" * 64},
            )
            await connection.run_sync(lambda sync: command.upgrade(migration_config(sync), "head"))
            assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
        accepted, _ = await store.reserve(scope, "files", workspace.fingerprint, request)
        async with store.database.sessions() as sql:
            parent = await sql.get(MixedWorkspaceBatchRow, accepted.batch_id)
            assert parent.request_hash == digest(
                {
                    "request": request.model_dump(mode="json"),
                    "key": "files",
                    "resource": workspace.fingerprint,
                }
            )
            assert await sql.get(MixedWorkspaceBatchModelSourceRow, accepted.batch_id) is None
        await store.cancel(scope, accepted.batch_id)
        model, _ = await store.reserve(
            scope,
            "files",
            workspace.fingerprint,
            request.model_copy(update={"request_key": "model"}),
            model_source=origin,
        )
        with pytest.raises(RuntimeError, match="模型混合批次来源非空"):
            async with store.database.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(
                        migration_config(sync), "0039_mixed_workspace_batches"
                    )
                )
        assert (await store.get(scope, model.batch_id)).batch_id == model.batch_id
        async with store.database.engine.connect() as sql:
            assert (
                await sql.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0043_tree_move_ledger"
            assert not (await sql.execute(text("PRAGMA foreign_key_check"))).all()
    finally:
        workspace.close()
