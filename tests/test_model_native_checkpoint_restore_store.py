"""恢复阶段来源、基线父关系和SQL最终预算门控的真实事务验证。"""

import asyncio
from dataclasses import replace
from uuid import uuid4

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import delete, event, func, select, text, update
from sqlalchemy.exc import IntegrityError
from test_model_native_checkpoint_restores import phase_fixture

from krim_agent.persistence.database import (
    Base,
    NativeCheckpointModelSourceRow,
    RunRow,
    WorkspaceEditAttemptRow,
    WorkspaceRestoreItemRow,
    WorkspaceRestoreModelSourceRow,
    WorkspaceRestorePlanRow,
)
from krim_agent.persistence.migration import migration_config
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.workspace_checkpoint import CheckpointModelSource, WorkspaceCheckpointCreate
from krim_agent.workspace_restore import CheckpointRestoreExecute, CheckpointRestorePreview
from krim_agent.workspace_writer import WorkspaceWriter


async def prepared(runtime, root):
    service, agent, _, target, _ = await phase_fixture(runtime, root, "preview")
    store = service.workspace_restore_store
    binding = service.resources.workspace("files")
    run, _ = await runtime.repository.begin_run(
        service._handle(agent.agent_id).topic_id,
        ChatRequest(text="可信恢复来源Store门禁", idempotency_key="store-run"),
        agent_session_id=agent.session_id,
        agent_id=agent.agent_id,
        agent_owner_id=store.owner_id,
    )
    return service, agent, target, binding, CheckpointModelSource(run.id, "preview-root")


async def reserve(service, target, binding, source, *, key="model-preview", authorize=None):
    return await service.workspace_restore_store.reserve(
        target.scope_id,
        CheckpointRestorePreview(checkpoint_id=target.checkpoint_id, request_key=key),
        binding.key,
        binding.fingerprint,
        binding.workspace.fingerprint,
        model_source=source,
        authorize=authorize,
    )


async def baseline(service, view, binding, source):
    store = service.workspace_restore_store
    checkpoints = service.workspace_checkpoint_store
    prep = await store.preparation(view.scope_id, view.plan_id)
    backup, _ = await checkpoints.reserve(
        view.scope_id,
        WorkspaceCheckpointCreate(
            paths=["plain"],
            request_key=prep.baseline_key,
            capture_format="bytes",
        ),
        binding.key,
        binding.fingerprint,
        binding.workspace.fingerprint,
        model_source=replace(source, restore_plan_id=view.plan_id),
    )
    fence = (await checkpoints.execution(view.scope_id, backup.checkpoint_id))[0]
    capture = await asyncio.to_thread(
        WorkspaceWriter(binding.workspace).capture_file, "plain", allow_binary=True
    )
    await checkpoints.record(view.scope_id, fence, capture)
    await checkpoints.finish(view.scope_id, fence)
    await store.attach_baseline(
        view.scope_id, view.plan_id, prep.token, backup.checkpoint_id, model_source=source
    )
    target_captures = await checkpoints.captures(view.scope_id, view.checkpoint_id)
    values = (
        WorkspaceWriter(binding.workspace).prepare_checkpoint_restore(capture, target_captures[0]),
    )
    return prep, backup, values


async def ready(service, view, binding, source):
    prep, backup, values = await baseline(service, view, binding, source)
    await service.workspace_restore_store.ready(
        view.scope_id, view.plan_id, prep.token, values, model_source=source
    )
    return await service.workspace_restore_store.get(view.scope_id, view.plan_id), backup, values


async def test_restore_plan_source_and_items_rollback_with_failed_source_insert(runtime, tmp_path):
    service, _, target, binding, source = await prepared(runtime, tmp_path)
    database = service.workspace_restore_store.database

    def failed(_conn, _cursor, statement, _parameters, _context, _many):
        if statement.startswith("INSERT INTO workspace_restore_model_sources"):
            raise RuntimeError("source insert unavailable")

    try:
        event.listen(database.engine.sync_engine, "before_cursor_execute", failed)
        with pytest.raises(RuntimeError, match="source insert unavailable"):
            await reserve(service, target, binding, source)
        event.remove(database.engine.sync_engine, "before_cursor_execute", failed)
        async with database.sessions() as sql:
            for table in (
                WorkspaceRestorePlanRow,
                WorkspaceRestoreItemRow,
                WorkspaceRestoreModelSourceRow,
            ):
                assert await sql.scalar(select(func.count()).select_from(table)) == 0
        view, created = await reserve(service, target, binding, source)
        assert (
            created
            and await service.workspace_restore_store.source(view.scope_id, view.plan_id, "preview")
            == source
        )
    finally:
        if event.contains(database.engine.sync_engine, "before_cursor_execute", failed):
            event.remove(database.engine.sync_engine, "before_cursor_execute", failed)
        await service.close()


async def test_restore_stage_call_and_baseline_role_cannot_be_claimed_or_reused(runtime, tmp_path):
    service, _, target, binding, source = await prepared(runtime, tmp_path)
    store = service.workspace_restore_store
    try:
        view, _ = await reserve(service, target, binding, source)
        assert (await reserve(service, target, binding, source))[0] == view
        for other_source, key, code in (
            (None, "model-preview", "workspace_restore_model_conflict"),
            (
                replace(source, call_id="other-root"),
                "model-preview",
                "workspace_restore_model_conflict",
            ),
            (source, "other-key", "workspace_restore_model_call_conflict"),
        ):
            with pytest.raises(ServiceError) as rejected:
                await reserve(service, target, binding, other_source, key=key)
            assert rejected.value.code == code
        with pytest.raises(ServiceError) as direct:
            await service.workspace_checkpoint_store.reserve(
                target.scope_id,
                WorkspaceCheckpointCreate(
                    paths=["plain"],
                    request_key="cannot-masquerade-direct",
                    capture_format="bytes",
                ),
                binding.key,
                binding.fingerprint,
                binding.workspace.fingerprint,
                model_source=source,
            )
        assert direct.value.code == "workspace_checkpoint_model_call_conflict"
        plan, backup, values = await ready(service, view, binding, source)
        assert await service.workspace_checkpoint_store.source(
            plan.scope_id, backup.checkpoint_id
        ) == replace(source, restore_plan_id=plan.plan_id)
        with pytest.raises(ServiceError) as reused:
            await store.start(
                plan.scope_id,
                CheckpointRestoreExecute(
                    plan_id=plan.plan_id,
                    expected_revision=plan.revision,
                    request_key="same-root-start",
                ),
                binding.key,
                binding.fingerprint,
                binding.workspace.fingerprint,
                model_source=source,
            )
        assert reused.value.code == "workspace_restore_model_call_conflict"
        execute_source = replace(source, call_id="execute-root")
        running, _ = await store.start(
            plan.scope_id,
            CheckpointRestoreExecute(
                plan_id=plan.plan_id, expected_revision=plan.revision, request_key="new-root-start"
            ),
            binding.key,
            binding.fingerprint,
            binding.workspace.fingerprint,
            model_source=execute_source,
        )
        dispatch = (await store.execution(plan.scope_id, plan.plan_id))[0]
        assert dispatch.fence.model_source == execute_source
        for fence, run_id, call_id in (
            (replace(dispatch.fence, model_source=None), source.run_id, execute_source.call_id),
            (dispatch.fence, source.run_id, "forged-call"),
        ):
            with pytest.raises(ServiceError) as child:
                await service.workspace_edits.reserve(
                    plan.scope_id,
                    dispatch.request,
                    binding.fingerprint,
                    values[0],
                    run_id=run_id,
                    call_id=call_id,
                    restore_fence=fence,
                )
            assert child.value.code == "workspace_restore_model_context"
        await store.cancel(plan.scope_id, plan.plan_id)
    finally:
        await service.close()


@pytest.mark.parametrize("stage", ["reserve", "ready", "start", "child"])
async def test_last_sql_budget_failure_leaves_no_new_stage_or_file_admission(
    runtime, tmp_path, stage
):
    service, _, target, binding, source = await prepared(runtime, tmp_path)
    store = service.workspace_restore_store
    checked = []

    def deny_final():
        checked.append(True)
        if len(checked) >= 2 or stage == "child":
            raise ServiceError("tool_timeout", "原最后SQL预算结束", 408)

    try:
        if stage == "reserve":
            with pytest.raises(ServiceError) as refused:
                await reserve(service, target, binding, source, authorize=deny_final)
            assert refused.value.code == "tool_timeout"
            assert await store.by_key(target.scope_id, "model-preview") is None
        else:
            view, _ = await reserve(service, target, binding, source)
            prep, backup, values = await baseline(service, view, binding, source)
            if stage == "ready":
                with pytest.raises(ServiceError) as refused:
                    await store.ready(
                        view.scope_id,
                        view.plan_id,
                        prep.token,
                        values,
                        model_source=source,
                        authorize=deny_final,
                    )
                assert refused.value.code == "tool_timeout"
                final = await store.get(view.scope_id, view.plan_id)
                assert final.phase == "preparing" and final.items[0].proposal is None
            else:
                await store.ready(
                    view.scope_id, view.plan_id, prep.token, values, model_source=source
                )
                plan = await store.get(view.scope_id, view.plan_id)
                request = CheckpointRestoreExecute(
                    plan_id=plan.plan_id, expected_revision=plan.revision, request_key="start"
                )
                execute_source = replace(source, call_id="execute-root")
                if stage == "start":
                    with pytest.raises(ServiceError) as refused:
                        await store.start(
                            plan.scope_id,
                            request,
                            binding.key,
                            binding.fingerprint,
                            binding.workspace.fingerprint,
                            model_source=execute_source,
                            authorize=deny_final,
                        )
                    assert refused.value.code == "tool_timeout"
                    final = await store.get(plan.scope_id, plan.plan_id)
                    assert final.phase == "ready" and final.execute_key is None
                    assert await store.source(plan.scope_id, plan.plan_id, "execute") is None
                else:
                    await store.start(
                        plan.scope_id,
                        request,
                        binding.key,
                        binding.fingerprint,
                        binding.workspace.fingerprint,
                        model_source=execute_source,
                    )
                    dispatch = (await store.execution(plan.scope_id, plan.plan_id))[0]
                    with pytest.raises(ServiceError) as refused:
                        await service.workspace_edits.reserve(
                            plan.scope_id,
                            dispatch.request,
                            binding.fingerprint,
                            values[0],
                            run_id=execute_source.run_id,
                            call_id=execute_source.call_id,
                            restore_fence=dispatch.fence,
                            authorize=deny_final,
                        )
                    assert refused.value.code == "tool_timeout"
                    async with store.database.sessions() as sql:
                        assert (
                            await sql.scalar(
                                select(func.count()).select_from(WorkspaceEditAttemptRow)
                            )
                            == 0
                        )
        assert len(checked) >= (1 if stage == "child" else 2)
        assert (tmp_path / "plain").read_bytes() == b"current\n"
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["source", "marker", "baseline_parent"])
async def test_restore_source_corruption_never_becomes_host_fact(runtime, tmp_path, fault):
    service, _, target, binding, source = await prepared(runtime, tmp_path)
    store = service.workspace_restore_store
    try:
        view, _ = await reserve(service, target, binding, source)
        plan, backup, _ = await ready(service, view, binding, source)
        async with store.database.write_lock, store.database.sessions.begin() as sql:
            if fault == "source":
                await sql.execute(
                    delete(WorkspaceRestoreModelSourceRow).where(
                        WorkspaceRestoreModelSourceRow.plan_id == plan.plan_id
                    )
                )
            elif fault == "marker":
                await sql.execute(
                    update(WorkspaceRestorePlanRow)
                    .where(WorkspaceRestorePlanRow.plan_id == plan.plan_id)
                    .values(preview_model_bound=False)
                )
            else:
                await sql.execute(
                    update(NativeCheckpointModelSourceRow)
                    .where(NativeCheckpointModelSourceRow.checkpoint_id == backup.checkpoint_id)
                    .values(restore_plan_id=None)
                )
        with pytest.raises(ServiceError) as refused:
            if fault == "baseline_parent":
                with pytest.raises(ServiceError) as baseline_history:
                    await service.workspace_checkpoint_store.get(
                        plan.scope_id, backup.checkpoint_id
                    )
                assert baseline_history.value.code == "workspace_checkpoint_corrupt"
                await store.start(
                    plan.scope_id,
                    CheckpointRestoreExecute(
                        plan_id=plan.plan_id, expected_revision=plan.revision, request_key="start"
                    ),
                    binding.key,
                    binding.fingerprint,
                    binding.workspace.fingerprint,
                )
            else:
                await store.get(plan.scope_id, plan.plan_id)
        assert refused.value.code in {
            "workspace_restore_corrupt",
            "workspace_restore_baseline",
            "workspace_checkpoint_corrupt",
        }
    finally:
        await service.close()


async def test_actual_0051_fk_unique_marker_and_stage_constraints(runtime, tmp_path):
    service, _, target, binding, source = await prepared(runtime, tmp_path)
    store = service.workspace_restore_store
    try:
        view, _ = await reserve(service, target, binding, source)
        plan, backup, _ = await ready(service, view, binding, source)
        host, _ = await reserve(service, target, binding, None, key="host-second")
        extra, _ = await service.workspace_checkpoint_store.reserve(
            target.scope_id,
            WorkspaceCheckpointCreate(paths=["plain"], request_key="extra"),
            binding.key,
            binding.fingerprint,
            binding.workspace.fingerprint,
        )
        base = {"id": host.plan_id, "run": source.run_id, "call": "unused", "phase": "preview"}
        cases = [
            (
                "INSERT INTO workspace_restore_model_sources(plan_id,phase,run_id,call_id) "
                "VALUES(:id,:phase,:run,:call)",
                {**base, "call": source.call_id},
            ),
            (
                "INSERT INTO workspace_restore_model_sources(plan_id,phase,run_id,call_id) "
                "VALUES(:id,:phase,:run,:call)",
                {**base, "phase": "invalid"},
            ),
            (
                "INSERT INTO workspace_restore_model_sources(plan_id,phase,run_id,call_id) "
                "VALUES(:id,:phase,:run,:call)",
                {**base, "call": ""},
            ),
            (
                "INSERT INTO workspace_restore_model_sources(plan_id,phase,run_id,call_id) "
                "VALUES(:id,:phase,:run,:call)",
                {**base, "run": str(uuid4())},
            ),
            (
                "INSERT INTO workspace_restore_model_sources(plan_id,phase,run_id,call_id) "
                "VALUES(:id,:phase,:run,:call)",
                {**base, "id": str(uuid4())},
            ),
            (
                "UPDATE workspace_restore_plans SET preview_model_bound=2 WHERE plan_id=:id",
                {"id": plan.plan_id},
            ),
            (
                "UPDATE native_checkpoint_model_sources SET restore_plan_id=:parent "
                "WHERE checkpoint_id=:id",
                {"parent": str(uuid4()), "id": backup.checkpoint_id},
            ),
            (
                "INSERT INTO native_checkpoint_model_sources"
                "(checkpoint_id,run_id,call_id,restore_plan_id) "
                "VALUES(:id,:run,:call,:parent)",
                {
                    "id": extra.checkpoint_id,
                    "run": source.run_id,
                    "call": "extra-baseline",
                    "parent": plan.plan_id,
                },
            ),
        ]
        for statement, parameters in cases:
            with pytest.raises(IntegrityError):
                async with store.database.engine.begin() as sql:
                    await sql.execute(text(statement), parameters)
        async with store.database.engine.connect() as sql:
            assert (await sql.execute(text("PRAGMA foreign_key_check"))).all() == []
    finally:
        await service.close()


async def test_0051_roundtrip_preserves_nonempty_host_restore_and_direct_model_source(
    runtime, tmp_path
):
    service, agent, target, binding, source = await prepared(runtime, tmp_path)
    store = service.workspace_restore_store
    database = store.database
    try:
        accepted = await agent.preview_file_checkpoint_restore(
            CheckpointRestorePreview(
                checkpoint_id=target.checkpoint_id,
                request_key="host-old",
            )
        )
        plan = await agent.wait_file_checkpoint_restore(accepted.plan_id)
        assert plan.phase == "ready", plan
        checkpoint, _ = await service.workspace_checkpoint_store.reserve(
            target.scope_id,
            WorkspaceCheckpointCreate(
                paths=["plain"], request_key="old-model", capture_format="bytes"
            ),
            binding.key,
            binding.fingerprint,
            binding.workspace.fingerprint,
            model_source=source,
        )
        fence = (
            await service.workspace_checkpoint_store.execution(
                target.scope_id, checkpoint.checkpoint_id
            )
        )[0]
        capture = await asyncio.to_thread(
            WorkspaceWriter(binding.workspace).capture_file, "plain", allow_binary=True
        )
        await service.workspace_checkpoint_store.record(target.scope_id, fence, capture)
        original = await service.workspace_checkpoint_store.finish(target.scope_id, fence)
        queries = [
            "SELECT request_hash,execute_hash,baseline_key "
            "FROM workspace_restore_plans ORDER BY plan_id",
            "SELECT capture_metadata,content "
            "FROM native_workspace_checkpoint_files ORDER BY checkpoint_id,position",
            "SELECT checkpoint_id,run_id,call_id "
            "FROM native_checkpoint_model_sources ORDER BY checkpoint_id",
        ]
        async with database.engine.connect() as sql:
            before = [(await sql.execute(text(query))).all() for query in queries]
        async with database.engine.begin() as sql:
            await sql.run_sync(
                lambda sync: command.downgrade(
                    migration_config(sync), "0050_native_checkpoint_models"
                )
            )
            assert (
                await sql.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0050_native_checkpoint_models"
            assert [(await sql.execute(text(query))).all() for query in queries] == before
            assert (await sql.execute(text("PRAGMA foreign_key_check"))).all() == []
        await database.initialize()
        assert await agent.file_checkpoint_restore(plan.plan_id) == plan
        assert await agent.file_checkpoint(original.checkpoint_id) == original
        assert (
            await service.workspace_checkpoint_store.source(target.scope_id, original.checkpoint_id)
            == source
        )
        async with database.engine.connect() as sql:
            assert [(await sql.execute(text(query))).all() for query in queries] == before
            assert (await sql.execute(text("PRAGMA foreign_key_check"))).all() == []

            def compare(sync):
                def include(obj, _name, kind, _reflected, _other):
                    table = obj if kind == "table" else getattr(obj, "table", None)
                    return table is not None and table.name.startswith(
                        ("native_checkpoint_", "native_workspace_checkpoint", "workspace_restore_")
                    )

                return compare_metadata(
                    MigrationContext.configure(sync, opts={"include_object": include}),
                    Base.metadata,
                )

            assert await sql.run_sync(compare) == []
    finally:
        await service.close()


@pytest.mark.parametrize("material", ["sources", "marker_only"])
async def test_0051_model_restore_refuses_lossy_downgrade_with_exact_original_material(
    runtime, tmp_path, material
):
    service, _, target, binding, source = await prepared(runtime, tmp_path)
    store = service.workspace_restore_store
    database = store.database
    try:
        plan, _ = await reserve(service, target, binding, source)
        if material == "marker_only":
            async with database.sessions.begin() as sql:
                await sql.execute(delete(WorkspaceRestoreModelSourceRow))
        with pytest.raises(RuntimeError, match="阶段来源或基线父关系"):
            async with database.engine.begin() as sql:
                await sql.run_sync(
                    lambda sync: command.downgrade(
                        migration_config(sync), "0050_native_checkpoint_models"
                    )
                )
        async with database.engine.connect() as sql:
            assert (
                await sql.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0054_directory_capture_models"
            assert (
                await sql.execute(
                    text(
                        "SELECT preview_model_bound FROM workspace_restore_plans WHERE plan_id=:id"
                    ),
                    {"id": plan.plan_id},
                )
            ).scalar_one() == 1
            assert (await sql.execute(text("PRAGMA foreign_key_check"))).all() == []
    finally:
        await service.close()


@pytest.mark.parametrize("stage", ["capture", "ready", "execute"])
async def test_original_model_run_stop_rejects_baseline_ready_and_file_admission(
    runtime, tmp_path, stage
):
    service, _, target, binding, source = await prepared(runtime, tmp_path)
    store = service.workspace_restore_store
    try:
        view, _ = await reserve(service, target, binding, source)
        if stage == "capture":
            prep = await store.preparation(view.scope_id, view.plan_id)
            backup, _ = await service.workspace_checkpoint_store.reserve(
                view.scope_id,
                WorkspaceCheckpointCreate(
                    paths=["plain"], request_key=prep.baseline_key, capture_format="bytes"
                ),
                binding.key,
                binding.fingerprint,
                binding.workspace.fingerprint,
                model_source=replace(source, restore_plan_id=view.plan_id),
            )
            fence = (
                await service.workspace_checkpoint_store.execution(
                    view.scope_id, backup.checkpoint_id
                )
            )[0]
            capture = await asyncio.to_thread(
                WorkspaceWriter(binding.workspace).capture_file, "plain", allow_binary=True
            )
        else:
            prep, backup, values = await baseline(service, view, binding, source)
            if stage == "execute":
                await store.ready(
                    view.scope_id, view.plan_id, prep.token, values, model_source=source
                )
                plan = await store.get(view.scope_id, view.plan_id)
                execute_source = replace(source, call_id="execute-root")
                await store.start(
                    plan.scope_id,
                    CheckpointRestoreExecute(
                        plan_id=plan.plan_id, expected_revision=plan.revision, request_key="start"
                    ),
                    binding.key,
                    binding.fingerprint,
                    binding.workspace.fingerprint,
                    model_source=execute_source,
                )
                dispatch = (await store.execution(plan.scope_id, plan.plan_id))[0]
        async with store.database.sessions.begin() as sql:
            await sql.execute(
                update(RunRow).where(RunRow.id == source.run_id).values(stop_requested=True)
            )
        with pytest.raises(ServiceError) as refused:
            if stage == "capture":
                await service.workspace_checkpoint_store.record(view.scope_id, fence, capture)
            elif stage == "ready":
                await store.ready(
                    view.scope_id, view.plan_id, prep.token, values, model_source=source
                )
            else:
                await service.workspace_edits.reserve(
                    view.scope_id,
                    dispatch.request,
                    binding.fingerprint,
                    values[0],
                    run_id=source.run_id,
                    call_id=execute_source.call_id,
                    restore_fence=dispatch.fence,
                )
        assert refused.value.code == "workspace_run_closed"
        assert (tmp_path / "plain").read_bytes() == b"current\n"
        if stage == "capture":
            assert (
                not (
                    await service.workspace_checkpoint_store.get(
                        view.scope_id, backup.checkpoint_id
                    )
                )
                .files[0]
                .captured
            )
    finally:
        await service.close()
