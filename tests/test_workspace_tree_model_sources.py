"""0042模型树准备/启动原调用：同事务、并发、回滚、迁移和零效果。"""

import asyncio
import shutil
import sys

import pytest
from alembic import command
from sqlalchemy import event, func, select, text
from sqlalchemy.exc import IntegrityError
from test_command_directory_dependency_source import counts
from test_workspace_tree_plan_store import tree_setup

from krim_agent.persistence.database import (
    Database,
    WorkspaceTreeModelSourceRow,
    WorkspaceTreePlanRow,
    WorkspaceTreeStepRow,
)
from krim_agent.persistence.migration import migration_config
from krim_agent.persistence.workspace_directories import WorkspaceDirectoryStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.persistence.workspace_tree_plans import WorkspaceTreePlanStore
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.workspace_tree import TreePlanModelSource

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要真实目录成果来源",
)


async def origin(runtime, service, agent, key="model-run"):
    run, _ = await runtime.repository.begin_run(
        service._handle(agent.agent_id).topic_id,
        ChatRequest(text="model source", idempotency_key=key),
        agent_session_id=agent.session_id,
        agent_id=agent.agent_id,
        agent_owner_id=service.manager.owner_id,
    )
    return run


async def ledger(database):
    async with database.sessions() as sql:
        return [
            await sql.scalar(select(func.count()).select_from(table))
            for table in (
                WorkspaceTreePlanRow,
                WorkspaceTreeStepRow,
                WorkspaceTreeModelSourceRow,
            )
        ]


async def test_original_prepare_call_same_transaction_cold_key_closed_run_no_effects(
    runtime, tmp_path
):
    service, agent, scope, binding, prepared, store, request = await tree_setup(runtime, tmp_path)
    try:
        run = await origin(runtime, service, agent)
        source = TreePlanModelSource(run.id, "prepare-call")
        plan, created = await store.reserve(scope, binding, request, prepared, model_source=source)
        assert created and plan.status == "planned" and await ledger(store.database) == [1, 3, 1]
        async with store.database.sessions() as sql:
            row = await sql.get(WorkspaceTreeModelSourceRow, (plan.plan_id, "prepare"))
            assert (row.run_id, row.call_id) == (run.id, "prepare-call")
        assert await counts(store.database) == [0] * 4 and list(tmp_path.iterdir()) == []
        with pytest.raises(ServiceError) as changed:
            await store.reserve(
                scope,
                binding,
                request.model_copy(update={"request_key": "changed-key"}),
                prepared,
                model_source=source,
            )
        assert changed.value.code == "workspace_tree_model_call_conflict"
        with pytest.raises(ServiceError) as caller:
            await store.reserve(
                scope, binding, request, None, model_source=TreePlanModelSource(run.id, "new-call")
            )
        assert caller.value.code == "workspace_tree_model_source_conflict"
        await runtime.repository.finish(run.id, "completed", None, [])
        assert (await store.reserve(scope, binding, request, None, model_source=source)) == (
            plan,
            False,
        )
        assert (await store.reserve(scope, binding, request, None)) == (plan, False)
        assert run.id not in plan.model_dump_json() and "prepare-call" not in plan.model_dump_json()
        assert await ledger(store.database) == [1, 3, 1]
    finally:
        await service.close()


async def test_start_separate_live_run_kind_call_unique_and_source_fence_no_effects(
    runtime, tmp_path
):
    service, agent, scope, binding, prepared, store, request = await tree_setup(runtime, tmp_path)
    try:
        run = await origin(runtime, service, agent, "prepare-run")
        prepare_source = TreePlanModelSource(run.id, "prepare-call")
        plan, _ = await store.reserve(
            scope, binding, request, prepared, model_source=prepare_source
        )
        with pytest.raises(ServiceError) as reused:
            await store.claim(scope, plan.plan_id, model_source=prepare_source)
        assert reused.value.code == "workspace_tree_model_call_conflict"
        assert (await store.get(scope, plan.plan_id)).producer_status == "planned"
        await runtime.repository.finish(run.id, "completed", None, [])
        started_run = await origin(runtime, service, agent, "start-run")
        source = TreePlanModelSource(started_run.id, "start-call")
        execution = await store.claim(scope, plan.plan_id, model_source=source)
        assert execution.model_source == source and execution.view.producer_status == "running"
        async with store.database.sessions() as sql:
            original = await sql.get(WorkspaceTreeModelSourceRow, (plan.plan_id, "prepare"))
            start = await sql.get(WorkspaceTreeModelSourceRow, (plan.plan_id, "start"))
            assert (original.run_id, start.run_id, start.call_id) == (
                run.id,
                started_run.id,
                "start-call",
            )
        dispatch = await store.dispatch(scope, plan.plan_id, execution.token, 0)
        assert dispatch.fence.model_source == source
        await store.renew(scope, plan.plan_id, execution.token)
        assert await ledger(store.database) == [1, 3, 2] and await counts(store.database) == [0] * 4
        cancelled = await store.cancel(scope, plan.plan_id)
        assert cancelled.status == "cancelled"
        with pytest.raises(ServiceError) as closed:
            await store.claim(scope, plan.plan_id, model_source=source)
        assert closed.value.code == "workspace_tree_claimed"
        assert list(tmp_path.iterdir()) == []
    finally:
        await service.close()


@pytest.mark.parametrize("kind", ["prepare", "start"])
async def test_original_source_and_parent_mutation_roll_back_together(runtime, tmp_path, kind):
    service, agent, scope, binding, prepared, store, request = await tree_setup(runtime, tmp_path)
    try:
        run = await origin(runtime, service, agent)
        source = TreePlanModelSource(run.id, "atomic-call")
        if kind == "start":
            original, _ = await store.reserve(scope, binding, request, prepared)

        def fail(_connection, _cursor, statement, _parameters, _context, _many):
            if statement.startswith("INSERT INTO workspace_tree_plan_model_sources"):
                raise RuntimeError("injected model source insert failure")

        event.listen(store.database.engine.sync_engine, "before_cursor_execute", fail)
        try:
            with pytest.raises(RuntimeError, match="injected"):
                if kind == "prepare":
                    await store.reserve(scope, binding, request, prepared, model_source=source)
                else:
                    await store.claim(scope, original.plan_id, model_source=source)
        finally:
            event.remove(store.database.engine.sync_engine, "before_cursor_execute", fail)
        assert await ledger(store.database) == ([0, 0, 0] if kind == "prepare" else [1, 3, 0])
        if kind == "start":
            assert await store.get(scope, original.plan_id) == original
        else:
            assert (await store.reserve(scope, binding, request, prepared, model_source=source))[1]
        assert await counts(store.database) == [0] * 4 and list(tmp_path.iterdir()) == []
    finally:
        await service.close()


async def test_two_connections_same_call_different_key_cannot_admit_two_plans(runtime, tmp_path):
    service, agent, scope, binding, prepared, store, request = await tree_setup(runtime, tmp_path)
    other = Database(str(store.database.engine.url))
    await other.initialize()
    second = WorkspaceTreePlanStore(
        WorkspaceEditStore(other, owner_id=store.owner_id),
        WorkspaceDirectoryStore(other, owner_id=store.owner_id),
    )
    try:
        run = await origin(runtime, service, agent)
        source = TreePlanModelSource(run.id, "one-call")
        results = await asyncio.gather(
            store.reserve(scope, binding, request, prepared, model_source=source),
            second.reserve(
                scope,
                binding,
                request.model_copy(update={"request_key": "second-key"}),
                prepared,
                model_source=source,
            ),
            return_exceptions=True,
        )
        assert sum(isinstance(result, tuple) for result in results) == 1
        failure = next(result for result in results if isinstance(result, ServiceError))
        assert failure.code == "workspace_tree_model_call_conflict"
        assert await ledger(store.database) == [1, 3, 1] and await counts(store.database) == [0] * 4
    finally:
        await other.close()
        await service.close()


@pytest.mark.parametrize(
    "run_value,call_value",
    [
        ("local", "call"),
        ("missing", "call"),
        ("", "call"),
        ("valid", ""),
        ("valid", "x\x7f"),
        ("valid", "x\0"),
        ("valid", "x" * 129),
    ],
)
async def test_unbound_invalid_original_call_rejects_all_new_metadata(
    runtime, tmp_path, run_value, call_value
):
    service, agent, scope, binding, prepared, store, request = await tree_setup(runtime, tmp_path)
    try:
        run = await origin(runtime, service, agent)
        with pytest.raises(ServiceError) as invalid:
            await store.reserve(
                scope,
                binding,
                request,
                prepared,
                model_source=TreePlanModelSource(
                    run.id if run_value == "valid" else run_value, call_value
                ),
            )
        assert invalid.value.code in {
            "workspace_tree_model_context",
            "workspace_tree_model_run_mismatch",
        }
        assert await ledger(store.database) == [0, 0, 0]
    finally:
        await service.close()


async def test_other_topic_and_closed_run_cannot_prepare_or_claim(runtime, tmp_path):
    service, agent, scope, binding, prepared, store, request = await tree_setup(runtime, tmp_path)
    try:
        topic = await runtime.repository.create_topic("other")
        foreign, _ = await runtime.repository.begin_run(
            topic.id, ChatRequest(text="other", idempotency_key="run")
        )
        with pytest.raises(ServiceError) as wrong:
            await store.reserve(
                scope,
                binding,
                request,
                prepared,
                model_source=TreePlanModelSource(foreign.id, "other-call"),
            )
        assert wrong.value.code == "workspace_tree_model_run_mismatch"
        run = await origin(runtime, service, agent)
        await runtime.repository.finish(run.id, "completed", None, [])
        with pytest.raises(ServiceError) as closed:
            await store.reserve(
                scope,
                binding,
                request,
                prepared,
                model_source=TreePlanModelSource(run.id, "closed"),
            )
        assert closed.value.code == "workspace_tree_model_run_closed"
        original, _ = await store.reserve(scope, binding, request, prepared)
        with pytest.raises(ServiceError) as claim:
            await store.claim(
                scope, original.plan_id, model_source=TreePlanModelSource(run.id, "closed")
            )
        assert claim.value.code == "workspace_tree_model_run_closed"
        assert await ledger(store.database) == [1, 3, 0]
    finally:
        await service.close()


async def test_0042_constraints_foreign_keys_and_nonempty_downgrade_preserve_history(
    runtime, tmp_path
):
    service, agent, scope, binding, prepared, store, request = await tree_setup(runtime, tmp_path)
    try:
        run = await origin(runtime, service, agent)
        original, _ = await store.reserve(
            scope,
            binding,
            request,
            prepared,
            model_source=TreePlanModelSource(run.id, "prepare-call"),
        )
        cases = [
            {"kind": None},
            {"kind": "unknown"},
            {"run_id": None},
            {"run_id": "missing"},
            {"call_id": None},
            {"call_id": ""},
            {"call_id": "x" * 129},
            {"plan_id": "missing"},
            {"call_id": "prepare-call"},
        ]
        for overrides in cases:
            params = {
                "plan_id": original.plan_id,
                "kind": "start",
                "run_id": run.id,
                "call_id": "start-call",
                **overrides,
            }
            with pytest.raises(IntegrityError):
                async with store.database.engine.begin() as connection:
                    await connection.execute(
                        text(
                            "INSERT INTO workspace_tree_plan_model_sources "
                            "(plan_id,kind,run_id,call_id) "
                            "VALUES (:plan_id,:kind,:run_id,:call_id)"
                        ),
                        params,
                    )
        with pytest.raises(RuntimeError, match="模型目录树来源非空"):
            async with store.database.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(
                        migration_config(sync), "0041_workspace_tree_plans"
                    )
                )
        assert await store.get(scope, original.plan_id) == original and await ledger(
            store.database
        ) == [1, 3, 1]
        async with store.database.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0053_directory_captures"
            assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
    finally:
        await service.close()


async def test_empty_0042_roundtrip_does_not_change_host_tree(runtime, tmp_path):
    service, agent, scope, binding, prepared, store, request = await tree_setup(runtime, tmp_path)
    try:
        original, _ = await store.reserve(scope, binding, request, prepared)
        async with store.database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(migration_config(sync), "0041_workspace_tree_plans")
            )
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0041_workspace_tree_plans"
            assert (
                await connection.execute(text("SELECT count(*) FROM workspace_tree_plans"))
            ).scalar_one() == 1
            await connection.run_sync(lambda sync: command.upgrade(migration_config(sync), "head"))
        assert await store.get(scope, original.plan_id) == original and await ledger(
            store.database
        ) == [1, 3, 0]
    finally:
        await service.close()
