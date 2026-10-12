"""原模型Run/start call树围栏：真实Registry发布、来源/停止/迟到拒绝。"""

import shutil
import sys
from dataclasses import replace

import pytest
from sqlalchemy import update
from test_command_directory_dependency_source import counts
from test_workspace_tree_fences import tree
from test_workspace_tree_model_sources import origin

from krim_agent.persistence.database import (
    RunRow,
    WorkspaceDirectoryAttemptRow,
    WorkspaceEditAttemptRow,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.tools import ToolContext
from krim_agent.workspace_directory import WorkspaceDirectoryWriter
from krim_agent.workspace_tree import TreePlanModelSource
from krim_agent.workspace_writer import WorkspaceWriter

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要真实模型树目录/二进制发布",
)


async def setup(runtime, root):
    service, agent, scope, store, plan = await tree(runtime, root, claim=False)
    run = await origin(runtime, service, agent)
    execution = await store.claim(
        scope, plan.plan_id, model_source=TreePlanModelSource(run.id, "original-start-call")
    )
    handle = service._handle(agent.agent_id)
    context = ToolContext(
        handle.runtime.tools,
        run_id=run.id,
        topic_id=handle.topic_id,
        agent_id=agent.agent_id,
        session_id=agent.session_id,
        workspace=handle.definition.workspace,
        execution=handle.definition.execution,
        commands=service.manager.commands,
        workspace_changes=service.manager.workspace_changes,
        directory_changes=service.manager.directory_changes,
        origin="model",
        source_call_id="original-start-call",
        call_id="original-start-call",
        policy=handle.runtime.tool_policy,
        depth=1,
        timeout=30,
    )
    return service, agent, scope, store, execution, context


async def invoke(dispatch, context):
    value = await context.registry.invoke(
        dispatch.name, dispatch.request.model_dump(), replace(context, tree_fence=dispatch.fence)
    )
    if value.is_error:
        raise ServiceError(value.code, value.content[0].text, 409)
    return value.data


async def publish(scope, store, execution, context, position):
    preview = await store.dispatch(scope, execution.view.plan_id, execution.token, position)
    await invoke(preview, context)
    change = await store.dispatch(scope, execution.view.plan_id, execution.token, position)
    result = await invoke(change, context)
    assert result["status"] == "done"
    await store.finish_step(scope, execution.view.plan_id, execution.token, position)
    return result


async def test_original_model_registry_tree_real_bytes_attempt_call_and_closed_run_cold_view(
    runtime, tmp_path
):
    service, agent, scope, store, execution, context = await setup(runtime, tmp_path)
    try:
        outcomes = [
            await publish(scope, store, execution, context, position) for position in range(3)
        ]
        done = await store.finish(scope, execution.view.plan_id, execution.token)
        assert done.status == "done" and done.done_count == 3
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff"
        assert await counts(store.database) == [2, 2, 1, 1]
        async with store.database.sessions() as sql:
            for position, outcome in enumerate(outcomes):
                row = await sql.get(
                    WorkspaceEditAttemptRow if position == 2 else WorkspaceDirectoryAttemptRow,
                    outcome["operation_id"],
                )
                assert (row.run_id, row.call_id) == (context.run_id, context.source_call_id)
        audits = [item for item in context.registry.audit if item.run_id == context.run_id]
        assert len(audits) == 6 and all(item.depth == 2 and item.ok for item in audits)
        assert all(item.call_id != context.source_call_id for item in audits)
        await runtime.repository.finish(context.run_id, "completed", None, [])
        assert await store.get(scope, done.plan_id) == done
        assert [
            item.operation.operation_id for item in (await store.steps(scope, done.plan_id)).items
        ] == [item["operation_id"] for item in outcomes]
    finally:
        await service.close()


@pytest.mark.parametrize("field", ["origin", "run_id", "source_call_id", "model_source"])
async def test_model_source_cannot_be_dropped_redirected_or_promoted_host(runtime, tmp_path, field):
    service, agent, scope, store, execution, context = await setup(runtime, tmp_path)
    try:
        dispatch = await store.dispatch(scope, execution.view.plan_id, execution.token, 0)
        if field == "model_source":
            dispatch = replace(dispatch, fence=replace(dispatch.fence, model_source=None))
            context = replace(context, origin="host", run_id="local", source_call_id=None)
        else:
            context = replace(
                context,
                **{
                    field: {"origin": "host", "run_id": "local", "source_call_id": "foreign"}[field]
                },
            )
        value = await context.registry.invoke(
            dispatch.name,
            dispatch.request.model_dump(),
            replace(context, tree_fence=dispatch.fence),
        )
        assert value.is_error and value.code in {
            "workspace_tree_model_source_conflict",
            "workspace_tree_model_run_mismatch",
        }
        assert await counts(store.database) == [0] * 4 and list(tmp_path.iterdir()) == []
    finally:
        await service.close()


@pytest.mark.parametrize("phase", ["preview", "change", "renew"])
async def test_stop_requested_or_closed_original_run_refuses_future_metadata_and_effect(
    runtime, tmp_path, phase
):
    service, agent, scope, store, execution, context = await setup(runtime, tmp_path)
    try:
        dispatch = await store.dispatch(scope, execution.view.plan_id, execution.token, 0)
        if phase == "change":
            await invoke(dispatch, context)
            dispatch = await store.dispatch(scope, execution.view.plan_id, execution.token, 0)
        async with store.database.write_lock, store.database.sessions.begin() as sql:
            await sql.execute(
                update(RunRow).where(RunRow.id == context.run_id).values(stop_requested=True)
            )
        with pytest.raises(ServiceError) as stopped:
            if phase == "renew":
                await store.renew(scope, execution.view.plan_id, execution.token)
            else:
                await invoke(dispatch, context)
        assert stopped.value.code in {"workspace_run_closed", "workspace_tree_model_run_closed"}
        assert await counts(store.database) == ([1, 0, 0, 0] if phase == "change" else [0] * 4)
        assert list(tmp_path.iterdir()) == []
    finally:
        await service.close()


@pytest.mark.parametrize("source", ["directory", "file"])
async def test_model_attempt_wrong_call_and_writer_lock_late_run_stop_fail_closed(
    runtime, tmp_path, monkeypatch, source
):
    service, agent, scope, store, execution, context = await setup(runtime, tmp_path)
    try:
        position = 0
        if source == "file":
            for parent in range(2):
                await publish(scope, store, execution, context, parent)
            position = 2
        preview = await store.dispatch(scope, execution.view.plan_id, execution.token, position)
        await invoke(preview, context)
        dispatch = await store.dispatch(scope, execution.view.plan_id, execution.token, position)
        backend = store.files if source == "file" else store.directories
        original = await backend.get(scope, dispatch.request.proposal_id)
        fingerprint = execution.view.resource_fingerprint
        with pytest.raises(ServiceError) as wrong:
            await backend.reserve(
                scope,
                dispatch.request,
                fingerprint,
                original.prepared,
                tree_fence=dispatch.fence,
                run_id=context.run_id,
                call_id="foreign-start-call",
            )
        assert wrong.value.code == "workspace_tree_model_source_conflict"
        writer = WorkspaceWriter if source == "file" else WorkspaceDirectoryWriter
        original_apply = writer.apply

        def stop_inside_lock(self, prepared, *, authorize=None, **kwargs):
            def invalidate():
                # 原Writer持有文件锁后调用此注入，随后仍调用原资格回调。
                import asyncio

                async def stop():
                    async with store.database.write_lock, store.database.sessions.begin() as sql:
                        await sql.execute(
                            update(RunRow)
                            .where(RunRow.id == context.run_id)
                            .values(stop_requested=True)
                        )

                future = asyncio.run_coroutine_threadsafe(stop(), loop)
                future.result(5)
                authorize()

            return original_apply(self, prepared, authorize=invalidate, **kwargs)

        import asyncio

        loop = asyncio.get_running_loop()
        monkeypatch.setattr(writer, "apply", stop_inside_lock)
        result = await invoke(dispatch, context)
        assert result["status"] == "rejected"
        assert result["receipt"]["changed"] is False
        assert result["receipt"]["error_code"] in {
            "workspace_run_closed",
            "workspace_tree_model_run_closed",
            "workspace_directory_authority",
            "workspace_write_cancelled",
        }
        if source == "directory":
            assert not (tmp_path / "tree").exists()
        else:
            assert not (tmp_path / "tree/sub/data.bin").exists()
    finally:
        await service.close()


async def test_persisted_source_run_or_attempt_call_tamper_is_not_a_new_authority(
    runtime, tmp_path
):
    service, agent, scope, store, execution, context = await setup(runtime, tmp_path)
    try:
        result = await publish(scope, store, execution, context, 0)
        async with store.database.write_lock, store.database.sessions.begin() as sql:
            await sql.execute(
                update(WorkspaceDirectoryAttemptRow)
                .where(WorkspaceDirectoryAttemptRow.operation_id == result["operation_id"])
                .values(call_id="other")
            )
        with pytest.raises(ServiceError) as corrupt:
            await store.get(scope, execution.view.plan_id)
        assert corrupt.value.code == "workspace_tree_corrupt"
        assert await counts(store.database) == [1, 1, 0, 0]
    finally:
        await service.close()


@pytest.mark.parametrize("stage", ["pre", "post"])
@pytest.mark.parametrize(
    "field", ["source_call_id", "tree_fence", "budget_account_ids", "execution_timeouts", "call_id"]
)
async def test_hooks_cannot_change_original_model_step_budget_source_or_audit(
    runtime, tmp_path, stage, field
):
    service, agent, scope, store, execution, context = await setup(runtime, tmp_path)
    try:
        dispatch = await store.dispatch(scope, execution.view.plan_id, execution.token, 0)

        def mutate(child):
            setattr(
                child,
                field,
                {
                    "source_call_id": "forged",
                    "tree_fence": None,
                    "budget_account_ids": ("forged",),
                    "execution_timeouts": (),
                    "call_id": "forged",
                }[field],
            )

        async def pre(definition, child):
            if definition.name == dispatch.name:
                mutate(child)

        async def post(definition, value, child):
            if definition.name == dispatch.name:
                mutate(child)
            return value

        if stage == "pre":
            context.registry.add_global_pre(pre)
        else:
            context.registry.add_global_post(post)
        result = await context.registry.invoke(
            dispatch.name,
            dispatch.request.model_dump(),
            replace(context, tree_fence=dispatch.fence),
        )
        assert result.is_error and result.code == "workspace_tree_fence_mismatch"
        assert await counts(store.database) == ([0] * 4 if stage == "pre" else [1, 0, 0, 0])
        audit = context.registry.audit[-1]
        assert audit.run_id == context.run_id and audit.call_id != "forged" and audit.depth == 2
        assert list(tmp_path.iterdir()) == []
    finally:
        await service.close()
