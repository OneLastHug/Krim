"""根撤销原同服务producer、双入口取消、unknown、预算/关闭与冷事实。"""

import asyncio
from dataclasses import replace
from threading import Event

import pytest
from test_generated_tree_undo_ledger import counts
from test_generated_tree_undo_tools import action, query, setup
from test_tree_move_tools import context
from test_workspace_changes import wait_event

from krim_agent.generated_tree_undo import GeneratedTreeUndoCancelRequest
from krim_agent.persistence.database import RunRow
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import PythonAI
from krim_agent.tools import ToolError
from krim_agent.workspace_tree_move import TreeMoveOperationRequest
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter


@pytest.mark.parametrize("phase", ["before", "after", "timeout"])
async def test_root_cancel_and_timeout_drain_original_admitted_writer_and_save_real_effect(
    runtime, tmp_path, monkeypatch, phase
):
    service, agent, scope, trees, plan, binding, snapshot, request = await setup(runtime, tmp_path)
    entered, release = Event(), Event()
    original = WorkspaceTreeWriter.apply
    task = None
    try:
        proposal = await service.generated_tree_undo_preview(agent.agent_id, request)

        def paused(writer, prepared, **kwargs):
            effect = original(writer, prepared, **kwargs) if phase == "after" else None
            entered.set()
            assert release.wait(10)
            return effect if effect is not None else original(writer, prepared, **kwargs)

        monkeypatch.setattr(WorkspaceTreeWriter, "apply", paused)
        ctx = await context(service, agent)
        captured = []

        async def observe(_definition, child):
            captured.append(child)

        ctx.registry.add_global_pre(observe)
        if phase == "timeout":
            ctx.timeout = 30
        task = asyncio.create_task(
            ctx.registry.call_tool("generated_tree_undo_change", action(proposal).model_dump(), ctx)
        )
        await wait_event(entered)
        if phase == "timeout":
            captured[-1].execution_timeouts[-1].deadline.reschedule(
                asyncio.get_running_loop().time() + 0.1
            )
            await asyncio.sleep(0.15)
        else:
            task.cancel()
            await asyncio.sleep(0.02)
            task.cancel()
        assert not task.done(), "原根线程未收束，不能提前返回取消或超时"
        release.set()
        if phase == "timeout":
            with pytest.raises(ToolError) as error:
                await task
            assert error.value.code == "timeout"
        else:
            with pytest.raises(asyncio.CancelledError):
                await task
        mover = service.manager.tree_moves
        saved = await mover.store.operation_by_key(scope, "root-remove")
        assert saved.view.status == ("done" if phase == "after" else "rejected")
        assert saved.view.receipt.changed == (phase == "after")
        assert (tmp_path / "retained").exists() == (phase == "after")
        assert not mover._active and not mover._operations
        assert (await counts(mover.store.database))[:4] == [1, 1, 1, 0]
    finally:
        release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await service.close()


@pytest.mark.parametrize("controller", ["move", "root"])
async def test_both_public_controllers_cancel_the_same_root_producer_without_new_operation(
    runtime, tmp_path, monkeypatch, controller
):
    service, agent, scope, trees, plan, binding, snapshot, request = await setup(runtime, tmp_path)
    entered, release = Event(), Event()
    original = WorkspaceTreeWriter.apply
    worker = cancelling = None
    try:
        proposal = await service.generated_tree_undo_preview(agent.agent_id, request)

        def paused(writer, prepared, **kwargs):
            entered.set()
            assert release.wait(10)
            return original(writer, prepared, **kwargs)

        monkeypatch.setattr(WorkspaceTreeWriter, "apply", paused)
        worker = asyncio.create_task(
            service.generated_tree_undo_change(agent.agent_id, action(proposal))
        )
        await wait_event(entered)
        mover = service.manager.tree_moves
        saved_cancel = asyncio.Event()
        original_cancel = mover.store.cancel

        async def notified(bound_scope, identifier):
            value = await original_cancel(bound_scope, identifier)
            saved_cancel.set()
            return value

        monkeypatch.setattr(mover.store, "cancel", notified)
        cancelling = asyncio.create_task(
            service.tree_move_cancel(agent.agent_id, proposal.move.proposal_id)
            if controller == "move"
            else service.generated_tree_undo_cancel(
                agent.agent_id,
                GeneratedTreeUndoCancelRequest(proposal_id=proposal.move.proposal_id),
            )
        )
        async with asyncio.timeout(5):
            await saved_cancel.wait()
        assert (await mover.store.operation_by_key(scope, "root-remove")).view.cancelled
        if controller == "move":
            assert not cancelling.done()
        release.set()
        denied = await worker
        await cancelling
        assert denied.operation.status == "rejected" and denied.operation.cancelled
        current = (
            await query(service, agent, "proposal", identifier=proposal.move.proposal_id)
        ).proposal
        assert current.root_state == "cancelled" and current.generation_status == "done"
        assert await service.generated_tree_undo_change(agent.agent_id, action(proposal)) == denied
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff" and not (
            tmp_path / "retained"
        ).exists()
        assert (await counts(mover.store.database))[:4] == [1, 1, 1, 0]
    finally:
        release.set()
        await asyncio.gather(
            *(task for task in (worker, cancelling) if task is not None), return_exceptions=True
        )
        await service.close()


@pytest.mark.parametrize("kind", ["remove_root", "keep"])
async def test_root_receipt_unknown_reconciles_same_operation_with_zero_namespace_or_source_replay(
    runtime, tmp_path, monkeypatch, kind
):
    service, agent, scope, trees, plan, binding, snapshot, request = await setup(runtime, tmp_path)
    try:
        proposal = await service.generated_tree_undo_preview(agent.agent_id, request)
        if kind == "keep":
            assert (
                await service.generated_tree_undo_change(agent.agent_id, action(proposal))
            ).operation.status == "done"
            proposal = (
                await query(service, agent, "proposal", identifier=proposal.move.proposal_id)
            ).proposal
        mover = service.manager.tree_moves
        original = mover.store.settle

        async def unavailable(*_args, **_kwargs):
            raise OSError("原根实际效果后的回执保存中断")

        monkeypatch.setattr(mover.store, "settle", unavailable)
        unknown = await service.generated_tree_undo_change(
            agent.agent_id, action(proposal, kind, "root-save-unknown")
        )
        assert unknown.operation.status == "uncertain" and unknown.operation.recovery_required
        assert (tmp_path / "retained/sub/data.bin").read_bytes() == b"\0\xff"
        monkeypatch.setattr(mover.store, "settle", original)

        def forbidden(*_args, **_kwargs):
            pytest.fail("根unknown不重放Provider/生成/prepare/原Writer")

        monkeypatch.setattr(WorkspaceTreeWriter, "apply", forbidden)
        monkeypatch.setattr(WorkspaceTreeWriter, "prepare", forbidden)
        resolved = await service.generated_tree_undo_reconcile(
            agent.agent_id, TreeMoveOperationRequest(operation_id=unknown.operation.operation_id)
        )
        assert (
            resolved.operation.status == "done"
            and resolved.operation.operation_id == unknown.operation.operation_id
        )
        assert resolved.operation.receipt.changed == (kind == "remove_root")
        assert resolved.source_plan_id == plan.plan_id
        assert (
            await service.generated_tree_undo_change(
                agent.agent_id, action(proposal, kind, "root-save-unknown")
            )
            == resolved
        )
        monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
        assert (
            await query(service, agent, "operation", identifier=resolved.operation.operation_id)
        ).operation == resolved
    finally:
        await service.close()


@pytest.mark.parametrize("phase", ["pre", "post"])
@pytest.mark.parametrize("target", ["service", "runtime", "sdk"])
async def test_root_registry_self_close_refuses_before_sdk_or_database_release(
    runtime, tmp_path, phase, target
):
    service, agent, scope, trees, plan, binding, snapshot, request = await setup(runtime, tmp_path)
    ai = PythonAI(runtime)
    ai._agent_service = service
    caught = []
    try:
        handle = service._handle(agent.agent_id)

        async def close_self():
            closer = {"service": service.close, "runtime": handle.runtime.close, "sdk": ai.close}[
                target
            ]
            with pytest.raises(ServiceError) as error:
                await closer()
            caught.append(error.value.code)
            assert not ai._closed and not service._closed and not service.manager._closed

        async def pre(_definition, _child):
            if phase == "pre":
                await close_self()

        async def post(_definition, value, _child):
            if phase == "post":
                await close_self()
            return value

        handle.runtime.tools.add_global_pre(pre)
        handle.runtime.tools.add_global_post(post)
        assert (await query(service, agent, "proposals")).proposals.items == []
        assert caught == ["workspace_tree_move_self_close"]
    finally:
        await ai.close()


async def test_root_cold_closed_session_and_disabled_resource_history_is_read_only(
    runtime, tmp_path, monkeypatch
):
    service, agent, scope, trees, plan, binding, snapshot, request = await setup(runtime, tmp_path)
    try:
        proposal = await service.generated_tree_undo_preview(agent.agent_id, request)
        await agent.close(expected_revision=(await agent.state()).revision)
        service.disable_workspace("files")

        def forbidden(*_args, **_kwargs):
            pytest.fail("关闭会话/停用资源只能读取原材料，不观察路径或恢复写能力")

        monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
        monkeypatch.setattr(WorkspaceTreeWriter, "apply", forbidden)
        assert (
            await query(service, agent, "proposal", identifier=proposal.move.proposal_id)
        ).proposal == proposal
        with pytest.raises(ServiceError):
            await service.generated_tree_undo_change(agent.agent_id, action(proposal))
        assert await counts(service.manager.tree_moves.store.database) == [1, 1, 0, 0, 0]
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["depth", "stopped_run", "disabled"])
async def test_root_original_depth_run_and_resource_refuse_before_operation(
    runtime, tmp_path, fault
):
    service, agent, scope, trees, plan, binding, snapshot, request = await setup(runtime, tmp_path)
    try:
        proposal = await service.generated_tree_undo_preview(agent.agent_id, request)
        ctx = await context(service, agent, model=True, call_id="root-boundary")
        if fault == "depth":
            ctx.depth = ctx.registry.max_depth
        elif fault == "disabled":
            service.disable_workspace("files")
        else:
            async with service.manager.tree_moves.store.database.sessions.begin() as sql:
                (await sql.get(RunRow, ctx.run_id)).stop_requested = True
        with pytest.raises(ToolError) as error:
            await ctx.registry.call_tool(
                "generated_tree_undo_change", action(proposal).model_dump(), ctx
            )
        assert (
            error.value.code
            == {
                "depth": "max_depth",
                "stopped_run": "workspace_run_closed",
                "disabled": "workspace_unavailable",
            }[fault]
        )
        assert (await counts(service.manager.tree_moves.store.database))[:3] == [1, 1, 0]
    finally:
        await service.close()


async def test_root_model_same_call_cold_retry_rejects_new_caller_before_scan(
    runtime, tmp_path, monkeypatch
):
    service, agent, scope, trees, plan, binding, snapshot, request = await setup(runtime, tmp_path)
    try:
        ctx = await context(service, agent, model=True, call_id="root-preview-call")
        original = await ctx.registry.call_tool(
            "generated_tree_undo_preview", request.model_dump(), ctx
        )

        def forbidden(*_args, **_kwargs):
            pytest.fail("根模型原key重试不得重新读取或prepare")

        monkeypatch.setattr(WorkspaceTreeWriter, "prepare", forbidden)
        monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
        assert (
            await ctx.registry.call_tool("generated_tree_undo_preview", request.model_dump(), ctx)
            == original
        )
        with pytest.raises(ToolError) as error:
            await ctx.registry.call_tool(
                "generated_tree_undo_preview",
                request.model_dump(),
                replace(ctx, call_id="other-call"),
            )
        assert error.value.code == "workspace_tree_move_source_conflict"
        assert await counts(service.manager.tree_moves.store.database) == [1, 1, 0, 0, 1]
    finally:
        await service.close()
