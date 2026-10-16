"""同一原Move worker的取消/超时/关闭及未知原操作核实，不重放副作用。"""

import asyncio
from threading import Event

import pytest
from test_missing_baseline_rollback_control import action, query, setup
from test_workspace_changes import wait_event

from krim_agent.protocol.domain import ServiceError
from krim_agent.tools import ToolContext, ToolError
from krim_agent.workspace_missing_directory_rollback_control import MissingDirectoryRollbackCancel
from krim_agent.workspace_tree_move import TreeMoveOperationRequest
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter


@pytest.mark.parametrize("phase", ["before", "after", "timeout"])
async def test_cancel_or_timeout_waits_admitted_worker_and_saves_actual_receipt(
    runtime, tmp_path, monkeypatch, phase
):
    service, agent, *_rest, request = await setup(runtime, tmp_path)
    entered, release = Event(), Event()
    original = WorkspaceTreeWriter.apply
    task = None
    try:
        proposed = await service.missing_directory_rollback_preview(agent.agent_id, request)

        def paused(writer, prepared, **kwargs):
            result = original(writer, prepared, **kwargs) if phase == "after" else None
            entered.set()
            assert release.wait(5)
            return result if result is not None else original(writer, prepared, **kwargs)

        monkeypatch.setattr(WorkspaceTreeWriter, "apply", paused)
        if phase == "timeout":
            handle = service._handle(agent.agent_id)
            context = ToolContext(
                handle.runtime.tools,
                topic_id=handle.topic_id,
                agent_id=agent.agent_id,
                session_id=agent.session_id,
                workspace=handle.definition.workspace,
                tree_moves=service.manager.tree_moves,
                policy=handle.runtime.tool_policy,
                origin="host",
                timeout=0.15,
            )
            task = asyncio.create_task(
                context.registry.call_tool(
                    "missing_directory_rollback_change", action(proposed).model_dump(), context
                )
            )
        else:
            task = asyncio.create_task(
                service.missing_directory_rollback_change(agent.agent_id, action(proposed))
            )
        await wait_event(entered)
        if phase == "timeout":
            await asyncio.sleep(0.2)
        else:
            task.cancel()
            await asyncio.sleep(0.02)
            task.cancel()
        assert not task.done(), "取消回程必须等原线程与SQL收束"
        release.set()
        if phase == "timeout":
            with pytest.raises(ToolError) as timed:
                await task
            assert timed.value.code == "timeout"
        else:
            with pytest.raises(asyncio.CancelledError):
                await task
        store = service.manager.tree_moves.store
        actual = await store.operation_by_key("session:" + agent.session_id, "rollback-apply")
        assert actual.view.status == ("done" if phase == "after" else "rejected")
        assert actual.view.receipt.changed == (phase == "after")
        assert (tmp_path / "preserved").exists() == (phase == "after")
        assert not service.manager.tree_moves._operations and not service.manager.tree_moves._active
        assert store.database.engine.sync_engine.pool.checkedout() == 0
    finally:
        release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await service.close()


@pytest.mark.parametrize("phase", ["pre", "post"])
async def test_own_callback_cannot_close_service_before_registry_finishes(runtime, tmp_path, phase):
    service, agent, *_rest, request = await setup(runtime, tmp_path)
    registry = service._handle(agent.agent_id).runtime.tools
    seen = []

    async def closing(definition, child):
        if definition.name == "missing_directory_rollback_change":
            with pytest.raises(ServiceError) as denied:
                await service.close()
            assert denied.value.code == "workspace_tree_move_self_close"
            seen.append(True)

    async def post(definition, value, child):
        await closing(definition, child)
        return value

    try:
        proposed = await service.missing_directory_rollback_preview(agent.agent_id, request)
        if phase == "pre":
            registry.add_global_pre(closing)
        else:
            registry.add_global_post(post)
        result = await service.missing_directory_rollback_change(agent.agent_id, action(proposed))
        assert result.operation.status == "done" and seen == [True]
        assert (
            await query(service, agent, "proposal", identifier=proposed.move.proposal_id)
        ).proposal.rollback_state == "preserved"
    finally:
        await service.close()


async def test_receipt_loss_unknown_reconciles_same_operation_without_new_namespace(
    runtime, tmp_path, monkeypatch
):
    service, agent, *_rest, request = await setup(runtime, tmp_path)
    store = service.manager.tree_moves.store
    save = store.settle
    try:
        proposed = await service.missing_directory_rollback_preview(agent.agent_id, request)

        async def unavailable(*_args, **_kwargs):
            raise OSError("synthetic receipt unavailable")

        monkeypatch.setattr(store, "settle", unavailable)
        returned = await service.missing_directory_rollback_change(agent.agent_id, action(proposed))
        assert returned.operation.status == "uncertain" and returned.operation.recovery_required
        assert returned.operation.receipt is None
        unknown = await store.operation_by_key("session:" + agent.session_id, "rollback-apply")
        assert unknown.view.operation_id == returned.operation.operation_id
        assert unknown.view.status == "uncertain" and unknown.intent_recorded
        assert (tmp_path / "preserved/user-extra").read_bytes() == b"preserve me"
        monkeypatch.setattr(store, "settle", save)

        def forbidden(*_args, **_kwargs):
            pytest.fail("未知核实禁止重发物理写入")

        monkeypatch.setattr(WorkspaceTreeWriter, "apply", forbidden)
        monkeypatch.setattr(WorkspaceTreeWriter, "prepare", forbidden)
        restored = await service.missing_directory_rollback_reconcile(
            agent.agent_id, TreeMoveOperationRequest(operation_id=unknown.view.operation_id)
        )
        assert (
            restored.operation.status == "done"
            and restored.operation.operation_id == unknown.view.operation_id
        )
        assert (
            await query(service, agent, "operation_by_key", request_key="rollback-apply")
        ).operation == restored
    finally:
        await service.close()


@pytest.mark.parametrize("phase", ["before", "after"])
async def test_public_cancel_waits_other_active_worker_before_returning(
    runtime, tmp_path, monkeypatch, phase
):
    service, agent, *_rest, request = await setup(runtime, tmp_path)
    entered, release = Event(), Event()
    saved = asyncio.Event()
    original = WorkspaceTreeWriter.apply
    store = service.manager.tree_moves.store
    worker = cancelling = None
    try:
        proposed = await service.missing_directory_rollback_preview(agent.agent_id, request)

        def paused(writer, prepared, **kwargs):
            effect = original(writer, prepared, **kwargs) if phase == "after" else None
            entered.set()
            assert release.wait(5)
            return effect if effect is not None else original(writer, prepared, **kwargs)

        cancel = store.cancel

        async def recorded(*args):
            result = await cancel(*args)
            saved.set()
            return result

        monkeypatch.setattr(WorkspaceTreeWriter, "apply", paused)
        monkeypatch.setattr(store, "cancel", recorded)
        worker = asyncio.create_task(
            service.missing_directory_rollback_change(agent.agent_id, action(proposed))
        )
        await wait_event(entered)
        cancelling = asyncio.create_task(
            service.missing_directory_rollback_cancel(
                agent.agent_id,
                MissingDirectoryRollbackCancel(proposal_id=proposed.move.proposal_id),
            )
        )
        async with asyncio.timeout(5):
            await saved.wait()
        done, _ = await asyncio.wait({cancelling}, timeout=0.3)
        assert not done, "外部取消回程不能跳过仍在运行的原worker"
        release.set()
        actual = await worker
        cancelled = await cancelling
        assert actual.operation.status == ("done" if phase == "after" else "rejected")
        assert actual.operation.cancelled
        assert cancelled.rollback_state == ("preserved" if phase == "after" else "cancelled")
        assert (tmp_path / "preserved").exists() == (phase == "after")
        assert not service.manager.tree_moves._active and not service.manager.tree_moves._operations
        assert not service.manager.tree_moves._active_parents
    finally:
        release.set()
        await asyncio.gather(
            *(task for task in (worker, cancelling) if task is not None), return_exceptions=True
        )
        await service.close()


async def test_controlled_cancel_from_original_authority_does_not_wait_for_itself(
    runtime, tmp_path, monkeypatch
):
    service, agent, *_rest, request = await setup(runtime, tmp_path)
    store = service.manager.tree_moves.store
    completed = asyncio.Event()
    returned_while_active = []
    try:
        proposed = await service.missing_directory_rollback_preview(agent.agent_id, request)
        original = store.assert_execution
        called = False

        async def cancel_from_authority(*args):
            nonlocal called
            await original(*args)
            if not called:
                called = True
                try:
                    await service.missing_directory_rollback_cancel(
                        agent.agent_id,
                        MissingDirectoryRollbackCancel(proposal_id=proposed.move.proposal_id),
                    )
                    returned_while_active.append(
                        proposed.move.proposal_id in service.manager.tree_moves._active
                    )
                finally:
                    completed.set()

        monkeypatch.setattr(store, "assert_execution", cancel_from_authority)
        actual = await service.missing_directory_rollback_change(agent.agent_id, action(proposed))
        async with asyncio.timeout(1):
            await completed.wait()
        assert returned_while_active == [True], "同一producer内部控制调用不能等待自身完成"
        assert actual.operation.status == "rejected" and actual.operation.cancelled
        assert not actual.operation.receipt.changed
        assert (tmp_path / "tree/user-extra").read_bytes() == b"preserve me"
        assert not (tmp_path / "preserved").exists()
    finally:
        await service.close()
