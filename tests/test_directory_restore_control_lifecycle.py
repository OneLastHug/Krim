"""取消、全阶段自关闭和未知核实必须沿原操作并收束真实IO线程。"""

import asyncio
import time
from threading import Event

import pytest
from test_directory_restore_control import drain, execute_request
from test_directory_restore_control_guards import controller as controller

import krim_agent.workspace_directory_restore_writer as physical
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_directory_restore_control import DIRECTORY_RESTORE_STEP_TOOLS
from krim_agent.workspace_directory_restore_writer import WorkspaceDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


async def test_unknown_reply_is_verified_by_read_leaf_same_operation_zero_publication_replay(
    controller, monkeypatch
):
    service, agent, binding, _, planned = controller
    rename, calls = physical.rename_at, []

    def lost(*args, **kwargs):
        rename(*args, **kwargs)
        calls.append(True)
        raise OSError("真实namespace已发布但回程丢失")

    monkeypatch.setattr(physical, "rename_at", lost)
    started = await service.workspace_start_directory_restore(
        agent.agent_id, execute_request(planned)
    )
    await drain(service)
    original = await service.workspace_directory_restore(agent.agent_id, started.plan_id)
    assert original.phase == "uncertain" and len(calls) == 1

    def forbidden(*_args, **_kwargs):
        pytest.fail("核实只能读原双侧，不能重新生产")

    monkeypatch.setattr(physical, "rename_at", forbidden)
    monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
    monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "prepare", forbidden)
    monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "apply", forbidden)
    await service.workspace_verify_directory_restore(agent.agent_id, original.operation_id)
    await drain(service)
    actual = await service.workspace_directory_restore(agent.agent_id, original.plan_id)
    assert actual.phase == "finished" and actual.operation_id == original.operation_id
    assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"\0\xff"
    audit = list(service._handle(agent.agent_id).runtime.tools.audit)[-1]
    assert audit.name == DIRECTORY_RESTORE_STEP_TOOLS["verify"] and audit.ok


@pytest.mark.parametrize("when", ["pre", "policy"])
async def test_root_hook_or_policy_close_is_rejected_before_lifecycle_changes(controller, when):
    service, agent, _, _, planned = controller
    handle = service._handle(agent.agent_id)
    caught = []

    async def close(definition, _context):
        if definition.name == DIRECTORY_RESTORE_STEP_TOOLS["execute"]:
            with pytest.raises(ServiceError) as denied:
                await service.close()
            caught.append(denied.value.code)
        return "allow"

    async def pre(definition, context):
        await close(definition, context)

    if when == "pre":
        handle.runtime.tools.add_global_pre(pre)
    else:
        handle.runtime.tool_policy = close
    await service.workspace_start_directory_restore(agent.agent_id, execute_request(planned))
    await drain(service)
    assert caught == ["workspace_directory_restore_reentrant"]
    assert not service._closed and not service.directory_restores._closed
    assert (
        await service.workspace_directory_restore(agent.agent_id, planned.plan_id)
    ).status == "done"


async def test_root_pre_can_cancel_own_operation_without_waiting_for_itself(controller):
    service, agent, binding, _, planned = controller
    registry = service._handle(agent.agent_id).runtime.tools
    entered = []

    async def pre(definition, context):
        if definition.name == DIRECTORY_RESTORE_STEP_TOOLS["execute"]:
            entered.append(True)
            await service.workspace_directory_restore_cancel(
                agent.agent_id, context.directory_restore.plan_id
            )

    registry.add_global_pre(pre)
    await service.workspace_start_directory_restore(agent.agent_id, execute_request(planned))
    await asyncio.wait_for(drain(service), 5)
    result = await service.workspace_directory_restore(agent.agent_id, planned.plan_id)
    assert entered and result.phase == "cancelled" and not result.cleanup_pending
    assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"current\x80\0"
    assert not service.directory_restores._pending


@pytest.mark.parametrize("action", ["cancel", "close", "controller_close"])
async def test_repeated_external_cancellation_waits_for_real_thread_before_closing_resources(
    controller, monkeypatch, action
):
    service, agent, binding, _, planned = controller
    original = WorkspaceDirectoryRestoreWriter._write_tree
    entered, release, left = Event(), Event(), Event()

    def block(*args, **kwargs):
        entered.set()
        try:
            assert release.wait(4), "测试必须主动释放原有界IO线程"
            return original(*args, **kwargs)
        finally:
            left.set()

    monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "_write_tree", block)
    started = await service.workspace_start_directory_restore(
        agent.agent_id, execute_request(planned)
    )
    assert await asyncio.to_thread(entered.wait, 3)
    stopping = asyncio.create_task(
        service.workspace_directory_restore_cancel(agent.agent_id, started.plan_id)
        if action == "cancel"
        else service.close()
        if action == "close"
        else service.directory_restores.close()
    )
    try:
        await asyncio.sleep(0.03)
        assert not stopping.done() and not left.is_set() and not binding.workspace.closed
        for _ in range(3):
            stopping.cancel()
            await asyncio.sleep(0)
        assert not stopping.done() and not left.is_set()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await stopping
    await drain(service)
    assert (
        left.is_set()
        and not service.directory_restores._pending
        and not service.directory_restores._callbacks
    )
    scope = "session:" + agent.session_id
    result = await service.directory_restore_store.get(scope, planned.plan_id)
    assert result.phase == "cancelled" and result.cleanup_pending
    assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"current\x80\0"
    assert service.directory_restore_store.database.engine.sync_engine.pool.checkedout() == 0


async def test_blocked_original_qualification_stops_at_physical_deadline_and_drains_sql_callback(
    controller, monkeypatch
):
    service, agent, binding, _, planned = controller
    original = service.manager.workspace_changes._guard
    entered, left = asyncio.Event(), asyncio.Event()
    started_at = []

    async def blocked(context, **kwargs):
        fence = context.directory_restore
        if (
            fence is not None
            and asyncio.current_task()
            in service.directory_restores._callbacks.get(fence.operation_id, set())
        ):
            started_at.append(time.monotonic())
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                left.set()
        return await original(context, **kwargs)

    monkeypatch.setattr(service.manager.workspace_changes, "_guard", blocked)
    started = await service.workspace_start_directory_restore(
        agent.agent_id, execute_request(planned)
    )
    await asyncio.wait_for(entered.wait(), 3)
    await asyncio.wait_for(drain(service), 6)
    elapsed = time.monotonic() - started_at[0]
    assert elapsed < 5.8 and left.is_set()
    result = await service.workspace_directory_restore(agent.agent_id, started.plan_id)
    assert result.phase == "failed" and not result.cleanup_pending, result
    operation = await service.workspace_directory_restore_operation(
        agent.agent_id, result.operation_id
    )
    assert not operation.intent_recorded
    assert not service.directory_restores._callbacks
    assert not list(binding.workspace.root.glob("._krim-tx-*"))
    assert service.directory_restore_store.database.engine.sync_engine.pool.checkedout() == 0
