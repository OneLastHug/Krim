"""真实IO线程、反复取消、关闭准入与物理SQL桥接共同收束。"""

import asyncio
import time
from threading import Event

import pytest
from test_container_retention_host import controller as controller
from test_container_retention_host import drain, request

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_container_retention_control import CONTAINER_RETENTION_STEP_TOOLS
from krim_agent.workspace_empty_container_retention import WorkspaceEmptyContainerRetention


@pytest.mark.parametrize("action", ["cancel", "close", "backend_close"])
async def test_repeated_caller_cancellation_waits_for_original_thread_before_resource_close(
    controller, monkeypatch, action
):
    service, agent, binding, original, preview = controller
    actual = WorkspaceEmptyContainerRetention.remove
    entered, release, left = Event(), Event(), Event()

    def block(*args, **kwargs):
        entered.set()
        try:
            assert release.wait(4), "测试主动释放原有界IO线程"
            return actual(*args, **kwargs)
        finally:
            left.set()

    monkeypatch.setattr(WorkspaceEmptyContainerRetention, "remove", block)
    started = await service.workspace_start_container_retention(agent.agent_id, request(preview))
    assert await asyncio.to_thread(entered.wait, 3)
    stopping = asyncio.create_task(
        service.workspace_container_retention_cancel(
            agent.agent_id, started.operation_id, expected_revision=started.revision
        )
        if action == "cancel"
        else service.close()
        if action == "close"
        else service.container_retentions.close()
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
    final = await service.workspace_container_retention(agent.agent_id, started.operation_id)
    assert final.status in {"cancelled", "interrupted"} and not final.intent_recorded, final
    assert left.is_set() and not service.container_retentions._pending
    assert not service.container_retentions._callbacks and not service.container_retentions._jobs
    assert (
        await service.workspace_missing_directory_restore(agent.agent_id, original.plan_id)
        == original
    )
    assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"\0\xff"
    assert list(binding.workspace.root.glob("._krim-tx-*"))
    assert service.container_retention_store.database.engine.sync_engine.pool.checkedout() == 0


async def test_original_pre_hook_can_cancel_own_operation_without_waiting_for_itself(controller):
    service, agent, binding, _, preview = controller

    async def cancel(definition, context):
        if definition.name == CONTAINER_RETENTION_STEP_TOOLS["remove"]:
            identifier = context.container_retention.operation_id
            view = await service.workspace_container_retention(agent.agent_id, identifier)
            await service.workspace_container_retention_cancel(
                agent.agent_id, identifier, expected_revision=view.revision
            )

    service._handle(agent.agent_id).runtime.tools.add_global_pre(cancel)
    started = await service.workspace_start_container_retention(agent.agent_id, request(preview))
    await asyncio.wait_for(drain(service), 5)
    final = await service.workspace_container_retention(agent.agent_id, started.operation_id)
    assert final.status == "cancelled" and not final.intent_recorded
    assert list(binding.workspace.root.glob("._krim-tx-*"))


async def test_capacity_and_parallel_same_key_join_do_not_allocate_or_reenter_physical_job(
    controller, monkeypatch
):
    service, agent, binding, original, preview = controller
    service.container_retentions.max_active = 1
    entered, release = Event(), Event()
    actual = WorkspaceEmptyContainerRetention.remove
    calls = []

    def block(*args, **kwargs):
        entered.set()
        calls.append(True)
        assert release.wait(4)
        return actual(*args, **kwargs)

    monkeypatch.setattr(WorkspaceEmptyContainerRetention, "remove", block)
    wanted = request(preview)
    started = await service.workspace_start_container_retention(agent.agent_id, wanted)
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        repeated = await asyncio.gather(
            *[service.workspace_start_container_retention(agent.agent_id, wanted) for _ in range(8)]
        )
        assert all(view == started for view in repeated)
        assert len(service.container_retentions._tasks) == 1 and len(calls) == 1
        with pytest.raises(ServiceError) as denied:
            await service.workspace_preview_container_retention(agent.agent_id, original.plan_id)
        assert denied.value.code == "workspace_container_retention_busy"
        assert len(service.container_retentions._tasks) == 1
    finally:
        release.set()
    await drain(service)
    final = await service.workspace_container_retention(agent.agent_id, started.operation_id)
    assert final.status == "done" and len(calls) == 1
    assert not list(binding.workspace.root.glob("._krim-tx-*"))


async def test_blocked_physical_authorization_expires_at_original_five_seconds_and_drains_callback(
    controller, monkeypatch
):
    service, agent, binding, _, preview = controller
    actual = service.manager.workspace_changes._guard
    entered, left = asyncio.Event(), asyncio.Event()
    times = []

    async def block(context, **kwargs):
        selector = context.container_retention
        if (
            selector is not None
            and asyncio.current_task()
            in service.container_retentions._callbacks.get(selector.operation_id, set())
        ):
            entered.set()
            times.append(time.monotonic())
            try:
                await asyncio.Event().wait()
            finally:
                left.set()
        return await actual(context, **kwargs)

    monkeypatch.setattr(service.manager.workspace_changes, "_guard", block)
    started = await service.workspace_start_container_retention(agent.agent_id, request(preview))
    await asyncio.wait_for(entered.wait(), 3)
    await asyncio.wait_for(drain(service), 6)
    assert time.monotonic() - times[0] < 5.8 and left.is_set()
    final = await service.workspace_container_retention(agent.agent_id, started.operation_id)
    assert final.status == "rejected" and not final.intent_recorded
    assert list(binding.workspace.root.glob("._krim-tx-*"))
    assert not service.container_retentions._callbacks
    assert service.container_retention_store.database.engine.sync_engine.pool.checkedout() == 0


async def test_cancelled_preview_waits_for_original_inspection_thread(controller, monkeypatch):
    service, agent, binding, original, _ = controller
    actual = WorkspaceEmptyContainerRetention.inspect
    entered, release, left = Event(), Event(), Event()

    def block(*args, **kwargs):
        entered.set()
        try:
            assert release.wait(4)
            return actual(*args, **kwargs)
        finally:
            left.set()

    monkeypatch.setattr(WorkspaceEmptyContainerRetention, "inspect", block)
    viewing = asyncio.create_task(
        service.workspace_preview_container_retention(agent.agent_id, original.plan_id)
    )
    assert await asyncio.to_thread(entered.wait, 3)
    try:
        for _ in range(3):
            viewing.cancel()
            await asyncio.sleep(0)
        assert not viewing.done() and not left.is_set() and not binding.workspace.closed
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await viewing
    assert left.is_set() and not service.container_retentions._previews
    assert not service.container_retentions._jobs and not service.container_retentions._callbacks
