"""原预算、核实原键和关闭期间准入的真实交界。"""

import asyncio
from threading import Event

import pytest
from test_container_retention_host import controller as controller
from test_container_retention_host import drain, request

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_container_retention_control import CONTAINER_RETENTION_STEP_TOOLS
from krim_agent.workspace_empty_container_retention import WorkspaceEmptyContainerRetention


@pytest.mark.parametrize("kind", ["inspect", "remove"])
async def test_policy_cannot_extend_original_budget_before_handler(controller, kind):
    service, agent, binding, original, preview = controller

    async def policy(definition, context):
        if definition.name == CONTAINER_RETENTION_STEP_TOOLS[kind]:
            budget = context.execution_timeouts[-1]
            budget.deadline.reschedule(budget.deadline.when() + 10)
        return "allow"

    service._handle(agent.agent_id).runtime.tool_policy = policy
    if kind == "inspect":
        with pytest.raises(ServiceError) as denied:
            await service.workspace_preview_container_retention(agent.agent_id, original.plan_id)
        assert denied.value.code == "workspace_container_retention_context"
    else:
        started = await service.workspace_start_container_retention(
            agent.agent_id, request(preview)
        )
        await drain(service)
        final = await service.workspace_container_retention(agent.agent_id, started.operation_id)
        assert final.status == "interrupted" and not final.intent_recorded
        assert final.error_code == "workspace_container_retention_context"
    assert list(binding.workspace.root.glob("._krim-tx-*"))


async def test_same_verify_key_joins_active_operation_and_other_key_cannot_spawn_again(
    controller, monkeypatch
):
    service, agent, _, _, preview = controller

    async def bad_receipt(definition, value, context):
        if definition.name == CONTAINER_RETENTION_STEP_TOOLS["remove"]:
            context._container_retention_response = "{}"
        return value

    service._handle(agent.agent_id).runtime.tools.add_global_post(bad_receipt)
    started = await service.workspace_start_container_retention(agent.agent_id, request(preview))
    await drain(service)
    unknown = await service.workspace_container_retention(agent.agent_id, started.operation_id)
    assert unknown.status == "unknown"
    entered, release = Event(), Event()
    actual = WorkspaceEmptyContainerRetention.inspect
    calls = []

    def block(*args, **kwargs):
        entered.set()
        calls.append(True)
        assert release.wait(4)
        return actual(*args, **kwargs)

    monkeypatch.setattr(WorkspaceEmptyContainerRetention, "inspect", block)
    started = await service.workspace_verify_container_retention(
        agent.agent_id,
        unknown.operation_id,
        expected_revision=unknown.revision,
        request_key="single-verification",
    )
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        assert (
            await service.workspace_verify_container_retention(
                agent.agent_id,
                unknown.operation_id,
                expected_revision=unknown.revision,
                request_key="single-verification",
            )
            == started
        )
        with pytest.raises(ServiceError) as denied:
            await service.workspace_verify_container_retention(
                agent.agent_id,
                unknown.operation_id,
                expected_revision=started.revision,
                request_key="different-verification",
            )
        assert denied.value.code == "workspace_container_retention_busy"
        assert len(service.container_retentions._tasks) == 1 and len(calls) == 1
    finally:
        release.set()
    await drain(service)
    final = await service.workspace_container_retention(agent.agent_id, unknown.operation_id)
    assert final.status == "done" and final.receipt.outcome == "absence_verified"
    assert final.verification_count == 1 and len(calls) == 1


async def test_close_refuses_new_admission_before_waiting_for_original_thread(
    controller, monkeypatch
):
    service, agent, binding, original, preview = controller
    entered, release = Event(), Event()
    actual = WorkspaceEmptyContainerRetention.remove

    def block(*args, **kwargs):
        entered.set()
        assert release.wait(4)
        return actual(*args, **kwargs)

    monkeypatch.setattr(WorkspaceEmptyContainerRetention, "remove", block)
    started = await service.workspace_start_container_retention(agent.agent_id, request(preview))
    assert await asyncio.to_thread(entered.wait, 3)
    marked = asyncio.Event()
    actual_stop = service.container_retentions._stop

    def stopped(identifier):
        if service.container_retentions._closed:
            marked.set()
        actual_stop(identifier)

    monkeypatch.setattr(service.container_retentions, "_stop", stopped)
    closing = asyncio.create_task(service.container_retentions.close())
    try:
        await asyncio.wait_for(marked.wait(), 2)
        assert not closing.done() and not binding.workspace.closed
        with pytest.raises(ServiceError) as denied:
            await service.workspace_start_container_retention(
                agent.agent_id, request(preview, "new-key-during-close")
            )
        assert denied.value.code == "workspace_container_retention_closed"
        with pytest.raises(ServiceError) as denied:
            await service.workspace_preview_container_retention(agent.agent_id, original.plan_id)
        assert denied.value.code == "workspace_container_retention_closed"
        old = await service.workspace_start_container_retention(agent.agent_id, request(preview))
        assert old.operation_id == started.operation_id
    finally:
        release.set()
        await closing
    assert not service.container_retentions._jobs


async def test_tool_deadline_covers_policy_wait_and_never_dispatches_physical_remove(controller):
    service, agent, binding, _, preview = controller
    handle = service._handle(agent.agent_id)
    from dataclasses import replace

    handle.definition = replace(handle.definition, tool_timeout_seconds=0.03)

    async def slow_policy(definition, _context):
        if definition.name == CONTAINER_RETENTION_STEP_TOOLS["remove"]:
            await asyncio.sleep(0.1)
        return "allow"

    handle.runtime.tool_policy = slow_policy
    started = await service.workspace_start_container_retention(agent.agent_id, request(preview))
    await drain(service)
    final = await service.workspace_container_retention(agent.agent_id, started.operation_id)
    assert final.status == "interrupted" and not final.intent_recorded
    assert final.error_code == "timeout"
    assert list(binding.workspace.root.glob("._krim-tx-*"))
