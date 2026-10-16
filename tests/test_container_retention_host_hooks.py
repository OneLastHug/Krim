"""原Registry所有Hook、策略和后置输出均不能改写回收能力或真实事实。"""

from dataclasses import replace

import pytest
from test_container_retention_host import controller as controller
from test_container_retention_host import drain, request

from krim_agent.protocol.domain import ServiceError
from krim_agent.tools import ShortCircuit
from krim_agent.workspace_container_retention_control import CONTAINER_RETENTION_STEP_TOOLS
from krim_agent.workspace_empty_container_retention import WorkspaceEmptyContainerRetention


@pytest.mark.parametrize("when", ["pre", "post"])
@pytest.mark.parametrize("fault", ["scope", "fence", "inplace", "backend", "deadline", "arguments"])
async def test_true_host_hooks_cannot_replace_original_binding_or_budget(controller, when, fault):
    service, agent, binding, original, preview = controller
    registry = service._handle(agent.agent_id).runtime.tools

    def change(context):
        if fault == "scope":
            context.topic_id = "different-topic"
        elif fault == "fence":
            context.container_retention = replace(context.container_retention, token="0" * 64)
        elif fault == "inplace":
            object.__setattr__(context.container_retention, "token", "0" * 64)
        elif fault == "backend":
            context.workspace_container_retentions = object()
        elif fault == "deadline":
            budget = context.execution_timeouts[-1]
            budget.deadline.reschedule(budget.deadline.when() + 10)
        else:
            context.arguments["operation_id"] = "different-operation"

    async def pre(definition, context):
        if definition.name == CONTAINER_RETENTION_STEP_TOOLS["remove"]:
            change(context)

    async def post(definition, value, context):
        if definition.name == CONTAINER_RETENTION_STEP_TOOLS["remove"]:
            change(context)
        return value

    if when == "pre":
        registry.add_global_pre(pre)
    else:
        registry.add_global_post(post)
    started = await service.workspace_start_container_retention(agent.agent_id, request(preview))
    await drain(service)
    final = await service.workspace_container_retention(agent.agent_id, started.operation_id)
    assert final.status == ("interrupted" if when == "pre" else "unknown"), final
    assert final.intent_recorded is (when == "post")
    assert bool(list(binding.workspace.root.glob("._krim-tx-*"))) is (when == "pre")
    assert final.error_code == "workspace_container_retention_context"
    assert (
        await service.workspace_missing_directory_restore(agent.agent_id, original.plan_id)
        == original
    )
    audit = list(registry.audit)[-1]
    assert not audit.ok and audit.code == final.error_code and audit.run_id == "local"
    assert not service.container_retentions._pending and not service.container_retentions._callbacks


@pytest.mark.parametrize("fault", ["short", "result", "marker"])
async def test_fake_done_never_becomes_durable_success(controller, fault):
    service, agent, binding, _, preview = controller
    registry = service._handle(agent.agent_id).runtime.tools

    async def pre(definition, _context):
        if definition.name == CONTAINER_RETENTION_STEP_TOOLS["remove"]:
            return ShortCircuit({"fake": "done"})

    async def post(definition, value, context):
        if definition.name == CONTAINER_RETENTION_STEP_TOOLS["remove"]:
            if fault == "marker":
                context._container_retention_response = "{}"
            else:
                value["receipt"]["expected_fingerprint"] = "0" * 64
        return value

    if fault == "short":
        registry.add_global_pre(pre)
    else:
        registry.add_global_post(post)
    started = await service.workspace_start_container_retention(agent.agent_id, request(preview))
    await drain(service)
    final = await service.workspace_container_retention(agent.agent_id, started.operation_id)
    assert final.status == ("interrupted" if fault == "short" else "unknown"), final
    assert bool(list(binding.workspace.root.glob("._krim-tx-*"))) is (fault == "short")
    assert final.error_code == "workspace_container_retention_unverified"


@pytest.mark.parametrize("when", ["policy", "pre", "post", "child"])
async def test_self_close_is_rejected_in_all_callback_phases_before_closing_resources(
    controller, when
):
    service, agent, binding, _, preview = controller
    handle = service._handle(agent.agent_id)
    caught = []

    async def closing(definition, _context):
        if definition.name == CONTAINER_RETENTION_STEP_TOOLS["remove"]:
            with pytest.raises(ServiceError) as denied:
                await service.close()
            caught.append(denied.value.code)
        return "allow"

    async def pre(definition, context):
        if when == "child":
            import asyncio

            await asyncio.create_task(closing(definition, context))
        else:
            await closing(definition, context)

    async def post(definition, value, context):
        await closing(definition, context)
        return value

    if when == "policy":
        handle.runtime.tool_policy = closing
    elif when == "post":
        handle.runtime.tools.add_global_post(post)
    else:
        handle.runtime.tools.add_global_pre(pre)
    started = await service.workspace_start_container_retention(agent.agent_id, request(preview))
    await drain(service)
    final = await service.workspace_container_retention(agent.agent_id, started.operation_id)
    assert final.status == "done" and caught == ["workspace_container_retention_reentrant"]
    assert not service._closed and not binding.workspace.closed


async def test_preview_and_verify_post_hooks_cannot_forge_observation(controller, monkeypatch):
    service, agent, binding, original, preview = controller
    registry = service._handle(agent.agent_id).runtime.tools
    current = ["inspect"]

    async def forged(definition, value, _context):
        if definition.name == CONTAINER_RETENTION_STEP_TOOLS[current[0]]:
            if current[0] == "inspect":
                value["fingerprint"] = "0" * 64
            else:
                value["observation"]["fingerprint"] = "0" * 64
        return value

    registry.add_global_post(forged)
    with pytest.raises(ServiceError) as denied:
        await service.workspace_preview_container_retention(agent.agent_id, original.plan_id)
    assert denied.value.code == "workspace_container_retention_unverified"
    assert list(binding.workspace.root.glob("._krim-tx-*"))
    assert not service.container_retentions._previews and not service.container_retentions._pending
    # 后置篡改真实remove回程，保留unknown后再测试核实候选同样不能持久。
    current[0] = "remove"
    actual_remove = WorkspaceEmptyContainerRetention.remove

    def lose(*args, **kwargs):
        actual_remove(*args, **kwargs)
        raise OSError("真实删除之后丢回程")

    monkeypatch.setattr(WorkspaceEmptyContainerRetention, "remove", lose)
    started = await service.workspace_start_container_retention(agent.agent_id, request(preview))
    await drain(service)
    unknown = await service.workspace_container_retention(agent.agent_id, started.operation_id)
    assert unknown.status == "unknown" and not list(binding.workspace.root.glob("._krim-tx-*"))
    current[0] = "verify"
    await service.workspace_verify_container_retention(
        agent.agent_id,
        unknown.operation_id,
        expected_revision=unknown.revision,
        request_key="bad-verify",
    )
    await drain(service)
    final = await service.workspace_container_retention(agent.agent_id, unknown.operation_id)
    assert final.status == "unknown" and final.verification_count == 1
    assert final.error_code == "workspace_container_retention_unverified"
