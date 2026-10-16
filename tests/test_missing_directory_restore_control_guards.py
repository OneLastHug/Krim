"""原Registry在全部Hook前后冻结目录恢复能力，拒绝伪来源与假回程。"""

from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_missing_directory_restore_control import drain, execute_request, ready, setup

from krim_agent.tools import ShortCircuit, ToolContext
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_missing_directory_restore import MissingDirectoryRestoreFence
from krim_agent.workspace_missing_directory_restore_control import (
    MISSING_DIRECTORY_RESTORE_STEP_TOOLS,
)


@pytest.fixture
async def controller(runtime, tmp_path):
    service, agent, binding = await setup(runtime, tmp_path)
    try:
        target, planned = await ready(service, agent, binding)
        yield service, agent, binding, target, planned
    finally:
        await service.close()


@pytest.mark.parametrize("when", ["pre", "post"])
@pytest.mark.parametrize(
    "fault",
    ["arguments", "scope", "origin", "fence", "backend", "budget", "deadline", "mutated_fence"],
)
async def test_original_registry_rejects_changed_capability_and_preserves_true_effect(
    controller, when, fault
):
    service, agent, binding, _, planned = controller
    handle = service._handle(agent.agent_id)

    def change(context):
        if fault == "arguments":
            context.arguments["plan_id"] = "unrelated-plan"
        elif fault == "scope":
            context.topic_id = "unrelated-topic"
        elif fault == "origin":
            context.origin = "model"
        elif fault == "fence":
            context.directory_restore = replace(context.directory_restore, token="0" * 64)
        elif fault == "backend":
            context.workspace_directory_restores = object()
        elif fault == "budget":
            context.timeout += 10
        elif fault == "deadline":
            budget = context.execution_timeouts[-1]
            budget.deadline.reschedule(budget.deadline.when() + 10)
        else:
            object.__setattr__(context.directory_restore, "token", "0" * 64)

    async def pre(definition, context):
        if definition.name == MISSING_DIRECTORY_RESTORE_STEP_TOOLS["execute"]:
            change(context)

    async def post(definition, value, context):
        if definition.name == MISSING_DIRECTORY_RESTORE_STEP_TOOLS["execute"]:
            change(context)
        return value

    if when == "pre":
        handle.runtime.tools.add_global_pre(pre)
    else:
        handle.runtime.tools.add_global_post(post)
    started = await service.workspace_start_missing_directory_restore(
        agent.agent_id, execute_request(planned)
    )
    await drain(service)
    result = await service.workspace_missing_directory_restore(agent.agent_id, started.plan_id)
    assert result.phase == ("failed" if when == "pre" else "uncertain"), result
    operation = await service.workspace_missing_directory_restore_operation(
        agent.agent_id, result.operation_id
    )
    assert operation.intent_recorded is (when == "post")
    if when == "pre":
        assert not (binding.workspace.root / "tree").exists()
    else:
        assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"\0\xff"
    audit = list(handle.runtime.tools.audit)[-1]
    assert audit.name == MISSING_DIRECTORY_RESTORE_STEP_TOOLS["execute"] and not audit.ok
    assert audit.run_id == "local" and audit.code == "workspace_missing_directory_restore_context"
    assert not service.missing_directory_restores._pending


@pytest.mark.parametrize("fault", ["short", "result", "marker"])
async def test_short_circuit_or_forged_result_cannot_claim_success(controller, fault):
    service, agent, binding, _, planned = controller
    registry = service._handle(agent.agent_id).runtime.tools

    async def pre(definition, _context):
        if definition.name == MISSING_DIRECTORY_RESTORE_STEP_TOOLS["execute"]:
            return ShortCircuit({"fake": "done"})

    async def post(definition, value, context):
        if definition.name == MISSING_DIRECTORY_RESTORE_STEP_TOOLS["execute"]:
            if fault == "marker":
                context._directory_restore_response = "{}"
            else:
                value = {**value, "target_fingerprint": "a" * 64}
        return value

    if fault == "short":
        registry.add_global_pre(pre)
    else:
        registry.add_global_post(post)
    started = await service.workspace_start_missing_directory_restore(
        agent.agent_id, execute_request(planned)
    )
    await drain(service)
    result = await service.workspace_missing_directory_restore(agent.agent_id, started.plan_id)
    assert result.phase == ("failed" if fault == "short" else "uncertain"), result
    assert list(registry.audit)[-1].code == "workspace_missing_directory_restore_unverified"
    if fault == "short":
        assert not (binding.workspace.root / "tree").exists()
    else:
        assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"\0\xff"


@pytest.mark.parametrize("bad", [False, 0, "", {}, SimpleNamespace(kind="execute")])
async def test_bad_falsey_private_fence_is_audited_before_any_producer(runtime, tmp_path, bad):
    service, agent, binding = await setup(runtime, tmp_path)
    handle = service._handle(agent.agent_id)
    registry = handle.runtime.tools
    try:
        result = await registry.invoke(
            MISSING_DIRECTORY_RESTORE_STEP_TOOLS["execute"],
            {"plan_id": "original-plan", "operation_id": "original-execution"},
            ToolContext(
                registry,
                topic_id=handle.topic_id,
                session_id=agent.session_id,
                agent_id=agent.agent_id,
                workspace=binding,
                origin="host",
                directory_restore=bad,
                workspace_directory_restores=service.missing_directory_restores,
            ),
        )
        assert result.is_error and result.code == "workspace_missing_directory_restore_context"
        assert list(registry.audit)[-1].code == result.code
        assert not list(binding.workspace.root.glob("._krim-tx-*"))
        assert not service.missing_directory_restores._tasks
    finally:
        await service.close()


@pytest.mark.parametrize(
    "source", [False, 0, "", {}, CheckpointModelSource("original-run", "original-call")]
)
async def test_unbound_model_or_falsey_source_never_downgrades_to_host(runtime, tmp_path, source):
    service, agent, binding = await setup(runtime, tmp_path)
    handle = service._handle(agent.agent_id)
    try:
        result = await handle.runtime.tools.invoke(
            MISSING_DIRECTORY_RESTORE_STEP_TOOLS["execute"],
            {"plan_id": "original-plan", "operation_id": "original-execution"},
            ToolContext(
                handle.runtime.tools,
                topic_id=handle.topic_id,
                session_id=agent.session_id,
                agent_id=agent.agent_id,
                workspace=binding,
                origin="host",
                directory_restore=MissingDirectoryRestoreFence(
                    "original-plan", "original-execution", "execute", "a" * 64, source
                ),
                workspace_directory_restores=service.missing_directory_restores,
            ),
        )
        assert result.is_error and result.code == "workspace_missing_directory_restore_context"
        assert list(handle.runtime.tools.audit)[-1].code == result.code
        assert not list(binding.workspace.root.glob("._krim-tx-*"))
    finally:
        await service.close()
