"""Read-only preview lookup failures stay actionable without weakening access."""

import json
from types import SimpleNamespace

import ai
import pytest
from ai import events, messages

from krim_agent.agent_sdk import SDKAdapter
from krim_agent.contracts import ServiceError
from krim_agent.development.service import (
    DevelopmentService,
    PreviewInput,
    PreviewStatusInput,
)
from krim_agent.tools.core import ToolSpec
from krim_agent.tools.executor import ToolExecutor
from krim_agent.tools.registry import ToolRegistry
from tests.coding_agent.sdk_fixtures import Scripted, done, model, setup


def service(code, *, cross_project=False, missing=False):
    checks = []
    environment = SimpleNamespace(
        owner_id="owner", project_id="project", environment_id="env", generation=1,
    )
    scope = SimpleNamespace(
        actor=SimpleNamespace(principal_id="owner"), environment=environment,
        check=lambda: checks.append(True),
    )
    visible = SimpleNamespace(**vars(environment))
    if cross_project:
        visible.project_id = "other-project"

    async def get(preview_id):
        if missing:
            raise ServiceError(code)
        return SimpleNamespace(handle=SimpleNamespace(environment=visible))

    async def inspect(*args, **kwargs):
        raise ServiceError(code)

    selected = SimpleNamespace(
        current=lambda call: scope,
        require_previews=lambda: SimpleNamespace(repository=SimpleNamespace(get=get)),
        inspect_preview=inspect,
        stop_preview=inspect,
    )
    return selected, checks


@pytest.mark.parametrize("code", [
    "not_found", "workspace_busy", "preview_inactive", "preview_not_ready",
    "preview_route_unavailable", "preview_route_invalidated", "preview_route_capacity",
])
async def test_unavailable_preview_status_returns_actionable_result(code):
    selected, checks = service(code, missing=code == "not_found")
    result = await DevelopmentService.execute(
        selected, None, PreviewStatusInput(preview_id="preview-old"), None)
    assert result["ok"] is False and result["code"] == code
    assert result["preview_id"] == "preview-old"
    assert "process_status/read_output" in result["next_action"]
    assert "ticket" not in result and "state" not in result
    assert checks == [True]


@pytest.mark.parametrize("code", [
    "permission_denied", "storage_unavailable", "tool_binding_changed",
    "preview_binding_drift", "facts_corrupt", "cancelled",
])
async def test_preview_authority_and_storage_errors_are_not_relabelled(code):
    selected, checks = service(code)
    with pytest.raises(ServiceError) as raised:
        await DevelopmentService.execute(
            selected, None, PreviewStatusInput(preview_id="preview-old"), None)
    assert raised.value.code == code and checks == []


async def test_inactive_preview_reaches_model_without_ending_fail_on_tool_error_run():
    selected, _ = service("workspace_busy")

    async def execute(parameters):
        return await DevelopmentService.execute(selected, None, parameters, None)

    async def allow(call):
        return "allow"

    @ai.tool
    async def preview_status(preview_id: str, wait_seconds: float = 0):
        return None

    spec = ToolSpec(
        "preview_status", PreviewStatusInput, execute, {"type": "object"}, "1", "a" * 64)
    executor = ToolExecutor(ToolRegistry((spec,)), allow)
    harness = setup(approval=False)
    adapter = SDKAdapter(
        store=harness.store, operations=harness.operations, executor=executor,
        tools=[preview_status], environment=harness.environment, ownership=harness.ownership,
        read_result=harness.store.read_result, approval_request=harness.adapter().approval_request,
        fail_on_tool_error=True)
    raw = json.dumps({"preview_id": "preview-old"})
    call = messages.ToolCallPart(
        tool_call_id="old-preview", tool_name="preview_status", tool_args=raw)
    provider = Scripted([[
        events.StreamStart(),
        events.ToolStart(tool_call_id="old-preview", tool_name="preview_status"),
        events.ToolDelta(tool_call_id="old-preview", chunk=raw),
        events.ToolEnd(tool_call_id="old-preview", tool_call=call),
        events.StreamEnd(finish_reason="tool_call"),
    ], done()])
    result = await adapter.run(model(provider), [ai.user_message("Continue the project")])
    assert result.status == "completed" and len(provider._seen) == 2
    observed = next(part.result for message in provider._seen[1]
                    for part in message.tool_results if part.tool_call_id == "old-preview")
    assert observed["value"]["ok"] is False
    assert observed["value"]["code"] == "workspace_busy"
    assert "process_status" in observed["value"]["next_action"]


async def test_cross_project_preview_and_stop_failures_keep_original_fences():
    selected, checks = service("workspace_busy", cross_project=True)
    with pytest.raises(ServiceError, match="permission_denied"):
        await DevelopmentService.execute(
            selected, None, PreviewStatusInput(preview_id="preview-old"), None)
    assert checks == []
    selected, checks = service("workspace_busy")
    with pytest.raises(ServiceError, match="workspace_busy"):
        await DevelopmentService.execute(
            selected, None, PreviewInput(preview_id="preview-old"), None)
    assert checks == []
