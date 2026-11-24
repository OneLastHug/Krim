from __future__ import annotations

from types import SimpleNamespace

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.subagents.workspace_tools import (
    ApprovedIntegrationPort,
    WorkspaceDiff,
    WorkspaceIntegrate,
    bundle,
)
from krim_agent.tools.core import Invocation
from krim_agent.tools.pipeline import CURRENT


def invocation() -> Invocation:
    return Invocation(
        run_id="root",
        call_id="merge",
        span_id="span",
        step_index=1,
        name="workspace_integrate",
        arguments_json="{}",
        tool_hash="a" * 64,
    )


async def test_integration_tool_requires_write_capability_and_confirmation() -> None:
    async def resolve(call):
        return SimpleNamespace(run_id=call.run_id, project_id="project", check=lambda: None)

    tools = bundle(SimpleNamespace(), resolve)
    spec = next(item for item in tools.specs if item.name == "workspace_integrate")
    assert spec.risk == "write" and spec.capability == "session_write"
    assert spec.concurrency == "exclusive"
    assert await tools.policy(invocation()) == "ask"
    value = WorkspaceIntegrate(task_id="task", expected_patch_fingerprint="a" * 64)
    assert value.task_id == "task"


async def test_approved_port_rejects_another_project_before_any_mutation() -> None:
    caller = SimpleNamespace(scope=SimpleNamespace(actor="owner"),
                             tree={"project_id": "00000000-0000-0000-0000-000000000001"})
    port = ApprovedIntegrationPort(SimpleNamespace(), caller, invocation(), lambda: None)
    with pytest.raises(ServiceError) as error:
        await port.write("00000000-0000-0000-0000-000000000002", "file.py", "value")
    assert error.value.code == "permission_denied"
    assert CURRENT.get() is None


async def test_workspace_tool_denies_unbound_session() -> None:
    async def resolve(call):
        return SimpleNamespace(run_id=call.run_id, project_id=None, check=lambda: None)

    tools = bundle(SimpleNamespace(), resolve)
    assert await tools.policy(invocation()) == "deny"


@pytest.mark.parametrize(
    "code", ["not_found", "storage_unavailable", "permission_denied", "cancelled"]
)
async def test_missing_workspace_patch_is_recoverable_without_hiding_other_errors(code) -> None:
    checks = []
    caller = SimpleNamespace(scope=SimpleNamespace(check=lambda: checks.append(True)))

    async def resolve(call):
        return SimpleNamespace(run_id=call.run_id, project_id="project", check=lambda: None)

    async def get_caller(call):
        return caller

    async def diff(selected, task_id):
        assert selected is caller and task_id == "read-only-task"
        raise ServiceError(code)

    tools = bundle(SimpleNamespace(_caller=get_caller, workspace_diff=diff), resolve)
    spec = next(item for item in tools.specs if item.name == "workspace_diff")
    from dataclasses import replace

    call = replace(invocation(), name="workspace_diff")
    if code == "not_found":
        result = await spec.scoped_executor(call, WorkspaceDiff(task_id="read-only-task"), None)
        assert result["ok"] is False and result["code"] == "not_found"
        assert "shared read-only" in result["action"]
        assert checks == [True]
    else:
        with pytest.raises(ServiceError) as error:
            await spec.scoped_executor(call, WorkspaceDiff(task_id="read-only-task"), None)
        assert error.value.code == code
        assert checks == []
