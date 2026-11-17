"""Shared session tools work without granting a project execution scope."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from krim_agent.contracts import StrictModel, encoded
from krim_agent.tools.core import Invocation, ToolSource, ToolSpec
from krim_agent.tools.executor import ToolContext, ToolExecutor
from krim_agent.tools.registry import ToolRegistry


class Empty(StrictModel):
    pass


async def unavailable(_value: BaseModel):
    raise AssertionError("unscoped fallback must not execute")


async def invoke(_invocation, _value, _context):
    return {"steps": []}


def spec(name="plan_read", capability="session_read", risk="read"):
    return ToolSpec(
        name, Empty, unavailable, {"type": "object"}, "session/1", "a" * 64,
        risk=risk, capability=capability, scoped_executor=invoke,
    )


def test_session_scope_definition_is_explicit():
    assert spec().descriptor()["policy_version"] == "session/1"
    with pytest.raises(ValueError):
        spec(name="invented", capability="session_read")
    with pytest.raises(ValueError):
        spec(name="memory_write", capability="session_write", risk="read")


def test_external_source_cannot_impersonate_session_tool():
    with pytest.raises(ValueError):
        ToolSpec(
            "plan_read", Empty, unavailable, {"type": "object"}, "external/1", "a" * 64,
            source=ToolSource("mcp", "server", "plan_read", "b" * 64),
            external_executor=invoke,
        )


def test_legacy_workspace_plan_keeps_policy_identity():
    old = spec("plan_update", "plan", "write")
    assert old.descriptor()["policy_version"] == "workspace/1"
    current = spec("plan_update", "session_state", "write")
    assert current.descriptor()["policy_version"] == "session/1"


@pytest.mark.asyncio
async def test_session_tool_executes_without_workspace_scope():
    tool = spec()
    events = []

    async def emit(kind, data):
        events.append((kind, data))

    async def approve(_call, _timeout):
        raise AssertionError("read should not ask")

    async def allow(_call):
        return "allow"

    call = Invocation("run", "call", "span", 0, tool.name, encoded({}), tool.fingerprint)
    result = await ToolExecutor(ToolRegistry((tool,)), allow).execute(
        call, ToolContext(emit, approve, 1),
    )
    assert result.status == "ok"
    assert result.value == {"steps": []}
    assert [kind for kind, _ in events] == ["tool_start", "tool_intent", "tool_end"]


@pytest.mark.asyncio
async def test_session_tool_does_not_bypass_policy():
    tool = spec()

    async def emit(_kind, _data):
        pass

    async def approve(_call, _timeout):
        return False

    call = Invocation("run", "call", "span", 0, tool.name, encoded({}), tool.fingerprint)
    result = await ToolExecutor(ToolRegistry((tool,)), None).execute(
        call, ToolContext(emit, approve, 1),
    )
    assert result.status == "denied"
    assert result.executor_started is False
