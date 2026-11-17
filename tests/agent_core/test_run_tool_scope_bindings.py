"""Override projections retain real producer/call authority and cannot be forged."""

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

from krim_agent.contracts import RunEvent, ServiceError, StrictModel
from krim_agent.runtime.run_tools import open_run_tools
from krim_agent.runtime.session_tools import SessionToolHost
from krim_agent.security import LOCAL
from krim_agent.tools.core import Invocation, ToolSource, ToolSpec
from krim_agent.tools.executor import ToolContext, ToolExecutor
from krim_agent.tools.pipeline import _CALL, CURRENT, Scope, _calling, _producing
from krim_agent.tools.registry import ToolRegistry
from krim_agent.tools.runtime import ToolRuntime
from krim_agent.workspace.types import ExecutionAccess


class Store:
    async def work_read(self, read):
        return {
            "session_id": "session",
            "workspace_id": "local",
            "owner_id": "local",
            "session_status": "open",
            "turn_id": "turn",
            "turn_status": "running",
            "stop_requested": False,
            "run_status": "running",
            "project_id": None,
        }


class Args(StrictModel):
    query: str


async def not_direct(arguments):
    raise ServiceError("tool_binding_changed")


@pytest.mark.parametrize("with_workspace_scope", [False, True])
async def test_override_with_real_session_host_producer_reauthorizes_target(with_workspace_scope):
    task = asyncio.current_task()
    runtime = SimpleNamespace(closed=False, tasks={"run": task}, _cancelled=set())
    host = SessionToolHost(Store())
    host.runtime = runtime
    policies, effects, hooks = [], [], []

    async def remote(call, arguments):
        actual = await host.resolve(call)
        assert actual.run_id == "run"
        assert _CALL.get().invocation == call
        if with_workspace_scope:
            assert CURRENT.get().invocation == call
        effects.append(arguments.query)
        return {"text": arguments.query}

    target = ToolSpec(
        "remote__search",
        Args,
        not_direct,
        {"type": "object"},
        "1",
        "a" * 64,
        source=ToolSource("mcp", "remote", "search", "b" * 64),
        external_executor=remote,
    )
    alias = replace(target, name="web_search", binding_target=target)

    async def policy(call):
        actual = await host.resolve(call)
        actual.check()
        assert _CALL.get().invocation == call
        current = CURRENT.get()
        if with_workspace_scope:
            assert current is not None and current.invocation == call
        else:
            assert current is None
        policies.append(call.name)
        return "allow"

    async def pre(call):
        actual = await host.resolve(call)
        actual.check()
        hooks.append(call.name)

    events = []

    async def emit(kind, data):
        events.append(kind)
        return RunEvent(
            run_id="run", seq=len(events), event_id=str(len(events)), kind=kind, data=data
        )

    async def approve(call, seconds):
        return True

    workspace = (
        Scope(
            frame=None,
            access=ExecutionAccess(LOCAL, "local", "session"),
            operations=None,
            check=lambda: None,
            initial_deadline=None,
        )
        if with_workspace_scope
        else None
    )
    base = ToolExecutor(ToolRegistry(()), None, pre=pre)
    tool_runtime = ToolRuntime(ToolRegistry((alias,)), policy)
    call = Invocation("run", "call", "span", 0, "web_search", '{"query":"ok"}', alias.fingerprint)
    with _producing(runtime, "run", task):
        scope = await host.by_run("run")
        tools = await open_run_tools(base, scope=scope, resolve=host.resolve, host=tool_runtime)
        try:
            spec = tools.executor.registry.get("web_search")
            call = replace(call, tool_hash=spec.fingerprint)
            result = await tools.executor.execute(
                call, ToolContext(emit, approve, None, scope=workspace)
            )
            assert result.status == "ok" and result.value == {"text": "ok"}
            assert policies == ["web_search", "remote__search"]
            assert effects == ["ok"] and hooks == ["web_search"]
            assert CURRENT.get() is None and _CALL.get() is None
            # Host preparation authority is insufficient to fake a target call.
            with pytest.raises(ServiceError, match="permission_denied"):
                await host.resolve(replace(call, name=target.name, tool_hash=target.fingerprint))
            with _calling(call):
                await host.resolve(call)
                with pytest.raises(ServiceError, match="permission_denied"):
                    await host.resolve(
                        replace(call, name=target.name, tool_hash=target.fingerprint)
                    )
            bad = replace(call, tool_hash="0" * 64)
            rejected = await tools.executor.execute(bad, ToolContext(emit, approve, None))
            assert rejected.code == "tool_binding_changed"
            assert effects == ["ok"]
        finally:
            await tools.close()
    assert CURRENT.get() is None and _CALL.get() is None
    json.dumps(events)
