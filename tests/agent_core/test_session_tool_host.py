"""Model preparation and tool calls retain distinct producer-owned authority."""

import asyncio
from types import SimpleNamespace

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.runtime.session_tools import SessionToolHost
from krim_agent.tools.core import Invocation
from krim_agent.tools.pipeline import _calling, _producing


class Store:
    def __init__(self):
        self.record = {
            "session_id": "session", "workspace_id": "local", "owner_id": "local",
            "session_status": "open", "turn_id": "turn", "turn_status": "running",
            "stop_requested": False, "run_status": "running", "project_id": None,
        }

    async def work_read(self, _read):
        return dict(self.record)


def setup():
    task = asyncio.current_task()
    runtime = SimpleNamespace(closed=False, tasks={"run": task}, _cancelled=set())
    host = SessionToolHost(Store())
    host.runtime = runtime
    return host, runtime, task


@pytest.mark.asyncio
async def test_preparation_requires_the_exact_registered_live_producer():
    host, runtime, task = setup()
    with pytest.raises(ServiceError, match="permission_denied"):
        await host.by_run("run")
    with _producing(runtime, "run", task):
        scope = await host.by_run("run")
        assert (scope.run_id, scope.session_id, scope.actor.principal_id) == (
            "run", "session", "local",
        )
        scope.check()
        with pytest.raises(ServiceError, match="permission_denied"):
            await host.by_run("another-run")
        runtime._cancelled.add("run")
        with pytest.raises(ServiceError, match="permission_denied"):
            scope.check()
        runtime._cancelled.clear()
    with pytest.raises(ServiceError, match="permission_denied"):
        scope.check()


@pytest.mark.asyncio
async def test_tool_resolver_cannot_be_replaced_by_a_fabricated_invocation():
    host, runtime, task = setup()
    call = Invocation("run", "call", "span", 0, "plan_read", "{}", "a" * 64)
    forged = Invocation("run", "other", "span", 0, "plan_read", "{}", "a" * 64)
    with _producing(runtime, "run", task):
        # Model preparation has producer authority, not tool-call authority.
        await host.by_run("run")
        with pytest.raises(ServiceError, match="permission_denied"):
            await host.resolve(call)
        with _calling(call):
            scope = await host.resolve(call)
            scope.check()
            with pytest.raises(ServiceError, match="permission_denied"):
                await host.resolve(forged)
        with pytest.raises(ServiceError, match="permission_denied"):
            scope.check()


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value,code", [
    ("stop_requested", True, "cancelled"),
    ("run_status", "awaiting", "cancelled"),
    ("session_status", "closed", "session_closed"),
    ("owner_id", "someone-else", "unauthorized"),
])
async def test_preparation_rechecks_current_session_and_run(field, value, code):
    host, runtime, task = setup()
    host.store.record[field] = value
    with _producing(runtime, "run", task):
        with pytest.raises(ServiceError, match=code):
            await host.by_run("run")
