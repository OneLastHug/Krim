"""Target-specific preparation/completion around the real SDK dispatcher.

OS execution and evidence stores are doubles here; these tests prove the host
phase boundaries, not the historical privileged execution fixture suite.
"""

import asyncio
import time
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from krim_agent.config import Settings
from krim_agent.context.types import ContextPolicy, ContextSnapshot, SessionContext
from krim_agent.contracts import Message, ServiceError, fingerprint
from krim_agent.runtime import engine
from krim_agent.runtime.contracts import ExecutionBudget
from krim_agent.sandbox import tasks
from tests.coding_agent.sdk_fixtures import setup
from tests.coding_agent.test_sdk_loop_migration import MigrationProvider


def snapshot(store, *, context=None, target=None):
    async def read(_):
        return SimpleNamespace(
            run=SimpleNamespace(status=store.status, stop_requested=False, created_at=time.time()),
            request=SimpleNamespace(text="current request"), session_context=context,
            target=target or {"provider": {"name": "local-fixture"}}, events=store.events,
        )
    store.snapshot = read


@pytest.mark.parametrize("completion", [None, "passed", "failed", "changed-contract"])
async def test_sandbox_scope_and_optional_completion_stay_outside_sdk(monkeypatch, completion):
    harness = setup(approval=False)
    policy = ContextPolicy()
    context = SessionContext(
        workspace_id="workspace", session_id="session", turn_id="turn",
        context=ContextSnapshot(
            messages=(Message(role="user", content="current request"),), sources=(),
            policy=policy, tool_fingerprint="fixture", builder_fingerprint="fixture",
            input_bytes=0, estimated_tokens=0, source_fingerprint="fixture",
        ),
    )
    contract = tasks.SandboxCompletion().model_dump(mode="json")
    digest = fingerprint(contract)
    target = {"context": {"sandbox_completion_fingerprint": digest}} if completion else {}
    snapshot(harness.store, context=context, target=target)
    phases = []
    producer = asyncio.current_task()

    class Connection:
        async def execute(self, statement):
            row = {"fingerprint": digest, "completion": contract}
            if completion == "changed-contract":
                row["fingerprint"] = "different"
            return SimpleNamespace(mappings=lambda: SimpleNamespace(first=lambda: row))

    @asynccontextmanager
    async def connect():
        yield Connection()

    store = SimpleNamespace(lock=asyncio.Lock(), engine=SimpleNamespace(connect=connect))

    class Sandbox:
        service = SimpleNamespace(store=store)

        async def scope(self, run_id, check, budget):
            phases.append("scope")
            return SimpleNamespace(check=check, deadline=budget.deadline)

    async def verify(service, run_id, session_id, declared, **kwargs):
        assert asyncio.current_task() is producer
        assert declared.model_dump(mode="json") == contract
        phases.append("completion")
        return {"state": completion}

    monkeypatch.setattr(tasks, "verify_completion", verify)
    loop = engine.AgentLoop(
        harness.store, MigrationProvider(), harness.adapter().executor, Settings(),
        sandbox=Sandbox(),
    )
    budget = ExecutionBudget(asyncio.get_running_loop().time() + 10)
    code = ("completion_unverified" if completion == "failed" else
            "target_binding_changed" if completion == "changed-contract" else None)
    if code:
        with pytest.raises(ServiceError, match=code):
            await loop.run("run", harness.adapter().approval_request, execution_budget=budget)
    else:
        await loop.run("run", harness.adapter().approval_request, execution_budget=budget)
    assert phases == (["scope", "completion"] if completion in {"passed", "failed"}
                      else ["scope"])
