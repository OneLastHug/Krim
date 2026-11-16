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
from tests.coding_agent.test_sdk_context_compaction import FixtureCompactor
from tests.coding_agent.test_sdk_loop_migration import MigrationProvider


def snapshot(store, *, context=None, target=None):
    async def read(_):
        return SimpleNamespace(
            run=SimpleNamespace(status=store.status, stop_requested=False, created_at=time.time()),
            request=SimpleNamespace(text="current request"), session_context=context,
            target=target or {"provider": {"name": "local-fixture"}}, events=store.events,
        )
    store.snapshot = read


@pytest.mark.parametrize("verdict", ["passed", "failed"])
async def test_workspace_keeps_preparation_compaction_and_producer_completion(monkeypatch, verdict):
    harness = setup(approval=False)
    snapshot(harness.store)
    phases = []
    producer = asyncio.current_task()

    class Verifier:
        def __init__(self, store, artifacts):
            pass

        def assert_current(self):
            pass

        async def verify(self, run_id, frame, access, **kwargs):
            assert asyncio.current_task() is producer
            phases.append("verify")
            return SimpleNamespace(state=verdict)

    def stage(result, **kwargs):
        assert kwargs["producer"] is producer and asyncio.current_task() is producer
        phases.append("stage")

    class Work:
        artifacts = object()

        async def prepare(self, run_id):
            phases.append("prepare")
            return SimpleNamespace(conditions={"runtime": {
                "model_steps": 3, "max_tools_per_step": 2, "tool_calls": 4,
                "tool_timeout_seconds": 2, "approval_timeout_seconds": 10,
                "read_parallelism": 2,
            }})

        def scope(self, frame, check, deadline, budget):
            phases.append("scope")
            return SimpleNamespace(access=None, check=check, deadline=deadline)

    def compactor(store, frame, access, step, check, system, request_limit, provider, **kwargs):
        phases.append("compactor")
        result = FixtureCompactor(step, history=[])
        result.system, result.request_limit, result.check = system, request_limit, check
        return result

    monkeypatch.setattr(engine, "Verifier", Verifier)
    monkeypatch.setattr(engine, "stage_result", stage)
    monkeypatch.setattr(engine, "compaction", SimpleNamespace(
        Compactor=compactor, binding_token=engine.compaction.binding_token,
    ))
    loop = engine.AgentLoop(
        harness.store, MigrationProvider(), harness.adapter().executor, Settings(), work=Work(),
    )
    budget = ExecutionBudget(asyncio.get_running_loop().time() + 10)
    if verdict == "failed":
        with pytest.raises(ServiceError, match="completion_unverified"):
            await loop.run("run", harness.adapter().approval_request, execution_budget=budget)
    else:
        await loop.run("run", harness.adapter().approval_request, execution_budget=budget)
    assert phases == ["prepare", "scope", "compactor", "verify", "stage"]
    assert [event.kind for event in harness.store.events].count("model_start") == 1


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
