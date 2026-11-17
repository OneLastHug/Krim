"""Real SDK/Compactor/ModelStep with deterministic provider and persistence doubles."""

import asyncio
import hashlib
from dataclasses import replace

import ai
import pytest

from krim_agent.agent_sdk.context import CompactorHistory
from krim_agent.agent_sdk.host import HostExecution
from krim_agent.agent_sdk.model import HostModelProvider
from krim_agent.context.compaction import Compactor
from krim_agent.context.compaction_source import WorkspaceCompactionSource
from krim_agent.context.types import CompactionPolicy, ContextPolicy
from krim_agent.contracts import Message, ServiceError, encoded, fingerprint
from krim_agent.providers.types import ModelText, ModelToolCall
from krim_agent.runtime.contracts import ExecutionBudget
from krim_agent.runtime.model_step import ModelStep
from tests.coding_agent.sdk_fixtures import setup


class Store:
    async def work_read(self, read):
        return await read(self)

    async def work_write(self, write, known):
        return await write(self)

    async def execute(self, statement):
        return None


class Script:
    def __init__(self, *, write=False):
        self.requests = []
        self.write = write

    async def stream(self, messages, tools):
        self.requests.append((messages, tools))
        if messages[0].content.startswith("Krim compaction/1"):
            assert not tools
            yield ModelText(text="SENTINEL=42; retain the user's goal")
        elif self.write:
            self.write = False
            yield ModelToolCall(id="write-once", name="write", arguments={"value": 7})
        else:
            yield ModelText(text="finished")


async def frozen_history(conn, run_id, offset, limit, access, *, source_anchor):
    raise AssertionError("history fixture was not supplied")


class FixtureSource:
    def __init__(self, owner):
        self.owner = owner

    async def page(self, conn, run_id, offset, limit):
        return await frozen_history(conn, run_id, offset, limit, None,
                                    source_anchor=self.owner.source_anchor)

    async def save_prepared(self, conn, run_id, prepared):
        await WorkspaceCompactionSource.save_prepared(self, conn, run_id, prepared)

    async def read_prepared(self, conn, run_id):
        return await WorkspaceCompactionSource.read_prepared(self, conn, run_id)


class FixtureCompactor(Compactor):
    """Real preparation and model call; frozen history and row I/O are doubles."""
    def __init__(self, step, *, history=None, rows=None):
        self.store = Store()
        self.check = lambda: None
        self.system = "system"
        self.policy = ContextPolicy(max_bytes=10000, reserved_output_tokens=64)
        self.compaction = CompactionPolicy()
        self.step = step
        self.request_limit = 65536
        self.extension_snapshot = {"version": "extensions/2", "revision": 0, "items": []}
        self.history = history if history is not None else []
        if history is None:
            for i in range(8):
                items = [Message(role="user", content=f"SENTINEL={i} " + "x" * 600),
                         Message(role="assistant", content="y" * 600)]
                ref = {"turn_id": f"turn-{i}", "run_id": f"prior-{i}",
                       "terminal_seq": 4, "transcript_fingerprint": fingerprint(
                           [m.model_dump(mode="json") for m in items])}
                self.history.append({"messages": items, "ref": ref})
        self.anchor_refs = tuple(turn["ref"] for turn in self.history)
        self.source_anchor = fingerprint(list(self.anchor_refs))
        self.record_policy = {"context": self.policy.model_dump(mode="json")}
        self.provider_identity = {"provider": "deterministic"}
        self.access = None
        self.summary, self.covered, self.passes = "", 0, 0
        self.compaction_changes = []
        self.rows = rows if rows is not None else {}
        self.session_id = "session"
        self.source = FixtureSource(self)

    def guard(self):
        self.check()

    async def verify(self, conn):
        self.check()

    async def row(self, index):
        return self.rows.get(index)

    async def summarize(self, refs, messages):
        assert self.passes not in self.rows, "same summary pass must never be reissued"
        assert self.compaction.max_passes is None or self.passes < self.compaction.max_passes
        step_index = self.step.index
        response = await self.step.call(
            messages, [], purpose="summary", summary_bytes=self.summary_capacity(),
            request_budget=self.step.request_budget(
                self.max_group, self.remaining_tools, purpose="summary"),
            request_prepared=True,
        )
        self.rows[self.passes] = {
            "status": "complete", "model_step": step_index, "source_refs": refs,
            "output": response.text,
            "output_hash": hashlib.sha256(response.text.encode()).hexdigest(),
        }
        self.passes += 1
        return response.text


def connected(harness, script, *, max_steps=6, previous=None):
    adapter = harness.adapter()

    async def emit(kind, data):
        return await harness.store.append("run", kind, data)

    budget = ExecutionBudget(asyncio.get_running_loop().time() + 30)
    context = replace(adapter.context(), emit=emit)
    step = ModelStep(script, emit, lambda: None, max_steps=max_steps, max_output=65536,
                     deadline=None, workspace=True, execution_budget=budget)
    compactor = FixtureCompactor(
        step, history=previous.history if previous else None,
        rows=previous.rows if previous else None,
    )
    history = CompactorHistory(compactor, "run")
    provider = HostModelProvider(
        step, adapter.executor.registry, max_group=2, max_calls=4,
        request_bytes=65536, history=history,
    )

    async def receipt(value):
        return await harness.store.read_result(value.result_ref)

    adapter.host = HostExecution(context, budget, receipt, provider.step_index,
                                 provider.checkpoint, provider.restore)
    return adapter, ai.Model(id="fixture", provider=provider), compactor, budget


async def test_compaction_uses_shared_steps_and_preserves_sources_and_tail():
    harness = setup(approval=False)
    script = Script()
    adapter, model, compactor, _ = connected(harness, script)
    original = encoded([m.model_dump(mode="json")
                        for t in compactor.history for m in t["messages"]])
    result = await adapter.run(model, [ai.user_message("current task")])
    assert result.status == "completed"
    assert compactor.passes == 1 and compactor.step.index == 2
    assert len(script.requests) == 2
    task = script.requests[-1][0]
    assert task[1].content.startswith("[不可信历史摘要]")
    assert task[-1].content == "current task"
    assert "SENTINEL=6" in encoded([m.model_dump(mode="json") for m in task])
    assert original == encoded([m.model_dump(mode="json")
                               for t in compactor.history for m in t["messages"]])
    starts = [e.data for e in harness.store.events if e.kind == "model_start"]
    assert starts[-1]["context_manifest"]["compaction_changes"]
    assert "SENTINEL=42" not in encoded(starts[-1]["context_manifest"])
    assert compactor.step.output_bytes >= len("finished")


async def test_compaction_consumes_last_model_step_without_dispatch():
    harness = setup(approval=False)
    script = Script(write=True)
    adapter, model, compactor, _ = connected(harness, script, max_steps=1)
    with pytest.raises(ExceptionGroup):
        await adapter.run(model, [ai.user_message("current task")])
    assert compactor.step.index == 1 and len(script.requests) == 1
    assert harness.counts["write"] == 0


async def test_active_run_history_fails_closed_instead_of_silent_truncation():
    harness = setup(approval=False)
    script = Script()
    adapter, model, compactor, _ = connected(harness, script)
    with pytest.raises(ExceptionGroup):
        await adapter.run(model, [ai.user_message("current " + "x" * 16000)])
    assert all(not tools for _, tools in script.requests)
    assert not any(messages[0].content == "system" for messages, _ in script.requests)
    assert harness.counts == {"read": 0, "write": 0}


async def test_resume_reuses_compaction_and_preserves_model_tool_output_budgets(monkeypatch):
    harness = setup()
    script = Script(write=True)
    adapter, model, compactor, budget = connected(harness, script)
    first = await adapter.run(model, [ai.user_message("current task")])
    assert first.status == "awaiting"
    saved, _ = harness.store.saved
    state = saved["host_state"]
    assert state["step"] == 2 and state["tool_count"] == 1
    assert state["context"]["passes"] == 1
    old_output = state["output_bytes"]
    assert saved["budget_remaining"] <= budget.remaining + 1
    original_history = compactor.history

    async def frozen_history(conn, run_id, offset, limit, access, *, source_anchor):
        assert source_anchor == compactor.source_anchor and run_id == "run"
        return original_history[offset:offset + limit]

    monkeypatch.setitem(globals(), "frozen_history", frozen_history)
    await harness.store.set_awaiting("run")
    await harness.store.set_running("run")
    resumed, model, restored, fresh_budget = connected(harness, script, previous=compactor)
    # RunService supplies the old remaining execution time; context does not own it.
    fresh_budget = ExecutionBudget(
        asyncio.get_running_loop().time() + saved["budget_remaining"])
    resumed.host = replace(resumed.host, budget=fresh_budget)
    before = fresh_budget.remaining
    request = next(e for e in harness.store.events if e.kind == "approval_request")
    result = await resumed.run(model, resume=True, replies={request.data["call_id"]: True})
    assert result.status == "completed"
    assert len(script.requests) == 3 and restored.step.index == 3
    assert restored.passes == 1 and restored.covered == compactor.covered
    assert restored.step.output_bytes == old_output + len("finished")
    assert model.provider.checkpoint()["tool_count"] == 1
    assert fresh_budget.remaining <= before
    assert harness.counts["write"] == 1
    assert not restored.compaction_changes


@pytest.mark.parametrize("field,value", [
    ("binding", "wrong"), ("passes", True), ("passes", 99),
    ("covered", -1), ("summary", "x"),
])
async def test_corrupt_context_checkpoint_rejected(field, value):
    harness = setup()
    _, model, _, _ = connected(harness, Script())
    state = model.provider.checkpoint()
    state["context"][field] = value
    with pytest.raises(ServiceError, match="facts_corrupt"):
        model.provider.restore(state)


async def test_missing_compaction_wiring_cannot_resume_its_checkpoint():
    harness = setup()
    _, model, compactor, _ = connected(harness, Script())
    state = model.provider.checkpoint()
    other = HostModelProvider(compactor.step, harness.adapter().executor.registry,
                              max_group=2, max_calls=4, request_bytes=65536)
    with pytest.raises(ServiceError):
        other.restore(state)


@pytest.mark.parametrize("damage", ["missing", "unfinished", "hash", "future_step", "summary"])
async def test_corrupt_durable_summary_blocks_model_continuation(damage, monkeypatch):
    harness = setup(approval=False)
    script = Script()
    _, model, compactor, _ = connected(harness, script)
    wrapper = CompactorHistory(compactor, "run")
    await wrapper.prepare([Message(role="user", content="go")], [],
                          max_group=2, remaining_tools=4)
    saved = model.provider.checkpoint()
    original_history = compactor.history
    _, restored_model, restored, _ = connected(harness, script, previous=compactor)
    restored_model.provider.restore(saved)
    if damage == "missing":
        restored.rows.clear()
    elif damage == "unfinished":
        restored.rows[0]["status"] = "reserved"
    elif damage == "hash":
        restored.rows[0]["output_hash"] = "0" * 64
    elif damage == "future_step":
        restored.rows[0]["model_step"] = restored.step.index
    else:
        restored.summary = "different summary"

    async def frozen_history(conn, run_id, offset, limit, access, *, source_anchor):
        return original_history[offset:offset + limit]

    monkeypatch.setitem(globals(), "frozen_history", frozen_history)
    before = len(script.requests)
    with pytest.raises(ServiceError, match="facts_corrupt"):
        async for _ in restored_model.provider.stream(restored_model, [ai.user_message("go")]):
            pass
    assert len(script.requests) == before


@pytest.mark.parametrize("damage", [None, "missing", "policy", "source", "provider", "status"])
async def test_zero_pass_restore_verifies_reused_summary_without_model_call(damage, monkeypatch):
    from types import SimpleNamespace

    from sqlalchemy import insert
    from sqlalchemy.ext.asyncio import create_async_engine

    from krim_agent.context.compaction import PROMPT
    from krim_agent.storage.models import compactions, runs

    harness = setup()
    script = Script()
    _, model, c, _ = connected(harness, script)
    c.summary, c.covered = "old cached summary", 2
    c.frame = SimpleNamespace(session_id="session")
    original_history = c.history
    state = model.provider.checkpoint()
    model.provider.restore(state)
    assert c.history is None and c.passes == 0
    engine = create_async_engine("sqlite+aiosqlite://")
    try:
        async with engine.begin() as conn:
            await conn.run_sync(lambda sync: runs.create(sync))
            await conn.run_sync(lambda sync: compactions.create(sync))
            await conn.execute(insert(runs).values(
                id="prior", request_key="prior", request_hash="hash", request={}, target={},
                status="failed" if damage == "status" else "completed", code=None,
                trace_id="trace", root_span_id="root", created_at=0, stop_requested=False,
            ))
            if damage != "missing":
                await conn.execute(insert(compactions).values(
                    run_id="prior", pass_index=0, session_id="session", status="complete",
                    source_refs=[] if damage == "source" else list(c.anchor_refs[:2]),
                    source_hash=fingerprint(list(c.anchor_refs[:2])),
                    policy={} if damage == "policy" else c.record_policy,
                    provider_identity={} if damage == "provider" else c.provider_identity,
                    prompt_hash=fingerprint(PROMPT), model_step=0, output=c.summary,
                    output_hash=hashlib.sha256(c.summary.encode()).hexdigest(), created_at=0,
                ))

        class SQLStore:
            async def work_read(self, read):
                async with engine.connect() as conn:
                    return await read(conn)

        c.store = SQLStore()

        async def frozen_history(conn, run_id, offset, limit, access, *, source_anchor):
            return original_history[offset:offset + limit]

        monkeypatch.setitem(globals(), "frozen_history", frozen_history)
        if damage:
            with pytest.raises(ServiceError, match="facts_corrupt"):
                await model.provider._history._restore_history()
        else:
            await model.provider._history._restore_history()
            assert c.covered == 2 and c.summary == "old cached summary"
            assert c.history == original_history
        assert not script.requests and c.step.index == 0
    finally:
        await engine.dispose()


async def test_compaction_rejects_full_session_history_with_system_prompt():
    harness = setup()
    _, _, compactor, _ = connected(harness, Script())
    with pytest.raises(ServiceError, match="facts_corrupt"):
        await CompactorHistory(compactor, "run").prepare(
            [Message(role="system", content="already assembled"),
             Message(role="user", content="go")], [], max_group=2, remaining_tools=4,
        )
    assert compactor.step.index == 0


async def test_real_compactor_ledger_survives_sdk_approval_recovery(monkeypatch):
    from types import SimpleNamespace

    from sqlalchemy import insert, select
    from sqlalchemy.ext.asyncio import create_async_engine

    from krim_agent.storage.models import compactions, runs, work_turns

    harness = setup()
    script = Script(write=True)
    adapter, model, c, _ = connected(harness, script)
    original_history = c.history
    engine = create_async_engine("sqlite+aiosqlite://")

    class SQLStore:
        async def work_read(self, read):
            async with engine.connect() as conn:
                return await read(conn)

        async def work_write(self, write, known):
            async with engine.begin() as conn:
                return await write(conn)

    def durable(compactor):
        compactor.store = SQLStore()
        compactor.frame = SimpleNamespace(session_id="session")
        compactor.row = Compactor.row.__get__(compactor)
        compactor.summarize = Compactor.summarize.__get__(compactor)

    try:
        async with engine.begin() as conn:
            for table in (runs, compactions, work_turns):
                await conn.run_sync(lambda sync, table=table: table.create(sync))
            await conn.execute(insert(runs).values(
                id="run", request_key="current", request_hash="hash", request={}, target={},
                status="running", code=None, trace_id="trace", root_span_id="root",
                created_at=0, stop_requested=False,
            ))
        durable(c)
        first = await adapter.run(model, [ai.user_message("current task")])
        assert first.status == "awaiting"
        async with engine.connect() as conn:
            rows = (await conn.execute(select(compactions))).mappings().all()
        assert len(rows) == 1 and rows[0]["status"] == "complete"
        assert rows[0]["model_step"] == 0
        assert rows[0]["output"] == c.summary

        async def frozen_history(conn, run_id, offset, limit, access, *, source_anchor):
            return original_history[offset:offset + limit]

        monkeypatch.setitem(globals(), "frozen_history", frozen_history)
        await harness.store.set_awaiting("run")
        await harness.store.set_running("run")
        resumed, next_model, restored, _ = connected(harness, script, previous=c)
        durable(restored)
        request = next(e for e in harness.store.events if e.kind == "approval_request")
        result = await resumed.run(next_model, resume=True,
                                   replies={request.data["call_id"]: True})
        assert result.status == "completed" and restored.step.index == 3
        assert len(script.requests) == 3 and harness.counts["write"] == 1
        async with engine.connect() as conn:
            after = (await conn.execute(select(compactions))).mappings().all()
        assert after == rows
    finally:
        await engine.dispose()


async def test_compactor_cannot_use_separate_model_budget():
    harness = setup()
    _, _, compactor, _ = connected(harness, Script())
    other = ModelStep(Script(), compactor.step.emit, lambda: None, max_steps=6,
                      max_output=65536, deadline=None, workspace=True)
    with pytest.raises(ValueError, match="share"):
        HostModelProvider(other, harness.adapter().executor.registry, max_group=2,
                          max_calls=4, request_bytes=65536,
                          history=CompactorHistory(compactor, "run"))
