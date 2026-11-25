"""当前单轮长任务的压缩：closed current-run groups 作为真实账本segment。

覆盖：无既往turn的自动压缩、审批拒绝后的恢复压缩（失败status保全）、
pending组拒绝且不消费手动压缩请求、checkpoint恢复不重复摘要且拒绝篡改来源、
RunGroupSource 直接从真实 SQLite run_events 推导。
"""

import asyncio
import time
from dataclasses import replace

import ai
import pytest
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import create_async_engine

from krim_agent.agent_sdk.context import CompactorHistory
from krim_agent.agent_sdk.host import HostExecution
from krim_agent.agent_sdk.model import HostModelProvider
from krim_agent.context.compaction import Compactor
from krim_agent.context.compaction_source import RunGroupSource, WorkspaceCompactionSource
from krim_agent.context.counter import DEFAULT_COUNTER, counter_for_model
from krim_agent.context.types import CompactionPolicy, ContextPolicy
from krim_agent.contracts import (
    Message,
    ServiceError,
    encoded,
    fingerprint,
    parse_json,
)
from krim_agent.providers.types import ModelText, ModelToolCall, ModelUsage
from krim_agent.runtime.contracts import ExecutionBudget
from krim_agent.runtime.model_step import ModelStep
from krim_agent.storage.models import compactions, run_events, runs, work_turns
from tests.coding_agent.sdk_fixtures import setup

TURN_ID = "turn-current"
RUN_ID = "run"


class Script:
    """Deterministic provider: N tool groups, then final text; summaries answered."""

    def __init__(self, groups=4, write_at=None, summary="SUMMARY-1"):
        self.requests = []
        self.summary_requests = []
        self.groups = groups
        self.write_at = write_at
        self.summary_text = summary
        self.steps = 0

    async def stream(self, messages, tools):
        self.requests.append(list(messages))
        if messages[0].content.startswith("Krim compaction/1"):
            self.summary_requests.append(list(messages))
            yield ModelText(text=self.summary_text)
            return
        if self.steps >= self.groups:
            yield ModelText(text="done")
            return
        self.steps += 1
        name = "write" if self.write_at == self.steps else "read"
        yield ModelToolCall(
            id=f"call-{self.steps}", name=name, arguments={"value": self.steps}
        )


class FixtureSource:
    """Frozen anchor rows for the compactor; prepared context lands in work_turns."""

    def __init__(self, rows):
        self.rows = rows

    async def page(self, conn, run_id, offset, limit):
        return self.rows[offset : offset + limit]

    async def save_prepared(self, conn, run_id, prepared):
        await WorkspaceCompactionSource.save_prepared(self, conn, run_id, prepared)

    async def read_prepared(self, conn, run_id):
        return await WorkspaceCompactionSource.read_prepared(self, conn, run_id)


class FixtureCompactor(Compactor):
    """Real prepare/summarize/row/save paths over SQLite; anchors are fixture rows."""

    def __init__(self, step, *, store, anchors=()):
        self.store = store
        self.check = lambda: None
        self.system = "system"
        self.policy = ContextPolicy(compaction_trigger_tokens=100)
        self.compaction = CompactionPolicy()
        self.counts_tokens = self.policy.counts_tokens()
        self._counter = counter_for_model(None) if self.counts_tokens else DEFAULT_COUNTER
        self.step = step
        self.request_limit = 65536
        self.extension_snapshot = {"version": "extensions/2", "revision": 0, "items": []}
        self.history = list(anchors)
        self.anchor_refs = tuple(row["ref"] for row in self.history)
        self.source_anchor = fingerprint(list(self.anchor_refs))
        self.record_policy = {"context": self.policy.model_dump(mode="json")}
        self.provider_identity = {"provider": "deterministic"}
        self.summary, self.covered, self.passes = "", 0, 0
        self.compaction_changes = []
        self.session_id = "session"
        self.source = FixtureSource(list(self.history))

    def guard(self):
        self.check()

    async def verify(self, conn):
        self.check()


class SQLStore:
    def __init__(self, engine):
        self.engine = engine

    async def work_read(self, read):
        async with self.engine.connect() as conn:
            return await read(conn)

    async def work_write(self, write, known):
        async with self.engine.begin() as conn:
            return await write(conn)


async def make_store():
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        for table in (runs, compactions, work_turns):
            await conn.run_sync(lambda sync, table=table: table.create(sync))
        await conn.execute(insert(runs).values(
            id=RUN_ID, request_key="ac-turn." + TURN_ID, request_hash="hash",
            request={}, target={}, status="running", code=None,
            trace_id="trace", root_span_id="root", created_at=0, stop_requested=False,
        ))
    return engine, SQLStore(engine)


def anchor_rows(count=3):
    rows = []
    for i in range(count):
        items = [Message(role="user", content=f"PAST={i} " + "x" * 40),
                 Message(role="assistant", content="past answer")]
        rows.append({
            "ref": {"turn_id": f"turn-{i}", "run_id": f"prior-{i}", "terminal_seq": 2,
                    "transcript_fingerprint": fingerprint(
                        [m.model_dump(mode="json") for m in items])},
            "messages": items,
        })
    return rows


async def allow(conn, run_id):
    return None


def harness_events(harness):
    async def read(conn, run_id, after_seq):
        return [event for event in harness.store.events if event.seq > after_seq]

    return read


async def connected(harness, script, store, *, anchors=()):
    adapter = harness.adapter()

    async def emit(kind, data):
        return await harness.store.append(RUN_ID, kind, data)

    budget = ExecutionBudget(asyncio.get_running_loop().time() + 120)
    context = replace(adapter.context(), emit=emit)
    step = ModelStep(script, emit, lambda: None, max_steps=24, max_output=65536,
                     deadline=None, workspace=True, execution_budget=budget)
    compactor = FixtureCompactor(step, store=store, anchors=anchors)
    source = RunGroupSource(TURN_ID, allow, events=harness_events(harness))
    history = CompactorHistory(compactor, RUN_ID, run_source=source)
    provider = HostModelProvider(
        step, adapter.executor.registry, max_group=2, max_calls=24,
        request_bytes=65536, history=history,
    )

    async def receipt(value):
        return await harness.store.read_result(value.result_ref)

    adapter.host = HostExecution(context, budget, receipt, provider.step_index,
                                 provider.checkpoint, provider.restore)
    return adapter, ai.Model(id="fixture", provider=provider), compactor, history, provider


def group_conversation(total=4):
    conversation = [Message(role="user", content="current task")]
    for i in range(1, total + 1):
        conversation.append(Message(role="assistant", content="", tool_calls=[
            {"kind": "tool_call", "id": f"call-{i}", "name": "read",
             "arguments": {"value": i}}]))
        conversation.append(
            Message(role="tool", call_id=f"call-{i}", content=encoded({"value": i})))
    # The run's final text response is its own closed group in the events.
    conversation.append(Message(role="assistant", content="done"))
    return conversation


async def ledger_rows(engine):
    async with engine.connect() as conn:
        return (await conn.execute(select(compactions))).mappings().all()


async def test_summary_usage_calibration_compares_both_projections_at_same_scale():
    from krim_agent.context.counter import UsageCalibratedCounter
    from krim_agent.usage import TokenUsage

    class CalibratingScript(Script):
        async def stream(self, messages, tools):
            async for item in super().stream(messages, tools):
                yield item
            if messages[0].content.startswith("Krim compaction/1"):
                yield ModelUsage(
                    usage=TokenUsage(input_tokens=100000, output_tokens=1, total_tokens=100001),
                    final=True)

    engine, store = await make_store()
    harness = setup(approval=False)
    script = CalibratingScript(groups=0, summary="short summary")
    _, _, compactor, history, _ = await connected(harness, script, store, anchors=anchor_rows(3))
    compactor.policy = ContextPolicy()
    compactor.record_policy = {"context": compactor.policy.model_dump(mode="json")}
    compactor._counter = compactor.step.token_counter
    assert isinstance(compactor._counter, UsageCalibratedCounter)
    tail = "Keep this current goal. " + "tail " * 1600

    class Boundary:
        async def pending(self):
            return "manual-request"

        async def prepare(self):
            return []

        async def complete(self, token, receipt=None):
            assert token == "manual-request"

    history.boundary = Boundary()
    messages = await history.prepare(
        [Message(role="user", content=tail)], [], max_group=2, remaining_tools=4)
    assert messages[-1].content == tail
    assert compactor._counter.ratio > 1
    assert compactor.passes == 1
    change = compactor.compaction_changes[0]
    assert change["after_input_tokens"] < change["before_input_tokens"]
    await engine.dispose()


async def test_current_run_identity_is_bound_before_first_source_verification():
    class VerifiedSource(FixtureSource):
        domain = "verified-test-source"

        def __init__(self):
            super().__init__([])
            self.session_id = "session"
            self.source_refs = ()
            self.source_fingerprint = fingerprint([])
            self.policy_binding = {}
            self.context_policy = ContextPolicy()
            self.compaction_policy = CompactionPolicy()
            self.extensions = {"version": "extensions/2", "revision": 0, "items": []}
            self.verified = []

        async def verify(self, conn, run_id):
            assert run_id == RUN_ID
            self.verified.append(run_id)

    engine, store = await make_store()

    async def emit(kind, data):
        raise AssertionError("An empty initial history needs no model call")

    async with engine.begin() as conn:
        await conn.run_sync(lambda sync: run_events.create(sync))
    source = VerifiedSource()
    step = ModelStep(
        Script(groups=0), emit, lambda: None, max_steps=None, max_output=None,
        deadline=None, workspace=True)
    compactor = Compactor.from_source(
        store, source, step, lambda: None, "system", 65536, {"provider": "fixture"})
    history = CompactorHistory(
        compactor, RUN_ID, run_source=RunGroupSource(TURN_ID, source.verify))
    messages = await history.prepare(
        [Message(role="user", content="original goal")], [],
        max_group=2, remaining_tools=4)
    assert messages[-1].content == "original goal"
    assert source.verified and compactor.run_id == RUN_ID
    await engine.dispose()


async def test_auto_compaction_without_prior_turns_uses_real_run_segments():
    harness = setup(approval=False)
    script = Script(groups=4)
    engine, store = await make_store()
    adapter, model, compactor, history, _ = await connected(harness, script, store)
    result = await adapter.run(model, [ai.user_message("current task")])
    assert result.status == "completed"
    assert compactor.passes == 2 and compactor.covered == 2
    assert len(script.summary_requests) == 2
    rows = await ledger_rows(engine)
    assert len(rows) == 2 and all(row["status"] == "complete" for row in rows)
    refs = rows[-1]["source_refs"]
    assert len(refs) == 2
    ends = {event.data["provider_call_id"]: event.seq for event in harness.store.events
            if event.kind == "tool_end"}
    for offset, ref in enumerate(refs, start=1):
        assert ref["turn_id"] == TURN_ID
        assert ref["run_id"] == RUN_ID
        assert ref["terminal_seq"] == ends[f"call-{offset}"]
        assert len(ref["transcript_fingerprint"]) == 64
    assert refs[0]["transcript_fingerprint"] != refs[1]["transcript_fingerprint"]
    # Each pass summarizes only its new rows; the earlier pass travels as
    # previous_summary. Both consumed the real SDK view of their groups.
    first_body = encoded([m.model_dump(mode="json") for m in script.summary_requests[0]])
    second_body = encoded([m.model_dump(mode="json") for m in script.summary_requests[-1]])
    assert "call-1" in first_body and "call-2" not in first_body
    assert "call-2" in second_body and "call-1" not in second_body
    assert "SUMMARY-1" in second_body
    # Model view: summary, then the untouched user goal, then the kept tail.
    final = script.requests[-1]
    assert final[0].content.startswith("system")
    assert final[1].content.startswith("[不可信历史摘要] SUMMARY-1")
    assert final[2].content == "current task"
    body = encoded([m.model_dump(mode="json") for m in final])
    assert "call-1" not in body and "call-2" not in body
    assert "call-3" in body and "call-4" in body
    # The frozen anchor ledger stays untouched.
    assert compactor.history == []
    await engine.dispose()


async def test_denied_tool_recovers_and_summary_keeps_failure_status():
    harness = setup()
    script = Script(groups=3, write_at=1, summary="SUMMARY-2")
    engine, store = await make_store()
    adapter, model, compactor, history, _ = await connected(harness, script, store)
    first = await adapter.run(model, [ai.user_message("current task")])
    assert first.status == "awaiting"
    request = next(e for e in harness.store.events if e.kind == "approval_request")
    await harness.store.set_awaiting(RUN_ID)
    await harness.store.set_running(RUN_ID)
    resumed, next_model, restored, _, _ = await connected(harness, script, store)
    result = await resumed.run(
        next_model, resume=True, replies={request.data["call_id"]: False})
    assert result.status == "completed"
    # The denied group was summarized: the summary request saw the real status
    # and the whole flow survived the projection/SDK-view encoding difference.
    assert len(script.summary_requests) == 1
    summary_body = encoded([m.model_dump(mode="json") for m in script.summary_requests[0]])
    assert "denied" in summary_body
    final = script.requests[-1]
    assert final[1].content.startswith("[不可信历史摘要] SUMMARY-2")
    assert final[2].content == "current task"
    body = encoded([m.model_dump(mode="json") for m in final])
    assert "call-1" not in body and "call-2" in body and "call-3" in body
    await engine.dispose()


async def test_input_correction_remains_compactable_with_full_error_diagnostics():
    class InvalidFirst(Script):
        async def stream(self, messages, tools):
            if self.steps == 0 and not messages[0].content.startswith("Krim compaction/1"):
                self.steps += 1
                self.requests.append(list(messages))
                yield ModelToolCall(id="call-1", name="read", arguments={"value": "invalid"})
                return
            async for event in super().stream(messages, tools):
                yield event

    harness = setup(approval=False)
    script = InvalidFirst(groups=4, summary="input error was corrected")
    engine, store = await make_store()
    adapter, model, compactor, _, _ = await connected(harness, script, store)
    adapter.fail_on_tool_error = True
    result = await adapter.run(model, [ai.user_message("current task")])
    assert result.status == "completed" and compactor.covered >= 1
    body = encoded([message.model_dump(mode="json")
                    for request in script.summary_requests for message in request])
    assert "invalid_input" in body and "errors" in body
    assert harness.counts["read"] == 3
    await engine.dispose()


async def test_pending_group_rejected_before_consuming_compact_request():
    harness = setup(approval=False)
    script = Script(groups=1)
    engine, store = await make_store()
    _, model, compactor, history, _ = await connected(harness, script, store)
    consumed = []

    class Boundary:
        async def pending(self):
            return "token"

        async def complete(self, token, receipt=None):
            consumed.append(token)

        async def prepare(self):
            return []

    history.boundary = Boundary()
    pending = [
        Message(role="user", content="goal"),
        Message(role="assistant", content="", tool_calls=[
            {"kind": "tool_call", "id": "call-x", "name": "read",
             "arguments": {"value": 1}}]),
    ]
    with pytest.raises(ServiceError, match="facts_corrupt"):
        await history.prepare(pending, [], max_group=2, remaining_tools=4)
    assert consumed == []
    assert compactor.passes == 0 and compactor.step.index == 0
    await engine.dispose()


async def test_checkpoint_restore_with_anchors_and_run_refs_avoids_duplicate_summary():
    harness = setup(approval=False)
    script = Script(groups=4, summary="SUMMARY-3")
    engine, store = await make_store()
    adapter, model, compactor, history, _ = await connected(
        harness, script, store, anchors=anchor_rows(3))
    result = await adapter.run(model, [ai.user_message("current task")])
    assert result.status == "completed"
    # 3 anchors + 4 closed groups: the covered prefix is all anchors plus two
    # current-run segments; restore must not reject on the anchor bound alone.
    assert compactor.passes >= 1 and compactor.covered == 5
    saved = model.provider.checkpoint()
    refs = saved["context"]["current_refs"]
    assert len(refs) == 2 and all(ref["run_id"] == RUN_ID for ref in refs)
    assert saved["context"]["current_count"] >= len(refs)
    rows_before = await ledger_rows(engine)
    assert len(rows_before) == compactor.passes

    fresh = CompactorHistory(
        FixtureCompactor(compactor.step, store=store, anchors=anchor_rows(3)),
        RUN_ID,
        run_source=RunGroupSource(TURN_ID, allow, events=harness_events(harness)),
    )
    fresh.restore(saved["context"])
    assert fresh.compactor.passes == compactor.passes
    assert fresh.compactor.covered == 5

    # Non-force prepare first: the restored prefix is reused as
    # previous_summary and the single new pass only extends into the newly
    # closed final row; no restored pass or source is re-issued.
    messages = await fresh.prepare(
        group_conversation(4), [], max_group=2, remaining_tools=4)
    assert fresh.compactor.passes == compactor.passes + 1
    assert fresh.compactor.covered == 6
    rows_after = await ledger_rows(engine)
    assert len(rows_after) == len(rows_before) + 1
    assert rows_after[-1]["source_refs"][:5] == rows_before[-1]["source_refs"]
    assert len(rows_after[-1]["source_refs"]) == 6
    extension = encoded([m.model_dump(mode="json") for m in script.summary_requests[-1]])
    assert "call-3" in extension and "SUMMARY-3" in extension and "call-1" not in extension
    assert messages[1].content.startswith("[不可信历史摘要]")
    assert messages[2].content == "current task"

    # The manual Compact path shares the force branch: with the combined bound
    # satisfied it consumes the token without any further summary pass.
    consumed = []

    class Boundary:
        async def pending(self):
            return "token"

        async def complete(self, token, receipt=None):
            consumed.append(receipt)

        async def prepare(self):
            return []

    fresh.boundary = Boundary()
    await fresh.prepare(group_conversation(4), [], max_group=2, remaining_tools=4)
    assert fresh.compactor.passes == compactor.passes + 1
    assert fresh.compactor.covered == 6
    assert len(await ledger_rows(engine)) == len(rows_before) + 1
    assert consumed and consumed[0]["covered"] == 6
    await engine.dispose()


async def test_restore_rejects_tampered_run_sources_and_foreign_refs():
    harness = setup(approval=False)
    script = Script(groups=4, summary="SUMMARY-4")
    engine, store = await make_store()
    adapter, model, compactor, history, _ = await connected(
        harness, script, store, anchors=anchor_rows(3))
    result = await adapter.run(model, [ai.user_message("current task")])
    assert result.status == "completed"
    saved = model.provider.checkpoint()
    assert saved["context"]["current_refs"]

    foreign = {**saved["context"]["current_refs"][0], "run_id": "someone-else"}
    tampered_context = {
        **saved["context"],
        "current_refs": [foreign, *saved["context"]["current_refs"][1:]],
    }
    with pytest.raises(ServiceError, match="facts_corrupt"):
        CompactorHistory(
            FixtureCompactor(compactor.step, store=store, anchors=anchor_rows(3)),
            RUN_ID,
            run_source=RunGroupSource(TURN_ID, allow, events=harness_events(harness)),
        ).restore(tampered_context)

    victim = next(event for event in harness.store.events
                  if event.kind == "tool_end"
                  and event.data.get("provider_call_id") == "call-1")
    victim.data["value"] = {"value": "tampered"}
    fresh = CompactorHistory(
        FixtureCompactor(compactor.step, store=store, anchors=anchor_rows(3)),
        RUN_ID,
        run_source=RunGroupSource(TURN_ID, allow, events=harness_events(harness)),
    )
    fresh.restore(saved["context"])
    with pytest.raises(ServiceError, match="facts_corrupt"):
        await fresh.prepare(
            group_conversation(4), [], max_group=2, remaining_tools=4)
    await engine.dispose()


def tool_end_event(seq, provider_id, host_id, *, status, step_index, value=None):
    data = {
        "provider_call_id": provider_id, "call_id": host_id, "name": "read",
        "status": status, "result_available": value is not None,
        "executor_started": False, "step_index": step_index,
        "code": "permission_denied",
        "decision": "denied" if value is None else None,
    }
    if value is not None:
        data["value"] = value
    return {
        "run_id": RUN_ID, "seq": seq, "event_id": f"event-{seq}", "kind": "tool_end",
        "data": data, "at": time.time(),
    }


async def test_run_group_source_reads_sqlite_events_and_keeps_failure_status():
    engine = create_async_engine("sqlite+aiosqlite://")
    calls = [
        {"kind": "tool_call", "id": "c1", "name": "read", "arguments": {"value": 1}},
        {"kind": "tool_call", "id": "c2", "name": "read", "arguments": {"value": 2}},
        {"kind": "tool_call", "id": "c3", "name": "read", "arguments": {"value": 3}},
    ]
    events = [
        {"run_id": RUN_ID, "seq": 0, "event_id": "event-0", "kind": "text",
         "data": {"text": "working", "step_index": 0}, "at": time.time()},
        {"run_id": RUN_ID, "seq": 1, "event_id": "event-1", "kind": "model_end",
         "data": {"step_index": 0, "tool_calls": [calls[0]]}, "at": time.time()},
        {"run_id": RUN_ID, "seq": 2, "event_id": "event-2", "kind": "tool_start",
         "data": {"provider_call_id": "c1", "call_id": "host-1", "name": "read",
                  "status": "admitted", "step_index": 0}, "at": time.time()},
        tool_end_event(3, "c1", "host-1", status="denied", step_index=0),
        {"run_id": RUN_ID, "seq": 4, "event_id": "event-4", "kind": "model_end",
         "data": {"step_index": 1, "tool_calls": [calls[1]]}, "at": time.time()},
        {"run_id": RUN_ID, "seq": 5, "event_id": "event-5", "kind": "tool_start",
         "data": {"provider_call_id": "c2", "call_id": "host-2", "name": "read",
                  "status": "admitted", "step_index": 1}, "at": time.time()},
        tool_end_event(6, "c2", "host-2", status="ok", step_index=1, value={"value": 2}),
    ]
    async with engine.begin() as conn:
        await conn.run_sync(lambda sync: run_events.create(sync))
        await conn.execute(insert(run_events).values(events))
    source = RunGroupSource(TURN_ID, allow)
    async with engine.connect() as conn:
        rows = await source.rows(conn, RUN_ID)
    assert len(rows) == 2
    first, second = rows
    assert first["ref"]["turn_id"] == TURN_ID
    assert first["ref"]["run_id"] == RUN_ID
    assert first["ref"]["terminal_seq"] == 3
    assert second["ref"]["terminal_seq"] == 6
    assert len(first["ref"]["transcript_fingerprint"]) == 64
    assert second["ref"]["transcript_fingerprint"] != first["ref"]["transcript_fingerprint"]
    # The denied result keeps its structured failure status in the projection.
    denied = first["messages"][1]
    assert denied.role == "tool" and denied.call_id == "c1"
    assert '"status":"denied"' in denied.content
    assert '"decision":"denied"' in denied.content
    ok = second["messages"][1]
    assert ok.content == encoded({"value": 2})
    # Incremental: a new incomplete group extends the scan without shifting
    # the already-derived prefix.
    async with engine.begin() as conn:
        await conn.execute(insert(run_events).values([
            {"run_id": RUN_ID, "seq": 7, "event_id": "event-7", "kind": "model_end",
             "data": {"step_index": 2, "tool_calls": [calls[2]]}, "at": time.time()},
        ]))
    async with engine.connect() as conn:
        again = await source.rows(conn, RUN_ID)
    assert len(again) == 2
    assert [row["ref"] for row in again] == [first["ref"], second["ref"]]
    await engine.dispose()


def test_tool_value_consistency_binds_shared_fields_not_status_alone():
    from krim_agent.agent_sdk.context import _tool_value_consistent as consistent

    ok = encoded({"value": 7})
    assert consistent(ok, ok)
    # Unequal plain values, empty intersections and ok-payload subsets are
    # content drift: only exact equality accepts them.
    assert not consistent(ok, encoded({"value": 8}))
    assert not consistent(ok, encoded({**parse_json(ok), "extra": 1}))
    assert not consistent(ok, "not json")
    assert not consistent("{}", encoded({"value": 7}))
    assert not consistent(encoded({}), encoded({"value": 7}))
    assert not consistent(encoded({"rows": 5}), encoded({"rows": 6}))
    # Denied: the SDK view is reduced, the ledger projection is structured;
    # the non-ok observation needs the same explicit status and code on both
    # sides, and every field present on both sides must agree exactly.
    sdk_denial = encoded({"call_id": "c1", "name": "write", "status": "denied",
                          "code": "permission_denied"})
    ledger = {"status": "denied", "code": "permission_denied", "decision": "denied",
              "executor_started": False, "result_available": False,
              "source": "persisted_tool_end"}
    assert consistent(sdk_denial, encoded(ledger))
    assert not consistent(
        sdk_denial, encoded({**ledger, "status": "failed"}))
    assert not consistent(
        sdk_denial, encoded({**ledger, "code": "ratelimited"}))
    sdk_decision = encoded({**parse_json(sdk_denial), "decision": "denied"})
    assert not consistent(
        sdk_decision, encoded({**ledger, "decision": "suppressed"}))
    assert not consistent(
        encoded({"status": "failed", "code": "boom", "value": {"a": 1}}),
        encoded({"status": "failed", "code": "boom", "value": {"a": 2}}))
    assert not consistent(
        encoded({"status": "failed", "code": "boom"}), encoded({"rows": 6}))


async def test_injected_user_envelopes_survive_segment_compaction_and_restore():
    """Subagent result/message envelopes between groups never enter a segment.

    prepare_input appends sdk user messages between closed tool groups; the
    overlay must bind groups to their real positions, keep every envelope in
    place while a span-prefix is summarized, and keep doing so after restore.
    """
    harness = setup(approval=False)
    script = Script(groups=4, summary="SUMMARY-6")
    engine, store = await make_store()
    adapter, model, compactor, history, _ = await connected(harness, script, store)
    # Populate the real event ledger without generating a summary first. The
    # lower-threshold adapter below owns pass zero for this run.
    compactor.policy = ContextPolicy()
    result = await adapter.run(model, [ai.user_message("current task")])
    assert result.status == "completed"
    assert await ledger_rows(engine) == []

    injected = Message(role="user", content="[协作信封] subagent_result: task A finished")
    conversation = group_conversation(4)
    conversation[3:3] = [injected]
    conversation.append(Message(role="assistant", content="done"))
    fresh = CompactorHistory(
        FixtureCompactor(compactor.step, store=store, anchors=()),
        RUN_ID,
        run_source=RunGroupSource(TURN_ID, allow, events=harness_events(harness)),
    )
    messages = await fresh.prepare(conversation, [], max_group=2, remaining_tools=4)
    assert fresh.compactor.covered == 3
    assert messages[0].content.startswith("system")
    assert messages[1].content.startswith("[不可信历史摘要] SUMMARY-6")
    assert messages[2].content == "current task"
    # The envelope keeps its place between the summarized prefix and the tail.
    assert messages[3].role == "user" and messages[3].content == injected.content
    body = encoded([m.model_dump(mode="json") for m in messages])
    assert "call-1" not in body and "call-2" not in body and "call-3" not in body
    assert "call-4" in body and '"done"' in body
    metadata = fresh.compactor.manifest_metadata(messages)
    assert list(metadata[1].source_refs) == fresh.compactor._covered_refs(3)
    assert metadata[2] is None and metadata[3] is None
    tool_index = next(i for i, message in enumerate(messages) if message.call_id == "call-4")
    assert metadata[tool_index].category == "tool_result"
    assert metadata[tool_index].source_refs[0]["turn_id"] == TURN_ID

    saved = fresh.checkpoint()
    again = CompactorHistory(
        FixtureCompactor(compactor.step, store=store, anchors=()),
        RUN_ID,
        run_source=RunGroupSource(TURN_ID, allow, events=harness_events(harness)),
    )
    again.restore(saved)
    messages = await again.prepare(conversation, [], max_group=2, remaining_tools=4)
    assert again.compactor.passes == fresh.compactor.passes
    assert again.compactor.covered == 3
    assert len(await ledger_rows(engine)) == 1
    assert messages[2].content == "current task"
    assert messages[3].content == injected.content
    await engine.dispose()


async def test_restore_allows_anchor_only_coverage_with_sufficient_current_tail():
    """A real parked checkpoint: all anchors covered, current_refs still empty.

    At compaction time the combined tail (3 anchors + 2 closed current rows)
    satisfied the keep bound, so covering every anchor was legal. The old
    anchor-only bound would reject this state; the recorded current_count
    decides, and the resumed run extends under real refs without re-issuing
    the anchor passes.
    """
    harness = setup()
    script = Script(groups=3, write_at=3, summary="SUMMARY-5")
    engine, store = await make_store()
    adapter, model, compactor, history, _ = await connected(
        harness, script, store, anchors=anchor_rows(3))
    result = await adapter.run(model, [ai.user_message("current task")])
    assert result.status == "awaiting"
    saved, _ = harness.store.saved
    context = saved["host_state"]["context"]
    assert context["passes"] == 3 and context["covered"] == 3
    assert context["current_refs"] == [] and context["current_count"] == 2
    rows = await ledger_rows(engine)
    assert len(rows) == 3

    fresh = CompactorHistory(
        FixtureCompactor(compactor.step, store=store, anchors=anchor_rows(3)),
        RUN_ID,
        run_source=RunGroupSource(TURN_ID, allow, events=harness_events(harness)),
    )
    fresh.restore(context)
    assert fresh.compactor.covered == 3 and fresh.compactor.passes == 3

    request = next(e for e in harness.store.events if e.kind == "approval_request")
    await harness.store.set_awaiting(RUN_ID)
    await harness.store.set_running(RUN_ID)
    resumed, next_model, restored, _, _ = await connected(
        harness, script, store, anchors=anchor_rows(3))
    result = await resumed.run(
        next_model, resume=True, replies={request.data["call_id"]: False})
    assert result.status == "completed"
    # The denial group is the next uncovered row: one extension pass under
    # real refs; the anchor passes keep their ledger rows untouched.
    assert restored.passes == 4 and restored.covered == 4
    after = await ledger_rows(engine)
    assert len(after) == len(rows) + 1
    assert after[:3] == rows
    assert [ref["turn_id"] for ref in after[-1]["source_refs"][:3]] == [
        "turn-0", "turn-1", "turn-2"]
    assert all(ref["run_id"] == RUN_ID for ref in after[-1]["source_refs"][3:])
    assert len(after[-1]["source_refs"]) == 4
    ends = {event.data["provider_call_id"]: event.seq for event in harness.store.events
            if event.kind == "tool_end"}
    assert after[-1]["source_refs"][3]["terminal_seq"] == ends["call-1"]
    extension = encoded([m.model_dump(mode="json") for m in script.summary_requests[-1]])
    assert "call-1" in extension and "SUMMARY-5" in extension
    # The denied group stays in the kept tail with its real status.
    final = script.requests[-1]
    assert "call-3" in encoded([m.model_dump(mode="json") for m in final])
    await engine.dispose()
