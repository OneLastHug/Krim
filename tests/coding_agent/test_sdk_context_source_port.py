"""Real Compactor/SQLite/SDK using source ports; no model network or OS worker."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import ai
import pytest
from sqlalchemy import JSON, Column, MetaData, String, Table, insert, select, update
from sqlalchemy.ext.asyncio import create_async_engine

from krim_agent.agent_sdk.context import CompactorHistory
from krim_agent.agent_sdk.host import HostExecution
from krim_agent.agent_sdk.model import HostModelProvider
from krim_agent.context.compaction import Compactor
from krim_agent.context.compaction_source import WorkspaceCompactionSource
from krim_agent.context.types import CompactionPolicy, ContextPolicy
from krim_agent.contracts import Message, ServiceError, encoded, fingerprint
from krim_agent.runtime.contracts import ExecutionBudget
from krim_agent.runtime.model_step import ModelStep
from krim_agent.security import LOCAL
from krim_agent.storage.models import compactions, runs, work_turns
from krim_agent.workspace.types import ExecutionAccess, WorkFrame
from tests.coding_agent.sdk_fixtures import setup
from tests.coding_agent.test_sdk_context_compaction import Script

prepared_rows = Table("fixture_prepared", MetaData(), Column("run_id", String, primary_key=True),
                      Column("value", JSON))


class SQLStore:
    def __init__(self, engine):
        self.engine = engine
        self.lose_return = False

    async def work_read(self, read):
        async with self.engine.connect() as conn:
            return await read(conn)

    async def work_write(self, write, known):
        async with self.engine.begin() as conn:
            result = await write(conn)
        if self.lose_return:
            self.lose_return = False
            return await known()
        return result


class SessionSource:
    """Only host storage/authorization are fixtures; Compactor is unmodified."""
    domain = "session-history/3"
    session_id = "session"
    context_policy = ContextPolicy(max_bytes=10000, reserved_output_tokens=64)
    compaction_policy = CompactionPolicy()
    extensions = {"version": "extensions/2", "revision": 0, "items": []}

    def __init__(self, *, count=8, padding=600, owner="owner"):
        self.policy_binding = {"owner_id": owner, "project_id": "project", "projection": "test/1"}
        self.rows = []
        for i in range(count):
            messages = [Message(role="user", content=f"SENTINEL={i} " + "x" * padding),
                        Message(role="assistant", content="y" * padding)]
            ref = {"turn_id": f"turn-{i}", "run_id": f"prior-{i}", "terminal_seq": 4,
                   "transcript_fingerprint": fingerprint([m.model_dump(mode="json")
                                                           for m in messages])}
            self.rows.append({"ref": ref, "messages": messages})
        self.source_refs = tuple(row["ref"] for row in self.rows)
        self.source_fingerprint = fingerprint({"version": self.domain,
            "scope": {**self.policy_binding, "session_id": self.session_id},
            "sources": list(self.source_refs), "extensions": self.extensions})
        self.expected_anchor = self.source_fingerprint
        self.revoked = False
        self.revoke_after_page = False
        self.trace = []

    async def verify(self, conn, run_id):
        self.trace.append(("verify", id(conn), run_id))
        if self.revoked:
            raise ServiceError("unauthorized")
        if self.source_fingerprint != self.expected_anchor:
            raise ServiceError("facts_corrupt")

    async def page(self, conn, run_id, offset, limit):
        self.trace.append(("page", id(conn), offset, limit))
        assert 1 <= limit <= 16
        rows = self.rows[offset:offset + limit]
        for row in rows:
            if fingerprint([m.model_dump(mode="json") for m in row["messages"]]) != row[
                "ref"
            ]["transcript_fingerprint"]:
                raise ServiceError("facts_corrupt")
        if self.revoke_after_page:
            self.revoked = True
        return rows

    async def save_prepared(self, conn, run_id, prepared):
        self.trace.append(("save", id(conn), run_id))
        await conn.execute(update(prepared_rows).where(prepared_rows.c.run_id == run_id).values(
            value=prepared.model_dump(mode="json")))

    async def read_prepared(self, conn, run_id):
        from krim_agent.context.types import PreparedContext
        self.trace.append(("read", id(conn), run_id))
        raw = await conn.scalar(select(prepared_rows.c.value).where(
            prepared_rows.c.run_id == run_id))
        return None if raw is None else PreparedContext.model_validate_json(encoded(raw))


@pytest.fixture
async def store():
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        for table in (runs, compactions, work_turns, prepared_rows):
            await conn.run_sync(lambda sync, table=table: table.create(sync))
        for run_id in ("run", "next"):
            await conn.execute(insert(runs).values(
                id=run_id, request_key=run_id, request_hash="hash", request={}, target={},
                status="running", code=None, trace_id="trace", root_span_id="root",
                created_at=0, stop_requested=False))
            await conn.execute(insert(prepared_rows).values(run_id=run_id, value=None))
    try:
        yield SQLStore(engine)
    finally:
        await engine.dispose()


def build(store, source, script=None, *, max_steps=8):
    script = script or Script()
    events = []

    async def emit(kind, data):
        events.append((kind, data))
        return SimpleNamespace()

    step = ModelStep(script, emit, lambda: None, max_steps=max_steps, max_output=65536,
                     deadline=None, workspace=True)
    compactor = Compactor.from_source(store, source, step, lambda: None, "system", 65536,
                                      {"provider": "deterministic"})
    return compactor, script, events


async def test_versioned_scope_anchor_drives_real_summary_and_persistence(store):
    source = SessionSource()
    c, script, events = build(store, source)
    assert c.source_anchor != fingerprint(list(c.anchor_refs))
    assert not hasattr(c, "frame") and not hasattr(c, "access")
    prepared = await c.prepare("run", [Message(role="user", content="go")], [],
                               max_group=2, remaining_tools=4)
    assert c.passes == c.step.index == 1 and len(script.requests) == 1
    assert prepared[1].content.startswith("[不可信历史摘要]")
    async with store.engine.connect() as conn:
        row = (await conn.execute(select(compactions))).mappings().one()
        value = await c.read_prepared(conn)
    assert row["source_hash"] == fingerprint(list(c.anchor_refs[:c.covered]))
    assert row["policy"]["source"]["domain"] == source.domain
    assert value.source_fingerprint == source.source_fingerprint
    for i, entry in enumerate(source.trace):
        if entry[0] in {"page", "save", "read"}:
            assert source.trace[i - 1][0] == source.trace[i + 1][0] == "verify"
            assert source.trace[i - 1][1] == entry[1] == source.trace[i + 1][1]


async def test_initial_and_restored_history_share_paged_source(store):
    source = SessionSource(count=18, padding=2)
    c, script, _ = build(store, source)
    await c.prepare("run", [Message(role="user", content="go")], [],
                    max_group=2, remaining_tools=4)
    assert [(v[2], v[3]) for v in source.trace if v[0] == "page"] == [(0, 16), (16, 16)]
    assert not script.requests
    restored_source = SessionSource(count=18, padding=2)
    restored, _, _ = build(store, restored_source)
    wrapper = CompactorHistory(restored, "run")
    wrapper.restore(CompactorHistory(c, "run").checkpoint())
    await wrapper.prepare([Message(role="user", content="go")], [],
                          max_group=2, remaining_tools=4)
    assert [(v[2], v[3]) for v in restored_source.trace if v[0] == "page"] == [(0, 16), (16, 16)]


async def test_sdk_approval_restore_uses_source_without_workspace(store):
    harness, script = setup(), Script(write=True)

    def adapter(source):
        result = harness.adapter()
        c, _, _ = build(store, source, script)
        provider = HostModelProvider(c.step, result.executor.registry, max_group=2,
                                     max_calls=4, request_bytes=65536,
                                     history=CompactorHistory(c, "run"))
        budget = ExecutionBudget(asyncio.get_running_loop().time() + 30)
        async def emit(kind, data):
            return await harness.store.append("run", kind, data)
        context = replace(result.context(), emit=emit)
        result.host = HostExecution(context, budget, lambda receipt: harness.store.read_result(
            receipt.result_ref), provider.step_index, provider.checkpoint, provider.restore)
        return result, ai.Model(id="fixture", provider=provider), c

    initial, model, c = adapter(SessionSource())
    result = await initial.run(model, [ai.user_message("go")])
    assert result.status == "awaiting" and c.passes == 1
    await harness.store.set_awaiting("run")
    await harness.store.set_running("run")
    source = SessionSource()
    restored, model, next_c = adapter(source)
    request = next(e for e in harness.store.events if e.kind == "approval_request")
    result = await restored.run(model, resume=True, replies={request.data["call_id"]: True})
    assert result.status == "completed" and harness.counts["write"] == 1
    assert next_c.step.index == 3 and next_c.passes == 1 and len(script.requests) == 3
    assert any(item[0] == "page" for item in source.trace)


@pytest.mark.parametrize("change", ["domain", "owner", "anchor", "refs", "policy"])
async def test_source_descriptor_drift_stops_before_model(store, change):
    source = SessionSource()
    c, script, _ = build(store, source)
    if change == "domain":
        source.domain = "other/1"
    elif change == "owner":
        source.policy_binding = {"owner_id": "other"}
    elif change == "anchor":
        source.source_fingerprint = "changed"
    elif change == "refs":
        source.source_refs = ()
    else:
        source.context_policy = ContextPolicy()
    with pytest.raises(ServiceError, match="facts_corrupt"):
        await c.prepare("run", [Message(role="user", content="go")], [],
                        max_group=2, remaining_tools=4)
    assert not script.requests


@pytest.mark.parametrize("empty,after_page", [(True, False), (False, False), (False, True)])
async def test_authorization_is_checked_even_empty_or_revoked_during_page(store, empty, after_page):
    source = SessionSource(count=0 if empty else 2)
    c, script, _ = build(store, source)
    source.revoked = not after_page
    source.revoke_after_page = after_page
    with pytest.raises(ServiceError, match="unauthorized"):
        await c.prepare("run", [Message(role="user", content="go")], [],
                        max_group=2, remaining_tools=4)
    assert not script.requests


@pytest.mark.parametrize("offset,limit", [(0, 17), (-1, 1), (0, 0), (False, 1), (0, True)])
async def test_page_bounds_are_checked_before_source(store, offset, limit):
    source = SessionSource()
    c, _, _ = build(store, source)
    c.run_id = "run"
    async with store.engine.connect() as conn:
        with pytest.raises(ServiceError):
            await c.page(conn, offset, limit)
    assert not source.trace


async def test_domain_policy_prevents_cross_source_summary_reuse(store):
    c, _, _ = build(store, SessionSource())
    await c.prepare("run", [Message(role="user", content="go")], [],
                    max_group=2, remaining_tools=4)
    async with store.engine.begin() as conn:
        await conn.execute(update(runs).where(runs.c.id == "run").values(status="completed"))
    other = SessionSource(owner="different-owner")
    other.domain = "other-source/3"
    next_c, script, _ = build(store, other)
    await next_c.prepare("next", [Message(role="user", content="go")], [],
                         max_group=2, remaining_tools=4)
    assert next_c.passes == 1 and len(script.requests) == 1
    assert next_c.record_policy != c.record_policy


@pytest.mark.parametrize("damage", ["missing_row", "changed_ref", "broken_group", "transcript"])
async def test_source_page_mismatch_stops_before_model(store, damage):
    source = SessionSource(count=2)
    c, script, _ = build(store, source)
    original_page = source.page

    async def corrupted(conn, run_id, offset, limit):
        if damage == "transcript":
            source.rows[0]["messages"][0] = Message(role="user", content="tampered")
        rows = await original_page(conn, run_id, offset, limit)
        if damage == "missing_row":
            return rows[:-1]
        if damage == "changed_ref":
            return [{**row, "ref": {**row["ref"], "turn_id": "wrong"}} for row in rows]
        if damage == "broken_group":
            return [{**row, "messages": [Message(role="tool", content="x", call_id="missing")]}
                    for row in rows]
        return rows

    source.page = corrupted
    with pytest.raises(ServiceError, match="facts_corrupt"):
        await c.prepare("run", [Message(role="user", content="go")], [],
                        max_group=2, remaining_tools=4)
    assert not script.requests


async def test_prepared_anchor_drift_is_rejected(store):
    c, _, _ = build(store, SessionSource(count=0))
    await c.prepare("run", [Message(role="user", content="go")], [],
                    max_group=2, remaining_tools=4)
    async with store.engine.begin() as conn:
        raw = await conn.scalar(select(prepared_rows.c.value).where(
            prepared_rows.c.run_id == "run"))
        raw["source_fingerprint"] = "changed"
        await conn.execute(update(prepared_rows).where(prepared_rows.c.run_id == "run").values(
            value=raw))
        with pytest.raises(ServiceError, match="facts_corrupt"):
            await c.read_prepared(conn)


async def test_source_authorization_failure_rolls_back_prepared_write(store):
    source = SessionSource(count=0)
    c, script, _ = build(store, source)
    original = source.save_prepared

    async def revoked_after_write(conn, run_id, prepared):
        await original(conn, run_id, prepared)
        source.revoked = True

    source.save_prepared = revoked_after_write
    with pytest.raises(ServiceError, match="unauthorized"):
        await c.prepare("run", [Message(role="user", content="go")], [],
                        max_group=2, remaining_tools=4)
    async with store.engine.connect() as conn:
        assert await conn.scalar(select(prepared_rows.c.value).where(
            prepared_rows.c.run_id == "run")) is None
    assert not script.requests


async def test_same_source_reuses_summary_without_new_model_step(store):
    c, _, _ = build(store, SessionSource())
    await c.prepare("run", [Message(role="user", content="go")], [],
                    max_group=2, remaining_tools=4)
    async with store.engine.begin() as conn:
        await conn.execute(update(runs).where(runs.c.id == "run").values(status="completed"))
    reused, script, _ = build(store, SessionSource())
    await reused.prepare("next", [Message(role="user", content="go")], [],
                         max_group=2, remaining_tools=4)
    assert reused.summary == c.summary and reused.covered == c.covered
    assert reused.passes == reused.step.index == 0 and not script.requests
    again, script, _ = build(store, SessionSource())
    wrapper = CompactorHistory(again, "next")
    wrapper.restore(CompactorHistory(reused, "next").checkpoint())
    await wrapper.prepare([Message(role="user", content="go")], [],
                          max_group=2, remaining_tools=4)
    assert again.summary == c.summary and again.passes == 0 and not script.requests


async def test_prepared_confirmation_uses_source_without_reissuing_model(store):
    c, script, _ = build(store, SessionSource(count=0))
    store.lose_return = True
    await c.prepare("run", [Message(role="user", content="go")], [],
                    max_group=2, remaining_tools=4)
    assert any(item[0] == "read" for item in c.source.trace)
    assert not script.requests


def test_workspace_source_runtime_binding_tracks_original_history_seam(monkeypatch):
    from krim_agent.context import compaction, compaction_source

    before = compaction.binding_token()
    async def different_history(*args, **kwargs):
        raise AssertionError("not called")
    monkeypatch.setattr(compaction_source, "workspace_history", different_history)
    assert compaction.binding_token() != before


def workspace_frame(refs):
    session, turn, revision = (str(uuid4()) for _ in range(3))
    policy = ContextPolicy(max_bytes=10000, reserved_output_tokens=64)
    return WorkFrame.model_validate_json(encoded({
        "session_id": session, "turn_id": turn, "actor_id": "local",
        "grant": {"actor_id": "local", "generation": 1, "workspace_id": "local",
                  "operations": ["read", "write", "exec", "approve"]},
        "original_request": {"request_key": "go", "text": "go", "expected_revision": 0,
                             "completion": {}},
        "input_revision": {"revision_id": revision, "session_id": session, "parent_id": None,
                           "revision": 0, "source_kind": "input", "request_key": "input",
                           "request_hash": "0" * 64, "manifest": [], "manifest_hash": "0" * 64,
                           "producer_run_id": None, "producer_call_id": None},
        "source_refs": list(refs), "subject": {},
        "conditions": {"context": {"policy": policy.model_dump(mode="json"),
                                    "compaction": CompactionPolicy().model_dump(mode="json")}},
        "effective_fingerprint": "0" * 64, "phase": "preparing", "verification": None,
    }))


async def test_original_workspace_constructor_keeps_real_prepare_summary_and_ledger(
    store, monkeypatch,
):
    fixture = SessionSource()
    frame = workspace_frame(fixture.source_refs)
    access = ExecutionAccess(LOCAL, "local", frame.session_id)
    seen = []

    async def workspace_history(conn, run_id, offset, limit, current_access, *, source_anchor):
        assert current_access is access and source_anchor == fingerprint(list(frame.source_refs))
        seen.append((id(conn), run_id, offset, limit))
        return fixture.rows[offset:offset + limit]

    monkeypatch.setattr("krim_agent.context.compaction_source.workspace_history", workspace_history)
    base, script, _ = build(store, fixture)
    c = Compactor(store, frame, access, base.step, lambda: None, "system", 65536,
                  {"provider": "deterministic"}, source_anchor=fingerprint(list(frame.source_refs)))
    assert isinstance(c.source, WorkspaceCompactionSource)
    assert c.frame == frame and c.access is access
    async with store.engine.begin() as conn:
        await conn.execute(insert(work_turns).values(
            turn_id=frame.turn_id, run_id="run", session_id=frame.session_id, actor_id="local",
            grant_snapshot=frame.grant.model_dump(mode="json"),
            request_json=frame.original_request.model_dump(mode="json"), request_hash="0" * 64,
            requested_input={}, plan={}, plan_revision=0, phase="preparing", created_at=0,
        ))
    result = await c.prepare("run", [Message(role="user", content="go")], [],
                             max_group=2, remaining_tools=4)
    assert c.passes == 1 and c.step.index == 1
    assert result[1].content.startswith("[不可信历史摘要]")
    assert c.record_policy["grant"] == frame.grant.model_dump(mode="json")
    async with store.engine.connect() as conn:
        row = (await conn.execute(select(compactions))).mappings().one()
        prepared = await c.read_prepared(conn)
    assert prepared is not None and list(prepared.messages) == result
    assert row["session_id"] == frame.session_id
    assert row["source_hash"] == fingerprint(list(frame.source_refs[:c.covered]))
    assert seen and all(item[-1] <= 16 for item in seen)


async def test_workspace_none_transcript_fingerprint_is_not_reinterpreted(store, monkeypatch):
    ref = {"turn_id": str(uuid4()), "run_id": None, "terminal_seq": -1,
           "transcript_fingerprint": fingerprint(None), "status": "cancelled"}
    frame = workspace_frame([ref])
    access = ExecutionAccess(LOCAL, "local", frame.session_id)
    fallback = [Message(role="user", content="old"),
                Message(role="user", content="[不可信历史状态数据] cancelled: unknown")]

    async def history(conn, run_id, offset, limit, current_access, *, source_anchor):
        return [{"ref": ref, "messages": fallback}][offset:offset + limit]

    monkeypatch.setattr("krim_agent.context.compaction_source.workspace_history", history)
    base, script, _ = build(store, SessionSource(count=0))
    c = Compactor(store, frame, access, base.step, lambda: None, "system", 65536, {},
                  source_anchor=fingerprint([ref]))
    result = await c.prepare("run", [Message(role="user", content="go")], [],
                             max_group=2, remaining_tools=4)
    assert result[1:3] == fallback and not script.requests
