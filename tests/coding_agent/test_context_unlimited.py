"""Context capacity follows model boundaries, not history count/product quotas."""

from __future__ import annotations

import pytest
from sqlalchemy import select

from krim_agent.agent_sdk.context import CompactorHistory
from krim_agent.context import builder
from krim_agent.context.serialization import encoded, fingerprint
from krim_agent.context.service import ContextSnapshotItem
from krim_agent.context.types import CompactionPolicy, ContextPolicy, DevelopmentContextState
from krim_agent.contracts import Message, ServiceError
from krim_agent.providers.types import ModelText
from krim_agent.storage.development_context import make_frozen, state_from_raw
from krim_agent.storage.models import compactions
from tests.coding_agent import test_sdk_context_source_port as source_helpers
from tests.coding_agent.test_sdk_context_source_port import SessionSource, build


@pytest.fixture
async def store():
    async for value in source_helpers.store.__wrapped__():
        yield value


async def test_more_than_two_summary_passes_preserve_all_sources_and_resume(store):
    source = SessionSource(count=30)
    compactor, script, _ = build(store, source, max_steps=50)
    messages = await compactor.prepare(
        "run", [Message(role="user", content="continue")], [], max_group=2, remaining_tools=4
    )
    assert compactor.passes > 2
    assert compactor.step.index == compactor.passes == len(script.requests)
    assert len(compactor.anchor_refs) == 30
    assert messages[-1].content == "continue"
    assert any("SENTINEL=29" in message.content for message in messages)
    async with store.engine.connect() as conn:
        rows = (
            (await conn.execute(select(compactions).order_by(compactions.c.pass_index)))
            .mappings()
            .all()
        )
        assert [row["pass_index"] for row in rows] == list(range(compactor.passes))
        assert all(
            row["source_refs"] == list(source.source_refs[: len(row["source_refs"])])
            for row in rows
        )
    restored, _, _ = build(store, SessionSource(count=30), max_steps=50)
    restored.step.index = compactor.step.index
    wrapper = CompactorHistory(restored, "run")
    wrapper.restore(CompactorHistory(compactor, "run").checkpoint())
    await wrapper.prepare(
        [Message(role="user", content="continue")], [], max_group=2, remaining_tools=4
    )
    assert restored.passes == compactor.passes


async def test_summary_above_two_kib_uses_model_capacity(store):
    class LongSummary:
        async def stream(self, messages, tools):
            yield ModelText(text="summary facts " + "s" * 3000)

    source = SessionSource(count=10, padding=1500)
    source.context_policy = ContextPolicy(max_bytes=25000, reserved_output_tokens=2048)
    compactor, _, _ = build(store, source, LongSummary(), max_steps=50)
    await compactor.prepare(
        "run", [Message(role="user", content="continue")], [], max_group=2, remaining_tools=4
    )
    assert len(compactor.summary.encode()) > 2048
    assert compactor.summary_capacity() == compactor.input_capacity()


async def test_output_reservation_is_not_a_four_byte_summary_limit(store):
    source = SessionSource(count=8)
    source.context_policy = ContextPolicy(max_bytes=10000, reserved_output_tokens=1)
    compactor, _, _ = build(store, source)
    messages = await compactor.prepare(
        "run",
        [Message(role="user", content="continue")],
        [],
        max_group=2,
        remaining_tools=None,
    )
    assert len(compactor.summary.encode()) > 4
    assert compactor.summary_capacity() == compactor.input_capacity()
    assert messages[-1].content == "continue"
    source.compaction_policy = CompactionPolicy(summary_bytes=3)
    legacy, _, _ = build(store, source)
    with pytest.raises(ServiceError, match="summary_failed"):
        await legacy.prepare(
            "next",
            [Message(role="user", content="continue")],
            [],
            max_group=2,
            remaining_tools=None,
        )


async def test_no_progress_stops_without_spending_repeated_summary_calls(store):
    class ExpandingSummary:
        def __init__(self):
            self.calls = 0

        async def stream(self, messages, tools):
            self.calls += 1
            yield ModelText(text="unhelpful " * 150)

    source = SessionSource(count=3, padding=2)
    source.context_policy = ContextPolicy(max_bytes=10000, reserved_output_tokens=1024)
    provider = ExpandingSummary()
    compactor, _, _ = build(store, source, provider)
    with pytest.raises(ServiceError, match="compaction_budget"):
        await compactor.prepare(
            "run",
            [Message(role="user", content="continue")],
            [],
            max_group=2,
            remaining_tools=4,
            force=True,
        )
    assert provider.calls == 1
    assert len(source.rows) == 3


class Boundary:
    def __init__(self):
        self.pending_calls = 0
        self.finished = []
        self.revision = 1

    async def pending(self):
        self.pending_calls += 1
        return "manual-1" if not self.finished else None

    async def prepare(self):
        return [
            ContextSnapshotItem(
                name="plan", revision=str(self.revision), content="finish checks before release"
            )
        ]

    async def complete(self, token, metadata=None):
        self.finished.append((token, metadata))


async def test_manual_request_waits_for_complete_tool_group_and_refreshes_snapshots(store):
    compactor, script, _ = build(store, SessionSource(count=5, padding=100))
    boundary = Boundary()
    wrapper = CompactorHistory(compactor, "run", boundary=boundary)
    conversation = [
        Message(role="user", content="continue"),
        Message(
            role="assistant",
            content="",
            tool_calls=[{"id": "call-1", "name": "read", "arguments": {}}],
        ),
    ]
    with pytest.raises(ServiceError, match="facts_corrupt"):
        await wrapper.prepare(conversation, [], max_group=2, remaining_tools=4)
    assert boundary.pending_calls == 0 and not script.requests
    conversation.append(Message(role="tool", content="original result", call_id="call-1"))
    messages = await wrapper.prepare(conversation, [], max_group=2, remaining_tools=4)
    assert compactor.passes == 1 and boundary.finished[0][0] == "manual-1"
    assert messages[-3:] == conversation
    assert any("finish checks" in message.content for message in messages)
    checkpoint = wrapper.checkpoint()
    assert checkpoint["snapshot_refs"][0]["revision"] == "1"
    boundary.revision = 2
    messages = await wrapper.prepare(conversation, [], max_group=2, remaining_tools=4)
    assert wrapper.checkpoint()["snapshot_refs"][0]["revision"] == "2"
    assert compactor.passes == 1 and len(boundary.finished) == 1
    metadata = compactor.manifest_metadata(messages)
    assert any(item and item.label == "plan snapshot / 2" for item in metadata)


async def test_live_memory_replaces_frozen_projection_without_mutating_audit_snapshot(store):
    source = SessionSource(count=8)
    source.extensions = {
        "version": "extensions/2",
        "revision": 4,
        "items": [
            {
                "kind": "memory",
                "id": "old-memory",
                "revision": 1,
                "content_hash": "1" * 64,
                "body": "STALE_MEMORY_SENTINEL",
            },
            {
                "kind": "skill",
                "id": "skill",
                "revision": 2,
                "content_hash": "2" * 64,
                "body": "SKILL_SENTINEL",
            },
        ],
    }
    source.source_fingerprint = fingerprint(
        {"sources": list(source.source_refs), "extensions": source.extensions}
    )
    source.expected_anchor = source.source_fingerprint
    audit_hash = fingerprint(source.extensions)
    compactor, _, _ = build(store, source)

    class LiveBoundary:
        enabled = True

        async def pending(self):
            return None

        async def prepare(self):
            return (
                [ContextSnapshotItem(name="memory", revision=7, content="CURRENT_MEMORY_SENTINEL")]
                if self.enabled
                else []
            )

        async def complete(self, token, receipt=None):
            raise AssertionError("no manual request")

    boundary = LiveBoundary()
    wrapper = CompactorHistory(compactor, "run", boundary=boundary, live_memory=True)
    current = [Message(role="user", content="continue")]
    messages = await wrapper.prepare(current, [], max_group=2, remaining_tools=None)
    assert not any("STALE_MEMORY_SENTINEL" in message.content for message in messages)
    assert sum("CURRENT_MEMORY_SENTINEL" in message.content for message in messages) == 1
    assert sum("SKILL_SENTINEL" in message.content for message in messages) == 1
    assert fingerprint(compactor.extension_snapshot) == audit_hash == fingerprint(source.extensions)
    metadata = compactor.manifest_metadata(messages)
    assert sum(item is not None and item.label == "Frozen Skill snapshot" for item in metadata) == 1
    assert not any(item and item.label == "Frozen private memory snapshot" for item in metadata)
    prepared = await store.work_read(lambda conn: compactor.read_prepared(conn))
    assert not any("STALE_MEMORY_SENTINEL" in message.content for message in prepared.messages)
    boundary.enabled = False
    messages = await wrapper.prepare(current, [], max_group=2, remaining_tools=None)
    assert not any("MEMORY_SENTINEL" in message.content for message in messages)
    assert any("SKILL_SENTINEL" in message.content for message in messages)
    assert not wrapper.checkpoint()["snapshot_refs"]


async def test_legacy_frozen_memory_projection_is_retained_without_live_port(store):
    source = SessionSource(count=0)
    source.extensions = {
        "version": "extensions/2",
        "revision": 1,
        "items": [
            {
                "kind": "memory",
                "id": "legacy-memory",
                "revision": 1,
                "content_hash": "1" * 64,
                "body": "LEGACY_MEMORY_SENTINEL",
            },
        ],
    }
    source.source_fingerprint = fingerprint(source.extensions)
    source.expected_anchor = source.source_fingerprint
    compactor, _, _ = build(store, source)
    wrapper = CompactorHistory(compactor, "run")
    messages = await wrapper.prepare(
        [Message(role="user", content="continue")], [], max_group=2, remaining_tools=None
    )
    assert any("LEGACY_MEMORY_SENTINEL" in message.content for message in messages)
    with pytest.raises(ValueError, match="snapshot boundary"):
        CompactorHistory(compactor, "run", live_memory=True)


async def test_failed_compaction_does_not_acknowledge_manual_request(store):
    source = SessionSource(count=3, padding=10000)
    compactor, _, _ = build(store, source)
    boundary = Boundary()
    wrapper = CompactorHistory(compactor, "run", boundary=boundary)
    with pytest.raises(ServiceError, match="compaction_budget"):
        await wrapper.prepare(
            [Message(role="user", content="continue")], [], max_group=2, remaining_tools=4
        )
    assert boundary.pending_calls == 1 and not boundary.finished


async def test_history_prefers_combined_boundary_prepare_without_legacy_reads(store):
    compactor, _, _ = build(store, SessionSource(count=0))

    class Combined:
        calls = 0

        async def prepare_context(self):
            self.calls += 1
            return None, [ContextSnapshotItem(name="plan", revision=self.calls, content="current")]

        async def pending(self):
            raise AssertionError("combined request must not re-read pending")

        async def prepare(self):
            raise AssertionError("combined request must not re-read scope")

        async def complete(self, token, receipt=None):
            raise AssertionError("no pending token")

    boundary = Combined()
    wrapper = CompactorHistory(compactor, "run", boundary=boundary)
    for _ in range(2):
        messages = await wrapper.prepare(
            [Message(role="user", content="go")], [], max_group=2, remaining_tools=None
        )
        assert any("current" in message.content for message in messages)
    assert boundary.calls == 2
    assert wrapper.checkpoint()["snapshot_refs"][0]["revision"] == 2


async def test_prepare_accepts_unlimited_tool_allowance(store):
    compactor, _, _ = build(store, SessionSource(count=8))
    compactor.step.max_steps = None
    wrapper = CompactorHistory(compactor, "run")
    messages = await wrapper.prepare(
        [Message(role="user", content="continue")],
        [],
        max_group=2,
        remaining_tools=None,
    )
    assert compactor.passes >= 1 and messages[-1].content == "continue"


def test_canonical_context_json_has_no_total_storage_ceiling():
    from krim_agent.context.types import HistorySource
    from krim_agent.contracts import encoded as old_encoded
    from krim_agent.contracts import fingerprint as old_fingerprint

    small = {"b": [1, "中"], "a": True}
    assert encoded(small) == old_encoded(small)
    assert fingerprint(small) == old_fingerprint(small)
    sources = tuple(
        HistorySource(turn_id=f"turn-{i}", run_id=f"run-{i}", last_seq=4, fingerprint="f" * 64)
        for i in range(9000)
    )
    frozen = make_frozen(
        owner_id="owner",
        workspace_id="workspace",
        project_id="project",
        session_id="session",
        turn_id="turn-current",
        run_id="run-current",
        authorization_generation=1,
        sources=sources,
        current=Message(role="user", content="go"),
        system_prompt="system",
        extensions={},
        policy=ContextPolicy(),
        compaction=CompactionPolicy(),
        tool_fingerprint="tools",
    )
    state = DevelopmentContextState(frozen=frozen)
    raw = state.model_dump(mode="json")
    assert len(encoded(raw).encode()) > 1048576
    assert state_from_raw(raw, fingerprint(frozen.model_dump(mode="json"))) == state


def test_large_model_capacity_and_message_count_are_not_application_capped():
    policy = ContextPolicy(max_bytes=3000000, reserved_output_tokens=1)
    messages = [Message(role="system", content="system")]
    messages.extend(Message(role="user", content="x" * 1100) for _ in range(1100))
    assert builder.check_budget(messages, [], policy, 3000000) > 1048576
    legacy = ContextPolicy(max_bytes=3000000, max_messages=256, reserved_output_tokens=1)
    with pytest.raises(ServiceError, match="context_budget"):
        builder.check_budget(messages, [], legacy, 3000000)
