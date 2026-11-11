import asyncio

import pytest

from krim_agent.composition import open_application
from krim_agent.context.manifest import (
    ItemMetadata,
    build_context_manifest,
    session_message_metadata,
)
from krim_agent.context.types import HistorySource
from krim_agent.contracts import Message, RunEvent, RunRequest, encoded, fingerprint
from krim_agent.providers.types import ModelRequestBudget, ModelText, ModelToolCall
from krim_agent.runtime.model_step import ModelStep


class RecordingProvider:
    def __init__(self, deltas=()):
        self.deltas = deltas
        self.request = None

    async def stream(self, messages, tools):
        self.request = (list(messages), list(tools))
        for delta in self.deltas:
            if isinstance(delta, float):
                await asyncio.sleep(delta)
            else:
                yield delta

    async def close(self):
        return None


async def record(events, kind, data):
    events.append((kind, data))
    return None


def test_manifest_counts_canonical_wrappers_and_repeated_messages_without_body():
    repeated = Message(role="user", content="private request body")
    messages = [Message(role="system", content="private system prompt"), repeated, repeated]
    tools = [{"name": "lookup", "input_schema": {"type": "object"}}]
    manifest = build_context_manifest(messages, tools)
    serialized = encoded({
        "messages": [message.model_dump(mode="json") for message in messages],
        "tools": tools,
    }).encode("utf-8")
    items = manifest["items"]
    repeated_items = [item for item in items if item.get("message_index") in {1, 2}]

    assert manifest["input_bytes"] == len(serialized)
    assert sum(item["input_bytes"] for item in items) == len(serialized)
    assert sum(item["estimated_tokens"] for item in items) == manifest["estimated_tokens"]
    assert manifest["estimated_tokens"] == (len(serialized) + 3) // 4
    assert manifest["model_context_window"] is None
    assert "reserved_output_tokens" not in manifest
    assert "input_budget_bytes" not in manifest
    assert any(item["key"] == "unknown:request-envelope" for item in items)
    assert [item["key"] for item in repeated_items] == ["message:1:user", "message:2:user"]
    assert repeated_items[0]["content_hash"] == repeated_items[1]["content_hash"]
    assert "private request body" not in encoded(manifest)
    assert "private system prompt" not in encoded(manifest)


def test_manifest_uses_explicit_metadata_and_hashes_content_changes():
    source = {"turn_id": "turn-1", "run_id": "run-1", "last_seq": 8,
              "fingerprint": "a" * 64}
    original = [Message(role="user", content="file summary memory words")]
    changed = [Message(role="user", content="file summary memory changed")]
    metadata = [ItemMetadata(category="files", label="project source", source_refs=(source,))]

    first = build_context_manifest(original, [], message_metadata=metadata)
    second = build_context_manifest(changed, [], message_metadata=metadata)
    first_item = next(item for item in first["items"] if item.get("message_index") == 0)
    second_item = next(item for item in second["items"] if item.get("message_index") == 0)

    assert first_item["category"] == "files"
    assert first_item["label"] == "project source"
    assert first_item["source_refs"] == [source]
    assert first_item["key"] == second_item["key"]
    assert first_item["content_hash"] != second_item["content_hash"]
    # Words that resemble file/summary/memory content do not override the role-based category.
    guessed = build_context_manifest(original, [])
    assert next(item for item in guessed["items"] if item.get("message_index") == 0)[
        "category"
    ] == "history"


def test_manifest_bounds_items_and_preserves_byte_total():
    messages = [Message(role="user", content=f"message-{index}") for index in range(400)]
    manifest = build_context_manifest(messages, [])

    assert len(manifest["items"]) <= 128
    assert manifest["items_truncated"] is True
    assert manifest["omitted_items"] > 0
    assert manifest["items"][-1]["key"] == "unknown:truncated"
    assert sum(item["input_bytes"] for item in manifest["items"]) == manifest["input_bytes"]
    assert (sum(item["estimated_tokens"] for item in manifest["items"])
            == manifest["estimated_tokens"])
    assert all("message-" not in item["label"] for item in manifest["items"])


def test_tool_names_are_joined_from_schema_and_call_id():
    tools = [{"name": "lookup", "input_schema": {"type": "object"}}]
    messages = [
        Message(role="assistant", content="", tool_calls=[
            {"id": "provider-call-1", "name": "lookup", "arguments": {}}
        ]),
        Message(role="tool", content="private tool result", call_id="provider-call-1"),
    ]
    manifest = build_context_manifest(messages, tools)
    definition = next(item for item in manifest["items"] if item.get("tool_index") == 0)
    result = next(item for item in manifest["items"] if item.get("call_id") == "provider-call-1")

    assert definition["label"] == "Tool: lookup"
    assert result["category"] == "tool_result"
    assert result["label"] == "Tool result: lookup"
    assert result["provider_call_id"] == "provider-call-1"
    assert "private tool result" not in encoded(manifest)
    no_tools = build_context_manifest(messages, [])
    no_tools_result = next(item for item in no_tools["items"]
                           if item.get("provider_call_id") == "provider-call-1")
    assert no_tools_result["label"] == "Tool result: lookup"


def test_manifest_caps_total_source_refs_and_marks_truncation():
    messages = [Message(role="user", content=f"context-{index}") for index in range(300)]
    metadata = [
        ItemMetadata(source_refs=({"turn_id": f"turn-{index}"},))
        for index in range(len(messages))
    ]
    manifest = build_context_manifest(messages, [], message_metadata=metadata)
    refs = [ref for item in manifest["items"] for ref in item.get("source_refs", [])]

    assert len(refs) <= 16
    assert manifest["source_refs_truncated"] is True


def test_compactor_metadata_marks_summary_and_retained_history_from_state_only():
    from krim_agent.context.compaction import Compactor

    covered = {"turn_id": "turn-covered", "run_id": "run-covered", "terminal_seq": 3,
               "transcript_fingerprint": "c" * 64}
    retained = {"turn_id": "turn-retained", "run_id": "run-retained", "terminal_seq": 7,
                "transcript_fingerprint": "r" * 64}
    summary_message = Message(role="user", content="summary body must not be copied")
    retained_message = Message(role="tool", content="tool result body must not be copied",
                               call_id="provider-call")
    compactor = object.__new__(Compactor)
    compactor.summary = "summary body must not be copied"
    compactor.covered = 1
    compactor.anchor_refs = (covered, retained)
    compactor.history = [
        {"ref": covered, "messages": [Message(role="user", content="old source")]},
        {"ref": retained, "messages": [retained_message]},
    ]
    messages = [
        Message(role="system", content="system"),
        summary_message,
        retained_message,
        Message(role="user", content="current request"),
    ]

    metadata = Compactor.manifest_metadata(compactor, messages)
    manifest = build_context_manifest(messages, [], message_metadata=metadata)
    summary_item = next(item for item in manifest["items"] if item.get("message_index") == 1)
    retained_item = next(item for item in manifest["items"] if item.get("message_index") == 2)

    assert summary_item["category"] == "summary"
    assert summary_item["source_refs"][0]["turn_id"] == "turn-covered"
    assert retained_item["category"] == "tool_result"
    assert retained_item["source_refs"][0]["turn_id"] == "turn-retained"
    assert "summary body must not be copied" not in encoded(manifest)
    assert "tool result body must not be copied" not in encoded(manifest)


def test_session_source_refs_follow_exact_frozen_turn_fingerprint():
    system = Message(role="system", content="system")
    history = [Message(role="user", content="old question"),
               Message(role="assistant", content="old answer")]
    current = Message(role="user", content="current question")
    source = HistorySource(
        turn_id="turn-old",
        run_id="run-old",
        last_seq=7,
        fingerprint=fingerprint([message.model_dump(mode="json") for message in history]),
    )
    messages = [system, *history, current]
    metadata = session_message_metadata(messages, [source])
    manifest = build_context_manifest(messages, [], message_metadata=metadata)
    history_items = [item for item in manifest["items"] if item.get("message_index") in {1, 2}]

    assert all(item["source_refs"][0]["turn_id"] == "turn-old" for item in history_items)
    assert "source_refs" not in manifest["items"][3]


def test_manifest_stays_below_event_size_budget_with_long_metadata():
    messages = [
        Message(role="user", content="request body " + "界" * 100)
        for _index in range(400)
    ]
    metadata = [
        ItemMetadata(
            label="来源" * 100,
            source_refs=({"turn_id": "轮次" * 100, "fingerprint": "f" * 64},),
        )
        for _index in messages
    ]
    manifest = build_context_manifest(messages, [], message_metadata=metadata)

    assert len(encoded(manifest).encode("utf-8")) <= 32768
    event = RunEvent(
        run_id="run",
        seq=0,
        event_id="event",
        kind="model_start",
        data={"span_id": "span", "step_index": 0, "context_manifest": manifest},
    )
    assert len(encoded(event.model_dump(mode="json")).encode("utf-8")) <= 65536
    assert sum(item["input_bytes"] for item in manifest["items"]) == manifest["input_bytes"]
    assert (sum(item["estimated_tokens"] for item in manifest["items"])
            == manifest["estimated_tokens"])
    assert manifest["items_truncated"] is True


@pytest.mark.asyncio
async def test_model_start_manifest_matches_budgeted_text_only_request():
    provider = RecordingProvider([ModelText(text="done")])
    events = []
    step = ModelStep(
        provider, lambda kind, data: record(events, kind, data), lambda: None,
        max_steps=2, max_output=4096, deadline=None, workspace=False,
    )
    messages = [Message(role="system", content="system"), Message(role="user", content="body")]
    tools = [{"name": "unused", "input_schema": {"type": "object"}}]

    await step.call(
        messages,
        tools,
        request_budget=ModelRequestBudget(2, 0, 0),
        request_metadata=(None, ItemMetadata(category="history", label="Current turn")),
        reserved_output_tokens=32,
        input_budget_bytes=2048,
    )

    assert provider.request is not None
    actual_messages, actual_tools = provider.request
    start = next(data for kind, data in events if kind == "model_start")
    manifest = start["context_manifest"]
    expected_bytes = len(encoded({
        "messages": [message.model_dump(mode="json") for message in actual_messages],
        "tools": actual_tools,
    }).encode("utf-8"))
    assert actual_tools == []
    assert "Krim request budget/1" in actual_messages[0].content
    assert manifest["input_bytes"] == expected_bytes
    assert sum(item["input_bytes"] for item in manifest["items"]) == expected_bytes
    assert manifest["reserved_output_tokens"] == 32
    assert manifest["input_budget_bytes"] == 2048
    assert manifest["model_context_window"] is None
    assert "body" not in encoded(manifest)
    assert sum(item.get("tool_index") is not None for item in manifest["items"]) == 0


@pytest.mark.asyncio
async def test_budget_inserted_system_does_not_shift_frozen_user_source():
    provider, events = RecordingProvider([ModelText(text="done")]), []
    step = ModelStep(provider, lambda kind, data: record(events, kind, data), lambda: None,
                     max_steps=2, max_output=4096, deadline=None, workspace=False)
    await step.call([Message(role="user", content="user")], [],
                    request_metadata=[ItemMetadata(source_refs=({"turn_id": "source"},))])
    manifest = next(data["context_manifest"] for kind, data in events if kind == "model_start")
    messages = [item for item in manifest["items"] if "message_index" in item]
    assert "source_refs" not in messages[0]
    assert messages[1]["source_refs"][0]["turn_id"] == "source"


@pytest.mark.asyncio
async def test_actual_tool_result_manifest_joins_provider_id_not_internal_id(tmp_path):
    async with await open_application(tmp_path / "tool-source.db") as app:
        run = await app.service.start(RunRequest(text="/add 2 3", request_key="tool-source"))
        await app.service.wait(run.id)
        facts = await app.service.result(run.id)
    tool = next(event.data for event in facts.events if event.kind == "tool_end")
    items = [item for event in facts.events if event.kind == "model_start"
             for item in event.data["context_manifest"]["items"]
             if item["category"] == "tool_result"]
    assert len(items) == 1
    assert items[0]["provider_call_id"] == tool["provider_call_id"]
    assert items[0]["provider_call_id"] != tool["call_id"]


@pytest.mark.asyncio
async def test_engine_model_start_manifest_matches_the_actual_provider_request(tmp_path):
    provider = RecordingProvider([ModelText(text="answer")])
    async with await open_application(tmp_path / "manifest-run.db", provider=provider) as app:
        run = await app.service.start(
            RunRequest(text="private engine request", request_key="manifest")
        )
        await app.service.wait(run.id)
        facts = await app.service.result(run.id)

    assert provider.request is not None
    actual_messages, actual_tools = provider.request
    start = next(event.data for event in facts.events if event.kind == "model_start")
    manifest = start["context_manifest"]
    expected_bytes = len(encoded({
        "messages": [message.model_dump(mode="json") for message in actual_messages],
        "tools": actual_tools,
    }).encode("utf-8"))
    assert facts.run.status == "completed"
    assert manifest["input_bytes"] == expected_bytes
    assert sum(item["input_bytes"] for item in manifest["items"]) == expected_bytes
    assert "private engine request" not in encoded(manifest)
    assert next(event.data for event in facts.events if event.kind == "model_end")[
        "timing_origin"
    ] == "provider_event"


@pytest.mark.asyncio
async def test_model_end_records_monotonic_first_content_and_first_text_for_tool_call():
    provider = RecordingProvider([
        ModelToolCall(id="provider-call-1", name="lookup", arguments={}),
        0.005,
        ModelText(text="done"),
    ])
    events = []
    step = ModelStep(
        provider, lambda kind, data: record(events, kind, data), lambda: None,
        max_steps=1, max_output=4096, deadline=None, workspace=False,
    )

    await step.call(
        [Message(role="user", content="invoke lookup")],
        [{"name": "lookup", "input_schema": {"type": "object"}}],
        max_group=1,
        remaining_tools=1,
    )
    ended = next(data for kind, data in events if kind == "model_end")

    assert ended["timing_origin"] == "provider_event"
    assert ended["elapsed_ms"] >= ended["first_content_ms"] >= 0
    assert ended["first_text_ms"] >= ended["first_content_ms"]
    assert ended["tool_calls"][0]["id"] == "provider-call-1"


@pytest.mark.asyncio
async def test_model_end_uses_null_first_content_when_stream_has_no_content():
    provider = RecordingProvider()
    events = []
    step = ModelStep(
        provider, lambda kind, data: record(events, kind, data), lambda: None,
        max_steps=1, max_output=4096, deadline=None, workspace=False,
    )

    await step.call([Message(role="user", content="empty")], [])
    ended = next(data for kind, data in events if kind == "model_end")

    assert ended["elapsed_ms"] >= 0
    assert ended["first_content_ms"] is None
    assert ended["first_text_ms"] is None


@pytest.mark.asyncio
async def test_failed_stream_does_not_emit_successful_model_end():
    class BrokenProvider:
        async def stream(self, _messages, _tools):
            yield ModelText(text="partial")
            raise RuntimeError("stream broke")

    events = []
    step = ModelStep(
        BrokenProvider(), lambda kind, data: record(events, kind, data), lambda: None,
        max_steps=1, max_output=4096, deadline=None, workspace=False,
    )

    with pytest.raises(RuntimeError, match="stream broke"):
        await step.call([Message(role="user", content="failure")], [])

    assert any(kind == "model_start" for kind, _data in events)
    assert not any(kind == "model_end" for kind, _data in events)
