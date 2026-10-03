import asyncio
from datetime import UTC, datetime

import pytest

from krim_agent.config import Settings
from krim_agent.context_engine import ContextEngine, ContextError, ContextOptions
from krim_agent.protocol.domain import ChatRequest, MessageView
from krim_agent.providers import ModelText
from krim_agent.runtime import ChatRuntime
from krim_agent.tools import ToolRegistry


def message(index: int, role: str, content: str, details=None) -> MessageView:
    return MessageView(
        id=f"m-{index}",
        topic_id="topic",
        run_id="run",
        sequence=index,
        role=role,
        content=content,
        details=details or {},
        created_at=datetime(2026, 10, 3, tzinfo=UTC),
    )


def history():
    return [
        message(1, "system", "规则"),
        message(2, "user", "问题一"),
        message(3, "assistant", "回答一"),
        message(4, "user", "问题二"),
        message(5, "assistant", "工具请求", {"tool_calls": [{"id": "call-2"}]}),
        message(6, "tool", "工具结果", {"call_id": "call-2"}),
        message(7, "assistant", "回答二"),
        message(8, "user", "问题三"),
        message(9, "assistant", "回答三"),
    ]


def test_never_policy_and_no_limits_do_not_change_history():
    engine = ContextEngine()
    messages = history()
    result = engine.prepare(messages, ContextOptions(policy="never", max_messages=2))
    assert result.messages == messages and not result.compacted and result.omitted_count == 0
    result = engine.prepare(messages, ContextOptions())
    assert result.messages == messages and result.processor_trace == ["message_cleanup"]


def test_sliding_window_keeps_complete_latest_turn():
    result = ContextEngine().prepare(
        history(), ContextOptions(strategy="sliding_window", policy="auto", max_messages=4)
    )
    assert result.compacted and result.omitted_count == 7
    assert [item.id for item in result.messages] == ["m-8", "m-9"]
    assert result.processor_trace[-1] == "sliding_window"


@pytest.mark.parametrize("strategy", ["truncate", "summarize_then_truncate", "annotated_compact"])
def test_strategies_have_distinct_processor_trace_and_summary_injection(strategy):
    async def summary(omitted):
        return "异步摘要:" + str(len(omitted))

    result = asyncio.run(
        ContextEngine().prepare_async(
            history(), ContextOptions(strategy=strategy, max_messages=3), summary
        )
    )
    assert result.compacted and result.omitted_count > 0
    if strategy == "truncate":
        assert result.summary is None and "summary_injector" not in result.processor_trace
    else:
        assert result.summary == "异步摘要:" + str(result.omitted_count)
        assert result.messages[0].role == "user" and result.messages[0].details["context_summary"]


def test_tool_group_or_latest_group_over_budget_is_explicit_error():
    engine = ContextEngine()
    malformed = history()[:5] + [message(6, "tool", "孤儿", {"call_id": "missing"})]
    with pytest.raises(ContextError, match="工具组"):
        engine.prepare(malformed, ContextOptions(max_messages=2))
    with pytest.raises(ContextError, match="最新完整"):
        engine.prepare(
            [message(1, "user", "x" * 150), message(2, "assistant", "y")],
            ContextOptions(max_chars=128),
        )


class RecordingProvider:
    name = "context-recorder"

    def __init__(self):
        self.contexts: list[list[str]] = []

    async def stream(self, messages, _tools):
        self.contexts.append([message.id for message in messages])
        yield ModelText(text="最终回答")


async def test_runtime_consumes_compacted_context_and_publishes_event(repository):
    provider = RecordingProvider()
    settings = Settings(context_max_messages=4, context_policy="auto")
    runtime = ChatRuntime(repository, provider, ToolRegistry(), settings)
    try:
        topic = await repository.create_topic("context")
        for index in range(3):
            run = await runtime.start(
                topic.id, ChatRequest(text=f"第 {index}", idempotency_key=str(index))
            )
            state = await runtime.stream_state(run.id, 0)
            await state.task
        latest = await runtime.start(topic.id, ChatRequest(text="最后", idempotency_key="last"))
        state = await runtime.stream_state(latest.id, 0)
        frames = [frame async for frame in runtime.subscribe(state) if frame is not None]
        assert any(
            frame.data.get("type") == "context_compact"
            for frame in frames
            if isinstance(frame.data, dict)
        )
        assert len(provider.contexts[-1]) <= 4
        assert provider.contexts[-1][-1] == latest.input_message_id
    finally:
        await runtime.close()


def test_budget_includes_tool_call_arguments_even_when_content_is_empty():
    messages = [
        message(1, "user", "问"),
        message(
            2,
            "assistant",
            "",
            {"tool_calls": [{"id": "large", "name": "lookup", "arguments": {"data": "x" * 500}}]},
        ),
        message(3, "tool", "", {"call_id": "large"}),
        message(4, "assistant", "答"),
    ]
    with pytest.raises(ContextError) as error:
        ContextEngine().prepare(messages, ContextOptions(max_chars=128))
    assert error.value.code == "context_budget"


@pytest.mark.parametrize("case", ["duplicate", "missing_id", "invalid_calls", "interleaved"])
def test_malformed_tool_call_groups_are_rejected_before_compaction(case):
    calls = [{"id": "call"}]
    if case == "duplicate":
        calls.append({"id": "call"})
    elif case == "missing_id":
        calls = [{"name": "lookup"}]
    elif case == "invalid_calls":
        calls = {"id": "call"}
    messages = [message(1, "user", "问"), message(2, "assistant", "", {"tool_calls": calls})]
    if case == "interleaved":
        messages.append(message(3, "assistant", "不能穿插在工具请求和结果之间"))
    messages.extend([message(4, "tool", "结果", {"call_id": "call"}), message(5, "user", "新问")])
    with pytest.raises(ContextError) as error:
        ContextEngine().prepare(messages, ContextOptions(max_messages=2))
    assert error.value.code == "context_tool_group"


async def test_injected_summary_must_fit_budget_with_latest_complete_turn():
    with pytest.raises(ContextError) as error:
        await ContextEngine().prepare_async(
            history(),
            ContextOptions(strategy="annotated_compact", max_messages=2),
            lambda _messages: "摘要",
        )
    assert error.value.code == "context_budget"
    with pytest.raises(ContextError) as error:
        await ContextEngine().prepare_async(
            history(),
            ContextOptions(strategy="annotated_compact", max_chars=200),
            lambda _messages: "x" * 500,
        )
    assert error.value.code == "context_budget"


@pytest.mark.parametrize("strategy", ["truncate", "sliding_window", "annotated_compact"])
async def test_existing_summary_and_latest_tool_group_cannot_be_split_to_fit(strategy):
    messages = history()[3:7]
    with pytest.raises(ContextError) as error:
        await ContextEngine().prepare_async(
            messages,
            ContextOptions(strategy=strategy, max_messages=4),
            lambda _messages: "短摘要",
            existing_summary=("旧摘要", ["old-user", "old-assistant"]),
        )
    assert error.value.code == "context_budget"


async def test_summary_source_ids_include_previous_summary_without_duplicate_injection():
    seen = []

    def summarize(messages):
        seen.append(messages)
        return "更新后的摘要"

    result = await ContextEngine().prepare_async(
        history()[3:],
        ContextOptions(strategy="annotated_compact", max_messages=3),
        summarize,
        existing_summary=("旧摘要", ["m-1", "m-2", "m-3"]),
    )
    assert result.source_message_ids == [f"m-{index}" for index in range(1, 8)]
    assert result.original_count == 9 and result.omitted_count == 7
    assert seen[0][0].details["context_summary"]
    assert len(result.messages) == 3
    assert result.messages[0].details["source_message_ids"] == result.source_message_ids
    assert result.messages[0].role == "user" and "[不可信历史摘要]" in result.messages[0].content
    assert sum(bool(item.details.get("context_summary")) for item in result.messages) == 1


async def test_custom_token_counter_controls_final_budget_after_summary_injection():
    result = await ContextEngine(token_counter=lambda messages: 10 * len(messages)).prepare_async(
        history(),
        ContextOptions(strategy="annotated_compact", max_tokens=32),
        lambda _messages: "摘要",
    )
    assert len(result.messages) == 3 and result.estimated_tokens == 30


@pytest.mark.parametrize("count", [-1, True, "invalid"])
def test_invalid_token_counter_is_explicit_error(count):
    with pytest.raises(ContextError) as error:
        ContextEngine(token_counter=lambda _messages: count).prepare(history(), ContextOptions())
    assert error.value.code == "context_token_counter_invalid"


async def test_summary_callback_failure_has_stable_error_without_callback_body():
    def broken(_messages):
        raise RuntimeError("secret-callback-body")

    with pytest.raises(ContextError) as error:
        await ContextEngine().prepare_async(
            history(), ContextOptions(max_messages=3, strategy="annotated_compact"), broken
        )
    assert error.value.code == "context_summary_failed"
    assert "secret-callback-body" not in str(error.value)


async def test_runtime_budget_error_is_saved_without_invoking_provider(repository):
    recorder = RecordingProvider()
    runtime = ChatRuntime(repository, recorder, ToolRegistry(), Settings(context_max_chars=128))
    try:
        topic = await repository.create_topic("预算失败")
        run = await runtime.start(topic.id, ChatRequest(text="x" * 200, idempotency_key="budget"))
        state = await runtime.stream_state(run.id, 0)
        await state.task
        saved = await repository.get_run(run.id)
        assert saved.status == "failed" and saved.error_code == "context_budget"
        assert not recorder.contexts
        assert (await repository.list_events(run.id))[-1].data["code"] == "context_budget"
    finally:
        await runtime.close()
