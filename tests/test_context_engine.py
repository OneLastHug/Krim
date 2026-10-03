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
        assert result.messages[0].role == "system" and result.messages[0].details["context_summary"]


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
