import asyncio
import json

import pytest
from pydantic import BaseModel, Field, ValidationError

from krim_agent.config import Settings
from krim_agent.protocol.domain import ChatRequest, EditRequest, ServiceError
from krim_agent.providers import FakeProvider, ModelText, ModelToolCall
from krim_agent.sdk import (
    PythonAI,
    SDKOutputError,
    StepFinish,
    StreamFinish,
    StreamStart,
    ToolCallPart,
    ToolResultPart,
)
from krim_agent.tools import ToolRegistry, tool


async def test_text_full_collect_and_replay_execute_tools_once(runtime, repository):
    ai = PythonAI(runtime)
    stream = await ai.stream_text("/add 2 3", idempotency_key="sdk-once")

    parts = [part async for part in stream.full_stream()]
    text_value = "".join([part async for part in stream.text_stream()])
    result = await stream.collect()
    assert isinstance(parts[0], StreamStart) and isinstance(parts[-1], StreamFinish)
    assert [part.tool_name for part in parts if isinstance(part, ToolCallPart)] == ["add"]
    assert [part.result.data for part in parts if isinstance(part, ToolResultPart)] == [
        {"sum": 5.0}
    ]
    assert [part.finish_reason for part in parts if isinstance(part, StepFinish)] == [
        "tool-calls",
        "stop",
    ]
    assert result.text == text_value and "5.0" in result.text
    assert result.finish_reason == "stop" and result.token_usage is None
    assert len(result.steps) == 2 and result.steps[0].tool_results[0].data == {"sum": 5.0}
    assert len(result.audits) == 1 and result.run.status == "completed"
    assert [part async for part in stream.full_stream()] == parts
    repeat = await ai.stream_text(
        "/add 2 3", topic_id=result.run.topic_id, idempotency_key="sdk-once"
    )
    assert repeat.run.id == result.run.id
    assert (await repeat.collect()).text == result.text
    assert len(await repository.audits(stream.run.id)) == 1
    await ai.close()
    # 借用型 SDK 关闭后，用户拥有的运行时仍可接收下一轮。
    assert (
        await runtime.start(result.run.topic_id, ChatRequest(text="继续", idempotency_key="next"))
    ).id


async def test_collect_reads_original_run_after_branch_switch(runtime, repository):
    ai = PythonAI(runtime)
    original = await ai.stream_text("/echo 原 SDK 分支", idempotency_key="original")
    first = await original.collect()
    edited = await runtime.edit(
        first.run.topic_id,
        first.run.input_message_id,
        EditRequest(text="/echo 新分支", idempotency_key="edited"),
    )
    state = await runtime.stream_state(edited.id, 0)
    await state.task
    assert "新分支" in (await repository.list_messages(first.run.topic_id))[-1].content
    restored = await original.collect()
    assert restored.text == first.text and "新分支" not in restored.text
    assert restored.steps == first.steps and len(restored.audits) == 1


class PausedProvider:
    name = "sdk-paused"

    async def stream(self, _messages, _tools):
        yield ModelText(text="部分 SDK 输出")
        await asyncio.Event().wait()


async def test_close_subscription_does_not_stop_producer_and_abort_is_scoped():
    async with await PythonAI.create(provider=PausedProvider()) as ai:
        first = await ai.stream_text("first")
        second = await ai.stream_text("second")
        subscription = first.text_stream()
        assert await anext(subscription) == "部分 SDK 输出"
        await subscription.aclose()
        assert (await ai.runtime.repository.get_run(first.run.id)).status == "running"
        assert (await first.abort()).status == "aborted"
        result = await first.collect()
        assert result.finish_reason == "abort" and result.code == "cancelled"
        assert result.steps[-1].partial and result.text == "部分 SDK 输出"
        assert (await ai.runtime.repository.get_run(second.run.id)).status == "running"
        await second.abort()


async def test_step_budget_produces_length_and_owned_close_releases_active_runs():
    ai = await PythonAI.create(
        settings=Settings(database_url="sqlite+aiosqlite:///:memory:", max_steps=1)
    )
    result = await ai.generate_text("/add 1 2")
    assert result.finish_reason == "length" and result.code == "step_limit"
    assert result.run.error_code == "step_limit"
    assert result.steps[0].tool_results[0].data == {"sum": 3.0}
    await ai.close()
    assert ai.runtime._closed
    with pytest.raises(ServiceError) as error:
        await ai.stream_text("closed")
    assert error.value.code == "sdk_closed"
    await ai.close()


class OutputModel(BaseModel):
    value: int = Field(ge=0)


class FloatOutput(BaseModel):
    value: float


class JSONProvider:
    name = "sdk-json"

    async def stream(self, messages, _tools):
        yield ModelText(text=messages[-1].content)


async def test_structured_output_is_strict_and_does_not_pretend_provider_support():
    async with await PythonAI.create(provider=JSONProvider()) as ai:
        valid = await ai.stream_text(json.dumps({"value": 3}))
        assert (await valid.output(OutputModel)).value == 3
        for text in ("not JSON", '{"value":"3"}', '{"value":-1}'):
            invalid = await ai.stream_text(text)
            with pytest.raises(SDKOutputError):
                await invalid.output(OutputModel)
        nonfinite = await ai.stream_text('{"value":NaN}')
        with pytest.raises(SDKOutputError):
            await nonfinite.output(FloatOutput)
        with pytest.raises(ValidationError):
            await ai.stream_text("   ")


async def test_sdk_keeps_default_write_denial_and_tool_result_audit():
    called = []

    @tool(name="write_demo")
    async def write_demo() -> str:
        called.append(True)
        return "written"

    class ToolProvider:
        name = "sdk-tool"

        async def stream(self, messages, _tools):
            if messages[-1].role == "tool":
                yield ModelText(text="裁决已返回")
            else:
                yield ModelToolCall(name="write_demo", arguments={})

    registry = ToolRegistry()
    registry.register(write_demo)
    async with await PythonAI.create(provider=ToolProvider(), tools=registry) as ai:
        result = await ai.generate_text("write")
        assert not called
        assert result.steps[0].tool_results[0].code == "permission_denied"
        assert result.audits[0].decision == "deny" and not result.audits[0].ok


async def test_no_output_is_error_and_timeout_keeps_stable_reason():
    class EmptyProvider:
        name = "empty"

        async def stream(self, _messages, _tools):
            if False:
                yield ModelText(text="never")

    async with await PythonAI.create(provider=EmptyProvider()) as ai:
        result = await ai.generate_text("empty")
        assert result.finish_reason == "error" and result.code == "no_output"
        assert not result.steps and not result.text
    async with await PythonAI.create(
        provider=PausedProvider(),
        settings=Settings(database_url="sqlite+aiosqlite:///:memory:", run_timeout_seconds=1),
    ) as ai:
        stream = await ai.stream_text("timeout")
        # 先证明模型已经给出部分输出，再验证实际总超时保留该输出。
        # 10 ms 可能在上下文准备阶段耗尽，不能据此要求存在模型步骤。
        subscription = stream.text_stream()
        assert await anext(subscription) == "部分 SDK 输出"
        await subscription.aclose()
        result = await stream.collect()
        assert result.finish_reason == "error" and result.code == "run_timeout"
        assert result.steps[-1].partial and result.text == "部分 SDK 输出"


async def test_sdk_finish_reason_and_step_metadata_survive_database_reopen(tmp_path):
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path / 'sdk.sqlite'}", max_steps=1)
    ai = await PythonAI.create(settings=settings, provider=FakeProvider(delay=0))
    result = await ai.generate_text("/add 2 3")
    run_id = result.run.id
    await ai.close()
    reopened = await PythonAI.create(settings=settings)
    try:
        from krim_agent.sdk import StreamTextResult

        restored = StreamTextResult(
            reopened.runtime, await reopened.runtime.repository.get_run(run_id)
        )
        recovered = await restored.collect()
        assert recovered.finish_reason == "length" and recovered.code == "step_limit"
        assert recovered.steps == result.steps and recovered.audits == result.audits
    finally:
        await reopened.close()


async def test_full_text_and_collect_subscribe_concurrently_to_one_producer():
    entered, release = asyncio.Event(), asyncio.Event()

    class LiveProvider:
        name = "sdk-live"

        async def stream(self, _messages, _tools):
            yield ModelText(text="FIRST")
            entered.set()
            await release.wait()
            yield ModelText(text="SECOND")

    async with await PythonAI.create(provider=LiveProvider()) as ai:
        stream = await ai.stream_text("parallel")

        async def full():
            return [part async for part in stream.full_stream()]

        async def text():
            return "".join([part async for part in stream.text_stream()])

        readers = [
            asyncio.create_task(full()),
            asyncio.create_task(text()),
            asyncio.create_task(stream.collect()),
        ]
        try:
            await asyncio.wait_for(entered.wait(), 3)
            await asyncio.sleep(0.01)
            assert all(not reader.done() for reader in readers)
            release.set()
            parts, text_value, result = await asyncio.gather(*readers)
            assert isinstance(parts[-1], StreamFinish)
            assert text_value == "FIRSTSECOND" and result.text == text_value
            assert result.run.status == "completed"
        finally:
            release.set()


async def test_final_text_excludes_intermediate_tool_step_for_structured_output():
    class ExplainedProvider:
        name = "sdk-explained"

        async def stream(self, messages, _tools):
            if messages[-1].role == "tool":
                yield ModelText(text='{"value":3}')
            else:
                yield ModelText(text="正在计算")
                yield ModelToolCall(name="add", arguments={"left": 1, "right": 2})

    async with await PythonAI.create(provider=ExplainedProvider()) as ai:
        stream = await ai.stream_text("calculate")
        result = await stream.collect()
        assert result.text == '{"value":3}'
        assert result.all_text == '正在计算{"value":3}'
        assert "".join([part async for part in stream.text_stream()]) == result.all_text
        assert (await stream.output(OutputModel)).value == 3
        assert result.steps[0].text == "正在计算" and len(result.steps) == 2
