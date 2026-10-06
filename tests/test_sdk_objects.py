"""结构化 SDK 真生成、严格合约、取消、幂等与原始运行证据。"""

import asyncio

import pytest
from pydantic import Field

from krim_agent.protocol.models import StrictModel
from krim_agent.providers import ModelText
from krim_agent.sdk import PythonAI, SDKOutputError


class Answer(StrictModel):
    number: int = Field(ge=0)


class JsonProvider:
    name = "object-json"

    def __init__(self, value):
        self.value, self.calls = value, 0

    async def stream(self, messages, _tools):
        self.calls += 1
        assert "JSON Schema" in messages[-1].content
        yield ModelText(text=self.value)


async def test_structured_generation_uses_declared_schema_and_keeps_raw_run_and_idempotency():
    provider = JsonProvider('{"number":3}')
    async with await PythonAI.create(provider=provider) as ai:
        topic = await ai.runtime.repository.create_topic("objects")
        result = await ai.generate_object(
            "返回数字", schema=Answer, topic_id=topic.id, idempotency_key="one"
        )
        assert result.value == {"number": 3} and result.generation.run.status == "completed"
        assert len(result.schema_fingerprint) == 64 and provider.calls == 1
        again = await ai.generate_object(
            "返回数字", schema=Answer, topic_id=topic.id, idempotency_key="one"
        )
        assert again == result and provider.calls == 1
        assert (await ai.read_result(result.generation.run.id)).text == '{"number":3}'


@pytest.mark.parametrize(
    "raw",
    [
        '{"number":"3"}',
        '{"number":3,"number":4}',
        "NaN",
        '{"number":-1}',
        '```json\n{"number":3}\n```',
    ],
)
async def test_structured_failure_keeps_generation_and_does_not_auto_retry(raw):
    provider = JsonProvider(raw)
    async with await PythonAI.create(provider=provider) as ai:
        with pytest.raises(SDKOutputError) as error:
            await ai.generate_object("返回数字", schema=Answer)
        result = error.value.result
        assert result is not None and result.run.status == "completed" and result.text == raw
        assert provider.calls == 1


async def test_invalid_remote_schema_is_rejected_before_any_generation():
    provider = JsonProvider('{"number":3}')
    async with await PythonAI.create(provider=provider) as ai:
        with pytest.raises(ValueError):
            await ai.generate_object("返回数字", schema={"$ref": "https://invalid.example/schema"})
        assert provider.calls == 0


async def test_cancel_structured_generation_stops_real_production_and_retains_partial():
    entered, closed = asyncio.Event(), asyncio.Event()

    class Partial:
        name = "object-partial"

        async def stream(self, _messages, _tools):
            yield ModelText(text='{"number":')
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                closed.set()

    async with await PythonAI.create(provider=Partial()) as ai:
        topic = await ai.runtime.repository.create_topic("partial")
        task = asyncio.create_task(ai.generate_object("返回数字", schema=Answer, topic_id=topic.id))
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert closed.is_set()
        rows = await ai.runtime.repository.list_messages(topic.id)
        assert any(m.role == "assistant" and m.content == '{"number":' for m in rows)
