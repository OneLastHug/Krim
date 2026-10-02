import asyncio
from collections.abc import AsyncIterator

import pytest

from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.providers import FakeProvider, ModelText
from krim_agent.runtime import ChatRuntime
from krim_agent.tools import ToolRegistry, tool
from krim_agent.tools.builtins import builtin_registry


async def finish(runtime, run):
    state = await runtime.stream_state(run.id, 0)
    return [frame async for frame in runtime.subscribe(state) if frame is not None]


async def test_full_loop_multiturn_and_audit(runtime, repository):
    topic = await repository.create_topic("算术")
    first = await runtime.start(topic.id, ChatRequest(text="/add 2 3", idempotency_key="one"))
    frames = await finish(runtime, first)
    assert frames[0].kind == "start" and frames[-1].data["status"] == "completed"
    assert [frame.seq for frame in frames] == list(range(len(frames)))
    messages = await repository.list_messages(topic.id)
    assert [message.role for message in messages] == ["user", "assistant", "tool", "assistant"]
    assert "5.0" in messages[-1].content and "fake" in messages[-1].content
    audits = await repository.audits(first.id)
    assert len(audits) == 1 and audits[0].name == "add" and audits[0].ok
    assert audits[0].call_id == messages[2].details["call_id"]
    second = await runtime.start(topic.id, ChatRequest(text="/echo 新一轮", idempotency_key="two"))
    await finish(runtime, second)
    history = await repository.list_messages(topic.id)
    assert "新一轮" in history[-1].content and "5.0" not in history[-1].content


async def test_idempotency_conflict_and_no_duplicate_tools(runtime, repository):
    topic = await repository.create_topic("幂等")
    request = ChatRequest(text="/add 1 2", idempotency_key="same")
    runs = await asyncio.gather(*[runtime.start(topic.id, request) for _index in range(4)])
    assert len({run.id for run in runs}) == 1
    await finish(runtime, runs[0])
    again = await runtime.start(topic.id, request)
    assert again.status == "completed" and again.id == runs[0].id
    assert len(await repository.audits(again.id)) == 1
    with pytest.raises(ServiceError) as error:
        await runtime.start(topic.id, ChatRequest(text="different", idempotency_key="same"))
    assert error.value.code == "idempotency_conflict"


class PausedProvider:
    name = "test-paused"

    def __init__(self):
        self.entered = asyncio.Event()

    async def stream(self, _messages, _tools) -> AsyncIterator[ModelText]:
        yield ModelText(text="部分输出")
        self.entered.set()
        await asyncio.Event().wait()


async def test_topic_busy_stop_and_partial_persistence(repository):
    provider = PausedProvider()
    runtime = ChatRuntime(repository, provider, ToolRegistry(), Settings())
    topic = await repository.create_topic("取消")
    run = await runtime.start(topic.id, ChatRequest(text="hello", idempotency_key="1"))
    await asyncio.wait_for(provider.entered.wait(), 1)
    with pytest.raises(ServiceError) as error:
        await runtime.start(topic.id, ChatRequest(text="second", idempotency_key="2"))
    assert error.value.code == "topic_busy"
    results = await asyncio.gather(runtime.stop(run.id), runtime.stop(run.id))
    assert all(result.status == "aborted" and result.stop_requested for result in results)
    history = await repository.list_messages(topic.id)
    assert history[-1].content == "部分输出" and history[-1].details["partial"]
    assert (await finish(runtime, run))[-1].data["status"] == "aborted"
    await runtime.close()


async def test_disconnect_does_not_cancel_producer_and_replay(repository):
    runtime = ChatRuntime(repository, FakeProvider(delay=0.01), ToolRegistry(), Settings())
    topic = await repository.create_topic("断开")
    run = await runtime.start(topic.id, ChatRequest(text="x" * 64, idempotency_key="1"))
    state = await runtime.stream_state(run.id, 0)
    subscription = runtime.subscribe(state)
    first = await anext(subscription)
    await subscription.aclose()
    assert first.seq == 0 and state.task is not None
    await state.task
    assert (await repository.get_run(run.id)).status == "completed"
    replay = [frame async for frame in runtime.subscribe(state, 2)]
    assert replay[0].seq == 2 and replay[-1].kind == "end"
    await runtime.close()


async def test_tool_cancel_persists_result_before_stop_returns(repository):
    entered = asyncio.Event()
    cleanup = asyncio.Event()

    @tool(name="echo", risk="read")
    async def delayed(text: str) -> str:
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleanup.set()
        return text

    tools = ToolRegistry()
    tools.register(delayed)
    runtime = ChatRuntime(repository, FakeProvider(), tools, Settings())
    topic = await repository.create_topic("停止工具")
    run = await runtime.start(topic.id, ChatRequest(text="/echo wait", idempotency_key="1"))
    await asyncio.wait_for(entered.wait(), 1)
    result = await runtime.stop(run.id)
    history = await repository.list_messages(topic.id)
    assert result.status == "aborted" and cleanup.is_set()
    assert history[-1].role == "tool" and history[-1].details["code"] == "cancelled"
    audits = await repository.audits(run.id)
    assert audits[-1].code == "cancelled" and not audits[-1].ok
    await runtime.close()


async def test_timeout_limits_and_recovery(repository):
    runtime = ChatRuntime(
        repository, PausedProvider(), ToolRegistry(), Settings(run_timeout_seconds=0.01)
    )
    topic = await repository.create_topic("超时")
    run = await runtime.start(topic.id, ChatRequest(text="hello", idempotency_key="1"))
    assert (await finish(runtime, run))[-1].data["status"] == "failed"
    before = len(await repository.list_messages(topic.id))
    abandoned, _created = await repository.begin_run(
        topic.id, ChatRequest(text="recovery", idempotency_key="2")
    )
    await repository.recover_interrupted()
    recovered = await repository.get_run(abandoned.id)
    assert recovered.status == "interrupted" and recovered.finished_at is not None
    assert len(await repository.list_messages(topic.id)) == before + 1
    await runtime.close()


async def test_capacity_denial_does_not_create_a_run(repository):
    provider = PausedProvider()
    runtime = ChatRuntime(repository, provider, ToolRegistry(), Settings(max_active_runs=1))
    first_topic = await repository.create_topic("one")
    second_topic = await repository.create_topic("two")
    request = ChatRequest(text="hello", idempotency_key="same")
    run = await runtime.start(first_topic.id, request)
    assert (await runtime.start(first_topic.id, request)).id == run.id
    with pytest.raises(ServiceError) as error:
        await runtime.start(second_topic.id, request)
    assert error.value.code == "run_limit"
    assert not await repository.list_messages(second_topic.id)
    await runtime.close()


async def test_memory_sqlite_parallel_runs_are_not_rolled_back_by_reads():
    database = Database("sqlite+aiosqlite:///:memory:")
    await database.initialize()
    repository = KrimRepository(database)
    runtime = ChatRuntime(repository, FakeProvider(delay=0), builtin_registry(), Settings())
    try:
        topics = [await repository.create_topic(str(index)) for index in range(4)]
        runs = await asyncio.gather(
            *[
                runtime.start(topic.id, ChatRequest(text="/add 2 3", idempotency_key="one"))
                for topic in topics
            ]
        )
        await asyncio.gather(*[finish(runtime, run) for run in runs])
        for run in runs:
            assert (await repository.get_run(run.id)).status == "completed"
            assert len(await repository.audits(run.id)) == 1
            assert "5.0" in (await repository.list_messages(run.topic_id))[-1].content
    finally:
        await runtime.close()
        await database.close()
