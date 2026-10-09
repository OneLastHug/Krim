"""重复取消必须等待同一事件提交和本地序号收束。"""

import asyncio

import pytest

from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.protocol.domain import ChatRequest
from krim_agent.providers import ModelText
from krim_agent.runtime import ChatRuntime
from krim_agent.runtime.chat import RunState
from krim_agent.tools import ToolRegistry


@pytest.mark.parametrize("phase", ["before-commit", "after-commit", "before-notify"])
async def test_repeated_cancellation_keeps_inflight_event_and_next_sequence(repository, phase):
    topic = await repository.create_topic("重复取消发布")
    run, _ = await repository.begin_run(
        topic.id, ChatRequest(text="original", idempotency_key="run")
    )
    await repository.set_running(run.id)
    entered, release = asyncio.Event(), asyncio.Event()
    commits = []

    async def writer(frame):
        commits.append(asyncio.current_task())
        if phase == "before-commit":
            entered.set()
            await release.wait()
        stored = await repository.append_event(frame)
        if phase != "before-commit":
            entered.set()
        if phase == "after-commit":
            await release.wait()
        return stored

    state = RunState(run, event_writer=writer)
    if phase == "before-notify":
        await state.condition.acquire()
    publishing = asyncio.create_task(state.publish("start", {"provider": "publication-test"}))
    try:
        await asyncio.wait_for(entered.wait(), 10)
        publishing.cancel()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        publishing.cancel()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert not publishing.done(), "重复取消不应丢弃仍在提交的原帧"
        assert len(commits) == 1 and not commits[0].cancelled()
        release.set()
        if phase == "before-notify":
            state.condition.release()
        with pytest.raises(asyncio.CancelledError):
            await publishing
        assert len(state.frames) == 1 and state.frames[0].seq == 0
        assert await repository.list_events(run.id) == state.frames
        state.event_writer = repository.append_event
        await state.publish("chunk", {"type": "text_delta", "text": "next"})
        await repository.finish(run.id, "completed", None, [])
        await state.publish("end", {"status": "completed"})
        assert [frame.seq for frame in state.frames] == [0, 1, 2]
        database = Database(str(repository.database.engine.url))
        try:
            await database.initialize()
            assert await KrimRepository(database).list_events(run.id) == state.frames
        finally:
            await database.close()
    finally:
        release.set()
        if phase == "before-notify" and state.condition.locked():
            state.condition.release()
        await asyncio.gather(publishing, *commits, return_exceptions=True)


async def test_run_stop_with_a_second_cancel_preserves_durable_text_and_single_terminal(repository):
    entered, release, stop_requested = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = repository.append_event
    original_stop = repository.request_stop

    async def observed_stop(run_id):
        await original_stop(run_id)
        stop_requested.set()

    async def paused(frame):
        stored = await original(frame)
        if isinstance(frame.data, dict) and frame.data.get("type") == "text_delta":
            entered.set()
            await release.wait()
        return stored

    class Provider:
        name = "repeat-cancel"
        calls = 0

        async def stream(self, _messages, _tools):
            self.calls += 1
            yield ModelText(text="只产生一次的部分文本")
            await asyncio.Event().wait()

    repository.append_event = paused
    repository.request_stop = observed_stop
    provider = Provider()
    runtime = ChatRuntime(repository, provider, ToolRegistry(), Settings())
    stopping = None
    try:
        topic = await repository.create_topic("停止与再次取消")
        run = await runtime.start(topic.id, ChatRequest(text="wait", idempotency_key="run"))
        await asyncio.wait_for(entered.wait(), 10)
        stopping = asyncio.create_task(runtime.stop(run.id))
        # 等待原stop确实发出首次取消，再模拟同时到达的上层/超时取消。
        production = runtime.production_task(run.id)
        assert production is not None
        await asyncio.wait_for(stop_requested.wait(), 10)
        await asyncio.sleep(0)
        assert production.cancelling()
        production.cancel()
        await asyncio.sleep(0)
        release.set()
        assert (await asyncio.wait_for(stopping, 10)).status == "aborted"
        state = await runtime.stream_state(run.id, 0)
        persisted = await repository.list_events(run.id)
        assert state.frames == persisted
        assert [frame.seq for frame in persisted] == list(range(len(persisted)))
        assert sum(frame.kind == "end" for frame in persisted) == 1
        assert (
            sum(
                isinstance(frame.data, dict) and frame.data.get("type") == "text_delta"
                for frame in persisted
            )
            == 1
        )
        assert persisted[-1].data["status"] == "aborted"
        assert provider.calls == 1
        assert (await repository.list_messages(topic.id))[-1].content == "只产生一次的部分文本"
    finally:
        release.set()
        if stopping is not None:
            await asyncio.gather(stopping, return_exceptions=True)
        await runtime.close()
