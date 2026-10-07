"""后台话题多路读取、独立游标、背压与公开 client/CLI 合约。"""

import asyncio
from uuid import uuid4

import httpx
import pytest

from krim_agent.application.streams import MultiplexReader
from krim_agent.client import KrimClient
from krim_agent.config import Settings
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.streams import MultiplexRequest, RunCursor
from krim_agent.tools import ToolRegistry


async def finish(runtime, run):
    state = await runtime.stream_state(run.id, 0)
    await state.task


async def test_two_topics_keep_independent_seq_and_topic_association(repository):
    runtime = ChatRuntime(repository, FakeProvider(delay=0), ToolRegistry(), Settings())
    try:
        topics = [await repository.create_topic(f"后台 {index}") for index in range(2)]
        runs = [
            await runtime.start(
                topic.id, ChatRequest(text=f"消息 {index}", idempotency_key=str(index))
            )
            for index, topic in enumerate(topics)
        ]
        await asyncio.gather(*(finish(runtime, run) for run in runs))
        request = MultiplexRequest(
            sources=[RunCursor(run_id=run.id, topic_id=run.topic_id) for run in runs]
        )
        reader = MultiplexReader(runtime, queue_size=2)
        prepared = await reader.prepare(request)
        values = [
            item async for item in reader.subscribe(prepared, heartbeat=0.01) if item is not None
        ]
        assert {item.frame.stream_id for item in values} == {run.id for run in runs}
        for run in runs:
            source = [item for item in values if item.frame.stream_id == run.id]
            assert [item.frame.seq for item in source] == list(range(len(source)))
            assert all(item.topic_id == run.topic_id for item in source)
            assert source[-1].frame.kind == "end"
    finally:
        await runtime.close()


async def test_cursor_resume_and_invalid_sources(repository):
    runtime = ChatRuntime(repository, FakeProvider(delay=0), ToolRegistry(), Settings())
    try:
        topic = await repository.create_topic("游标")
        run = await runtime.start(topic.id, ChatRequest(text="消息", idempotency_key="one"))
        await finish(runtime, run)
        reader = MultiplexReader(runtime)
        first = await reader.prepare(
            MultiplexRequest(sources=[RunCursor(run_id=run.id, topic_id=topic.id, next_seq=0)])
        )
        all_frames = [item async for item in reader.subscribe(first) if item]
        cursor = all_frames[len(all_frames) // 2].frame.seq + 1
        resumed = await reader.prepare(
            MultiplexRequest(sources=[RunCursor(run_id=run.id, topic_id=topic.id, next_seq=cursor)])
        )
        values = [item async for item in reader.subscribe(resumed) if item]
        assert values and values[0].frame.seq == cursor and values[-1].frame.kind == "end"
        with pytest.raises(ServiceError, match="话题"):
            await reader.prepare(
                MultiplexRequest(sources=[RunCursor(run_id=run.id, topic_id=str(uuid4()))])
            )
        with pytest.raises(ServiceError, match="游标"):
            await reader.prepare(
                MultiplexRequest(
                    sources=[RunCursor(run_id=run.id, topic_id=topic.id, next_seq=999)]
                )
            )
        with pytest.raises(ValueError, match="重复"):
            MultiplexRequest(
                sources=[
                    RunCursor(run_id=run.id, topic_id=topic.id),
                    RunCursor(run_id=run.id, topic_id=topic.id),
                ]
            )
    finally:
        await runtime.close()


async def test_disconnect_reader_does_not_stop_live_production(repository):
    class SlowProvider(FakeProvider):
        async def stream(self, messages, tools):
            await asyncio.sleep(0.1)
            async for item in super().stream(messages, tools):
                yield item

    runtime = ChatRuntime(repository, SlowProvider(delay=0.05), ToolRegistry(), Settings())
    try:
        topic = await repository.create_topic("断开")
        run = await runtime.start(topic.id, ChatRequest(text="后台", idempotency_key="one"))
        reader = MultiplexReader(runtime)
        prepared = await reader.prepare(
            MultiplexRequest(sources=[RunCursor(run_id=run.id, topic_id=topic.id)])
        )
        subscription = reader.subscribe(prepared)
        first = await anext(subscription)
        assert first is not None
        await subscription.aclose()
        await asyncio.wait_for(runtime.production_task(run.id), 10)
        assert (await repository.get_run(run.id)).status == "completed"
    finally:
        await runtime.close()


async def test_http_client_and_cli_read_two_run_multiplex_over_real_tcp(server_url, repository):
    topics = [await repository.create_topic(f"TCP 多路 {index}") for index in range(2)]
    # 通过公开 HTTP 创建两个运行；server_url fixture 使用同一持久库。
    async with httpx.AsyncClient(base_url=server_url, timeout=30, trust_env=False) as api:
        runs = []
        for index, topic in enumerate(topics):
            response = await api.post(
                f"/v1/topics/{topic.id}/messages",
                json={"text": f"m{index}", "idempotency_key": str(index)},
            )
            runs.append(response.json())
        for run in runs:
            response = await api.get(f"/v1/runs/{run['id']}/events")
            assert response.status_code == 200 and '"status":"completed"' in response.text
    request = MultiplexRequest(
        sources=[RunCursor(run_id=run["id"], topic_id=run["topic_id"]) for run in runs]
    )
    with KrimClient(server_url) as client:
        values = await asyncio.to_thread(lambda: list(client.multiplex_events(request)))
        assert {value.frame.stream_id for value in values if value} == {run["id"] for run in runs}
        assert all(
            value is None or value.frame.kind in {"start", "chunk", "end", "error"}
            for value in values
        )


async def test_active_run_catalog_is_read_only_and_does_not_create_event_facts(
    api, runtime, repository
):
    from krim_agent.providers import ModelText

    class Paused:
        name = "catalog-paused"
        entered = asyncio.Event()

        async def stream(self, _messages, _tools):
            self.entered.set()
            await asyncio.Event().wait()
            yield ModelText(text="finished")

    provider = Paused()
    runtime.provider = provider
    topic = await repository.create_topic("活动目录")
    run = await runtime.start(topic.id, ChatRequest(text="waiting", idempotency_key="active"))
    await asyncio.wait_for(provider.entered.wait(), 5)
    before = await repository.list_events(run.id)
    response = await api.get("/v1/runs/active")
    assert response.status_code == 200
    assert response.json()[0]["run"]["id"] == run.id
    assert response.json()[0]["events"]["count"] == len(before)
    assert await repository.list_events(run.id) == before
    await runtime.stop(run.id)
    assert (await api.get("/v1/runs/active")).json() == []


async def test_invalid_cold_cursor_never_repairs_existing_events(repository):
    from sqlalchemy import update

    from krim_agent.persistence.database import RunRow
    from krim_agent.protocol.models import StreamFrame

    topic = await repository.create_topic("缺少终帧")
    run, _ = await repository.begin_run(topic.id, ChatRequest(text="x", idempotency_key="cold"))
    await repository.append_event(StreamFrame(stream_id=run.id, seq=0, kind="start"))
    async with repository.database.sessions.begin() as session:
        await session.execute(update(RunRow).where(RunRow.id == run.id).values(status="completed"))
    runtime = ChatRuntime(repository, FakeProvider(delay=0), ToolRegistry(), Settings())
    try:
        before = await repository.list_events(run.id)
        with pytest.raises(ServiceError) as error:
            await MultiplexReader(runtime).prepare(
                MultiplexRequest(sources=[RunCursor(run_id=run.id, topic_id=topic.id, next_seq=2)])
            )
        assert error.value.code == "invalid_cursor"
        assert await repository.list_events(run.id) == before
        assert runtime.production_task(run.id) is None
    finally:
        await runtime.close()


async def test_replay_from_other_runtime_zero_calls_and_zero_event_writes(runtime, repository):
    from krim_agent.providers import ModelText

    topic = await repository.create_topic("只读")
    run = await runtime.start(topic.id, ChatRequest(text="/add 2 3", idempotency_key="one"))
    await finish(runtime, run)
    before, audits = await repository.list_events(run.id), await repository.audits(run.id)

    class Never:
        name = "never"

        async def stream(self, _messages, _tools):
            raise AssertionError("读取不能重新生产")
            yield ModelText(text="never")

    cold = ChatRuntime(repository, Never(), ToolRegistry(), Settings())
    try:
        reader = MultiplexReader(cold, queue_size=1)
        prepared = await reader.prepare(
            MultiplexRequest(sources=[RunCursor(run_id=run.id, topic_id=topic.id)])
        )
        values = [frame async for frame in reader.subscribe(prepared) if frame]
        assert [frame.frame for frame in values] == before
        assert await repository.audits(run.id) == audits
        assert await repository.list_events(run.id) == before
        assert cold.production_task(run.id) is None
    finally:
        await cold.close()


async def test_reader_close_drains_children_under_asgi_cancel_scope(runtime, repository):
    from anyio import create_task_group

    topic = await repository.create_topic("取消")
    run = await runtime.start(topic.id, ChatRequest(text="较长后台输出", idempotency_key="one"))
    reader = MultiplexReader(runtime, queue_size=1)
    prepared = await reader.prepare(
        MultiplexRequest(sources=[RunCursor(run_id=run.id, topic_id=topic.id)])
    )
    entered = asyncio.Event()

    async def read():
        subscription = reader.subscribe(prepared)
        try:
            await anext(subscription)
            entered.set()
            await asyncio.Event().wait()
        finally:
            await subscription.aclose()

    async with create_task_group() as group:
        group.start_soon(read)
        await entered.wait()
        group.cancel_scope.cancel()
    assert not any(t.get_name() == f"krim-subscribe-{run.id}" for t in asyncio.all_tasks())
    await runtime.production_task(run.id)
    assert (await repository.get_run(run.id)).status == "completed"
