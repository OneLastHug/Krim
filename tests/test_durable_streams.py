import asyncio
import json
import sys

import httpx
import pytest
from alembic import command

from krim_agent.api import create_app
from krim_agent.app import LocalApp
from krim_agent.client import KrimClient
from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.migration import migration_config
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.protocol.models import StreamFrame
from krim_agent.providers import FakeProvider, ModelText
from krim_agent.runtime import ChatRuntime
from krim_agent.tools import ToolRegistry


class NeverProvider:
    name = "must-not-run"

    async def stream(self, _messages, _tools):
        raise AssertionError("续传不得调用模型或重跑工具")
        yield ModelText(text="unreachable")


async def frames(runtime, run_id, cursor=0):
    state = await runtime.stream_state(run_id, cursor)
    return [frame async for frame in runtime.subscribe(state, cursor) if frame is not None]


async def test_events_survive_database_reopen_without_replaying_tools(runtime, repository):
    topic = await repository.create_topic("持久恢复")
    run = await runtime.start(topic.id, ChatRequest(text="/add 2 3", idempotency_key="once"))
    original = await frames(runtime, run.id)
    audits = await repository.audits(run.id)
    history = await repository.list_messages(topic.id)
    assert len(audits) == 1
    database = Database(str(repository.database.engine.url))
    await database.initialize()
    restored = KrimRepository(database)
    reader = ChatRuntime(restored, NeverProvider(), ToolRegistry(), Settings())
    try:
        replay = await frames(reader, run.id)
        assert replay == original
        assert [
            item async for item in reader.subscribe(await reader.stream_state(run.id, 2), 2)
        ] == original[2:]
        assert await frames(reader, run.id, len(original)) == []
        for cursor in (-1, len(original) + 1):
            with pytest.raises(ServiceError) as error:
                await reader.stream_state(run.id, cursor)
            assert error.value.code == "invalid_cursor"
        assert await restored.audits(run.id) == audits
        assert await restored.list_messages(topic.id) == history
        runtime._runs.pop(run.id)
        assert await frames(runtime, run.id) == original
        app = LocalApp(reader)
        try:
            assert [item async for item in app.handle.events(run.id, 2)] == original[2:]
        finally:
            await app.close()
    finally:
        await reader.close()
        await database.close()


async def test_restart_adds_one_interrupted_terminal_and_retains_partial_events(repository):
    topic = await repository.create_topic("模拟进程中断")
    run, _created = await repository.begin_run(
        topic.id, ChatRequest(text="start", idempotency_key="one")
    )
    original = [
        StreamFrame(stream_id=run.id, seq=0, kind="start", data={"provider": "test"}),
        StreamFrame(
            stream_id=run.id, seq=1, kind="chunk", data={"type": "text_delta", "text": "已发送部分"}
        ),
    ]
    for frame in original:
        await repository.append_event(frame)
    await repository.recover_interrupted()
    await repository.recover_interrupted()
    reader = ChatRuntime(repository, NeverProvider(), ToolRegistry(), Settings())
    try:
        replay = await frames(reader, run.id)
        assert replay[:2] == original and [item.seq for item in replay] == [0, 1, 2]
        assert replay[-1].data["status"] == "interrupted"
        assert len(await repository.run_messages(run.id)) == 1
        assert not await repository.audits(run.id)
    finally:
        await reader.close()


async def test_event_gaps_conflicts_and_post_terminal_writes_are_rejected(repository):
    topic = await repository.create_topic("序号约束")
    run, _created = await repository.begin_run(
        topic.id, ChatRequest(text="x", idempotency_key="one")
    )
    first = StreamFrame(stream_id=run.id, seq=0, kind="start")
    with pytest.raises(ServiceError) as error:
        await repository.append_event(StreamFrame(stream_id=run.id, seq=2, kind="chunk"))
    assert error.value.code == "event_gap"
    await repository.append_event(first)
    assert await repository.append_event(first) == first
    with pytest.raises(ServiceError) as error:
        await repository.append_event(first.model_copy(update={"data": {"changed": True}}))
    assert error.value.code == "event_conflict"
    with pytest.raises(ServiceError) as error:
        await repository.append_event(StreamFrame(stream_id=run.id, seq=1, kind="end"))
    assert error.value.code == "run_not_terminal"
    await repository.finish(run.id, "completed", None, [])
    terminal = StreamFrame(
        stream_id=run.id, seq=1, kind="end", data={"status": "completed", "error": None}
    )
    await repository.append_event(terminal)
    with pytest.raises(ServiceError) as error:
        await repository.append_event(StreamFrame(stream_id=run.id, seq=2, kind="chunk"))
    assert error.value.code == "event_after_terminal"
    assert (await repository.event_summary(run.id)).count == 2


async def test_cancel_during_committed_frame_waits_and_keeps_next_sequence(repository):
    entered, release = asyncio.Event(), asyncio.Event()
    original_writer = repository.append_event

    async def blocked_writer(frame):
        stored = await original_writer(frame)
        if (
            frame.kind == "chunk"
            and isinstance(frame.data, dict)
            and frame.data.get("type") == "text_delta"
        ):
            entered.set()
            await release.wait()
        return stored

    class PausedProvider:
        name = "cancel-frame"

        async def stream(self, _messages, _tools):
            yield ModelText(text="持久部分")
            await asyncio.Event().wait()

    repository.append_event = blocked_writer
    runtime = ChatRuntime(repository, PausedProvider(), ToolRegistry(), Settings())
    stopping = None
    try:
        topic = await repository.create_topic("发布取消")
        run = await runtime.start(topic.id, ChatRequest(text="wait", idempotency_key="one"))
        await asyncio.wait_for(entered.wait(), 3)
        stopping = asyncio.create_task(runtime.stop(run.id))
        await asyncio.sleep(0.01)
        assert not stopping.done()
        release.set()
        assert (await stopping).status == "aborted"
        replay = await frames(runtime, run.id)
        assert [item.seq for item in replay] == list(range(len(replay)))
        assert (
            len(
                [
                    item
                    for item in replay
                    if isinstance(item.data, dict) and item.data.get("type") == "text_delta"
                ]
            )
            == 1
        )
        assert replay[-1].data["status"] == "aborted"
        assert (await repository.list_events(run.id)) == replay
        assert (await repository.list_messages(topic.id))[-1].content == "持久部分"
    finally:
        release.set()
        await runtime.close()


async def test_terminal_repair_race_keeps_one_original_canonical_frame(repository):
    entered, release = asyncio.Event(), asyncio.Event()
    original_writer = repository.append_event

    async def blocked_terminal(frame):
        if frame.kind == "end":
            entered.set()
            await release.wait()
        return await original_writer(frame)

    repository.append_event = blocked_terminal
    runtime = ChatRuntime(repository, FakeProvider(delay=0), ToolRegistry(), Settings())
    reader = ChatRuntime(repository, NeverProvider(), ToolRegistry(), Settings())
    try:
        topic = await repository.create_topic("终帧竞态")
        run = await runtime.start(topic.id, ChatRequest(text="done", idempotency_key="one"))
        await asyncio.wait_for(entered.wait(), 3)
        recovered = await frames(reader, run.id)
        stopping = asyncio.create_task(runtime.stop(run.id))
        await asyncio.sleep(0.01)
        assert not stopping.done()
        release.set()
        await stopping
        produced = await frames(runtime, run.id)
        assert produced == recovered
        assert len([item for item in produced if item.kind == "end"]) == 1
    finally:
        release.set()
        await reader.close()
        await runtime.close()


async def test_readonly_live_subscription_uses_durable_updates_and_heartbeats(repository):
    entered, release = asyncio.Event(), asyncio.Event()

    class LiveProvider:
        name = "live-reader"

        async def stream(self, _messages, _tools):
            yield ModelText(text="FIRST")
            entered.set()
            await release.wait()
            yield ModelText(text="SECOND")

    writer = ChatRuntime(repository, LiveProvider(), ToolRegistry(), Settings())
    reader = ChatRuntime(repository, NeverProvider(), ToolRegistry(), Settings())
    try:
        topic = await repository.create_topic("只读实时")
        run = await writer.start(topic.id, ChatRequest(text="live", idempotency_key="one"))
        await asyncio.wait_for(entered.wait(), 3)
        state = await reader.stream_state(run.id, 0)
        subscription = reader.subscribe(state, heartbeat=0.02)
        seen = []
        while (
            not seen
            or not isinstance(seen[-1].data, dict)
            or seen[-1].data.get("type") != "text_delta"
        ):
            seen.append(await anext(subscription))
        assert seen[-1].data["text"] == "FIRST"
        assert await asyncio.wait_for(anext(subscription), 2) is None
        release.set()
        seen.extend([item async for item in subscription if item is not None])
        assert seen[-1].data["status"] == "completed"
        assert [item.seq for item in seen] == list(range(len(seen)))
    finally:
        release.set()
        await reader.close()
        await writer.close()


async def test_http_resume_cursor_summary_and_legacy_gone(api, runtime, repository):
    topic = await repository.create_topic("HTTP cursor")
    run = await runtime.start(topic.id, ChatRequest(text="/echo restart", idempotency_key="one"))
    original = await frames(runtime, run.id)
    response = await api.get(f"/v1/runs/{run.id}/resume?from_seq=1", headers={"Last-Event-ID": "2"})
    replay = [
        StreamFrame.model_validate_json(line[6:])
        for line in response.text.splitlines()
        if line.startswith("data: ")
    ]
    assert replay == original[3:]
    summary = (await api.get(f"/v1/runs/{run.id}/events/summary")).json()
    assert summary["count"] == len(original) and summary["terminal"]
    assert (await api.get(f"/v1/runs/{run.id}/events?from_seq={len(original)}")).text == ""
    assert (
        await api.get(f"/v1/runs/{run.id}/events", headers={"Last-Event-ID": "bad"})
    ).status_code == 416
    legacy_topic = await repository.create_topic("无日志旧运行")
    legacy, _created = await repository.begin_run(
        legacy_topic.id, ChatRequest(text="legacy", idempotency_key="one")
    )
    await repository.finish(legacy.id, "completed", None, [])
    assert (await api.get(f"/v1/runs/{legacy.id}/events")).status_code == 410


async def test_real_tcp_client_and_cli_resume_handle_terminal_cursor(server_url):
    def scenario():
        with KrimClient(server_url) as client:
            topic = client.create_topic("续传客户端")
            run = client.send(topic.id, "/add 2 3", "one")
            original = list(client.events(run.id))
            summary = client.event_summary(run.id)
            assert summary.terminal and summary.count == len(original)
            assert list(client.events(run.id, summary.count)) == []
            assert list(client.events(run.id, 2)) == original[2:]
            assert len(client.run_messages(run.id)) == 4
            return run.id, original

    run_id, original = await asyncio.to_thread(scenario)
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "krim_agent",
        "resume",
        run_id,
        "--from-seq",
        "2",
        "--url",
        server_url,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
    assert process.returncode == 0, stderr.decode()
    replay = [StreamFrame.model_validate_json(line) for line in stdout.decode().splitlines()]
    assert replay == original[2:]


async def test_event_migration_refuses_to_discard_persisted_data(runtime, repository):
    topic = await repository.create_topic("事件回退保护")
    run = await runtime.start(topic.id, ChatRequest(text="retained", idempotency_key="one"))
    original = await frames(runtime, run.id)
    with pytest.raises(RuntimeError, match="持久流事件"):
        async with repository.database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(migration_config(sync), "0002_message_tree")
            )
    assert (await repository.list_events(run.id)) == original


async def test_real_process_exit_preserves_acknowledged_frames(tmp_path):
    url = f"sqlite+aiosqlite:///{tmp_path / 'process.sqlite'}"
    script = """
import asyncio
import json
import os
import sys
from krim_agent.persistence import Database, KrimRepository
from krim_agent.protocol.domain import ChatRequest
from krim_agent.protocol.models import StreamFrame

async def main():
    database = Database(sys.argv[1])
    await database.initialize()
    repository = KrimRepository(database)
    topic = await repository.create_topic("进程中断")
    run, _ = await repository.begin_run(topic.id, ChatRequest(text="start", idempotency_key="one"))
    await repository.set_running(run.id)
    events = [
        StreamFrame(stream_id=run.id, seq=0, kind="start"),
        StreamFrame(
            stream_id=run.id, seq=1, kind="chunk",
            data={"type":"text_delta","text":"已发送"},
        ),
    ]
    for frame in events:
        await repository.append_event(frame)
    output = {"run_id":run.id,"events":[frame.model_dump(mode="json") for frame in events]}
    print(json.dumps(output), flush=True)
    os._exit(0)
asyncio.run(main())
"""
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        script,
        url,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
    assert process.returncode == 0, stderr.decode()
    result = json.loads(stdout.decode())
    app = create_app(Settings(database_url=url))
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as client,
    ):
        response = await client.get(f"/v1/runs/{result['run_id']}/events")
        events = [
            json.loads(line[6:]) for line in response.text.splitlines() if line.startswith("data: ")
        ]
    for actual, expected in zip(events[:2], result["events"], strict=True):
        assert actual == {key: value for key, value in expected.items() if value is not None}
    assert [frame["seq"] for frame in events] == [0, 1, 2]
    assert events[-1]["data"]["status"] == "interrupted"


async def test_failed_event_write_is_not_sent_or_reported_as_completed(repository):
    original_writer = repository.append_event
    failed = False

    async def fail_once(frame):
        nonlocal failed
        if (
            frame.kind == "chunk"
            and isinstance(frame.data, dict)
            and frame.data.get("type") == "text_delta"
            and not failed
        ):
            failed = True
            raise OSError("模拟事件存储写入失败")
        return await original_writer(frame)

    repository.append_event = fail_once
    runtime = ChatRuntime(repository, FakeProvider(delay=0), ToolRegistry(), Settings())
    try:
        topic = await repository.create_topic("写入失败")
        run = await runtime.start(topic.id, ChatRequest(text="unsent", idempotency_key="one"))
        observed = await frames(runtime, run.id)
        assert observed[0].kind == "start" and observed[-1].kind == "end"
        assert not any(
            isinstance(frame.data, dict) and frame.data.get("type") == "text_delta"
            for frame in observed
        )
        assert observed[-1].data["status"] == "failed"
        assert (await repository.get_run(run.id)).status == "failed"
        assert await repository.list_events(run.id) == observed
        assert (await repository.list_messages(topic.id))[-1].details["partial"]
    finally:
        await runtime.close()
