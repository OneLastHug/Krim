"""原消息与关联帧同事务、未知提交只读及取消后的原记账。"""

import asyncio
import copy
import sqlite3

import pytest
from sqlalchemy import event
from sqlalchemy.orm import Session
from test_durable_streams import frames

from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.protocol.models import StreamFrame
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.runtime import ChatRuntime
from krim_agent.tools import ToolContext, ToolRegistry, tool
from krim_agent.tools.core import TextContent, ToolResult


async def prepared(repository, role):
    topic = await repository.create_topic("成对持久元数据")
    run, _ = await repository.begin_run(topic.id, ChatRequest(text="seed", idempotency_key="one"))
    await repository.set_running(run.id)
    await repository.append_event(StreamFrame(stream_id=run.id, seq=0, kind="start"))
    content = "original"
    if role == "tool":
        details = {
            "call_id": "original-call",
            "name": "effect",
            **ToolResult(
                content=[TextContent(text=content)],
                data={"effect": "original"},
            ).model_dump(mode="json"),
        }
        data = {"type": "tool_result", **details}
    else:
        details = {"step": 0, "partial": False}
        data = {"type": "step_finish", "step": 0, "finish_reason": "stop"}
    return run, content, details, StreamFrame(stream_id=run.id, seq=1, kind="chunk", data=data)


@pytest.mark.parametrize("role", ["tool", "assistant"])
async def test_pair_uses_one_commit_and_idempotent_read_never_adds_message(repository, role):
    run, content, details, frame = await prepared(repository, role)
    commits = []

    def committed(_session):
        commits.append(True)

    event.listen(Session, "after_commit", committed)
    observer = sqlite3.connect(str(repository.database.engine.url.database))
    try:
        before = observer.execute("PRAGMA data_version").fetchone()[0]
        assert await repository.append_message_event(run, role, content, details, frame) == frame
        after = observer.execute("PRAGMA data_version").fetchone()[0]
        assert after != before and len(commits) == 1
        messages = await repository.run_messages(run.id)
        assert len(messages) == 2 and messages[-1].parent_id == run.input_message_id
        assert messages[-1].content == content and messages[-1].details == details
        assert (await repository.list_events(run.id))[-1] == frame
        assert await repository.append_message_event(run, role, content, details, frame) == frame
        assert await repository.read_message_event(run, role, content, details, frame) == frame
        assert observer.execute("PRAGMA data_version").fetchone()[0] == after
        assert await repository.run_messages(run.id) == messages
    finally:
        event.remove(Session, "after_commit", committed)
        observer.close()


@pytest.mark.parametrize("role", ["tool", "assistant"])
@pytest.mark.parametrize("point", ["messages", "run_events"])
async def test_pair_sql_failure_rolls_back_both_rows_and_original_branch(repository, role, point):
    run, content, details, frame = await prepared(repository, role)
    topic = await repository.get_topic(run.topic_id)
    before = await repository.run_messages(run.id)

    def fail(_conn, _cursor, statement, _params, _context, _many):
        if statement.startswith("INSERT INTO " + point):
            raise OSError("synthetic paired SQL failure")

    engine = repository.database.engine.sync_engine
    event.listen(engine, "before_cursor_execute", fail)
    try:
        with pytest.raises(OSError, match="paired SQL failure"):
            await repository.append_message_event(run, role, content, details, frame)
    finally:
        event.remove(engine, "before_cursor_execute", fail)
    assert await repository.run_messages(run.id) == before
    assert await repository.get_topic(run.topic_id) == topic
    assert len(await repository.list_events(run.id)) == 1
    assert await repository.read_message_event(run, role, content, details, frame) is None
    assert await repository.append_message_event(run, role, content, details, frame) == frame


@pytest.mark.parametrize("role", ["tool", "assistant"])
@pytest.mark.parametrize("half", ["message", "event"])
async def test_old_single_sided_material_is_not_claimed_as_a_pair(repository, role, half):
    run, content, details, frame = await prepared(repository, role)
    if half == "message":
        await repository.add_message(run, role, content, details)
    else:
        await repository.append_event(frame)
    for operation in (repository.append_message_event, repository.read_message_event):
        with pytest.raises(ServiceError) as refused:
            await operation(run, role, content, details, frame)
        assert refused.value.code == "message_event_incomplete"


@pytest.mark.parametrize("role", ["tool", "assistant"])
async def test_pair_conflicting_frame_message_or_foreign_run_identity_refuses(repository, role):
    run, content, details, frame = await prepared(repository, role)
    await repository.append_message_event(run, role, content, details, frame)
    with pytest.raises(ServiceError) as conflict:
        await repository.append_message_event(run, role, content + "changed", details, frame)
    assert conflict.value.code in {"message_event_context", "message_event_conflict"}
    with pytest.raises(ServiceError) as duplicate:
        await repository.append_message_event(
            run, role, content, details, frame.model_copy(update={"seq": 2})
        )
    assert duplicate.value.code == "message_event_incomplete"
    other = await repository.create_topic("另一原话题")
    with pytest.raises(ServiceError) as foreign:
        await repository.append_message_event(
            run.model_copy(update={"topic_id": other.id}), role, content, details, frame
        )
    assert foreign.value.code == "message_event_context"
    assert (
        len(await repository.run_messages(run.id)) == len(await repository.list_events(run.id)) == 2
    )


@pytest.mark.parametrize("role", ["tool", "assistant"])
async def test_pair_json_boolean_is_not_an_integer_and_key_order_is_irrelevant(repository, role):
    run, content, details, frame = await prepared(repository, role)
    bad_data = dict(frame.data)
    if role == "tool":
        bad_data["is_error"] = 0
    else:
        bad_data["step"] = False
    with pytest.raises(ServiceError) as refused:
        await repository.append_message_event(
            run, role, content, details, frame.model_copy(update={"data": bad_data})
        )
    assert refused.value.code == "message_event_context"
    await repository.append_message_event(run, role, content, details, frame)
    reordered = frame.model_copy(update={"data": dict(reversed(list(frame.data.items())))})
    assert await repository.read_message_event(run, role, content, details, reordered) == frame


@pytest.mark.parametrize("role", ["tool", "assistant"])
async def test_concurrent_identical_and_conflicting_pairs_keep_one_message_and_frame(
    repository, role
):
    run, content, details, frame = await prepared(repository, role)
    values = await asyncio.gather(
        *[repository.append_message_event(run, role, content, details, frame) for _ in range(3)]
    )
    assert values == [frame] * 3
    assert len(await repository.run_messages(run.id)) == 2
    other_details = copy.deepcopy(details)
    other_data = copy.deepcopy(frame.data)
    if role == "tool":
        other_details["content"][0]["text"] = "other"
        other_data = {"type": "tool_result", **other_details}
    result = await asyncio.gather(
        repository.append_message_event(run, role, content, details, frame),
        repository.append_message_event(
            run, role, "other", other_details, frame.model_copy(update={"data": other_data})
        ),
        return_exceptions=True,
    )
    assert result[0] == frame and isinstance(result[1], ServiceError)
    assert result[1].code == "message_event_conflict"
    assert (
        len(await repository.run_messages(run.id)) == len(await repository.list_events(run.id)) == 2
    )


async def test_regenerate_pair_keeps_original_parent_source_and_historical_replay(repository):
    from krim_agent.config import Settings
    from krim_agent.protocol.domain import RegenerateRequest

    class Echo:
        name = "offline-regenerate-pair"
        calls = 0

        async def stream(self, _messages, _tools):
            self.calls += 1
            yield ModelText(text="原分支回复")

    provider = Echo()
    runtime = ChatRuntime(repository, provider, ToolRegistry(), Settings())
    try:
        topic = await repository.create_topic("再生分支成对消息")
        first = await runtime.start(topic.id, ChatRequest(text="original", idempotency_key="first"))
        original = await frames(runtime, first.id)
        before = await repository.run_messages(first.id)
        second = await runtime.regenerate(
            topic.id, before[-1].id, RegenerateRequest(idempotency_key="second")
        )
        current = await frames(runtime, second.id)
        assert current[-1].data["status"] == "completed"
        messages = await repository.run_messages(second.id)
        final = messages[-1]
        assert final.parent_id == before[0].id
        assert final.details == {"step": 0, "partial": False, "regenerated_from": before[-1].id}
        target = next(
            frame
            for frame in current
            if isinstance(frame.data, dict) and frame.data.get("type") == "step_finish"
        )
        assert (
            await repository.read_message_event(
                second, "assistant", final.content, {"step": 0, "partial": False}, target
            )
            == target
        )
        assert await frames(runtime, first.id) == original and provider.calls == 2
        assert await repository.run_messages(first.id) == before
    finally:
        await runtime.close()


class Provider:
    name = "offline-paired-message-events"

    def __init__(self, call_id="actual-tool-call"):
        self.calls = 0
        self.call_id = call_id

    async def stream(self, _messages, _tools):
        self.calls += 1
        if self.calls == 1:
            yield ModelToolCall(id=self.call_id, name="effect", arguments={})
        else:
            assert self.calls == 2
            yield ModelText(text="最终原回复")


def registry(effects):
    tools = ToolRegistry()

    @tool(name="effect", risk="write")
    async def effect(context: ToolContext) -> dict:
        effects.append(context.run_id)
        return {"effect": "once"}

    tools.register(effect)
    return tools


async def test_pair_committed_but_return_lost_reads_original_without_provider_or_tool_replay(
    repository,
):
    provider, effects = Provider(), []

    async def allow(*_args):
        return "allow"

    from krim_agent.config import Settings

    runtime = ChatRuntime(repository, provider, registry(effects), Settings(), tool_policy=allow)
    original = repository.append_message_event
    writes = []

    async def lost(*args, **kwargs):
        stored = await original(*args, **kwargs)
        writes.append(stored.seq)
        raise OSError("synthetic lost paired return after COMMIT")

    repository.append_message_event = lost
    try:
        topic = await repository.create_topic("未知成对提交")
        run = await runtime.start(topic.id, ChatRequest(text="do once", idempotency_key="once"))
        observed = await frames(runtime, run.id)
        assert observed[-1].data["status"] == "completed"
        assert len(writes) == 2 and len(effects) == 1 and provider.calls == 2
        assert [m.role for m in await repository.run_messages(run.id)] == [
            "user",
            "assistant",
            "tool",
            "assistant",
        ]
        assert observed == await repository.list_events(run.id)
        assert [x.seq for x in observed] == list(range(len(observed)))
    finally:
        await runtime.close()


async def test_existing_provider_schema_empty_call_id_keeps_original_runtime_contract(repository):
    from krim_agent.config import Settings

    provider, effects = Provider(call_id=""), []

    async def allow(*_args):
        return "allow"

    runtime = ChatRuntime(repository, provider, registry(effects), Settings(), tool_policy=allow)
    try:
        topic = await repository.create_topic("保留原Provider ID Schema")
        run = await runtime.start(
            topic.id, ChatRequest(text="original empty id", idempotency_key="empty")
        )
        result = await frames(runtime, run.id)
        assert result[-1].data["status"] == "completed" and len(effects) == 1
        messages = await repository.run_messages(run.id)
        assert [m.details["call_id"] for m in messages if m.role == "tool"] == [""]
    finally:
        await runtime.close()


@pytest.mark.parametrize("role", ["tool", "assistant"])
async def test_pair_commit_repeat_cancel_clears_only_original_pending_or_draft(repository, role):
    provider, effects = Provider(), []

    async def allow(*_args):
        return "allow"

    from krim_agent.config import Settings

    runtime = ChatRuntime(repository, provider, registry(effects), Settings(), tool_policy=allow)
    original = repository.append_message_event
    entered, release, stopped = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original_stop = repository.request_stop

    async def mark_stop(identifier):
        await original_stop(identifier)
        stopped.set()

    repository.request_stop = mark_stop

    async def paused(run, message_role, content, details, frame):
        result = await original(run, message_role, content, details, frame)
        if message_role == role:
            entered.set()
            await release.wait()
        return result

    repository.append_message_event = paused
    stopping = None
    try:
        topic = await repository.create_topic("成对提交后反复取消")
        run = await runtime.start(topic.id, ChatRequest(text="cancel", idempotency_key="one"))
        await asyncio.wait_for(entered.wait(), 10)
        stopping = asyncio.create_task(runtime.stop(run.id))
        await asyncio.wait_for(stopped.wait(), 10)
        assert not stopping.done()
        runtime.production_task(run.id).cancel()
        await asyncio.sleep(0)
        assert not stopping.done()
        release.set()
        assert (await stopping).status == "aborted"
        observed = await frames(runtime, run.id)
        messages = await repository.run_messages(run.id)
        assert sum(item.role == "tool" for item in messages) == 1
        assert sum(
            item.role == "assistant" and not item.details.get("tool_calls") for item in messages
        ) == int(role == "assistant")
        assert len(effects) == 1 and provider.calls == (1 if role == "tool" else 2)
        assert observed[-1].data["status"] == "aborted"
        assert observed == await repository.list_events(run.id)
        assert [item.seq for item in observed] == list(range(len(observed)))
    finally:
        release.set()
        if stopping is not None:
            await asyncio.gather(stopping, return_exceptions=True)
        await runtime.close()
