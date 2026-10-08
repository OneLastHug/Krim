import asyncio
import json
import sys
from uuid import uuid4

import pytest
from alembic import command
from sqlalchemy import func, select, text

from krim_agent.app import LocalApp
from krim_agent.application.context import (
    PersistentContext,
    PreCompactDecision,
)
from krim_agent.client import KrimClient
from krim_agent.config import Settings
from krim_agent.context_engine import (
    ContextCompactRequest,
    ContextEngine,
    ContextError,
    ContextOptions,
)
from krim_agent.persistence import ContextSummaryRow, Database, KrimRepository
from krim_agent.persistence.migration import migration_config
from krim_agent.protocol.domain import ChatRequest, EditRequest, ServiceError
from krim_agent.providers import ModelText
from krim_agent.runtime import ChatRuntime
from krim_agent.sdk import ContextCompactPart, PythonAI
from krim_agent.tools import ToolRegistry


async def complete_turn(repository, topic_id, text_value, *, tool_call=False):
    run, _ = await repository.begin_run(
        topic_id, ChatRequest(text=text_value, idempotency_key=str(uuid4()))
    )
    if tool_call:
        call_id = str(uuid4())
        await repository.add_message(
            run, "assistant", "", {"tool_calls": [{"id": call_id, "name": "lookup"}]}
        )
        await repository.add_message(run, "tool", "工具结果", {"call_id": call_id})
    await repository.add_message(run, "assistant", "回复：" + text_value)
    await repository.finish(run.id, "completed", None, [])
    return run


async def seed(repository, count=4):
    topic = await repository.create_topic("持久上下文")
    runs = [await complete_turn(repository, topic.id, f"历史 {index}") for index in range(count)]
    return topic, runs


async def summaries(repository):
    async with repository.database.sessions() as session:
        return list(await session.scalars(select(ContextSummaryRow)))


def options(**values):
    return ContextOptions(strategy="annotated_compact", max_messages=3, **values)


class ContextRecorder:
    name = "context-recorder"

    def __init__(self):
        self.inputs = []

    async def stream(self, messages, _tools):
        self.inputs.append(list(messages))
        yield ModelText(text="离线回答")


async def wait_run(runtime, run):
    state = await runtime.stream_state(run.id, 0)
    await state.task
    return await runtime.repository.get_run(run.id)


async def test_automatic_summary_survives_reopen_and_never_reads_without_writing(tmp_path):
    url = f"sqlite+aiosqlite:///{tmp_path / 'context.sqlite'}"
    database = Database(url)
    await database.initialize()
    repository = KrimRepository(database)
    recorder = ContextRecorder()
    runtime = ChatRuntime(
        repository,
        recorder,
        ToolRegistry(),
        Settings(context_strategy="annotated_compact", context_max_messages=3),
    )
    try:
        topic = await repository.create_topic("实际运行")
        for index in range(3):
            run = await runtime.start(
                topic.id, ChatRequest(text=f"第 {index}", idempotency_key=str(index))
            )
            assert (await wait_run(runtime, run)).status == "completed"
        stored = await summaries(repository)
        assert len(stored) == 1 and len(stored[0].source_message_ids) == 4
        sources = stored[0].source_message_ids
        assert len(recorder.inputs[-1]) == 2
        assert recorder.inputs[-1][0].role == "user"
        assert "[不可信历史摘要]" in recorder.inputs[-1][0].content
    finally:
        await runtime.close()
        await database.close()

    reopened = Database(url)
    await reopened.initialize()
    repository = KrimRepository(reopened)
    recorder = ContextRecorder()
    runtime = ChatRuntime(
        repository,
        recorder,
        ToolRegistry(),
        Settings(context_policy="never", context_max_messages=2),
    )
    try:
        run = await runtime.start(topic.id, ChatRequest(text="重开", idempotency_key="reopen"))
        assert (await wait_run(runtime, run)).status == "completed"
        assert recorder.inputs[-1][0].details["source_message_ids"] == sources
        assert not set(sources) & {message.id for message in recorder.inputs[-1]}
        assert len(recorder.inputs[-1]) == 4  # never 保留全部未覆盖消息，不再次压缩。
        assert len(await summaries(repository)) == 1
        assert len(await repository.list_messages(topic.id)) == 8
    finally:
        await runtime.close()
        await reopened.close()


async def test_repeated_compaction_extends_full_prefix_and_injects_only_one_summary(repository):
    topic, _ = await seed(repository)
    service = PersistentContext(repository, ContextEngine())
    original = await repository.list_messages(topic.id)
    first = await service.prepare(topic.id, original, options())
    assert first.source_message_ids == [message.id for message in original[:6]]
    await complete_turn(repository, topic.id, "新增历史")
    extended = await repository.list_messages(topic.id)
    second = await service.prepare(topic.id, extended, options())
    assert second.source_message_ids == [message.id for message in extended[:8]]
    assert second.original_count == 10 and second.omitted_count == 8
    assert len(second.messages) == 3
    assert sum(bool(message.details.get("context_summary")) for message in second.messages) == 1
    assert len(await summaries(repository)) == 2
    restored = await service.prepare(topic.id, extended, ContextOptions(policy="never"))
    assert restored.source_message_ids == second.source_message_ids
    assert restored.messages[0].content == second.messages[0].content
    assert not restored.compacted


async def test_editing_covered_branch_does_not_reuse_summary_but_shared_prefix_can(repository):
    topic, runs = await seed(repository)
    service = PersistentContext(repository, ContextEngine())
    original = await repository.list_messages(topic.id)
    stored = await service.prepare(topic.id, original, options())
    edited, _ = await repository.begin_run(
        topic.id,
        EditRequest(text="编辑最后一轮", idempotency_key="last-edit"),
        action="edit",
        source_message_id=runs[-1].input_message_id,
    )
    await repository.add_message(edited, "assistant", "新回答")
    await repository.finish(edited.id, "completed", None, [])
    shared = await service.prepare(
        topic.id, await repository.list_messages(topic.id), ContextOptions(policy="never")
    )
    assert shared.source_message_ids == stored.source_message_ids
    edited, _ = await repository.begin_run(
        topic.id,
        EditRequest(text="编辑已覆盖的第一轮", idempotency_key="root-edit"),
        action="edit",
        source_message_id=runs[0].input_message_id,
    )
    await repository.add_message(edited, "assistant", "新根回答")
    await repository.finish(edited.id, "completed", None, [])
    sibling = await service.prepare(
        topic.id, await repository.list_messages(topic.id), ContextOptions(policy="never")
    )
    assert sibling.summary is None and not sibling.source_message_ids
    assert not any(message.details.get("context_summary") for message in sibling.messages)
    await repository.select_branch(topic.id, original[-1].id)
    restored = await service.prepare(
        topic.id, await repository.list_messages(topic.id), ContextOptions(policy="never")
    )
    assert restored.source_message_ids == stored.source_message_ids


async def test_preview_confirm_and_never_do_not_rewrite_message_tree(repository):
    topic, _ = await seed(repository)
    before = await repository.message_tree(topic.id)
    service = PersistentContext(repository, ContextEngine())
    preview = await service.compact(topic.id, options())
    assert preview.status == "proposed" and not preview.persisted
    assert not await summaries(repository)
    confirmed = await service.compact(
        topic.id, options(), confirm=True, expected_branch_key=preview.branch_key
    )
    assert confirmed.status == "completed" and confirmed.persisted
    assert confirmed.result.source_message_ids == preview.result.source_message_ids
    assert (await repository.message_tree(topic.id)) == before
    unchanged = await service.compact(topic.id, options(policy="never"), confirm=True)
    assert unchanged.status == "unchanged" and unchanged.reason == "policy_never"
    assert not unchanged.persisted and len(await summaries(repository)) == 1


async def test_stale_preview_confirmation_is_rejected(repository):
    topic, _ = await seed(repository)
    service = PersistentContext(repository, ContextEngine())
    preview = await service.compact(topic.id, options())
    await complete_turn(repository, topic.id, "预览之后的消息")
    with pytest.raises(ServiceError) as error:
        await service.compact(
            topic.id, options(), confirm=True, expected_branch_key=preview.branch_key
        )
    assert error.value.code == "summary_branch_changed"
    assert not await summaries(repository)


async def test_manual_compaction_blocks_same_topic_start_but_allows_other_topic(repository):
    topic, _ = await seed(repository)
    other = await repository.create_topic("独立话题")
    started, release = asyncio.Event(), asyncio.Event()

    async def slow_summary(_messages):
        started.set()
        await release.wait()
        return "本地摘要"

    runtime = ChatRuntime(repository, ContextRecorder(), ToolRegistry(), Settings())
    runtime.persistent_context.summarizer = slow_summary
    task = asyncio.create_task(
        runtime.compact(topic.id, ContextCompactRequest(max_messages=3, confirm=True))
    )
    try:
        await asyncio.wait_for(started.wait(), 5)
        with pytest.raises(ServiceError, match="手动压缩"):
            await runtime.compact(topic.id, ContextCompactRequest(max_messages=3))
        with pytest.raises(ServiceError) as error:
            await runtime.start(topic.id, ChatRequest(text="冲突", idempotency_key="busy"))
        assert error.value.code == "context_busy"
        other_run = await runtime.start(other.id, ChatRequest(text="并行", idempotency_key="ok"))
        assert (await wait_run(runtime, other_run)).status == "completed"
        release.set()
        assert (await task).status == "completed"
    finally:
        release.set()
        await task
        await runtime.close()


async def test_external_run_started_during_summary_is_rechecked_before_commit(repository):
    topic, _ = await seed(repository)
    started, release = asyncio.Event(), asyncio.Event()

    async def slow_summary(_messages):
        started.set()
        await release.wait()
        return "不能保存的摘要"

    service = PersistentContext(repository, ContextEngine(), slow_summary)
    task = asyncio.create_task(service.compact(topic.id, options(), confirm=True))
    await asyncio.wait_for(started.wait(), 5)
    run, _ = await repository.begin_run(
        topic.id, ChatRequest(text="外部运行", idempotency_key="external")
    )
    release.set()
    with pytest.raises(ServiceError) as error:
        await task
    assert error.value.code == "topic_busy"
    assert not await summaries(repository)
    await repository.finish(run.id, "aborted", None, [])
    assert not service.is_busy(topic.id)


async def test_branch_changed_during_summary_is_rejected_in_save_transaction(repository):
    topic, runs = await seed(repository)
    started, release = asyncio.Event(), asyncio.Event()

    async def slow_summary(_messages):
        started.set()
        await release.wait()
        return "过时摘要"

    service = PersistentContext(repository, ContextEngine(), slow_summary)
    task = asyncio.create_task(service.compact(topic.id, options(), confirm=True))
    await asyncio.wait_for(started.wait(), 5)
    edited, _ = await repository.begin_run(
        topic.id,
        EditRequest(text="外部编辑", idempotency_key="external-edit"),
        action="edit",
        source_message_id=runs[0].input_message_id,
    )
    await repository.add_message(edited, "assistant", "外部回答")
    await repository.finish(edited.id, "completed", None, [])
    release.set()
    with pytest.raises(ServiceError) as error:
        await task
    assert error.value.code == "summary_branch_changed"
    assert not await summaries(repository)


@pytest.mark.parametrize("case", ["empty", "nonprefix", "partial_turn", "all", "wrong_hash"])
async def test_storage_rejects_invalid_sources_and_partial_groups(repository, case):
    topic, _ = await seed(repository)
    messages = await repository.list_messages(topic.id)
    sources = messages[:2]
    if case == "empty":
        sources = []
    elif case == "nonprefix":
        sources = messages[2:4]
    elif case == "partial_turn":
        sources = messages[:1]
    elif case == "all":
        sources = messages
    key = "0" * 64 if case == "wrong_hash" else repository.context_branch_key(sources)
    with pytest.raises(ServiceError):
        await repository.save_context_summary(
            topic.id, key, [message.id for message in sources], "摘要", "annotated_compact", "auto"
        )
    assert not await summaries(repository)


async def test_storage_refuses_unclosed_tools_and_frozen_summary_rewrite(repository):
    topic = await repository.create_topic("工具来源")
    await complete_turn(repository, topic.id, "带工具的一轮", tool_call=True)
    await complete_turn(repository, topic.id, "最新一轮")
    messages = await repository.list_messages(topic.id)
    with pytest.raises(ServiceError, match="工具请求"):
        await repository.save_context_summary(
            topic.id,
            repository.context_branch_key(messages[:2]),
            [message.id for message in messages[:2]],
            "部分工具组",
            "annotated_compact",
            "auto",
        )
    sources = messages[:4]
    key = repository.context_branch_key(sources)
    await repository.save_context_summary(
        topic.id,
        key,
        [message.id for message in sources],
        "冻结摘要",
        "annotated_compact",
        "auto",
        never_update=True,
    )
    with pytest.raises(ServiceError) as error:
        await repository.save_context_summary(
            topic.id, key, [message.id for message in sources], "篡改", "annotated_compact", "auto"
        )
    assert error.value.code == "summary_frozen"
    assert (await repository.get_context_summary(topic.id, key)).summary == "冻结摘要"


async def test_hooks_adjust_copied_input_and_post_failure_keeps_committed_fact(repository):
    topic, _ = await seed(repository)
    pre_events, post_events = [], []

    def pre(event):
        pre_events.append(event.trigger)
        event.messages[0].content = "只修改 Hook 副本"
        return PreCompactDecision(options=options())

    async def post(event):
        post_events.append((event.trigger, event.persisted))
        assert len(await summaries(repository)) == 1
        event.result.summary = "只修改观测副本"
        raise RuntimeError("不能泄露的回调正文")

    service = PersistentContext(repository, ContextEngine(), pre_compact=pre, post_compact=post)
    preview = await service.compact(topic.id, options())
    assert preview.status == "proposed" and not post_events
    confirmed = await service.compact(topic.id, options(), confirm=True)
    assert confirmed.status == "completed" and post_events == [("manual", True)]
    assert pre_events == ["manual", "manual"]
    assert "post_compact_failed" in confirmed.result.processor_trace
    assert "不能泄露" not in confirmed.result.model_dump_json()
    assert (await repository.list_messages(topic.id))[0].content == "历史 0"
    assert (await summaries(repository))[0].summary == confirmed.result.summary


async def test_pre_hook_denial_does_not_generate_or_save_summary(repository):
    topic, _ = await seed(repository)

    def pre(_event):
        return PreCompactDecision(allowed=False)

    def summary(_messages):
        pytest.fail("被拒绝的压缩不能生成摘要")

    service = PersistentContext(repository, ContextEngine(), summary, pre_compact=pre)
    with pytest.raises(ContextError) as error:
        await service.compact(topic.id, options(), confirm=True)
    assert error.value.code == "context_denied"
    assert not await summaries(repository)


async def test_cancelled_summary_does_not_save_and_releases_topic(repository):
    topic, _ = await seed(repository)
    started = asyncio.Event()

    async def slow_summary(_messages):
        started.set()
        await asyncio.Event().wait()

    service = PersistentContext(repository, ContextEngine(), slow_summary)
    task = asyncio.create_task(service.compact(topic.id, options(), confirm=True))
    await asyncio.wait_for(started.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not await summaries(repository) and not service.is_busy(topic.id)
    service.summarizer = lambda _messages: "完成的摘要"
    assert (await service.compact(topic.id, options(), confirm=True)).status == "completed"


async def test_cancellation_after_save_starts_waits_for_transaction(repository, monkeypatch):
    topic, _ = await seed(repository)
    entered, release = asyncio.Event(), asyncio.Event()
    service = PersistentContext(repository, ContextEngine())
    original_save = service.proposals.decide

    async def blocked_save(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original_save(*args, **kwargs)

    monkeypatch.setattr(service.proposals, "decide", blocked_save)
    task = asyncio.create_task(service.compact(topic.id, options(), confirm=True))
    await asyncio.wait_for(entered.wait(), 5)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(await summaries(repository)) == 1 and not service.is_busy(topic.id)


async def test_summary_migration_downgrade_preserves_existing_data(repository):
    topic, _ = await seed(repository)
    service = PersistentContext(repository, ContextEngine())
    await service.compact(topic.id, options(), confirm=True)
    before = (await summaries(repository))[0].summary
    with pytest.raises(RuntimeError, match="Context 摘要"):
        async with repository.database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(migration_config(sync), "0007_workflow_requests")
            )
    async with repository.database.engine.connect() as connection:
        assert (
            await connection.execute(text("SELECT version_num FROM alembic_version"))
        ).scalar() == "0027_native_workspace_batches"
    assert (await summaries(repository))[0].summary == before


async def test_manual_api_preview_confirm_errors_and_source_audit(api, repository, runtime):
    topic, _ = await seed(repository)
    endpoint = f"/v1/topics/{topic.id}/compact"
    preview = await api.post(endpoint, json={"max_messages": 3})
    assert preview.status_code == 200, preview.text
    proposed = preview.json()
    assert proposed["status"] == "proposed" and not proposed["persisted"]
    assert proposed["source_until_message_id"] == proposed["source_message_ids"][-1]
    assert proposed["source_branch_key"] and not await summaries(repository)
    confirmed = await api.post(
        endpoint,
        json={"max_messages": 3, "confirm": True, "expected_branch_key": proposed["branch_key"]},
    )
    assert confirmed.status_code == 200, confirmed.text
    assert confirmed.json()["status"] == "completed" and confirmed.json()["persisted"]
    assert len(await summaries(repository)) == 1
    never = await api.post(endpoint, json={"max_messages": 3, "confirm": True, "policy": "never"})
    assert never.json()["status"] == "unchanged" and never.json()["reason"] == "policy_never"
    assert (await api.post(endpoint, json={"max_messages": 1})).status_code == 422
    assert (await api.post(endpoint, json={"strategy": "truncate"})).status_code == 422
    missing = await api.post("/v1/topics/missing/compact", json={})
    assert missing.status_code == 404 and missing.json()["error"]["code"] == "topic_not_found"
    run, _ = await repository.begin_run(
        topic.id, ChatRequest(text="活动运行", idempotency_key="active")
    )
    busy = await api.post(endpoint, json={"max_messages": 3})
    assert busy.status_code == 409 and busy.json()["error"]["code"] == "topic_busy"
    await repository.finish(run.id, "aborted", None, [])
    runtime.persistent_context.pre_compact = lambda _event: PreCompactDecision(allowed=False)
    denied = await api.post(endpoint, json={"max_messages": 2, "confirm": True})
    assert denied.status_code == 403 and denied.json()["error"]["code"] == "context_denied"


async def test_client_and_cli_consume_public_compact_api_over_real_tcp(server_url, repository):
    topic, _ = await seed(repository)

    def preview_client():
        with KrimClient(server_url) as client:
            return client.compact(topic.id, options())

    preview = await asyncio.to_thread(preview_client)
    assert preview.status == "proposed" and not await summaries(repository)

    async def cli(*flags):
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "krim_agent",
            "compact",
            topic.id,
            "--url",
            server_url,
            "--max-messages",
            "3",
            *flags,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
        assert process.returncode == 0, stderr.decode()
        return json.loads(stdout)

    cli_preview = await cli("--preview")
    assert cli_preview["status"] == "proposed" and not await summaries(repository)
    confirmed = await cli("--confirm", "--expected-branch-key", preview.branch_key)
    assert confirmed["status"] == "completed" and len(await summaries(repository)) == 1
    assert (await asyncio.to_thread(preview_client)).status == "unchanged"
    async with repository.database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(ContextSummaryRow)) == 1


async def test_host_compact_is_consumed_by_runtime_and_preserves_confirmation(runtime, repository):
    topic, _ = await seed(repository)
    app = LocalApp(runtime)
    try:
        preview = await app.handle.compact(topic.id, ContextCompactRequest(max_messages=3))
        assert preview.status == "proposed" and not await summaries(repository)
        result = await app.handle.compact(
            topic.id,
            ContextCompactRequest(
                max_messages=3, confirm=True, expected_branch_key=preview.branch_key
            ),
        )
        assert result.status == "completed" and result.persisted
        assert len(await summaries(repository)) == 1
    finally:
        await app.close()


async def test_generation_sdk_delivers_committed_context_source_and_hook_facts(repository):
    topic, _ = await seed(repository)
    observations = []

    async def post(event):
        observations.append((event.trigger, event.persisted))

    runtime = ChatRuntime(
        repository,
        ContextRecorder(),
        ToolRegistry(),
        Settings(context_strategy="annotated_compact", context_max_messages=3),
    )
    runtime.persistent_context.post_compact = post
    ai = PythonAI(runtime)
    try:
        stream = await ai.stream_text("最新输入", topic_id=topic.id)
        parts = [part async for part in stream.full_stream()]
        compacted = [part for part in parts if isinstance(part, ContextCompactPart)]
        assert len(compacted) == 1 and compacted[0].persisted
        row = (await summaries(repository))[0]
        assert compacted[0].status == "completed" and compacted[0].source_count == 8
        assert compacted[0].source_branch_key == row.branch_key
        assert compacted[0].source_until_message_id == row.source_until_message_id
        assert observations == [("automatic", True)]
        assert (await stream.collect()).run.status == "completed"
    finally:
        await ai.close()
        await runtime.close()


async def test_empty_history_defaults_and_summary_failure_api_are_explicit(
    api, repository, runtime
):
    topic = await repository.create_topic("空历史")
    response = await api.post(f"/v1/topics/{topic.id}/compact", json={"confirm": True})
    assert response.status_code == 200
    assert response.json()["status"] == "unchanged" and not response.json()["persisted"]
    assert response.json()["options"]["max_messages"] == 20
    topic, _ = await seed(repository)

    def broken(_messages):
        raise RuntimeError("secret-summary-body")

    runtime.persistent_context.summarizer = broken
    response = await api.post(
        f"/v1/topics/{topic.id}/compact", json={"max_messages": 3, "confirm": True}
    )
    assert (
        response.status_code == 502 and response.json()["error"]["code"] == "context_summary_failed"
    )
    assert "secret-summary-body" not in response.text
    assert not await summaries(repository)


async def test_python_ai_manual_compact_confirmation_and_close_boundary(runtime, repository):
    topic, _ = await seed(repository)
    ai = PythonAI(runtime)
    preview = await ai.compact(topic.id, ContextCompactRequest(max_messages=3))
    assert preview.status == "proposed" and not await summaries(repository)
    confirmed = await ai.compact(
        topic.id,
        ContextCompactRequest(max_messages=3, confirm=True, expected_branch_key=preview.branch_key),
    )
    assert confirmed.status == "completed" and confirmed.persisted
    await ai.close()
    with pytest.raises(ServiceError) as error:
        await ai.compact(topic.id)
    assert error.value.code == "sdk_closed"


async def test_manual_runtime_timeout_cancels_summary_and_releases_topic(repository):
    topic, _ = await seed(repository)
    entered, cancelled = asyncio.Event(), asyncio.Event()

    async def slow_summary(_messages):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    runtime = ChatRuntime(
        repository, ContextRecorder(), ToolRegistry(), Settings(run_timeout_seconds=0.5)
    )
    runtime.persistent_context.summarizer = slow_summary
    try:
        with pytest.raises(ServiceError) as error:
            await runtime.compact(topic.id, ContextCompactRequest(max_messages=3, confirm=True))
        assert error.value.code == "context_timeout" and error.value.status == 504
        assert entered.is_set() and cancelled.is_set()
        assert not await summaries(repository)
        assert not runtime.persistent_context.is_busy(topic.id)
    finally:
        await runtime.close()


async def test_post_hook_timeout_preserves_committed_compact_result(repository):
    topic, _ = await seed(repository)
    entered, cancelled = asyncio.Event(), asyncio.Event()

    async def slow_observer(_event):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    service = PersistentContext(
        repository,
        ContextEngine(),
        post_compact=slow_observer,
        observer_timeout_seconds=0.05,
    )
    outcome = await service.compact(topic.id, options(), confirm=True)
    assert outcome.status == "completed" and outcome.persisted
    assert entered.is_set() and cancelled.is_set()
    assert "post_compact_failed" in outcome.result.processor_trace
    assert len(await summaries(repository)) == 1
