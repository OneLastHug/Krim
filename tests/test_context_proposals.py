"""持久提案的原摘要确认、事务、进程、运行暂停与公开入口验证。"""

import asyncio
import json
import sys
import time
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from pydantic import ValidationError
from sqlalchemy import event, select, text, update
from sqlalchemy.orm import Session

from krim_agent.agents import AgentManager
from krim_agent.api import create_app
from krim_agent.app import LocalApp
from krim_agent.app_sdk import RemoteHostHandle
from krim_agent.application.context import PersistentContext
from krim_agent.client import KrimClient
from krim_agent.config import Settings
from krim_agent.context_engine import (
    ContextCompactRequest,
    ContextEngine,
    ContextOptions,
    ContextProposalDecision,
    ContextRunConfig,
)
from krim_agent.persistence import ContextSummaryRow, Database, KrimRepository
from krim_agent.persistence.context_proposals import ContextProposalStore
from krim_agent.persistence.database import ContextProposalRow, MessageRow
from krim_agent.persistence.migration import migration_config
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.runtime import ChatRuntime
from krim_agent.sdk import ContextNoticePart, ContextProposalPart, PythonAI
from krim_agent.tools import ToolRegistry, tool
from krim_agent.tools.core import ToolExecutionBudget


async def seed(repository, *, count=4):
    topic = await repository.create_topic("提案测试")
    for index in range(count):
        run, _ = await repository.begin_run(
            topic.id, ChatRequest(text=f"历史 {index}", idempotency_key=str(uuid4()))
        )
        await repository.add_message(run, "assistant", f"回答 {index}")
        await repository.finish(run.id, "completed", None, [])
    return topic


async def summary_rows(repository):
    async with repository.database.sessions() as session:
        return list(await session.scalars(select(ContextSummaryRow)))


def options():
    return ContextOptions(strategy="annotated_compact", max_messages=3)


def confirm(revision=0):
    return ContextProposalDecision(action="confirm", revision=revision)


async def test_exact_summary_confirmation_and_replay_call_summarizer_and_hooks_once(repository):
    topic = await seed(repository)
    generated, pre, post = [], [], []

    def summarize(messages):
        generated.append([message.id for message in messages])
        return "审批者实际看过的摘要"

    def before(event_value):
        from krim_agent.application.context import PreCompactDecision

        pre.append(event_value.trigger)
        return PreCompactDecision()

    service = PersistentContext(
        repository,
        ContextEngine(),
        summarize,
        pre_compact=before,
        post_compact=lambda value: post.append(value.persisted),
    )
    original = await repository.message_tree(topic.id)
    preview = await service.compact(topic.id, options(), config_hash="same")
    assert preview.proposal_id and preview.proposal_revision == 0
    assert not await summary_rows(repository)
    assert (await service.proposals.list(topic.id, pending_only=True))[
        0
    ].result.summary == preview.result.summary
    result = await service.compact(
        topic.id,
        options(),
        confirm=True,
        proposal_id=preview.proposal_id,
        proposal_revision=0,
        config_hash="same",
    )
    replay = await service.compact(
        topic.id,
        options(),
        confirm=True,
        proposal_id=preview.proposal_id,
        proposal_revision=0,
        config_hash="same",
    )
    assert result.result.summary == replay.result.summary == "审批者实际看过的摘要"
    assert len(generated) == len(pre) == len(post) == 1
    rows = await summary_rows(repository)
    assert len(rows) == 1 and rows[0].branch_key == result.source_branch_key
    assert rows[0].source_message_ids == preview.result.source_message_ids
    assert (await repository.message_tree(topic.id)) == original
    with pytest.raises(ServiceError, match="revision"):
        await service.decide(topic.id, preview.proposal_id, confirm(99), config_hash="same")


@pytest.mark.parametrize("change", ["path", "content", "details", "prior", "config", "expiry"])
async def test_changed_inputs_or_expiry_cannot_confirm_saved_summary(repository, change):
    topic = await seed(repository)
    service = PersistentContext(repository, ContextEngine())
    messages = await repository.list_messages(topic.id)
    if change == "prior":
        prefix = messages[:2]
        await repository.save_context_summary(
            topic.id,
            repository.context_branch_key(prefix),
            [message.id for message in prefix],
            "先前摘要",
            "annotated_compact",
            "auto",
        )
    preview = await service.compact(topic.id, options(), config_hash="same")
    async with repository.database.sessions.begin() as session:
        if change == "content":
            await session.execute(
                update(MessageRow)
                .where(MessageRow.id == messages[0].id)
                .values(content="同 ID 不同正文")
            )
        elif change == "details":
            await session.execute(
                update(MessageRow)
                .where(MessageRow.id == messages[0].id)
                .values(details={"changed": True})
            )
        elif change == "prior":
            await session.execute(update(ContextSummaryRow).values(summary="替换过的先前摘要"))
        elif change == "expiry":
            await session.execute(
                update(ContextProposalRow)
                .where(ContextProposalRow.id == preview.proposal_id)
                .values(expires_at=time.time() - 1)
            )
    if change == "path":
        edited, _ = await repository.begin_run(
            topic.id,
            ChatRequest(text="另一个分支", idempotency_key="branch-change"),
            action="edit",
            source_message_id=messages[0].id,
        )
        await repository.add_message(edited, "assistant", "另一个回答")
        await repository.finish(edited.id, "completed", None, [])
    with pytest.raises(ServiceError):
        await service.compact(
            topic.id,
            options(),
            confirm=True,
            proposal_id=preview.proposal_id,
            proposal_revision=0,
            config_hash="changed" if change == "config" else "same",
        )
    row = await service.proposals.get(topic.id, preview.proposal_id)
    assert row.status == ("expired" if change == "expiry" else "stale")
    assert len(await summary_rows(repository)) == (1 if change == "prior" else 0)
    assert not await service.proposals.list(topic.id, pending_only=True)


async def test_cross_topic_reject_revision_and_idempotency(repository):
    topic = await seed(repository)
    other = await repository.create_topic("其他话题")
    service = PersistentContext(repository, ContextEngine())
    preview = await service.compact(topic.id, options(), config_hash="same")
    with pytest.raises(ServiceError) as error:
        await service.proposals.get(other.id, preview.proposal_id)
    assert error.value.code == "proposal_not_found"
    with pytest.raises(ServiceError):
        await service.decide(topic.id, preview.proposal_id, confirm(2), config_hash="same")
    request = ContextProposalDecision(action="reject", revision=0)
    rejected = await service.decide(topic.id, preview.proposal_id, request, config_hash="same")
    assert rejected.status == "rejected"
    assert (
        await service.decide(topic.id, preview.proposal_id, request, config_hash="same") == rejected
    )
    with pytest.raises(ServiceError):
        await service.decide(topic.id, preview.proposal_id, confirm(), config_hash="same")
    assert not await summary_rows(repository)


async def test_summary_and_proposal_state_rollback_together_on_storage_failure(repository):
    topic = await seed(repository)
    service = PersistentContext(repository, ContextEngine())
    preview = await service.compact(topic.id, options(), config_hash="same")

    def fail_flush(session, *_args):
        if any(isinstance(row, ContextSummaryRow) for row in session.new):
            raise RuntimeError("模拟结算事务写失败")

    event.listen(Session, "before_flush", fail_flush)
    try:
        with pytest.raises(RuntimeError):
            await service.decide(topic.id, preview.proposal_id, confirm(), config_hash="same")
    finally:
        event.remove(Session, "before_flush", fail_flush)
    assert not await summary_rows(repository)
    current = await service.proposals.get(topic.id, preview.proposal_id)
    assert current.status == "proposed" and current.revision == 0
    assert (
        await service.decide(topic.id, preview.proposal_id, confirm(), config_hash="same")
    ).status == "completed"


async def test_two_database_instances_confirm_once_and_reopen(tmp_path):
    url = f"sqlite+aiosqlite:///{tmp_path / 'shared.sqlite'}"
    first, second = Database(url), Database(url)
    await first.initialize()
    await second.initialize()
    one, two = KrimRepository(first), KrimRepository(second)
    observed = []
    a = PersistentContext(
        one, ContextEngine(), post_compact=lambda value: observed.append(value.persisted)
    )
    b = PersistentContext(
        two, ContextEngine(), post_compact=lambda value: observed.append(value.persisted)
    )
    try:
        topic = await seed(one)
        preview = await a.compact(topic.id, options(), config_hash="same")
        both = await asyncio.gather(
            a.decide(topic.id, preview.proposal_id, confirm(), config_hash="same"),
            b.decide(topic.id, preview.proposal_id, confirm(), config_hash="same"),
        )
        assert all(value.status == "completed" and value.revision == 1 for value in both)
        assert observed == [True] and len(await summary_rows(one)) == 1
    finally:
        await first.close()
        await second.close()
    reopened = Database(url)
    await reopened.initialize()
    try:
        rows = await ContextProposalStore(KrimRepository(reopened)).list(topic.id)
        assert len(rows) == 1 and rows[0].result.summary == preview.result.summary
    finally:
        await reopened.close()


class Recorder:
    name = "proposal-recorder"

    def __init__(self):
        self.inputs = []

    async def stream(self, messages, _tools):
        self.inputs.append(messages)
        yield ModelText(text="确认后回答")


async def wait_proposal(runtime, topic_id):
    async with asyncio.timeout(15):
        while True:
            rows = await runtime.context_proposals(topic_id, pending_only=True)
            if rows:
                return rows[0]
            await asyncio.sleep(0.02)


async def test_run_waits_before_provider_and_sdk_replay_preserves_current_state(repository):
    topic = await seed(repository)
    provider = Recorder()
    runtime = ChatRuntime(
        repository,
        provider,
        ToolRegistry(),
        Settings(
            context_strategy="annotated_compact",
            context_max_messages=3,
            context_compact_mode="confirmation",
            context_notice_messages=3,
        ),
    )
    ai = PythonAI(runtime)
    observed = []
    runtime.persistent_context.post_compact = lambda value: observed.append(value.trigger)
    try:
        stream = await ai.stream_text("新的用户组", topic_id=topic.id, idempotency_key="same")
        proposal = await wait_proposal(runtime, topic.id)
        assert proposal.run_id == stream.run.id and provider.inputs == []
        assert not await summary_rows(repository)
        await ai.decide_context_proposal(topic.id, proposal.id, confirm(proposal.revision))
        result = await stream.collect()
        assert result.run.status == "completed" and len(provider.inputs) == 1
        parts = [part async for part in stream.full_stream()]
        phases = [part.phase for part in parts if isinstance(part, ContextProposalPart)]
        assert phases == ["pending", "resolved"]
        assert len([part for part in parts if isinstance(part, ContextNoticePart)]) == 1
        assert observed == ["automatic"]
        assert proposal.result.summary in provider.inputs[0][0].content
        replay = await ai.stream_text("新的用户组", topic_id=topic.id, idempotency_key="same")
        assert (await replay.collect()).run.id == result.run.id and len(provider.inputs) == 1
        assert not await ai.context_proposals(topic.id, pending_only=True)
    finally:
        await ai.close()
        await runtime.close()


@pytest.mark.parametrize("action", ["reject", "expire", "stop", "timeout"])
async def test_pending_run_terminal_does_not_dispatch_provider(repository, action):
    topic = await seed(repository)
    provider = Recorder()
    runtime = ChatRuntime(
        repository,
        provider,
        ToolRegistry(),
        Settings(
            context_strategy="annotated_compact",
            context_max_messages=3,
            context_compact_mode="confirmation",
            run_timeout_seconds=2 if action == "timeout" else 60,
        ),
    )
    try:
        run = await runtime.start(topic.id, ChatRequest(text="待确认", idempotency_key=action))
        proposal = await wait_proposal(runtime, topic.id)
        if action == "reject":
            await runtime.decide_context_proposal(
                topic.id, proposal.id, ContextProposalDecision(action="reject", revision=0)
            )
        elif action == "expire":
            async with repository.database.sessions.begin() as session:
                await session.execute(
                    update(ContextProposalRow)
                    .where(ContextProposalRow.id == proposal.id)
                    .values(expires_at=time.time() - 1)
                )
        elif action == "stop":
            await runtime.stop(run.id)
        state = await runtime.stream_state(run.id, 0)
        await asyncio.wait_for(state.task, 10)
        terminal = await repository.get_run(run.id)
        assert (
            terminal.error_code
            == {
                "reject": "proposal_rejected",
                "expire": "proposal_expired",
                "stop": "cancelled",
                "timeout": "run_timeout",
            }[action]
        )
        assert provider.inputs == [] and not await summary_rows(repository)
        assert not await runtime.context_proposals(topic.id, pending_only=True)
    finally:
        await runtime.close()


async def test_confirmation_between_model_steps_does_not_repeat_tools(repository):
    invocations = []

    @tool(name="lookup", risk="read")
    async def lookup(value: str) -> str:
        invocations.append(value)
        return value

    class Provider(Recorder):
        async def stream(self, messages, _tools):
            self.inputs.append(messages)
            if len(self.inputs) == 1:
                yield ModelToolCall(id="one", name="lookup", arguments={"value": "真实工具"})
            else:
                yield ModelText(text="工具后回答")

    topic = await seed(repository, count=1)
    registry = ToolRegistry()
    registry.register(lookup)
    provider = Provider()
    runtime = ChatRuntime(
        repository,
        provider,
        registry,
        Settings(
            context_strategy="annotated_compact",
            context_max_messages=4,
            context_compact_mode="confirmation",
        ),
    )
    try:
        run = await runtime.start(topic.id, ChatRequest(text="调用工具", idempotency_key="tool"))
        proposal = await wait_proposal(runtime, topic.id)
        assert proposal.step == 1 and len(provider.inputs) == 1 and invocations == ["真实工具"]
        await runtime.decide_context_proposal(topic.id, proposal.id, confirm())
        state = await runtime.stream_state(run.id, 0)
        await state.task
        assert (await repository.get_run(run.id)).status == "completed"
        assert len(provider.inputs) == 2 and invocations == ["真实工具"]
        assert len(await repository.audits(run.id)) == 1
    finally:
        await runtime.close()


async def test_restart_cancels_run_proposal_but_preserves_manual_proposal(repository):
    manual_topic = await seed(repository)
    runtime = ChatRuntime(repository, Recorder(), ToolRegistry(), Settings())
    preview = await runtime.compact(manual_topic.id, ContextCompactRequest(max_messages=3))
    run_topic = await seed(repository)
    run, _ = await repository.begin_run(
        run_topic.id, ChatRequest(text="重启前", idempotency_key="restart")
    )
    messages = await repository.list_messages(run_topic.id)
    pending = await runtime.persistent_context.prepare_confirmation(
        run_topic.id,
        messages,
        options(),
        engine=ContextEngine(),
        run_id=run.id,
        step=0,
        config_hash="same",
        ttl_seconds=300,
    )
    await repository.recover_interrupted()
    assert (await runtime.context_proposal(run_topic.id, pending.id)).status == "cancelled"
    assert (
        await runtime.context_proposal(manual_topic.id, preview.proposal_id)
    ).status == "proposed"
    assert (await repository.get_run(run.id)).status == "interrupted"
    await runtime.close()


async def test_http_client_cli_and_hosts_use_same_durable_proposal(server_url, runtime, repository):
    topic = await seed(repository)
    with KrimClient(server_url) as client:
        preview = await asyncio.to_thread(client.compact, topic.id, options())
        rows = await asyncio.to_thread(client.context_proposals, topic.id, pending_only=True)
        assert rows[0].id == preview.proposal_id
        assert (
            await asyncio.to_thread(client.context_proposal, topic.id, preview.proposal_id)
        ).result.summary == preview.summary
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "krim_agent",
        "proposal",
        "confirm",
        topic.id,
        preview.proposal_id,
        "--revision",
        "0",
        "--url",
        server_url,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    output, errors = await asyncio.wait_for(process.communicate(), 20)
    assert process.returncode == 0, errors.decode()
    assert json.loads(output)["status"] == "completed"
    app = LocalApp(runtime)
    remote = RemoteHostHandle(server_url, "unused-session")
    try:
        assert (
            await app.handle.context_proposal(topic.id, preview.proposal_id)
        ).status == "completed"
        assert (await remote.context_proposals(topic.id))[0].result.summary == preview.summary
        assert (
            await remote.decide_context_proposal(topic.id, preview.proposal_id, confirm())
        ).status == "completed"
        assert (
            await app.handle.decide_context_proposal(topic.id, preview.proposal_id, confirm())
        ).status == "completed"
    finally:
        await app.close()
        await remote.close()
    assert len(await summary_rows(repository)) == 1


async def test_api_schema_rejects_incomplete_or_cross_topic_requests(api, repository):
    topic = await seed(repository)
    endpoint = f"/v1/topics/{topic.id}/compact"
    response = await api.post(endpoint, json={"max_messages": 3})
    preview = response.json()
    for body in (
        {"proposal_id": preview["proposal_id"], "confirm": True},
        {"proposal_revision": 0},
        {"proposal_id": preview["proposal_id"], "proposal_revision": 0},
    ):
        assert (await api.post(endpoint, json=body)).status_code == 422
    other = await repository.create_topic("其他")
    assert (
        await api.get(f"/v1/topics/{other.id}/compact-proposals/{preview['proposal_id']}")
    ).status_code == 404
    confirmed = await api.post(
        endpoint,
        json={"confirm": True, "proposal_id": preview["proposal_id"], "proposal_revision": 0},
    )
    assert confirmed.status_code == 200 and confirmed.json()["summary"] == preview["summary"]


async def test_proposal_migration_refuses_loss_and_0017_can_upgrade(repository, tmp_path):
    topic = await seed(repository)
    service = PersistentContext(repository, ContextEngine())
    preview = await service.compact(topic.id, options())
    with pytest.raises(RuntimeError, match="压缩提案"):
        async with repository.database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(migration_config(sync), "0017_usage_budgets")
            )
    assert (await service.proposals.get(topic.id, preview.proposal_id)).status == "proposed"
    async with repository.database.engine.connect() as connection:
        assert (
            await connection.execute(text("SELECT version_num FROM alembic_version"))
        ).scalar() == "0018_context_proposals"
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'upgrade.sqlite'}")
    await database.initialize()
    async with database.engine.begin() as connection:
        await connection.run_sync(
            lambda sync: command.downgrade(migration_config(sync), "0017_usage_budgets")
        )
    await database.close()
    reopened = Database(f"sqlite+aiosqlite:///{tmp_path / 'upgrade.sqlite'}")
    await reopened.initialize()
    try:
        async with reopened.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT COUNT(*) FROM context_compact_proposals"))
            ).scalar() == 0
    finally:
        await reopened.close()


def test_confirmation_and_ratio_configuration_is_explicit_and_inherited():
    with pytest.raises(ValidationError):
        Settings(context_compact_mode="confirmation")
    with pytest.raises(ValidationError):
        Settings(context_trigger_ratio=0.8)
    parent = ContextRunConfig(
        strategy="annotated_compact",
        compact_mode="confirmation",
        notice_messages=20,
        proposal_ttl_seconds=120,
    )
    child = ContextRunConfig(strategy="annotated_compact", notice_messages=50).restricted_by(parent)
    assert child.compact_mode == "confirmation" and child.notice_messages == 20
    assert child.proposal_ttl_seconds == 120
    with pytest.raises(ValidationError):
        ContextCompactRequest(proposal_id="missing", confirm=True)


async def test_real_processes_reopen_and_compete_for_same_proposal(tmp_path):
    url = f"sqlite+aiosqlite:///{tmp_path / 'process.sqlite'}"
    worker = str(Path(__file__).parent / "fixtures" / "context_proposal_worker.py")

    async def execute(*args):
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            worker,
            url,
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, errors = await asyncio.wait_for(process.communicate(), 30)
        assert process.returncode == 0, errors.decode()
        return json.loads(output)

    preview = await execute("preview")
    decisions = await asyncio.gather(
        execute("confirm", preview["topic_id"], preview["proposal_id"]),
        execute("confirm", preview["topic_id"], preview["proposal_id"]),
    )
    assert all(
        value == {"status": "completed", "revision": 1, "summary": preview["summary"]}
        for value in decisions
    )
    database = Database(url)
    await database.initialize()
    try:
        assert len(await summary_rows(KrimRepository(database))) == 1
    finally:
        await database.close()


async def test_capacity_ratio_creates_proposal_before_provider_with_frozen_budget(repository):
    topic = await seed(repository)
    async with repository.database.sessions.begin() as session:
        await session.execute(
            update(MessageRow)
            .where(MessageRow.topic_id == topic.id)
            .values(content="长期历史内容 " * 100)
        )
    provider = Recorder()
    runtime = ChatRuntime(
        repository,
        provider,
        ToolRegistry(),
        Settings(
            context_strategy="annotated_compact",
            context_compact_mode="confirmation",
            context_tokenizer="cl100k_base",
            model_context_window_tokens=512,
            context_reserved_output_tokens=16,
            model_framing_reserve_tokens=16,
            context_trigger_ratio=0.5,
        ),
    )
    runtime.persistent_context.summarizer = lambda _messages: "历史摘要"
    try:
        run = await runtime.start(topic.id, ChatRequest(text="当前问题", idempotency_key="ratio"))
        proposal = await wait_proposal(runtime, topic.id)
        assert proposal.options.max_tokens == 256 and provider.inputs == []
        start = (await repository.list_events(run.id))[0]
        assert start.data["context_config"]["trigger_ratio"] == 0.5
        await runtime.decide_context_proposal(topic.id, proposal.id, confirm())
        state = await runtime.stream_state(run.id, 0)
        await state.task
        assert (await repository.get_run(run.id)).status == "completed" and len(
            provider.inputs
        ) == 1
    finally:
        await runtime.close()


async def test_never_policy_still_enforces_hard_window_without_proposal(repository):
    topic = await repository.create_topic("硬窗口")
    provider = Recorder()
    runtime = ChatRuntime(
        repository,
        provider,
        ToolRegistry(),
        Settings(
            context_strategy="annotated_compact",
            context_compact_mode="confirmation",
            context_policy="never",
            context_tokenizer="cl100k_base",
            model_context_window_tokens=256,
            context_reserved_output_tokens=16,
        ),
    )
    try:
        run = await runtime.start(
            topic.id, ChatRequest(text="超过窗口的正文 " * 500, idempotency_key="never")
        )
        state = await runtime.stream_state(run.id, 0)
        await state.task
        assert (await repository.get_run(run.id)).error_code == "model_input_budget"
        assert provider.inputs == [] and not await runtime.context_proposals(topic.id)
    finally:
        await runtime.close()


async def test_large_retained_context_uses_bounded_proposal_events(repository):
    topic = await seed(repository)
    messages = await repository.list_messages(topic.id)
    async with repository.database.sessions.begin() as session:
        await session.execute(
            update(MessageRow)
            .where(MessageRow.id == messages[-1].id)
            .values(content="较大保留上下文" * 30000)
        )
    provider = Recorder()
    runtime = ChatRuntime(
        repository,
        provider,
        ToolRegistry(),
        Settings(
            context_strategy="annotated_compact",
            context_max_messages=5,
            context_compact_mode="confirmation",
        ),
    )
    runtime.persistent_context.summarizer = lambda _messages: "历史摘要"
    try:
        run = await runtime.start(topic.id, ChatRequest(text="当前问题", idempotency_key="large"))
        proposal = await wait_proposal(runtime, topic.id)
        assert len(proposal.model_dump_json().encode()) > 131072
        await runtime.decide_context_proposal(topic.id, proposal.id, confirm())
        state = await runtime.stream_state(run.id, 0)
        await state.task
        assert (await repository.get_run(run.id)).status == "completed"
        events = [
            frame
            for frame in await repository.list_events(run.id)
            if isinstance(frame.data, dict) and frame.data.get("type") == "context_compact_proposal"
        ]
        assert len(events) == 2 and all(
            len(frame.model_dump_json().encode()) < 2048 for frame in events
        )
        assert events[0].data["proposal"]["source_count"] == len(proposal.result.source_message_ids)
        assert "messages" not in events[0].data["proposal"]
    finally:
        await runtime.close()


async def test_other_runtime_decides_using_producer_snapshot_and_producer_hook(repository):
    topic = await seed(repository)
    provider = Recorder()
    producer = ChatRuntime(
        repository,
        provider,
        ToolRegistry(),
        Settings(
            context_strategy="annotated_compact",
            context_max_messages=3,
            context_compact_mode="confirmation",
            context_tokenizer="cl100k_base",
        ),
    )
    responder = ChatRuntime(repository, Recorder(), ToolRegistry(), Settings())
    observed, wrong = [], []
    producer.persistent_context.post_compact = lambda value: observed.append(value.trigger)
    responder.persistent_context.post_compact = lambda value: wrong.append(value.trigger)
    try:
        run = await producer.start(topic.id, ChatRequest(text="子运行", idempotency_key="snapshot"))
        proposal = await wait_proposal(producer, topic.id)
        await responder.decide_context_proposal(topic.id, proposal.id, confirm())
        state = await producer.stream_state(run.id, 0)
        await state.task
        assert (await repository.get_run(run.id)).status == "completed"
        assert observed == ["automatic"] and wrong == [] and len(provider.inputs) == 1
    finally:
        await producer.close()
        await responder.close()


async def test_context_confirmation_pauses_only_borrowed_tool_deadline(repository):
    topic = await seed(repository)
    provider = Recorder()
    runtime = None
    async with asyncio.timeout(5) as deadline:
        budget = ToolExecutionBudget(deadline)
        runtime = ChatRuntime(
            repository,
            provider,
            ToolRegistry(),
            Settings(
                context_strategy="annotated_compact",
                context_max_messages=3,
                context_compact_mode="confirmation",
                run_timeout_seconds=20,
            ),
            execution_timeouts=(budget,),
        )
        try:
            run = await runtime.start(
                topic.id, ChatRequest(text="继承工具预算", idempotency_key="borrowed")
            )
            proposal = await wait_proposal(runtime, topic.id)
            async with asyncio.timeout(5):
                for _ in range(500):
                    if budget.waiters != 0:
                        break
                    await asyncio.sleep(0.01)
                else:
                    pytest.fail("提案等待没有暂停借用的 deadline")
            assert deadline.when() is None and budget.remaining > 0
            await asyncio.sleep(5.25)
            assert (await repository.get_run(run.id)).status == "running"
            assert provider.inputs == [] and not deadline.expired()
            await runtime.decide_context_proposal(topic.id, proposal.id, confirm())
            state = await runtime.stream_state(run.id, 0)
            await state.task
            assert (await repository.get_run(run.id)).status == "completed"
            assert budget.waiters == 0 and deadline.when() is not None
        finally:
            await runtime.close()


async def test_default_confirmation_fields_preserve_committed_0017_agent_fingerprint():
    # 固定值来自与提交 2ec3a7a 的 types.py/manager.py 字节一致的旧 wheel，
    # 真实旧进程已创建该 BPE 持久句柄，再由新进程恢复到第二轮。
    app = create_app(
        Settings(
            database_url="sqlite+aiosqlite:///:memory:",
            context_tokenizer="cl100k_base",
            context_reserved_output_tokens=16,
            model_context_window_tokens=2048,
        )
    )
    async with app.router.lifespan_context(app):
        definition = app.state.agents.catalog.get("default").definition
        digest = AgentManager.definition_hash(definition)
        assert digest == "13909543be3a8fefe30f125b923169a223006b6b204471af4d82df89883f99f5"
        changed = replace(
            definition,
            context_config=definition.context_config.model_copy(update={"notice_messages": 20}),
        )
        assert AgentManager.definition_hash(changed) != digest
