import asyncio

import httpx
import pytest
from alembic import command
from pydantic import ValidationError
from sqlalchemy import select, text

from krim_agent.api import create_app
from krim_agent.app import LocalApp
from krim_agent.client import KrimClient
from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.database import RunRow
from krim_agent.persistence.migration import migration_config
from krim_agent.prompt_context import ContextTagInput, MentionInput, MentionOccurrence
from krim_agent.protocol.domain import ChatRequest, EditRequest, RegenerateRequest, ServiceError
from krim_agent.providers import ModelText
from krim_agent.runtime import ChatRuntime
from krim_agent.sdk import PythonAI
from krim_agent.tools import ToolRegistry


class RecordingProvider:
    name = "offline-prompt-context"

    def __init__(self):
        self.messages = []

    async def stream(self, messages, _tools):
        self.messages.append(messages)
        yield ModelText(text="引用与背景已收到")


async def finish(runtime, run):
    task = runtime.production_task(run.id)
    assert task is not None
    await asyncio.wait_for(asyncio.shield(task), 5)
    return await runtime.repository.get_run(run.id)


async def test_directory_crud_ids_topic_binding_overwrite_and_clear(repository):
    one, two = await repository.create_topic("one"), await repository.create_topic("two")
    mention = await repository.add_mention(
        one.id,
        MentionInput(
            kind="file", label="doc.txt", content="明确提供的内容", metadata={"path": "/not/read"}
        ),
    )
    assert mention.id and mention.label == "doc.txt"
    with pytest.raises(ServiceError) as missing:
        await repository.remove_mention(two.id, mention.id)
    assert missing.value.code == "mention_not_found"
    await repository.add_context_tag(one.id, ContextTagInput(name="env", content="local"))
    await repository.add_context_tag(one.id, ContextTagInput(name="env", content="test"))
    context = await repository.get_prompt_context(one.id)
    assert len(context.context_tags) == 1 and context.context_tags[0].content == "test"
    assert not (await repository.get_prompt_context(two.id)).mentions
    assert not (await repository.remove_mention(one.id, mention.id)).mentions
    assert not (await repository.remove_context_tag(one.id)).context_tags
    with pytest.raises(ServiceError):
        await repository.remove_context_tag(one.id, "missing")


async def test_unicode_snapshots_survive_directory_change(
    repository,
):
    provider = RecordingProvider()
    runtime = ChatRuntime(repository, provider, ToolRegistry(), Settings())
    try:
        topic = await repository.create_topic("unicode")
        ref = await repository.add_mention(
            topic.id, MentionInput(label="文档", content="引用中的真值")
        )
        await repository.add_context_tag(
            topic.id, ContextTagInput(name="工作区", content="原始背景")
        )
        body = "😀 @文档"  # Unicode code point 为 start=2；UTF-16 位置为 3。
        request = ChatRequest(
            text=body,
            idempotency_key="once",
            mentions=[MentionOccurrence(mention_id=ref.id, start=2, end=5)],
        )
        run = await runtime.start(topic.id, request)
        assert (await finish(runtime, run)).status == "completed"
        saved = (await repository.run_messages(run.id))[0]
        assert (
            saved.content == body and saved.details["prompt_context"]["mentions"][0]["start"] == 2
        )
        model = provider.messages[0][-1]
        assert (
            model.role == "user" and "引用中的真值" in model.content and "原始背景" in model.content
        )
        assert all(message.role != "system" for message in provider.messages[0])
        await repository.remove_mention(topic.id, ref.id)
        await repository.add_context_tag(topic.id, ContextTagInput(name="工作区", content="新背景"))
        retry = await runtime.start(topic.id, request)
        assert retry.id == run.id and len(provider.messages) == 1
        assert (await repository.run_messages(run.id))[0].details == saved.details
        regenerated = await runtime.regenerate(
            topic.id, saved.id, RegenerateRequest(idempotency_key="regenerate")
        )
        assert (await finish(runtime, regenerated)).status == "completed"
        assert (
            "原始背景" in provider.messages[-1][-1].content
            and "新背景" not in provider.messages[-1][-1].content
        )
    finally:
        await runtime.close()


@pytest.mark.parametrize(
    "case", ["missing", "foreign", "overlap", "out_of_bounds", "wrong_text", "utf16"]
)
async def test_invalid_occurrences_rollback_run_and_user_message(repository, case):
    topic, foreign = (
        await repository.create_topic("topic"),
        await repository.create_topic("foreign"),
    )
    ref = await repository.add_mention(topic.id, MentionInput(label="doc", content="local data"))
    other = await repository.add_mention(
        foreign.id, MentionInput(label="doc", content="foreign data")
    )
    occurrence = MentionOccurrence(mention_id=ref.id, start=2, end=6)
    values = [occurrence]
    if case == "missing":
        values = [occurrence.model_copy(update={"mention_id": "missing"})]
    elif case == "foreign":
        values = [occurrence.model_copy(update={"mention_id": other.id})]
    elif case == "overlap":
        values.append(occurrence)
    elif case == "out_of_bounds":
        values = [occurrence.model_copy(update={"end": 20})]
    elif case == "wrong_text":
        values = [occurrence.model_copy(update={"start": 0, "end": 4})]
    elif case == "utf16":
        values = [occurrence.model_copy(update={"start": 3, "end": 7})]
    with pytest.raises(ServiceError) as invalid:
        await repository.begin_run(
            topic.id, ChatRequest(text="😀 @doc", idempotency_key=case, mentions=values)
        )
    assert invalid.value.code in {"mention_not_found", "mention_position_invalid"}
    assert not await repository.list_messages(topic.id)
    async with repository.database.sessions() as session:
        assert not list(await session.scalars(select(RunRow).where(RunRow.topic_id == topic.id)))


async def test_metadata_list_limits_and_no_partial_directory_write(repository):
    topic = await repository.create_topic("limit")
    for index in range(32):
        await repository.add_mention(topic.id, MentionInput(label=f"doc{index}", content="x"))
    before = await repository.get_prompt_context(topic.id)
    with pytest.raises(ServiceError) as limit:
        await repository.add_mention(topic.id, MentionInput(label="overflow", content="x"))
    assert limit.value.code == "prompt_context_limit"
    assert await repository.get_prompt_context(topic.id) == before
    with pytest.raises(ValidationError):
        MentionInput(label="x", content="x", metadata={"huge": "x" * 8193})
    with pytest.raises(ValidationError):
        MentionInput(label="x\n", content="x")


async def test_snapshot_consumes_character_budget_before_provider(repository):
    provider = RecordingProvider()
    runtime = ChatRuntime(repository, provider, ToolRegistry(), Settings(context_max_chars=128))
    try:
        topic = await repository.create_topic("budget")
        await repository.add_context_tag(
            topic.id, ContextTagInput(name="large", content="用户上下文" * 100)
        )
        run = await runtime.start(topic.id, ChatRequest(text="short", idempotency_key="budget"))
        completed = await finish(runtime, run)
        assert completed.status == "failed" and completed.error_code.startswith("context_")
        assert not provider.messages
    finally:
        await runtime.close()


async def test_empty_request_retains_legacy_digest_and_edit_has_new_snapshot(repository):
    provider = RecordingProvider()
    runtime = ChatRuntime(repository, provider, ToolRegistry(), Settings())
    try:
        topic = await repository.create_topic("edit")
        initial = await runtime.start(topic.id, ChatRequest(text="plain", idempotency_key="plain"))
        await finish(runtime, initial)
        source = (await repository.run_messages(initial.id))[0]
        assert source.details == {}
        ref = await repository.add_mention(
            topic.id, MentionInput(label="doc", content="new content")
        )
        edited = await runtime.edit(
            topic.id,
            source.id,
            EditRequest(
                text="@doc",
                idempotency_key="edit",
                mentions=[MentionOccurrence(mention_id=ref.id, start=0, end=4)],
            ),
        )
        assert (await finish(runtime, edited)).status == "completed"
        saved = (await repository.run_messages(edited.id))[0]
        assert saved.details["edited_from"] == source.id
        assert saved.details["prompt_context"]["mentions"][0]["content"] == "new content"
        assert (await repository.run_messages(initial.id))[0].details == {}
    finally:
        await runtime.close()


async def test_api_and_local_host_directory_and_python_ai_send(repository):
    runtime = ChatRuntime(repository, RecordingProvider(), ToolRegistry(), Settings())
    api_app = create_app(runtime=runtime)
    local = LocalApp(runtime)
    try:
        async with (
            api_app.router.lifespan_context(api_app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api_app), base_url="http://127.0.0.1"
            ) as api,
        ):
            topic = await local.handle.create_topic("host")
            ref = await local.handle.add_mention(
                topic.id, MentionInput(label="doc", content="宿主来源")
            )
            await local.handle.add_context_tag(
                topic.id, ContextTagInput(name="env", content="本地")
            )
            context = (await api.get(f"/v1/topics/{topic.id}/prompt-context")).json()
            assert context["mentions"][0]["id"] == ref.id
            async with PythonAI(runtime) as ai:
                result = await ai.generate_text(
                    "@doc",
                    topic_id=topic.id,
                    mentions=[MentionOccurrence(mention_id=ref.id, start=0, end=4)],
                )
                assert result.run.status == "completed"
            assert not (await local.handle.clear_mentions(topic.id)).mentions
            removed = await api.delete(
                f"/v1/topics/{topic.id}/context-tags", params={"name": "env"}
            )
            assert removed.status_code == 200 and not removed.json()["context_tags"]
            assert (
                await api.post(
                    f"/v1/topics/{topic.id}/mentions",
                    json={"id": "forged", "label": "x", "content": "x"},
                )
            ).status_code == 422
    finally:
        await local.close()
        await runtime.close()


async def test_prompt_context_reopen_and_nonempty_downgrade_protection(tmp_path):
    url = f"sqlite+aiosqlite:///{tmp_path / 'context.sqlite'}"
    database = Database(url)
    await database.initialize()
    repository = KrimRepository(database)
    topic = await repository.create_topic("reopen")
    ref = await repository.add_mention(topic.id, MentionInput(label="doc", content="持久引用"))
    await database.close()
    reopened = Database(url)
    try:
        await reopened.initialize()
        repository = KrimRepository(reopened)
        assert (await repository.get_prompt_context(topic.id)).mentions[0].id == ref.id
        with pytest.raises(RuntimeError, match="引用/背景标签"):
            async with reopened.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(migration_config(sync), "0009_user_interactions")
                )
        async with reopened.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar() == "0055_directory_restore_plans"
        await repository.remove_mention(topic.id)
        async with reopened.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(migration_config(sync), "0009_user_interactions")
            )
        await reopened.initialize()
        assert not (await repository.get_prompt_context(topic.id)).mentions
    finally:
        await reopened.close()


async def test_python_client_crud_and_send_use_real_tcp(server_url, runtime):
    def scenario():
        with KrimClient(server_url) as client:
            topic = client.create_topic("client context")
            reference = client.add_mention(topic.id, MentionInput(label="doc", content="TCP 引用"))
            client.add_context_tag(topic.id, ContextTagInput(name="env", content="TCP 背景"))
            assert client.prompt_context(topic.id).mentions[0].id == reference.id
            run = client.send(
                topic.id,
                "@doc",
                "tcp-context",
                mentions=[MentionOccurrence(mention_id=reference.id, start=0, end=4)],
            )
            frames = list(client.events(run.id))
            assert frames[-1].kind == "end"
            messages = client.run_messages(run.id)
            assert messages[0].details["prompt_context"]["mentions"][0]["content"] == "TCP 引用"
            assert "TCP 背景" in messages[-1].content
            assert not client.remove_mention(topic.id).mentions
            assert not client.remove_context_tag(topic.id).context_tags
            return run

    run = await asyncio.to_thread(scenario)
    assert (await runtime.repository.get_run(run.id)).status == "completed"


async def test_fake_commands_keep_original_text_while_context_is_budgeted(repository):
    from krim_agent.providers import FakeProvider
    from krim_agent.tools.builtins import builtin_registry

    runtime = ChatRuntime(repository, FakeProvider(delay=0), builtin_registry(), Settings())
    try:
        topic = await repository.create_topic("fake command")
        await repository.add_context_tag(topic.id, ContextTagInput(name="env", content="local"))
        run = await runtime.start(topic.id, ChatRequest(text="/add 2 3", idempotency_key="add"))
        assert (await finish(runtime, run)).status == "completed"
        assert "5.0" in (await repository.run_messages(run.id))[-1].content
        assert len(await repository.audits(run.id)) == 1
    finally:
        await runtime.close()
