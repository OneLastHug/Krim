import asyncio
import json

import httpx
import pytest
from alembic import command
from pydantic import ValidationError
from sqlalchemy import select, text

from krim_agent.agents import AgentDefinition, AgentManager
from krim_agent.api import create_app
from krim_agent.app import LocalApp
from krim_agent.app_sdk import RemoteHostHandle
from krim_agent.client import KrimClient
from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.database import RunRow
from krim_agent.persistence.migration import migration_config
from krim_agent.protocol.domain import ChatRequest, EditRequest, RegenerateRequest, ServiceError
from krim_agent.providers import FakeProvider, ModelText, ModelToolCall
from krim_agent.runtime import ChatRuntime
from krim_agent.sdk import PythonAI, SkillLoadedPart
from krim_agent.skills import (
    SkillBatch,
    SkillDefinition,
    SkillReadInput,
    SkillResource,
    SkillSnapshot,
)
from krim_agent.tools import ToolContext, ToolRegistry, tool


def definition(name="review", *, loading="deferred", content="BODY_MARKER_原始正文"):
    return SkillDefinition(
        name=name,
        description="检查代码并给出可执行建议",
        content=content,
        loading=loading,
        allowed_tools=["write_value"],
        resources=[SkillResource(path="references/guide.md", content="RESOURCE_MARKER_原始资源")],
    )


class RecordingProvider:
    name = "offline-skills-recording"

    def __init__(self, *, read=True, gate=False, write=False):
        self.messages, self.tools = [], []
        self.read, self.write = read, write
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        if not gate:
            self.release.set()

    async def stream(self, messages, tools):
        self.messages.append(messages)
        self.tools.append([item.name for item in tools])
        latest = max(index for index, item in enumerate(messages) if item.role == "user")
        results = [item for item in messages[latest + 1 :] if item.role == "tool"]
        if not results:
            self.entered.set()
            await self.release.wait()
        if not self.read:
            yield ModelText(text="只读取了元数据")
        elif not results:
            yield ModelToolCall(id="body", name="read_skill", arguments={"name": "review"})
        elif len(results) == 1:
            yield ModelToolCall(
                id="resource",
                name="read_skill",
                arguments={"name": "review", "resource": "references/guide.md"},
            )
        elif self.write and len(results) == 2:
            yield ModelToolCall(id="write", name="write_value", arguments={"value": "changed"})
        else:
            yield ModelText(text=";".join(item.content for item in results))


async def finish(runtime, run):
    task = runtime.production_task(run.id)
    assert task is not None
    await asyncio.wait_for(asyncio.shield(task), 10)
    return await runtime.repository.get_run(run.id)


async def test_catalog_merges_replaces_resources_and_is_topic_scoped(repository):
    one, two = await repository.create_topic("one"), await repository.create_topic("two")
    view = await repository.inject_skills(
        one.id, SkillBatch(skills=[definition(), definition("other")])
    )
    assert [item.name for item in view.skills] == ["other", "review"]
    assert "BODY_MARKER" not in view.model_dump_json()
    changed = definition(content="更新正文")
    changed.resources = []
    await repository.inject_skills(one.id, SkillBatch(skills=[changed]))
    assert (await repository.read_skill(one.id, "review")).content == "更新正文"
    assert not (await repository.skill_catalog(two.id)).skills
    for topic, resource, code in [
        (two.id, None, "skill_not_found"),
        (one.id, "references/guide.md", "skill_resource_not_found"),
    ]:
        with pytest.raises(ServiceError) as invalid:
            await repository.read_skill(topic, "review", resource)
        assert invalid.value.code == code
    with pytest.raises(ServiceError) as missing:
        await repository.remove_skill(two.id, "review")
    assert missing.value.code == "skill_not_found"
    assert [item.name for item in (await repository.remove_skill(one.id, "review")).skills] == [
        "other"
    ]
    assert not (await repository.remove_skill(one.id)).skills


@pytest.mark.parametrize("name", ["Review", "-review", "review-", "review--v2", "../review"])
def test_invalid_names_are_rejected(name):
    with pytest.raises(ValidationError):
        SkillDefinition(name=name, description="x", content="y")


@pytest.mark.parametrize(
    "path",
    [
        "/etc/passwd",
        "../secret",
        "references/../secret",
        "x//y",
        "https://host/x",
        "x\\y",
        "%2e%2e/secret",
        "SKILL.md",
        "x\ny",
    ],
)
def test_resource_keys_reject_paths_urls_aliases_and_controls(path):
    with pytest.raises(ValidationError):
        SkillResource(path=path, content="data")
    with pytest.raises(ValidationError):
        SkillReadInput(name="review", resource=path)


@pytest.mark.parametrize("content", ["😀" * 8192, "\x00" * 8192])
def test_body_serialized_limit_fits_doubled_tool_result_stream_envelope(content):
    with pytest.raises(ValidationError):
        definition(content=content)
    with pytest.raises(ValidationError):
        SkillResource(path="references/text.md", content=content)


async def test_capacity_duplicates_and_oversized_batches_have_no_partial_write(repository):
    topic = await repository.create_topic("limit")
    for start in (0, 16):
        await repository.inject_skills(
            topic.id,
            SkillBatch(
                skills=[definition(f"s-{n}", content="x") for n in range(start, start + 16)]
            ),
        )
    before = await repository.skill_catalog(topic.id)
    with pytest.raises(ServiceError) as limit:
        await repository.inject_skills(
            topic.id,
            SkillBatch(skills=[definition("s-0", content="modified"), definition("overflow")]),
        )
    assert limit.value.code == "skill_limit" and await repository.skill_catalog(topic.id) == before
    assert (await repository.read_skill(topic.id, "s-0")).content == "x"
    with pytest.raises(ValidationError):
        SkillBatch(skills=[definition(), definition()])
    with pytest.raises(ValidationError):
        SkillBatch(skills=[definition(f"long-{n}", content="文" * 8192) for n in range(3)])


async def test_progressive_metadata_body_resource_sdk_events_and_permissions(repository):
    effects, registry = [], ToolRegistry()

    @tool(risk="write")
    async def write_value(value: str):
        effects.append(value)
        return value

    registry.register(write_value)
    provider = RecordingProvider(write=True)
    runtime = ChatRuntime(repository, provider, registry, Settings())
    try:
        topic = await repository.create_topic("progressive")
        await repository.inject_skills(topic.id, SkillBatch(skills=[definition()]))
        async with PythonAI(runtime) as ai:
            stream = await ai.stream_text("执行检查", topic_id=topic.id)
            result = await stream.collect()
            parts = [part async for part in stream.full_stream()]
        assert result.run.status == "completed"
        first = json.dumps(
            [item.model_dump(mode="json") for item in provider.messages[0]], ensure_ascii=False
        )
        assert "检查代码" in first and "references/guide.md" in first
        assert "BODY_MARKER" not in first and "RESOURCE_MARKER" not in first
        assert "read_skill" in provider.tools[0]
        assert "BODY_MARKER" in provider.messages[1][-1].content
        assert "RESOURCE_MARKER" in provider.messages[2][-1].content
        assert all(item.role != "system" for group in provider.messages for item in group)
        loaded = [part for part in parts if isinstance(part, SkillLoadedPart)]
        assert [(part.name, part.resource) for part in loaded] == [
            ("review", None),
            ("review", "references/guide.md"),
        ]
        assert not effects
        assert [(item.name, item.ok, item.code) for item in result.audits] == [
            ("read_skill", True, None),
            ("read_skill", True, None),
            ("write_value", False, "permission_denied"),
        ]
    finally:
        await runtime.close()


async def test_running_snapshot_directory_changes_idempotency_regeneration_and_edit(repository):
    provider = RecordingProvider(gate=True)
    runtime = ChatRuntime(repository, provider, ToolRegistry(), Settings())
    try:
        topic = await repository.create_topic("immutable")
        await repository.inject_skills(topic.id, SkillBatch(skills=[definition()]))
        request = ChatRequest(text="inspect", idempotency_key="once")
        run = await runtime.start(topic.id, request)
        await asyncio.wait_for(provider.entered.wait(), 5)
        replacement = definition(content="BODY_NEW_新正文")
        replacement.resources[0].content = "RESOURCE_NEW_新资源"
        await repository.inject_skills(topic.id, SkillBatch(skills=[replacement]))
        provider.release.set()
        assert (await finish(runtime, run)).status == "completed"
        before = await repository.run_messages(run.id)
        assert "BODY_MARKER" in before[-1].content and "BODY_NEW" not in before[-1].content
        assert (await runtime.start(topic.id, request)).id == run.id
        regenerated = await runtime.regenerate(
            topic.id, before[0].id, RegenerateRequest(idempotency_key="regenerate")
        )
        assert (await finish(runtime, regenerated)).status == "completed"
        assert "BODY_MARKER" in (await repository.run_messages(regenerated.id))[-1].content
        edited = await runtime.edit(
            topic.id,
            before[0].id,
            EditRequest(text="inspect-new", idempotency_key="edit", skill_names=["review"]),
        )
        assert (await finish(runtime, edited)).status == "completed"
        assert "BODY_NEW" in (await repository.run_messages(edited.id))[-1].content
        assert await repository.run_messages(run.id) == before
    finally:
        await runtime.close()


@pytest.mark.parametrize("choice", ["always", "selected"])
async def test_eager_and_explicit_selection_are_budgeted_before_provider(repository, choice):
    topic = await repository.create_topic("budget")
    provider = RecordingProvider(read=False)
    runtime = ChatRuntime(repository, provider, ToolRegistry(), Settings(context_max_chars=1200))
    try:
        skill = definition(
            loading="always" if choice == "always" else "deferred", content="正文" * 2000
        )
        await repository.inject_skills(topic.id, SkillBatch(skills=[skill]))
        run = await runtime.start(
            topic.id,
            ChatRequest(
                text="inspect",
                idempotency_key="budget",
                skill_names=["review"] if choice == "selected" else [],
            ),
        )
        done = await finish(runtime, run)
        assert done.status == "failed" and done.error_code.startswith("context_")
        assert not provider.messages
    finally:
        await runtime.close()


async def test_invalid_selection_rolls_back_and_empty_selection_retains_old_digest(repository):
    topic = await repository.create_topic("invalid")
    await repository.inject_skills(topic.id, SkillBatch(skills=[definition()]))
    for names in [["missing"], ["review", "review"]]:
        with pytest.raises(ServiceError) as invalid:
            await repository.begin_run(
                topic.id, ChatRequest(text="hello", idempotency_key=str(names), skill_names=names)
            )
        assert invalid.value.code == "skill_selection_invalid"
        assert not await repository.list_messages(topic.id)
    async with repository.database.sessions() as session:
        assert not list(await session.scalars(select(RunRow).where(RunRow.topic_id == topic.id)))
    assert ChatRequest(text="old", idempotency_key="old").skill_names == []


async def test_api_local_app_and_host_relay_apply_actual_directory(runtime):
    app, local = create_app(runtime=runtime), LocalApp(runtime)
    try:
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
            ) as api,
        ):
            topic = await local.handle.create_topic("API")
            await local.handle.inject_skills(topic.id, SkillBatch(skills=[definition()]))
            assert (await api.get(f"/v1/topics/{topic.id}/skills")).json()["skills"][0][
                "name"
            ] == "review"
            assert (await api.get(f"/v1/topics/{topic.id}/skills/review")).json()[
                "content"
            ] == definition().content
            assert (
                await api.get(
                    f"/v1/topics/{topic.id}/skills/review", params={"resource": "../secret"}
                )
            ).status_code == 422
            assert (await api.delete(f"/v1/topics/{topic.id}/skills/missing")).status_code == 404
            sid = (await api.post("/v1/host/sessions")).json()["id"]
            await api.post(f"/v1/host/sessions/{sid}/ready")
            async with RemoteHostHandle(
                "http://127.0.0.1", sid, transport=httpx.ASGITransport(app=app)
            ) as host:
                reader = app.state.host_sessions.events(sid, "app", 0)
                pending = asyncio.create_task(host.remove_skill(topic.id, "review"))
                frame = await asyncio.wait_for(anext(reader), 3)
                result = await local.handle.emit(frame.data["channel"], frame.data["data"])
                reply = await api.post(
                    f"/v1/host/sessions/{sid}/reply",
                    json={
                        "protocol_version": 2,
                        "kind": "reply",
                        "id": frame.data["id"],
                        "channel": frame.data["channel"],
                        "data": result.model_dump(mode="json"),
                    },
                )
                assert reply.status_code == 200 and not (await pending).skills
                await reader.aclose()
            assert not (await local.handle.skills(topic.id)).skills
    finally:
        await local.close()


async def test_real_tcp_client_crud_and_selected_send(server_url):
    def scenario():
        with KrimClient(server_url) as client:
            topic = client.create_topic("TCP")
            client.inject_skills(topic.id, SkillBatch(skills=[definition()]))
            assert client.skills(topic.id).skills[0].name == "review"
            assert client.read_skill(topic.id, "review", "references/guide.md").content.startswith(
                "RESOURCE_MARKER"
            )
            run = client.send(topic.id, "hello", "selected", skill_names=["review"])
            assert list(client.events(run.id))[-1].kind == "end"
            assert "BODY_MARKER" in client.run_messages(run.id)[-1].content
            assert not client.remove_skill(topic.id).skills

    await asyncio.to_thread(scenario)


async def test_subagent_inherits_snapshot_but_cannot_widen_parent_policy(repository):
    manager = AgentManager(repository)
    registry = ToolRegistry()
    snapshot = SkillSnapshot(skills=[definition()])

    async def deny_read(_definition, _context):
        return "deny"

    try:
        handle = await manager.spawn(
            AgentDefinition(name="child", provider=FakeProvider(delay=0), tools=registry),
            parent_context=ToolContext(registry, policy=deny_read, skill_snapshot=snapshot),
        )
        result = await handle.execute("/skill review")
        assert result.status == "completed" and "BODY_MARKER" not in result.text
        assert (await repository.skill_catalog(handle.topic_id)).skills[0].name == "review"
        assert (await repository.audits(result.run_id))[0].code == "permission_denied"
    finally:
        await manager.close()


async def test_reopen_snapshot_and_nonempty_migration_downgrade_protection(tmp_path):
    url = f"sqlite+aiosqlite:///{tmp_path / 'skills.sqlite'}"
    database = Database(url)
    await database.initialize()
    repository = KrimRepository(database)
    topic = await repository.create_topic("reopen")
    await repository.inject_skills(topic.id, SkillBatch(skills=[definition()]))
    run, _ = await repository.begin_run(topic.id, ChatRequest(text="hello", idempotency_key="save"))
    await database.close()
    reopened = Database(url)
    try:
        await reopened.initialize()
        repository = KrimRepository(reopened)
        assert (await repository.run_skill_snapshot(run.id)).skills[
            0
        ].content == definition().content
        with pytest.raises(RuntimeError, match="Skills"):
            async with reopened.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(migration_config(sync), "0011_locale")
                )
        assert (await repository.skill_catalog(topic.id)).skills[0].name == "review"
        await repository.remove_skill(topic.id)
        async with reopened.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(migration_config(sync), "0011_locale")
            )
        await reopened.initialize()
        assert not (await repository.skill_catalog(topic.id)).skills
        assert (await repository.run_skill_snapshot(run.id)).skills[0].name == "review"
        async with reopened.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar() == "0051_checkpoint_restore_models"
    finally:
        await reopened.close()
