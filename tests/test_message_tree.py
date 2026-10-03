import asyncio
from datetime import UTC, datetime

import pytest

from krim_agent.app import LocalApp
from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.tree import MessageTree
from krim_agent.protocol.domain import (
    ChatRequest,
    EditRequest,
    MessageView,
    RegenerateRequest,
    ServiceError,
)
from krim_agent.providers import FakeProvider, ModelText
from krim_agent.runtime import ChatRuntime
from krim_agent.tools import ToolRegistry, tool
from krim_agent.tools.builtins import builtin_registry


async def finish(runtime, run):
    state = await runtime.stream_state(run.id, 0)
    frames = [frame async for frame in runtime.subscribe(state) if frame is not None]
    assert frames[-1].data["status"] == "completed"


def node(identifier, parent=None, *, topic="topic", role="user"):
    return MessageView(
        id=identifier,
        parent_id=parent,
        topic_id=topic,
        run_id="run",
        sequence=1,
        role=role,
        content=identifier,
        details={},
        created_at=datetime(2026, 10, 3, tzinfo=UTC),
    )


def test_root_and_sibling_sorting_is_stable_and_null_ends_a_path():
    tree = MessageTree([node("b"), node("a"), node("z", "a"), node("y", "a")])
    assert [item.id for item in tree.active_path({})] == ["a", "y"]
    assert [item.id for item in tree.active_path({"__root__": "b"})] == ["b"]
    assert [item.id for item in tree.active_path({"a": None})] == ["a"]
    assert [item.id for item in tree.active_path(tree.select("z", {}))] == ["a", "z"]


@pytest.mark.parametrize(
    "nodes",
    [
        [node("a", "missing")],
        [node("a", "b"), node("b", "a")],
        [node("a"), node("b", "a", topic="other")],
        [node("a"), node("a")],
    ],
)
def test_invalid_parent_chains_are_rejected(nodes):
    with pytest.raises(ServiceError, match="消息"):
        MessageTree(nodes)


def test_invalid_active_selection_and_missing_user_ancestor_are_rejected():
    tree = MessageTree([node("a"), node("b", "a")])
    for choices in ({"a": "a"}, {"missing": None}, {"__root__": "b"}):
        with pytest.raises(ServiceError):
            tree.active_path(choices)
    with pytest.raises(ServiceError, match="用户祖先"):
        MessageTree([node("assistant", role="assistant")]).user_ancestor("assistant")


class RecordingProvider(FakeProvider):
    def __init__(self):
        super().__init__(delay=0)
        self.contexts = []

    async def stream(self, messages, tools):
        self.contexts.append([message.id for message in messages])
        async for delta in super().stream(messages, tools):
            yield delta


async def test_edit_regenerate_switch_and_model_context_are_isolated(repository):
    provider = RecordingProvider()
    runtime = ChatRuntime(repository, provider, builtin_registry(), Settings())
    try:
        topic = await repository.create_topic("分支隔离")
        first = await runtime.start(
            topic.id, ChatRequest(text="/echo 第一轮", idempotency_key="one")
        )
        await finish(runtime, first)
        common = await repository.list_messages(topic.id)
        second = await runtime.start(topic.id, ChatRequest(text="/add 2 3", idempotency_key="two"))
        await finish(runtime, second)
        original = await repository.list_messages(topic.id)
        assert all(
            current.parent_id == parent.id
            for parent, current in zip(original, original[1:], strict=False)
        )
        before = len((await repository.message_tree(topic.id)).messages)

        request = EditRequest(text="/echo 编辑分支", idempotency_key="edit-once")
        edits = await asyncio.gather(
            *[runtime.edit(topic.id, second.input_message_id, request) for _ in range(3)]
        )
        assert len({run.id for run in edits}) == 1
        edited = edits[0]
        await finish(runtime, edited)
        active = await repository.list_messages(topic.id)
        assert active[: len(common)] == common
        assert active[len(common)].parent_id == common[-1].id
        assert active[len(common)].details["edited_from"] == second.input_message_id
        assert all(item.id not in provider.contexts[-1] for item in original[len(common) :])
        assert len((await repository.message_tree(topic.id)).messages) == before + 4
        assert len(await repository.audits(edited.id)) == 1
        assert (await runtime.edit(topic.id, second.input_message_id, request)).id == edited.id
        with pytest.raises(ServiceError) as error:
            await runtime.edit(
                topic.id,
                second.input_message_id,
                EditRequest(text="changed", idempotency_key="edit-once"),
            )
        assert error.value.code == "idempotency_conflict"

        regeneration = RegenerateRequest(idempotency_key="regen-once")
        regenerated = await runtime.regenerate(topic.id, original[-1].id, regeneration)
        await finish(runtime, regenerated)
        active = await repository.list_messages(topic.id)
        assert regenerated.input_message_id == second.input_message_id
        assert active[len(common)].id == second.input_message_id
        assert all(item.id not in provider.contexts[-1] for item in original[len(common) + 1 :])
        assert "5.0" in active[-1].content
        assert active[len(common) + 1].parent_id == second.input_message_id
        assert (
            await runtime.regenerate(topic.id, original[-1].id, regeneration)
        ).id == regenerated.id
        assert len(await repository.audits(regenerated.id)) == 1
        assert (
            len(
                [
                    item
                    for item in (await repository.message_tree(topic.id)).messages
                    if item.role == "user"
                ]
            )
            == 3
        )

        await repository.select_branch(topic.id, original[-1].id)
        assert [item.id for item in await repository.list_messages(topic.id)] == [
            item.id for item in original
        ]
        await repository.select_branch(topic.id, edited.input_message_id)
        assert "编辑分支" in (await repository.list_messages(topic.id))[-1].content
        # 不活跃分支的旧回复仍可按运行读取，幂等重试不改变当前选择。
        assert (await repository.run_messages(second.id))[-1].id == original[-1].id
        assert (
            await runtime.start(topic.id, ChatRequest(text="/add 2 3", idempotency_key="two"))
        ).id == second.id
        assert "编辑分支" in (await repository.list_messages(topic.id))[-1].content
    finally:
        await runtime.close()


async def test_root_edit_explicit_parent_and_restart_keep_all_branches(runtime, repository):
    topic = await repository.create_topic("根分支")
    first = await runtime.start(topic.id, ChatRequest(text="原根", idempotency_key="root"))
    await finish(runtime, first)
    original = await repository.list_messages(topic.id)
    edited = await runtime.edit(
        topic.id, first.input_message_id, EditRequest(text="新根", idempotency_key="new-root")
    )
    await finish(runtime, edited)
    tree = await repository.message_tree(topic.id)
    assert len([item for item in tree.messages if item.parent_id is None]) == 2
    branch = await runtime.start(
        topic.id,
        ChatRequest(text="原根后续", idempotency_key="child", parent_message_id=original[-1].id),
    )
    await finish(runtime, branch)
    active = await repository.list_messages(topic.id)
    assert active[0].id == first.input_message_id and active[2].id == branch.input_message_id
    assert edited.input_message_id not in {item.id for item in active}
    await repository.select_branch(topic.id, edited.input_message_id)
    expected = [item.id for item in await repository.list_messages(topic.id)]
    reopened = Database(str(repository.database.engine.url))
    try:
        await reopened.initialize()
        restored = KrimRepository(reopened)
        assert [item.id for item in await restored.list_messages(topic.id)] == expected
        assert len((await restored.message_tree(topic.id)).messages) == 6
    finally:
        await reopened.close()


async def test_branch_boundaries_and_cross_topic_operations_are_rejected(runtime, repository):
    topic = await repository.create_topic("边界")
    other = await repository.create_topic("其他")
    run = await runtime.start(topic.id, ChatRequest(text="/add 1 2", idempotency_key="original"))
    await finish(runtime, run)
    messages = await repository.list_messages(topic.id)
    for target in messages[1:3]:
        with pytest.raises(ServiceError) as error:
            await repository.select_branch(topic.id, target.id)
        assert error.value.code == "incomplete_tool_group"
        with pytest.raises(ServiceError):
            await runtime.start(
                topic.id,
                ChatRequest(text="unsafe", idempotency_key=target.id, parent_message_id=target.id),
            )
    with pytest.raises(ServiceError) as error:
        await runtime.edit(
            topic.id, messages[-1].id, EditRequest(text="edit assistant", idempotency_key="invalid")
        )
    assert error.value.code == "message_not_editable"
    with pytest.raises(ServiceError) as error:
        await runtime.regenerate(
            other.id, messages[-1].id, RegenerateRequest(idempotency_key="cross")
        )
    assert error.value.code == "message_not_found"
    assert not (await repository.message_tree(other.id)).messages
    assert len((await repository.message_tree(topic.id)).messages) == 4


async def test_active_run_prevents_branch_changes_without_writing_nodes(repository):
    release = asyncio.Event()
    entered = asyncio.Event()

    class WaitingProvider:
        name = "waiting"

        async def stream(self, _messages, _tools):
            entered.set()
            await release.wait()
            yield ModelText(text="完成")

    runtime = ChatRuntime(repository, WaitingProvider(), builtin_registry(), Settings())
    topic = await repository.create_topic("运行中")
    run = await runtime.start(topic.id, ChatRequest(text="wait", idempotency_key="active"))
    await asyncio.wait_for(entered.wait(), 2)
    try:
        for action in (
            repository.select_branch(topic.id, run.input_message_id),
            runtime.edit(
                topic.id, run.input_message_id, EditRequest(text="edit", idempotency_key="edit")
            ),
            runtime.regenerate(
                topic.id, run.input_message_id, RegenerateRequest(idempotency_key="regenerate")
            ),
        ):
            with pytest.raises(ServiceError) as error:
                await action
            assert error.value.code == "topic_busy"
        assert len((await repository.message_tree(topic.id)).messages) == 1
    finally:
        release.set()
        await finish(runtime, run)
        await runtime.close()


async def test_http_and_host_sdk_expose_real_branch_operations(api, runtime):
    topic = (await api.post("/v1/topics", json={"title": "HTTP 分支"})).json()
    url = f"/v1/topics/{topic['id']}"
    original = (
        await api.post(f"{url}/messages", json={"text": "原消息", "idempotency_key": "original"})
    ).json()
    await finish(runtime, type("Run", (), original))
    source = original["input_message_id"]
    edited_response = await api.post(
        f"{url}/messages/{source}/edit", json={"text": "新消息", "idempotency_key": "edit"}
    )
    assert edited_response.status_code == 202
    edited = edited_response.json()
    assert edited["action"] == "edit" and edited["source_message_id"] == source
    await finish(runtime, type("Run", (), edited))
    tree = (await api.get(f"{url}/tree")).json()
    assert len(tree["messages"]) == 4 and len(tree["active_path"]) == 2
    assert (await api.get(f"/v1/runs/{original['id']}/messages")).json()["messages"][-1][
        "content"
    ] == "[fake] 收到：原消息"
    assert (await api.put(f"{url}/branch", json={"message_id": source})).status_code == 200
    assert (await api.get(f"{url}/messages")).json()["messages"][0]["id"] == source
    regenerated = (
        await api.post(
            f"{url}/messages/{source}/regenerate", json={"idempotency_key": "regenerate"}
        )
    ).json()
    await finish(runtime, type("Run", (), regenerated))
    assert regenerated["input_message_id"] == source
    assert (await api.put(f"{url}/branch", json={"message_id": "missing"})).status_code == 404

    app = LocalApp(runtime)
    try:
        handle = app.handle
        host_edit = await handle.edit(
            topic["id"], source, EditRequest(text="宿主分支", idempotency_key="host-edit")
        )
        assert (await finish(runtime, host_edit)) is None
        assert "宿主分支" in (await handle.messages(topic["id"]))[-1].content
        await handle.select_branch(topic["id"], source)
        host_regen = await handle.regenerate(
            topic["id"], source, RegenerateRequest(idempotency_key="host-regen")
        )
        frames = [frame async for frame in handle.events(host_regen.id)]
        assert frames[-1].data["status"] == "completed"
        assert len((await handle.tree(topic["id"])).messages) == 8
    finally:
        await app.close()


async def test_cancelled_tool_group_can_continue_but_unclosed_requests_cannot(repository):
    entered = asyncio.Event()

    @tool(name="echo", risk="read")
    async def waiting(text: str) -> str:
        entered.set()
        await asyncio.Event().wait()
        return text

    registry = ToolRegistry()
    registry.register(waiting)
    runtime = ChatRuntime(repository, FakeProvider(delay=0), registry, Settings())
    try:
        topic = await repository.create_topic("取消后继续")
        first = await runtime.start(topic.id, ChatRequest(text="/echo 等待", idempotency_key="one"))
        await asyncio.wait_for(entered.wait(), 2)
        await runtime.stop(first.id)
        history = await repository.list_messages(topic.id)
        assert history[-1].role == "tool" and history[-1].details["code"] == "cancelled"
        second = await runtime.start(topic.id, ChatRequest(text="继续对话", idempotency_key="two"))
        await finish(runtime, second)
        assert (await repository.list_messages(topic.id))[-2].parent_id == history[-1].id
        interrupted = await repository.create_topic("崩溃遗留")
        abandoned, _created = await repository.begin_run(
            interrupted.id, ChatRequest(text="恢复", idempotency_key="abandoned")
        )
        await repository.add_message(
            abandoned,
            "assistant",
            "",
            {"tool_calls": [{"id": "unclosed", "name": "echo", "arguments": {"text": "恢复"}}]},
        )
        await repository.recover_interrupted()
        with pytest.raises(ServiceError) as error:
            await runtime.start(
                interrupted.id, ChatRequest(text="下一轮", idempotency_key="unsafe")
            )
        assert error.value.code == "incomplete_tool_group"
        assert len((await repository.message_tree(interrupted.id)).messages) == 2
    finally:
        await runtime.close()
