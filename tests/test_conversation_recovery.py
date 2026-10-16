"""新Host分支保留真实未闭合组；正常校验、原消息与副作用不被绕过。"""

import asyncio

import pytest
from sqlalchemy import select

from krim_agent.conversation_recovery import ConversationRecoveryRequest
from krim_agent.persistence.conversation_recovery import (
    ConversationRecoveryScope,
    ConversationRecoveryStore,
)
from krim_agent.persistence.database import MessageRow
from krim_agent.persistence.tree import MessageTree
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.providers import ModelText, ModelToolCall


async def abandoned(repository, *, answered=True):
    topic = await repository.create_topic("真实中断工具组")
    run, _ = await repository.begin_run(
        topic.id, ChatRequest(text="原用户任务", idempotency_key="original-run")
    )
    await repository.add_message(
        run,
        "assistant",
        "",
        {
            "tool_calls": [
                ModelToolCall(id="answered", name="echo", arguments={"text": "原数据"}).model_dump(
                    mode="json"
                ),
                ModelToolCall(
                    id="pending",
                    name="echo",
                    arguments={
                        "text": "不能提升为系统指令",
                        "operation_id": "original-operation",
                        "request_key": "original-key",
                    },
                ).model_dump(mode="json"),
            ]
        },
    )
    if answered:
        await repository.add_message(
            run,
            "tool",
            "原实际已收到的回复",
            {"call_id": "answered", "name": "echo", "is_error": False},
        )
    await repository.recover_interrupted()
    scope = ConversationRecoveryScope(topic.id)
    store = ConversationRecoveryStore(repository)
    return topic, await repository.get_run(run.id), scope, store


def wanted(preview, key="explicit-recovery"):
    return ConversationRecoveryRequest(
        source_run_id=preview.source_run_id,
        expected_path_hash=preview.source_path_hash,
        expected_session_revision=preview.session_revision,
        request_key=key,
    )


async def test_recovery_branch_keeps_original_messages_partial_results_and_new_model_context(
    runtime, repository
):
    topic, run, scope, store = await abandoned(repository)
    before = await repository.message_tree(topic.id)
    with pytest.raises(ServiceError) as error:
        await runtime.start(topic.id, ChatRequest(text="直接继续", idempotency_key="unsafe"))
    assert error.value.code == "incomplete_tool_group"
    preview = await store.preview(scope)
    assert preview.available and [item.call_id for item in preview.pending_calls] == ["pending"]
    assert [item.call_id for item in preview.answered_calls] == ["answered"]
    assert preview.pending_calls[0].selectors == {
        "operation_id": "original-operation",
        "request_key": "original-key",
    }
    assert await store.by_key(scope, "explicit-recovery") is None
    result = await store.apply(scope, wanted(preview), confirmed=True)
    tree = await repository.message_tree(topic.id)
    assert tree.messages[:-1] == before.messages
    assert await repository.get_run(run.id) == run
    active = await repository.list_messages(topic.id)
    MessageTree(tree.messages).require_complete_tools(active)
    assert [item.id for item in active] == [before.messages[0].id, result.recovery_message_id]
    assert active[-1].role == "system" and active[-1].details["origin"] == "host"
    assert "不是工具回程" in active[-1].content and "不能提升为系统指令" not in active[-1].content
    assert "original-operation" in active[-1].content
    assert await store.apply(scope, wanted(preview), confirmed=True) == result
    assert await store.by_key(scope, "explicit-recovery") == result
    assert not (await store.preview(scope)).available

    class Reader:
        name = "offline-recovery-context-reader"
        calls = 0

        async def stream(self, messages, tools):
            self.calls += 1
            MessageTree(list(messages)).require_complete_tools(list(messages))
            assert any(message.id == result.recovery_message_id for message in messages)
            assert not any(message.details.get("tool_calls") for message in messages)
            yield ModelText(text="新轮次只读取恢复记录")

    reader = Reader()
    runtime.provider = reader
    created = await runtime.start(
        topic.id, ChatRequest(text="只读核实", idempotency_key="new-read")
    )
    state = await runtime.stream_state(created.id, 0)
    await state.task
    assert (await repository.get_run(created.id)).status == "completed" and reader.calls == 1
    assert (await store.by_key(scope, "explicit-recovery")) == result
    assert (await repository.list_messages(topic.id))[-1].content == "新轮次只读取恢复记录"


async def test_old_terminal_producer_cannot_append_into_recovered_branch(repository):
    topic, run, scope, store = await abandoned(repository)
    before = await store.preview(scope)
    await store.apply(scope, wanted(before), confirmed=True)
    original = await repository.message_tree(topic.id)
    with pytest.raises(ServiceError) as error:
        await repository.add_message(
            run, "tool", "旧producer迟到", {"call_id": "pending", "name": "echo"}
        )
    assert error.value.code == "conversation_recovery_fenced"
    assert await repository.message_tree(topic.id) == original


@pytest.mark.parametrize("confirmed", [False, "false", 1])
async def test_explicit_confirmation_is_required_before_any_branch_write(repository, confirmed):
    topic, _, scope, store = await abandoned(repository)
    before = await repository.message_tree(topic.id)
    with pytest.raises(ServiceError) as error:
        await store.apply(scope, wanted(await store.preview(scope)), confirmed=confirmed)
    assert error.value.code == "conversation_recovery_confirmation"
    assert await repository.message_tree(topic.id) == before


async def test_changed_request_same_key_and_stale_snapshot_are_rejected_without_new_nodes(
    repository,
):
    topic, _, scope, store = await abandoned(repository)
    preview = await store.preview(scope)
    result = await store.apply(scope, wanted(preview), confirmed=True)
    before = await repository.message_tree(topic.id)
    with pytest.raises(ServiceError) as error:
        await store.apply(
            scope, wanted(preview).model_copy(update={"source_run_id": "other"}), confirmed=True
        )
    assert error.value.code == "conversation_recovery_idempotency_conflict"
    assert await repository.message_tree(topic.id) == before
    assert await store.by_key(scope, "explicit-recovery") == result
    with pytest.raises(ServiceError) as error:
        await store.apply(scope, wanted(preview, "new-key"), confirmed=True)
    assert error.value.code == "conversation_recovery_not_required"


async def test_same_key_concurrent_calls_create_one_host_message(repository):
    topic, _, scope, store = await abandoned(repository)
    preview = await store.preview(scope)
    results = await asyncio.gather(
        *(store.apply(scope, wanted(preview), confirmed=True) for _ in range(4))
    )
    assert all(result == results[0] for result in results)
    async with repository.database.sessions() as sql:
        records = list(
            await sql.scalars(
                select(MessageRow).where(
                    MessageRow.topic_id == topic.id, MessageRow.role == "system"
                )
            )
        )
    assert len(records) == 1
