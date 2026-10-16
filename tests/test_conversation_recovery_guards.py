"""恢复源/当前资格/存储损坏与正常旧分支校验均不得被新记录覆盖。"""

import copy
import json
from dataclasses import replace

import pytest
from sqlalchemy import event, select, update
from sqlalchemy.orm import Session
from test_conversation_recovery import abandoned, wanted

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentService
from krim_agent.conversation_recovery import ConversationRecoveryRecord, recovery_notice
from krim_agent.persistence.conversation_recovery import (
    ConversationRecoveryScope,
    ConversationRecoveryStore,
)
from krim_agent.persistence.database import AgentSessionRow, MessageRow, TopicRow
from krim_agent.persistence.tree import MessageTree
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.providers import ModelToolCall


@pytest.mark.parametrize(
    "fault", ["duplicate", "orphan", "midgroup", "crossrun", "shape", "completed"]
)
async def test_recovery_rejects_corrupt_or_nonterminal_source_without_new_branch(repository, fault):
    topic, run, scope, store = await abandoned(repository)
    other_run = None
    if fault == "crossrun":
        second_topic = await repository.create_topic("其他原运行")
        other_run, _ = await repository.begin_run(
            second_topic.id, ChatRequest(text="另一来源", idempotency_key="other")
        )
    async with repository.database.sessions() as sql, sql.begin():
        rows = list(
            await sql.scalars(
                select(MessageRow)
                .where(MessageRow.topic_id == topic.id)
                .order_by(MessageRow.sequence)
            )
        )
        assistant, answer = rows[1:]
        if fault == "duplicate":
            details = copy.deepcopy(assistant.details)
            details["tool_calls"].append(details["tool_calls"][0])
            assistant.details = details
        elif fault == "orphan":
            answer.details = {**answer.details, "call_id": "other"}
        elif fault == "midgroup":
            answer.role = "user"
        elif fault == "crossrun":
            # 真实不同Run，避免以非外键身份制造无意义失败。
            answer.run_id = other_run.id
        elif fault == "shape":
            assistant.details = {"tool_calls": {"not": "array"}}
        else:
            from krim_agent.persistence.database import RunRow

            original = await sql.get(RunRow, run.id)
            original.status = "completed"
    with pytest.raises(ServiceError) as error:
        await store.preview(scope)
    assert error.value.code in {
        "conversation_recovery_corrupt",
        "conversation_recovery_run",
        "topic_busy",
    }
    async with repository.database.sessions() as sql:
        assert not list(
            await sql.scalars(
                select(MessageRow).where(
                    MessageRow.topic_id == topic.id, MessageRow.role == "system"
                )
            )
        )


@pytest.mark.parametrize(
    "fault", ["message", "selector-and-text", "answered-and-text", "branch-key", "identity"]
)
async def test_original_key_read_revalidates_real_source_not_just_saved_json(repository, fault):
    topic, _, scope, store = await abandoned(repository)
    result = await store.apply(scope, wanted(await store.preview(scope)), confirmed=True)
    async with repository.database.sessions() as sql, sql.begin():
        row = await sql.get(MessageRow, result.recovery_message_id)
        if fault == "message":
            source = await sql.get(MessageRow, result.source.excluded_message_ids[0])
            source.content = "原消息被改变"
        else:
            data = copy.deepcopy(row.details)
            record = data["record"]
            if fault == "selector-and-text":
                record["source"]["pending_calls"][0]["selectors"]["operation_id"] = (
                    "forged-operation"
                )
            elif fault == "answered-and-text":
                record["source"]["answered_calls"][0]["message_hash"] = "f" * 64
            elif fault == "branch-key":
                record["recovered_branch_key"] = "f" * 64
            else:
                record["source"]["topic_id"] = "other"
            row.details = data
            if fault in {"selector-and-text", "answered-and-text"}:
                # 同时篡改正文，单纯重算JSON正文也不能把假引用当原来源。
                altered = ConversationRecoveryRecord.model_validate_json(json.dumps(record))
                row.content = recovery_notice(altered.source)
    with pytest.raises(ServiceError) as error:
        await store.by_key(scope, "explicit-recovery")
    assert error.value.code == "conversation_recovery_corrupt"


async def actor_source(runtime, *, persistent=False):
    service = AgentService(runtime, runtime.settings)
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name="recovery-owner",
                description="恢复原会话资格",
                tool_names=["echo"],
                persistent=persistent,
            ),
        )
    )
    actor = await service.spawn("recovery-owner")
    run, _ = await runtime.repository.begin_run(
        actor.topic_id,
        ChatRequest(text="原任务", idempotency_key="original"),
        agent_session_id=actor.session_id,
        agent_id=actor.agent_id,
        agent_owner_id=service.manager.owner_id,
    )
    await runtime.repository.add_message(
        run,
        "assistant",
        "",
        {
            "tool_calls": [
                ModelToolCall(id="pending", name="echo", arguments={"text": "原请求"}).model_dump(
                    mode="json"
                )
            ]
        },
    )
    await runtime.repository.recover_interrupted()
    scope = ConversationRecoveryScope(
        actor.topic_id, actor.session_id, actor.agent_id, service.manager.owner_id
    )
    return service, actor, scope, ConversationRecoveryStore(runtime.repository)


@pytest.mark.parametrize("fault", ["owner", "scope", "closed", "revision", "parent"])
async def test_original_agent_owner_scope_closed_revision_and_parent_are_required(runtime, fault):
    service, actor, scope, store = await actor_source(runtime)
    try:
        preview = await store.preview(scope)
        before = await runtime.repository.message_tree(actor.topic_id)
        selected = scope
        request = wanted(preview)
        if fault == "owner":
            selected = replace(scope, owner_id="forged")
        elif fault == "scope":
            selected = replace(scope, agent_id="other")
        elif fault == "revision":
            request = request.model_copy(
                update={"expected_session_revision": preview.session_revision + 1}
            )
        elif fault == "closed":
            await service.sessions.transition(
                actor.session_id, service.manager.owner_id, preview.session_revision, "closed"
            )
        else:
            parent = await service.spawn("recovery-owner")
            async with runtime.repository.database.sessions() as sql, sql.begin():
                child = await sql.get(AgentSessionRow, actor.session_id)
                child.parent_session_id = parent.session_id
            view = await service.sessions.get(parent.session_id)
            await service.sessions.transition(
                parent.session_id, service.manager.owner_id, view.revision, "closed"
            )
        with pytest.raises(ServiceError):
            await store.apply(selected, request, confirmed=True)
        assert (await runtime.repository.message_tree(actor.topic_id)).messages == before.messages
    finally:
        await service.close()


async def test_last_qualification_is_refreshed_after_projection_before_commit(runtime):
    service, actor, scope, store = await actor_source(runtime)
    observed = []

    def revoke(sql, _context):
        if any(
            isinstance(row, MessageRow)
            and row.role == "system"
            and row.details.get("kind") == "conversation_recovery"
            for row in sql.new
        ):
            observed.append(True)
            sql.connection().execute(
                update(AgentSessionRow)
                .where(AgentSessionRow.session_id == actor.session_id)
                .values(status="closed")
            )

    try:
        preview = await store.preview(scope)
        before = await runtime.repository.message_tree(actor.topic_id)
        event.listen(Session, "after_flush", revoke)
        with pytest.raises(ServiceError) as error:
            await store.apply(scope, wanted(preview), confirmed=True)
        assert observed and error.value.code == "agent_session_closed"
        assert await runtime.repository.message_tree(actor.topic_id) == before
        assert await store.by_key(scope, "explicit-recovery") is None
    finally:
        event.remove(Session, "after_flush", revoke)
        await service.close()


async def test_original_incomplete_path_still_rejected_and_idempotent_read_does_not_select_it(
    repository,
):
    topic, _, scope, store = await abandoned(repository)
    before = await repository.message_tree(topic.id)
    record = await store.apply(scope, wanted(await store.preview(scope)), confirmed=True)
    with pytest.raises(ServiceError) as error:
        MessageTree(before.messages).require_complete_tools(before.messages)
    assert error.value.code == "incomplete_tool_group"
    async with repository.database.sessions() as sql, sql.begin():
        row = await sql.get(TopicRow, topic.id)
        row.active_branches = before.topic.active_branches
    assert await store.by_key(scope, "explicit-recovery") == record
    assert await store.apply(scope, record.request, confirmed=True) == record
    active = await repository.list_messages(topic.id)
    assert active == before.messages
    with pytest.raises(ServiceError):
        MessageTree(active).require_complete_tools(active)
