"""恢复准入边界：原持久租约、取消、真实过期快照及材料上限。"""

import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import insert, select, update
from test_conversation_recovery import abandoned, wanted
from test_conversation_recovery_guards import actor_source

from krim_agent.conversation_recovery import MAX_MESSAGES, incomplete_tail, path_fingerprint
from krim_agent.persistence.database import AgentHandleRow, MessageRow
from krim_agent.protocol.domain import MessageView, ServiceError
from krim_agent.protocol.models import utc_now


async def test_persistent_lease_expiry_blocks_recovery_without_reviving_old_owner(runtime):
    service, actor, scope, store = await actor_source(runtime, persistent=True)
    try:
        before = await runtime.repository.message_tree(actor.topic_id)
        preview = await store.preview(scope)
        async with runtime.repository.database.sessions() as sql, sql.begin():
            await sql.execute(
                update(AgentHandleRow)
                .where(AgentHandleRow.agent_id == actor.agent_id)
                .values(lease_expires_at=0)
            )
        with pytest.raises(ServiceError) as error:
            await store.apply(scope, wanted(preview), confirmed=True)
        assert error.value.code == "agent_lease_lost"
        assert await runtime.repository.message_tree(actor.topic_id) == before
    finally:
        # 仅告知已失权的本实例停止续租/落盘；不恢复旧owner或原租约。
        service._handle(actor.agent_id)._lease_lost = True
        await service.close()


async def test_cancel_before_sql_admission_does_not_switch_branch(repository):
    topic, _, scope, store = await abandoned(repository)
    preview = await store.preview(scope)
    before = await repository.message_tree(topic.id)
    async with repository.database.write_lock:
        task = asyncio.create_task(store.apply(scope, wanted(preview), confirmed=True))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert await store.by_key(scope, "explicit-recovery") is None
    assert await repository.message_tree(topic.id) == before


async def test_cancel_after_flush_rolls_back_note_and_branch_together(repository, monkeypatch):
    topic, _, scope, store = await abandoned(repository)
    preview = await store.preview(scope)
    before = await repository.message_tree(topic.id)
    entered, release = asyncio.Event(), asyncio.Event()
    guard = store._guard
    calls = 0

    async def hold(sql, current, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 3:
            entered.set()
            await release.wait()
        return await guard(sql, current, **kwargs)

    monkeypatch.setattr(store, "_guard", hold)
    task = asyncio.create_task(store.apply(scope, wanted(preview), confirmed=True))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
    monkeypatch.setattr(store, "_guard", guard)
    assert await store.by_key(scope, "explicit-recovery") is None
    assert await repository.message_tree(topic.id) == before


async def test_path_changed_after_preview_is_rejected_and_no_recovery_note_written(repository):
    topic, run, scope, store = await abandoned(repository, answered=False)
    preview = await store.preview(scope)
    await repository.add_message(
        run, "tool", "真实新到达的一项回复", {"call_id": "answered", "name": "echo"}
    )
    before = await repository.message_tree(topic.id)
    with pytest.raises(ServiceError) as error:
        await store.apply(scope, wanted(preview), confirmed=True)
    assert error.value.code == "conversation_recovery_conflict"
    assert await repository.message_tree(topic.id) == before
    assert await store.by_key(scope, "explicit-recovery") is None


async def test_active_run_blocks_preview_and_recovery(repository):
    topic, run, scope, store = await abandoned(repository)
    preview = await store.preview(scope)
    await repository.set_running(run.id)
    with pytest.raises(ServiceError) as error:
        await store.apply(scope, wanted(preview), confirmed=True)
    assert error.value.code == "topic_busy"
    assert await store.by_key(scope, "explicit-recovery") is None


async def test_recovery_message_tree_limit_rejects_without_truncating_history(repository):
    topic, run, scope, store = await abandoned(repository)
    async with repository.database.sessions() as sql, sql.begin():
        highest = max(
            (
                row.sequence
                for row in await sql.scalars(
                    select(MessageRow).where(MessageRow.topic_id == topic.id)
                )
            ),
            default=0,
        )
        await sql.execute(
            insert(MessageRow),
            [
                {
                    "id": str(uuid4()),
                    "topic_id": topic.id,
                    "run_id": run.id,
                    "sequence": highest + index + 1,
                    "role": "assistant",
                    "content": "原历史",
                    "details": {},
                    "created_at": utc_now(),
                    "parent_id": None,
                }
                for index in range(MAX_MESSAGES)
            ],
        )
    with pytest.raises(ServiceError) as error:
        await store.preview(scope)
    assert error.value.code == "conversation_recovery_limit" and error.value.status == 413
    async with repository.database.sessions() as sql:
        assert (
            len(
                list(
                    await sql.scalars(select(MessageRow.id).where(MessageRow.topic_id == topic.id))
                )
            )
            == MAX_MESSAGES + 3
        )
    assert await store.by_key(scope, "explicit-recovery") is None


def test_recovery_path_material_limit_and_tool_group_limit_are_explicit():
    message = MessageView(
        id="message",
        topic_id="topic",
        run_id="run",
        sequence=1,
        role="user",
        content="x" * (16 * 1024 * 1024 + 1),
        details={},
        created_at=utc_now(),
    )
    with pytest.raises(ServiceError) as error:
        path_fingerprint([message])
    assert error.value.status == 413
    calls = [{"id": f"call-{index}", "name": "echo", "arguments": {}} for index in range(65)]
    with pytest.raises(ServiceError) as error:
        incomplete_tail(
            [
                message.model_copy(
                    update={"role": "assistant", "content": "", "details": {"tool_calls": calls}}
                )
            ]
        )
    assert error.value.code == "conversation_recovery_corrupt"
