"""任务真实持久化、工具/会话隔离、CAS与幂等、跨连接与权限。"""

import asyncio

import pytest
from sqlalchemy import func, select

from krim_agent.application.agents import AgentService
from krim_agent.persistence import Database
from krim_agent.persistence.database import AgentTaskHeadRow, AgentTaskOperationRow
from krim_agent.persistence.tasks import AgentTaskStore
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.sdk import PythonAI, TaskMutation
from krim_agent.tools import ToolContext


async def test_task_crud_replays_original_result_and_retains_operation_history(runtime):
    service = AgentService(runtime, runtime.settings)
    try:
        agent = await service.spawn("default")
        empty = await service.task_snapshot(agent.agent_id, "todo")
        assert empty.revision == 0 and empty.items == []
        async with runtime.repository.database.sessions() as db:
            assert await db.scalar(select(func.count()).select_from(AgentTaskHeadRow)) == 0
        request = TaskMutation(
            operation="add", content="第一项", expected_revision=0, request_key="add"
        )
        first = await service.task_mutate(agent.agent_id, "todo", request)
        assert first.revision == 1 and first.items[0].content == "第一项"
        second = await service.task_mutate(
            agent.agent_id,
            "todo",
            TaskMutation(
                operation="complete",
                item_id=first.items[0].id,
                expected_revision=1,
                request_key="done",
            ),
        )
        assert second.items[0].completed and second.revision == 2
        replay = await service.task_mutate(agent.agent_id, "todo", request)
        assert replay == first
        assert (await service.task_snapshot(agent.agent_id, "todo")).revision == 2
        with pytest.raises(ServiceError) as conflict:
            await service.task_mutate(
                agent.agent_id, "todo", request.model_copy(update={"content": "其他"})
            )
        assert conflict.value.code == "task_idempotency_conflict"
        removed = await service.task_mutate(
            agent.agent_id,
            "todo",
            TaskMutation(
                operation="remove",
                item_id=first.items[0].id,
                expected_revision=2,
                request_key="remove",
            ),
        )
        assert removed.items == [] and removed.revision == 3
        history = await service.task_history(agent.agent_id, "todo")
        assert [item.operation for item in history] == ["add", "complete", "remove"]
    finally:
        await service.close()


async def test_closed_and_destroyed_sessions_keep_task_history_but_reject_writes(runtime):
    service = AgentService(runtime, runtime.settings)
    try:
        agent = await service.spawn("default")
        await service.task_mutate(
            agent.agent_id,
            "plan",
            TaskMutation(
                operation="replace", items=["计划"], expected_revision=0, request_key="plan"
            ),
        )
        closed = await service.close_session(agent.agent_id, 0)
        with pytest.raises(ServiceError) as denied:
            await service.task_mutate(
                agent.agent_id,
                "plan",
                TaskMutation(
                    operation="replace", items=[], expected_revision=1, request_key="blocked"
                ),
            )
        assert denied.value.code == "agent_session_closed"
        await service.reopen_session(agent.agent_id, closed.revision)
        assert (await service.task_snapshot(agent.agent_id, "plan")).items[0].content == "计划"
        await service.destroy(agent.agent_id)
        assert (await service.task_snapshot(agent.agent_id, "plan")).revision == 1
    finally:
        await service.close()


async def test_two_database_connections_cannot_both_accept_same_revision(runtime):
    service = AgentService(runtime, runtime.settings)
    agent = await service.spawn("default")
    second_database = Database(str(runtime.repository.database.engine.url))
    await second_database.initialize()
    remote = AgentTaskStore(second_database, owner_id=service.manager.owner_id)
    try:
        scope = f"session:{agent.agent_id}"
        values = await asyncio.gather(
            service.manager.tasks.mutate(
                scope,
                "todo",
                TaskMutation(
                    operation="add", content="one", expected_revision=0, request_key="one"
                ),
            ),
            remote.mutate(
                scope,
                "todo",
                TaskMutation(
                    operation="add", content="two", expected_revision=0, request_key="two"
                ),
            ),
            return_exceptions=True,
        )
        assert sum(isinstance(value, ServiceError) for value in values) == 1
        assert (
            next(value for value in values if isinstance(value, ServiceError)).code
            == "task_revision_conflict"
        )
        assert (await remote.snapshot(scope, "todo")).revision == 1
    finally:
        await second_database.close()
        await service.close()


async def test_runtime_tools_use_separate_session_scope_and_keep_policy_audit(runtime):
    async def allow(_definition, _context):
        return "allow"

    service = AgentService(runtime, runtime.settings)
    try:
        one, two = await service.spawn("default"), await service.spawn("default")
        handle = service.manager._handles[one.agent_id]
        run, _ = await runtime.repository.begin_run(
            handle.topic_id,
            ChatRequest(text="工具", idempotency_key="tools"),
            agent_id=handle.agent_id,
            agent_session_id=handle.session_id,
            agent_owner_id=service.manager.owner_id,
        )
        context = ToolContext(
            handle.runtime.tools,
            run_id=run.id,
            topic_id=handle.topic_id,
            session_id=handle.session_id,
            task_store=service.manager.tasks,
            policy=allow,
            call_id="plan-call",
        )
        result = await handle.runtime.tools.invoke(
            "plan", {"operation": "replace", "items": ["a", "b"]}, context
        )
        assert not result.is_error and result.data["revision"] == 1
        again = await handle.runtime.tools.invoke(
            "plan", {"operation": "replace", "items": ["a", "b"]}, context
        )
        assert again.data == result.data
        assert (await service.task_snapshot(two.agent_id, "plan")).items == []
        context.call_id = "complete-call"
        done = await handle.runtime.tools.invoke(
            "plan", {"operation": "complete", "item": "a"}, context
        )
        assert done.data["items"] == ["b"]
        replay = await handle.runtime.tools.invoke(
            "plan", {"operation": "complete", "item": "a"}, context
        )
        assert replay.data == done.data
        denied = await handle.runtime.tools.invoke(
            "todo",
            {"operation": "add", "item": "deny"},
            ToolContext(
                handle.runtime.tools,
                run_id=run.id,
                topic_id=handle.topic_id,
                session_id=handle.session_id,
                task_store=service.manager.tasks,
            ),
        )
        assert denied.is_error and denied.code == "permission_denied"
        assert (await service.task_snapshot(one.agent_id, "todo")).items == []
        assert (await service.task_history(one.agent_id, "plan"))[0].call_id == "plan-call"
        await runtime.repository.finish(run.id, "completed", None, [])
    finally:
        await service.close()


async def test_scope_owner_and_managed_topic_cannot_bypass_agent_tasks(runtime):
    service = AgentService(runtime, runtime.settings)
    try:
        agent = await service.spawn("default")
        other = AgentTaskStore(runtime.repository.database, owner_id="forged")
        with pytest.raises(ServiceError) as denied:
            await other.mutate(
                f"session:{agent.agent_id}",
                "todo",
                TaskMutation(operation="add", content="bad", request_key="bad"),
            )
        assert denied.value.code == "agent_session_owner"
        with pytest.raises(ServiceError) as topic:
            await service.manager.tasks.snapshot(f"topic:{agent.topic_id}", "todo")
        assert topic.value.code == "task_scope_managed"
    finally:
        await service.close()


async def test_task_api_cas_history_and_input_boundary(api):
    agent = (await api.post("/v1/agents", json={"name": "default"})).json()
    endpoint = f"/v1/agents/{agent['agent_id']}/tasks/todo"
    response = await api.post(
        endpoint,
        json={
            "operation": "add",
            "content": "API task",
            "expected_revision": 0,
            "request_key": "api",
        },
    )
    assert response.status_code == 200 and response.json()["revision"] == 1
    conflict = await api.post(
        endpoint,
        json={
            "operation": "add",
            "content": "stale",
            "expected_revision": 0,
            "request_key": "stale",
        },
    )
    assert (
        conflict.status_code == 409 and conflict.json()["error"]["code"] == "task_revision_conflict"
    )
    assert (
        await api.post(
            endpoint,
            json={"operation": "add", "content": "missing revision", "request_key": "invalid"},
        )
    ).status_code == 422
    assert len((await api.get(endpoint + "/history")).json()) == 1


async def test_sdk_cold_restore_keeps_task_state_and_model_has_no_new_calls(tmp_path):
    from krim_agent.config import Settings
    from krim_agent.providers import FakeProvider

    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path / 'task.sqlite'}")
    async with await PythonAI.create(settings=settings, provider=FakeProvider(delay=0)) as ai:
        session = await ai.agent(persistent=True)
        value = await session.update_tasks(
            "todo",
            TaskMutation(
                operation="add", content="持续任务", expected_revision=0, request_key="persistent"
            ),
        )
        identifier = session.agent_id
    async with await PythonAI.create(settings=settings, provider=FakeProvider(delay=0)) as cold:
        session = cold.agent_session(identifier)
        assert (await session.tasks()).items == value.items
        await session.restore()
        assert (await session.tasks()).revision == 1


async def test_task_snapshot_capacity_rolls_back_entire_mutation(runtime):
    service = AgentService(runtime, runtime.settings)
    try:
        agent = await service.spawn("default")
        with pytest.raises(ServiceError) as limit:
            await service.task_mutate(
                agent.agent_id,
                "plan",
                TaskMutation(
                    operation="replace",
                    items=["中" * 200] * 100,
                    expected_revision=0,
                    request_key="large",
                ),
            )
        assert limit.value.code == "task_snapshot_limit"
        assert (await service.task_snapshot(agent.agent_id, "plan")).revision == 0
        async with runtime.repository.database.sessions() as db:
            assert await db.scalar(select(func.count()).select_from(AgentTaskOperationRow)) == 0
    finally:
        await service.close()


async def test_real_model_calls_persistent_plan_and_todo_in_the_same_session(runtime):
    from krim_agent.application.agents import AgentPromptRequest
    from krim_agent.providers import ModelText, ModelToolCall

    class Planner:
        name = "offline-task-planner"

        async def stream(self, messages, tools):
            results = [message for message in messages if message.role == "tool"]
            if not results:
                yield ModelToolCall(
                    id="plan",
                    name="plan",
                    arguments={"operation": "replace", "items": ["实现", "验证"]},
                )
            elif len(results) == 1:
                yield ModelToolCall(
                    id="todo", name="todo", arguments={"operation": "add", "item": "执行测试"}
                )
            else:
                yield ModelText(text="任务已记录")

    async def allow(_definition, _context):
        return "allow"

    runtime.provider, runtime.tool_policy = Planner(), allow
    service = AgentService(runtime, runtime.settings)
    try:
        agent = await service.spawn("default")
        result = await service.execute(
            agent.agent_id, AgentPromptRequest(prompt="做任务", idempotency_key="planner")
        )
        assert result.status == "completed" and result.result.text == "任务已记录"
        assert [
            item.content for item in (await service.task_snapshot(agent.agent_id, "plan")).items
        ] == ["实现", "验证"]
        assert (await service.task_snapshot(agent.agent_id, "todo")).items[0].content == "执行测试"
        audits = await runtime.repository.audits(result.current_run_id)
        assert [audit.name for audit in audits] == ["plan", "todo"] and all(
            audit.ok for audit in audits
        )
    finally:
        await service.close()


async def test_runtime_ordinary_topics_have_independent_persistent_task_scope(runtime):
    one, two = (
        await runtime.repository.create_topic("one"),
        await runtime.repository.create_topic("two"),
    )
    run, _ = await runtime.repository.begin_run(
        one.id, ChatRequest(text="任务", idempotency_key="one")
    )
    context = ToolContext(
        runtime.tools, topic_id=one.id, run_id=run.id, task_store=runtime.task_store
    )

    async def allow(_definition, _context):
        return "allow"

    context.policy = allow
    value = await runtime.tools.call_tool(
        "todo", {"operation": "add", "item": "topic only"}, context
    )
    assert value["revision"] == 1
    store = AgentTaskStore(runtime.repository.database)
    assert (await store.snapshot(f"topic:{one.id}", "todo")).items[0].content == "topic only"
    assert not (await store.snapshot(f"topic:{two.id}", "todo")).items
    await runtime.repository.finish(run.id, "completed", None, [])


async def test_real_tcp_client_and_cli_task_mutation_history(server_url, tmp_path):
    import json
    import sys

    from krim_agent.client import KrimClient
    from krim_agent.task_state import TaskWriteRequest

    def create():
        with KrimClient(server_url) as client:
            agent = client.agent_spawn("default")
            result = client.agent_tasks_write(
                agent.agent_id,
                "todo",
                TaskWriteRequest(
                    operation="add", content="client", expected_revision=0, request_key="client"
                ),
            )
            assert client.agent_tasks(agent.agent_id, "todo") == result
            return agent, result

    agent, current = await asyncio.to_thread(create)
    request = tmp_path / "tasks.json"
    request.write_text(
        TaskWriteRequest(
            operation="complete",
            item_id=current.items[0].id,
            expected_revision=1,
            request_key="cli",
        ).model_dump_json(),
        encoding="utf-8",
    )

    async def cli(*args):
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "krim_agent",
            "tasks",
            *args,
            "--url",
            server_url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, errors = await asyncio.wait_for(process.communicate(), 20)
        assert process.returncode == 0, errors.decode()
        return json.loads(output)

    updated = await cli("update", agent.agent_id, "--request", str(request))
    assert updated["items"][0]["completed"] and updated["revision"] == 2
    assert [item["operation"] for item in await cli("history", agent.agent_id)] == [
        "add",
        "complete",
    ]
    assert (await cli("get", agent.agent_id))["revision"] == 2


async def test_task_history_blocks_destructive_downgrade_even_when_items_removed(runtime):
    from alembic import command

    from krim_agent.persistence.migration import migration_config

    service = AgentService(runtime, runtime.settings)
    try:
        agent = await service.spawn("default")
        current = await service.task_mutate(
            agent.agent_id,
            "todo",
            TaskMutation(
                operation="add", content="history", expected_revision=0, request_key="add"
            ),
        )
        await service.task_mutate(
            agent.agent_id,
            "todo",
            TaskMutation(
                operation="remove",
                item_id=current.items[0].id,
                expected_revision=1,
                request_key="remove",
            ),
        )
        with pytest.raises(RuntimeError, match="历史非空"):
            async with runtime.repository.database.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(migration_config(sync), "0022_agent_sessions")
                )
    finally:
        await service.close()
