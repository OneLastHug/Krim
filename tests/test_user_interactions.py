import asyncio
import json
import sys

import httpx
import pytest
from alembic import command
from sqlalchemy import text

from krim_agent.agents import AgentCatalog, AgentDefinition, AgentManager
from krim_agent.api import create_app
from krim_agent.app import LocalApp
from krim_agent.application.interactions import InteractionService
from krim_agent.client import KrimClient
from krim_agent.config import Settings
from krim_agent.interactions import InteractionReply, InteractionView
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.migration import migration_config
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.providers import FakeProvider, ModelText, ModelToolCall
from krim_agent.runtime import ChatRuntime
from krim_agent.sdk import InteractionPart, PythonAI
from krim_agent.tools import ToolContext, ToolRegistry, tool
from krim_agent.tools.builtins import builtin_registry


class WriterProvider:
    name = "interactive-offline-writer"

    async def stream(self, messages, _tools):
        result = next((message for message in reversed(messages) if message.role == "tool"), None)
        if result is None:
            yield ModelToolCall(
                id="write-call", name="write_value", arguments={"value": "original"}
            )
        else:
            yield ModelText(text="模型收到：" + result.content)


def writer_tools(effects):
    @tool(risk="write")
    async def write_value(value: str):
        effects.append(value)
        return value

    registry = ToolRegistry()
    registry.register(write_value)
    return registry


async def start(runtime, prompt="write"):
    topic = await runtime.repository.create_topic("用户交互")
    return await runtime.start(topic.id, ChatRequest(text=prompt, idempotency_key="start"))


async def request_part(runtime, run, *, from_seq=0):
    state = await runtime.stream_state(run.id, from_seq)
    async with asyncio.timeout(5):
        async for frame in runtime.subscribe(state, from_seq):
            if (
                frame is not None
                and isinstance(frame.data, dict)
                and frame.data.get("type") == "interaction_request"
            ):
                return InteractionView.model_validate(frame.data["interaction"]), frame.seq + 1
    raise AssertionError("运行没有创建交互请求")


async def wait(runtime, run):
    task = runtime.production_task(run.id)
    assert task is not None
    # 子 Agent 回程包含父流、审批和收尾；观察窗口不替代产品超时。
    await asyncio.wait_for(asyncio.shield(task), 10)
    return await runtime.repository.get_run(run.id)


async def test_approval_wait_has_no_effect_and_accepts_only_bound_response_once(repository):
    effects = []
    runtime = ChatRuntime(
        repository,
        WriterProvider(),
        writer_tools(effects),
        Settings(interactive_tools=True, run_timeout_seconds=10),
    )
    try:
        run = await start(runtime)
        request, _ = await request_part(runtime, run)
        assert request.kind == "approval" and request.status == "pending"
        assert request.run_id == run.id and request.call_id == "write-call"
        assert request.payload["arguments"] == {"value": "original"} and not effects
        with pytest.raises(ServiceError) as mismatch:
            await runtime.interactions.reply(
                "wrong-run", request.id, InteractionReply(action="approve")
            )
        assert mismatch.value.code in {"run_not_found", "interaction_not_found"}
        replies = await asyncio.gather(
            runtime.interactions.reply(run.id, request.id, InteractionReply(action="approve")),
            runtime.interactions.reply(run.id, request.id, InteractionReply(action="approve")),
            return_exceptions=True,
        )
        assert sum(isinstance(reply, InteractionView) for reply in replies) == 1
        assert sum(isinstance(reply, ServiceError) for reply in replies) == 1
        assert (await wait(runtime, run)).status == "completed" and effects == ["original"]
        records = await repository.list_interactions(run.id)
        assert records[0].status == "approved"
        audit = (await repository.audits(run.id))[0]
        assert audit.ok and audit.decision == "allow"
        # 事件读取只是回放，不再次执行 write_value。
        assert await repository.list_events(run.id)
        assert effects == ["original"]
    finally:
        await runtime.close()


async def test_denied_approval_returns_tool_failure_without_side_effect(repository):
    effects = []
    runtime = ChatRuntime(
        repository, WriterProvider(), writer_tools(effects), Settings(interactive_tools=True)
    )
    try:
        run = await start(runtime)
        request, _ = await request_part(runtime, run)
        await runtime.interactions.reply(run.id, request.id, InteractionReply(action="deny"))
        assert (await wait(runtime, run)).status == "completed" and not effects
        messages = await repository.run_messages(run.id)
        result = next(message for message in messages if message.role == "tool")
        assert result.details["is_error"] and result.details["code"] == "permission_denied"
        assert (await repository.list_interactions(run.id))[0].status == "denied"
    finally:
        await runtime.close()


async def test_default_write_deny_and_explicit_global_deny_never_request_approval(repository):
    async def deny(_definition, _context):
        return "deny"

    effects = []
    for settings, policy in [(Settings(), None), (Settings(interactive_tools=True), deny)]:
        runtime = ChatRuntime(
            repository, WriterProvider(), writer_tools(effects), settings, tool_policy=policy
        )
        try:
            run = await start(runtime)
            assert (await wait(runtime, run)).status == "completed"
            assert not await repository.list_interactions(run.id) and not effects
        finally:
            await runtime.close()


async def test_question_requires_approval_then_real_answer_reaches_model(repository):
    runtime = ChatRuntime(
        repository, FakeProvider(delay=0), builtin_registry(), Settings(interactive_tools=True)
    )
    ai = PythonAI(runtime)
    try:
        stream = await ai.stream_text("/question 选择发布环境")
        approval, cursor = await request_part(runtime, stream.run)
        await runtime.interactions.reply(
            stream.run.id, approval.id, InteractionReply(action="approve")
        )
        question, _ = await request_part(runtime, stream.run, from_seq=cursor)
        assert question.kind == "question" and question.payload["question"] == "选择发布环境"
        with pytest.raises(ServiceError) as invalid:
            await runtime.interactions.reply(
                stream.run.id, question.id, InteractionReply(action="approve")
            )
        assert invalid.value.code == "interaction_reply_invalid"
        await runtime.interactions.reply(
            stream.run.id, question.id, InteractionReply(action="answer", answer="只在本机演练")
        )
        result = await stream.collect()
        assert result.run.status == "completed" and "只在本机演练" in result.text
        records = await repository.list_interactions(stream.run.id)
        assert [record.status for record in records] == ["approved", "answered"]
        assert len(await repository.audits(stream.run.id)) == 1
        parts = [part async for part in stream.full_stream()]
        assert len([part for part in parts if isinstance(part, InteractionPart)]) == 2
    finally:
        await ai.close()
        await runtime.close()


async def test_hook_parameter_change_needs_second_approval(repository):
    effects = []
    registry = writer_tools(effects)

    async def mutate(_definition, context):
        context.arguments["value"] = "modified"

    registry.add_global_pre(mutate)
    runtime = ChatRuntime(repository, WriterProvider(), registry, Settings(interactive_tools=True))
    try:
        run = await start(runtime)
        first, cursor = await request_part(runtime, run)
        await runtime.interactions.reply(run.id, first.id, InteractionReply(action="approve"))
        second, _ = await request_part(runtime, run, from_seq=cursor)
        assert second.call_id == first.call_id and second.id != first.id
        assert second.arguments_hash != first.arguments_hash
        assert second.payload["arguments"] == {"value": "modified"} and not effects
        await runtime.interactions.reply(run.id, second.id, InteractionReply(action="approve"))
        assert (await wait(runtime, run)).status == "completed" and effects == ["modified"]
    finally:
        await runtime.close()


async def test_human_wait_does_not_consume_short_tool_execution_deadline(repository):
    effects = []
    runtime = ChatRuntime(
        repository,
        WriterProvider(),
        writer_tools(effects),
        # 本测试只验证 100 ms 工具预算的暂停；总运行超时由下一项测试独立验证。
        Settings(interactive_tools=True, tool_timeout_seconds=0.1, run_timeout_seconds=30),
    )
    try:
        run = await start(runtime)
        request, _ = await request_part(runtime, run)
        await asyncio.sleep(0.25)
        assert not effects and (await repository.get_run(run.id)).status == "running"
        await runtime.interactions.reply(run.id, request.id, InteractionReply(action="approve"))
        result = await wait(runtime, run)
        assert result.status == "completed" and effects == ["original"], (
            result.error_code,
            result.error,
            effects,
        )
    finally:
        await runtime.close()


async def test_pending_request_cancelled_on_stop_and_total_run_timeout(repository):
    effects = []
    runtime = ChatRuntime(
        repository,
        WriterProvider(),
        writer_tools(effects),
        Settings(interactive_tools=True, run_timeout_seconds=10),
    )
    run = await start(runtime)
    request, _ = await request_part(runtime, run)
    assert (await runtime.stop(run.id)).status == "aborted"
    assert (await repository.get_interaction(run.id, request.id)).status == "cancelled"
    with pytest.raises(ServiceError):
        await runtime.interactions.reply(run.id, request.id, InteractionReply(action="approve"))
    await runtime.close()
    runtime = ChatRuntime(
        repository,
        WriterProvider(),
        writer_tools(effects),
        Settings(interactive_tools=True, run_timeout_seconds=2),
    )
    try:
        run = await start(runtime)
        request, _ = await request_part(runtime, run)
        result = await wait(runtime, run)
        assert result.status == "failed" and result.error_code == "run_timeout"
        assert (await repository.get_interaction(run.id, request.id)).status == "cancelled"
        assert not effects
    finally:
        await runtime.close()


async def test_independent_interaction_timeout_is_persisted_and_no_effect_occurs(repository):
    effects = []
    runtime = ChatRuntime(
        repository,
        WriterProvider(),
        writer_tools(effects),
        Settings(interactive_tools=True, interaction_timeout_seconds=0.2, run_timeout_seconds=10),
    )
    try:
        run = await start(runtime)
        request, _ = await request_part(runtime, run)
        assert (await wait(runtime, run)).status == "completed"
        assert (await repository.get_interaction(run.id, request.id)).status == "expired"
        assert not effects
        assert (await repository.audits(run.id))[0].code == "interaction_expired"
    finally:
        await runtime.close()


async def test_restart_cancels_pending_and_preserves_reply_history_and_downgrade_protection(
    tmp_path,
):
    url = f"sqlite+aiosqlite:///{tmp_path / 'interactions.sqlite'}"
    database = Database(url)
    await database.initialize()
    repository = KrimRepository(database)
    topic = await repository.create_topic("重启")
    run, _ = await repository.begin_run(
        topic.id, ChatRequest(text="paused", idempotency_key="paused")
    )
    request = await repository.create_interaction(
        topic.id,
        run.id,
        "call",
        "write",
        "approval",
        "a" * 64,
        {"arguments": {}},
        timeout_seconds=100,
    )
    await database.close()
    reopened = Database(url)
    await reopened.initialize()
    repository = KrimRepository(reopened)
    try:
        await repository.recover_interrupted()
        assert (await repository.get_interaction(run.id, request.id)).status == "cancelled"
        assert (await repository.get_run(run.id)).status == "interrupted"
        with pytest.raises(ServiceError):
            await InteractionService(repository).reply(
                run.id, request.id, InteractionReply(action="approve")
            )
        with pytest.raises(RuntimeError, match="用户交互历史"):
            async with reopened.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(migration_config(sync), "0008_context_summaries")
                )
        async with reopened.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar() == "0048_native_checkpoint_bytes"
        assert (await repository.get_interaction(run.id, request.id)).status == "cancelled"
    finally:
        await reopened.close()


async def test_api_respond_endpoint_rejects_cross_run_and_conflicting_actions(repository):
    effects = []
    runtime = ChatRuntime(
        repository, WriterProvider(), writer_tools(effects), Settings(interactive_tools=True)
    )
    app = create_app(runtime=runtime)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as client,
    ):
        topic = (await client.post("/v1/topics", json={"title": "API"})).json()
        run = (
            await client.post(
                f"/v1/topics/{topic['id']}/messages",
                json={"text": "write", "idempotency_key": "once"},
            )
        ).json()
        saved = await repository.get_run(run["id"])
        request, _ = await request_part(runtime, saved)
        pending = await client.get(
            f"/v1/runs/{saved.id}/interactions", params={"pending_only": True}
        )
        assert pending.status_code == 200 and pending.json()[0]["id"] == request.id
        invalid = await client.post(
            f"/v1/runs/{saved.id}/interactions/{request.id}/respond",
            json={"action": "answer", "answer": "forged"},
        )
        assert invalid.status_code == 422
        missing = await client.post(
            f"/v1/runs/other/interactions/{request.id}/respond", json={"action": "approve"}
        )
        assert missing.status_code == 404
        response = await client.post(
            f"/v1/runs/{saved.id}/interactions/{request.id}/respond", json={"action": "approve"}
        )
        assert response.status_code == 200 and response.json()["status"] == "approved"
        assert (await wait(runtime, saved)).status == "completed" and effects == ["original"]


async def test_question_answer_wait_preserves_tool_budget_and_rejects_model_forgery(repository):
    async def allow(_definition, _context):
        return "allow"

    tools = builtin_registry()
    runtime = ChatRuntime(
        repository,
        FakeProvider(delay=0),
        tools,
        Settings(tool_timeout_seconds=0.1, run_timeout_seconds=5),
        tool_policy=allow,
    )
    try:
        run = await start(runtime, "/question 选择环境")
        request, _ = await request_part(runtime, run)
        assert request.kind == "question"
        forged = await tools.invoke(
            "question",
            {"question": "选择环境", "answer": "模型伪造的答案"},
            ToolContext(tools, run_id=run.id, topic_id=run.topic_id, policy=allow),
        )
        assert forged.is_error and forged.code == "invalid_input"
        assert len(await repository.list_interactions(run.id)) == 1
        await asyncio.sleep(0.25)
        assert (await repository.get_run(run.id)).status == "running"
        await runtime.interactions.reply(
            run.id, request.id, InteractionReply(action="answer", answer="本机")
        )
        assert (await wait(runtime, run)).status == "completed"
        assert "本机" in (await repository.run_messages(run.id))[-1].content
    finally:
        await runtime.close()


async def test_host_handle_lists_and_responds_to_real_pending_interactions(repository):
    runtime = ChatRuntime(
        repository, FakeProvider(delay=0), builtin_registry(), Settings(interactive_tools=True)
    )
    app = LocalApp(runtime)
    try:
        topic = await app.handle.create_topic("宿主问答")
        run = await app.handle.send(
            topic.id, ChatRequest(text="/question 如何执行", idempotency_key="host-interaction")
        )
        approval, cursor = await request_part(runtime, run)
        assert (await app.handle.interactions(run.id, pending_only=True))[0].id == approval.id
        await app.handle.respond_interaction(
            run.id, approval.id, InteractionReply(action="approve")
        )
        question, _ = await request_part(runtime, run, from_seq=cursor)
        await app.handle.respond_interaction(
            run.id, question.id, InteractionReply(action="answer", answer="仅本地")
        )
        assert (await wait(runtime, run)).status == "completed"
        assert not await app.handle.interactions(run.id, pending_only=True)
        assert [item.status for item in await app.handle.interactions(run.id)] == [
            "approved",
            "answered",
        ]
    finally:
        await app.close()
        await runtime.close()


async def test_python_client_and_cli_use_real_tcp_approval_and_question(server_url, runtime):
    runtime.tool_policy = ChatRuntime._confirm_writes

    async def cli(*args, expected=0):
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "krim_agent",
            "interaction",
            *args,
            "--url",
            server_url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
        assert process.returncode == expected, stderr.decode()
        return stdout.decode(), stderr.decode()

    def send():
        with KrimClient(server_url) as client:
            topic = client.create_topic("TCP 问答")
            return client.send(topic.id, "/question 选择环境", "tcp-interaction")

    run = await asyncio.to_thread(send)
    approval, cursor = await request_part(runtime, run)
    listed, _ = await cli("list", run.id, "--pending")
    assert json.loads(listed)[0]["id"] == approval.id
    approved, _ = await cli("approve", run.id, approval.id)
    assert json.loads(approved)["status"] == "approved"
    question, _ = await request_part(runtime, run, from_seq=cursor)

    def read_question():
        with KrimClient(server_url) as client:
            return client.interactions(run.id, pending_only=True)

    assert (await asyncio.to_thread(read_question))[0].id == question.id
    answered, _ = await cli("answer", run.id, question.id, "真实 TCP 答案")
    assert json.loads(answered)["status"] == "answered"
    assert (await wait(runtime, run)).status == "completed"
    assert "真实 TCP 答案" in (await runtime.repository.run_messages(run.id))[-1].content
    _, duplicate = await cli("approve", run.id, approval.id, expected=1)
    assert "操作失败" in duplicate and "Traceback" not in duplicate
    listed, _ = await cli("list", run.id)
    assert [item["status"] for item in json.loads(listed)] == ["approved", "answered"]


class DelegatingProvider:
    name = "interactive-offline-delegation"

    async def stream(self, messages, _tools):
        result = next((message for message in reversed(messages) if message.role == "tool"), None)
        if result is None:
            yield ModelToolCall(
                id="delegate-call", name="agent_writer", arguments={"prompt": "write"}
            )
        else:
            yield ModelText(text="父模型收到：" + result.content)


async def test_child_approval_reaches_parent_stream_and_keeps_production_slot(repository):
    async def allow(_definition, _context):
        return "allow"

    effects = []
    manager = AgentManager(repository, max_concurrent=1)
    catalog = AgentCatalog(manager)
    catalog.register(
        "writer",
        AgentDefinition(
            name="writer",
            provider=WriterProvider(),
            tools=writer_tools(effects),
            timeout_seconds=10,
            tool_timeout_seconds=0.2,
            policy=allow,
        ),
        description="需要真实审批的子任务",
    )
    tools = ToolRegistry()
    catalog.install_tools(tools)
    runtime = ChatRuntime(
        repository,
        DelegatingProvider(),
        tools,
        Settings(interactive_tools=True, tool_timeout_seconds=5, run_timeout_seconds=20),
    )
    queued_task = None
    try:
        run = await start(runtime)
        parent_approval, cursor = await request_part(runtime, run)
        await runtime.interactions.reply(
            run.id, parent_approval.id, InteractionReply(action="approve")
        )
        child_approval, _ = await request_part(runtime, run, from_seq=cursor)
        assert child_approval.run_id != run.id and child_approval.tool_name == "write_value"
        # 父工具批准只授权创建委派，不能复用为子工具的许可。
        assert not effects
        queued = await manager.spawn(
            AgentDefinition(
                name="queued",
                provider=FakeProvider(delay=0),
                tools=ToolRegistry(),
                timeout_seconds=10,
            )
        )
        queued_task = asyncio.create_task(queued.execute("/echo queued"))
        # 等待超过父工具预算；留足 SQLite 启动/关闭的真实执行余量。
        await asyncio.sleep(5.25)
        assert queued.pending_start and queued.current_run is None
        assert (await repository.get_run(run.id)).status == "running"
        assert (await repository.get_run(child_approval.run_id)).status == "running"
        await runtime.interactions.reply(
            child_approval.run_id, child_approval.id, InteractionReply(action="approve")
        )
        assert (await wait(runtime, run)).status == "completed" and effects == ["original"]
        parent_result = next(
            message for message in await repository.run_messages(run.id) if message.role == "tool"
        )
        assert not parent_result.details["is_error"], parent_result.details
        assert parent_result.details["data"]["status"] == "completed"
        assert (await asyncio.wait_for(queued_task, 5)).status == "completed"
        assert (
            await repository.get_interaction(child_approval.run_id, child_approval.id)
        ).status == "approved"
    finally:
        await runtime.close()
        await manager.close()
        if queued_task is not None:
            await asyncio.gather(queued_task, return_exceptions=True)


async def test_child_write_deny_cannot_be_overridden_by_delegation_approval(repository):
    async def deny_writes(definition, _context):
        return "allow" if definition.name == "agent_writer" else "deny"

    async def allow(_definition, _context):
        return "allow"

    effects = []
    manager = AgentManager(repository)
    catalog = AgentCatalog(manager)
    catalog.register(
        "writer",
        AgentDefinition(
            name="writer", provider=WriterProvider(), tools=writer_tools(effects), policy=allow
        ),
        description="父策略禁止写入",
    )
    tools = ToolRegistry()
    catalog.install_tools(tools)
    runtime = ChatRuntime(
        repository,
        DelegatingProvider(),
        tools,
        Settings(interactive_tools=True),
        tool_policy=deny_writes,
    )
    try:
        run = await start(runtime)
        assert (await wait(runtime, run)).status == "completed" and not effects
        assert not await repository.list_interactions(run.id)
        parent_result = next(
            message for message in await repository.run_messages(run.id) if message.role == "tool"
        )
        child_run_id = parent_result.details["data"]["run_id"]
        child_result = next(
            message
            for message in await repository.run_messages(child_run_id)
            if message.role == "tool"
        )
        assert child_result.details["code"] == "permission_denied"
        assert not await repository.list_interactions(child_run_id)
        assert not manager._handles
    finally:
        await runtime.close()
        await manager.close()


async def test_persistent_agent_does_not_publish_to_completed_parent_or_reuse_deadline(
    repository, monkeypatch
):
    async def policy(definition, _context):
        return "allow" if definition.risk == "read" else "ask"

    effects = []
    manager = AgentManager(repository)
    handles = []
    parent_deadlines = []
    tools = ToolRegistry()

    @tool(name="make_agent", risk="read")
    async def make_agent(context: ToolContext):
        parent_deadlines.extend(
            budget.deadline.when()
            for budget in context.execution_timeouts
            if budget.deadline.when() is not None
        )
        handle = await manager.spawn(
            AgentDefinition(
                name="persistent",
                provider=WriterProvider(),
                tools=writer_tools(effects),
                persistent=True,
                timeout_seconds=5,
            ),
            parent_context=context,
        )
        handles.append(handle)
        return {"agent_id": handle.agent_id}

    class FactoryProvider:
        name = "interactive-offline-factory"

        async def stream(self, messages, _tools):
            if any(message.role == "tool" for message in messages):
                yield ModelText(text="句柄已创建")
            else:
                yield ModelToolCall(id="make-call", name="make_agent", arguments={})

    tools.register(make_agent)
    runtime = ChatRuntime(
        repository, FactoryProvider(), tools, Settings(tool_timeout_seconds=1), tool_policy=policy
    )
    operation = None
    try:
        parent = await start(runtime)
        assert (await wait(runtime, parent)).status == "completed"
        assert handles and parent_deadlines
        frames = await repository.list_events(parent.id)
        handle = handles[0]
        ready = asyncio.Event()
        stream_state = handle.runtime.stream_state

        async def observed_state(*args, **kwargs):
            state = await stream_state(*args, **kwargs)
            ready.set()
            return state

        monkeypatch.setattr(handle.runtime, "stream_state", observed_state)
        operation = asyncio.create_task(handle.execute("write"))
        await asyncio.wait_for(ready.wait(), 5)
        approval, _ = await request_part(handle.runtime, handle.current_run)
        await asyncio.sleep(
            max(0, max(parent_deadlines) - asyncio.get_running_loop().time()) + 0.05
        )
        assert not effects
        await runtime.interactions.reply(
            approval.run_id, approval.id, InteractionReply(action="approve")
        )
        assert (await asyncio.wait_for(operation, 5)).status == "completed"
        assert effects == ["original"]
        assert await repository.list_events(parent.id) == frames
    finally:
        await manager.close()
        await runtime.close()
        if operation is not None:
            await asyncio.gather(operation, return_exceptions=True)


async def test_parallel_questions_keep_shared_parent_deadline_paused_until_both_answers(repository):
    async def allow(_definition, _context):
        return "allow"

    tools = builtin_registry()

    @tool(name="ask_pair", risk="read")
    async def ask_pair(context: ToolContext):
        return await asyncio.gather(
            context.call_tool("question", {"question": "第一问"}),
            context.call_tool("question", {"question": "第二问"}),
        )

    class PairProvider:
        name = "interactive-offline-pair"

        async def stream(self, messages, _tools):
            result = next((message for message in messages if message.role == "tool"), None)
            if result is None:
                yield ModelToolCall(id="pair-call", name="ask_pair", arguments={})
            else:
                yield ModelText(text=result.content)

    tools.register(ask_pair)
    runtime = ChatRuntime(
        repository,
        PairProvider(),
        tools,
        Settings(tool_timeout_seconds=0.2, run_timeout_seconds=5),
        tool_policy=allow,
    )
    try:
        run = await start(runtime)
        first, cursor = await request_part(runtime, run)
        second, _ = await request_part(runtime, run, from_seq=cursor)
        assert first.call_id != second.call_id
        await asyncio.sleep(0.3)
        await runtime.interactions.reply(
            run.id, first.id, InteractionReply(action="answer", answer="答案一")
        )
        await asyncio.sleep(0.3)
        assert (await repository.get_run(run.id)).status == "running"
        assert (await repository.get_interaction(run.id, second.id)).status == "pending"
        await runtime.interactions.reply(
            run.id, second.id, InteractionReply(action="answer", answer="答案二")
        )
        assert (await wait(runtime, run)).status == "completed"
        text = (await repository.run_messages(run.id))[-1].content
        assert "答案一" in text and "答案二" in text
    finally:
        await runtime.close()
