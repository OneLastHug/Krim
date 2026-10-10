"""会话关闭、原子轮次、专属话题、TTL、父子边界与真实恢复。"""

import asyncio
import time
from dataclasses import replace

import pytest
from alembic import command
from sqlalchemy import select, text, update

from krim_agent.agents import AgentDefinition, AgentManager, AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentPromptRequest, AgentService
from krim_agent.persistence.database import AgentHandleRow, AgentSessionRow, RunRow
from krim_agent.persistence.migration import migration_config
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.providers import FakeProvider, ModelText
from krim_agent.sdk import PythonAI
from krim_agent.tools import ToolRegistry
from krim_agent.tools.builtins import builtin_registry


async def test_close_reopen_cas_retains_topic_turn_limit_and_historical_retries(runtime):
    service = AgentService(runtime, runtime.settings)
    try:
        await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name="worker",
                    description="会话",
                    tool_names=["add"],
                    max_turns=2,
                ),
            )
        )
        spawned = await service.spawn("worker")
        first = await service.execute(
            spawned.agent_id, AgentPromptRequest(prompt="/add 1 2", idempotency_key="first")
        )
        closed = await service.close_session(spawned.agent_id, spawned.session.revision)
        assert closed.status == "closed" and closed.turn == 1
        with pytest.raises(ServiceError) as denied:
            await service.execute(spawned.agent_id, AgentPromptRequest(prompt="拒绝新输入"))
        assert denied.value.code == "agent_session_closed"
        with pytest.raises(ServiceError) as stale:
            await service.reopen_session(spawned.agent_id, 0)
        assert stale.value.code == "agent_session_conflict"
        reopened = await service.reopen_session(spawned.agent_id, closed.revision)
        assert reopened.turn == 1 and reopened.topic_id == spawned.topic_id
        second = await service.resume(
            spawned.agent_id, AgentPromptRequest(prompt="/add 3 4", idempotency_key="second")
        )
        retry = await service.execute(
            spawned.agent_id, AgentPromptRequest(prompt="/add 1 2", idempotency_key="first")
        )
        assert retry.result.run_id == first.result.run_id and retry.result.turn == 1
        assert retry.current_run_id == second.current_run_id and retry.turn == 2
        assert (await service.get(spawned.agent_id)).result.run_id == second.result.run_id
        with pytest.raises(ServiceError) as conflict:
            await service.execute(
                spawned.agent_id, AgentPromptRequest(prompt="其他输入", idempotency_key="first")
            )
        assert conflict.value.code == "idempotency_conflict"
        exhausted = await service.execute(spawned.agent_id, AgentPromptRequest(prompt="第三轮"))
        assert exhausted.result.code == "max_turns"
        turns = await service.session_turns(spawned.agent_id)
        assert [value.turn for value in turns] == [1, 2]
        assert len(await runtime.repository.audits(first.current_run_id)) == 1
        assert (await runtime.repository.list_events(first.current_run_id))[0].data[
            "session_id"
        ] == spawned.session_id
    finally:
        await service.close()


async def test_http_managed_topic_rejects_chat_edit_regenerate_and_foreign_run(api, repository):
    spawned = (await api.post("/v1/agents", json={"name": "default"})).json()
    identifier, topic = spawned["agent_id"], spawned["topic_id"]
    before = await repository.list_messages(topic)
    rejected = await api.post(
        f"/v1/topics/{topic}/messages", json={"text": "绕过权限", "idempotency_key": "bypass"}
    )
    assert rejected.status_code == 403 and rejected.json()["error"]["code"] == "agent_topic_managed"
    assert await repository.list_messages(topic) == before
    first = (await api.post(f"/v1/agents/{identifier}/execute", json={"prompt": "正常输入"})).json()
    messages = await repository.list_messages(topic)
    for action in ("edit", "regenerate"):
        payload = {"idempotency_key": action}
        if action == "edit":
            payload["text"] = "非法编辑"
        rejected = await api.post(
            f"/v1/topics/{topic}/messages/{messages[0].id}/{action}", json=payload
        )
        assert rejected.status_code == 403
    other = (await api.post("/v1/agents", json={"name": "default"})).json()
    foreign = await api.get(
        f"/v1/agents/{other['agent_id']}/events", params={"run_id": first["current_run_id"]}
    )
    assert foreign.status_code == 403
    assert (await api.get(f"/v1/agents/{identifier}/turns")).json()[0]["run_id"] == first[
        "current_run_id"
    ]
    closed = await api.post(f"/v1/agents/{identifier}/session/close", json={"expected_revision": 0})
    assert closed.json()["status"] == "closed"
    reopened = await api.post(
        f"/v1/agents/{identifier}/session/reopen", json={"expected_revision": 1}
    )
    assert reopened.json()["status"] == "open"
    await api.delete(f"/v1/agents/{identifier}")
    assert (await api.get(f"/v1/agents/{identifier}/session")).json()["status"] == "destroyed"
    rejected = await api.post(
        f"/v1/topics/{topic}/messages", json={"text": "销毁后旁路", "idempotency_key": "destroyed"}
    )
    assert rejected.status_code == 403


class WaitingProvider:
    name = "session-wait"

    def __init__(self):
        self.entered, self.closed = asyncio.Event(), asyncio.Event()

    async def stream(self, _messages, _tools):
        self.entered.set()
        try:
            yield ModelText(text="partial")
            await asyncio.Event().wait()
        finally:
            self.closed.set()


async def test_ttl_stops_real_production_and_cannot_be_reopened(repository):
    provider = WaitingProvider()
    manager = AgentManager(repository)
    try:
        handle = await manager.spawn(
            AgentDefinition(
                name="short",
                provider=provider,
                tools=ToolRegistry(),
                session_ttl_seconds=60,
            )
        )
        task = asyncio.create_task(handle.execute("wait"))
        await asyncio.wait_for(provider.entered.wait(), 3)
        # 生产确实进入后再给同一会话0.4秒TTL，初始化耗时不混入停止断言。
        await handle._close_expiry()
        expires_at = time.time() + 0.4
        async with repository.database.write_lock, repository.database.sessions.begin() as session:
            row = await session.get(AgentSessionRow, handle.session_id)
            row.expires_at = expires_at
        handle._expiry = asyncio.create_task(handle._expire(expires_at))
        result = await asyncio.wait_for(task, 3)
        assert result.status == "aborted" and provider.closed.is_set()
        session = await handle.session()
        assert session.status == "expired" and session.turn == 1
        with pytest.raises(ServiceError) as expired:
            await handle.reopen_session(session.revision)
        assert expired.value.code == "agent_session_expired"
        assert manager._semaphore._value == 8
    finally:
        await manager.close()


async def test_session_already_expired_never_enters_provider(repository):
    provider = WaitingProvider()
    manager = AgentManager(repository)
    try:
        handle = await manager.spawn(
            AgentDefinition(name="expired", provider=provider, tools=ToolRegistry())
        )
        async with repository.database.write_lock, repository.database.sessions.begin() as session:
            row = await session.get(AgentSessionRow, handle.session_id)
            row.expires_at = time.time() - 1
        with pytest.raises(ServiceError) as error:
            await handle.execute("must not start")
        assert error.value.code == "agent_session_expired"
        assert not provider.entered.is_set()
        assert not await repository.list_messages(handle.topic_id)
        assert (await handle.session()).turn == 0
    finally:
        await manager.close()


async def test_close_queued_session_does_not_create_run_and_releases_queue(repository):
    provider = WaitingProvider()
    manager = AgentManager(repository, max_concurrent=1)
    try:
        definition = AgentDefinition(name="queued", provider=provider, tools=ToolRegistry())
        first, second = await manager.spawn(definition), await manager.spawn(definition)
        busy = asyncio.create_task(first.execute("busy"))
        await asyncio.wait_for(provider.entered.wait(), 3)
        queued = asyncio.create_task(second.execute("queued"))
        async with asyncio.timeout(3):
            while not second.pending_start:  # noqa: ASYNC110 - wait for scheduler handoff
                await asyncio.sleep(0.01)
        closed = await second.close_session(0)
        outcome = await asyncio.gather(queued, return_exceptions=True)
        assert closed.status == "closed" and isinstance(outcome[0], asyncio.CancelledError)
        assert (await second.session()).turn == 0
        assert not await repository.list_messages(second.topic_id)
        await first.stop()
        await busy
        assert manager._semaphore._value == 1
    finally:
        await manager.close()


async def test_direct_child_inherits_mode_and_parent_close_stops_child(repository):
    manager = AgentManager(repository)
    try:
        parent = await manager.spawn(
            AgentDefinition(
                name="parent",
                provider=FakeProvider(delay=0),
                tools=builtin_registry(),
                mode="ask",
            )
        )
        child = await manager.spawn(
            AgentDefinition(
                name="child",
                provider=FakeProvider(delay=0),
                tools=builtin_registry(),
                parent_agent_id=parent.agent_id,
            )
        )
        result = await child.execute("/add 2 3")
        assert not (await repository.audits(result.run_id))[0].ok
        assert (await child.session()).parent_session_id == parent.session_id
        assert child.topic_id != parent.topic_id
        provider = WaitingProvider()
        live = await manager.spawn(
            AgentDefinition(
                name="livechild",
                provider=provider,
                tools=ToolRegistry(),
                parent_agent_id=parent.agent_id,
            )
        )
        task = asyncio.create_task(live.execute("wait"))
        await asyncio.wait_for(provider.entered.wait(), 3)
        await parent.close_session(0)
        assert (await asyncio.wait_for(task, 3)).status == "aborted" and provider.closed.is_set()
        assert (await live.session()).status == "closed"
        with pytest.raises(ServiceError) as refused:
            await live.reopen_session((await live.session()).revision)
        assert refused.value.code == "agent_parent_session_closed"
        assert (await live.session()).status == "closed"
    finally:
        await manager.close()


async def test_session_start_cannot_forge_another_owner_or_topic(repository):
    manager = AgentManager(repository)
    try:
        handle = await manager.spawn(
            AgentDefinition(name="owner", provider=FakeProvider(delay=0), tools=ToolRegistry())
        )
        for kwargs, code in [
            (
                {
                    "agent_session_id": handle.session_id,
                    "agent_id": handle.agent_id,
                    "agent_owner_id": "forged",
                },
                "agent_session_owner",
            ),
            (
                {
                    "agent_session_id": "forged",
                    "agent_id": handle.agent_id,
                    "agent_owner_id": manager.owner_id,
                },
                "agent_topic_managed",
            ),
        ]:
            with pytest.raises(ServiceError) as denied:
                await repository.begin_run(
                    handle.topic_id, ChatRequest(text="拒绝", idempotency_key=code), **kwargs
                )
            assert denied.value.code == code
        assert (await handle.session()).turn == 0
    finally:
        await manager.close()


async def test_persistent_closed_session_sdk_restart_preserves_budget_and_history(tmp_path):
    from krim_agent.config import Settings

    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path / 'session.sqlite'}")
    async with await PythonAI.create(settings=settings, provider=FakeProvider(delay=0)) as ai:
        session = await ai.agent(persistent=True)
        first = await session.execute("/add 2 3", idempotency_key="one")
        closed = await session.close()
        assert closed.turn == 1
        identifier = session.agent_id
    async with await PythonAI.create(settings=settings, provider=FakeProvider(delay=0)) as cold:
        session = cold.agent_session(identifier)
        assert (await session.state()).status == "closed"
        restored = await session.restore()
        assert restored.session.status == "closed" and restored.result.run_id == first.run_id
        assert (await session.reopen()).turn == 1
        second = await session.resume("/echo next", idempotency_key="two")
        assert second.turn == 2 and second.run_id != first.run_id
        assert [t.run_id for t in await session.turns()] == [first.run_id, second.run_id]
        frames = [f async for f in session.events(2, run_id=first.run_id)]
        assert frames[0].seq == 2 and {f.stream_id for f in frames} == {first.run_id}
        assert (await (await session.stream(run_id=first.run_id)).collect()).run.id == first.run_id


async def test_atomic_turn_recovers_when_handle_current_pointer_is_stale(runtime):
    service = AgentService(runtime, runtime.settings)
    view = await service.spawn("default", persistent=True)
    result = await service.execute(
        view.agent_id, AgentPromptRequest(prompt="已完成", idempotency_key="once")
    )
    await service.close()
    # 模拟 Run 事务成功、handle 指针尚未保存的进程退出窗口。
    async with runtime.repository.database.sessions.begin() as db:
        await db.execute(
            update(AgentHandleRow)
            .where(AgentHandleRow.agent_id == view.agent_id)
            .values(turn=0, current_run_id=None)
        )
    cold = AgentService(runtime, runtime.settings)
    try:
        assert await cold.restore_persistent() == 1
        restored = await cold.get(view.agent_id)
        assert restored.turn == 1 and restored.current_run_id == result.current_run_id
        retry = await cold.execute(
            view.agent_id, AgentPromptRequest(prompt="已完成", idempotency_key="once")
        )
        assert retry.current_run_id == result.current_run_id and retry.turn == 1
        assert len(await cold.session_turns(view.agent_id)) == 1
    finally:
        await cold.close()


async def test_session_migration_backfills_old_persistent_handles_and_protects_downgrade(
    repository,
):
    database = repository.database
    async with database.engine.begin() as connection:
        await connection.run_sync(
            lambda sync: command.downgrade(migration_config(sync), "0021_agent_profiles")
        )
    topic = await repository.create_topic("旧实例")
    async with database.sessions.begin() as db:
        db.add(
            RunRow(
                id="legacy-run",
                topic_id=topic.id,
                input_message_id="legacy-user",
                idempotency_key="legacy",
                input_hash="0" * 64,
                status="completed",
            )
        )
    async with database.engine.begin() as connection:
        await connection.execute(
            text(
                "INSERT INTO agent_handles "
                "(agent_id,name,topic_id,persistent,turn,status,definition_hash,revision,"
                "updated_at) "
                "VALUES ('old','default',:topic,1,0,'created',:hash,0,'2026-10-07')"
            ),
            {"topic": topic.id, "hash": "0" * 64},
        )
    await database.initialize()
    async with database.sessions() as db:
        row = await db.scalar(select(AgentSessionRow).where(AgentSessionRow.agent_id == "old"))
        assert row.session_id == "old" and row.topic_id == topic.id and row.status == "open"
        assert row.turn == 1 and row.current_run_id == "legacy-run"
    with pytest.raises(RuntimeError, match="拒绝有损降级"):
        async with database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(migration_config(sync), "0021_agent_profiles")
            )


async def test_parent_ttl_bounds_child_and_child_destroy_keeps_host_tools(repository):
    manager = AgentManager(repository)
    host_tools = builtin_registry()
    try:
        definition = AgentDefinition(
            name="parent", provider=FakeProvider(delay=0), tools=host_tools, session_ttl_seconds=30
        )
        parent = await manager.spawn(definition)
        child = await manager.spawn(
            replace(
                definition, name="child", session_ttl_seconds=60, parent_agent_id=parent.agent_id
            )
        )
        assert (await child.session()).expires_at <= (await parent.session()).expires_at
        await child.destroy()
        assert await host_tools.call_tool("echo", {"text": "仍可使用"}) == "仍可使用"
        assert (await parent.session()).status == "open"
    finally:
        await manager.close()


async def test_session_client_cli_history_controls_and_structured_errors(server_url):
    import json
    import sys

    from krim_agent.client import ClientError, KrimClient

    def create():
        with KrimClient(server_url) as client:
            value = client.agent_spawn("default", persistent=True)
            one = client.agent_execute(value.agent_id, "/echo first", "one")
            two = client.agent_resume(value.agent_id, "/echo second", "two")
            frames = list(client.agent_events(value.agent_id, 2, run_id=one.current_run_id))
            assert {f.stream_id for f in frames} == {one.current_run_id}
            return value, two

    value, second = await asyncio.to_thread(create)

    async def cli(*arguments):
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "krim_agent",
            "agent",
            *arguments,
            "--url",
            server_url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, errors = await asyncio.wait_for(process.communicate(), 20)
        assert process.returncode == 0, errors.decode()
        return json.loads(output)

    snapshot = await cli("session", value.agent_id)
    assert snapshot["turn"] == 2 and snapshot["status"] == "open"
    assert len(await cli("turns", value.agent_id)) == 2
    closed = await cli("close", value.agent_id, "--revision", str(snapshot["revision"]))
    assert closed["status"] == "closed"

    def stale():
        with KrimClient(server_url) as client:
            with pytest.raises(ClientError) as denied:
                client.agent_session_reopen(value.agent_id, snapshot["revision"])
            assert denied.value.code == "agent_session_conflict" and denied.value.status == 409

    await asyncio.to_thread(stale)
    reopened = await cli("reopen", value.agent_id, "--revision", str(closed["revision"]))
    assert reopened["current_run_id"] == second.current_run_id and reopened["turn"] == 2


async def test_session_close_reopen_and_restart_never_reset_accumulated_budget(tmp_path):
    from krim_agent.budgets import UsageBudgetConfig
    from krim_agent.config import Settings
    from krim_agent.providers import ModelUsage
    from krim_agent.usage import TokenUsage

    class Metered:
        name = "session-metered"

        async def stream(self, _messages, _tools):
            yield ModelText(text="完成")
            yield ModelUsage(usage=TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7))

    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'budget-session.sqlite'}",
        context_tokenizer="cl100k_base",
        context_reserved_output_tokens=32,
        agent_usage_budget=UsageBudgetConfig(max_total_tokens=1000),
    )
    async with await PythonAI.create(
        settings=settings, provider=Metered(), tools=ToolRegistry()
    ) as ai:
        session = await ai.agent(persistent=True)
        await session.execute("first")
        accounts = (
            await ai.agents()
            .manager._handles[session.agent_id]
            .runtime.ensure_usage_budget_binding()
        )
        assert (await ai.runtime.budget_store.account(accounts[-1])).spent_tokens == 7
        await session.close()
        await session.reopen()
        await session.resume("second")
        assert (await ai.runtime.budget_store.account(accounts[-1])).spent_tokens == 14
        identifier = session.agent_id
    async with await PythonAI.create(
        settings=settings, provider=Metered(), tools=ToolRegistry()
    ) as cold:
        session = cold.agent_session(identifier)
        await session.restore()
        bindings = (
            await cold.agents().manager._handles[identifier].runtime.ensure_usage_budget_binding()
        )
        assert bindings == accounts
        assert (await cold.runtime.budget_store.account(bindings[-1])).spent_tokens == 14
        assert (await session.state()).turn == 2


async def test_real_crash_after_session_run_commit_restores_without_repeating_model(tmp_path):
    import json
    import sys

    async def worker(mode, *arguments):
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "tests/fixtures/agent_session_worker.py",
            str(tmp_path / "crash.sqlite"),
            mode,
            *arguments,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, errors = await asyncio.wait_for(process.communicate(), 20)
        assert process.returncode == 0, errors.decode()
        return json.loads(output)

    first = await worker("crash")
    await asyncio.sleep(0.3)
    cold = await worker("restore", first["agent_id"])
    assert cold["agent_id"] == first["agent_id"] and cold["run_id"] == first["run_id"]
    assert cold["turn"] == 1 and cold["calls"] == 0
