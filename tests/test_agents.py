import asyncio

import pytest

from krim_agent.agents import AgentDefinition, AgentError, AgentManager
from krim_agent.persistence import Database, KrimRepository
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import FakeProvider, ModelText
from krim_agent.tools import ToolRegistry
from krim_agent.tools.builtins import builtin_registry


async def test_execute_resume_events_and_parent_tool_binding(repository):
    manager = AgentManager(repository, max_concurrent=2)
    definition = AgentDefinition(
        name="calculator",
        provider=FakeProvider(delay=0),
        tools=builtin_registry(),
        max_turns=3,
        parent_tool_call_id="parent-call",
    )
    handle = await manager.spawn(definition)
    first = await handle.execute("/add 2 3", "turn-one")
    assert first.status == "completed" and "5.0" in first.text
    assert first.parent_tool_call_id == "parent-call" and first.turn == 1
    frames = [frame async for frame in handle.events()]
    assert frames[-1].kind == "end"
    resumed = await handle.resume("/echo next", "turn-two")
    assert resumed.status == "completed" and "next" in resumed.text and resumed.turn == 2
    assert (await repository.list_messages(handle.topic_id))[-1].content.endswith("next")
    retry = await handle.execute("/echo next", "turn-two")
    assert retry.run_id == resumed.run_id and retry.turn == resumed.turn
    with pytest.raises(ServiceError) as error:
        await handle.execute("different text", "turn-two")
    assert error.value.code == "idempotency_conflict"
    destroyed = await handle.destroy()
    assert destroyed.status == "destroyed"
    with pytest.raises(AgentError) as error:
        await handle.execute("after destroy")
    assert error.value.code == "agent_destroyed"
    await manager.destroy_all()


class WaitingProvider:
    name = "agent-waiting"

    def __init__(self):
        self.entered = asyncio.Event()

    async def stream(self, _messages, _tools):
        yield ModelText(text="partial")
        self.entered.set()
        await asyncio.Event().wait()


async def test_stop_destroy_and_timeout_cleanup(repository):
    provider = WaitingProvider()
    manager = AgentManager(repository)
    handle = await manager.spawn(
        AgentDefinition(
            name="stop-me",
            provider=provider,
            tools=ToolRegistry(),
            timeout_seconds=10,
        )
    )
    task = asyncio.create_task(handle.execute("wait", "stop-once"))
    await asyncio.wait_for(provider.entered.wait(), 2)
    stopped = await handle.stop()
    assert stopped.status == "aborted" and stopped.code == "cancelled"
    assert (await task).status == "aborted"
    assert "partial" in (await repository.list_messages(handle.topic_id))[-1].content

    timeout_provider = WaitingProvider()
    timeout_handle = await manager.spawn(
        AgentDefinition(
            name="timeout",
            provider=timeout_provider,
            tools=ToolRegistry(),
            timeout_seconds=0.01,
        )
    )
    result = await timeout_handle.execute("wait", "timeout")
    assert result.status == "aborted" and result.code == "timeout"
    assert (await timeout_handle.destroy()).status == "destroyed"
    await manager.destroy_all()


async def test_busy_execute_and_forked_hooks_are_enforced(repository):
    provider = WaitingProvider()
    seen: list[str] = []
    source = ToolRegistry()

    async def pre(_definition, _context):
        seen.append("pre")

    source.add_global_pre(pre)
    manager = AgentManager(repository)
    handle = await manager.spawn(AgentDefinition(name="hooked", provider=provider, tools=source))
    task = asyncio.create_task(handle.execute("wait", "busy"))
    await asyncio.wait_for(provider.entered.wait(), 2)
    with pytest.raises(AgentError) as error:
        await handle.execute("again", "busy-again")
    assert error.value.code == "agent_busy"
    assert await handle.stop()
    await task
    # 只读工具无调用时 hook 不会凭空触发；fork 仍保留同一 hook 给后续工具执行。
    assert seen == []
    await handle.destroy()
    source_with_tools = builtin_registry()
    source_with_tools.add_global_pre(pre)
    hooked = await manager.spawn(
        AgentDefinition(
            name="inherits-hooks", provider=FakeProvider(delay=0), tools=source_with_tools
        )
    )
    assert (await hooked.execute("/echo hook")).status == "completed"
    assert seen == ["pre"]
    await hooked.destroy()
    assert (
        await source_with_tools.call_tool("echo", {"text": "host still works"})
        == "host still works"
    )
    assert seen == ["pre", "pre"]


async def test_turn_concurrency_recursion_and_agent_limit(repository):
    manager = AgentManager(repository, max_agents=4, max_concurrent=1, max_recursion_depth=2)
    definition = AgentDefinition(
        name="limited",
        provider=FakeProvider(delay=0),
        tools=builtin_registry(),
        max_turns=1,
    )
    first = await manager.spawn(definition)
    assert (await first.execute("hello", "one")).status == "completed"
    budget = await first.resume("second", "two")
    assert budget.status == "failed" and budget.code == "max_turns"
    await manager.spawn(definition)
    limit_manager = AgentManager(repository, max_agents=2)
    await limit_manager.spawn(definition)
    await limit_manager.spawn(definition)
    with pytest.raises(AgentError) as error:
        await limit_manager.spawn(definition)
    assert error.value.code == "agent_limit"
    await limit_manager.destroy_all()
    child = await manager.spawn(
        AgentDefinition(
            name="child",
            provider=FakeProvider(delay=0),
            tools=builtin_registry(),
            parent_agent_id=first.agent_id,
        )
    )
    assert child.definition.parent_agent_id == first.agent_id
    with pytest.raises(AgentError) as error:
        await manager.spawn(
            AgentDefinition(
                name="too-deep",
                provider=FakeProvider(delay=0),
                tools=builtin_registry(),
                parent_agent_id=child.agent_id,
            )
        )
    assert error.value.code == "recursion_limit"
    await manager.destroy_all()


async def test_two_agents_share_repository_but_keep_topics_and_audits_separate(repository):
    manager = AgentManager(repository, max_concurrent=2)
    one = await manager.spawn(
        AgentDefinition(name="one", provider=FakeProvider(delay=0), tools=builtin_registry())
    )
    two = await manager.spawn(
        AgentDefinition(name="two", provider=FakeProvider(delay=0), tools=builtin_registry())
    )
    results = await asyncio.gather(one.execute("/add 1 2"), two.execute("/add 3 4"))
    assert {result.status for result in results} == {"completed"}
    assert results[0].run_id != results[1].run_id
    assert "3.0" in results[0].text and "7.0" in results[1].text
    assert len(await repository.audits(results[0].run_id)) == 1
    assert len(await repository.audits(results[1].run_id)) == 1
    await manager.destroy_all()


async def test_manager_can_use_persistent_database_after_reopen(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'agents.sqlite'}")
    await database.initialize()
    repository = KrimRepository(database)
    manager = AgentManager(repository)
    handle = await manager.spawn(
        AgentDefinition(name="persist", provider=FakeProvider(delay=0), tools=builtin_registry())
    )
    result = await handle.execute("/echo retained")
    await handle.destroy()
    await database.close()
    reopened = Database(f"sqlite+aiosqlite:///{tmp_path / 'agents.sqlite'}")
    await reopened.initialize()
    restored = KrimRepository(reopened)
    assert (await restored.get_run(result.run_id)).status == "completed"
    assert "retained" in (await restored.run_messages(result.run_id))[-1].content
    await reopened.close()


async def test_persistent_handle_survives_consumer_cancel_and_tool_scope_closes(repository):
    provider = WaitingProvider()
    manager = AgentManager(repository)
    consumers = []
    try:
        persistent = await manager.spawn(
            AgentDefinition(
                name="persistent",
                provider=provider,
                tools=ToolRegistry(),
                persistent=True,
                timeout_seconds=30,
            )
        )
        task = asyncio.create_task(persistent.execute("continue", "persistent"))
        consumers.append(task)
        await asyncio.wait_for(provider.entered.wait(), 10)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert (await repository.get_run(persistent.current_run.id)).status == "running"
        await persistent.stop()

        scoped_provider = WaitingProvider()
        scoped = await manager.spawn(
            AgentDefinition(
                name="scoped",
                provider=scoped_provider,
                tools=ToolRegistry(),
                parent_tool_call_id="scope-call",
                timeout_seconds=30,
            )
        )
        scoped_task = asyncio.create_task(scoped.execute("stop on consumer cancel", "scoped"))
        consumers.append(scoped_task)
        await asyncio.wait_for(scoped_provider.entered.wait(), 10)
        scoped_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await scoped_task
        assert (await repository.get_run(scoped.current_run.id)).status == "aborted"
        await manager.close_tool_scope("scope-call")
        assert scoped.destroyed and not persistent.destroyed
        await persistent.destroy()
    finally:
        # 观察或断言失败也先正常收束生产者，不能依赖事件循环强制取消全部内部写任务。
        try:
            await manager.close()
        finally:
            await asyncio.gather(*consumers, return_exceptions=True)


async def test_queue_timeout_and_manager_close_are_explicit(repository):
    manager = AgentManager(repository, max_concurrent=1)
    first_provider = WaitingProvider()
    first = await manager.spawn(
        AgentDefinition(
            name="first", provider=first_provider, tools=ToolRegistry(), timeout_seconds=10
        )
    )
    first_task = asyncio.create_task(first.execute("first", "first"))
    await asyncio.wait_for(first_provider.entered.wait(), 2)
    second = await manager.spawn(
        AgentDefinition(
            name="second",
            provider=FakeProvider(delay=0),
            tools=builtin_registry(),
            timeout_seconds=0.01,
        )
    )
    queued = await second.execute("queued", "queued")
    assert queued.status == "aborted" and queued.code == "timeout" and queued.run_id is None
    await first.stop()
    await first_task
    await manager.close()
    with pytest.raises(AgentError) as error:
        await manager.spawn(
            AgentDefinition(name="closed", provider=FakeProvider(delay=0), tools=builtin_registry())
        )
    assert error.value.code == "manager_closed"


async def test_persistent_consumer_cancellation_retains_production_concurrency_slot(repository):
    manager = AgentManager(repository, max_concurrent=1)
    provider = WaitingProvider()
    first = await manager.spawn(
        AgentDefinition(
            name="persistent-slot",
            provider=provider,
            tools=ToolRegistry(),
            persistent=True,
            timeout_seconds=10,
        )
    )
    task = asyncio.create_task(first.execute("保留生产"))
    await asyncio.wait_for(provider.entered.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert (await repository.get_run(first.current_run.id)).status == "running"
    second = await manager.spawn(
        AgentDefinition(
            name="queued-slot",
            provider=FakeProvider(delay=0),
            tools=builtin_registry(),
            timeout_seconds=0.5,
        )
    )
    queued = await second.execute("不能超过生产并发")
    assert queued.status == "aborted" and queued.run_id is None and queued.code == "timeout"
    await first.stop()
    next_handle = await manager.spawn(
        AgentDefinition(
            name="after-slot",
            provider=FakeProvider(delay=0),
            tools=builtin_registry(),
            timeout_seconds=10,
        )
    )
    resumed = await next_handle.execute("名额已释放", "after-stop")
    assert resumed.status == "completed", resumed.model_dump()
    await manager.close()


@pytest.mark.parametrize("persistent", [False, True])
async def test_cancellation_during_start_tracks_accepted_run_and_cleans_up(
    repository, monkeypatch, persistent
):
    manager = AgentManager(repository, max_concurrent=1)
    provider = WaitingProvider()
    handle = await manager.spawn(
        AgentDefinition(
            name="start-cancel",
            provider=provider,
            tools=ToolRegistry(),
            persistent=persistent,
            timeout_seconds=10,
        )
    )
    started, release = asyncio.Event(), asyncio.Event()
    original_start = handle.runtime.start

    async def slow_start(*args, **kwargs):
        run = await original_start(*args, **kwargs)
        started.set()
        await release.wait()
        return run

    monkeypatch.setattr(handle.runtime, "start", slow_start)
    task = asyncio.create_task(handle.execute("启动阶段取消"))
    await asyncio.wait_for(started.wait(), 5)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert handle.current_run is not None and handle.turn == 1
    saved = await repository.get_run(handle.current_run.id)
    assert saved.status == ("running" if persistent else "aborted")
    await handle.destroy()
    assert handle.runtime.production_task(saved.id).done()
    assert not manager._handles
    await manager.close()


async def test_destroy_cancels_queued_execution_without_creating_orphan_run(
    repository, monkeypatch
):
    manager = AgentManager(repository, max_concurrent=1)
    provider = WaitingProvider()
    first = await manager.spawn(
        AgentDefinition(name="occupy", provider=provider, tools=ToolRegistry(), timeout_seconds=10)
    )
    first_task = asyncio.create_task(first.execute("占用名额"))
    await asyncio.wait_for(provider.entered.wait(), 5)
    second = await manager.spawn(
        AgentDefinition(
            name="queued-destroy",
            provider=FakeProvider(delay=0),
            tools=ToolRegistry(),
            timeout_seconds=10,
        )
    )
    queued = asyncio.Event()
    acquire = manager._semaphore.acquire

    async def observed_acquire():
        queued.set()
        return await acquire()

    monkeypatch.setattr(manager._semaphore, "acquire", observed_acquire)
    task = asyncio.create_task(second.execute("尚未开始"))
    await asyncio.wait_for(queued.wait(), 5)
    assert (await second.destroy()).status == "destroyed"
    with pytest.raises(asyncio.CancelledError):
        await task
    assert second.current_run is None and not await repository.list_messages(second.topic_id)
    assert (await repository.get_run(first.current_run.id)).status == "running"
    await first.stop()
    await first_task
    await manager.close()


async def test_destroy_live_persistent_agent_waits_for_real_production_terminal(repository):
    manager = AgentManager(repository, max_concurrent=1)
    provider = WaitingProvider()
    handle = await manager.spawn(
        AgentDefinition(
            name="persistent-destroy",
            provider=provider,
            tools=ToolRegistry(),
            persistent=True,
            timeout_seconds=10,
        )
    )
    task = asyncio.create_task(handle.execute("等待显式销毁"))
    await asyncio.wait_for(provider.entered.wait(), 5)
    run_id = handle.current_run.id
    results = await asyncio.gather(handle.destroy(), handle.destroy())
    assert all(result.status == "destroyed" for result in results)
    with pytest.raises(asyncio.CancelledError):
        await task
    assert (await repository.get_run(run_id)).status == "aborted"
    assert handle.runtime.production_task(run_id).done() and not manager._handles
    next_handle = await manager.spawn(
        AgentDefinition(name="after-destroy", provider=FakeProvider(delay=0), tools=ToolRegistry())
    )
    assert (await next_handle.execute("可以继续")).status == "completed"
    await manager.close()


async def test_self_destroy_is_rejected_without_deadlock(repository):
    manager = AgentManager(repository)
    captured = []

    class SelfDestroyProvider:
        name = "self-destroy-test"

        async def stream(self, _messages, _tools):
            with pytest.raises(AgentError) as error:
                await handle.destroy()
            captured.append(error.value.code)
            yield ModelText(text="拒绝自等待后正常结束")

    handle = await manager.spawn(
        AgentDefinition(name="self-destroy", provider=SelfDestroyProvider(), tools=ToolRegistry())
    )
    assert (await handle.execute("自销毁")).status == "completed"
    assert captured == ["agent_self_destroy"] and not handle.destroyed
    await manager.close()
