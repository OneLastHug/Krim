"""公开恢复的执行互斥/反复取消/关闭/回调自关闭与只读历史。"""

import asyncio

import pytest
from test_conversation_recovery import wanted
from test_conversation_recovery_public_consumers import setup

from krim_agent.application.agents import AgentPromptRequest
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import ModelText


async def test_recovery_holds_original_execution_lock_and_blocks_all_input_paths(
    runtime, monkeypatch
):
    service, agent, _, _ = await setup(runtime)
    entered, release = asyncio.Event(), asyncio.Event()
    original = service.conversation_recoveries.store.apply

    async def held(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    worker = None
    try:
        request = wanted(await agent.preview_conversation_recovery())
        monkeypatch.setattr(service.conversation_recoveries.store, "apply", held)
        worker = asyncio.create_task(agent.recover_conversation(request, confirmed=True))
        await asyncio.wait_for(entered.wait(), 2)
        handle = service._handle(agent.agent_id)
        assert handle._execute_lock.locked()
        for submit in (service.execute, service.start, service.resume):
            with pytest.raises(ServiceError) as error:
                await submit(
                    agent.agent_id,
                    AgentPromptRequest(prompt="不能在恢复期间执行", idempotency_key="new"),
                )
            assert error.value.code == "agent_busy"
        with pytest.raises(ServiceError):
            await agent.preview_conversation_recovery()
        assert await agent.conversation_recovery_by_key(request.request_key) is None
        assert not service._tasks
        release.set()
        result = await worker
        assert result.status == "context_recovered" and not handle._execute_lock.locked()
    finally:
        release.set()
        if worker is not None:
            await asyncio.gather(worker, return_exceptions=True)
        await service.close()


async def test_repeated_caller_cancel_waits_backend_cleanup_before_releasing_admission(
    runtime, monkeypatch
):
    service, agent, _, _ = await setup(runtime)
    entered, cleanup, release, exited = (
        asyncio.Event(),
        asyncio.Event(),
        asyncio.Event(),
        asyncio.Event(),
    )

    async def blocked(*_args, **_kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleanup.set()
            await release.wait()
            exited.set()

    worker = None
    try:
        request = wanted(await agent.preview_conversation_recovery())
        monkeypatch.setattr(service.conversation_recoveries.store, "apply", blocked)
        worker = asyncio.create_task(agent.recover_conversation(request, confirmed=True))
        await asyncio.wait_for(entered.wait(), 2)
        worker.cancel()
        await asyncio.wait_for(cleanup.wait(), 2)
        worker.cancel()
        await asyncio.sleep(0)
        worker.cancel()
        await asyncio.sleep(0)
        assert not worker.done() and not exited.is_set()
        assert (
            service._handle(agent.agent_id)._execute_lock.locked()
            and service.conversation_recoveries.active
        )
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await worker
        assert (
            exited.is_set()
            and not service.conversation_recoveries.operations
            and not service.conversation_recoveries.active
        )
        assert await agent.conversation_recovery_by_key(request.request_key) is None
    finally:
        release.set()
        if worker is not None:
            await asyncio.gather(worker, return_exceptions=True)
        await service.close()


async def test_close_waits_admitted_backend_and_blocks_new_calls(runtime, monkeypatch):
    service, agent, _, _ = await setup(runtime)
    entered, cleanup, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def blocked(*_args, **_kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleanup.set()
            await release.wait()

    worker = closing = None
    try:
        request = wanted(await agent.preview_conversation_recovery())
        monkeypatch.setattr(service.conversation_recoveries.store, "apply", blocked)
        worker = asyncio.create_task(agent.recover_conversation(request, confirmed=True))
        await asyncio.wait_for(entered.wait(), 2)
        closing = asyncio.create_task(service.close())
        await asyncio.wait_for(cleanup.wait(), 2)
        assert not closing.done()
        with pytest.raises(ServiceError) as error:
            await service.execute(agent.agent_id, AgentPromptRequest(prompt="关闭后禁止准入"))
        assert error.value.status == 503
        release.set()
        await closing
        assert (
            not service.conversation_recoveries.operations
            and not service.conversation_recoveries.active
        )
        with pytest.raises(asyncio.CancelledError):
            await worker
    finally:
        release.set()
        await asyncio.gather(
            *(task for task in (worker, closing) if task is not None), return_exceptions=True
        )
        await service.close()


@pytest.mark.parametrize("nested", [False, True])
async def test_recovery_callback_cannot_close_its_own_service_or_deadlock(
    runtime, monkeypatch, nested
):
    service, agent, _, _ = await setup(runtime)

    async def close_inside(_scope):
        if nested:
            await asyncio.create_task(service.close())
        else:
            await service.close()

    try:
        monkeypatch.setattr(service.conversation_recoveries.store, "preview", close_inside)
        with pytest.raises(ServiceError) as error:
            await asyncio.wait_for(agent.preview_conversation_recovery(), 2)
        assert error.value.code == "conversation_recovery_self_close" and not service._closed
        assert (
            service._close_task is None
            and not service.conversation_recoveries.operations
            and not service.conversation_recoveries.active
        )
    finally:
        await service.close()


async def test_original_recovery_key_remains_readonly_during_new_model_run(runtime):
    service, agent, _, _ = await setup(runtime)
    entered, release = asyncio.Event(), asyncio.Event()
    worker = None

    class Reader:
        name = "offline-read-history-during-run"

        async def stream(self, messages, tools):
            entered.set()
            await release.wait()
            yield ModelText(text="原key历史读取没有打断新轮次")

    try:
        request = wanted(await agent.preview_conversation_recovery())
        record = await agent.recover_conversation(request, confirmed=True)
        service._handle(agent.agent_id).runtime.provider = Reader()
        worker = asyncio.create_task(agent.execute("新的显式任务", idempotency_key="new-active"))
        await asyncio.wait_for(entered.wait(), 3)
        assert service._handle(agent.agent_id)._execute_lock.locked()
        assert await agent.conversation_recovery_by_key(request.request_key) == record
        with pytest.raises(ServiceError) as error:
            await agent.preview_conversation_recovery()
        assert error.value.code == "agent_busy"
        release.set()
        assert (await worker).status == "completed"
    finally:
        release.set()
        if worker is not None:
            await asyncio.gather(worker, return_exceptions=True)
        await service.close()
