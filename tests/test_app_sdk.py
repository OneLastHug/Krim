import asyncio

import pytest

from krim_agent.app import LocalApp
from krim_agent.protocol.domain import ChatRequest


async def test_host_handle_uses_real_runtime(runtime, repository):
    app = LocalApp(runtime)
    handle = app.handle
    assert handle.ready
    topic = await handle.create_topic("宿主")
    run = await handle.send(topic.id, ChatRequest(text="/add 3 4", idempotency_key="host"))
    frames = [frame async for frame in handle.events(run.id)]
    assert frames[-1].data["status"] == "completed"
    assert "7.0" in (await repository.list_messages(topic.id))[-1].content
    assert not (await handle.emit("missing")).ok
    await app.close()
    assert not handle.ready
    with pytest.raises(ConnectionError):
        await handle.emit("chat/send")


async def test_bridge_close_cancels_pending_calls():
    from krim_agent.protocol import InProcessBridge

    host, app = InProcessBridge.pair()
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def handler(_data):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    app.on("wait", handler)
    task = asyncio.create_task(host.emit("wait"))
    await asyncio.wait_for(entered.wait(), 1)
    await app.close()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cancelled.is_set()
    await host.close()
