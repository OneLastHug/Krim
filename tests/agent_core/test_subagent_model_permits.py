from __future__ import annotations

import asyncio
from typing import Any

import pytest

from krim_agent.contracts import Message, RunEvent, ServiceError
from krim_agent.providers.types import ModelText
from krim_agent.runtime.model_step import ModelStep
from krim_agent.subagents.scheduling import RequestLimiter


class BlockingProvider:
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0

    async def stream(self, messages: Any, tools: Any):
        self.calls += 1
        self.entered.set()
        await self.release.wait()
        yield ModelText(text="done")

    async def close(self) -> None:
        pass


def step(
    provider: BlockingProvider, limiter: RequestLimiter, owner: str, **kwargs: Any
) -> ModelStep:
    async def emit(kind: str, data: Any) -> RunEvent:
        return RunEvent(run_id=owner, seq=0, event_id=kind, kind=kind, data=data)

    return ModelStep(
        provider,
        emit,
        lambda: None,
        max_steps=3,
        max_output=None,
        deadline=None,
        workspace=False,
        request_slot=lambda index: limiter.slot(owner),
        **kwargs,
    )


async def test_only_actual_model_requests_hold_shared_permits() -> None:
    limiter = RequestLimiter(1)
    one, two = BlockingProvider(), BlockingProvider()
    history = [Message(role="user", content="task")]
    root = asyncio.create_task(step(one, limiter, "root").call(history, []))
    await one.entered.wait()
    child = asyncio.create_task(step(two, limiter, "child").call(history, []))
    await asyncio.sleep(0)
    assert one.calls == 1 and two.calls == 0 and limiter.active == 1
    one.release.set()
    assert (await root).text == "done"
    await asyncio.wait_for(two.entered.wait(), 2)
    assert two.calls == 1
    two.release.set()
    assert (await child).text == "done"
    assert limiter.active == 0


async def test_queued_model_timeout_does_not_send_or_leak_permit() -> None:
    limiter = RequestLimiter(1)
    held = await limiter.acquire("earlier")
    provider = BlockingProvider()
    model = step(provider, limiter, "child", model_timeout=0.02)
    with pytest.raises(ServiceError) as error:
        await model.call([Message(role="user", content="task")], [])
    assert error.value.code == "model_timeout"
    assert provider.calls == 0 and limiter.waiting == 0 and limiter.active == 1
    await held.release()
    assert limiter.active == 0
