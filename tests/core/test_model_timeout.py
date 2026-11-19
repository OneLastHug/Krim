"""The configured per-model deadline never extends the enclosing run deadline."""

import asyncio

import pytest

from krim_agent.contracts import Message
from krim_agent.providers.types import ModelText
from krim_agent.runtime.model_step import ModelStep


class SlowModel:
    async def stream(self, messages, tools):
        await asyncio.sleep(0.05)
        yield ModelText(text="finished")


@pytest.mark.parametrize(
    "model_timeout,remaining,success",
    [
        (0.3, 1, True),
        (0.01, 1, False),
        (0.3, 0.01, False),
    ],
)
async def test_model_step_uses_configured_and_run_deadlines(model_timeout, remaining, success):
    events = []

    async def emit(kind, data):
        events.append(kind)

    step = ModelStep(
        SlowModel(),
        emit,
        lambda: None,
        max_steps=1,
        max_output=1024,
        deadline=asyncio.get_running_loop().time() + remaining,
        workspace=False,
        model_timeout=model_timeout,
    )
    if success:
        result = await step.call([Message(role="user", content="hello")], [])
        assert result.text == "finished" and "model_end" in events
    else:
        with pytest.raises(TimeoutError):
            await step.call([Message(role="user", content="hello")], [])
        assert "model_start" in events and "model_end" not in events
