"""The provider consumes real SessionMediaRun state without persisting image bytes."""

import base64
import json

import ai
import pytest
from ai import messages as sdk
from sqlalchemy import select

from krim_agent.agent_sdk.model import HostModelProvider
from krim_agent.contracts import ServiceError
from krim_agent.providers.types import ModelText
from krim_agent.runtime.model_step import ModelStep
from krim_agent.storage.models import run_events
from krim_agent.tools.registry import ToolRegistry
from tests.coding_agent.test_session_media import environment
from tests.coding_agent.test_view_image import PNG


async def test_real_media_run_provider_checkpoint_restore_and_acceptance(tmp_path):
    async with environment(tmp_path) as (store, host, media, artifacts, scope):
        saved = await artifacts.save(scope, PNG, "image/png")
        ready = {
            "status": "ready", "frame": None,
            "media_ref": saved.reference.model_dump(mode="json"),
        }
        await store.append(scope.run_id, "tool_end", {
            "name": "view_image", "status": "ok", "result_available": True,
            "provider_call_id": "view-message", "value": ready,
        })

        class Script:
            def __init__(self, fail=False):
                self.fail = fail
                self.images = []

            async def stream(self, history, tools):
                self.images.append([image.data for message in history
                                    for image in getattr(message, "images", ())])
                if self.fail:
                    raise ServiceError("provider_network")
                yield ModelText(text="image received")

        async def emit(kind, data):
            return await store.append(scope.run_id, kind, data)

        def provider(script):
            step = ModelStep(script, emit, scope.check, max_steps=None, max_output=None,
                             deadline=None, workspace=False)
            return HostModelProvider(step, ToolRegistry(()), max_group=1, max_calls=None,
                                     request_bytes=None, host_media=media.for_run(scope.run_id))

        history = [
            sdk.Message(role="user", parts=[sdk.TextPart(text="inspect image")]),
            sdk.Message(role="assistant", parts=[sdk.ToolCallPart(
                tool_call_id="view-message", tool_name="view_image",
                tool_args=json.dumps({"reference": saved.reference.reference}),
            )]),
            sdk.Message(role="tool", parts=[sdk.ToolResultPart(
                tool_call_id="view-message", tool_name="view_image",
                result=ready, model_input=ready,
            )]),
        ]
        script = Script()
        original = provider(script)
        async for _ in original.stream(ai.Model(id="fixture", provider=original), history):
            pass
        assert script.images == [[PNG]]
        state = json.loads(json.dumps(original.checkpoint()))
        assert state["media"]["run_id"] == scope.run_id
        assert base64.b64encode(PNG).decode() not in json.dumps(state)

        async def accepted_count():
            async with store.lock, store.engine.connect() as conn:
                rows = (await conn.execute(select(run_events.c.data).where(
                    run_events.c.run_id == scope.run_id, run_events.c.kind == "media_selection",
                ))).scalars().all()
            return sum(row.get("status") == "accepted" for row in rows)

        assert await accepted_count() == 1
        restored_script = Script()
        restored = provider(restored_script)
        restored.restore(state)
        async for _ in restored.stream(ai.Model(id="fixture", provider=restored), history):
            pass
        assert restored_script.images == [[PNG]]
        # Reconstructing one accepted request is idempotent, not a new receipt.
        assert await accepted_count() == 1
        failing_script = Script(fail=True)
        failing = provider(failing_script)
        with pytest.raises(ServiceError, match="provider_network"):
            async for _ in failing.stream(ai.Model(id="fixture", provider=failing), history):
                pass
        assert failing_script.images == [[PNG]]
        assert await accepted_count() == 1
