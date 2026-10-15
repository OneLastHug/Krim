"""真实工具/最终消息与帧SQL后强杀，冷读取原消息/帧与效果零重放。"""

import asyncio
import hashlib
import json
import os
import sys
from pathlib import Path

import httpx

import krim_agent
from krim_agent.api import create_app
from krim_agent.app import LocalApp
from krim_agent.config import Settings
from krim_agent.protocol.domain import ChatRequest, RunView
from krim_agent.protocol.models import StreamFrame
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.sdk import PythonAI
from krim_agent.tools import ToolContext, tool


async def main():
    phase, point, directory = sys.argv[1:4]
    root = Path(directory)
    replay = []

    class Provider:
        name = "offline-paired-process"

        def __init__(self):
            self.calls = 0

        async def stream(self, _messages, _tools):
            self.calls += 1
            if phase != "produce":
                replay.append("model")
                raise AssertionError("冷查询不得重新调用模型")
            if self.calls == 1:
                yield ModelToolCall(id="original-effect-call", name="effect", arguments={})
            else:
                assert self.calls == 2
                yield ModelText(text="原最终回复")

    provider = Provider()
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=provider,
    ) as ai:
        runtime = ai.runtime

        async def allow(*_args):
            return "allow"

        runtime.tool_policy = allow

        @tool(name="effect", risk="write")
        async def effect(context: ToolContext) -> dict:
            if phase != "produce":
                replay.append("tool")
                raise AssertionError("冷查询不得重放原工具")
            count = root / "effect-count.txt"
            old = int(count.read_text()) if count.exists() else 0
            count.write_text(str(old + 1))
            (root / "result.bin").write_bytes(b"\x00\xff")
            for path in (count, root / "result.bin"):
                with path.open("rb") as file:
                    os.fsync(file.fileno())
            return {"effect": old + 1, "sha256": hashlib.sha256(b"\x00\xff").hexdigest()}

        runtime.tools.register(effect)
        if phase == "produce":
            original = runtime.repository.append_message_event

            async def pause(run, role, content, details, frame):
                result = await original(run, role, content, details, frame)
                if role == point:
                    history = await runtime.repository.run_messages(run.id)
                    metadata = {
                        "pid": os.getpid(),
                        "point": point,
                        "run": run.model_dump(mode="json"),
                        "frame": result.model_dump(mode="json"),
                        "message": history[-1].model_dump(mode="json"),
                        "role": role,
                        "content": content,
                        "details": details,
                        "inode": (root / "result.bin").stat().st_ino,
                        "calls": provider.calls,
                        "package": str(krim_agent.__file__),
                    }
                    (root / "metadata.json").write_text(json.dumps(metadata))
                    print(json.dumps(metadata), flush=True)
                    await asyncio.to_thread(sys.stdin.readline)
                    raise AssertionError("应在成对COMMIT后被真正强杀")
                return result

            runtime.repository.append_message_event = pause
            topic = await runtime.repository.create_topic("真实成对提交强杀")
            await runtime.start(topic.id, ChatRequest(text="original", idempotency_key="one"))
            await asyncio.Future()
        metadata = json.loads((root / "metadata.json").read_text())
        original_run = RunView.model_validate_json(json.dumps(metadata["run"]))
        original_frame = StreamFrame.model_validate_json(json.dumps(metadata["frame"]))
        await runtime.repository.recover_interrupted()
        stored = await runtime.repository.read_message_event(
            original_run, metadata["role"], metadata["content"], metadata["details"], original_frame
        )
        assert stored == original_frame
        app = LocalApp(runtime)
        result = [frame async for frame in app.handle.events(original_run.id)]
        assert result[original_frame.seq] == original_frame
        assert result[-1].data["status"] == "interrupted"
        assert [frame.seq for frame in result] == list(range(len(result)))
        messages = await runtime.repository.run_messages(original_run.id)
        assert messages[-1].model_dump(mode="json") == metadata["message"]
        assert len(messages) == (3 if point == "tool" else 4)
        assert sum(message.role == "tool" for message in messages) == 1
        assert (root / "effect-count.txt").read_text() == "1"
        assert (root / "result.bin").read_bytes() == b"\x00\xff"
        assert (root / "result.bin").stat().st_ino == metadata["inode"]
        api = create_app(runtime=runtime)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://127.0.0.1"
        ) as client:
            response = await client.get(f"/v1/runs/{original_run.id}/events")
            assert response.status_code == 200, response.text
            events = [
                json.loads(line[6:])
                for line in response.text.splitlines()
                if line.startswith("data: ")
            ]
            assert events[original_frame.seq] == original_frame.model_dump(mode="json")
            assert [frame["seq"] for frame in events] == list(range(len(events)))
        assert not replay and provider.calls == 0
        print(
            json.dumps(
                {
                    "run_id": original_run.id,
                    "frame": stored.model_dump(mode="json"),
                    "message_id": messages[-1].id,
                    "replay_calls": len(replay),
                    "provider_calls": provider.calls,
                    "inode": metadata["inode"],
                    "same_sdk_http": True,
                    "package": str(krim_agent.__file__),
                }
            ),
            flush=True,
        )


asyncio.run(main())
