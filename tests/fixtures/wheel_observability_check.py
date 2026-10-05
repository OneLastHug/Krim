"""独立 wheel 中离线 Trace/JSONL/工具和原始回放验证。"""

import asyncio
import json
import socket
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import krim_agent
from krim_agent.config import Settings
from krim_agent.providers import ModelText
from krim_agent.sdk import PythonAI, StreamStart
from krim_agent.tools import ToolRegistry

assert "site-packages" in Path(krim_agent.__file__).resolve().parts
assert Path(krim_agent.__file__).resolve().is_relative_to(Path(sys.prefix))


def forbidden(*_args, **_kwargs):
    raise AssertionError("观测分发测试禁止联网")


socket.socket.connect = forbidden


class Provider:
    name = "wheel-observation"
    calls = 0

    async def stream(self, _messages, _tools):
        self.calls += 1
        yield ModelText(text="private-wheel-canary")


async def main():
    with TemporaryDirectory(prefix="krim-observation-wheel-") as directory:
        db = f"sqlite+aiosqlite:///{Path(directory) / 'trace.sqlite'}"
        path = Path(directory) / "trace.jsonl"
        provider = Provider()
        settings = Settings(
            database_url=db, observability_enabled=True, observability_jsonl_path=str(path)
        )
        async with await PythonAI.create(
            settings=settings, provider=provider, tools=ToolRegistry()
        ) as ai:
            stream = await ai.stream_text("private-wheel-canary", idempotency_key="same")
            result = await stream.collect()
            parts = [part async for part in stream.full_stream()]
            start = next(part for part in parts if isinstance(part, StreamStart))
            assert start.trace_context is not None
            assert await ai.runtime.tracer.flush()
            trace_id = start.trace_context.trace_id
            view = await ai.runtime.tracer.store.trace(trace_id)
            assert len(view.spans) == 2 and view.complete
        assert "private-wheel-canary" not in path.read_text()
        before = path.read_bytes()
        async with await PythonAI.create(
            settings=settings, provider=provider, tools=ToolRegistry()
        ) as reopened:
            replay = await reopened.stream_text(
                "private-wheel-canary", topic_id=result.run.topic_id, idempotency_key="same"
            )
            assert (await replay.collect()).run.id == result.run.id
            assert len((await reopened.runtime.tracer.store.trace(trace_id)).spans) == 2
        assert path.read_bytes() == before and provider.calls == 1
        print(
            json.dumps(
                {
                    "package": krim_agent.__file__,
                    "spans": 2,
                    "provider_calls": 1,
                    "replay_calls": 0,
                    "private": True,
                },
                ensure_ascii=False,
            )
        )


asyncio.run(main())
