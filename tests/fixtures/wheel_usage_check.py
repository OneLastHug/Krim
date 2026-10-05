"""独立 wheel 的供应商报告/持久重放验证；禁止模型联网与开发环境导入。"""

import asyncio
import json
import socket
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import krim_agent
from krim_agent.config import Settings
from krim_agent.providers import ModelText, ModelUsage
from krim_agent.sdk import PythonAI, UsagePart
from krim_agent.tools import ToolRegistry
from krim_agent.usage import TokenUsage

assert "site-packages" in Path(krim_agent.__file__).resolve().parts
assert Path(krim_agent.__file__).resolve().is_relative_to(Path(sys.prefix))
assert all("/workspace/Krim/.venv/" not in value for value in sys.path)


def forbidden(*_args, **_kwargs):
    raise AssertionError("独立用量验证禁止联网")


socket.socket.connect = forbidden


class Provider:
    name = "wheel-usage"

    def __init__(self):
        self.calls = 0

    async def stream(self, _messages, _tools):
        self.calls += 1
        yield ModelText(text="独立回答")
        yield ModelUsage(usage=TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7))


async def main():
    with TemporaryDirectory(prefix="krim-usage-wheel-") as directory:
        settings = Settings(database_url=f"sqlite+aiosqlite:///{Path(directory) / 'usage.sqlite'}")
        producer = Provider()
        async with await PythonAI.create(
            settings=settings, provider=producer, tools=ToolRegistry()
        ) as ai:
            stream = await ai.stream_text("独立用量", idempotency_key="one")
            result = await stream.collect()
            before = [part async for part in stream.full_stream()]
            assert result.usage_complete and result.token_usage.total_tokens == 7
            assert result.steps[0].usage_complete and result.steps[0].token_usage.input_tokens == 5
            assert sum(isinstance(part, UsagePart) for part in before) == 1
        reader = Provider()
        async with await PythonAI.create(
            settings=settings, provider=reader, tools=ToolRegistry()
        ) as reopened:
            replay = await reopened.stream_text(
                "独立用量", topic_id=result.run.topic_id, idempotency_key="one"
            )
            restored = await replay.collect()
            assert restored.token_usage == result.token_usage and reader.calls == 0
            assert [part async for part in replay.full_stream()] == before
        print(
            json.dumps(
                {
                    "package": krim_agent.__file__,
                    "total_tokens": 7,
                    "provider_calls": producer.calls,
                    "replay_calls": reader.calls,
                },
                ensure_ascii=False,
            )
        )


asyncio.run(main())
