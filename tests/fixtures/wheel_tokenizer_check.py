"""在独立 site-packages 环境中禁止联网，验证编码数据与持久 SDK 预算。"""

import asyncio
import json
import socket
import sys
import tempfile
from pathlib import Path

import requests
import tiktoken
import tiktoken.load

import krim_agent
from krim_agent.config import Settings
from krim_agent.context_engine import ContextRunConfig
from krim_agent.providers import ModelText
from krim_agent.sdk import ContextBudgetPart, PythonAI, StreamStart
from krim_agent.tokenizer import TokenizerSpec
from krim_agent.tools import ToolRegistry


def forbidden(*_args, **_kwargs):
    raise AssertionError("独立 tokenizer 验证禁止联网")


socket.socket.connect = forbidden
requests.get = forbidden
tiktoken.load.read_file = forbidden
assert "site-packages" in Path(krim_agent.__file__).resolve().parts
for package in (krim_agent, tiktoken, requests):
    assert Path(package.__file__).resolve().is_relative_to(Path(sys.prefix))
assert all("/workspace/Krim/.venv/" not in entry for entry in sys.path)


class Provider:
    name = "offline-wheel-budget"

    def __init__(self):
        self.calls = 0

    async def stream(self, _messages, _tools):
        self.calls += 1
        yield ModelText(text="本机回答")


async def main():
    assert TokenizerSpec("cl100k_base").text_tokens("你好世界😀") == 7
    assert TokenizerSpec("o200k_base").text_tokens("你好世界😀") == 3
    assert TokenizerSpec("cl100k_base").text_tokens("<|endoftext|>") == 7
    parent = ContextRunConfig(tokenizer="cl100k_base", max_tokens=128)
    inherited = ContextRunConfig().restricted_by(parent)
    assert inherited.tokenizer == "cl100k_base" and inherited.max_tokens == 128
    with tempfile.TemporaryDirectory(prefix="krim-wheel-tokenizer-") as directory:
        url = f"sqlite+aiosqlite:///{Path(directory) / 'budget.sqlite'}"
        settings = Settings(
            database_url=url,
            context_tokenizer="cl100k_base",
            context_reserved_output_tokens=16,
            model_context_window_tokens=128,
            model_framing_reserve_tokens=8,
        )
        provider = Provider()
        async with await PythonAI.create(
            settings=settings, provider=provider, tools=ToolRegistry()
        ) as ai:
            stream = await ai.stream_text("你好世界😀", idempotency_key="one")
            result = await stream.collect()
            parts = [part async for part in stream.full_stream()]
            assert result.run.status == "completed" and result.token_usage is None
            assert (
                isinstance(parts[0], StreamStart)
                and parts[0].context_config["tokenizer"] == "cl100k_base"
            )
            budget = next(part for part in parts if isinstance(part, ContextBudgetPart))
            assert budget.within_limit and budget.reserved_output_tokens == 16
            failed = await ai.generate_text("😀" * 200)
            assert failed.code == "model_input_budget" and provider.calls == 1
        reader = Provider()
        settings.context_tokenizer = "o200k_base"
        async with await PythonAI.create(
            settings=settings, provider=reader, tools=ToolRegistry()
        ) as reopened:
            replay = await reopened.stream_text(
                "你好世界😀", topic_id=result.run.topic_id, idempotency_key="one"
            )
            assert [part async for part in replay.full_stream()] == parts
            assert reader.calls == 0
        print(
            json.dumps(
                {
                    "package": krim_agent.__file__,
                    "cl100k": 7,
                    "o200k": 3,
                    "provider_calls": provider.calls,
                    "replay_calls": reader.calls,
                    "window_tokens": budget.window_tokens,
                },
                ensure_ascii=False,
            )
        )


asyncio.run(main())
