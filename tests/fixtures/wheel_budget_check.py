"""独立 site-packages 环境验证预算迁移、预留/结算、重开与预算拒绝。"""

import asyncio
import json
import socket
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import krim_agent
from krim_agent.budgets import UsageBudgetConfig
from krim_agent.config import Settings
from krim_agent.providers import ModelText, ModelUsage
from krim_agent.sdk import PythonAI, UsageBudgetPart
from krim_agent.tools import ToolRegistry
from krim_agent.usage import TokenUsage

assert "site-packages" in Path(krim_agent.__file__).resolve().parts
assert Path(krim_agent.__file__).resolve().is_relative_to(Path(sys.prefix))
assert all("/workspace/Krim/.venv/" not in value for value in sys.path)


def forbidden(*_args, **_kwargs):
    raise AssertionError("预算独立验证禁止联网")


socket.socket.connect = forbidden


class Provider:
    name = "wheel-budget"

    def __init__(self):
        self.calls = 0

    async def stream(self, _messages, _tools):
        self.calls += 1
        yield ModelText(text="独立预算回答")
        yield ModelUsage(usage=TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7))


async def main():
    with TemporaryDirectory(prefix="krim-budget-wheel-") as directory:
        settings = Settings(
            database_url=f"sqlite+aiosqlite:///{Path(directory) / 'budget.sqlite'}",
            context_tokenizer="cl100k_base",
            context_reserved_output_tokens=16,
            usage_budget=UsageBudgetConfig(max_total_tokens=100),
        )
        producer = Provider()
        async with await PythonAI.create(
            settings=settings, provider=producer, tools=ToolRegistry()
        ) as ai:
            stream = await ai.stream_text("one", idempotency_key="one")
            result = await stream.collect()
            parts = [part async for part in stream.full_stream()]
            assert result.run.status == "completed"
            assert [part.request.state for part in parts if isinstance(part, UsageBudgetPart)] == [
                "reserved",
                "settled",
            ]
            account_id = (await ai.runtime.budget_store.accounts())[0].account_id
            assert (await ai.runtime.budget_store.account(account_id)).spent_tokens == 7
        reader = Provider()
        async with await PythonAI.create(
            settings=settings, provider=reader, tools=ToolRegistry()
        ) as reopened:
            replay = await reopened.stream_text(
                "one", topic_id=result.run.topic_id, idempotency_key="one"
            )
            assert (await replay.collect()).run.id == result.run.id and reader.calls == 0
            rejected = await reopened.generate_text(
                "this longer prompt does not fit remaining budget"
            )
            assert rejected.code == "budget_tokens_exceeded" and reader.calls == 0
            assert (await reopened.runtime.budget_store.account(account_id)).spent_tokens == 7
        print(
            json.dumps(
                {
                    "package": krim_agent.__file__,
                    "spent_tokens": 7,
                    "provider_calls": producer.calls,
                    "replay_calls": reader.calls,
                },
                ensure_ascii=False,
            )
        )


asyncio.run(main())
