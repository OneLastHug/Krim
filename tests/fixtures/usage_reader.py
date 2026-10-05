"""真实 Python 读进程只读取持久用量/SDK 事件，不接管或调用模型。"""

import asyncio
import json
import sys

from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.providers import ModelText
from krim_agent.runtime import ChatRuntime
from krim_agent.sdk import StreamTextResult
from krim_agent.tools import ToolRegistry


class NeverProvider:
    name = "reader-never-provider"
    calls = 0

    async def stream(self, _messages, _tools):
        self.calls += 1
        raise AssertionError("读进程禁止调用模型")
        yield ModelText(text="never")


async def main():
    database = Database(f"sqlite+aiosqlite:///{sys.argv[1]}")
    await database.initialize()
    provider = NeverProvider()
    runtime = ChatRuntime(KrimRepository(database), provider, ToolRegistry(), Settings())
    try:
        run = await runtime.repository.get_run(sys.argv[2])
        result = await StreamTextResult(runtime, run).collect()
        print(
            json.dumps(
                {
                    "usage_complete": result.usage_complete,
                    "token_usage": result.token_usage.model_dump(mode="json"),
                    "provider_calls": provider.calls,
                }
            )
        )
    finally:
        await runtime.close()
        await database.close()


asyncio.run(main())
