"""工作区根目录由每个可信进程显式重供，读取与恢复不重新执行模型。"""

import asyncio
import json
import sys

from krim_agent.config import Settings
from krim_agent.providers import FakeProvider
from krim_agent.sdk import PythonAI


class Counting(FakeProvider):
    def __init__(self):
        super().__init__(delay=0)
        self.calls = 0

    async def stream(self, messages, tools):
        self.calls += 1
        async for part in super().stream(messages, tools):
            yield part


async def main():
    provider = Counting()
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{sys.argv[1]}"),
        provider=provider,
        workspace_root=sys.argv[2],
    ) as ai:
        if sys.argv[3] == "create":
            agent = await ai.agent(persistent=True)
            await agent.execute("/echo persisted", idempotency_key="persisted")
        else:
            assert await ai.agents().restore_persistent() == 1
            agent = ai.agent_session(sys.argv[4])
            assert provider.calls == 0
        view = await agent.view()
        file = await agent.read_file("file.txt")
        print(
            json.dumps(
                {
                    "agent_id": agent.agent_id,
                    "run_id": view.current_run_id,
                    "sha256": file.sha256,
                    "calls": provider.calls,
                }
            )
        )


asyncio.run(main())
