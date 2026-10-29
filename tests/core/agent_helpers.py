"""各测试模块显式导入的共享合成对象，避免 conftest 同名冲突。"""

import asyncio
import hashlib

from krim_agent.tools.builtins import EchoInput
from krim_agent.tools.core import ToolSpec
from krim_agent.tools.registry import ToolRegistry


class EventProvider:
    def __init__(self, events):
        self.events, self.calls, self.active, self.streams_closed = events, 0, 0, 0

    async def stream(self, messages, tools):
        self.calls += 1
        self.active += 1
        try:
            for value in self.events:
                await asyncio.sleep(0)
                yield value
        finally:
            self.active -= 1
            self.streams_closed += 1

    async def close(self):
        assert self.active == 0


def registry_for(executor, output=None):
    return ToolRegistry((ToolSpec("echo", EchoInput, executor,
        {"type": "string"} if output is None else output,
        "test-1", hashlib.sha256(b"trusted-test-fixture").hexdigest()),))


async def pending(service, run_id):
    async with asyncio.timeout(2):
        while True:
            values = await service.approvals(run_id)
            if values:
                return values[0]
            await asyncio.sleep(0.005)
