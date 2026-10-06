"""结构化 SDK 离线试运行：一次真实 Runtime 生成、严格对象校验。"""

import asyncio

from pydantic import Field

from krim_agent.protocol.models import StrictModel
from krim_agent.providers import ModelText
from krim_agent.sdk import PythonAI


class Answer(StrictModel):
    number: int = Field(ge=0)


class OfflineJsonProvider:
    # 此模型只演示 JSON 协议，不代表真实模型质量。
    name = "offline-json-example"

    async def stream(self, _messages, _tools):
        yield ModelText(text='{"number":3}')


async def main():
    async with await PythonAI.create(provider=OfflineJsonProvider()) as ai:
        result = await ai.generate_object("返回数字3", schema=Answer)
        print(result.value)
        print(result.generation.run.id, result.schema_fingerprint)


asyncio.run(main())
