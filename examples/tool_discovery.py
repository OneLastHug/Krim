"""按需工具发现：每次生成独立目录状态，命中后从下一模型步骤生效。"""

import asyncio

from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.sdk import PythonAI, ToolDiscoveryPart
from krim_agent.tools import ToolRegistry, tool


@tool(name="lookup_number", description="查询编号 lookup", risk="read", should_defer=True)
async def lookup_number(value: int = 5) -> int:
    return value


class DiscoveryDemo:
    name = "fake-discovery-offline"

    async def stream(self, messages, tools):
        names = [definition.name for definition in tools]
        if messages[-1].role != "tool":
            # 第一模型步骤只看到搜索入口，目标工具 Schema 尚未发送。
            assert names == ["tool_search"]
            yield ModelToolCall(id="search", name="tool_search", arguments={"query": "编号"})
        elif messages[-1].details["name"] == "tool_search":
            # 搜索成功后，下一模型步骤才拿到目标工具的完整声明。
            assert "lookup_number" in names
            yield ModelToolCall(id="lookup", name="lookup_number", arguments={"value": 5})
        else:
            yield ModelText(text="已发现并执行工具：5")


async def main() -> None:
    registry = ToolRegistry()
    registry.register(lookup_number)
    async with await PythonAI.create(provider=DiscoveryDemo(), tools=registry) as ai:
        stream = await ai.stream_text("查询编号")
        result = await stream.collect()
        assert result.run.status == "completed" and len(result.audits) == 2
        discovered = [
            part async for part in stream.full_stream() if isinstance(part, ToolDiscoveryPart)
        ]
        assert len(discovered) == 1 and discovered[0].tools == ["lookup_number"]
        print(result.text)
        print("本次已加载工具：", discovered[0].tools)


if __name__ == "__main__":
    asyncio.run(main())
