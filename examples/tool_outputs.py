"""工具输出声明示例：原始 Python 返回值、JSON 消费与真实失败审计。"""

import asyncio
from typing import Annotated

from pydantic import Field

from krim_agent.protocol.models import StrictModel
from krim_agent.tools import ToolContext, ToolRegistry, tool


class SquareOutput(StrictModel):
    value: int = Field(ge=0)


@tool(risk="read", output_schema=SquareOutput)
async def square(value: Annotated[int, Field(ge=0, le=100)]) -> SquareOutput:
    return SquareOutput(value=value * value)


@tool(risk="read", output_schema=SquareOutput)
async def bad_square(value: int) -> dict[str, str]:
    return {"value": str(value * value)}


async def main() -> None:
    registry = ToolRegistry()
    registry.register_many(square, bad_square)
    try:
        # 嵌套 Python 调用拿到原始模型，按需访问属性。
        native = await registry.call_tool("square", {"value": 3})
        assert isinstance(native, SquareOutput) and native.value == 9
        # 模型/Workflow 等消费者拿到通过输出合约的有限 JSON。
        result = await registry.invoke("square", {"value": 3}, ToolContext(registry))
        assert result.data == {"value": 9} and not result.is_error
        # 字符串不能被隐式转换成整数，失败结果与本次审计保持一致。
        rejected = await registry.invoke("bad_square", {"value": 3}, ToolContext(registry))
        assert rejected.code == "invalid_output_schema"
        assert not registry.audit[-1].ok and registry.audit[-1].code == rejected.code
        print("原始模型：", native.model_dump())
        print("消费结果：", result.model_dump())
        print("失败代码：", rejected.code, "审计成功：", registry.audit[-1].ok)
    finally:
        await registry.close()


if __name__ == "__main__":
    asyncio.run(main())
