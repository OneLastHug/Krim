"""Python 生成 SDK 示例：真实本地工具循环、类型化部件与可回放结果。"""

import asyncio

from krim_agent.sdk import PythonAI, StepFinish, TextDelta, ToolResultPart


async def main() -> None:
    # 默认使用内存 SQLite 和明确标记的离线 provider，不调用外部模型。
    async with await PythonAI.create() as ai:
        result = await ai.stream_text("/add 2 3", idempotency_key="sdk-example")
        async for part in result.full_stream():
            if isinstance(part, TextDelta):
                print(part.text, end="", flush=True)
            elif isinstance(part, ToolResultPart):
                print("工具结果：", part.tool_name, part.result.data)
            elif isinstance(part, StepFinish):
                print("步骤完成：", part.step, part.finish_reason)
        # collect 只读取该运行的持久结果；再次调用不会重跑工具。
        completed = await result.collect()
        print("\n最终文本：", completed.text)
        print("终态原因：", completed.finish_reason)
        print("步骤数：", len(completed.steps), "审计数：", len(completed.audits))
        replay = "".join([text async for text in result.text_stream()])
        print("文本回放一致：", replay == completed.all_text)


if __name__ == "__main__":
    asyncio.run(main())
