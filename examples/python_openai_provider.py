"""离线 HTTPX 模拟兼容端点；演示 Python Provider→工具→模型闭环，不调用商业服务。"""

import asyncio
import json

import httpx

from krim_agent.providers import OpenAICompatibleConfig, OpenAICompatibleProvider
from krim_agent.sdk import PythonAI


async def main() -> None:
    def endpoint(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        if payload["messages"][-1]["role"] == "tool":
            delta = {"content": "5"}
            reason = "stop"
        else:
            delta = {
                "tool_calls": [
                    {
                        "index": 0,
                        "id": "demo-call",
                        "type": "function",
                        "function": {"name": "add", "arguments": '{"left":2,"right":3}'},
                    }
                ]
            }
            reason = "tool_calls"
        frames = [
            {"choices": [{"index": 0, "delta": delta, "finish_reason": None}]},
            {"choices": [{"index": 0, "delta": {}, "finish_reason": reason}]},
        ]
        body = "".join(f"data: {json.dumps(frame)}\n\n" for frame in frames) + "data: [DONE]\n\n"
        return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content=body)

    provider = OpenAICompatibleProvider(
        OpenAICompatibleConfig(base_url="http://127.0.0.1:9000/v1", model="offline-demo"),
        transport=httpx.MockTransport(endpoint),
    )
    try:
        # 注入的 provider 由调用方拥有，SDK 关闭后显式回收 HTTP client。
        async with await PythonAI.create(provider=provider) as ai:
            result = await ai.generate_text("计算 2 + 3")
            print(f"状态：{result.run.status}；结果：{result.text}；步骤：{len(result.steps)}")
            print(f"工具审计：{len(result.audits)}")
    finally:
        await provider.close()


if __name__ == "__main__":
    asyncio.run(main())
