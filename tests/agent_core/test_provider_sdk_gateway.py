"""SDK → 本机预算网关 → 重复工具元数据上游 → 工具执行 → 第二轮模型请求。"""

import json

import httpx
from test_budget_gateway_runtime import (
    _MODEL,
    _TRIAL_TOKEN,
    _UPSTREAM_API_KEY,
    _final_stream,
    _gateway_config,
    _sse_response,
)

from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import RunRequest
from krim_agent.providers.gateway_runtime import LocalBudgetGateway
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig, OpenAICompatibleProvider


async def test_sdk_gateway_repeated_metadata_round_trip(tmp_path):
    bodies = []
    identifier = "call_moerouter_gateway_125_75"

    async def upstream(request):
        assert request.headers["authorization"] == f"Bearer {_UPSTREAM_API_KEY}"
        body = json.loads(request.content)
        bodies.append(body)
        if body["messages"][-1]["role"] == "tool":
            assert body["messages"][-1]["tool_call_id"] == identifier
            assert "tools" not in body  # 工具预算用完后仍允许最终文本回复。
            return _final_stream()
        assert any(tool["function"]["name"] == "add" for tool in body["tools"])
        events = []
        for part in '{"left":125,"right":75}':
            events.append(
                {
                    "choices": [
                        {
                            "index": 0,
                            "delta": {
                                "tool_calls": [
                                    {
                                        "index": 0,
                                        "id": identifier,
                                        "type": "function",
                                        "function": {"name": "add", "arguments": part},
                                    }
                                ]
                            },
                            "finish_reason": None,
                        }
                    ]
                }
            )
        events.extend(
            [
                {"choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]},
                {
                    "choices": [],
                    "usage": {
                        "prompt_tokens": 11,
                        "completion_tokens": 3,
                        "total_tokens": 14,
                    },
                },
                "[DONE]",
            ]
        )
        return _sse_response(*events)

    gateway = LocalBudgetGateway(_gateway_config(tmp_path), transport=httpx.MockTransport(upstream))
    async with gateway:
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(
                base_url=gateway.base_url,
                model=_MODEL,
                api_key=_TRIAL_TOKEN,
                max_output_tokens=128,
                max_tool_calls=1,
            )
        )
        try:
            async with await open_application(
                tmp_path / "run.sqlite",
                provider=provider,
                settings=Settings(
                    provider="http",
                    http_mode="agent",
                    max_steps=2,
                    max_tool_calls=1,
                    max_tools_per_step=1,
                ),
            ) as app:
                run = await app.service.start(
                    RunRequest(
                        text="/add 125 75",
                        request_key="sdk-gateway",
                        target="target-http",
                    )
                )
                finished = await app.service.wait(run.id)
                assert finished.status == "completed", finished.code
                result = await app.service.result(run.id)
                assert result.text == "200" and result.model_steps == 2
                assert len(result.tools) == 1
                assert result.tools[0].provider_call_id == identifier
                assert result.tools[0].executor_started and result.tools[0].result_available
                assert result.usage_complete and result.token_usage["total_tokens"] == 33
                assert len(bodies) == 2
        finally:
            await provider.close()
    assert gateway.resources == {"closed": True, "server_joined": True, "socket_closed": True}
