from __future__ import annotations

import json
from decimal import Decimal
from urllib.parse import urlsplit

import httpx
import pytest
from pydantic import SecretStr

from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.evaluation.types import EvalRequest, FreezeRequest, rule
from krim_agent.providers.budget_gateway import BudgetLimits, GatewayConfig, PricePolicy
from krim_agent.providers.gateway_runtime import LocalBudgetGateway
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig, OpenAICompatibleProvider
from krim_agent.sdk.text import RunSDK

_MODEL = "glm-5.3-flash"
_TRIAL_TOKEN = "T" * 43
_UPSTREAM_API_KEY = "synthetic-upstream-api-key-never-real"


def _gateway_config(tmp_path) -> GatewayConfig:
    return GatewayConfig(
        upstream_origin="https://api.site",
        model=_MODEL,
        api_key=_UPSTREAM_API_KEY,
        campaign_id="synthetic-local-gateway-test",
        ledger_path=tmp_path / "budget-ledger.sqlite",
        trial_tokens={"synthetic-trial": _TRIAL_TOKEN},
        price_policy=PricePolicy(
            model=_MODEL,
            currency="CNY",
            unit="CNY_PER_MILLION_TOKENS",
            input_cny_per_million=Decimal("1.25"),
            output_cny_per_million=Decimal("4.5"),
            source="synthetic test fixture only",
            price_verified=True,
            input_bound_verified=True,
            input_fixed_overhead_tokens=64,
            input_per_message_overhead_tokens=32,
            input_per_tool_overhead_tokens=16,
        ),
        budget=BudgetLimits(
            max_requests=120,
            max_requests_per_trial=5,
            max_input_tokens=1_500_000,
            max_output_tokens=200_000,
            max_output_tokens_per_request=2048,
            max_cost_cny=Decimal("20"),
            currency="CNY",
        ),
    )


def _frame(value: dict | str) -> bytes:
    payload = value if isinstance(value, str) else json.dumps(value, separators=(",", ":"))
    return f"data: {payload}\n\n".encode()


class _ChunkedSSE(httpx.AsyncByteStream):
    def __init__(self, *events: dict | str) -> None:
        self.chunks = [_frame(event) for event in events]
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


def _sse_response(*events: dict | str) -> httpx.Response:
    return httpx.Response(
        200,
        headers={"Content-Type": "text/event-stream", "Content-Encoding": "identity"},
        stream=_ChunkedSSE(*events),
    )


def _tool_call_stream() -> httpx.Response:
    return _sse_response(
        {
            "id": "chatcmpl-synthetic-tool",
            "object": "chat.completion.chunk",
            "model": _MODEL,
            "choices": [
                {
                    "index": 0,
                    "delta": {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": "call-add-125-75",
                                "type": "function",
                                "function": {
                                    "name": "add",
                                    "arguments": '{"left":125,"right":75}',
                                },
                            }
                        ],
                    },
                    "finish_reason": None,
                }
            ],
        },
        {
            "id": "chatcmpl-synthetic-tool",
            "object": "chat.completion.chunk",
            "model": _MODEL,
            "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}],
        },
        {
            "id": "chatcmpl-synthetic-tool",
            "object": "chat.completion.chunk",
            "model": _MODEL,
            "choices": [],
            "usage": {"prompt_tokens": 11, "completion_tokens": 3, "total_tokens": 14},
        },
        "[DONE]",
    )


def _final_stream() -> httpx.Response:
    return _sse_response(
        {
            "id": "chatcmpl-synthetic-final",
            "object": "chat.completion.chunk",
            "model": _MODEL,
            "choices": [
                {
                    "index": 0,
                    "delta": {"role": "assistant", "content": "200"},
                    "finish_reason": None,
                }
            ],
        },
        {
            "id": "chatcmpl-synthetic-final",
            "object": "chat.completion.chunk",
            "model": _MODEL,
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
        },
        {
            "id": "chatcmpl-synthetic-final",
            "object": "chat.completion.chunk",
            "model": _MODEL,
            "choices": [],
            "usage": {"prompt_tokens": 17, "completion_tokens": 2, "total_tokens": 19},
        },
        "[DONE]",
    )


@pytest.mark.asyncio
async def test_local_gateway_binds_loopback_and_joins_closed_resources(tmp_path):
    async def unused_upstream(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("the lifecycle-only fixture must not forward requests")

    gateway = LocalBudgetGateway(
        _gateway_config(tmp_path), transport=httpx.MockTransport(unused_upstream)
    )
    async with gateway as entered:
        assert entered is gateway
        url = urlsplit(gateway.base_url)
        assert url.scheme == "http"
        assert url.hostname == "127.0.0.1"
        assert url.port is not None
        assert url.path in ("", "/")
        assert not url.query and not url.fragment
        assert "/v1" not in gateway.base_url
        assert gateway.resources == {
            "closed": False,
            "server_joined": False,
            "socket_closed": False,
        }

    assert gateway.resources == {
        "closed": True,
        "server_joined": True,
        "socket_closed": True,
    }


@pytest.mark.asyncio
async def test_real_provider_gateway_run_facts_and_rule_scores_use_mock_sse(tmp_path):
    forwarded: list[httpx.Request] = []

    async def upstream(request: httpx.Request) -> httpx.Response:
        forwarded.append(request)
        assert request.url == "https://api.site/v1/chat/completions"
        assert request.headers["authorization"] == f"Bearer {_UPSTREAM_API_KEY}"
        body = json.loads(request.content)
        assert body["stream"] is True
        assert any(tool["function"]["name"] == "add" for tool in body["tools"])
        messages = body["messages"]
        return _final_stream() if messages[-1]["role"] == "tool" else _tool_call_stream()

    async with LocalBudgetGateway(
        _gateway_config(tmp_path), transport=httpx.MockTransport(upstream)
    ) as gateway:
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(
                base_url=gateway.base_url,
                model=_MODEL,
                api_key=SecretStr(_TRIAL_TOKEN),
                max_output_tokens=128,
                max_tool_calls=1,
            )
        )
        application = None
        try:
            application = await open_application(
                tmp_path / "runs.sqlite",
                settings=Settings(
                    provider="http",
                    http_mode="agent",
                    max_steps=2,
                    max_tool_calls=1,
                    max_tools_per_step=1,
                    run_timeout=30,
                    system_prompt=(
                        "你是 Krim Agent。必须调用 add 工具计算；工具完成后只输出结果数字。"
                    ),
                ),
                provider=provider,
            )
            sdk = RunSDK(application.service)
            instruction = "请调用 add 工具计算 125 与 75 的和；工具返回后只输出数字。"
            run = await sdk.start(instruction, "gateway-run", target="target-http")
            finished = await sdk.wait(run.id)
            facts = await sdk.result(run.id)

            assert finished.status == "completed"
            assert facts.run.id == run.id
            assert facts.run.status == "completed"
            assert facts.text == "200"
            assert facts.model_steps == 2
            assert facts.usage_complete is True
            assert facts.token_usage == {
                "input_tokens": 28,
                "output_tokens": 5,
                "total_tokens": 33,
            }
            assert len(facts.tools) == 1
            assert facts.tools[0].name == "add"
            assert facts.tools[0].executor_started is True
            assert facts.tools[0].result_available is True

            evaluation_service = application.evaluations
            assert evaluation_service is not None
            candidate = await evaluation_service.candidate(run.id, include_input=True)
            assert candidate.eligibility == "eligible"
            dataset = await evaluation_service.freeze(
                FreezeRequest.model_validate(
                    {
                        "name": "gateway-runtime",
                        "version": "1",
                        "cases": [
                            {
                                "case_id": "add-125-75",
                                "source_kind": "run",
                                "source_run_id": run.id,
                                "candidate_fingerprint": candidate.candidate_fingerprint,
                                "selected_input": instruction,
                                "reason": "synthetic add result from RunService",
                                "reference": {
                                    "expected": "200",
                                    "expected_status": "completed",
                                    "expected_code": None,
                                    "expected_tools": None,
                                    "trajectory": None,
                                    "expected_schema": None,
                                },
                            }
                        ],
                    }
                )
            )
            evaluation = await evaluation_service.start(
                EvalRequest.model_validate(
                    {
                        "request_key": "gateway-rule-score",
                        "dataset": {
                            "name": dataset.name,
                            "version": dataset.version,
                            "fingerprint": dataset.fingerprint,
                        },
                        "target_key": "target-http",
                        "rules": [
                            rule("status_code").model_dump(mode="json"),
                            rule("exact_match").model_dump(mode="json"),
                        ],
                        "execution": {
                            "repetitions": 1,
                            "concurrency": 1,
                            "item_timeout_seconds": 12,
                            "scorer_timeout_seconds": 1,
                            "batch_timeout_seconds": 600,
                        },
                    }
                )
            )
            await evaluation_service.wait(evaluation.id)
            report = await evaluation_service.report(evaluation.id)
            assert report["complete"] is True, json.dumps(report, ensure_ascii=False)
            assert report["items"][0]["quality"] == "passed"
            scores = {
                item["rule"]["name"]: item["value"] for item in report["items"][0]["scores"]
            }
            assert scores == {"status_code": 1, "exact_match": 1}
            assert len(forwarded) == 4
        finally:
            if application is not None:
                await application.close()
            await provider.close()

    assert gateway.resources["closed"] is True
    assert gateway.resources["server_joined"] is True
    assert gateway.resources["socket_closed"] is True


@pytest.mark.asyncio
async def test_invalid_trial_token_is_rejected_before_upstream_forward(tmp_path):
    forwarded: list[httpx.Request] = []

    async def upstream(request: httpx.Request) -> httpx.Response:
        forwarded.append(request)
        return _final_stream()

    async with LocalBudgetGateway(
        _gateway_config(tmp_path), transport=httpx.MockTransport(upstream)
    ) as gateway:
        async with httpx.AsyncClient(trust_env=False) as client:
            response = await client.post(
                gateway.base_url + "/chat/completions",
                headers={"Authorization": "Bearer invalid-synthetic-token"},
                json={
                    "model": _MODEL,
                    "messages": [{"role": "user", "content": "synthetic unauthorized"}],
                    "max_tokens": 32,
                    "stream": True,
                },
            )
        assert response.status_code == 401
        assert forwarded == []
