"""Opt-in semantic scoring stays on a real session-backed product case."""

import json
from typing import Any

import httpx

from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.evaluation.domain import CaseSpec, MetricSpec
from krim_agent.evaluation.product_types import ProductDatasetSpec
from krim_agent.evaluation.semantic_judge import register_semantic_scorer
from krim_agent.providers.openai_compatible import (
    OpenAICompatibleConfig,
    OpenAICompatibleProvider,
)
from krim_agent.providers.types import ModelText
from krim_agent.sdk import WorkspaceEvaluationSDK


class LocalJudge:
    def __init__(self):
        self.calls = 0
        self.streams_closed = 0

    async def stream(self, messages, tools):
        assert tools == []
        assert "reference" in messages[-1].content
        self.calls += 1
        try:
            yield ModelText(text='{"score_milli":900,"rationale":"人工参考与结果一致"}')
        finally:
            self.streams_closed += 1

    async def close(self):
        pass


class SSE(httpx.AsyncByteStream):
    async def __aiter__(self):
        events: list[dict[str, Any] | str] = [
            {"choices": [{"index": 0, "delta": {"content": "6"}, "finish_reason": None}]},
            {"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
            "[DONE]",
        ]
        for event in events:
            raw = event if isinstance(event, str) else json.dumps(event, separators=(",", ":"))
            yield f"data: {raw}\n\n".encode()

    async def aclose(self) -> None:
        return None


def configured_mock_provider() -> OpenAICompatibleProvider:
    async def respond(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=SSE(),
        )

    provider = OpenAICompatibleProvider(
        OpenAICompatibleConfig(base_url="https://model.example/v1", model="fixture")
    )
    provider._client._transport = httpx.MockTransport(respond)
    provider._sealed = provider.binding_token()
    return provider


async def test_semantic_scorer_is_opt_in_catalogued_and_runs_in_product_kernel(tmp_path):
    judge = LocalJudge()
    async with await open_application(
        tmp_path / "semantic.sqlite",
        settings=Settings(product_eval_enabled=True),
        provider=configured_mock_provider(),
    ) as app:
        assert app.product_evaluations is not None and app.evaluation_product is not None
        sdk = WorkspaceEvaluationSDK(app.evaluation_product)
        assert "semantic_judge" not in {
            item["name"] for item in await sdk.metrics("krim-session", mode="manual")
        }
        registered = register_semantic_scorer(
            app.product_evaluations.repository.metric_registry,
            provider=judge,
            provider_id="local",
            model_id="synthetic",
            rubric="核对实际答案与人工参考。",
        )
        metrics = await sdk.metrics("krim-session", mode="manual")
        assert registered.model_dump(mode="json") in metrics
        assert judge.calls == 0
        frozen = await sdk.freeze_dataset(
            ProductDatasetSpec(
                name="semantic-catalog",
                version="v1",
                cases=[
                    CaseSpec(
                        case_id="add",
                        instruction="Answer with the result of 2.5 plus 3.5.",
                        files={},
                        reference={"expected": "6"},
                    )
                ],
            )
        )
        task = await sdk.start_task(
            {
                "request_key": "semantic-product",
                "target_key": "krim-session",
                "dataset": {key: frozen[key] for key in ("name", "version", "fingerprint")},
                "metrics": [
                    MetricSpec.model_validate(row).model_dump(mode="json") for row in metrics
                ],
                "execution": {"scorer_timeout_seconds": 2},
            }
        )
        completed = await app.product_evaluations.kernel.wait(task["id"])
        assert completed["status"] == "completed"
        report = await sdk.report_page(task["id"])
        assert report["rows"][0]["quality"] == "passed", report["rows"][0]
        assert completed["items"][0]["output"]["target_result"]["text"] == "6"
        assert judge.calls == judge.streams_closed == 1
