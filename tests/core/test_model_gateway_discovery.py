"""Directory discovery is explicit, bounded, and never stores upstream secrets."""

import json

import httpx
import pytest
from fixtures.http_provider_server import HTTPFixture, choice, frame
from pydantic import SecretStr, ValidationError

from krim_agent.contracts import Message, ServiceError
from krim_agent.model_gateway.discovery import discover_models
from krim_agent.providers.openai_compatible import (
    OpenAICompatibleConfig,
    OpenAICompatibleProvider,
)


async def test_discovery_projects_and_deduplicates_without_mutating_config():
    config = OpenAICompatibleConfig(
        base_url="https://gateway.example/v1", model="saved-model", api_key=SecretStr("test-key")
    )

    def handle(request):
        assert request.url.path == "/v1/models"
        assert request.headers["authorization"] == "Bearer test-key"
        return httpx.Response(
            200,
            json={
                "data": [
                    {"id": "model-a", "name": "Model A", "api_key": "never-return"},
                    {"id": "model-a"},
                    {"id": "bad id"},
                    {"id": "model-b"},
                ]
            },
        )

    values = await discover_models(config, transport=httpx.MockTransport(handle))
    assert [v["id"] for v in values] == ["model-a", "model-b"]
    assert values[0]["display_name"] == "Model A"
    assert "never-return" not in json.dumps(values)
    assert config.model == "saved-model"
    assert all(v["reasoning_levels"] == {} for v in values)


@pytest.mark.parametrize(
    "status,body,code",
    [
        (302, {}, "provider_http_error"),
        (401, {"error": "secret-body"}, "provider_http_error"),
        (200, {"bad": []}, "provider_protocol"),
    ],
)
async def test_discovery_rejects_redirects_errors_and_invalid_shape(status, body, code):
    config = OpenAICompatibleConfig(base_url="https://gateway.example/v1", model="a")
    with pytest.raises(ServiceError) as error:
        await discover_models(
            config, transport=httpx.MockTransport(lambda request: httpx.Response(status, json=body))
        )
    assert error.value.code == code
    assert "secret-body" not in str(error.value)


async def test_discovery_bounded_payload():
    config = OpenAICompatibleConfig(base_url="https://gateway.example/v1", model="a")
    with pytest.raises(ServiceError) as error:
        await discover_models(
            config,
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, content=b" " * (4 * 1024 * 1024 + 1))
            ),
        )
    assert error.value.code == "provider_response_limit"


@pytest.mark.parametrize(
    "effort",
    [
        "none",
        "minimal",
        "low",
        "medium",
        "high",
        "xhigh",
        "max",
        "custom-depth",
    ],
)
@pytest.mark.parametrize("thinking", [None, "enabled", "disabled"])
async def test_custom_reasoning_reaches_chat_wire(effort, thinking):
    async with HTTPFixture(
        chunks=[frame(choice({"content": "ok"})), frame(choice(finish="stop")), frame("[DONE]")]
    ) as fixture:
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(
                base_url=fixture.url,
                model="custom-model",
                reasoning_effort=effort,
                thinking=thinking,
            )
        )
        try:
            _ = [item async for item in provider.stream([Message(role="user", content="test")], [])]
            request = fixture.requests[0]["body"]
            assert request["model"] == "custom-model"
            assert request["reasoning_effort"] == effort
            if thinking is None:
                assert "thinking" not in request
            else:
                assert request["thinking"] == {"type": thinking}
        finally:
            await provider.close()


def test_responses_rejects_chat_only_thinking_at_configuration_time():
    with pytest.raises(ValidationError):
        OpenAICompatibleConfig(
            base_url="https://gateway.example", model="a", wire_api="responses", thinking="enabled"
        )
