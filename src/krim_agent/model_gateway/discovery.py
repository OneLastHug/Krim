"""Read an OpenAI-compatible model directory without mutating saved settings."""

from __future__ import annotations

import json
from typing import Any

import httpx
from pydantic import ValidationError

from krim_agent.contracts import ServiceError
from krim_agent.model_gateway.contracts import GatewayModel
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig

_MAX_BYTES = 4 * 1024 * 1024
_MAX_MODELS = 2000


async def discover_models(
    config: OpenAICompatibleConfig,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> list[dict[str, Any]]:
    headers = {"Accept": "application/json", "Accept-Encoding": "identity"}
    key = config.api_key.get_secret_value()
    if key:
        headers["Authorization"] = "Bearer " + key
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(20, connect=10),
            follow_redirects=False,
            trust_env=False,
            transport=transport,
        ) as client:
            async with client.stream(
                "GET",
                config.base_url + "/models",
                headers=headers,
            ) as response:
                if not response.is_success:
                    raise ServiceError("provider_http_error", 502)
                raw = bytearray()
                async for chunk in response.aiter_bytes():
                    raw.extend(chunk)
                    if len(raw) > _MAX_BYTES:
                        raise ServiceError("provider_response_limit", 502)
        payload = json.loads(raw)
    except httpx.TimeoutException:
        raise ServiceError("provider_timeout", 504) from None
    except httpx.HTTPError:
        raise ServiceError("provider_network", 502) from None
    except (ValueError, UnicodeError):
        raise ServiceError("provider_protocol", 502) from None
    values = payload.get("data") if isinstance(payload, dict) else None
    if values is None and isinstance(payload, dict) and isinstance(payload.get("models"), dict):
        values = [
            dict(value, id=model_id)
            for model_id, value in payload["models"].items()
            if isinstance(value, dict)
        ]
    if not isinstance(values, list) or len(values) > _MAX_MODELS:
        raise ServiceError("provider_protocol", 502)
    models: list[dict[str, Any]] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, dict):
            continue
        model_id = value.get("id")
        display_name = value.get("name") or value.get("display_name") or model_id
        try:
            model = GatewayModel.model_validate({"id": model_id, "display_name": display_name})
        except ValidationError:
            continue
        if model.id not in seen:
            seen.add(model.id)
            models.append(model.model_dump(mode="json"))
    return models
