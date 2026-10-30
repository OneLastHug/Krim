from __future__ import annotations

import asyncio
import json
import sqlite3
from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest

from krim_agent.providers.budget_gateway import (
    BudgetLimits,
    GatewayConfig,
    PricePolicy,
    create_gateway,
    generate_trial_token,
)

_TOKEN_A = "A" * 43
_TOKEN_B = "B" * 43
_API_KEY = "synthetic-test-api-key-never-log"
_MODEL = "glm-5.3-flash"


def policy() -> PricePolicy:
    return PricePolicy(
        model=_MODEL,
        currency="CNY",
        unit="CNY_PER_MILLION_TOKENS",
        input_cny_per_million=Decimal("1.25"),
        output_cny_per_million=Decimal("4.5"),
        source="synthetic test fixture only; not a real price quote",
        price_verified=True,
        input_bound_verified=True,
        input_fixed_overhead_tokens=64,
        input_per_message_overhead_tokens=32,
        input_per_tool_overhead_tokens=16,
    )


def budget(**overrides: Any) -> BudgetLimits:
    values: dict[str, Any] = {
        "max_requests": 120,
        "max_requests_per_trial": 5,
        "max_input_tokens": 1_500_000,
        "max_output_tokens": 200_000,
        "max_output_tokens_per_request": 2048,
        "max_cost_cny": Decimal("20"),
        "currency": "CNY",
    }
    values.update(overrides)
    return BudgetLimits(**values)


def config(
    path: Path,
    *,
    trial_tokens: dict[str, str] | None = None,
    limits: BudgetLimits | None = None,
    api_key: str = _API_KEY,
) -> GatewayConfig:
    return GatewayConfig(
        upstream_origin="https://api.site",
        model=_MODEL,
        api_key=api_key,
        campaign_id="synthetic-campaign-2026-10",
        ledger_path=path,
        trial_tokens=trial_tokens or {"trial-a": _TOKEN_A, "trial-b": _TOKEN_B},
        price_policy=policy(),
        budget=limits or budget(),
    )


@asynccontextmanager
async def running_gateway(cfg: GatewayConfig, transport: httpx.AsyncBaseTransport):
    app = create_gateway(cfg, transport=transport)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://gateway.test",
        ) as client:
            yield app, client


def chat_body(
    *, stream: bool = False, model: str = _MODEL, tools: list[dict[str, Any]] | None = None
):
    body: dict[str, Any] = {
        "model": model,
        "messages": [{"role": "user", "content": "synthetic hello"}],
        "max_tokens": 64,
        "stream": stream,
    }
    if tools is not None:
        body["tools"] = tools
    return body


def completion(*, usage: dict[str, int] | None, content: str = "ok") -> dict[str, Any]:
    result: dict[str, Any] = {
        "id": "chatcmpl-synthetic",
        "object": "chat.completion",
        "model": _MODEL,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop",
            }
        ],
    }
    if usage is not None:
        result["usage"] = usage
    return result


def usage(input_tokens: int, output_tokens: int) -> dict[str, int]:
    return {
        "prompt_tokens": input_tokens,
        "completion_tokens": output_tokens,
        "total_tokens": input_tokens + output_tokens,
    }


def frame(value: dict[str, Any] | str) -> bytes:
    payload = value if isinstance(value, str) else json.dumps(value, separators=(",", ":"))
    return f"data: {payload}\n\n".encode()


class ChunkedBody(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


def json_stream_response(value: dict[str, Any], *, status_code: int = 200) -> httpx.Response:
    return httpx.Response(
        status_code,
        headers={"Content-Type": "application/json"},
        stream=ChunkedBody([json.dumps(value, separators=(",", ":")).encode("utf-8")]),
    )


class WaitingBody(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.closed = False

    async def __aiter__(self):
        self.started.set()
        await asyncio.Event().wait()
        yield b"unreachable"

    async def aclose(self) -> None:
        self.closed = True


class TrackingMockTransport(httpx.MockTransport):
    def __init__(self, handler):
        super().__init__(handler)
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True
        await super().aclose()


def rows(path: Path) -> list[tuple[Any, ...]]:
    with sqlite3.connect(path) as database:
        return database.execute(
            "SELECT trial_id,state,reserved_input,reserved_output,reserved_cost_microcny,"
            "actual_input,actual_output,actual_cost_microcny FROM gateway_reservations "
            "ORDER BY created_at,request_id"
        ).fetchall()


@pytest.mark.asyncio
async def test_streaming_function_call_settles_only_complete_usage(tmp_path):
    stream_body = ChunkedBody(
        [
            frame(
                {
                    "id": "stream-id",
                    "choices": [
                        {
                            "index": 0,
                            "delta": {
                                "role": "assistant",
                                "content": _API_KEY[:12],
                                "tool_calls": [
                                    {
                                        "index": 0,
                                        "id": "call-1",
                                        "type": "function",
                                        "function": {"name": "lookup", "arguments": '{"q":'},
                                    }
                                ],
                            },
                            "finish_reason": None,
                        }
                    ],
                }
            ),
            frame(
                {
                    "choices": [
                        {
                            "index": 0,
                            "delta": {
                                "content": _API_KEY[12:],
                                "tool_calls": [
                                    {
                                        "index": 0,
                                        "function": {"arguments": '"weather"}'},
                                    }
                                ],
                            },
                            "finish_reason": None,
                        }
                    ]
                }
            ),
            frame({"choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]}),
            frame({"choices": [], "usage": usage(17, 9)}),
            frame("[DONE]"),
        ]
    )
    outbound: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        outbound.append(request)
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            stream=stream_body,
        )

    transport = TrackingMockTransport(handler)
    ledger = tmp_path / "budget.sqlite"
    async with running_gateway(config(ledger), transport) as (_app, client):
        response = await client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {_TOKEN_A}"},
            json=chat_body(
                stream=True,
                tools=[
                    {
                        "type": "function",
                        "function": {
                            "name": "lookup",
                            "description": "Look up a synthetic item",
                            "parameters": {
                                "type": "object",
                                "properties": {"q": {"type": "string"}},
                                "required": ["q"],
                                "additionalProperties": False,
                            },
                        },
                    }
                ],
            ),
        )
        stream = response.text
        assert response.status_code == 200
        assert "call-1" in stream and "lookup" in stream
        assert _API_KEY not in stream and "[redacted]" in stream
        events = [
            json.loads(item[6:])
            for item in stream.strip().split("\n\n")
            if item.startswith("data: ") and item[6:] != "[DONE]"
        ]
        arguments = "".join(
            part["function"].get("arguments", "")
            for event in events
            for choice in event.get("choices", [])
            for delta in [choice.get("delta", {})]
            for part in delta.get("tool_calls", [])
        )
        assert arguments == '{"q":"weather"}'
        assert any(item.get("usage") == usage(17, 9) for item in events)
        assert outbound[0].url == "https://api.site/v1/chat/completions"
        assert outbound[0].headers["authorization"] == f"Bearer {_API_KEY}"
        sent = json.loads(outbound[0].content)
        assert sent["model"] == _MODEL
        assert sent["stream_options"] == {"include_usage": True}
        assert stream_body.closed
    record = rows(ledger)[0]
    assert record[1] == "settled"
    assert record[5:7] == (17, 9)
    assert transport.closed


@pytest.mark.asyncio
async def test_concurrent_reservations_are_atomic_and_budget_survives_reopen(tmp_path):
    entered = asyncio.Event()
    release = asyncio.Event()
    outbound_count = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal outbound_count
        outbound_count += 1
        entered.set()
        await release.wait()
        return json_stream_response(completion(usage=usage(10, 4)))

    transport = httpx.MockTransport(handler)
    ledger = tmp_path / "concurrent.sqlite"
    cfg = config(
        ledger,
        limits=budget(max_requests=1, max_input_tokens=4096, max_output_tokens=2048),
    )
    async with running_gateway(cfg, transport) as (_app, client):
        first = asyncio.create_task(
            client.post(
                "/chat/completions",
                headers={"Authorization": f"Bearer {_TOKEN_A}"},
                json=chat_body(),
            )
        )
        await asyncio.wait_for(entered.wait(), timeout=3)
        second = await client.post(
            "/chat/completions",
            headers={"Authorization": f"Bearer {_TOKEN_B}"},
            json=chat_body(),
        )
        assert second.status_code == 429
        assert second.json()["error"]["code"] == "GW_BUDGET_EXHAUSTED"
        assert outbound_count == 1
        release.set()
        assert (await first).status_code == 200

    async with running_gateway(cfg, httpx.MockTransport(handler)) as (_app, client):
        reopened = await client.post(
            "/chat/completions",
            headers={"Authorization": f"Bearer {_TOKEN_B}"},
            json=chat_body(),
        )
        assert reopened.status_code == 429
        assert outbound_count == 1
    assert len(rows(ledger)) == 1


@pytest.mark.asyncio
async def test_unknown_usage_keeps_reservation_and_bad_usage_is_not_assumed_zero(tmp_path):
    outbound_count = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal outbound_count
        outbound_count += 1
        if outbound_count == 1:
            return json_stream_response(completion(usage=None))
        return json_stream_response(
            completion(usage={"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 1})
        )

    from krim_agent.providers.budget_gateway import _json_bytes

    ledger = tmp_path / "unknown.sqlite"
    first_bound = policy().input_upper_bound(
        _json_bytes(chat_body() | {"thinking": {"type": "disabled"}}),
        messages=1,
        tools=0,
    )
    cfg = config(
        ledger,
        limits=budget(max_requests=4, max_input_tokens=first_bound, max_output_tokens=2048),
    )
    async with running_gateway(cfg, httpx.MockTransport(handler)) as (_app, client):
        first = await client.post(
            "/chat/completions",
            headers={"Authorization": f"Bearer {_TOKEN_A}"},
            json=chat_body(),
        )
        assert first.status_code == 502
        assert first.json() == {"error": {"code": "GW_USAGE_UNKNOWN"}}
        records = rows(ledger)
        assert records[0][1] == "unknown"
        assert records[0][2] > 0 and records[0][3] == 64
        second = await client.post(
            "/chat/completions",
            headers={"Authorization": f"Bearer {_TOKEN_A}"},
            json=chat_body(),
        )
        assert second.status_code == 429
        assert second.json()["error"]["code"] == "GW_BUDGET_EXHAUSTED"
        assert outbound_count == 1
    assert rows(ledger)[0][5:] == (None, None, None)


@pytest.mark.asyncio
async def test_reported_zero_usage_is_preserved_without_estimated_substitution(tmp_path):
    async def handler(_request: httpx.Request) -> httpx.Response:
        return json_stream_response(
            completion(usage={"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0})
        )

    ledger = tmp_path / "zero.sqlite"
    async with running_gateway(config(ledger), httpx.MockTransport(handler)) as (_app, client):
        response = await client.post(
            "/chat/completions",
            headers={"Authorization": f"Bearer {_TOKEN_A}"},
            json=chat_body(),
        )
        assert response.status_code == 200
        assert response.json()["usage"] == usage(0, 0)
    record = rows(ledger)[0]
    assert record[1] == "settled"
    assert record[5:8] == (0, 0, 0)


@pytest.mark.asyncio
async def test_invalid_requests_redirect_and_exhaustion_never_reforward(tmp_path):
    outbound: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        outbound.append(request)
        return httpx.Response(302, headers={"Location": "https://rightapi.io/steal"})

    ledger = tmp_path / "deny.sqlite"
    cfg = config(
        ledger,
        limits=budget(max_requests=1, max_input_tokens=4096, max_output_tokens=2048),
    )
    async with running_gateway(cfg, httpx.MockTransport(handler)) as (_app, client):
        bad_token = await client.post(
            "/chat/completions",
            headers={"Authorization": "Bearer wrong-token"},
            json=chat_body(),
        )
        assert bad_token.status_code == 401
        wrong_model = await client.post(
            "/chat/completions",
            headers={"Authorization": f"Bearer {_TOKEN_A}"},
            json=chat_body(model="another-model"),
        )
        assert wrong_model.status_code == 400
        assert wrong_model.json()["error"]["code"] == "GW_BAD_MODEL"
        bad_path = await client.get(
            "/v1/private?url=https://attacker.example",
            headers={"Authorization": f"Bearer {_TOKEN_A}"},
        )
        assert bad_path.status_code == 404
        head = await client.head(
            "/models",
            headers={"Authorization": f"Bearer {_TOKEN_A}"},
        )
        assert head.status_code == 405
        trailing_slash = await client.get(
            "/models/",
            headers={"Authorization": f"Bearer {_TOKEN_A}"},
        )
        assert trailing_slash.status_code == 404
        proxy = await client.post(
            "/chat/completions",
            headers={
                "Authorization": f"Bearer {_TOKEN_A}",
                "X-Forwarded-Host": "attacker.example",
            },
            json=chat_body(),
        )
        assert proxy.status_code == 400
        assert len(outbound) == 0

        redirect = await client.post(
            "/chat/completions",
            headers={"Authorization": f"Bearer {_TOKEN_A}"},
            json=chat_body(),
        )
        assert redirect.status_code == 502
        assert redirect.json() == {"error": {"code": "GW_UPSTREAM_REDIRECT"}}
        assert len(outbound) == 1
        assert outbound[0].url == "https://api.site/v1/chat/completions"

        over_budget = await client.post(
            "/chat/completions",
            headers={"Authorization": f"Bearer {_TOKEN_A}"},
            json=chat_body(),
        )
        assert over_budget.status_code == 429
        assert over_budget.json()["error"]["code"] == "GW_BUDGET_EXHAUSTED"
        assert len(outbound) == 1


@pytest.mark.asyncio
async def test_secret_chaff_is_not_persisted_or_echoed_in_error_or_success(tmp_path):
    outbound: list[httpx.Request] = []
    chaff = "SYNTHETIC-CHaff-credential-looking-value"

    async def handler(request: httpx.Request) -> httpx.Response:
        outbound.append(request)
        if len(outbound) == 1:
            return httpx.Response(502, text=f"vendor reflected {chaff} {_API_KEY}")
        return json_stream_response(completion(usage=usage(8, 2), content=f"output {_API_KEY}"))

    ledger = tmp_path / "secrets.sqlite"
    cfg = config(ledger, api_key=_API_KEY)
    async with running_gateway(cfg, httpx.MockTransport(handler)) as (_app, client):
        failed = await client.post(
            "/chat/completions",
            headers={"Authorization": f"Bearer {_TOKEN_A}"},
            json={
                **chat_body(),
                "messages": [{"role": "user", "content": chaff}],
            },
        )
        assert failed.status_code == 502
        assert chaff not in failed.text and _API_KEY not in failed.text
        succeeded = await client.post(
            "/chat/completions",
            headers={"Authorization": f"Bearer {_TOKEN_B}"},
            json=chat_body(),
        )
        assert succeeded.status_code == 200
        assert _API_KEY not in succeeded.text
        assert "[redacted]" in succeeded.text
        assert all(_API_KEY.encode() not in request.content for request in outbound)
    raw_ledger = ledger.read_bytes()
    assert chaff.encode() not in raw_ledger
    assert _API_KEY.encode() not in raw_ledger


@pytest.mark.asyncio
async def test_cancellation_keeps_unknown_reservation_and_closes_stream_resources(tmp_path):
    upstream_stream = WaitingBody()

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            stream=upstream_stream,
        )

    transport = TrackingMockTransport(handler)
    ledger = tmp_path / "cancel.sqlite"
    app = create_gateway(config(ledger), transport=transport)
    async with app.router.lifespan_context(app):
        owned_client = app.state.gateway_client
        assert owned_client is not None
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://gateway.test",
        ) as client:
            task = asyncio.create_task(
                client.post(
                    "/chat/completions",
                    headers={"Authorization": f"Bearer {_TOKEN_A}"},
                    json=chat_body(stream=True),
                )
            )
            await asyncio.wait_for(upstream_stream.started.wait(), timeout=3)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert upstream_stream.closed
        assert rows(ledger)[0][1] in {"reserved", "unknown"}
        assert not owned_client.is_closed
    assert transport.closed
    assert app.state.gateway_client is None
    assert rows(ledger)[0][1] == "unknown"


@pytest.mark.asyncio
async def test_invalid_usage_is_unknown_and_keeps_original_reservation(tmp_path):
    async def handler(_request: httpx.Request) -> httpx.Response:
        return json_stream_response(
            completion(usage={"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 99})
        )

    ledger = tmp_path / "invalid-usage.sqlite"
    async with running_gateway(config(ledger), httpx.MockTransport(handler)) as (_app, client):
        response = await client.post(
            "/chat/completions",
            headers={"Authorization": f"Bearer {_TOKEN_A}"},
            json=chat_body(),
        )
        assert response.status_code == 502
        assert response.json() == {"error": {"code": "GW_USAGE_INVALID"}}
    record = rows(ledger)[0]
    assert record[1] == "unknown"
    assert record[2] > 0 and record[3] == 64
    assert record[5:] == (None, None, None)


@pytest.mark.asyncio
async def test_usage_over_reservation_poisons_campaign_without_appending_budget(tmp_path):
    outbound_count = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal outbound_count
        outbound_count += 1
        return json_stream_response(completion(usage=usage(10_000, 2)))

    ledger = tmp_path / "overrun.sqlite"
    async with running_gateway(config(ledger), httpx.MockTransport(handler)) as (_app, client):
        first = await client.post(
            "/chat/completions",
            headers={"Authorization": f"Bearer {_TOKEN_A}"},
            json=chat_body(),
        )
        assert first.status_code == 502
        assert first.json() == {"error": {"code": "GW_USAGE_OVERRUN"}}
        next_response = await client.post(
            "/chat/completions",
            headers={"Authorization": f"Bearer {_TOKEN_B}"},
            json=chat_body(),
        )
        assert next_response.status_code == 429
        assert next_response.json()["error"]["code"] == "GW_CAMPAIGN_CLOSED"
        assert outbound_count == 1
    record = rows(ledger)[0]
    assert record[1] == "overrun"
    assert record[5:7] == (10_000, 2)


@pytest.mark.asyncio
async def test_models_aliases_count_toward_five_request_trial_cap(tmp_path):
    outbound: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        outbound.append(request)
        return json_stream_response({"object": "list", "data": []})

    ledger = tmp_path / "trial-cap.sqlite"
    async with running_gateway(config(ledger), httpx.MockTransport(handler)) as (_app, client):
        for index in range(5):
            path = "/models" if index % 2 == 0 else "/v1/models"
            response = await client.get(
                path,
                headers={"Authorization": f"Bearer {_TOKEN_A}"},
            )
            assert response.status_code == 200
        sixth = await client.get(
            "/v1/models",
            headers={"Authorization": f"Bearer {_TOKEN_A}"},
        )
        assert sixth.status_code == 429
        assert sixth.json()["error"]["code"] == "GW_BUDGET_EXHAUSTED"
        other_trial = await client.get(
            "/models",
            headers={"Authorization": f"Bearer {_TOKEN_B}"},
        )
        assert other_trial.status_code == 200
    assert len(outbound) == 6
    assert {request.url.path for request in outbound} == {"/v1/models"}
    assert len(rows(ledger)) == 6


@pytest.mark.asyncio
async def test_oversized_requests_and_upstream_frames_are_rejected(tmp_path):
    outbound: list[httpx.Request] = []
    large_json_body = ChunkedBody([b"x" * (4 * 1024 * 1024 + 1)])
    large_sse_body = ChunkedBody([b"data: " + b"x" * (64 * 1024 + 1)])

    async def handler(request: httpx.Request) -> httpx.Response:
        outbound.append(request)
        if request.headers.get("accept") == "text/event-stream":
            return httpx.Response(
                200,
                headers={"Content-Type": "text/event-stream"},
                stream=large_sse_body,
            )
        return httpx.Response(
            200,
            headers={"Content-Type": "application/json"},
            stream=large_json_body,
        )

    ledger = tmp_path / "size-limits.sqlite"
    async with running_gateway(config(ledger), httpx.MockTransport(handler)) as (_app, client):
        oversized_request = await client.post(
            "/chat/completions",
            headers={"Authorization": f"Bearer {_TOKEN_A}"},
            json={
                **chat_body(),
                "messages": [{"role": "user", "content": "x" * (512 * 1024)}],
            },
        )
        assert oversized_request.status_code == 413
        assert outbound == []

        oversized_json = await client.post(
            "/chat/completions",
            headers={"Authorization": f"Bearer {_TOKEN_A}"},
            json=chat_body(),
        )
        assert oversized_json.status_code == 502
        assert oversized_json.json()["error"]["code"] == "GW_UPSTREAM_SIZE"

        oversized_frame = await client.post(
            "/chat/completions",
            headers={"Authorization": f"Bearer {_TOKEN_B}"},
            json=chat_body(stream=True),
        )
        assert oversized_frame.status_code == 200
        assert "GW_UPSTREAM_FRAME_LIMIT" in oversized_frame.text
    assert len(outbound) == 2
    assert large_json_body.closed and large_sse_body.closed
    assert [record[1] for record in rows(ledger)] == ["unknown", "unknown"]


@pytest.mark.asyncio
async def test_factory_without_explicit_prices_budget_or_credentials_stays_disabled():
    first_token = generate_trial_token()
    assert len(first_token) >= 43 and first_token != generate_trial_token()
    app = create_gateway()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://gateway.test",
        ) as client:
            response = await client.post("/chat/completions", json=chat_body())
            assert response.status_code == 503
            assert response.json() == {"error": {"code": "GW_GATEWAY_DISABLED"}}
        assert app.state.gateway_client is None


def test_normalized_unicode_is_reserved_before_forwarding():
    import unicodedata

    text = "ﷺ" * 10 + "é"
    body = json.dumps(
        {"messages": [{"role": "user", "content": text}]}, ensure_ascii=False
    ).encode()
    bound = policy().input_upper_bound(body, messages=1, tools=0)
    expanded = unicodedata.normalize("NFKD", text).casefold().encode()
    assert bound >= len(body) + len(expanded) + 64 + 32


def test_user_usd_quote_can_be_conservatively_budgeted_in_cny():
    from dataclasses import replace

    priced = replace(
        policy(), input_cny_per_million=Decimal("1.6"), output_cny_per_million=Decimal("5.6")
    )
    assert priced.cost_microcny(1_500_000, 200_000) == 3_520_000
    assert priced.cost_microcny(1_500_000, 200_000) < budget().max_cost_microcny


@pytest.mark.asyncio
async def test_sdk_options_keep_non_thinking_and_metering_strict(tmp_path):
    sent = []

    async def handler(request: httpx.Request) -> httpx.Response:
        sent.append(json.loads(request.content))
        reply = completion(usage=usage(10, 2))
        reply["choices"][0]["message"]["reasoning_content"] = None
        reply["choices"][0]["message"]["tool_calls"] = None
        reply["choices"][0]["message"]["annotations"] = []
        reply[_API_KEY] = {"echo": _API_KEY}
        return json_stream_response(reply)

    async with running_gateway(
        config(tmp_path / "sdk-options.sqlite"), httpx.MockTransport(handler)
    ) as (_app, client):
        body = chat_body() | {
            "thinking": {"type": "disabled"},
            "parallel_tool_calls": False,
            "n": 1,
        }
        response = await client.post(
            "/v1/chat/completions", headers={"Authorization": f"Bearer {_TOKEN_A}"}, json=body
        )
        assert response.status_code == 200
        assert _API_KEY not in response.text
        assert sent[0]["thinking"] == {"type": "disabled"}
        for invalid in (
            {"thinking": {"type": "enabled"}},
            {"stream": True, "stream_options": {"include_usage": False}},
            {"n": True},
            {"n": 2},
            {"parallel_tool_calls": "false"},
        ):
            result = await client.post(
                "/chat/completions",
                headers={"Authorization": f"Bearer {_TOKEN_A}"},
                json=chat_body() | invalid,
            )
            assert result.status_code == 400
        assert len(sent) == 1


def test_missing_unknown_or_untrusted_prices_and_budget_configuration_fail_closed(tmp_path):
    disabled = create_gateway()
    assert disabled is not None
    with pytest.raises(ValueError):
        PricePolicy(
            model=_MODEL,
            currency="CNY",
            unit="unknown-unit",
            input_cny_per_million=Decimal("1"),
            output_cny_per_million=Decimal("2"),
            source="unverified",
            price_verified=False,
            input_bound_verified=False,
            input_fixed_overhead_tokens=0,
            input_per_message_overhead_tokens=0,
            input_per_tool_overhead_tokens=0,
        )
    with pytest.raises(ValueError):
        BudgetLimits(
            max_requests=121,
            max_requests_per_trial=5,
            max_input_tokens=1_500_000,
            max_output_tokens=200_000,
            max_output_tokens_per_request=2048,
            max_cost_cny=Decimal("20"),
            currency="CNY",
        )
    with pytest.raises(ValueError):
        GatewayConfig(
            upstream_origin="https://api.site/redirected/path",
            model=_MODEL,
            api_key=_API_KEY,
            campaign_id="test",
            ledger_path=tmp_path / "invalid.sqlite",
            trial_tokens={"trial-a": _TOKEN_A},
            price_policy=policy(),
            budget=budget(),
        )
