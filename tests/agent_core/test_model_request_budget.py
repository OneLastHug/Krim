import json

import httpx
import pytest

from krim_agent.contracts import Message, ServiceError
from krim_agent.providers.openai_compatible import (
    OpenAICompatibleConfig,
    OpenAICompatibleProvider,
    ProviderError,
)
from krim_agent.providers.types import ModelRequestBudget, ModelText, ModelToolCall
from krim_agent.runtime.model_step import ModelStep


def frame(value):
    data = value if isinstance(value, str) else json.dumps(value, separators=(",", ":"))
    return f"data: {data}\n\n".encode()


def choice(delta=None, finish=None):
    return {"choices": [{"index": 0, "delta": delta or {}, "finish_reason": finish}]}


def tool(index, *, kind="function", identity=None, name=None, arguments="{}"):
    result = {
        "index": index,
        "id": identity or f"call-{index}",
        "function": {"name": name or f"tool_{index}", "arguments": arguments},
    }
    if kind != "omitted":
        result["type"] = kind
    return result


class StreamBody(httpx.AsyncByteStream):
    def __init__(self, chunks):
        self.chunks = chunks

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk

    async def aclose(self):
        pass


def mock_provider(monkeypatch, responder, *, max_tool_calls=4, max_request_bytes=32768):
    requests = []
    original_client = httpx.AsyncClient

    async def handler(request):
        requests.append(json.loads(request.content))
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            stream=StreamBody(responder(len(requests))),
        )

    class MockClient(original_client):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", MockClient)
    provider = OpenAICompatibleProvider(
        OpenAICompatibleConfig(
            base_url="https://api.example.com/v1",
            model="fixture",
            max_tool_calls=max_tool_calls,
            max_request_bytes=max_request_bytes,
        )
    )
    return provider, requests


async def emit(_kind, _data):
    return None


def registry_tools():
    return [
        {"name": "tool_0", "input_schema": {"type": "object"}},
        {"name": "tool_1", "input_schema": {"type": "object"}},
    ]


@pytest.mark.asyncio
async def test_budgeted_http_requests_decrease_across_multi_tool_steps(monkeypatch):
    def responder(index):
        if index == 1:
            return [
                frame(choice({"tool_calls": [tool(0, kind=None), tool(1, kind="omitted")]})),
                frame(choice(finish="tool_calls")),
                frame("[DONE]"),
            ]
        return [frame(choice({"content": "done"})), frame(choice(finish="stop")), frame("[DONE]")]

    provider, requests = mock_provider(monkeypatch, responder)
    step = ModelStep(
        provider, emit, lambda: None, max_steps=8, max_output=4096, deadline=None, workspace=False
    )
    messages = [Message(role="system", content="base"), Message(role="user", content="start")]
    tools = registry_tools()
    try:
        first = await step.call(messages, tools, max_group=4, remaining_tools=32)
        assert [call.id for call in first.calls] == ["call-0", "call-1"]
        followup = [
            *messages,
            Message(
                role="assistant",
                content="",
                tool_calls=[call.model_dump(mode="json") for call in first.calls],
            ),
            Message(role="tool", content="{}", call_id="call-0"),
            Message(role="tool", content="{}", call_id="call-1"),
        ]
        second = await step.call(followup, tools, max_group=4, remaining_tools=30)

        assert second.text == "done"
        assert (
            "Remaining model steps including this request: 8"
            in requests[0]["messages"][0]["content"]
        )
        assert "remaining tool calls for this run: 32" in requests[0]["messages"][0]["content"]
        assert "tool calls allowed in this request: 4" in requests[0]["messages"][0]["content"]
        assert (
            "Remaining model steps including this request: 7"
            in requests[1]["messages"][0]["content"]
        )
        assert "remaining tool calls for this run: 30" in requests[1]["messages"][0]["content"]
        assert len(requests[0]["tools"]) == 2 and "parallel_tool_calls" not in requests[0]
        assert any(message.get("tool_call_id") == "call-1" for message in requests[1]["messages"])
        assert messages[0].content == "base" and messages[1].content == "start"
    finally:
        await provider.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "budget,expected_tools,parallel",
    [
        (ModelRequestBudget(2, 1, 1), True, False),
        (ModelRequestBudget(1, 0, 0), False, None),
    ],
)
async def test_http_tool_wire_matches_one_or_zero_remaining(
    monkeypatch, budget, expected_tools, parallel
):
    provider, requests = mock_provider(
        monkeypatch,
        lambda _index: [
            frame(choice({"content": "ok"})),
            frame(choice(finish="stop")),
            frame("[DONE]"),
        ],
    )
    messages = ModelStep.with_budget_prompt([Message(role="user", content="hello")], budget)
    try:
        events = [
            event async for event in provider.stream_with_budget(messages, registry_tools(), budget)
        ]
        assert any(type(event) is ModelText for event in events)
        body = requests[0]
        assert ("tools" in body) is expected_tools
        assert body.get("parallel_tool_calls") is parallel
    finally:
        await provider.close()


@pytest.mark.asyncio
async def test_over_budget_complete_tool_group_is_rejected_before_release(monkeypatch):
    provider, requests = mock_provider(
        monkeypatch,
        lambda _index: [
            frame(choice({"tool_calls": [tool(0), tool(1)]})),
            frame(choice(finish="tool_calls")),
            frame("[DONE]"),
        ],
    )
    budget = ModelRequestBudget(1, 1, 1)
    messages = ModelStep.with_budget_prompt([Message(role="user", content="run")], budget)
    released = []
    try:
        with pytest.raises(ProviderError) as error:
            async for event in provider.stream_with_budget(messages, registry_tools(), budget):
                if type(event) is ModelToolCall:
                    released.append(event)
        assert error.value.code == "tool_limit"
        assert released == []
        assert len(requests) == 1
        assert requests[0]["parallel_tool_calls"] is False
    finally:
        await provider.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "call,expected",
    [
        (tool(0, kind="message"), "provider_protocol"),
        (tool(True), "provider_protocol"),
        (tool(-1), "provider_protocol"),
        (tool(1), "provider_protocol"),
        (tool(0, arguments="{bad"), "provider_tool_arguments"),
    ],
)
async def test_bad_incremental_tool_metadata_stays_rejected(monkeypatch, call, expected):
    provider, _requests = mock_provider(
        monkeypatch,
        lambda _index: [
            frame(choice({"tool_calls": [call]})),
            frame(choice(finish="tool_calls")),
            frame("[DONE]"),
        ],
    )
    try:
        with pytest.raises(ProviderError) as error:
            [
                event
                async for event in provider.stream(
                    [Message(role="user", content="invalid")], registry_tools()
                )
            ]
        assert error.value.code == expected
    finally:
        await provider.close()


@pytest.mark.asyncio
async def test_legacy_provider_signature_and_summary_keep_budget_ephemeral():
    class LegacyProvider:
        def __init__(self):
            self.requests = []

        async def stream(self, messages, tools):
            self.requests.append((list(messages), list(tools)))
            yield ModelText(text="summary")

        async def close(self):
            return None

    provider = LegacyProvider()
    step = ModelStep(
        provider, emit, lambda: None, max_steps=2, max_output=4096, deadline=None, workspace=False
    )
    original = [Message(role="user", content="untrusted history")]
    request = await step.call(
        original, registry_tools(), purpose="summary", max_group=4, remaining_tools=8
    )

    assert request.text == "summary"
    messages, tools = provider.requests[0]
    assert tools == []
    assert messages[0].role == "system"
    assert "Remaining model steps including this request: 2" in messages[0].content
    assert "remaining tool calls for this run: 8" in messages[0].content
    assert "tool calls allowed in this request: 0" in messages[0].content
    assert original == [Message(role="user", content="untrusted history")]
    await step.call(original, [], max_group=4, remaining_tools=8)
    with pytest.raises(ServiceError) as error:
        await step.call(original, [], max_group=4, remaining_tools=8)
    assert error.value.code == "step_limit"


@pytest.mark.asyncio
async def test_budget_prompt_counts_toward_http_request_byte_limit(monkeypatch):
    provider, requests = mock_provider(
        monkeypatch,
        lambda _index: [
            frame(choice({"content": "unused"})),
            frame(choice(finish="stop")),
            frame("[DONE]"),
        ],
        max_request_bytes=256,
    )
    step = ModelStep(
        provider, emit, lambda: None, max_steps=8, max_output=4096, deadline=None, workspace=False
    )
    try:
        with pytest.raises(ProviderError) as error:
            await step.call([Message(role="user", content="")], [], max_group=4, remaining_tools=32)
        assert error.value.code == "provider_request_limit"
        assert requests == []
    finally:
        await provider.close()


def test_workspace_compaction_measure_includes_the_temporary_budget_prompt():
    from krim_agent.context.compaction import Compactor
    from krim_agent.context.types import CompactionPolicy, ContextPolicy
    from krim_agent.contracts import encoded

    class LegacyProvider:
        async def stream(self, _messages, _tools):
            yield ModelText(text="unused")

        async def close(self):
            return None

    step = ModelStep(
        LegacyProvider(),
        emit,
        lambda: None,
        max_steps=8,
        max_output=4096,
        deadline=None,
        workspace=True,
    )
    compactor = object.__new__(Compactor)
    compactor.step = step
    compactor.policy = ContextPolicy(max_bytes=8192, max_messages=16, reserved_output_tokens=1)
    compactor.compaction = CompactionPolicy()
    compactor.request_limit = 8192
    compactor.max_group = 4
    compactor.remaining_tools = 32
    messages = [Message(role="system", content="system"), Message(role="user", content="history")]
    tools = registry_tools()

    measured, _trigger = compactor.measure(messages, tools)
    budget = step.request_budget(4, 32)
    request_messages = step.with_budget_prompt(messages, budget)
    expected = len(
        encoded(
            {
                "messages": [message.model_dump(mode="json") for message in request_messages],
                "tools": tools,
            }
        ).encode()
    )
    raw = len(
        encoded(
            {
                "messages": [message.model_dump(mode="json") for message in messages],
                "tools": tools,
            }
        ).encode()
    )

    assert measured == expected
    assert measured > raw


def tool_group_response(*calls):
    return [
        frame(choice({"tool_calls": list(calls)})),
        frame(choice(finish="tool_calls")),
        frame("[DONE]"),
    ]


def text_response(text):
    return [frame(choice({"content": text})), frame(choice(finish="stop")), frame("[DONE]")]


async def run_http_session(
    tmp_path,
    monkeypatch,
    *,
    name,
    request_text,
    responses,
    max_steps,
    max_tools_per_step,
    max_tool_calls,
    provider_cap,
):
    from krim_agent.composition import open_application
    from krim_agent.config import Settings
    from krim_agent.sdk import SessionSDK
    from krim_agent.sessions.types import SessionCreate, TurnRequest
    from krim_agent.tools.builtins import builtin_registry

    def responder(index):
        if not 1 <= index <= len(responses):
            raise AssertionError("unexpected model request")
        return responses[index - 1]

    provider, requests = mock_provider(monkeypatch, responder, max_tool_calls=provider_cap)
    path = tmp_path / (name + ".sqlite")
    settings = Settings(
        database=path,
        provider="http",
        http_mode="agent",
        max_steps=max_steps,
        max_tools_per_step=max_tools_per_step,
        max_tool_calls=max_tool_calls,
    )
    async with await open_application(
        path, settings=settings, provider=provider, registry=builtin_registry()
    ) as app:
        sdk = SessionSDK(app.sessions)
        session = await sdk.create(SessionCreate(request_key=name, target_key="target-http"))
        submitted = await sdk.submit(
            session.id,
            TurnRequest(
                request_key=name + "-turn",
                text=request_text,
                expected_revision=session.revision,
            ),
        )
        finished = await sdk.wait(session.id, submitted.id)
        snapshot = await app.store.snapshot(finished.run_id)
        result = await app.service.result(finished.run_id)
        history = await sdk.history(session.id, limit=50)
        return requests, finished, snapshot, result, history


@pytest.mark.asyncio
async def test_open_application_session_carries_budgets_across_real_tool_steps(
    tmp_path, monkeypatch
):
    from krim_agent.contracts import encoded

    request_text = "同一步计算并回显，下一步再计算"
    requests, finished, snapshot, result, history = await run_http_session(
        tmp_path,
        monkeypatch,
        name="multi-step-budget",
        request_text=request_text,
        responses=[
            tool_group_response(
                tool(0, identity="add-first", name="add", arguments='{"left":2,"right":3}'),
                tool(1, identity="echo-first", name="echo", arguments='{"text":"hello"}'),
            ),
            tool_group_response(
                tool(0, identity="add-second", name="add", arguments='{"left":5,"right":7}')
            ),
            text_response("完成"),
        ],
        max_steps=8,
        max_tools_per_step=4,
        max_tool_calls=32,
        provider_cap=4,
    )

    assert finished.status == "completed"
    assert result.run.status == "completed" and result.text == "完成"
    assert result.model_steps == 3
    starts = [event.data for event in snapshot.events if event.kind == "tool_start"]
    ends = [event.data for event in snapshot.events if event.kind == "tool_end"]
    assert [(item["name"], item["step_index"]) for item in starts] == [
        ("add", 0),
        ("echo", 0),
        ("add", 1),
    ]
    assert len(ends) == 3
    assert len(result.tools) == 3
    assert len(requests) == 3
    for request, steps, remaining in zip(requests, (8, 7, 6), (32, 30, 29), strict=True):
        budget = request["messages"][0]["content"]
        assert f"Remaining model steps including this request: {steps}" in budget
        assert f"remaining tool calls for this run: {remaining}" in budget
        assert "tool calls allowed in this request: 4" in budget
    assert len(requests[0]["tools"]) == 3
    assert len(requests[1]["tools"]) == 3
    assert len(requests[2]["tools"]) == 3
    assert any(message.get("tool_call_id") == "echo-first" for message in requests[1]["messages"])
    assert snapshot.request.text == request_text
    canonical_history = encoded([turn.model_dump(mode="json") for turn in history])
    assert "Krim request budget/1" not in canonical_history
    assert "Krim request budget/1" not in encoded(snapshot.request.model_dump(mode="json"))


@pytest.mark.asyncio
async def test_session_with_no_remaining_tools_gets_text_only_request(tmp_path, monkeypatch):
    requests, finished, snapshot, result, _history = await run_http_session(
        tmp_path,
        monkeypatch,
        name="zero-tool-budget",
        request_text="只执行一次加法，然后总结",
        responses=[
            tool_group_response(
                tool(0, identity="only-add", name="add", arguments='{"left":4,"right":6}')
            ),
            text_response("结果是10"),
        ],
        max_steps=8,
        max_tools_per_step=4,
        max_tool_calls=1,
        provider_cap=1,
    )

    assert finished.status == "completed"
    assert result.run.status == "completed" and result.text == "结果是10"
    assert len(result.tools) == 1
    assert len(requests) == 2
    first = requests[0]
    assert first["parallel_tool_calls"] is False and first["tools"]
    second = requests[1]
    budget = second["messages"][0]["content"]
    assert "Remaining model steps including this request: 7" in budget
    assert "remaining tool calls for this run: 0" in budget
    assert "tool calls allowed in this request: 0" in budget
    assert "tools" not in second and "parallel_tool_calls" not in second
    assert not any(
        event.kind == "tool_start" for event in snapshot.events if event.data.get("step_index") == 1
    )


@pytest.mark.asyncio
async def test_old_2_1_1_session_rejects_complete_group_without_execution(tmp_path, monkeypatch):
    requests, finished, snapshot, result, _history = await run_http_session(
        tmp_path,
        monkeypatch,
        name="old-one-tool-budget",
        request_text="同时计算并回显",
        responses=[
            tool_group_response(
                tool(0, identity="over-add", name="add", arguments='{"left":1,"right":2}'),
                tool(1, identity="over-echo", name="echo", arguments='{"text":"no side effect"}'),
            ),
        ],
        max_steps=2,
        max_tools_per_step=1,
        max_tool_calls=1,
        provider_cap=1,
    )

    assert finished.status == "failed" and finished.code == "tool_limit"
    assert result.run.status == "failed" and result.run.code == "tool_limit"
    assert len(requests) == 1
    prompt = requests[0]["messages"][0]["content"]
    assert "Remaining model steps including this request: 2" in prompt
    assert "remaining tool calls for this run: 1" in prompt
    assert "tool calls allowed in this request: 1" in prompt
    assert requests[0]["parallel_tool_calls"] is False
    assert not [event for event in snapshot.events if event.kind == "tool_start"]
    assert result.tools == []
