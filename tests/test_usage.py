import asyncio
import json
from datetime import UTC, datetime

import httpx
import pytest

from krim_agent.client import KrimClient
from krim_agent.config import Settings
from krim_agent.protocol.domain import MessageView, ServiceError
from krim_agent.protocol.models import StreamFrame
from krim_agent.providers import (
    ModelText,
    ModelUsage,
    OpenAICompatibleConfig,
    OpenAICompatibleProvider,
)
from krim_agent.sdk import ModelStepPart, PythonAI, UsagePart
from krim_agent.tools import ToolRegistry
from krim_agent.usage import TokenUsage, usage_from_events


def frame(seq, kind, data):
    return StreamFrame(stream_id="run", seq=seq, kind=kind, data=data, created_at=datetime.now(UTC))


def usage(input_tokens=5, output_tokens=2, total_tokens=7):
    return {
        "source": "provider",
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
    }


def test_usage_requires_consistent_nonnegative_totals_and_keeps_zero():
    assert TokenUsage(input_tokens=0, output_tokens=0, total_tokens=0).total_tokens == 0
    with pytest.raises(ValueError):
        TokenUsage(input_tokens=5, output_tokens=2, total_tokens=8)
    with pytest.raises(ValueError):
        TokenUsage(input_tokens=5, cached_input_tokens=6)
    with pytest.raises(ValueError):
        TokenUsage(input_tokens=5, output_tokens=2, reasoning_output_tokens=3)


def test_usage_has_no_price_or_estimate_fields():
    with pytest.raises(ValueError):
        TokenUsage(input_tokens=1, cost_usd=0.2)
    with pytest.raises(ValueError):
        TokenUsage(input_tokens=1, source="estimated")


def test_event_projection_requires_ordered_model_steps_and_complete_reports():
    events = [
        frame(0, "start", {"provider": "local", "model_usage_trace": 1}),
        frame(1, "chunk", {"type": "model_step", "step": 0, "phase": "start", "provider": "local"}),
        frame(2, "chunk", {"type": "token_usage", "step": 0, "usage": usage(), "final": False}),
        frame(3, "chunk", {"type": "token_usage", "step": 0, "usage": usage(), "final": True}),
        frame(
            4,
            "chunk",
            {
                "type": "model_step",
                "step": 0,
                "phase": "end",
                "provider": "local",
                "completed": True,
            },
        ),
        frame(5, "end", {"status": "completed"}),
    ]
    result = usage_from_events("run", events)
    assert result.usage_complete and result.token_usage is not None
    assert result.token_usage.total_tokens == 7 and result.token_usage.reported_steps == 1
    with pytest.raises(ServiceError) as error:
        usage_from_events("run", [events[0], events[2]])
    assert error.value.code == "usage_invalid_event"


async def test_python_sdk_exposes_provider_usage_and_replays_without_call(repository):
    class Provider:
        name = "usage-provider"

        def __init__(self):
            self.calls = 0

        async def stream(self, _messages, _tools):
            self.calls += 1
            yield ModelText(text="回答")
            yield ModelUsage(
                usage=TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7), final=True
            )

    provider = Provider()
    async with await PythonAI.create(provider=provider, tools=ToolRegistry()) as ai:
        stream = await ai.stream_text("用量", idempotency_key="usage-once")
        parts = [part async for part in stream.full_stream()]
        result = await stream.collect()
        assert isinstance(next(part for part in parts if isinstance(part, UsagePart)), UsagePart)
        assert any(isinstance(part, ModelStepPart) for part in parts)
        assert result.token_usage is not None and result.token_usage.total_tokens == 7
        assert result.usage_complete and provider.calls == 1
        replay = await ai.stream_text(
            "用量", topic_id=result.run.topic_id, idempotency_key="usage-once"
        )
        replay_result = await replay.collect()
        assert replay_result.token_usage == result.token_usage and provider.calls == 1
        view = await ai.runtime.repository.usage_view(result.run.id)
        assert view.trace_available and len(view.steps) == 1


async def test_unknown_provider_usage_stays_unknown_and_is_durable(repository):
    class Provider:
        name = "unknown-usage"

        async def stream(self, _messages, _tools):
            yield ModelText(text="无报告")

    async with await PythonAI.create(provider=Provider(), tools=ToolRegistry()) as ai:
        result = await ai.generate_text("未知用量")
        assert result.token_usage is None and not result.usage_complete
        view = await ai.runtime.repository.usage_view(result.run.id)
        assert view.trace_available and view.steps[0].usage is None


async def test_openai_compatible_usage_only_event_is_parsed():
    async def handler(request):
        body = json.loads(request.content)
        assert body["stream_options"] == {"include_usage": True}
        events = [
            {"choices": [{"index": 0, "delta": {"content": "好"}, "finish_reason": None}]},
            {"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
            {
                "choices": [],
                "usage": {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7},
            },
        ]
        text = "".join(f"data: {json.dumps(item, ensure_ascii=False)}\n\n" for item in events)
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=(text + "data: [DONE]\n\n").encode(),
        )

    provider = OpenAICompatibleProvider(
        OpenAICompatibleConfig(
            base_url="http://127.0.0.1:9000/v1", model="local", include_usage=True
        ),
        transport=httpx.MockTransport(handler),
    )
    try:
        values = [
            item
            async for item in provider.stream(
                [
                    MessageView(
                        id="m",
                        topic_id="t",
                        run_id="r",
                        sequence=1,
                        role="user",
                        content="问",
                        details={},
                        created_at=datetime.now(UTC),
                    )
                ],
                [],
            )
        ]
        reports = [item for item in values if isinstance(item, ModelUsage)]
        assert len(reports) == 2 and reports[-1].final and reports[-1].usage.total_tokens == 7
    finally:
        await provider.close()


async def test_usage_http_route_and_client(repository, server_url):
    class Provider:
        name = "http-usage"

        async def stream(self, _messages, _tools):
            yield ModelText(text="答")
            yield ModelUsage(
                usage=TokenUsage(input_tokens=2, output_tokens=1, total_tokens=3), final=True
            )

    ai = await PythonAI.create(
        settings=Settings(database_url=str(repository.database.engine.url)),
        provider=Provider(),
        tools=ToolRegistry(),
    )
    try:
        result = await ai.generate_text("HTTP 用量")

        def read_usage():
            with KrimClient(server_url) as client:
                return client.usage(result.run.id)

        value = await asyncio.to_thread(read_usage)
        assert value.token_usage is not None and value.token_usage.total_tokens == 3
    finally:
        await ai.close()


@pytest.mark.parametrize("bad", [-1, True, 1.0, "1", float("nan"), 9_007_199_254_740_992])
def test_invalid_provider_counts_are_not_coerced(bad):
    with pytest.raises(ValueError):
        TokenUsage(input_tokens=bad)


def test_partial_fields_are_not_inferred_from_other_fields():
    reported = TokenUsage(input_tokens=5, output_tokens=2)
    assert reported.total_tokens is None
    only_total = TokenUsage(total_tokens=7)
    assert only_total.input_tokens is None and only_total.output_tokens is None


class ReportProvider:
    name = "local-report-provider"
    reports_usage = True

    def __init__(self, reports, *, interrupted=False, gate=None):
        self.reports, self.interrupted, self.gate = reports, interrupted, gate
        self.calls = 0

    async def stream(self, _messages, _tools):
        self.calls += 1
        yield ModelText(text="真实测试输出")
        for report in self.reports:
            yield report
        if self.gate is not None:
            await self.gate.wait()
        if self.interrupted:
            from krim_agent.providers import ProviderError

            raise ProviderError("provider_incomplete_stream", "测试断流")


@pytest.mark.parametrize(
    "reports,expected",
    [
        ([ModelUsage(usage=TokenUsage(input_tokens=0, output_tokens=0, total_tokens=0))], 0),
        (
            [
                ModelUsage(
                    usage=TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7), final=False
                ),
                ModelUsage(usage=TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7)),
            ],
            7,
        ),
        (
            [
                ModelUsage(usage=TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7)),
                ModelUsage(usage=TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7)),
            ],
            7,
        ),
        (
            [
                ModelUsage(usage=TokenUsage(input_tokens=5), final=False),
                ModelUsage(usage=TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7)),
            ],
            7,
        ),
        (
            [
                ModelUsage(
                    usage=TokenUsage(input_tokens=5, output_tokens=1, total_tokens=6), final=False
                ),
                ModelUsage(usage=TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7)),
            ],
            7,
        ),
    ],
)
async def test_snapshots_are_projected_once_and_final_fields_are_complete(reports, expected):
    async with await PythonAI.create(provider=ReportProvider(reports), tools=ToolRegistry()) as ai:
        result = await ai.generate_text("snapshot")
        assert result.run.status == "completed" and result.usage_complete
        assert result.token_usage.total_tokens == expected
        assert result.token_usage.reported_steps == 1
        assert (
            result.steps[0].usage_complete and result.steps[0].token_usage.total_tokens == expected
        )


@pytest.mark.parametrize(
    "changed",
    [
        ModelUsage(usage=TokenUsage(input_tokens=4, output_tokens=2, total_tokens=6)),
        ModelUsage(usage=TokenUsage(output_tokens=2, total_tokens=7)),
        ModelUsage(usage=TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7), final=False),
    ],
)
async def test_regression_or_rewriting_final_usage_fails_and_preserves_first_report(changed):
    original = ModelUsage(usage=TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7))
    async with await PythonAI.create(
        provider=ReportProvider([original, changed]), tools=ToolRegistry()
    ) as ai:
        result = await ai.generate_text("conflict")
        assert result.code == "provider_usage_conflict" and not result.usage_complete
        assert result.token_usage.total_tokens is None
        assert result.token_usage.reported["total_tokens"] == 7
        view = await ai.runtime.repository.usage_view(result.run.id)
        assert not view.steps[0].completed


async def test_interrupted_stream_retains_report_without_confirming_it():
    report = ModelUsage(
        usage=TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7), final=False
    )
    async with await PythonAI.create(
        provider=ReportProvider([report], interrupted=True), tools=ToolRegistry()
    ) as ai:
        result = await ai.generate_text("interrupted")
        assert result.code == "provider_incomplete_stream" and result.steps[0].partial
        assert not result.usage_complete and result.token_usage.total_tokens is None
        assert result.token_usage.reported["total_tokens"] == 7
        assert not result.steps[0].usage_complete


async def test_cancelled_stream_retains_unconfirmed_usage():
    gate = asyncio.Event()
    report = ModelUsage(
        usage=TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7), final=False
    )
    async with await PythonAI.create(
        provider=ReportProvider([report], gate=gate), tools=ToolRegistry()
    ) as ai:
        stream = await ai.stream_text("cancel")
        subscription = stream.full_stream()
        async for part in subscription:
            if isinstance(part, UsagePart):
                break
        await subscription.aclose()
        await stream.abort()
        result = await stream.collect()
        assert result.run.status == "aborted" and not result.usage_complete
        assert result.token_usage.reported["total_tokens"] == 7
        assert result.token_usage.total_tokens is None


@pytest.mark.parametrize("unknown_second", [False, True])
async def test_two_model_requests_sum_usage_or_keep_missing_step_unknown(unknown_second):
    from krim_agent.providers import ModelToolCall
    from krim_agent.tools import tool

    @tool(name="lookup", risk="read")
    async def lookup() -> str:
        return "结果"

    class Provider:
        name = "two-requests"
        reports_usage = True
        calls = 0

        async def stream(self, messages, _tools):
            self.calls += 1
            if messages[-1].role != "tool":
                yield ModelToolCall(id="lookup", name="lookup", arguments={})
                yield ModelUsage(usage=TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7))
            else:
                yield ModelText(text="答复")
                if not unknown_second:
                    yield ModelUsage(
                        usage=TokenUsage(input_tokens=9, output_tokens=3, total_tokens=12)
                    )

    registry = ToolRegistry()
    registry.register(lookup)
    provider = Provider()
    async with await PythonAI.create(provider=provider, tools=registry) as ai:
        result = await ai.generate_text("two requests")
        assert provider.calls == 2 and result.run.status == "completed"
        assert result.token_usage.attempted_steps == 2
        if unknown_second:
            assert not result.usage_complete and result.token_usage.total_tokens is None
            assert result.token_usage.reported["total_tokens"] == 7
        else:
            assert result.usage_complete and result.token_usage.total_tokens == 19
            assert result.token_usage.input_tokens == 14 and result.token_usage.output_tokens == 5


async def test_model_start_is_saved_before_synchronous_provider_factory(repository, monkeypatch):
    from krim_agent.protocol.domain import ChatRequest
    from krim_agent.runtime import ChatRuntime

    class Provider:
        name = "synchronous-factory"
        calls = 0

        def stream(self, _messages, _tools):
            self.calls += 1

            async def items():
                yield ModelText(text="不应执行")

            return items()

    provider = Provider()
    runtime = ChatRuntime(repository, provider, ToolRegistry(), Settings())
    original = repository.append_event

    async def reject_model_start(event):
        data = event.data if isinstance(event.data, dict) else {}
        if data.get("type") == "model_step" and data.get("phase") == "start":
            raise OSError("测试存储拒绝")
        return await original(event)

    monkeypatch.setattr(repository, "append_event", reject_model_start)
    try:
        topic = await repository.create_topic("pre-call durability")
        run = await runtime.start(topic.id, ChatRequest(text="one", idempotency_key="one"))
        await runtime.production_task(run.id)
        assert provider.calls == 0
        assert (await repository.get_run(run.id)).status == "failed"
    finally:
        await runtime.close()


@pytest.mark.parametrize(
    "usage_body",
    [
        {"prompt_tokens": True},
        {"prompt_tokens": -1},
        {"prompt_tokens": "2"},
        {"prompt_tokens": 2, "completion_tokens": 1, "total_tokens": 4},
        {"prompt_tokens": 2, "prompt_tokens_details": {"cached_tokens": 3}},
        {"completion_tokens": 2, "completion_tokens_details": {"reasoning_tokens": 3}},
        {"prompt_tokens_details": []},
    ],
)
async def test_invalid_compatible_reports_fail_before_becoming_facts(usage_body):
    from krim_agent.providers import ProviderError

    async def handler(_request):
        payload = {"choices": [], "usage": usage_body}
        body = "data: " + json.dumps(payload) + "\n\ndata: [DONE]\n\n"
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, content=body)

    provider = OpenAICompatibleProvider(
        OpenAICompatibleConfig(base_url="http://127.0.0.1:9000/v1", model="local"),
        transport=httpx.MockTransport(handler),
    )
    try:
        with pytest.raises(ProviderError) as error:
            _ = [item async for item in provider.stream([], [])]
        assert error.value.code == "provider_usage"
    finally:
        await provider.close()


def test_provider_detailed_usage_ignores_opaque_cost_and_raw_fields():
    from krim_agent.providers.openai_compatible import _usage_report

    result = _usage_report(
        {
            "prompt_tokens": 10,
            "completion_tokens": 5,
            "total_tokens": 15,
            "prompt_tokens_details": {"cached_tokens": 4, "secret": "do-not-store"},
            "completion_tokens_details": {"reasoning_tokens": 3},
            "cost_usd": 0.9,
            "raw": "do-not-store",
        }
    )
    assert result.cached_input_tokens == 4 and result.reasoning_output_tokens == 3
    assert "do-not-store" not in result.model_dump_json()
    assert "cost_usd" not in result.model_dump()


async def test_persistent_reopen_replays_usage_without_new_provider_call(tmp_path):
    path = tmp_path / "usage.sqlite"
    settings = Settings(database_url=f"sqlite+aiosqlite:///{path}")
    report = ModelUsage(usage=TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7))
    provider = ReportProvider([report])
    async with await PythonAI.create(
        settings=settings, provider=provider, tools=ToolRegistry()
    ) as ai:
        stream = await ai.stream_text("persisted", idempotency_key="persisted")
        result = await stream.collect()
        before = [part async for part in stream.full_stream()]
    reader = ReportProvider([])
    async with await PythonAI.create(
        settings=settings, provider=reader, tools=ToolRegistry()
    ) as reopened:
        replay = await reopened.stream_text(
            "persisted", topic_id=result.run.topic_id, idempotency_key="persisted"
        )
        restored = await replay.collect()
        assert restored.token_usage == result.token_usage and restored.steps == result.steps
        assert [part async for part in replay.full_stream()] == before and reader.calls == 0


async def test_cli_usage_reads_same_persisted_facts(repository, server_url):
    import sys

    settings = Settings(database_url=str(repository.database.engine.url))
    report = ModelUsage(usage=TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7))
    async with await PythonAI.create(
        settings=settings, provider=ReportProvider([report]), tools=ToolRegistry()
    ) as ai:
        result = await ai.generate_text("cli")
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "krim_agent",
            "usage",
            result.run.id,
            "--url",
            server_url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
        assert process.returncode == 0, stderr.decode()
        value = json.loads(stdout)
        assert value["usage_complete"] and value["token_usage"]["total_tokens"] == 7


def test_legacy_events_do_not_invent_zero_usage_or_attempt_count():
    events = [frame(0, "start", {"provider": "legacy"}), frame(1, "end", {"status": "completed"})]
    value = usage_from_events("run", events)
    assert not value.trace_available and value.token_usage is None and value.steps == []


async def test_agent_result_and_workflow_model_consume_persisted_usage(repository):
    from krim_agent.agents import AgentDefinition, AgentManager
    from krim_agent.application.workflow import WorkflowServices
    from krim_agent.workflow import WorkflowEngine, WorkflowGraph, WorkflowNode

    report = ModelUsage(usage=TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7))
    provider = ReportProvider([report])
    manager = AgentManager(repository)
    try:
        handle = await manager.spawn(
            AgentDefinition(name="reported", provider=provider, tools=ToolRegistry())
        )
        result = await handle.execute("agent report", "agent-report")
        assert result.usage_complete and result.token_usage.total_tokens == 7
        assert (await repository.usage_view(result.run_id)).token_usage == result.token_usage
    finally:
        await manager.close()

    async with await PythonAI.create(provider=ReportProvider([report]), tools=ToolRegistry()) as ai:
        run_ids = []
        original = ai.stream_text

        async def track(*args, **kwargs):
            stream = await original(*args, **kwargs)
            run_ids.append(stream.run.id)
            return stream

        ai.stream_text = track
        graph = WorkflowGraph(nodes=[WorkflowNode(id="model", kind="agent")])
        services = WorkflowServices(models={"default": ai})
        result = (
            await WorkflowEngine()
            .execute(graph, "workflow report", services.bindings(graph))
            .wait()
        )
        assert result.status == "completed" and run_ids
        view = await ai.runtime.repository.usage_view(run_ids[0])
        assert view.usage_complete and view.token_usage.total_tokens == 7


async def test_real_python_reader_process_preserves_usage_without_model(tmp_path):
    import sys

    path = tmp_path / "process.sqlite"
    settings = Settings(database_url=f"sqlite+aiosqlite:///{path}")
    report = ModelUsage(usage=TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7))
    async with await PythonAI.create(
        settings=settings, provider=ReportProvider([report]), tools=ToolRegistry()
    ) as ai:
        result = await ai.generate_text("process report")
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "tests/fixtures/usage_reader.py",
        str(path),
        result.run.id,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
    assert process.returncode == 0, stderr.decode()
    value = json.loads(stdout)
    assert value["usage_complete"] and value["token_usage"]["total_tokens"] == 7
    assert value["provider_calls"] == 0


@pytest.mark.parametrize("name", ["cached_input_tokens", "reasoning_output_tokens"])
def test_partial_details_cannot_exceed_reported_total(name):
    with pytest.raises(ValueError):
        TokenUsage.model_validate({"total_tokens": 3, name: 4})
