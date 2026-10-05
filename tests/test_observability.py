"""实际追踪链、默认关闭、净化、导出故障与真实公开入口。"""

import asyncio
import json
import sys
from pathlib import Path

import httpx
import pytest
from alembic import command
from pydantic import SecretStr
from sqlalchemy import select

from krim_agent.agents import AgentCatalog, AgentDefinition, AgentManager
from krim_agent.application.workflow import WorkflowServices
from krim_agent.client import KrimClient
from krim_agent.config import Settings
from krim_agent.observability import JsonlSink, SpanRecord, Tracer
from krim_agent.otel import OtlpJsonSink
from krim_agent.persistence.database import TraceSpanRow
from krim_agent.persistence.migration import migration_config
from krim_agent.persistence.traces import TraceStore
from krim_agent.providers import FakeProvider, ModelText, ModelToolCall
from krim_agent.runtime import ChatRuntime
from krim_agent.sdk import PythonAI, StreamStart
from krim_agent.tools import ToolContext, ToolRegistry, tool
from krim_agent.workflow import WorkflowEngine, WorkflowGraph, WorkflowNode

CANARY = "private-canary-key-abc123"


def tracer(repository, **options):
    return Tracer(TraceStore(repository.database), enabled=True, timeout_seconds=2, **options)


async def records(repository):
    async with repository.database.sessions() as session:
        return [
            SpanRecord.model_validate(row.payload)
            for row in await session.scalars(select(TraceSpanRow))
        ]


async def test_disabled_is_noop_and_does_not_create_writer_or_sink_file(repository, tmp_path):
    runtime = ChatRuntime(
        repository,
        FakeProvider(delay=0),
        ToolRegistry(),
        Settings(observability_jsonl_path=str(tmp_path / "disabled.jsonl")),
    )
    ai = PythonAI(runtime)
    try:
        result = await ai.generate_text(CANARY)
        assert result.run.status == "completed" and not await records(repository)
        assert runtime.tracer.task is None and runtime.tracer.sink is None
        assert not (tmp_path / "disabled.jsonl").exists()
    finally:
        await runtime.close()


async def test_real_model_tool_hierarchy_sdk_and_replay_are_private(repository, tmp_path):
    path = tmp_path / "trace.jsonl"
    observation = tracer(repository, sink=JsonlSink(path))
    tools = ToolRegistry()

    @tool(name="lookup", risk="read")
    async def lookup(value: str) -> str:
        return value

    tools.register(lookup)

    class Provider:
        name = "metadata-only"
        calls = 0

        async def stream(self, messages, _tools):
            self.calls += 1
            if self.calls == 1:
                yield ModelToolCall(id=CANARY, name="lookup", arguments={"value": CANARY})
            else:
                yield ModelText(text=CANARY)

    provider = Provider()
    runtime = ChatRuntime(repository, provider, tools, Settings(), tracer=observation)
    ai = PythonAI(runtime)
    try:
        stream = await ai.stream_text(CANARY, idempotency_key="original")
        result = await stream.collect()
        parts = [part async for part in stream.full_stream()]
        start = next(part for part in parts if isinstance(part, StreamStart))
        assert start.trace_context and start.trace_context.sampled
        assert await observation.flush()
        spans = await observation.store.spans("run", result.run.id)
        assert len(spans) == 4 and [span.kind for span in spans].count("model") == 2
        root = next(span for span in spans if span.kind == "run")
        assert root.span_id == start.trace_context.span_id
        assert all(span.parent_span_id == root.span_id for span in spans if span is not root)
        assert all(span.status == "ok" and span.end_ns >= span.start_ns for span in spans)
        assert (await observation.store.trace(root.trace_id)).complete
        before = [span.model_dump() for span in spans]
        replay = await ai.stream_text(
            CANARY, topic_id=result.run.topic_id, idempotency_key="original"
        )
        assert (await replay.collect()).run.id == result.run.id and provider.calls == 2
        assert [
            span.model_dump() for span in await observation.store.spans("run", result.run.id)
        ] == before
        assert CANARY not in path.read_text() and CANARY not in json.dumps(before)
    finally:
        await runtime.close()
        await observation.close()


async def test_nested_tool_and_agent_run_are_children_of_current_tool(repository):
    observation = tracer(repository)
    manager = AgentManager(repository, tracer=observation)
    catalog = AgentCatalog(manager)
    catalog.register(
        "child",
        AgentDefinition(name="child", provider=FakeProvider(delay=0), tools=ToolRegistry()),
        description="可信子任务",
    )
    outer = ToolRegistry()
    catalog.install_tools(outer)
    try:

        async def allow(_definition, _context):
            return "allow"

        with observation.scope("host", "host", "local-host") as parent:
            result = await outer.invoke(
                "agent_child",
                {"prompt": CANARY},
                ToolContext(outer, policy=allow, tracer=observation),
            )
            assert not result.is_error
        assert await observation.flush()
        spans = (await observation.store.trace(parent.context.trace_id)).spans
        tool_span = next(span for span in spans if span.kind == "tool")
        child_run = next(span for span in spans if span.kind == "run")
        assert tool_span.parent_span_id == parent.context.span_id
        assert child_run.parent_span_id == tool_span.span_id
        assert CANARY not in json.dumps([span.model_dump() for span in spans])
    finally:
        await manager.close()
        await outer.close()
        await observation.close()


async def test_workflow_model_node_shares_trace_and_cold_read_does_not_emit(repository):
    observation = tracer(repository)
    runtime = ChatRuntime(
        repository, FakeProvider(delay=0), ToolRegistry(), Settings(), tracer=observation
    )
    services = WorkflowServices(models={"default": PythonAI(runtime)}, tracer=observation)
    graph = WorkflowGraph(
        nodes=[WorkflowNode(id="secret-node-name", kind="agent")], output_node="secret-node-name"
    )
    try:
        handle = WorkflowEngine(tracer=observation).execute(graph, CANARY, services.bindings(graph))
        assert (await handle.wait()).status == "completed"
        assert await observation.flush()
        spans = await records(repository)
        assert {span.kind for span in spans} == {"workflow", "node", "run", "model"}
        node = next(span for span in spans if span.kind == "node")
        model_run = next(span for span in spans if span.kind == "run")
        assert model_run.parent_span_id == node.span_id
        assert len({span.trace_id for span in spans}) == 1
        assert "secret-node-name" not in json.dumps([span.model_dump() for span in spans])
        assert CANARY not in json.dumps([span.model_dump() for span in spans])
    finally:
        await runtime.close()
        await observation.close()


@pytest.mark.parametrize("rate", [0, 1])
async def test_sampling_is_inherited_and_parallel_roots_are_isolated(repository, rate):
    observation = tracer(repository, sample_rate=rate)

    async def chain(owner):
        with observation.scope("host", "host", owner) as root:
            await asyncio.sleep(0)
            with observation.scope("tool", "host", owner) as child:
                assert child.context.trace_id == root.context.trace_id
            return root.context.trace_id

    one, two = await asyncio.gather(chain("one"), chain("two"))
    assert one != two and await observation.flush()
    assert len(await records(repository)) == (4 if rate else 0)
    await observation.close()


async def test_mutable_payload_and_exception_text_never_reach_store_or_jsonl(repository, tmp_path):
    path = tmp_path / "private.jsonl"
    observation = tracer(repository, sink=JsonlSink(path))
    with pytest.raises(RuntimeError):
        with observation.scope(
            "host",
            "host",
            CANARY,
            {
                "prompt": CANARY,
                "Authorization": SecretStr(CANARY),
                "result": {"key": CANARY},
                "step": 1,
                "path": "/private/" + CANARY,
            },
        ) as span:
            span.record.attributes["headers"] = CANARY
            raise RuntimeError(CANARY)
    assert await observation.flush()
    assert CANARY not in path.read_text()
    saved = (await records(repository))[0]
    assert saved.status == "error" and saved.attributes["step"] == 1
    assert "headers" not in saved.attributes and "prompt" not in saved.attributes
    await observation.close()


@pytest.mark.parametrize("failure", ["store", "export", "timeout"])
async def test_observation_faults_do_not_change_model_result(repository, monkeypatch, failure):
    class Sink:
        async def export(self, _record):
            if failure == "timeout":
                await asyncio.Event().wait()
            else:
                raise RuntimeError(CANARY)

    observation = Tracer(
        TraceStore(repository.database), enabled=True, sink=Sink(), timeout_seconds=0.05
    )
    if failure == "store":

        async def fail(_record):
            raise RuntimeError(CANARY)

        monkeypatch.setattr(observation.store, "write", fail)
    runtime = ChatRuntime(
        repository, FakeProvider(delay=0), ToolRegistry(), Settings(), tracer=observation
    )
    try:
        assert (await PythonAI(runtime).generate_text("normal")).run.status == "completed"
        assert await observation.flush()
        stats = observation.status()
        assert stats.export_failures > 0
        if failure == "store":
            assert stats.store_failures > 0
    finally:
        await runtime.close()
        await observation.close()


async def test_bounded_queue_reports_loss_and_closed_observer_remains_noop(repository):
    observation = Tracer(TraceStore(repository.database), enabled=True, queue_size=1)
    for _ in range(20):
        with observation.scope("host", "host", "bounded"):
            pass
    assert observation.status().dropped > 0 and observation.status().queued <= 1
    assert await observation.flush()
    await observation.close()
    with observation.scope("host", "host", "after-close") as span:
        assert span.record is None


async def test_cancelled_run_and_model_are_cancelled_and_logger_hides_error(repository, caplog):
    class Provider:
        name = "cancelled"
        entered = asyncio.Event()

        async def stream(self, _messages, _tools):
            self.entered.set()
            await asyncio.Event().wait()
            yield ModelText(text=CANARY)

    provider = Provider()
    observation = tracer(repository)
    runtime = ChatRuntime(repository, provider, ToolRegistry(), Settings(), tracer=observation)
    try:
        stream = await PythonAI(runtime).stream_text(CANARY)
        await asyncio.wait_for(provider.entered.wait(), 5)
        await stream.abort()
        assert await observation.flush()
        assert all(span.status == "cancelled" for span in await records(repository))
        assert CANARY not in caplog.text
    finally:
        await runtime.close()
        await observation.close()


async def test_api_client_cli_read_same_private_facts(server_url, runtime, repository):
    runtime.tracer.enabled = True
    result = await PythonAI(runtime).generate_text(CANARY)
    with KrimClient(server_url) as client:
        spans = await asyncio.to_thread(client.run_trace, result.run.id)
        view = await asyncio.to_thread(client.trace, spans[0].trace_id)
        assert view.complete and CANARY not in view.model_dump_json()
        assert (await asyncio.to_thread(client.observability_status)).enabled
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "krim_agent",
        "trace",
        "run",
        result.run.id,
        "--url",
        server_url,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), 20)
    assert process.returncode == 0, stderr.decode()
    assert len(json.loads(stdout)) == len(spans) and CANARY not in stdout.decode()


async def test_otel_payload_retains_ids_and_never_exports_body(repository):
    payloads = []

    async def collector(request):
        assert request.headers["Authorization"] == "Bearer " + CANARY
        payloads.append(json.loads(request.content))
        return httpx.Response(200, json={})

    sink = OtlpJsonSink(
        "http://127.0.0.1/v1/traces",
        headers={"Authorization": SecretStr("Bearer " + CANARY)},
        transport=httpx.MockTransport(collector),
    )
    observation = tracer(repository, sink=sink)
    with observation.scope("host", "host", "owner", {"prompt": CANARY}) as root:
        with observation.scope("tool", "host", "owner", {"step": 1}):
            pass
    assert await observation.flush()
    assert len(payloads) == 2 and CANARY not in json.dumps(payloads)
    exported = [payload["resourceSpans"][0]["scopeSpans"][0]["spans"][0] for payload in payloads]
    assert {span["traceId"] for span in exported} == {root.context.trace_id}
    child = next(span for span in exported if span["name"] == "krim.tool")
    assert child["parentSpanId"] == root.context.span_id
    await observation.close()


async def test_otlp_rejection_counts_failure_and_no_redirect_follow(repository):
    async def collector(_request):
        return httpx.Response(200, json={"partialSuccess": {"rejectedSpans": "1"}})

    sink = OtlpJsonSink("http://127.0.0.1/v1/traces", transport=httpx.MockTransport(collector))
    observation = tracer(repository, sink=sink)
    with observation.scope("host", "host", "owner"):
        pass
    assert await observation.flush() and observation.status().export_failures == 1
    await observation.close()
    with pytest.raises(ValueError):
        OtlpJsonSink("http://example.com/v1/traces")


async def test_recovery_and_downgrade_keep_trace_history(repository):
    observation = tracer(repository)
    with observation.scope("host", "host", "owner") as span:
        pass
    assert await observation.flush()
    async with repository.database.sessions.begin() as session:
        row = await session.get(TraceSpanRow, span.context.span_id)
        row.payload = {**row.payload, "end_ns": None, "status": "running"}
    await observation.store.recover()
    assert (await records(repository))[0].status == "interrupted"
    with pytest.raises(RuntimeError, match="追踪记录"):
        async with repository.database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(migration_config(sync), "0018_context_proposals")
            )
    assert len(await records(repository)) == 1
    await observation.close()


async def test_real_tcp_collector_receives_standard_private_otlp(repository):
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    captured = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            data = self.rfile.read(int(self.headers["Content-Length"]))
            captured.append(json.loads(data))
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"{}")

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    sink = OtlpJsonSink(f"http://127.0.0.1:{server.server_port}/v1/traces")
    observation = tracer(repository, sink=sink)
    runtime = ChatRuntime(
        repository, FakeProvider(delay=0), ToolRegistry(), Settings(), tracer=observation
    )
    try:
        result = await PythonAI(runtime).generate_text(CANARY)
        assert result.run.status == "completed" and await observation.flush()
        assert len(captured) == 2 and CANARY not in json.dumps(captured)
        spans = [payload["resourceSpans"][0]["scopeSpans"][0]["spans"][0] for payload in captured]
        root = next(item for item in spans if item["name"] == "krim.run")
        model = next(item for item in spans if item["name"] == "krim.model")
        assert model["parentSpanId"] == root["spanId"]
        assert len(root["traceId"]) == 32 and len(root["spanId"]) == 16
        assert int(model["endTimeUnixNano"]) >= int(model["startTimeUnixNano"])
    finally:
        await runtime.close()
        await observation.close()
        await asyncio.to_thread(server.shutdown)
        server.server_close()
        thread.join(2)


async def test_exporter_close_failure_is_isolated_and_counted(repository):
    class Sink:
        async def export(self, _record):
            pass

        async def close(self):
            raise RuntimeError(CANARY)

    observation = tracer(repository, sink=Sink())
    with observation.scope("host", "host", "owner"):
        pass
    await observation.close()
    assert observation.status().export_failures == 1


async def test_real_reader_process_does_not_reexecute_or_export(repository):
    observation = tracer(repository)
    runtime = ChatRuntime(
        repository, FakeProvider(delay=0), ToolRegistry(), Settings(), tracer=observation
    )
    try:
        result = await PythonAI(runtime).generate_text(CANARY)
        assert await observation.flush()
        spans = await observation.store.spans("run", result.run.id)
        worker = str(Path(__file__).parent / "fixtures" / "trace_reader.py")
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            worker,
            str(repository.database.engine.url),
            spans[0].trace_id,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), 20)
        assert process.returncode == 0, stderr.decode()
        assert len(json.loads(stdout)["spans"]) == len(spans) and CANARY not in stdout.decode()
        assert len(await records(repository)) == len(spans)
        # 原始 SQL JSON 和观测返回均无 canary，不能由读取时过滤掩盖存储泄漏。
        async with repository.database.sessions() as session:
            raw = [row.payload for row in await session.scalars(select(TraceSpanRow))]
            assert CANARY not in json.dumps(raw)
    finally:
        await runtime.close()
        await observation.close()


async def test_one_sampling_includes_largest_valid_trace_id(repository, monkeypatch):
    monkeypatch.setattr(
        "krim_agent.observability.secrets.token_hex", lambda count: "f" * (count * 2)
    )
    observation = tracer(repository, sample_rate=1)
    with observation.scope("host", "host", "edge") as span:
        assert span.context.sampled
    assert await observation.flush() and len(await records(repository)) == 1
    await observation.close()


async def test_workflow_tool_is_indexed_under_workflow_owner(repository):
    observation = tracer(repository)
    registry = ToolRegistry()

    @tool(name="identity", risk="read")
    async def identity(value: str) -> str:
        return value

    registry.register(identity)
    services = WorkflowServices(tools=registry, tracer=observation)
    graph = WorkflowGraph(
        nodes=[WorkflowNode(id="tool-node", kind="tool", config={"name": "identity"})],
        output_node="tool-node",
    )
    try:
        handle = WorkflowEngine(tracer=observation).execute(
            graph, {"value": CANARY}, services.bindings(graph)
        )
        assert (await handle.wait()).status == "completed" and await observation.flush()
        indexed = await observation.store.spans("workflow", handle.run_id)
        assert {span.kind for span in indexed} == {"workflow", "node", "tool"}
        assert CANARY not in json.dumps([span.model_dump() for span in indexed])
    finally:
        await registry.close()
        await observation.close()
