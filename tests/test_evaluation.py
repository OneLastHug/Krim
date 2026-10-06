"""评估合约、隔离执行、评分覆盖、真实取消与离线报告验收。"""

import asyncio
import json

import pytest
from pydantic import SecretStr

from krim_agent.agents import AgentDefinition, AgentManager
from krim_agent.config import Settings
from krim_agent.evaluation import (
    AgentTarget,
    CallableScorer,
    CallableTarget,
    ContainsScorer,
    EvalCase,
    EvalConfig,
    EvalDataset,
    EvalOutput,
    EvalScore,
    EvalToolCall,
    ExactMatchScorer,
    JsonMatchScorer,
    JsonSchemaScorer,
    SuccessScorer,
    ToolCallsScorer,
    compare_reports,
    evaluate,
    load_dataset,
    load_report,
    rescore,
    save_dataset,
    save_report,
)
from krim_agent.evaluation.runner import EvalCleanupError
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import FakeProvider, ModelText, ModelUsage
from krim_agent.sdk import PythonAI
from krim_agent.tools.builtins import builtin_registry
from krim_agent.usage import TokenUsage


def dataset(*cases):
    return EvalDataset(name="验收数据", cases=list(cases))


def identity_target():
    async def execute(case, _attempt):
        return EvalOutput(value=case.input)

    return CallableTarget("identity", execute)


@pytest.mark.parametrize(
    "content",
    [
        {"name": "empty", "cases": []},
        {"name": "duplicate", "cases": [{"id": "a", "input": "x"}, {"id": "a", "input": "y"}]},
        {"name": "extra", "cases": [{"id": "a", "input": "x", "script": "run_me"}]},
        {"name": "number", "cases": [{"id": "a", "input": float("nan")}]},
        {"name": "large", "cases": [{"id": "a", "input": "x" * 65536}]},
        {"name": "unsafe", "cases": [{"id": "a", "input": 2**54}]},
    ],
)
def test_dataset_rejects_ambiguous_or_unbounded_content(content):
    with pytest.raises(ValueError):
        EvalDataset.model_validate(content)


def test_json_and_jsonl_preserve_content_and_reject_duplicate_keys(tmp_path):
    data = dataset(EvalCase(id="中文", input="你好", expected="你好", metadata={"domain": "工具"}))
    source = tmp_path / "data.json"
    save_dataset(data, source)
    assert load_dataset(source) == data
    line_file = tmp_path / "data.jsonl"
    line_file.write_text(data.cases[0].model_dump_json() + "\n\n", "utf-8")
    assert load_dataset(line_file, name=data.name) == data
    source.write_text('{"name":"one","name":"two","cases":[]}', "utf-8")
    with pytest.raises(ValueError, match="重复键"):
        load_dataset(source)
    line_file.write_text('{"id":"a","id":"b","input":"x"}\n', "utf-8")
    with pytest.raises(ValueError, match="第 1 行"):
        load_dataset(line_file)
    deep = "[" * 35 + "null" + "]" * 35
    line_file.write_text('{"id":"deep","input":' + deep + "}\n", "utf-8")
    with pytest.raises(ValueError):
        load_dataset(line_file)


@pytest.mark.parametrize(
    "scorer,actual,expected,score",
    [
        (ExactMatchScorer(), "好", "好", 1),
        (ExactMatchScorer(), True, 1, 0),
        (ExactMatchScorer(), {"a": [True]}, {"a": [1]}, 0),
        (ExactMatchScorer(normalize_whitespace=True), "a \n b", "a b", 1),
        (ContainsScorer(), "你好世界", ["你好", "世界"], 1),
        (ContainsScorer(), "你好世界", ["你好", "缺失"], 0),
        (JsonMatchScorer(), '{"b":2,"a":1}', {"a": 1, "b": 2.0}, 1),
        (JsonMatchScorer(), '{"a":1,"a":2}', {"a": 2}, 0),
        (JsonMatchScorer(), "NaN", 1, 0),
        (
            JsonSchemaScorer(
                {"type": "object", "required": ["n"], "properties": {"n": {"type": "integer"}}}
            ),
            '{"n":3}',
            None,
            1,
        ),
        (JsonSchemaScorer({"type": "integer"}), '"3"', None, 0),
    ],
)
async def test_rule_scores_have_real_semantics(scorer, actual, expected, score):
    value = await scorer.score(
        EvalCase(id="one", input="x", expected=expected), EvalOutput(value=actual)
    )
    assert value.status == "scored" and value.value == score


def test_schema_scorer_rejects_network_reference():
    with pytest.raises(ValueError, match="本地引用"):
        JsonSchemaScorer({"$ref": "https://invalid.example/schema"})


async def test_tool_grading_checks_parameters_success_order_and_duplicate_calls():
    observed = [
        EvalToolCall(call_id="a", name="add", arguments={"left": 2.0}, ok=True),
        EvalToolCall(call_id="b", name="add", arguments={"left": 3.0}, ok=True),
    ]
    case = EvalCase(
        id="tools",
        input="x",
        metadata={"expected_tools": ["add", {"name": "add", "arguments": {"left": 2}}]},
    )
    assert (await ToolCallsScorer().score(case, EvalOutput(tools=observed))).value == 0
    assert (await ToolCallsScorer(ordered=False).score(case, EvalOutput(tools=observed))).value == 1
    observed[0].ok = False
    assert (await ToolCallsScorer(ordered=False).score(case, EvalOutput(tools=observed))).value == 0
    assert (await ToolCallsScorer().score(case, EvalOutput())).status == "skipped"


async def test_missing_reference_and_partial_score_coverage_never_pass():
    data = dataset(EvalCase(id="no-ref", input="x"))
    report = await evaluate(data, identity_target(), scorers=[SuccessScorer(), ExactMatchScorer()])
    assert report.results[0].status == "unscored"
    assert report.summary.passed == 0 and report.summary.pass_rate == 0
    assert report.summary.mean_scores == {"success": 1.0, "exact_match": None}
    assert report.summary.scored_counts == {"success": 1, "exact_match": 0}


async def test_bounded_workers_order_repetitions_and_snapshot_isolation():
    active = peak = 0
    keys = set()

    async def execute(case, attempt):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        keys.add(attempt.idempotency_key)
        try:
            case.metadata["mutated"] = True
            await asyncio.sleep(0.002 * (4 - int(case.id)))
            return EvalOutput(value=case.input)
        finally:
            active -= 1

    data = dataset(
        *(EvalCase(id=str(index), input=str(index), expected=str(index)) for index in range(4))
    )

    async def mutate(case, output):
        case.expected = "changed"
        output.value = "changed"
        return 1.0

    report = await evaluate(
        data,
        CallableTarget("bounded", execute),
        scorers=[CallableScorer("isolated", mutate), ExactMatchScorer()],
        config=EvalConfig(concurrency=3, repetitions=2),
    )
    assert peak == 3 and active == 0 and len(keys) == 8
    assert [(row.case_id, row.repetition) for row in report.results] == [
        (str(index), repeat) for index in range(4) for repeat in (1, 2)
    ]
    assert report.summary.passed == 8
    assert all(case.metadata == {} for case in data.cases + report.dataset.cases)
    assert report.dataset.fingerprint == data.fingerprint


@pytest.mark.parametrize("kind", ["raise", "timeout", "nan", "wrong-name", "string"])
async def test_custom_score_failures_are_errors_and_hide_exception_body(kind):
    async def score(_case, _output):
        if kind == "raise":
            raise RuntimeError("PRIVATE-SCORER-CANARY")
        if kind == "timeout":
            await asyncio.Event().wait()
        if kind == "nan":
            return float("nan")
        if kind == "string":
            return "1"
        return EvalScore(name="wrong", value=1.0)

    report = await evaluate(
        dataset(EvalCase(id="one", input="x")),
        identity_target(),
        scorers=[CallableScorer("custom", score)],
        config=EvalConfig(scorer_timeout_seconds=0.01),
    )
    assert report.summary.errors == 1
    assert report.summary.mean_scores["custom"] is None
    assert "PRIVATE-SCORER-CANARY" not in report.model_dump_json()


async def test_float_threshold_and_duplicate_scorer_rejection():
    data = dataset(EvalCase(id="one", input="x"))
    selected = [CallableScorer("partial", lambda _case, _output: 0.75)]
    passing = await evaluate(
        data, identity_target(), scorers=selected, config=EvalConfig(threshold=0.7)
    )
    failing = await evaluate(
        data, identity_target(), scorers=selected, config=EvalConfig(threshold=0.8)
    )
    assert passing.summary.passed == 1 and failing.summary.failed == 1
    with pytest.raises(ValueError, match="唯一"):
        await evaluate(data, identity_target(), scorers=[ExactMatchScorer(), ExactMatchScorer()])


async def test_target_failures_timeouts_and_other_samples_continue():
    cleaned = asyncio.Event()

    async def execute(case, _attempt):
        if case.id == "raise":
            raise RuntimeError("PRIVATE-TARGET-CANARY")
        if case.id == "wait":
            try:
                await asyncio.Event().wait()
            finally:
                cleaned.set()
        return EvalOutput(value=case.input)

    data = dataset(
        *(EvalCase(id=key, input=key, expected=key) for key in ("raise", "wait", "good"))
    )
    report = await evaluate(
        data,
        CallableTarget("failure", execute),
        config=EvalConfig(timeout_seconds=0.02, concurrency=2),
    )
    assert [row.status for row in report.results] == ["error", "timed_out", "passed"]
    assert cleaned.is_set() and report.summary.scored_counts["exact_match"] == 1
    assert "PRIVATE-TARGET-CANARY" not in report.model_dump_json()


async def test_external_cancel_waits_for_all_workers_and_propagates():
    active = 0
    entered = asyncio.Event()

    async def execute(_case, _attempt):
        nonlocal active
        active += 1
        if active == 3:
            entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0.01)
            active -= 1

    data = dataset(*(EvalCase(id=str(index), input="x") for index in range(5)))
    task = asyncio.create_task(
        evaluate(data, CallableTarget("cancel", execute), config=EvalConfig(concurrency=3))
    )
    await asyncio.wait_for(entered.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert active == 0


async def test_cleanup_failure_aborts_batch_and_cancels_other_workers():
    entered = asyncio.Event()
    other_closed = asyncio.Event()

    async def execute(case, _attempt):
        if case.id == "broken":
            await entered.wait()
            raise EvalCleanupError("cleanup")
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            other_closed.set()

    data = dataset(EvalCase(id="broken", input="x"), EvalCase(id="other", input="y"))
    with pytest.raises(EvalCleanupError):
        await evaluate(data, CallableTarget("cleanup", execute), config=EvalConfig(concurrency=2))
    assert other_closed.is_set()


async def test_pythonai_real_tool_runs_are_isolated_and_read_result_is_read_only(runtime):
    ai = PythonAI(runtime)
    data = dataset(
        EvalCase(
            id="add",
            input="/add 2 3",
            expected='[fake] 工具结果：{"sum": 5.0}',
            metadata={"expected_tools": [{"name": "add", "arguments": {"left": 2, "right": 3}}]},
        )
    )
    report = await ai.evaluate(
        data,
        scorers=[ExactMatchScorer(), ToolCallsScorer()],
        config=EvalConfig(repetitions=2, concurrency=2),
    )
    assert report.summary.passed == 2 and report.summary.total_tokens is None
    outputs = [row.output for row in report.results]
    assert len({out.run_id for out in outputs}) == len({out.topic_id for out in outputs}) == 2
    for output in outputs:
        assert output.model_steps == 2 and output.tools[0].result == {"sum": 5.0}
        restored = await ai.read_result(output.run_id)
        assert len(restored.audits) == 1 and len(restored.model_steps) == 2
        assert len(await runtime.repository.audits(output.run_id)) == 1
    rescored = await rescore(report, scorers=[ToolCallsScorer()])
    assert rescored.summary.passed == 2 and rescored.parent_report_id == report.id
    assert all([len(await runtime.repository.audits(output.run_id)) == 1 for output in outputs])
    await ai.close()
    with pytest.raises(ServiceError, match="关闭"):
        await ai.evaluate(data)


class PausedProvider:
    name = "eval-paused"

    def __init__(self):
        self.entered = asyncio.Event()
        self.closed = 0

    async def stream(self, _messages, _tools):
        self.entered.set()
        try:
            yield ModelText(text="partial")
            await asyncio.Event().wait()
        finally:
            self.closed += 1


async def test_generation_timeout_stops_real_production_and_keeps_partial_evidence():
    provider = PausedProvider()
    async with await PythonAI.create(provider=provider) as ai:
        data = dataset(EvalCase(id="wait", input="x", expected="partial"))
        task = asyncio.create_task(ai.evaluate(data, config=EvalConfig(timeout_seconds=0.2)))
        await asyncio.wait_for(provider.entered.wait(), 5)
        report = await task
        output = report.results[0].output
        assert report.summary.timed_out == 1 and report.summary.passed == 0
        assert output.status == "aborted" and output.value == "partial"
        assert (await ai.runtime.repository.get_run(output.run_id)).status == "aborted"
        assert ai.runtime.production_task(output.run_id).done() and provider.closed == 1


async def test_cancel_during_start_waits_for_run_identity_then_stops(monkeypatch):
    provider = PausedProvider()
    async with await PythonAI.create(provider=provider) as ai:
        original = ai.runtime.start
        start_entered, release = asyncio.Event(), asyncio.Event()
        captured = []

        async def delayed(*args, **kwargs):
            run = await original(*args, **kwargs)
            captured.append(run)
            start_entered.set()
            await release.wait()
            return run

        monkeypatch.setattr(ai.runtime, "start", delayed)
        task = asyncio.create_task(ai.evaluate(dataset(EvalCase(id="start", input="x"))))
        await asyncio.wait_for(start_entered.wait(), 5)
        task.cancel()
        await asyncio.sleep(0.01)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert (await ai.runtime.repository.get_run(captured[0].id)).status == "aborted"
        assert ai.runtime.production_task(captured[0].id).done()


async def test_read_result_refuses_live_run():
    async with await PythonAI.create(provider=PausedProvider()) as ai:
        stream = await ai.stream_text("x")
        with pytest.raises(ServiceError) as error:
            await ai.read_result(stream.run.id)
        assert error.value.code == "result_not_terminal"
        await stream.abort()
        assert (await ai.read_result(stream.run.id)).run.status == "aborted"


@pytest.mark.parametrize("allow", [True, False])
async def test_real_agent_keeps_permissions_and_releases_persistent_handles(repository, allow):
    manager = AgentManager(repository, max_concurrent=2)
    tools = builtin_registry()

    async def policy(_definition, _context):
        return "allow" if allow else "deny"

    definition = AgentDefinition(
        name="calc", provider=FakeProvider(delay=0), tools=tools, persistent=True, policy=policy
    )
    data = dataset(EvalCase(id="add", input="/add 2 3", metadata={"expected_tools": ["add"]}))
    try:
        report = await evaluate(
            data,
            AgentTarget(manager, definition),
            scorers=[ToolCallsScorer()],
            config=EvalConfig(repetitions=2, concurrency=2),
        )
        assert report.summary.passed == (2 if allow else 0)
        assert len({row.output.agent_id for row in report.results}) == 2
        assert not manager._handles and await manager.store.list() == []
        assert all(row.output.tools[0].ok is allow for row in report.results)
    finally:
        await manager.close()
        await tools.close()


async def test_real_persistent_agent_external_cancel_closes_provider_and_inventory(repository):
    manager = AgentManager(repository)
    provider = PausedProvider()
    tools = builtin_registry()
    definition = AgentDefinition(name="waiting", provider=provider, tools=tools, persistent=True)
    task = asyncio.create_task(
        evaluate(
            dataset(EvalCase(id="a", input="x")),
            AgentTarget(manager, definition),
            scorers=[SuccessScorer()],
        )
    )
    try:
        await asyncio.wait_for(provider.entered.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert provider.closed == 1 and not manager._handles
        assert await manager.store.list() == []
    finally:
        await manager.close()
        await tools.close()


async def test_reported_partial_and_unknown_usage_never_become_zero_or_estimate():
    class Provider:
        name = "eval-usage"
        calls = 0

        async def stream(self, messages, _tools):
            self.calls += 1
            text = messages[-1].content
            yield ModelText(text=text)
            if text == "complete":
                yield ModelUsage(usage=TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7))
            elif text == "partial":
                yield ModelUsage(usage=TokenUsage(input_tokens=3))

    provider = Provider()
    async with await PythonAI.create(provider=provider) as ai:
        data = dataset(
            *(
                EvalCase(id=word, input=word, expected=word)
                for word in ("complete", "partial", "unknown")
            )
        )
        report = await ai.evaluate(data)
        assert report.summary.passed == 3 and provider.calls == 3
        assert report.summary.input_tokens is report.summary.total_tokens is None
        assert report.summary.reported_tokens == {
            "input_tokens": 8,
            "output_tokens": 2,
            "total_tokens": 7,
        }
        assert report.summary.reported_cases == 2 and not report.summary.usage_complete
        rescored = await rescore(report)
        assert (
            provider.calls == 3
            and rescored.summary.reported_tokens == report.summary.reported_tokens
        )


async def test_report_config_excludes_keys_endpoint_paths_and_keeps_trace():
    settings = Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        api_key=SecretStr("PRIVATE-API-CANARY"),
        provider_api_key=SecretStr("PRIVATE-MODEL-CANARY"),
        provider_base_url="https://invalid.example/PRIVATE-ENDPOINT-CANARY",
        observability_enabled=True,
    )
    async with await PythonAI.create(settings=settings, provider=FakeProvider(delay=0)) as ai:
        report = await ai.evaluate(
            dataset(EvalCase(id="echo", input="hello")), scorers=[SuccessScorer()]
        )
        payload = report.model_dump_json()
        assert not any(
            word in payload
            for word in ("PRIVATE-API-CANARY", "PRIVATE-MODEL-CANARY", "PRIVATE-ENDPOINT-CANARY")
        )
        assert report.results[0].output.trace_context is not None
        await ai.runtime.tracer.flush(1)
        assert (
            await ai.runtime.tracer.store.trace(report.results[0].output.trace_context.trace_id)
        ).spans


async def test_reports_validate_facts_atomic_write_and_offline_regression(tmp_path, monkeypatch):
    data = dataset(EvalCase(id="one", input="good", expected="good"))
    before = await evaluate(data, identity_target())

    async def fail(_case, _attempt):
        return EvalOutput(status="failed", code="test_failure")

    after = await evaluate(data, CallableTarget("new-version", fail, version="2"))
    comparison = compare_reports(before, after)
    assert comparison.compatible and len(comparison.regressions) == 1
    assert comparison.pass_rate_delta == -1
    path = tmp_path / "nested" / "report.json"
    save_report(before, path)
    assert load_report(path) == before
    import krim_agent.evaluation.files as files

    def denied(_source, _destination):
        raise OSError("disk failure")

    monkeypatch.setattr(files.os, "replace", denied)
    with pytest.raises(OSError):
        save_report(after, path)
    assert load_report(path) == before
    assert list(path.parent.iterdir()) == [path]
    tampered = json.loads(path.read_text("utf-8"))
    tampered["summary"]["passed"] = 0
    path.write_text(json.dumps(tampered), "utf-8")
    with pytest.raises(ValueError, match="汇总"):
        load_report(path)
    for invalid in (
        before.model_copy(update={"results": []}),
        before.model_copy(update={"dataset_fingerprint": "bad"}),
    ):
        with pytest.raises(ValueError):
            save_report(invalid, path)


async def test_comparison_rejects_changed_dataset_and_scorer_config():
    data = dataset(EvalCase(id="one", input="good", expected="good"))
    report = await evaluate(data, identity_target())
    changed = await evaluate(
        dataset(EvalCase(id="one", input="other", expected="other")), identity_target()
    )
    assert not compare_reports(report, changed).compatible
    rescored = await rescore(report, scorers=[ExactMatchScorer(normalize_whitespace=True)])
    assert not compare_reports(report, rescored).compatible
    threshold = await rescore(report, threshold=0.5)
    assert not compare_reports(report, threshold).compatible
