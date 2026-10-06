"""旧报告、负例契约、指标策略与真正同话题多轮/恢复。"""

import asyncio
import json

import pytest

from krim_agent.config import Settings
from krim_agent.evaluation import (
    CallableScorer,
    CallableTarget,
    ConversationScorer,
    EvalCase,
    EvalConfig,
    EvalDataset,
    EvalMetricPolicy,
    EvalOutput,
    EvalScore,
    EvalTurn,
    GenerationTarget,
    SuccessScorer,
    evaluate,
    load_report,
    rescore,
    save_dataset,
)
from krim_agent.evaluation.jobs import EvalJobSpec
from krim_agent.evaluation.types import fingerprint, weighted_score
from krim_agent.providers import FakeProvider, ModelText, ModelUsage
from krim_agent.sdk import PythonAI
from krim_agent.usage import TokenUsage


def dataset(case=None):
    return EvalDataset(name="策略", cases=[case or EvalCase(id="one", input="a", expected="a")])


async def echo(case, _attempt):
    return EvalOutput(value=case.input)


async def test_old_dataset_job_fingerprint_and_schema_one_report_remain_readable(tmp_path):
    old_data = {
        "name": "策略",
        "version": "1",
        "cases": [{"id": "one", "input": "a", "expected": "a", "metadata": {}, "tags": []}],
    }
    data = EvalDataset.model_validate(old_data)
    assert data.fingerprint == fingerprint(old_data)
    target = CallableTarget("echo", echo)
    report = await evaluate(data, target)
    old = report.model_dump(mode="json")
    old["dataset"] = old_data
    for name in ("metrics", "aggregate_threshold"):
        old["config"].pop(name)
    for result in old["results"]:
        result["output"].pop("turns")
        for score in result["scores"]:
            for name in ("evidence", "run_id", "topic_id", "code", "token_usage", "usage_complete"):
                score.pop(name)
    path = tmp_path / "legacy.json"
    path.write_text(json.dumps(old, ensure_ascii=False), encoding="utf-8")
    assert load_report(path) == report
    spec = EvalJobSpec(
        dataset=data, target=target.describe(), config=report.config, scorers=report.scorers
    )
    assert spec.fingerprint == fingerprint(
        {
            "dataset": old_data,
            "target": old["target"],
            "config": old["config"],
            "scorers": old["scorers"],
        }
    )
    changed = data.model_copy(deep=True)
    changed.cases[0].expected_status = "failed"
    assert changed.fingerprint != data.fingerprint


@pytest.mark.parametrize(
    "observed,code,status",
    [("failed", "denied", "passed"), ("failed", "other", "failed"), ("completed", None, "failed")],
)
async def test_expected_negative_status_and_code_are_native(observed, code, status):
    async def target(_case, _attempt):
        return EvalOutput(status=observed, code=code)

    data = dataset(
        EvalCase(id="negative", input="x", expected_status="failed", expected_code="denied")
    )
    report = await evaluate(data, CallableTarget("negative", target), scorers=[SuccessScorer()])
    assert report.results[0].status == status


@pytest.mark.parametrize("mode,expected", [("raise", "error"), ("timeout", "timed_out")])
async def test_framework_failures_cannot_be_counted_as_expected_failures(mode, expected):
    async def target(_case, attempt):
        attempt.output = EvalOutput(status="failed", code="denied")
        if mode == "raise":
            raise RuntimeError("secret exception text")
        await asyncio.Event().wait()

    report = await evaluate(
        dataset(EvalCase(id="bad", input="x", expected_status="failed", expected_code="denied")),
        CallableTarget("framework", target),
        scorers=[SuccessScorer()],
        config=EvalConfig(timeout_seconds=0.02),
    )
    assert report.results[0].status == expected and report.results[0].scores[0].status == "skipped"
    assert "secret exception text" not in report.model_dump_json()


async def test_required_optional_weighted_gates_and_skips_are_explicit():
    scorers = [CallableScorer("quality", lambda *_: 0.9), CallableScorer("style", lambda *_: 0.3)]
    config = EvalConfig(
        metrics={
            "quality": EvalMetricPolicy(threshold=0.8, weight=3),
            "style": EvalMetricPolicy(required=False),
        },
        aggregate_threshold=0.7,
    )
    report = await evaluate(dataset(), CallableTarget("echo", echo), scorers=scorers, config=config)
    assert report.summary.passed == 1
    assert weighted_score(report.results[0].scores, config) == pytest.approx(0.75)
    failed = await rescore(
        report, scorers=scorers, config=config.model_copy(update={"aggregate_threshold": 0.8})
    )
    assert failed.summary.failed == 1
    skipped = [
        scorers[0],
        CallableScorer("style", lambda *_: EvalScore(name="style", status="skipped")),
    ]
    strict = await evaluate(dataset(), CallableTarget("echo", echo), scorers=skipped, config=config)
    assert strict.summary.unscored == 1
    relaxed = config.model_copy(deep=True)
    relaxed.metrics["style"].allow_skipped = True
    optional = await rescore(strict, scorers=skipped, config=relaxed)
    assert optional.summary.passed == 1 and optional.summary.scored_counts["style"] == 0
    error = [
        scorers[0],
        CallableScorer("style", lambda *_: EvalScore(name="style", status="error")),
    ]
    assert (await rescore(strict, scorers=error, config=relaxed)).summary.errors == 1
    with pytest.raises(ValueError):
        EvalMetricPolicy(required=True, allow_skipped=True)
    with pytest.raises(ValueError):
        await evaluate(
            dataset(),
            CallableTarget("echo", echo),
            config=EvalConfig(metrics={"unknown": EvalMetricPolicy()}),
        )


async def test_conversation_really_preserves_context_and_aggregates_all_turn_usage():
    class ContextProvider:
        name = "conversation-context"

        async def stream(self, messages, _tools):
            users = [m.content for m in messages if m.role == "user"]
            if len(users) == 1:
                yield ModelText(text="已记住北京")
            else:
                assert users[0] == "记住北京" and users[-1] == "是哪座城"
                assert any(m.role == "assistant" and m.content == "已记住北京" for m in messages)
                yield ModelText(text="北京")
            yield ModelUsage(usage=TokenUsage(input_tokens=10, output_tokens=2, total_tokens=12))

    data = dataset(
        EvalCase(
            id="memory",
            input="记住北京",
            expected="北京",
            turns=[EvalTurn(input="是哪座城", expected="北京")],
        )
    )
    async with await PythonAI.create(provider=ContextProvider()) as ai:
        report = await ai.evaluate(
            data,
            conversation=True,
            scorers=[ConversationScorer()],
            config=EvalConfig(repetitions=2, concurrency=2),
        )
        assert report.summary.passed == 2 and report.summary.total_tokens == 48
        assert len({r.output.topic_id for r in report.results}) == 2
        for result in report.results:
            assert len(result.output.turns) == 2 and result.output.model_steps == 2
            assert result.output.token_usage.total_tokens == 24 and result.output.usage_complete
            assert [t.output.token_usage.total_tokens for t in result.output.turns] == [12, 12]
            assert len({t.output.topic_id for t in result.output.turns}) == 1


async def test_conversation_pause_between_turns_reuses_first_real_run_and_tools(
    tmp_path, monkeypatch
):
    class Counting(FakeProvider):
        calls = 0

        async def stream(self, messages, tools):
            self.calls += 1
            async for value in super().stream(messages, tools):
                yield value

    provider = Counting(delay=0)
    entered = asyncio.Event()
    original = GenerationTarget.run

    async def delayed(self, case, attempt):
        value = await original(self, case, attempt)
        if case.input == "/add 2 3" and not entered.is_set():
            entered.set()
            await asyncio.Event().wait()
        return value

    monkeypatch.setattr(GenerationTarget, "run", delayed)
    data = dataset(
        EvalCase(
            id="tools",
            input="/add 2 3",
            expected="[fake] 收到：下一轮",
            turns=[EvalTurn(input="下一轮")],
        )
    )
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path / 'turns.sqlite'}")
    async with await PythonAI.create(provider=provider, settings=settings) as ai:
        handle = await ai.start_evaluation(data, conversation=True)
        await asyncio.wait_for(entered.wait(), 5)
        assert provider.calls == 2
        await handle.pause()
        item = (await handle.items()).items[0]
        assert item.phase == "uncertain" and len(item.output.turns) == 1
        assert item.output.status == "interrupted"
        first_run = item.output.turns[0].output.run_id
        await ai.resume_evaluation(handle.id, conversation=True)
        assert (await handle.wait()).status == "completed"
        report = await handle.report()
        assert report.summary.passed == 1 and provider.calls == 3
        assert report.results[0].output.turns[0].output.run_id == first_run
        assert len(report.results[0].output.tools) == 1
        assert len(await ai.runtime.repository.audits(first_run)) == 1


async def test_conversation_cancel_mid_turn_keeps_partial_and_stops_actual_production():
    entered, closed = asyncio.Event(), asyncio.Event()

    class Partial:
        name = "multi-partial"

        async def stream(self, messages, _tools):
            if len([m for m in messages if m.role == "user"]) == 1:
                yield ModelText(text="first")
                return
            yield ModelText(text="partial")
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                closed.set()

    async with await PythonAI.create(provider=Partial()) as ai:
        handle = await ai.start_evaluation(
            dataset(EvalCase(id="cancel", input="a", turns=[EvalTurn(input="b")])),
            conversation=True,
        )
        await asyncio.wait_for(entered.wait(), 5)
        await handle.cancel()
        output = (await handle.items()).items[0].output
        assert closed.is_set() and len(output.turns) == 2
        assert output.value == "partial" and output.status == "aborted"
        assert (await ai.runtime.repository.get_run(output.run_id)).status == "aborted"


async def test_rest_and_cli_expose_conversation_and_metric_policy(api, tmp_path):
    data = dataset(
        EvalCase(
            id="cli",
            input="first",
            expected="[fake] 收到：second",
            turns=[EvalTurn(input="second", expected="[fake] 收到：second")],
        )
    )
    response = await api.post(
        "/v1/evals",
        json={
            "target": "conversation",
            "dataset": data.model_dump(mode="json"),
            "scorers": ["conversation-turns"],
        },
    )
    assert response.status_code == 202
    job_id = response.json()["id"]
    from krim_agent.evaluation.remote import AsyncEvalClient

    client = AsyncEvalClient(http=api)
    assert (await client.wait(job_id)).status == "completed"
    assert (await client.report(job_id)).summary.passed == 1
    path, report, policy = (
        tmp_path / "data.json",
        tmp_path / "report.json",
        tmp_path / "policy.json",
    )
    save_dataset(data, path)
    policy.write_text('{"metrics":{"conversation_turns":{"threshold":0.5}}}', encoding="utf-8")
    import sys

    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "krim_agent",
        "eval",
        "run",
        str(path),
        "--target",
        "conversation",
        "--scorer",
        "conversation-turns",
        "--policy",
        str(policy),
        "--database",
        str(tmp_path / "cli.sqlite"),
        "--output",
        str(report),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), 20)
    assert process.returncode == 0, (stdout + stderr).decode()
    assert load_report(report).summary.passed == 1


async def test_single_turn_target_does_not_silently_ignore_conversation():
    async with await PythonAI.create(provider=FakeProvider(delay=0)) as ai:
        report = await ai.evaluate(
            dataset(EvalCase(id="turn", input="first", turns=[EvalTurn(input="second")])),
            scorers=[SuccessScorer()],
        )
        assert report.summary.errors == 1


async def test_unknown_grading_keeps_target_output_and_resumes_without_reexecuting(repository):
    from krim_agent.evaluation import EvalScoringUnknown
    from krim_agent.evaluation.scorers import RuleScorer
    from krim_agent.evaluation.service import EvaluationService

    calls = 0

    async def target(case, _attempt):
        nonlocal calls
        calls += 1
        return EvalOutput(value=case.input)

    class ExternalGrade(RuleScorer):
        def __init__(self):
            super().__init__("external")
            self.ready = False

        async def score_with_attempt(self, _case, _output, attempt):
            if "external" not in attempt.score_bindings:
                await attempt.save_score_binding("external", {"receipt": "grade-one"})
            if not self.ready:
                raise EvalScoringUnknown("等待原评分结果")
            assert attempt.score_bindings["external"]["receipt"] == "grade-one"
            return self.result(True)

    grade = ExternalGrade()
    service = EvaluationService(repository.database)
    try:
        handle = await service.start(dataset(), CallableTarget("external", target), scorers=[grade])
        assert (await handle.wait()).status == "interrupted"
        item = (await handle.items()).items[0]
        assert item.phase == "grading_uncertain" and item.output.value == "a"
        grade.ready = True
        await handle.resume()
        assert (await handle.wait()).status == "completed"
        assert calls == 1 and (await handle.report()).summary.passed == 1
    finally:
        await service.close()
