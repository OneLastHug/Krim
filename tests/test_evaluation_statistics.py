"""统计真实覆盖、确定性筛选、不同目标矩阵与有界持久编排。"""

import asyncio
import json
import sys

import pytest

from krim_agent.evaluation import (
    CallableTarget,
    EvalCase,
    EvalConfig,
    EvalDataset,
    EvalOutput,
    analyze_report,
    compare_reports,
    evaluate,
    evaluate_matrix,
    save_report,
    select_dataset,
    start_matrix,
)
from krim_agent.evaluation.service import EvaluationService
from krim_agent.evaluation.types import summarize


def data():
    return EvalDataset(
        name="statistics",
        cases=[
            EvalCase(id="a", input="a", expected="a", tags=["group", "shared"]),
            EvalCase(id="b", input="b", expected="wrong", tags=["shared"]),
            EvalCase(id="c", input="c", tags=["unknown"]),
        ],
    )


def target(name="echo", version="1"):
    async def run(case, _attempt):
        return EvalOutput(value=case.input)

    return CallableTarget(name, run, version=version)


async def test_statistics_recomputes_original_counts_coverage_dispersion_and_quantiles(tmp_path):
    report = await evaluate(data(), target())
    rows = [
        row.model_copy(update={"duration_ms": float((i + 1) * 10), "scoring_ms": float(i + 1)})
        for i, row in enumerate(report.results)
    ]
    controlled = report.model_copy(
        update={"results": rows, "summary": summarize(rows, report.scorers)}
    )
    stats = analyze_report(controlled)
    assert stats.overall.total == 3 and stats.overall.pass_rate == pytest.approx(1 / 3)
    metric = stats.overall.metrics["exact_match"]
    assert (metric.scored, metric.skipped, metric.errors) == (2, 1, 0)
    assert metric.coverage == pytest.approx(2 / 3) and metric.mean == metric.stddev == 0.5
    assert stats.overall.execution.p50_ms == 20 and stats.overall.execution.p90_ms == 28
    assert stats.tags["shared"].total == 2 and stats.tags["group"].passed == 1
    assert stats.tags["unknown"].metrics["exact_match"].mean is None
    assert stats.grading.runs == 0 and stats.grading.total_tokens is None
    path = tmp_path / "report.json"
    save_report(controlled, path)
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "krim_agent",
        "eval",
        "stats",
        str(path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
    assert process.returncode == 0, stderr.decode()
    assert json.loads(stdout)["overall"]["execution"]["p90_ms"] == 28
    with pytest.raises(ValueError):
        analyze_report(
            report.model_copy(update={"summary": report.summary.model_copy(update={"passed": 3})})
        )


def test_selection_preserves_identity_order_seed_provenance_and_original_data():
    original = EvalDataset(
        name="sample",
        cases=[
            EvalCase(id=str(i), input=i, expected=i, tags=["even" if i % 2 == 0 else "odd"])
            for i in range(20)
        ],
    )
    before = original.fingerprint
    first = select_dataset(original, tags=["even"], limit=4, seed=42)
    second = select_dataset(original, tags=["even"], limit=4, seed=42)
    other = select_dataset(original, tags=["even"], limit=4, seed=43)
    assert first == second and first.fingerprint != original.fingerprint
    assert first.fingerprint != other.fingerprint
    assert first.selection["source_fingerprint"] == before and first.selection["seed"] == 42
    ids = [int(case.id) for case in first.cases]
    assert ids == sorted(ids) and len(ids) == 4 and all(i % 2 == 0 for i in ids)
    assert original.fingerprint == before and len(original.cases) == 20
    assert [c.id for c in select_dataset(original, tags=["even"], case_ids=["0", "1"]).cases] == [
        "0"
    ]
    for options in ({"case_ids": ["missing"]}, {"tags": ["missing"]}, {"seed": True}, {"limit": 0}):
        with pytest.raises(ValueError):
            select_dataset(original, **options)


async def test_target_matrix_uses_same_dataset_and_configuration_for_comparison():
    data_set = EvalDataset(name="matrix", cases=[EvalCase(id="one", input="a", expected="a")])

    async def changed(_case, _attempt):
        return EvalOutput(value="b")

    reports = await evaluate_matrix(
        data_set,
        {"before": target(version="1"), "after": CallableTarget("echo", changed, version="2")},
        config=EvalConfig(repetitions=2),
    )
    comparison = compare_reports(reports["before"], reports["after"])
    assert comparison.compatible and len(comparison.regressions) == 2
    assert comparison.pass_rate_delta == -1
    assert reports["before"].id != reports["after"].id


async def test_durable_matrix_uses_shared_concurrency_and_idempotent_job_keys(repository):
    active, maximum, calls = 0, 0, 0

    async def slow(case, _attempt):
        nonlocal active, maximum, calls
        active += 1
        maximum = max(maximum, active)
        calls += 1
        try:
            await asyncio.sleep(0.01)
            return EvalOutput(value=case.input)
        finally:
            active -= 1

    selected = CallableTarget("matrix", slow)
    service = EvaluationService(repository.database, max_concurrency=1)
    try:
        handles = await start_matrix(
            service,
            data(),
            {"first": selected, "second": selected},
            config=EvalConfig(concurrency=3),
            request_key="matrix-one",
        )
        await asyncio.gather(*(handle.wait() for handle in handles.values()))
        assert maximum == 1 and calls == 6
        again = await start_matrix(
            service,
            data(),
            {"first": selected, "second": selected},
            config=EvalConfig(concurrency=3),
            request_key="matrix-one",
        )
        assert {k: h.id for k, h in handles.items()} == {k: h.id for k, h in again.items()}
        assert calls == 6
    finally:
        await service.close()


async def test_remote_statistics_are_read_only_and_public_typed_client_can_read(api):
    from krim_agent.evaluation.jobs import EvalRemoteStartRequest
    from krim_agent.evaluation.remote import AsyncEvalClient

    async with AsyncEvalClient(http=api) as client:
        handle = await client.start(EvalRemoteStartRequest(dataset=data(), scorers=["success"]))
        await client.wait(handle.id)
        before = (await client.get(handle.id)).event_count
        stats = await client.statistics(handle.id)
        assert stats.overall.total == 3 and stats.overall.passed == 3
        assert (await client.get(handle.id)).event_count == before
    assert not api.is_closed
