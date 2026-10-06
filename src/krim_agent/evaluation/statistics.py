"""从完整原始报告重算覆盖/标签/分位与评分用量，不猜测缺失账单。"""

from __future__ import annotations

import statistics
from collections.abc import Sequence

from pydantic import Field

from krim_agent.protocol.models import StrictModel
from krim_agent.usage import USAGE_FIELDS, UsageField

from .types import EvalCaseResult, EvalObservation, EvalReport, EvalScore, weighted_score


class EvalMetricStats(StrictModel):
    scored: int = Field(ge=0)
    skipped: int = Field(ge=0)
    errors: int = Field(ge=0)
    coverage: float = Field(ge=0, le=1)
    mean: float | None = None
    stddev: float | None = Field(default=None, ge=0)
    minimum: float | None = None
    maximum: float | None = None


class EvalTimingStats(StrictModel):
    count: int = Field(ge=1)
    mean_ms: float = Field(ge=0)
    min_ms: float = Field(ge=0)
    p50_ms: float = Field(ge=0)
    p90_ms: float = Field(ge=0)
    p95_ms: float = Field(ge=0)
    p99_ms: float = Field(ge=0)
    max_ms: float = Field(ge=0)


class EvalGroupStats(StrictModel):
    total: int = Field(ge=1)
    passed: int = Field(ge=0)
    failed: int = Field(ge=0)
    errors: int = Field(ge=0)
    timed_out: int = Field(ge=0)
    unscored: int = Field(ge=0)
    pass_rate: float = Field(ge=0, le=1)
    metrics: dict[str, EvalMetricStats]
    execution: EvalTimingStats
    scoring: EvalTimingStats
    mean_weighted_score: float | None = None


class EvalGradingUsage(StrictModel):
    runs: int = Field(ge=0)
    reported_runs: int = Field(ge=0)
    usage_complete: bool = False
    total_tokens: int | None = Field(default=None, ge=0)
    reported_tokens: dict[UsageField, int]


class EvalStatistics(StrictModel):
    schema_version: int = 1
    report_id: str
    dataset_fingerprint: str
    overall: EvalGroupStats
    tags: dict[str, EvalGroupStats]
    grading: EvalGradingUsage
    wall_time_ms: float = Field(ge=0)


def _timing(values: Sequence[float]) -> EvalTimingStats:
    ordered = sorted(values)

    def percentile(p: float) -> float:
        position = (len(ordered) - 1) * p
        lower = int(position)
        upper = min(lower + 1, len(ordered) - 1)
        return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)

    return EvalTimingStats(
        count=len(ordered),
        mean_ms=sum(ordered) / len(ordered),
        min_ms=ordered[0],
        p50_ms=percentile(0.5),
        p90_ms=percentile(0.9),
        p95_ms=percentile(0.95),
        p99_ms=percentile(0.99),
        max_ms=ordered[-1],
    )


def _group(results: list[EvalCaseResult], report: EvalReport) -> EvalGroupStats:
    metrics: dict[str, EvalMetricStats] = {}
    for scorer in report.scorers:
        scores = [s for r in results for s in r.scores if s.name == scorer.name]
        values = [s.value for s in scores if s.status == "scored" and s.value is not None]
        metrics[scorer.name] = EvalMetricStats(
            scored=len(values),
            skipped=sum(s.status == "skipped" for s in scores),
            errors=sum(s.status == "error" for s in scores),
            coverage=len(values) / len(results),
            mean=sum(values) / len(values) if values else None,
            stddev=statistics.pstdev(values) if values else None,
            minimum=min(values) if values else None,
            maximum=max(values) if values else None,
        )
    weighted = [
        value
        for result in results
        if (value := weighted_score(result.scores, report.config)) is not None
    ]
    passed = sum(r.status == "passed" for r in results)
    return EvalGroupStats(
        total=len(results),
        passed=passed,
        failed=sum(r.status == "failed" for r in results),
        errors=sum(r.status == "error" for r in results),
        timed_out=sum(r.status == "timed_out" for r in results),
        unscored=sum(r.status == "unscored" for r in results),
        pass_rate=passed / len(results),
        metrics=metrics,
        execution=_timing([r.duration_ms for r in results]),
        scoring=_timing([r.scoring_ms for r in results]),
        mean_weighted_score=sum(weighted) / len(weighted) if weighted else None,
    )


def _grading(scores: Sequence[EvalScore]) -> EvalGradingUsage:
    observations: list[EvalObservation] = []
    seen: set[str] = set()
    for score in scores:
        attempts = score.attempts or (
            [
                EvalObservation(
                    run_id=score.run_id,
                    topic_id=score.topic_id,
                    token_usage=score.token_usage,
                    usage_complete=score.usage_complete,
                )
            ]
            if score.run_id
            else []
        )
        for observation in attempts:
            if observation.run_id is not None and observation.run_id not in seen:
                seen.add(observation.run_id)
                observations.append(observation)
    usages = [r.token_usage for r in observations]
    complete = bool(observations) and all(r.usage_complete for r in observations)
    return EvalGradingUsage(
        runs=len(observations),
        reported_runs=sum(u is not None for u in usages),
        usage_complete=complete,
        total_tokens=sum(
            u.total_tokens for u in usages if u is not None and u.total_tokens is not None
        )
        if observations and all(u is not None and u.total_tokens is not None for u in usages)
        else None,
        reported_tokens={
            field: sum(u.reported.get(field, 0) for u in usages if u is not None)
            for field in USAGE_FIELDS
            if any(u is not None and field in u.reported for u in usages)
        },
    )


def analyze_report(report: EvalReport) -> EvalStatistics:
    snapshot = EvalReport.model_validate(report.model_dump())
    cases = {case.id: case for case in snapshot.dataset.cases}
    tags = sorted({tag for case in snapshot.dataset.cases for tag in case.tags})
    return EvalStatistics(
        report_id=snapshot.id,
        dataset_fingerprint=snapshot.dataset_fingerprint,
        overall=_group(snapshot.results, snapshot),
        tags={
            tag: _group([r for r in snapshot.results if tag in cases[r.case_id].tags], snapshot)
            for tag in tags
        },
        grading=_grading([s for r in snapshot.results for s in r.scores]),
        wall_time_ms=(snapshot.finished_at - snapshot.created_at).total_seconds() * 1000,
    )
