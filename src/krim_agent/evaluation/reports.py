"""同数据集、同评分配置的离线回归对比。"""

from __future__ import annotations

import math

from pydantic import JsonValue

from .types import EvalComparison, EvalReport


def compare_reports(
    before: EvalReport, after: EvalReport, *, tolerance: float = 0
) -> EvalComparison:
    if not math.isfinite(tolerance) or not 0 <= tolerance <= 1:
        raise ValueError("评分容差必须为 0–1 的有限数值")
    baseline = EvalReport.model_validate(before.model_dump())
    candidate = EvalReport.model_validate(after.model_dump())
    compatible = (
        baseline.dataset_fingerprint == candidate.dataset_fingerprint
        and baseline.config == candidate.config
        and baseline.scorers == candidate.scorers
    )
    if not compatible:
        return EvalComparison(
            compatible=False,
            reason="数据集或评分/执行配置不一致",
            before_id=baseline.id,
            after_id=candidate.id,
            mean_score_delta={},
            pass_rate_delta=0,
        )
    regressions: list[dict[str, JsonValue]] = []
    for old, new in zip(baseline.results, candidate.results, strict=True):
        deltas: dict[str, JsonValue] = {}
        for a, b in zip(old.scores, new.scores, strict=True):
            if a.value is not None and b.value is not None and a.value - b.value > tolerance:
                deltas[a.name] = b.value - a.value
        if (old.status == "passed" and new.status != "passed") or deltas:
            regressions.append(
                {
                    "case_id": new.case_id,
                    "repetition": new.repetition,
                    "before_status": old.status,
                    "after_status": new.status,
                    "score_deltas": deltas,
                }
            )
    means: dict[str, float | None] = {}
    for name, old_value in baseline.summary.mean_scores.items():
        new_value = candidate.summary.mean_scores[name]
        means[name] = (
            new_value - old_value if old_value is not None and new_value is not None else None
        )
    return EvalComparison(
        compatible=True,
        reason="数据集、评分器和执行配置一致",
        before_id=baseline.id,
        after_id=candidate.id,
        mean_score_delta=means,
        pass_rate_delta=candidate.summary.pass_rate - baseline.summary.pass_rate,
        regressions=regressions,
    )
