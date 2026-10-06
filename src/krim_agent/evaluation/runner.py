"""有界评估编排；外部取消先收束 worker，再向调用方传播。"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol
from uuid import uuid4

from pydantic import JsonValue

from .scorers import EvalScorer, ExactMatchScorer
from .types import (
    EvalCase,
    EvalCaseResult,
    EvalConfig,
    EvalDataset,
    EvalOutput,
    EvalReport,
    EvalScore,
    EvalScorerInfo,
    EvalTargetInfo,
    classify,
    fingerprint,
    summarize,
)


class EvalCleanupError(RuntimeError):
    """生产无法收束时终止批次，避免继续扩大未回收运行。"""


class EvalScoringUnknown(RuntimeError):
    """评分器的真实调用尚未能确认终态，必须交给恢复者裁决。"""


@dataclass
class EvalAttempt:
    report_id: str
    case_id: str
    repetition: int
    output: EvalOutput | None = None
    generation: int = 1
    bindings: dict[str, JsonValue] = field(default_factory=dict)
    checkpoint: Callable[[dict[str, JsonValue]], Awaitable[None]] | None = None
    namespace: str = ""
    score_bindings: dict[str, JsonValue] = field(default_factory=dict)
    score_checkpoint: Callable[[dict[str, JsonValue]], Awaitable[None]] | None = None
    retry_scoring: bool = False
    skip_scoring: bool = False
    score_outputs: dict[str, EvalOutput] = field(default_factory=dict)

    async def save_binding(self, values: dict[str, JsonValue]) -> None:
        if self.checkpoint is not None:
            task = asyncio.ensure_future(self.checkpoint(values))
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                await task
                self.bindings = values
                raise
        self.bindings = values

    @property
    def idempotency_key(self) -> str:
        base = (
            f"eval:{self.report_id}:{fingerprint(self.case_id)[:16]}:"
            f"{self.repetition}:{self.generation}"
        )
        return base + (f":{self.namespace}" if self.namespace else "")

    async def save_score_binding(self, name: str, values: dict[str, JsonValue]) -> None:
        self.score_bindings[name] = values
        if self.score_checkpoint is not None:
            task = asyncio.ensure_future(self.score_checkpoint(self.score_bindings))
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                await task
                raise


class EvalTarget(Protocol):
    def describe(self) -> EvalTargetInfo: ...

    async def run(self, case: EvalCase, attempt: EvalAttempt) -> EvalOutput: ...


async def execute_case(
    case: EvalCase,
    repetition: int,
    report_id: str,
    target: EvalTarget,
    scorers: list[EvalScorer],
    infos: list[EvalScorerInfo],
    config: EvalConfig,
) -> EvalCaseResult:
    """执行一个样本；持久任务和一次性评估共用同一取消/评分语义。"""
    attempt = EvalAttempt(report_id, case.id, repetition)
    started = time.monotonic()
    output: EvalOutput | None = None
    error_code: str | None = None
    try:
        async with asyncio.timeout(config.timeout_seconds):
            value = await target.run(case.model_copy(deep=True), attempt)
        output = EvalOutput.model_validate(value.model_dump())
    except EvalCleanupError:
        raise
    except TimeoutError:
        output, error_code = attempt.output, "eval_timeout"
    except Exception:
        output, error_code = attempt.output, "eval_execution_error"
    duration = (time.monotonic() - started) * 1000
    scores, scoring = await _score(
        case, output, error_code, scorers, infos, config, attempt=attempt
    )
    return EvalCaseResult(
        case_id=case.id,
        repetition=repetition,
        status=classify(output, scores, config, error_code, case),
        output=output,
        scores=scores,
        error_code=error_code,
        duration_ms=duration,
        scoring_ms=scoring,
    )


def _scorers(values: Sequence[EvalScorer] | None) -> tuple[list[EvalScorer], list[EvalScorerInfo]]:
    scorers: list[EvalScorer] = list(values) if values is not None else [ExactMatchScorer()]
    if not 1 <= len(scorers) <= 32:
        raise ValueError("评估需要 1–32 个评分器")
    infos = [EvalScorerInfo.model_validate(scorer.describe().model_dump()) for scorer in scorers]
    if len({info.name for info in infos}) != len(infos):
        raise ValueError("评分器名称必须唯一")
    return scorers, infos


async def _score(
    case: EvalCase,
    output: EvalOutput | None,
    error_code: str | None,
    scorers: list[EvalScorer],
    infos: list[EvalScorerInfo],
    config: EvalConfig,
    *,
    saved_scores: list[EvalScore] | None = None,
    on_score: Callable[[list[EvalScore], float], Awaitable[None]] | None = None,
    attempt: EvalAttempt | None = None,
) -> tuple[list[EvalScore], float]:
    started = time.monotonic()
    scores = list(saved_scores or [])
    if [s.name for s in scores] != [i.name for i in infos[: len(scores)]]:
        raise ValueError("已保存评分来源不一致")
    for scorer, info in list(zip(scorers, infos, strict=True))[len(scores) :]:
        if (
            output is None
            or (output.status != "completed" and output.status != case.expected_status)
            or error_code is not None
        ):
            scores.append(
                EvalScore(name=info.name, status="skipped", reason="target_not_completed")
            )
            if on_score is not None:
                await on_score(scores, (time.monotonic() - started) * 1000)
            continue
        try:
            async with asyncio.timeout(config.scorer_timeout_seconds):
                with_attempt = getattr(scorer, "score_with_attempt", None)
                if with_attempt is not None:
                    value = await with_attempt(
                        case.model_copy(deep=True), output.model_copy(deep=True), attempt
                    )
                else:
                    value = await scorer.score(
                        case.model_copy(deep=True), output.model_copy(deep=True)
                    )
            score = EvalScore.model_validate(value.model_dump())
            if score.name != info.name:
                raise ValueError("评分来源不一致")
        except EvalScoringUnknown:
            raise
        except TimeoutError:
            score = EvalScore(name=info.name, status="error", reason="scorer_timeout")
        except Exception:
            score = EvalScore(name=info.name, status="error", reason="scorer_error")
        scores.append(score)
        if on_score is not None:
            await on_score(scores, (time.monotonic() - started) * 1000)
    return scores, (time.monotonic() - started) * 1000


async def evaluate(
    dataset: EvalDataset,
    target: EvalTarget,
    *,
    scorers: Sequence[EvalScorer] | None = None,
    config: EvalConfig | None = None,
) -> EvalReport:
    # 冻结调用方数据；目标和评分器分别收到副本，不能改变报告的基线。
    snapshot = EvalDataset.model_validate(dataset.model_dump())
    options = EvalConfig.model_validate((config or EvalConfig()).model_dump())
    selected, infos = _scorers(scorers)
    options.validate_metrics([info.name for info in infos])
    target_info = EvalTargetInfo.model_validate(target.describe().model_dump())
    if len(snapshot.cases) * options.repetitions > 10000:
        raise ValueError("评估总尝试数不能超过 10000")
    report_id = str(uuid4())
    created = datetime.now(UTC)
    jobs = [
        (case, repetition)
        for case in snapshot.cases
        for repetition in range(1, options.repetitions + 1)
    ]
    pending = iter(enumerate(jobs))
    results: dict[int, EvalCaseResult] = {}

    async def worker() -> None:
        for index, (case, repetition) in pending:
            results[index] = await execute_case(
                case, repetition, report_id, target, selected, infos, options
            )

    workers = [
        asyncio.create_task(worker(), name=f"krim-eval-{report_id}-{index}")
        for index in range(min(options.concurrency, len(jobs)))
    ]
    try:
        await asyncio.gather(*workers)
    finally:
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
    ordered = [results[index] for index in range(len(jobs))]
    return EvalReport(
        id=report_id,
        created_at=created,
        finished_at=datetime.now(UTC),
        dataset=snapshot,
        dataset_fingerprint=snapshot.fingerprint,
        target=target_info,
        config=options,
        scorers=infos,
        results=ordered,
        summary=summarize(ordered, infos),
    )


async def rescore(
    report: EvalReport,
    *,
    scorers: Sequence[EvalScorer] | None = None,
    threshold: float | None = None,
    config: EvalConfig | None = None,
) -> EvalReport:
    previous = EvalReport.model_validate(report.model_dump())
    selected, infos = _scorers(scorers)
    options = (
        EvalConfig.model_validate(config.model_dump())
        if config
        else previous.config.model_copy(deep=True)
    )
    if any(getattr(scorer, "requires_model", False) for scorer in selected):
        raise ValueError("离线重评分禁止模型评委；显式评分请使用持久任务")
    if options.repetitions != previous.config.repetitions:
        raise ValueError("重评分不能改变已执行的重复次数")
    if threshold is not None:
        options = EvalConfig.model_validate({**options.model_dump(), "threshold": threshold})
    options.validate_metrics([info.name for info in infos])
    created = datetime.now(UTC)
    cases = {case.id: case for case in previous.dataset.cases}
    results = []
    for item in previous.results:
        scores, scoring = await _score(
            cases[item.case_id], item.output, item.error_code, selected, infos, options
        )
        results.append(
            EvalCaseResult(
                **{
                    **item.model_dump(),
                    "scores": scores,
                    "scoring_ms": scoring,
                    "status": classify(
                        item.output, scores, options, item.error_code, cases[item.case_id]
                    ),
                }
            )
        )
    return EvalReport(
        id=str(uuid4()),
        parent_report_id=previous.id,
        created_at=created,
        finished_at=datetime.now(UTC),
        dataset=previous.dataset,
        dataset_fingerprint=previous.dataset_fingerprint,
        target=previous.target,
        config=options,
        scorers=infos,
        results=results,
        summary=summarize(results, infos),
    )


class EvaluationRunner:
    """可重复使用的可信目标/评分配置；每次 run 产生独立报告。"""

    def __init__(
        self,
        target: EvalTarget,
        *,
        scorers: Sequence[EvalScorer] | None = None,
        config: EvalConfig | None = None,
    ) -> None:
        self.target, self.scorers, self.config = target, scorers, config

    async def run(self, dataset: EvalDataset) -> EvalReport:
        return await evaluate(dataset, self.target, scorers=self.scorers, config=self.config)
