"""Krim 本地评估的数据合约；来源 SRC-USER-001/EXT/PY、ADR-0046。"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Literal

from pydantic import Field, JsonValue, model_validator

from krim_agent.observability import TraceContext
from krim_agent.protocol.models import StrictModel, check_json
from krim_agent.usage import USAGE_FIELDS, UsageField, UsageTotals


def fingerprint(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    ).hexdigest()


class EvalCase(StrictModel):
    id: str = Field(min_length=1, max_length=128)
    input: JsonValue
    # 缺少参考时保留 None，评分器必须明确记录未评分。
    expected: JsonValue = None
    metadata: dict[str, JsonValue] = Field(default_factory=dict, max_length=64)
    tags: list[str] = Field(default_factory=list, max_length=32)

    @model_validator(mode="after")
    def finite_content(self) -> EvalCase:
        check_json(self.model_dump(mode="json"))
        return self


class EvalDataset(StrictModel):
    name: str = Field(min_length=1, max_length=128)
    version: str = Field(default="1", min_length=1, max_length=64)
    cases: list[EvalCase] = Field(min_length=1, max_length=1000)

    @model_validator(mode="after")
    def unique_ids(self) -> EvalDataset:
        if len({case.id for case in self.cases}) != len(self.cases):
            raise ValueError("评估样本 ID 必须唯一")
        check_json(self.model_dump(mode="json"), max_bytes=16 * 1024 * 1024)
        return self

    @property
    def fingerprint(self) -> str:
        return fingerprint(self.model_dump(mode="json"))


class EvalConfig(StrictModel):
    concurrency: int = Field(default=1, ge=1, le=16)
    repetitions: int = Field(default=1, ge=1, le=20)
    timeout_seconds: float = Field(default=60, gt=0, le=300)
    scorer_timeout_seconds: float = Field(default=10, gt=0, le=60)
    threshold: float = Field(default=1, ge=0, le=1)


class EvalToolCall(StrictModel):
    call_id: str
    name: str
    arguments: dict[str, JsonValue]
    ok: bool | None = None
    code: str | None = None
    result: JsonValue = None


class EvalOutput(StrictModel):
    value: JsonValue = None
    status: Literal["completed", "failed", "aborted", "interrupted"] = "completed"
    run_id: str | None = None
    topic_id: str | None = None
    agent_id: str | None = None
    finish_reason: str | None = None
    code: str | None = None
    tools: list[EvalToolCall] | None = Field(default=None, max_length=128)
    model_steps: int | None = Field(default=None, ge=0, le=32)
    token_usage: UsageTotals | None = None
    usage_complete: bool = False
    trace_context: TraceContext | None = None

    @model_validator(mode="after")
    def finite_output(self) -> EvalOutput:
        check_json(self.model_dump(mode="json"), max_bytes=1024 * 1024)
        if self.usage_complete and (
            self.token_usage is None or not self.token_usage.usage_complete
        ):
            raise ValueError("完整用量需要实际完整供应商报告")
        return self


class EvalScore(StrictModel):
    name: str = Field(min_length=1, max_length=128)
    value: float | None = Field(default=None, ge=0, le=1)
    status: Literal["scored", "skipped", "error"] = "scored"
    reason: str | None = Field(default=None, max_length=512)

    @model_validator(mode="after")
    def valid_score(self) -> EvalScore:
        if (self.status == "scored") != (self.value is not None):
            raise ValueError("有效评分需要数值；跳过或失败不能伪造评分")
        return self


class EvalScorerInfo(StrictModel):
    name: str = Field(min_length=1, max_length=128)
    version: str = Field(default="1", min_length=1, max_length=64)
    config: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def finite_config(self) -> EvalScorerInfo:
        check_json(self.config)
        return self


class EvalTargetInfo(StrictModel):
    name: str = Field(min_length=1, max_length=128)
    kind: str = Field(min_length=1, max_length=64)
    version: str = Field(default="1", min_length=1, max_length=128)
    config: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def finite_config(self) -> EvalTargetInfo:
        check_json(self.config)
        return self


CaseStatus = Literal["passed", "failed", "error", "timed_out", "unscored"]


class EvalCaseResult(StrictModel):
    case_id: str
    repetition: int = Field(ge=1, le=20)
    status: CaseStatus
    output: EvalOutput | None = None
    scores: list[EvalScore] = Field(default_factory=list, max_length=32)
    error_code: str | None = None
    duration_ms: float = Field(ge=0)
    scoring_ms: float = Field(ge=0)


class EvalSummary(StrictModel):
    total: int = Field(ge=1)
    passed: int = Field(ge=0)
    failed: int = Field(ge=0)
    errors: int = Field(ge=0)
    timed_out: int = Field(ge=0)
    unscored: int = Field(ge=0)
    pass_rate: float = Field(ge=0, le=1)
    mean_scores: dict[str, float | None]
    scored_counts: dict[str, int]
    mean_duration_ms: float = Field(ge=0)
    input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
    total_tokens: int | None = Field(default=None, ge=0)
    reported_tokens: dict[UsageField, int] = Field(default_factory=dict)
    reported_cases: int = Field(ge=0)
    usage_complete: bool


def classify(
    output: EvalOutput | None, scores: list[EvalScore], config: EvalConfig, error_code: str | None
) -> CaseStatus:
    if error_code == "eval_timeout":
        return "timed_out"
    if error_code is not None or output is None or output.status != "completed":
        return "error"
    if any(score.status == "error" for score in scores):
        return "error"
    if not scores or any(score.status == "skipped" for score in scores):
        return "unscored"
    return (
        "passed"
        if all(score.value is not None and score.value >= config.threshold for score in scores)
        else "failed"
    )


def summarize(results: list[EvalCaseResult], scorers: list[EvalScorerInfo]) -> EvalSummary:
    means: dict[str, float | None] = {}
    counts: dict[str, int] = {}
    for scorer in scorers:
        values = [
            score.value
            for result in results
            for score in result.scores
            if score.name == scorer.name and score.status == "scored" and score.value is not None
        ]
        means[scorer.name] = sum(values) / len(values) if values else None
        counts[scorer.name] = len(values)
    usages = [result.output.token_usage if result.output else None for result in results]

    def total(field: Literal["input_tokens", "output_tokens", "total_tokens"]) -> int | None:
        values = [getattr(usage, field) if usage else None for usage in usages]
        return (
            sum(value for value in values if value is not None)
            if all(value is not None for value in values)
            else None
        )

    subtotals = {
        field: sum(usage.reported.get(field, 0) for usage in usages if usage is not None)
        for field in USAGE_FIELDS
        if any(usage is not None and field in usage.reported for usage in usages)
    }
    passed = sum(result.status == "passed" for result in results)
    return EvalSummary(
        total=len(results),
        passed=passed,
        failed=sum(result.status == "failed" for result in results),
        errors=sum(result.status == "error" for result in results),
        timed_out=sum(result.status == "timed_out" for result in results),
        unscored=sum(result.status == "unscored" for result in results),
        pass_rate=passed / len(results),
        mean_scores=means,
        scored_counts=counts,
        mean_duration_ms=sum(result.duration_ms for result in results) / len(results),
        input_tokens=total("input_tokens"),
        output_tokens=total("output_tokens"),
        total_tokens=total("total_tokens"),
        reported_tokens=subtotals,
        reported_cases=sum(usage is not None for usage in usages),
        usage_complete=all(result.output and result.output.usage_complete for result in results),
    )


class EvalReport(StrictModel):
    schema_version: Literal[1] = 1
    id: str
    parent_report_id: str | None = None
    created_at: datetime
    finished_at: datetime
    dataset: EvalDataset
    dataset_fingerprint: str
    target: EvalTargetInfo
    config: EvalConfig
    scorers: list[EvalScorerInfo] = Field(min_length=1, max_length=32)
    results: list[EvalCaseResult] = Field(min_length=1, max_length=10000)
    summary: EvalSummary
    metadata: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def consistent_report(self) -> EvalReport:
        if self.dataset_fingerprint != self.dataset.fingerprint:
            raise ValueError("评估数据集指纹不一致")
        names = [scorer.name for scorer in self.scorers]
        if len(set(names)) != len(names):
            raise ValueError("评分器名称必须唯一")
        expected = [
            (case.id, repetition)
            for case in self.dataset.cases
            for repetition in range(1, self.config.repetitions + 1)
        ]
        if [(result.case_id, result.repetition) for result in self.results] != expected:
            raise ValueError("报告的样本、重复次数或排列不完整")
        for result in self.results:
            if [score.name for score in result.scores] != names:
                raise ValueError("报告的评分来源或顺序不一致")
            if result.status != classify(
                result.output, result.scores, self.config, result.error_code
            ):
                raise ValueError("报告的通过状态与评分不一致")
        if self.summary != summarize(self.results, self.scorers):
            raise ValueError("报告汇总与实际样本不一致")
        if self.finished_at < self.created_at:
            raise ValueError("评估完成时间不能早于开始时间")
        return self


class EvalComparison(StrictModel):
    compatible: bool
    reason: str
    before_id: str
    after_id: str
    mean_score_delta: dict[str, float | None]
    pass_rate_delta: float
    regressions: list[dict[str, JsonValue]] = Field(default_factory=list)
