"""持久任务与部分结果协议；最终报告继续使用独立的 EvalReport。"""

from datetime import datetime
from typing import Literal

from pydantic import Field, JsonValue, model_validator

from krim_agent.protocol.models import StrictModel

from .types import (
    EvalCaseResult,
    EvalConfig,
    EvalDataset,
    EvalOutput,
    EvalScore,
    EvalScorerInfo,
    EvalSummary,
    EvalTargetInfo,
    fingerprint,
)

JobStatus = Literal[
    "queued", "running", "paused", "cancelled", "interrupted", "completed", "failed"
]
ItemPhase = Literal[
    "pending", "executing", "executed", "scoring", "done", "uncertain", "grading_uncertain"
]


class EvalJobSpec(StrictModel):
    dataset: EvalDataset
    target: EvalTargetInfo
    config: EvalConfig
    scorers: list[EvalScorerInfo] = Field(min_length=1, max_length=32)

    @model_validator(mode="after")
    def bounded(self) -> "EvalJobSpec":
        if len(self.dataset.cases) * self.config.repetitions > 10000:
            raise ValueError("评估尝试数超过 10000")
        if len({s.name for s in self.scorers}) != len(self.scorers):
            raise ValueError("评分器名称必须唯一")
        self.config.validate_metrics([s.name for s in self.scorers])
        return self

    @property
    def fingerprint(self) -> str:
        values = self.model_dump(mode="json")
        values["dataset"] = self.dataset.fingerprint_content()
        for name, default in (("metrics", {}), ("aggregate_threshold", None)):
            if values["config"][name] == default:
                values["config"].pop(name)
        return fingerprint(values)


class EvalJobView(StrictModel):
    id: str
    status: JobStatus
    revision: int = Field(ge=0)
    epoch: int = Field(ge=0)
    event_count: int = Field(ge=0)
    total: int = Field(ge=1)
    pending: int = Field(ge=0)
    active: int = Field(ge=0)
    completed: int = Field(ge=0)
    uncertain: int = Field(ge=0)
    can_resume: bool = False
    control: Literal["pause", "cancel"] | None = None
    error_code: str | None = None
    created_at: datetime
    updated_at: datetime
    finished_at: datetime | None = None


class EvalItem(StrictModel):
    job_id: str
    ordinal: int = Field(ge=0)
    case_id: str
    repetition: int = Field(ge=1)
    generation: int = Field(ge=1)
    phase: ItemPhase
    bindings: dict[str, JsonValue]
    output: EvalOutput | None = None
    scores: list[EvalScore]
    result: EvalCaseResult | None = None
    history: list[dict[str, JsonValue]]
    error_code: str | None = None
    duration_ms: float = Field(ge=0)
    scoring_ms: float = Field(ge=0)
    updated_at: datetime


class EvalItemPage(StrictModel):
    job_id: str
    total: int
    offset: int
    items: list[EvalItem]


class EvalEvent(StrictModel):
    job_id: str
    seq: int = Field(ge=0)
    kind: str
    data: dict[str, JsonValue]
    created_at: datetime


class EvalPartialReport(StrictModel):
    schema_version: Literal[1] = 1
    complete: Literal[False] = False
    job: EvalJobView
    spec: EvalJobSpec
    results: list[EvalCaseResult]
    summary: EvalSummary | None = None


class EvalResumeRequest(StrictModel):
    retry_uncertain: bool = False
    skip_uncertain: bool = False

    @model_validator(mode="after")
    def exclusive(self) -> "EvalResumeRequest":
        if self.retry_uncertain and self.skip_uncertain:
            raise ValueError("未知执行不能同时重试和跳过")
        return self


class EvalRemoteStartRequest(StrictModel):
    target: Literal["text", "agent", "conversation"] = "text"
    agent_name: str = Field(default="default", min_length=1, max_length=64)
    dataset: EvalDataset
    config: EvalConfig = Field(default_factory=EvalConfig)
    scorers: list[str] = Field(default_factory=lambda: ["exact-match"], min_length=1, max_length=32)
    version: str = Field(default="1", min_length=1, max_length=128)
    prompt_prefix: str = Field(default="", max_length=8192)
    request_key: str | None = Field(default=None, min_length=1, max_length=128)


class EvalRemoteResumeRequest(StrictModel):
    target: Literal["text", "agent", "conversation"] = "text"
    agent_name: str = Field(default="default", min_length=1, max_length=64)
    scorers: list[str] = Field(default_factory=lambda: ["exact-match"], min_length=1, max_length=32)
    version: str = Field(default="1", min_length=1, max_length=128)
    prompt_prefix: str = Field(default="", max_length=8192)
    retry_uncertain: bool = False
    skip_uncertain: bool = False
