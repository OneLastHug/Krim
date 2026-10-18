"""LC-M2有限数据契约；显式参考/来源一致性借鉴SRC-M2-01，适配当前纯契约。"""

from __future__ import annotations

from typing import Annotated, Literal, Self

from pydantic import Field, JsonValue, field_validator, model_validator

from krim_agent.contracts import (
    ERROR_CODES,
    RunStatus,
    StrictModel,
    check_json,
    encoded,
    fingerprint,
)
from krim_agent.tools.output import OutputContract

Identity = Annotated[str, Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")]
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
RuleName = Literal[
    "status_code", "exact_match", "json_match", "json_schema", "tool_calls", "trajectory"
]
Quality = Literal["passed", "failed", "error", "timed_out", "unscored"]
RULE_CONFIGS: dict[str, dict[str, JsonValue]] = {
    "status_code": {"match_code": True},
    "exact_match": {"normalize_whitespace": False},
    "json_match": {"strict_json": True},
    "json_schema": {"schema_source": "reference", "remote_refs": False},
    "tool_calls": {"ordered": True},
    "trajectory": {"scope": "single_round"},
}


def limited_text(value: str, limit: int) -> str:
    if not value.strip() or len(value.encode()) > limit:
        raise ValueError("文本为空或字节超限")
    return value


class ExpectedTool(StrictModel):
    name: Identity
    arguments: dict[str, JsonValue] | None
    executor_started: bool | None
    status: Literal["ok", "denied", "failed", "cancelled", "unknown"] | None


class Trajectory(StrictModel):
    exact_steps: int | None = Field(default=None, ge=0, le=32)
    min_steps: int | None = Field(default=None, ge=0, le=32)
    max_steps: int | None = Field(default=None, ge=0, le=32)
    tool_names: list[Identity] | None = Field(default=None, max_length=4)

    @model_validator(mode="after")
    def consistent(self) -> Self:
        if all(
            v is None for v in (self.exact_steps, self.min_steps, self.max_steps, self.tool_names)
        ):
            raise ValueError("轨迹约束不能为空")
        low = self.min_steps if self.min_steps is not None else 0
        high = self.max_steps if self.max_steps is not None else 32
        if low > high or self.exact_steps is not None and not low <= self.exact_steps <= high:
            raise ValueError("轨迹边界矛盾")
        return self


class Reference(StrictModel):
    expected: JsonValue
    expected_status: Literal["completed", "failed"]
    expected_code: str | None
    expected_tools: list[ExpectedTool] | None = Field(max_length=4)
    trajectory: Trajectory | None
    expected_schema: dict[str, JsonValue] | None

    @model_validator(mode="after")
    def consistent(self) -> Self:
        check_json(self.expected, max_bytes=4096)
        if self.expected_status == "completed" and self.expected_code is not None:
            raise ValueError("成功参考不能带错误码")
        if self.expected_status == "failed" and self.expected_code not in ERROR_CODES:
            raise ValueError("失败参考须有稳定错误码")
        if self.expected_schema is not None:
            check_json(self.expected_schema, max_bytes=4096)
            OutputContract(self.expected_schema)
        return self


class Candidate(StrictModel):
    schema_version: int = Field(default=1, ge=1, le=1)
    filter_version: Literal["self-contained/1"] = "self-contained/1"
    source_kind: Literal["manual", "run"]
    source_run_id: str | None
    eligibility: Literal["eligible", "needs_context", "ineligible"]
    reasons: list[str] = Field(min_length=1, max_length=8)
    input_fingerprint: Digest
    selected_input: str | None
    source_snapshot_fingerprint: Digest
    source_target_key: str | None
    source_target_fingerprint: str | None
    observed: dict[str, JsonValue] | None
    evidence: dict[str, JsonValue] | None
    candidate_fingerprint: Digest

    @model_validator(mode="after")
    def consistent(self) -> Self:
        content = self.model_dump(mode="json", exclude={"candidate_fingerprint", "selected_input"})
        if fingerprint(content) != self.candidate_fingerprint:
            raise ValueError("候选指纹不一致")
        if (
            self.selected_input is not None
            and fingerprint(self.selected_input) != self.input_fingerprint
        ):
            raise ValueError("候选输入指纹不一致")
        if self.source_kind == "manual" and any(
            v is not None
            for v in (
                self.source_run_id,
                self.source_target_key,
                self.source_target_fingerprint,
                self.observed,
                self.evidence,
            )
        ):
            raise ValueError("人工来源不得伪造Run")
        if self.source_kind == "run" and self.source_run_id is None:
            raise ValueError("Run来源身份缺失")
        return self


class ManualSelection(StrictModel):
    case_id: Identity
    source_kind: Literal["manual"]
    input: str
    reference: Reference
    reason: str

    @field_validator("input")
    @classmethod
    def input_size(cls, value: str) -> str:
        return limited_text(value, 4096)

    @field_validator("reason")
    @classmethod
    def reason_size(cls, value: str) -> str:
        return limited_text(value, 512)


class RunSelection(StrictModel):
    case_id: Identity
    source_kind: Literal["run"]
    source_run_id: str
    candidate_fingerprint: Digest
    selected_input: str
    reference: Reference
    reason: str

    @field_validator("selected_input")
    @classmethod
    def input_size(cls, value: str) -> str:
        return limited_text(value, 4096)

    @field_validator("reason")
    @classmethod
    def reason_size(cls, value: str) -> str:
        return limited_text(value, 512)


Selection = Annotated[ManualSelection | RunSelection, Field(discriminator="source_kind")]


class FreezeRequest(StrictModel):
    schema_version: int = Field(default=1, ge=1, le=1)
    name: Identity
    version: Identity
    cases: list[Selection] = Field(min_length=1, max_length=8)

    @model_validator(mode="after")
    def consistent(self) -> Self:
        ids = [c.case_id for c in self.cases]
        sources = [c.source_run_id for c in self.cases if isinstance(c, RunSelection)]
        if len(set(ids)) != len(ids) or len(set(sources)) != len(sources):
            raise ValueError("重复case或来源")
        object.__setattr__(self, "cases", sorted(self.cases, key=lambda c: c.case_id))
        check_json(self.model_dump(mode="json"), max_bytes=49152)
        return self


class FrozenCase(StrictModel):
    case_id: Identity
    input: str
    input_fingerprint: Digest
    source: Candidate
    reference: Reference
    reference_fingerprint: Digest
    reason: str

    @model_validator(mode="after")
    def consistent(self) -> Self:
        limited_text(self.input, 4096)
        limited_text(self.reason, 512)
        if self.source.eligibility != "eligible" or self.input_fingerprint != fingerprint(
            self.input
        ):
            raise ValueError("样本来源或输入无效")
        if self.source.input_fingerprint != self.input_fingerprint:
            raise ValueError("来源与输入不一致")
        if self.reference_fingerprint != fingerprint(self.reference.model_dump(mode="json")):
            raise ValueError("参考指纹不一致")
        return self


class DatasetRef(StrictModel):
    name: Identity
    version: Identity
    fingerprint: Digest


class DatasetRevision(DatasetRef):
    schema_version: int = Field(default=1, ge=1, le=1)
    cases: list[FrozenCase] = Field(min_length=1, max_length=8)
    created_at: float

    @model_validator(mode="after")
    def consistent(self) -> Self:
        ids = [c.case_id for c in self.cases]
        if ids != sorted(set(ids)):
            raise ValueError("样本身份或顺序无效")
        content = self.model_dump(mode="json", exclude={"fingerprint", "created_at"})
        if fingerprint(content) != self.fingerprint:
            raise ValueError("修订指纹不一致")
        check_json(self.model_dump(mode="json"), max_bytes=65536)
        return self

    def selection(self) -> FreezeRequest:
        cases: list[Selection] = []
        for case in self.cases:
            if case.source.source_kind == "manual":
                cases.append(
                    ManualSelection(
                        case_id=case.case_id,
                        reference=case.reference,
                        reason=case.reason,
                        source_kind="manual",
                        input=case.input,
                    )
                )
            else:
                assert case.source.source_run_id is not None
                cases.append(
                    RunSelection(
                        case_id=case.case_id,
                        reference=case.reference,
                        reason=case.reason,
                        source_kind="run",
                        source_run_id=case.source.source_run_id,
                        candidate_fingerprint=case.source.candidate_fingerprint,
                        selected_input=case.input,
                    )
                )
        return FreezeRequest(name=self.name, version=self.version, cases=cases)


class RuleSpec(StrictModel):
    name: RuleName
    version: Literal["1"]
    config: dict[str, JsonValue]

    @model_validator(mode="after")
    def consistent(self) -> Self:
        if encoded(self.config) != encoded(RULE_CONFIGS[self.name]):
            raise ValueError("规则配置不符")
        return self


def rule(name: RuleName) -> RuleSpec:
    return RuleSpec(name=name, version="1", config=dict(RULE_CONFIGS[name]))


class Execution(StrictModel):
    repetitions: int = Field(ge=1, le=3)
    concurrency: Literal[1]
    item_timeout_seconds: Literal[12]
    scorer_timeout_seconds: Literal[1]
    batch_timeout_seconds: Literal[600]

    @field_validator(
        "repetitions",
        "concurrency",
        "item_timeout_seconds",
        "scorer_timeout_seconds",
        "batch_timeout_seconds",
        mode="before",
    )
    @classmethod
    def integer_input(cls, value: object) -> object:
        if type(value) is not int:
            raise ValueError("配置必须是严格整数")
        return value

    @model_validator(mode="after")
    def numbers(self) -> Self:
        if any(type(v) is not int for v in self.model_dump().values()):
            raise ValueError("配置必须是严格整数")
        return self


class EvalRequest(StrictModel):
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    dataset: DatasetRef
    target_key: Literal["target-v1", "target-v2", "target-bad", "target-http"]
    rules: list[RuleSpec] = Field(min_length=1, max_length=6)
    execution: Execution

    @model_validator(mode="after")
    def consistent(self) -> Self:
        names = [r.name for r in self.rules]
        if "status_code" not in names or len(names) != len(set(names)):
            raise ValueError("缺状态规则或规则重复")
        object.__setattr__(self, "rules", sorted(self.rules, key=lambda r: r.name))
        return self


class EvalView(StrictModel):
    id: str
    request_key: str
    status: RunStatus
    code: str | None
    created_at: float
    finished_at: float | None
    stop_requested: bool
    revision: int
    planned_count: int
    finished_count: int


class EvalTool(StrictModel):
    call_id: str
    provider_call_id: str | None
    span_id: str
    step_index: int
    name: str
    arguments: dict[str, JsonValue] | None
    status: Literal["ok", "denied", "failed", "cancelled", "unknown"]
    executor_started: bool | None
    result_available: bool | None
    value_fingerprint: str | None
    code: str | None


class EvalOutput(StrictModel):
    run_id: str
    request_key: str
    trace_id: str
    status: RunStatus
    code: str | None
    snapshot_fingerprint: Digest
    last_seq: int
    facts_complete: bool
    text: str | None
    model_steps: int | None = Field(ge=0, le=32)
    tools: list[EvalTool] | None = Field(max_length=128)
    token_usage: dict[str, JsonValue] | None
    usage_complete: bool


class Score(StrictModel):
    rule: dict[str, str]
    status: Literal["scored", "skipped", "error"]
    value: int | None = Field(ge=0, le=1)
    reason: str | None
    evidence: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def consistent(self) -> Self:
        if (self.status == "scored") != (self.value is not None):
            raise ValueError("评分状态和值不符")
        check_json(self.evidence, max_bytes=512)
        return self
