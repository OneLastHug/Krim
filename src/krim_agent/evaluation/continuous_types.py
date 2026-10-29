"""持续评测的有界输入；来源身份和人工确认不赋予执行权限。"""

from __future__ import annotations

from typing import Literal, Self

from pydantic import Field, model_validator

from krim_agent.contracts import StrictModel

from .domain import (
    Budget,
    CaseSpec,
    DatasetRef,
    Digest,
    Identity,
    MetricSpec,
    neutral_encoded,
    neutral_fingerprint,
)


class Feedback(StrictModel):
    key: Identity
    reason: str = Field(min_length=1, max_length=512)

    @model_validator(mode="after")
    def bounded(self) -> Self:
        if not self.reason.strip() or len(self.reason.encode()) > 512:
            raise ValueError("feedback_reason")
        return self


class SourceSelection(StrictModel):
    task_id: str | None = None
    case_id: Identity | None = None
    run_id: str | None = None
    repetition: int = Field(default=0, ge=0, le=2)
    revision: int | None = Field(default=None, ge=0)
    evidence_hash: Digest | None = None
    feedback: Feedback | None = None

    @model_validator(mode="after")
    def consistent(self) -> Self:
        if (self.run_id is None) == (self.task_id is None):
            raise ValueError("source_selection")
        if self.task_id is not None and self.case_id is None:
            raise ValueError("source_selection")
        return self


class ImportRequest(StrictModel):
    scope: Identity = "local"
    request_key: Identity
    source_alias: Identity = "current-eval"
    sources: list[SourceSelection] = Field(min_length=1, max_length=32)

    @model_validator(mode="after")
    def bounded(self) -> Self:
        neutral_encoded(self.model_dump(mode="json"), max_bytes=65536)
        return self


class ConfirmationRequest(StrictModel):
    action: Literal["confirm", "edit", "reject"] = "confirm"
    scope: Identity = "local"
    request_key: Identity
    candidate_id: str
    expected_revision: int = Field(ge=0)
    expected_hash: Digest
    case: CaseSpec
    context: Literal["complete", "manual_reconstructed"]
    reason: str = Field(min_length=1, max_length=512)

    @model_validator(mode="after")
    def bounded(self) -> Self:
        if not self.reason.strip() or len(self.reason.encode()) > 512:
            raise ValueError("confirmation_reason")
        reference = self.case.reference
        if "process_constraints" in reference:
            process = reference["process_constraints"]

            def name(value: object) -> bool:
                return (
                    isinstance(value, str)
                    and 0 < len(value) <= 64
                    and all(c.isascii() and (c.isalnum() or c in "_.-") for c in value)
                )

            if not isinstance(process, dict) or set(process) - {
                "min_completed",
                "allowed_tools",
                "forbidden_tools",
                "arguments",
                "approvals",
            }:
                raise ValueError("process_reference_shape")
            if "min_completed" in process and (
                type(process["min_completed"]) is not int
                or not 0 <= process["min_completed"] <= 128
            ):
                raise ValueError("process_reference_shape")
            for field in ["allowed_tools", "forbidden_tools"]:
                if field in process:
                    tools = process[field]
                    if (
                        not isinstance(tools, list)
                        or len(tools) > 32
                        or not all(name(t) for t in tools)
                        or len(set(tools)) != len(tools)
                    ):
                        raise ValueError("process_reference_shape")
            allowed = process.get("allowed_tools", [])
            forbidden = process.get("forbidden_tools", [])
            if (
                isinstance(allowed, list)
                and isinstance(forbidden, list)
                and set(allowed) & set(forbidden)
            ):
                raise ValueError("process_reference_shape")
            if "arguments" in process:
                arguments = process["arguments"]
                if (
                    not isinstance(arguments, dict)
                    or len(arguments) > 32
                    or any(
                        not name(tool)
                        or not isinstance(values, dict)
                        or len(values) > 32
                        or any(not name(key) for key in values)
                        for tool, values in arguments.items()
                    )
                ):
                    raise ValueError("process_reference_shape")
            if "approvals" in process:
                approvals = process["approvals"]
                if (
                    not isinstance(approvals, list)
                    or len(approvals) > 32
                    or any(
                        not isinstance(item, dict)
                        or set(item) != {"name", "decision"}
                        or not name(item["name"])
                        or not isinstance(item["decision"], str)
                        or item["decision"] not in {"allow", "deny"}
                        for item in approvals
                    )
                ):
                    raise ValueError("process_reference_shape")
        if self.case.verifier == "reconciliation/1":
            if type(reference.get("exit_code")) is not int or not isinstance(
                reference.get("files"), dict
            ):
                raise ValueError("reference_shape")
        elif self.case.verifier == "krim-script/1" and not isinstance(
            reference.get("expected"), str
        ):
            raise ValueError("reference_shape")
        neutral_encoded(self.model_dump(mode="json"), max_bytes=1048576)
        return self


class FreezeConfirmed(StrictModel):
    scope: Identity = "local"
    request_key: Identity
    name: Identity
    version: Identity
    confirmations: list[str] = Field(min_length=1, max_length=100)


class ScoreSource(StrictModel):
    case_id: Identity
    repetition: int = Field(ge=0, le=2)
    observation_id: str = Field(min_length=1, max_length=128)
    observation_hash: Digest


class ContinuousRequest(StrictModel):
    schema_version: Literal["continuous-task/1"] = "continuous-task/1"
    scope: Identity = "local"
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    mode: Literal["execute", "rescore", "observed_score"]
    dataset: DatasetRef
    dataset_envelope_fp: Digest
    target_key: Identity = "reconcile-reference"
    metrics: list[MetricSpec] = Field(min_length=1, max_length=6)
    execution: Budget = Field(default_factory=Budget)
    parent_task_id: str | None = None
    parent_report_hash: Digest | None = None
    observations: list[ScoreSource] = Field(default_factory=list, max_length=300)
    seed_policy: Literal["unseeded/1"] = "unseeded/1"
    schedule: dict[str, str | int] | None = None

    @model_validator(mode="after")
    def consistent(self) -> Self:
        if self.execution.batch_timeout_seconds > 120 or self.execution.item_timeout_seconds > 20:
            raise ValueError("continuous_budget")
        if len({m.name for m in self.metrics}) != len(self.metrics):
            raise ValueError("metric_duplicate")
        if not {"fact_integrity", "context_coverage"} <= {m.name for m in self.metrics}:
            raise ValueError("hard_metrics_required")
        if self.mode == "rescore":
            if self.parent_task_id is None or self.parent_report_hash is None or self.observations:
                raise ValueError("source_missing")
        elif self.parent_task_id is not None or self.parent_report_hash is not None:
            raise ValueError("source_invalid")
        if (self.mode == "observed_score") != bool(self.observations):
            raise ValueError("source_invalid")
        object.__setattr__(self, "metrics", sorted(self.metrics, key=lambda metric: metric.name))
        neutral_encoded(
            self.model_dump(mode="json"),
            max_bytes=262144
            if len(self.observations) > 24 or self.execution.concurrency == 2
            else 65536,
        )
        return self


class ScheduleSpec(StrictModel):
    scope: Identity = "local"
    schedule_id: Identity
    version: Identity
    template: ContinuousRequest
    epoch: int = Field(ge=0)
    interval_seconds: int = Field(ge=60, le=86400)
    max_windows: int = Field(ge=1, le=3)

    @model_validator(mode="after")
    def consistent(self) -> Self:
        if self.scope != self.template.scope or self.template.schedule is not None:
            raise ValueError("schedule_scope")
        return self


class ComparisonPolicy(StrictModel):
    schema_version: Literal["continuous-regression/1"] = "continuous-regression/1"
    baseline_id: str
    candidate_id: str
    baseline_hash: Digest
    candidate_hash: Digest
    allowed_subjects: list[Digest] = Field(min_length=1, max_length=2)


def window_request(row: dict[str, object], index: int) -> ContinuousRequest:
    spec = ScheduleSpec.model_validate(row["spec"])
    schedule = {
        "schedule_id": spec.schedule_id,
        "version": spec.version,
        "window_index": index,
        "spec_hash": row["spec_hash"],
        "target_fp": row["target_fp"],
    }
    key = "schedule:" + neutral_fingerprint(
        {
            "scope": spec.scope,
            "schedule_id": spec.schedule_id + "/" + spec.version,
            "dataset_fp": spec.template.dataset_envelope_fp,
            "target_fp": row["target_fp"],
            "window_index": index,
        }
    )
    return ContinuousRequest.model_validate(
        spec.template.model_dump(mode="json") | {"request_key": key, "schedule": schedule}
    )
