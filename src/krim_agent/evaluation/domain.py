"""safe-eval/1纯契约；普通任务不要求Run或私有轨迹。"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Annotated, Any, Literal, Self

import pydantic
from pydantic import Field, JsonValue, model_validator

from krim_agent.contracts import StrictModel, check_json

Identity = Annotated[str, Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")]
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


# 中立契约只在本模块显式扩容，旧v1/public contracts的1MiB完全不改。
SCALE_REQUEST_BYTES = 262144
SCALE_STORAGE_BYTES = 4 * 1024 * 1048576

DATASET_BYTES = 1048576
REQUEST_BYTES = 65536
ARTIFACT_BYTES = 1048576
STDOUT_BYTES = 262144
STDERR_BYTES = 131072
OBSERVATION_BYTES = 10 * 1048576
REPORT_BYTES = 256 * 1048576
SNAPSHOT_BYTES = 512 * 1048576
METADATA_BYTES = 262144
PERSISTED_TASK_BYTES = 2 * 1024 * 1048576


def neutral_encoded(value: object, *, max_bytes: int = SNAPSHOT_BYTES) -> str:
    check_json(value, max_bytes=max_bytes, max_depth=32)
    text = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    if len(text.encode()) > max_bytes:
        raise ValueError("neutral_capacity")
    return text


def neutral_parse(text: str) -> Any:
    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise ValueError("JSON重复键")
            result[key] = value
        return result

    def constant(value: str) -> None:
        raise ValueError("JSON非有限数")

    if len(text.encode()) > SNAPSHOT_BYTES:
        raise ValueError("neutral_capacity")
    value = json.loads(text, object_pairs_hook=pairs, parse_constant=constant)
    check_json(value, max_bytes=SNAPSHOT_BYTES, max_depth=32)
    return value


def neutral_fingerprint(value: object, *, max_bytes: int = SNAPSHOT_BYTES) -> str:
    return hashlib.sha256(neutral_encoded(value, max_bytes=max_bytes).encode()).hexdigest()


def clone(value: Any) -> Any:
    """规范JSON深拷贝；小对象canonical/hash与旧契约逐字一致。"""
    return neutral_parse(neutral_encoded(value))


def execution_conditions(value: Any) -> dict[str, Any]:
    if isinstance(value, dict) and value.get("schema_version") in {"native-cli/1", "krim-run/1"}:
        return _extended_conditions(value)
    if (
        not isinstance(value, dict)
        or set(value) != {"schema_version", "python", "env"}
        or value["schema_version"] != "local-process/1"
        or not isinstance(value["python"], dict)
        or set(value["python"]) != {"executable", "realpath", "sha256", "prefix", "base_prefix"}
        or not all(isinstance(v, str) and v for v in value["python"].values())
        or not isinstance(value["env"], dict)
        or set(value["env"]) != {"PATH", "LANG", "LD_LIBRARY_PATH"}
        or not all(v is None or isinstance(v, str) for v in value["env"].values())
    ):
        raise ValueError("execution_conditions_missing")
    neutral_encoded(value, max_bytes=16384)
    return value


def relative_file(value: str) -> str:
    if not value or value.startswith("/") or "\\" in value:
        raise ValueError("文件路径无效")
    if any(p in {"", ".", ".."} for p in value.split("/")):
        raise ValueError("文件路径无效")
    if len(value.split("/")) > 8:
        raise ValueError("文件路径过深")
    return value


class CaseSpec(StrictModel):
    case_id: Identity
    instruction: str = Field(min_length=1, max_length=4096)
    files: dict[str, str]
    reference: dict[str, JsonValue] = Field(default_factory=dict)
    verifier: Literal[
        "reconciliation/1",
        "repository-repair/1",
        "krim-script/1",
        "agent-bench/1",
    ] = "reconciliation/1"

    @model_validator(mode="after")
    def bounded(self) -> Self:
        if not 1 <= len(self.files) <= 16:
            raise ValueError("初态文件数无效")
        for path, text in self.files.items():
            relative_file(path)
            if (
                path.split("/")[0] in {"outputs", ".eval", "INSTRUCTIONS.md"}
                or len(text.encode()) > 65536
            ):
                raise ValueError("初态与输出冲突或超限")
        check_json(self.model_dump(mode="json"), max_bytes=262144)
        return self


class DatasetRef(StrictModel):
    name: Identity
    version: Identity
    fingerprint: Digest


class DatasetSpec(StrictModel):
    name: Identity
    version: Identity
    cases: list[CaseSpec] = Field(min_length=1, max_length=100)

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if len({c.case_id for c in self.cases}) != len(self.cases):
            raise ValueError("重复Case")
        object.__setattr__(self, "cases", sorted(self.cases, key=lambda c: c.case_id))
        neutral_encoded(self.model_dump(mode="json"), max_bytes=DATASET_BYTES)
        return self

    def ref(self) -> DatasetRef:
        return DatasetRef(
            name=self.name,
            version=self.version,
            fingerprint=neutral_fingerprint(self.model_dump(mode="json"), max_bytes=DATASET_BYTES),
        )


class Budget(StrictModel):
    repetitions: int = Field(default=1, ge=1, le=3)
    concurrency: Literal[1, 2] = 1
    item_timeout_seconds: int = Field(default=20, ge=1, le=180)
    scorer_timeout_seconds: int = Field(default=1, ge=1, le=30)
    batch_timeout_seconds: int = Field(default=120, ge=1, le=240)


class MetricSpec(StrictModel):
    name: Identity
    version: Identity
    implementation_digest: Digest
    config: dict[str, JsonValue] = Field(default_factory=dict)
    required_evidence: list[str] = Field(default_factory=list)
    hard: bool = True


class TaskSpec(StrictModel):
    scope: Identity = "local"
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    dataset: DatasetRef
    target_key: Identity = "reconcile-reference"
    metrics: list[MetricSpec] = Field(min_length=1, max_length=6)
    execution: Budget = Field(default_factory=Budget)

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if len({m.name for m in self.metrics}) != len(self.metrics):
            raise ValueError("重复指标")
        object.__setattr__(self, "metrics", sorted(self.metrics, key=lambda m: m.name))
        neutral_encoded(self.model_dump(mode="json"), max_bytes=REQUEST_BYTES)
        return self


@dataclass(frozen=True)
class ExecutionRef:
    id: str


@dataclass(frozen=True)
class TrialSpec:
    task_id: str
    case_id: str
    repetition: int
    case: dict[str, Any]
    target: dict[str, Any]


@dataclass(frozen=True)
class PreparedTrial:
    spec: TrialSpec
    directory: str | None = None
    initial: dict[str, Any] | None = None


@dataclass(frozen=True)
class CleanupResult:
    closed: bool
    reason: str | None = None
    observation: dict[str, Any] | None = None


@dataclass(frozen=True)
class BatchPlan:
    trials: list[TrialSpec]
    rules: list[dict[str, Any]]
    item_timeout: float
    scorer_timeout: float
    batch_timeout: float
    mode: str = "execute"
    scoring_inputs: dict[tuple[str, int], dict[str, Any]] | None = None
    concurrency: int = 1
    paged: bool = False
    plan_fingerprint: str | None = None


class TrialView(StrictModel):
    case_id: str
    repetition: int
    status: str
    execution_ref: str | None
    output: dict[str, JsonValue] | None
    scores: list[dict[str, JsonValue]] | None
    quality: str | None


class Observation(StrictModel):
    execution_ref: str
    exit_code: int | None
    stdout: str | None
    stderr: str | None
    artifacts: dict[str, JsonValue]
    process: dict[str, JsonValue] | None
    cleanup: Literal["closed", "unknown"]
    token_usage: dict[str, JsonValue] | None = None
    cost: float | None = None
    usage_complete: bool = False

    native: dict[str, JsonValue] | None = None
    verification: dict[str, JsonValue] | None = None
    target_result: dict[str, JsonValue] | None = None

    @pydantic.model_serializer(mode="wrap")
    def legacy_keys(self, handler: pydantic.SerializerFunctionWrapHandler) -> dict[str, Any]:
        body: dict[str, Any] = handler(self)
        for key in ["native", "verification", "target_result"]:
            if body[key] is None:
                body.pop(key)
        return body

    @model_validator(mode="after")
    def bounded(self) -> Self:
        if (
            len((self.stdout or "").encode()) > 3 * STDOUT_BYTES
            or len((self.stderr or "").encode()) > 3 * STDERR_BYTES
        ):
            raise ValueError("stream_capacity")
        neutral_encoded(self.process, max_bytes=METADATA_BYTES)
        neutral_encoded(self.model_dump(mode="json"), max_bytes=OBSERVATION_BYTES)
        return self


class Report(StrictModel):
    schema_version: Literal["safe-eval/1"] = "safe-eval/1"
    body: dict[str, JsonValue]
    report_hash: Digest


def _extended_conditions(value: dict[str, Any]) -> dict[str, Any]:
    """只校验保存的纯数据；冷读不得用当前环境回填历史。"""
    schema = value["schema_version"]
    expected = (
        {
            "schema_version",
            "common",
            "subject",
            "admission",
        }
        if schema == "native-cli/1"
        else {
            "schema_version",
            "run_target_key",
            "binding_descriptor",
            "binding_fingerprint",
            "public_runtime_conditions",
            "budget",
        }
    )
    if set(value) != expected:
        raise ValueError("execution_conditions_missing")
    if schema == "native-cli/1":
        for key in ["common", "subject", "admission"]:
            if not isinstance(value[key], dict):
                raise ValueError("execution_conditions_missing")
        admission = value["admission"]
        if set(admission) != {
            "campaign_id",
            "campaign_store_id",
            "store_binding",
            "store_binding_digest",
            "slot_policy_digest",
            "scope",
            "slots_by_target",
        }:
            raise ValueError("native_admission_missing")
        if admission["scope"] != "local" or not isinstance(admission["slots_by_target"], dict):
            raise ValueError("native_admission_missing")
        for key in ["store_binding_digest", "slot_policy_digest"]:
            text = admission[key]
            if (
                not isinstance(
                    text,
                    str,
                )
                or len(text) != 64
                or any(c not in "0123456789abcdef" for c in text)
            ):
                raise ValueError("native_admission_missing")
    neutral_encoded(value, max_bytes=16384)
    return value
