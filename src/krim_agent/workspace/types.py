"""工作区严格纯类型；公开 DTO 不含宿主路径、凭据或可复用资源句柄。"""

from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass
from typing import Annotated, Literal, Self

from pydantic import Field, JsonValue, field_validator, model_validator

from krim_agent.context.types import CompactionPolicy
from krim_agent.contracts import ServiceError, StrictModel, check_json, encoded, fingerprint
from krim_agent.execution.types import path_parts as os_path_parts
from krim_agent.security import PrincipalContext


def path_parts(name: str) -> tuple[str, ...]:
    parts = os_path_parts(name)
    if any(
        p.startswith(".env.") or p.lower() in {"credentials", "id_rsa", "id_ed25519"} for p in parts
    ):
        raise ServiceError("artifact_invalid")
    return parts


Key = Annotated[str, Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")]
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Id = Annotated[
    str, Field(pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
]


class WorkLimits(StrictModel):
    run_timeout_seconds: float = Field(default=180, gt=0, le=300)
    tool_timeout_seconds: float = Field(default=90, gt=0, le=90)
    approval_timeout_seconds: float = Field(default=30, gt=0, le=30)
    model_steps: int = Field(default=12, ge=1, le=12)
    tool_calls: int = Field(default=32, ge=1, le=32)
    read_parallelism: int = Field(default=4, ge=1, le=4)


class WorkspaceProfile(StrictModel):
    """可信启动文件的数据投影；OS字段必须与已接受Profile逐项一致。"""

    schema_version: Literal[1] = 1
    base_target: Literal["target-http"] = "target-http"
    max_tools_per_step: int = Field(ge=1, le=4)
    runtime: WorkLimits = Field(default_factory=WorkLimits)
    compaction: CompactionPolicy = Field(default_factory=CompactionPolicy)
    environment: dict[str, JsonValue]
    security_version: Literal["workspace/1"] = "workspace/1"

    @field_validator("environment")
    @classmethod
    def bounded_environment(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        check_json(value, max_bytes=8192)
        return value


class InputSelector(StrictModel):
    mode: Literal["head_at_dispatch", "revision"] = "head_at_dispatch"
    revision_id: Id | None = None

    @model_validator(mode="after")
    def consistent(self) -> Self:
        if (self.mode == "revision") != (self.revision_id is not None):
            raise ValueError("输入选择器不一致")
        return self


class CompletionCondition(StrictModel):
    id: Key
    kind: Literal["json_equal", "ast_symbols"]
    path: str
    pointer: str = ""
    expected: JsonValue = None
    symbols: tuple[str, ...] = Field(default=(), max_length=16)

    @field_validator("path")
    @classmethod
    def path_valid(cls, value: str) -> str:
        path_parts(value)
        return value

    @field_validator("expected")
    @classmethod
    def expected_valid(cls, value: JsonValue) -> JsonValue:
        check_json(value, max_bytes=4096)
        return value


class Completion(StrictModel):
    profile: Literal["python-unittest/1"] = "python-unittest/1"
    required_paths: tuple[str, ...] = Field(default=(), max_length=16)
    conditions: tuple[CompletionCondition, ...] = Field(default=(), max_length=16)

    @model_validator(mode="after")
    def bounded(self) -> Self:
        for path in self.required_paths:
            path_parts(path)
        if len(set(self.required_paths)) != len(self.required_paths):
            raise ValueError("重复路径")
        if len(self.required_paths) + len(self.conditions) > 16:
            raise ValueError("完成条件过多")
        if len({v.id for v in self.conditions}) != len(self.conditions):
            raise ValueError("重复条件")
        return self


class WorkspaceTurnRequest(StrictModel):
    schema_version: Literal[1] = 1
    request_key: Key
    text: str = Field(min_length=1, max_length=32768)
    expected_revision: int = Field(ge=0)
    input: InputSelector = Field(default_factory=InputSelector)
    completion: Completion

    @model_validator(mode="after")
    def bounded(self) -> Self:
        if not self.text.strip() or len(self.text.encode()) > 32768:
            raise ValueError("输入为空或超限")
        if len(encoded(self.model_dump(mode="json")).encode()) > 65536:
            raise ValueError("输入编码超限")
        return self


class InputFile(StrictModel):
    path: str
    encoding: Literal["base64"] = "base64"
    data: str
    sha256: Digest

    def content(self) -> bytes:
        try:
            value = base64.b64decode(self.data, validate=True)
        except ValueError:
            raise ServiceError("artifact_invalid", 422) from None
        if base64.b64encode(value).decode() != self.data:
            raise ServiceError("artifact_invalid", 422)
        if hashlib.sha256(value).hexdigest() != self.sha256:
            raise ServiceError("artifact_invalid", 422)
        return value

    @model_validator(mode="after")
    def bounded(self) -> Self:
        path_parts(self.path)
        if len(self.content()) > 32768:
            raise ValueError("输入文件超限")
        return self


class InputSnapshotRequest(StrictModel):
    schema_version: Literal[1] = 1
    request_key: Key
    expected_head_revision: int = Field(ge=0)
    files: tuple[InputFile, ...] = Field(max_length=32)

    @model_validator(mode="after")
    def bounded(self) -> Self:
        if len({f.path for f in self.files}) != len(self.files):
            raise ValueError("重复路径")
        if sum(len(f.content()) for f in self.files) > 32768:
            raise ValueError("输入bundle超限")
        if len(encoded(self.model_dump(mode="json")).encode()) > 65536:
            raise ValueError("输入编码超限")
        return self

    def request_hash(self) -> str:
        return fingerprint(
            {
                "schema_version": self.schema_version,
                "request_key": self.request_key,
                "expected_head_revision": self.expected_head_revision,
                "files": sorted(
                    [
                        {"path": f.path, "size": len(f.content()), "sha256": f.sha256}
                        for f in self.files
                    ],
                    key=lambda f: str(f["path"]),
                ),
            }
        )


class ArtifactFile(StrictModel):
    path: str
    size: int = Field(ge=0, le=1048576)
    sha256: Digest

    @field_validator("path")
    @classmethod
    def valid_path(cls, value: str) -> str:
        path_parts(value)
        return value


class ArtifactRevision(StrictModel):
    revision_id: Id
    session_id: Id
    parent_id: Id | None
    revision: int = Field(ge=0)
    source_kind: Literal["input", "effect"]
    request_key: Key
    request_hash: Digest
    manifest: tuple[ArtifactFile, ...] = Field(max_length=32)
    manifest_hash: Digest
    producer_run_id: Id | None
    producer_call_id: Id | None


class ArtifactChunk(StrictModel):
    revision_id: Id
    path: str
    offset: int = Field(ge=0)
    total_size: int = Field(ge=0, le=1048576)
    length: int = Field(ge=0, le=16384)
    sha256: Digest
    data: str


class GrantSnapshot(StrictModel):
    actor_id: str = Field(min_length=1, max_length=64)
    generation: int = Field(ge=1)
    workspace_id: str = Field(min_length=1, max_length=64)
    operations: tuple[Literal["read", "write", "exec", "approve"], ...]
    policy_version: Literal["workspace/1"] = "workspace/1"


@dataclass(frozen=True)
class ExecutionAccess:
    principal: PrincipalContext
    workspace_id: str
    session_id: str

    def grant(self) -> GrantSnapshot:
        member = self.principal.member(self.workspace_id)
        operations = ["read"]
        if member.role != "viewer":
            operations += ["write", "exec"]
            if member.approve:
                operations += ["approve"]
        return GrantSnapshot.model_validate_json(
            encoded(
                {
                    "actor_id": self.principal.principal_id,
                    "generation": self.principal.generation,
                    "workspace_id": self.workspace_id,
                    "operations": operations,
                }
            )
        )

    def authorize(self, workspace: str, owner: str, session: str, *, write: bool = False) -> None:
        member = self.principal.member(workspace, write=write)
        if (
            self.workspace_id != workspace
            or self.session_id != session
            or self.principal.principal_id != owner
            and member.role != "admin"
        ):
            raise ServiceError("not_found", 404)


class WorkFrame(StrictModel):
    session_id: Id
    turn_id: Id
    actor_id: str
    grant: GrantSnapshot
    original_request: WorkspaceTurnRequest
    input_revision: ArtifactRevision
    source_refs: tuple[dict[str, JsonValue], ...]
    subject: dict[str, JsonValue]
    conditions: dict[str, JsonValue]
    effective_fingerprint: Digest
    phase: str
    verification: dict[str, JsonValue] | None


class EffectIntent(StrictModel):
    call_id: Id
    run_id: Id
    operation_key: Key
    session_id: Id
    turn_id: Id
    args_hash: Digest
    tool_hash: Digest
    grant_hash: Digest
    input_revision: Id
    kind: Literal["read", "write", "command"]


class EffectReceipt(StrictModel):
    executor_started: bool | None
    exit_code: int | None
    output_complete: bool | None
    cleanup: Literal["pending", "clean", "unknown"]
    code: str | None = None
    artifact_revision_id: Id | None = None


class PlanStep(StrictModel):
    id: Key
    text: str = Field(min_length=1, max_length=1024)
    status: Literal["pending", "running", "completed", "failed"]
    proposed_by: Literal["model", "user"]


class Plan(StrictModel):
    revision: int = Field(ge=0)
    steps: tuple[PlanStep, ...] = Field(max_length=8)


class Verification(StrictModel):
    state: Literal["pending", "passed", "failed", "unknown"]
    revision_id: Id | None
    command_call_id: Id | None
    results: tuple[dict[str, JsonValue], ...] = Field(max_length=16)


class PublicEffect(EffectReceipt):
    call_id: Id
    kind: Literal["read", "write", "command"]
    phase: Literal["intent", "dispatch_intent", "settled", "unknown"]
    result_available: bool | None


class SummaryView(StrictModel):
    pass_index: int = Field(ge=0, le=1)
    status: Literal["pending", "completed", "failed", "interrupted"]
    source_digest: Digest
    model_step: int = Field(ge=0, le=11)
    summary_hash: Digest | None
    summary: str | None

    @field_validator("summary")
    @classmethod
    def bounded_summary(cls, value: str | None) -> str | None:
        if value is not None and len(value.encode()) > 2048:
            raise ValueError("摘要超限")
        return value


class WorkView(StrictModel):
    schema_version: Literal[1] = 1
    session_id: Id
    turn_id: Id
    run_id: Id | None
    request_key: Key
    original_request: WorkspaceTurnRequest
    requested_input: InputSelector
    resolved_input_revision: Id | None
    phase: Literal["queued", "preparing", "running", "verifying", "settled", "interrupted"]
    plan: Plan
    compactions: tuple[SummaryView, ...] = Field(max_length=2)
    effects: tuple[PublicEffect, ...] = Field(max_length=32)
    verification: Verification | None
    effective_fingerprint: Digest | None
