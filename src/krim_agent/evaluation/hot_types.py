"""热观察的中立窄契约；键是幂等身份，不是授权凭据。"""

from __future__ import annotations

import base64
import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal, Protocol, Self

from pydantic import Field, model_validator

from krim_agent.contracts import StrictModel

from .continuous_types import Feedback, ImportRequest, SourceSelection
from .domain import Digest, Identity, clone, neutral_encoded, neutral_fingerprint, relative_file

KEY_ENCODING = "hot-key/1-sha256-base64url-nopad"
FeedbackReason = Literal["incorrect", "unsafe", "missing_context", "other"]


def delivery_key(
    source_id: str,
    capture_id: str,
    destination_id: str,
    scope: str,
    kind: Literal["base", "feedback"],
    actor: str,
    feedback_key: str = "base",
) -> str:
    body = {
        "encoding": KEY_ENCODING,
        "source_id": source_id,
        "capture_id": capture_id,
        "destination_id": destination_id,
        "scope": scope,
        "kind": kind,
        "actor": actor,
        "feedback_key": feedback_key,
    }
    digest = hashlib.sha256(neutral_encoded(body, max_bytes=4096).encode()).digest()
    return "hot1." + base64.urlsafe_b64encode(digest).decode().rstrip("=")


class CapturePolicy(StrictModel):
    schema_version: Literal["hot-capture/1"] = "hot-capture/1"
    workspace_id: Identity = "local"
    scope: Identity = "local"
    observer: Identity = "local"
    enabled: bool = False
    numerator: int = Field(default=1, ge=0, le=1000000)
    denominator: int = Field(default=1, ge=1, le=1000000)
    salt_id: Identity = "default"
    failure_override: bool = True
    feedback_override: bool = True
    pending_limit: int = Field(default=256, ge=1, le=256)
    retained_limit: int = Field(default=4096, ge=1, le=4096)
    artifact_paths: list[str] = Field(default_factory=list, max_length=16)
    include_text: bool = False
    include_arguments: bool = False

    @model_validator(mode="after")
    def bounded(self) -> Self:
        if self.numerator > self.denominator:
            raise ValueError("sampling_fraction")
        for path in self.artifact_paths:
            relative_file(path)
        if len(set(self.artifact_paths)) != len(self.artifact_paths):
            raise ValueError("artifact_paths")
        neutral_encoded(self.model_dump(mode="json"), max_bytes=4096)
        return self

    def fingerprint(self) -> str:
        return neutral_fingerprint(self.model_dump(mode="json"))

    def redaction(self) -> dict[str, Any]:
        return {
            "version": "continuous-redact/1",
            "artifact_paths": list(self.artifact_paths),
            "include_text": self.include_text,
            "include_arguments": self.include_arguments,
        }


class FeedbackSignal(StrictModel):
    key: Identity
    reason: FeedbackReason


class FeedbackEvent(StrictModel):
    revision: int = Field(ge=1, le=8)
    actor: Identity
    generation: int = Field(ge=1)
    key: Identity
    reason: FeedbackReason


class CaptureRecord(StrictModel):
    schema_version: Literal["hot-reference/1"] = "hot-reference/1"
    capture_id: Digest
    source_id: str
    workspace_id: Identity
    scope: Identity
    run_id: str
    terminal_event_id: str
    terminal_seq: int = Field(ge=1)
    policy: CapturePolicy
    policy_fingerprint: Digest
    snapshot_hash: Digest
    feedback: list[FeedbackEvent] = Field(default_factory=list, max_length=8)

    @model_validator(mode="after")
    def bounded(self) -> Self:
        if self.policy_fingerprint != self.policy.fingerprint():
            raise ValueError("policy_hash")
        if self.scope != self.policy.scope or self.workspace_id != self.policy.workspace_id:
            raise ValueError("policy_scope")
        if [f.revision for f in self.feedback] != list(range(1, len(self.feedback) + 1)):
            raise ValueError("feedback_revision")
        neutral_encoded(self.model_dump(mode="json"), max_bytes=4096)
        return self


@dataclass(frozen=True)
class Claim:
    record: CaptureRecord
    generation: int
    revision: int
    token: str


@dataclass(frozen=True)
class ActorViewBinding:
    actor: str
    generation: int
    view: Any
    check: Callable[[], None]


class UnitReceipt(StrictModel):
    key: Identity
    request_hash: Digest
    actor: Identity
    feedback_key: Identity | None = None
    observation_id: str
    observation_hash: Digest
    candidate_id: str
    candidate_hash: Digest


def import_request(
    record: CaptureRecord, destination_id: str, actor: str, feedback: FeedbackEvent | None = None
) -> ImportRequest:
    key = delivery_key(
        record.source_id,
        record.capture_id,
        destination_id,
        record.scope,
        "base" if feedback is None else "feedback",
        actor,
        "base" if feedback is None else feedback.key,
    )
    return ImportRequest(
        scope=record.scope,
        request_key=key,
        source_alias="hot-krim",
        sources=[
            SourceSelection(
                run_id=record.run_id,
                feedback=None
                if feedback is None
                else Feedback(key=feedback.key, reason=feedback.reason),
            )
        ],
    )


def request_hash(request: ImportRequest, view: Any) -> str:
    return _unit_hash(
        request, clone({"sampling": view.sampling, "redaction": view.redaction}), view.actor
    )


def receipts_root(receipts: list[UnitReceipt]) -> str:
    return neutral_fingerprint(
        [r.model_dump(mode="json") for r in sorted(receipts, key=lambda r: r.key)]
    )


class OutboxPort(Protocol):
    def binding(self) -> tuple[object, ...]: ...
    async def claim(self, *, limit: int = 16) -> list[Claim]: ...
    async def units(self, claim: Claim) -> dict[str, Any]: ...
    async def save_unit(self, claim: Claim, key: str, value: dict[str, Any]) -> None: ...
    async def ack(self, claim: Claim, receipts: list[UnitReceipt]) -> None: ...


def _unit_hash(request: ImportRequest, policies: dict[str, Any], actor: str) -> str:
    """冻结请求的原指纹算法；恢复不读取最新策略来改造原请求。"""
    frozen = ImportRequest.model_validate(request.model_dump(mode="json"))
    return neutral_fingerprint(
        {
            "request": frozen.model_dump(mode="json"),
            "policies": clone(policies),
            "feedback_actor": actor
            if any(item.feedback is not None for item in frozen.sources)
            else None,
        }
    )
