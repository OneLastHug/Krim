"""Goal wire contracts shared by tools, HTTP, context projection and continuation."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, field_validator

from krim_agent.contracts import StrictModel
from krim_agent.planning.contracts import PlanCounts, plan_digest

GoalStatus = Literal["active", "paused", "blocked", "completed", "cancelled"]
# Host-owned continuation state; a queued continuation is never a user authorization.
ContinuationStatus = Literal["queued", "consumed", "superseded", "failed"]
EvidenceKind = Literal[
    "tool_call", "test", "command", "file", "url", "user_message", "observation", "other"
]
TURN_KEY_PATTERN = r"^[!-~]+$"


def goal_digest(value: object) -> str:
    """Streaming canonical digest without the generic encoder's size ceiling."""
    return plan_digest(value)


def empty_goal_digest() -> str:
    return goal_digest({"version": "goals/1", "goal": None})


class EvidenceRef(StrictModel):
    kind: EvidenceKind = "other"
    ref: str = Field(min_length=1)
    note: str = ""

    @field_validator("ref")
    @classmethod
    def meaningful_ref(cls, value: str) -> str:
        ref = value.strip()
        if not ref:
            raise ValueError("证据引用不能为空")
        return ref

    @property
    def identity(self) -> tuple[str, str]:
        return (self.kind, self.ref)


class GoalUpdate(StrictModel):
    """Mutation body. Tools may only use ``goal.TOOL_ACTIONS``; pause/resume are owner API.

    Keeping one strict model for tool and owner paths means the idempotent request
    digest, CAS and evidence rules cannot diverge between callers.
    """

    request_key: str = Field(min_length=1, max_length=128, pattern=TURN_KEY_PATTERN)
    expected_revision: int = Field(ge=0)
    action: Literal["set", "progress", "pause", "resume", "block", "complete", "cancel"]
    objective: str | None = None
    completion_criteria: tuple[str, ...] = ()
    evidence_refs: tuple[EvidenceRef, ...] = ()
    note: str = ""
    reason: str = ""

    @field_validator("completion_criteria", mode="before")
    @classmethod
    def json_criteria(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @field_validator("evidence_refs", mode="before")
    @classmethod
    def json_evidence(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @field_validator("objective")
    @classmethod
    def meaningful_objective(cls, value: str | None) -> str | None:
        if value is None:
            return None
        objective = value.strip()
        if not objective:
            raise ValueError("目标说明不能为空")
        return objective

    @field_validator("completion_criteria")
    @classmethod
    def meaningful_criteria(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _clean_criteria(value)


def _clean_criteria(value: tuple[str, ...]) -> tuple[str, ...]:
    cleaned = []
    for item in value:
        if not isinstance(item, str):
            raise ValueError("完成条件必须是字符串")
        text = item.strip()
        if not text:
            raise ValueError("完成条件不能为空")
        cleaned.append(text)
    if len(set(cleaned)) != len(cleaned):
        raise ValueError("完成条件重复")
    return tuple(cleaned)


class GoalControlRequest(StrictModel):
    """Owner API body for pause/resume/cancel; direct user authority, no turn binding."""

    request_key: str = Field(min_length=1, max_length=128, pattern=TURN_KEY_PATTERN)
    expected_revision: int = Field(ge=0)
    reason: str = ""


class GoalCreateRequest(StrictModel):
    """Owner API creation/replacement, bound by the service to a real user turn.

    ``origin_turn_id`` is optional: the service picks the latest non-internal
    user turn of the session and verifies it. A replacement is allowed only
    while the current goal is paused or blocked, so an active goal cannot be
    silently redirected by the model or a stale UI.
    """

    request_key: str = Field(min_length=1, max_length=128, pattern=TURN_KEY_PATTERN)
    expected_revision: int = Field(ge=0)
    objective: str = Field(min_length=1)
    completion_criteria: tuple[str, ...] = ()
    evidence_refs: tuple[EvidenceRef, ...] = ()
    note: str = ""
    origin_turn_id: str | None = None
    origin_run_id: str | None = None

    @field_validator("completion_criteria", mode="before")
    @classmethod
    def json_criteria(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @field_validator("evidence_refs", mode="before")
    @classmethod
    def json_evidence(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @field_validator("objective")
    @classmethod
    def meaningful_objective(cls, value: str) -> str:
        objective = value.strip()
        if not objective:
            raise ValueError("目标说明不能为空")
        return objective

    @field_validator("completion_criteria")
    @classmethod
    def meaningful_criteria(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _clean_criteria(value)


class GoalRead(StrictModel):
    revision: int | None = Field(default=None, ge=0)


class GoalContinuationView(StrictModel):
    continue_key: str
    request_key: str
    status: ContinuationStatus
    internal: bool
    goal_revision_base: int
    source_turn_id: str | None
    source_run_id: str | None
    origin_turn_id: str
    origin_run_id: str
    turn_id: str | None = None
    outcome: str | None = None
    code: str | None = None
    goal_revision: int | None = None
    progressed: bool | None = None
    created_at: float
    updated_at: float


class GoalPlanBridge(StrictModel):
    """Read-only projection of the session plan; never evidence of goal completion."""

    revision: int = Field(ge=0)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    counts: PlanCounts
    total: int = Field(ge=0)


class GoalSnapshot(StrictModel):
    version: Literal["goals/1"] = "goals/1"
    goal_id: str | None
    session_id: str
    workspace_id: str
    owner_id: str | None
    revision: int = Field(ge=0)
    status: GoalStatus | None
    objective: str | None
    completion_criteria: tuple[str, ...] = ()
    evidence_refs: tuple[EvidenceRef, ...] = ()
    status_note: str | None = None
    blocked_reason: str | None = None
    origin_turn_id: str | None = None
    origin_run_id: str | None = None
    updated_turn_id: str | None = None
    updated_run_id: str | None = None
    created_at: float | None = None
    updated_at: float | None = None
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    continuation: GoalContinuationView | None = None
    plan: GoalPlanBridge | None = None
