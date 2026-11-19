"""Session plan data shared by tools, HTTP, context projection and goals."""

from __future__ import annotations

import hashlib
import json
from typing import Annotated, Literal

from pydantic import Field, field_validator, model_validator

from krim_agent.contracts import StrictModel

PlanStatus = Literal["pending", "in_progress", "completed", "blocked"]
StepId = Annotated[str, Field(pattern=r"^[A-Za-z0-9_.:-]{1,128}$")]
PAGE_SIZE = 50
MAX_PAGE_SIZE = 200


def plan_digest(value: object) -> str:
    """Canonical content hash without the generic JSON encoder's size ceiling."""
    digest = hashlib.sha256()
    encoder = json.JSONEncoder(
        ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    for part in encoder.iterencode(value):
        digest.update(part.encode("utf-8"))
    return digest.hexdigest()


class PlanStep(StrictModel):
    id: StepId
    text: str = Field(min_length=1)
    status: PlanStatus = "pending"

    @model_validator(mode="after")
    def meaningful_text(self) -> PlanStep:
        if not self.text.strip():
            raise ValueError("计划步骤不能为空")
        return self


class PlanRead(StrictModel):
    revision: int | None = Field(default=None, ge=0)
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE)
    statuses: tuple[PlanStatus, ...] = ()

    @field_validator("statuses", mode="before")
    @classmethod
    def json_statuses(cls, value: object) -> object:
        # ToolExecutor validates the already-decoded JSON with strict=True.
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def unique_statuses(self) -> PlanRead:
        if len(set(self.statuses)) != len(self.statuses):
            raise ValueError("重复计划状态")
        if self.offset and self.revision is None:
            raise ValueError("继续分页时须指定计划版本")
        return self


class PlanUpdate(StrictModel):
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[!-~]+$")
    expected_revision: int = Field(ge=0)
    steps: tuple[PlanStep, ...]

    @field_validator("steps", mode="before")
    @classmethod
    def json_steps(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def unique_steps(self) -> PlanUpdate:
        if len({step.id for step in self.steps}) != len(self.steps):
            raise ValueError("计划步骤标识重复")
        return self


class PlanCounts(StrictModel):
    pending: int = Field(default=0, ge=0)
    in_progress: int = Field(default=0, ge=0)
    completed: int = Field(default=0, ge=0)
    blocked: int = Field(default=0, ge=0)


class PlanSnapshot(StrictModel):
    version: Literal["planning/1"] = "planning/1"
    session_id: str
    revision: int = Field(ge=0)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    updated_at: float | None
    counts: PlanCounts
    total: int = Field(ge=0)
    matched_total: int = Field(ge=0)
    steps: tuple[PlanStep, ...]
    offset: int = Field(ge=0)
    limit: int = Field(ge=1, le=MAX_PAGE_SIZE)
    next_offset: int | None
