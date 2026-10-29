"""工作区评测产品层的有界HTTP契约。"""

from __future__ import annotations

from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from krim_agent.contracts import StrictModel

from .continuous_types import SourceSelection
from .domain import CaseSpec, Digest, Identity

# 内部Dataset name还要容纳稳定工作区前缀（总长不超过Identity的64字符）。
DisplayName = Annotated[str, Field(min_length=1, max_length=38, pattern=r"^[A-Za-z0-9_.-]+$")]


class ProductDatasetSpec(StrictModel):
    name: DisplayName
    version: Identity
    cases: list[CaseSpec] = Field(min_length=1, max_length=100)


class RunImportRequest(StrictModel):
    request_key: Identity
    sources: list[SourceSelection] = Field(min_length=1, max_length=32)

    @model_validator(mode="after")
    def only_runs(self) -> Self:
        if any(source.run_id is None or source.task_id is not None for source in self.sources):
            raise ValueError("run_sources_only")
        return self


class ProductConfirmationRequest(StrictModel):
    action: Literal["confirm", "edit", "reject"] = "confirm"
    request_key: Identity
    expected_revision: int = Field(ge=0)
    expected_hash: Digest
    case: CaseSpec
    context: Literal["complete", "manual_reconstructed"]
    reason: str = Field(min_length=1, max_length=512)

    @model_validator(mode="after")
    def bounded_reason(self) -> Self:
        if not self.reason.strip() or len(self.reason.encode()) > 512:
            raise ValueError("confirmation_reason")
        return self


class FreezeConfirmedRequest(StrictModel):
    request_key: Identity
    name: DisplayName
    version: Identity
    confirmations: list[str] = Field(min_length=1, max_length=100)


class ProductComparisonRequest(StrictModel):
    baseline_id: Identity
    candidate_id: Identity
    baseline_hash: Digest
    candidate_hash: Digest
    allowed_subjects: list[Digest] = Field(default_factory=list, max_length=2)
