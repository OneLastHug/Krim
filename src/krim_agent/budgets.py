"""持久累计预算的公开 Python 合约；成本来自显式费率，见 ADR-0042。"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import ConfigDict, Field, model_validator

from .protocol.models import StrictModel
from .usage import TokenUsage

BudgetCount = Annotated[int, Field(ge=0, le=9_007_199_254_740_991)]
BudgetOwner = Literal["runtime", "agent", "workflow"]
BudgetState = Literal["reserved", "dispatched", "settled", "uncertain", "released"]


class TokenPrice(StrictModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    input_micros_per_million: BudgetCount
    output_micros_per_million: BudgetCount

    def cost(self, input_tokens: int, output_tokens: int) -> int:
        if (
            type(input_tokens) is not int
            or type(output_tokens) is not int
            or min(input_tokens, output_tokens) < 0
        ):
            raise ValueError("费用计算需要非负整数 token")
        numerator = (
            input_tokens * self.input_micros_per_million
            + output_tokens * self.output_micros_per_million
        )
        return (numerator + 999_999) // 1_000_000


class UsageBudgetConfig(StrictModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    max_total_tokens: BudgetCount | None = None
    max_cost_micros: BudgetCount | None = None
    price: TokenPrice | None = None

    @model_validator(mode="after")
    def validate_limits(self) -> UsageBudgetConfig:
        if self.max_total_tokens is None and self.max_cost_micros is None:
            raise ValueError("累计预算至少需要 token 或费用上限")
        if self.max_cost_micros is not None and self.price is None:
            raise ValueError("费用上限需要显式币种与费率")
        return self


class BudgetAllocation(StrictModel):
    account_id: str
    reserved_tokens: BudgetCount
    reserved_cost_micros: BudgetCount | None = None
    charged_tokens: BudgetCount | None = None
    charged_cost_micros: BudgetCount | None = None
    currency: str | None = None


class BudgetRequestView(StrictModel):
    request_id: str
    run_id: str
    step: int = Field(ge=0, le=31)
    state: BudgetState
    revision: int = Field(ge=0)
    input_tokens: BudgetCount
    output_reserve: BudgetCount
    reported_usage: TokenUsage | None = None
    source: Literal["provider", "host_resolution"] | None = None
    receipt: str | None = None
    allocations: list[BudgetAllocation]
    overdrawn: bool = False


class BudgetAccountView(StrictModel):
    account_id: str
    owner_kind: BudgetOwner
    owner_id: str
    config: UsageBudgetConfig
    revision: int = Field(ge=0)
    spent_tokens: BudgetCount
    held_tokens: BudgetCount
    spent_cost_micros: BudgetCount | None = None
    held_cost_micros: BudgetCount | None = None
    uncertain_requests: int = Field(ge=0)
    overdrawn: bool = False


class BudgetResolveRequest(StrictModel):
    revision: int = Field(ge=0)
    usage: TokenUsage
    receipt: str = Field(min_length=1, max_length=256)
