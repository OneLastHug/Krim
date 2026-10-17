"""提取原 usage 的有限计数/未知语义；不推算缺失用量。"""

from pydantic import Field, model_validator

from krim_agent.contracts import StrictModel


class TokenUsage(StrictModel):
    input_tokens: int | None = Field(default=None, ge=0, le=9007199254740991)
    output_tokens: int | None = Field(default=None, ge=0, le=9007199254740991)
    total_tokens: int | None = Field(default=None, ge=0, le=9007199254740991)

    @model_validator(mode="after")
    def counts(self) -> "TokenUsage":
        reported = [value for value in (self.input_tokens, self.output_tokens) if value is not None]
        if not reported and self.total_tokens is None:
            raise ValueError("用量事件必须实际报告至少一个计数")
        if self.total_tokens is not None and sum(reported) > self.total_tokens:
            raise ValueError("已报告的部分计数超过总量")
        if (
            self.input_tokens is not None
            and self.output_tokens is not None
            and self.total_tokens is not None
            and self.input_tokens + self.output_tokens != self.total_tokens
        ):
            raise ValueError("用量合计不一致")
        return self

    def follows(self, old: "TokenUsage") -> bool:
        return all(
            getattr(old, k) is None
            or (getattr(self, k) is not None and getattr(self, k) >= getattr(old, k))
            for k in type(self).model_fields
        )
