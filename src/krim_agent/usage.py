"""提取原 usage 的有限计数/未知语义；不推算缺失用量。"""

from pydantic import Field, model_validator

from krim_agent.contracts import StrictModel


class TokenUsage(StrictModel):
    input_tokens: int | None = Field(default=None, ge=0, le=9007199254740991)
    output_tokens: int | None = Field(default=None, ge=0, le=9007199254740991)
    total_tokens: int | None = Field(default=None, ge=0, le=9007199254740991)
    # 输入/输出内部的子集，不能再次加到总量或上下文面积上。
    cached_input_tokens: int | None = Field(default=None, ge=0, le=9007199254740991)
    cache_write_input_tokens: int | None = Field(default=None, ge=0, le=9007199254740991)
    reasoning_output_tokens: int | None = Field(default=None, ge=0, le=9007199254740991)

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
        for detail, total in ((self.cached_input_tokens, self.input_tokens),
                              (self.cache_write_input_tokens, self.input_tokens),
                              (self.reasoning_output_tokens, self.output_tokens)):
            bound = total if total is not None else self.total_tokens
            if detail is not None and bound is not None and detail > bound:
                raise ValueError("用量子计数超过所属总量")
        return self

    def follows(self, old: "TokenUsage") -> bool:
        return all(
            getattr(old, k) is None
            or (getattr(self, k) is not None and getattr(self, k) >= getattr(old, k))
            for k in type(self).model_fields
        )
