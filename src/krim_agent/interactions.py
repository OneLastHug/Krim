"""工具审批和用户问题的中立 JSON 合约；回应不能修改工具参数。"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import Field, JsonValue, model_validator

from krim_agent.protocol.domain import View
from krim_agent.protocol.models import StrictModel

InteractionKind = Literal["approval", "question"]
InteractionStatus = Literal["pending", "approved", "denied", "answered", "cancelled", "expired"]


class InteractionView(View):
    id: str
    topic_id: str
    run_id: str
    call_id: str
    tool_name: str
    kind: InteractionKind
    arguments_hash: str
    payload: dict[str, JsonValue]
    status: InteractionStatus
    response: dict[str, JsonValue] | None
    expires_at: float
    created_at: datetime
    updated_at: datetime


class InteractionReply(StrictModel):
    action: Literal["approve", "deny", "answer"]
    answer: str | None = Field(default=None, min_length=1, max_length=4000)

    @model_validator(mode="after")
    def valid_answer(self) -> InteractionReply:
        if self.action == "answer":
            if self.answer is None or not self.answer.strip():
                raise ValueError("回答不能是空白")
        elif self.answer is not None:
            raise ValueError("审批回应不能带问题答案")
        return self
