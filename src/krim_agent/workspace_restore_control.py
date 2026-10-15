"""模型恢复只读原身份与有界历史，内部来源不在公开Schema中。"""

from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.models import StrictModel
from krim_agent.workspace_restore import CheckpointRestoreView


class RestoreLookup(StrictModel):
    plan_id: str | None = Field(
        default=None, min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,35}$"
    )
    request_key: str | None = Field(
        default=None, min_length=1, max_length=128, pattern=r"^[^\x00-\x1f]+$"
    )

    @model_validator(mode="after")
    def one_identity(self) -> "RestoreLookup":
        if (self.plan_id is None) == (self.request_key is None):
            raise ValueError("必须选择一个原恢复计划ID或预览请求键")
        return self


class RestoreQuery(RestoreLookup):
    kind: Literal["id", "key", "history"]
    offset: int = Field(default=0, ge=0, le=5000)
    limit: int = Field(default=32, ge=1, le=32)

    @model_validator(mode="after")
    def one_identity(self) -> "RestoreQuery":
        if self.kind == "id":
            valid = self.plan_id is not None and self.request_key is None
        elif self.kind == "key":
            valid = self.request_key is not None and self.plan_id is None
        else:
            valid = self.request_key is None and self.plan_id is None
        if not valid or self.kind != "history" and (self.offset != 0 or self.limit != 32):
            raise ValueError("恢复查询的种类、原身份或分页参数不一致")
        return self


class RestoreQueryResult(StrictModel):
    kind: Literal["id", "key", "history"]
    plan: CheckpointRestoreView | None = None
    entries: list[CheckpointRestoreView] = Field(default_factory=list, max_length=32)
