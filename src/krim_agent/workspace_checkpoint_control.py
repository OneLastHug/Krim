"""模型检查点只接公开规范路径/原ID，来源与私有Fence不属于输入Schema。"""

from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.models import StrictModel
from krim_agent.workspace_checkpoint import WorkspaceCheckpointView


class CheckpointLookup(StrictModel):
    checkpoint_id: str | None = Field(
        default=None, min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,35}$"
    )
    request_key: str | None = Field(
        default=None, min_length=1, max_length=128, pattern=r"^[^\x00-\x1f]+$"
    )

    @model_validator(mode="after")
    def one_identity(self) -> "CheckpointLookup":
        if (self.checkpoint_id is None) == (self.request_key is None):
            raise ValueError("必须选择一个原检查点ID或请求键")
        return self


class CheckpointQuery(StrictModel):
    kind: Literal["id", "key", "history"]
    checkpoint_id: str | None = Field(
        default=None, min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,35}$"
    )
    request_key: str | None = Field(
        default=None, min_length=1, max_length=128, pattern=r"^[^\x00-\x1f]+$"
    )
    offset: int = Field(default=0, ge=0, le=5000)
    limit: int = Field(default=32, ge=1, le=32)

    @model_validator(mode="after")
    def one_query(self) -> "CheckpointQuery":
        if self.kind == "id":
            valid = self.checkpoint_id is not None and self.request_key is None
        elif self.kind == "key":
            valid = self.request_key is not None and self.checkpoint_id is None
        else:
            valid = self.request_key is None and self.checkpoint_id is None
        if not valid or self.kind != "history" and (self.offset != 0 or self.limit != 32):
            raise ValueError("检查点查询的种类/原身份/分页参数不一致")
        return self


class CheckpointQueryResult(StrictModel):
    kind: Literal["id", "key", "history"]
    checkpoint: WorkspaceCheckpointView | None = None
    entries: list[WorkspaceCheckpointView] = Field(default_factory=list, max_length=32)
