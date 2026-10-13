"""原完成生成树根撤销独立请求/来源；不扩旧Move公开Schema。"""

from dataclasses import dataclass

from pydantic import Field, model_validator

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.workspace_tree_snapshot import _path


@dataclass(frozen=True)
class GeneratedTreeUndoSource:
    plan_id: str
    previous_restore_operation_id: str | None = None


class GeneratedTreeUndoPreviewRequest(StrictModel):
    plan_id: str = Field(min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    preserve_path: str = Field(min_length=1, max_length=512)
    expected_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[^\x00-\x1f\x7f]+$")
    ttl_seconds: float = Field(default=1800, gt=0, le=86400, allow_inf_nan=False)
    previous_restore_operation_id: str | None = Field(
        default=None, min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$"
    )

    @model_validator(mode="after")
    def canonical(self) -> "GeneratedTreeUndoPreviewRequest":
        try:
            _path(self.preserve_path)
        except ServiceError as error:
            raise ValueError(str(error)) from error
        return self
