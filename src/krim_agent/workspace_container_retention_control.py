"""回收私有Host叶的选择器与回程；当前Model调用不得降级为Host。"""

from dataclasses import dataclass, field
from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.models import StrictModel
from krim_agent.workspace_container_retention import (
    ContainerRetentionPreview,
    ContainerRetentionReceipt,
)
from krim_agent.workspace_directory_restore import IDENTIFIER

CONTAINER_RETENTION_STEP_TOOLS = {
    "inspect": "workspace_container_retention_inspect",
    "remove": "workspace_container_retention_remove",
    "verify": "workspace_container_retention_verify",
}
RetentionStepKind = Literal["inspect", "remove", "verify"]


@dataclass(frozen=True)
class ContainerRetentionInspection:
    inspection_id: str
    source_plan_id: str
    token: str = field(repr=False)
    kind: Literal["inspect"] = "inspect"


class ContainerRetentionInspectInput(StrictModel):
    source_plan_id: str = Field(pattern=IDENTIFIER, max_length=36)


class ContainerRetentionOperationInput(StrictModel):
    operation_id: str = Field(pattern=IDENTIFIER, max_length=36)


class ContainerRetentionStepView(StrictModel):
    kind: Literal["remove", "verify"]
    operation_id: str = Field(pattern=IDENTIFIER, max_length=36)
    receipt: ContainerRetentionReceipt | None
    observation: ContainerRetentionPreview | None

    @model_validator(mode="after")
    def single_actual_result(self) -> "ContainerRetentionStepView":
        if self.kind == "remove":
            if self.receipt is None or self.observation is not None:
                raise ValueError("回收叶必须有原真实receipt且不能混入观察")
        elif (
            self.receipt is not None
            or self.observation is None
            or self.observation.existing_operation_id != self.operation_id
        ):
            raise ValueError("核实叶必须保留原操作只读观察")
        return self
