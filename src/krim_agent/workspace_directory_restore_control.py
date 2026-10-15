"""目录恢复内部叶协议：参数只引用原操作，回程只含已捕获的安全事实。"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.models import StrictModel
from krim_agent.workspace_directory_restore import (
    FINGERPRINT,
    IDENTIFIER,
    DirectoryRestoreReceiptView,
)
from krim_agent.workspace_tree_snapshot import _path

DirectoryRestoreStepKind = Literal["prepare", "execute", "verify"]
DIRECTORY_RESTORE_STEP_TOOLS: dict[str, str] = {
    "prepare": "workspace_directory_restore_prepare",
    "execute": "workspace_directory_restore_apply",
    "verify": "workspace_directory_restore_verify",
}


class DirectoryRestoreStepInput(StrictModel):
    plan_id: str = Field(pattern=IDENTIFIER)
    operation_id: str = Field(pattern=IDENTIFIER)


class DirectoryRestoreStepView(DirectoryRestoreStepInput):
    source: Literal["workspace_directory_restore_step"] = "workspace_directory_restore_step"
    kind: DirectoryRestoreStepKind
    root_path: str = Field(min_length=1, max_length=512)
    target_fingerprint: str = Field(pattern=FINGERPRINT)
    baseline_fingerprint: str = Field(pattern=FINGERPRINT)
    receipt: DirectoryRestoreReceiptView | None

    @model_validator(mode="after")
    def original_step(self) -> DirectoryRestoreStepView:
        _path(self.root_path)
        if (self.kind == "prepare") != (self.plan_id == self.operation_id):
            raise ValueError("准备与独立执行必须保留原操作身份")
        if (self.kind == "prepare") != (self.receipt is None):
            raise ValueError("准备只能声明备份，执行与核实必须保留真实回执")
        if self.receipt is not None and (
            self.receipt.root_path != self.root_path
            or self.receipt.before_fingerprint != self.baseline_fingerprint
            or self.receipt.target_fingerprint != self.target_fingerprint
        ):
            raise ValueError("目录叶回程与原完整材料不匹配")
        return self
