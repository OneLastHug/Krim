"""缺失恢复独立私有叶协议；原缺失摘要不冒充完整树备份。"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.models import StrictModel
from krim_agent.workspace_directory_restore import FINGERPRINT, IDENTIFIER
from krim_agent.workspace_directory_restore_control import DirectoryRestoreStepKind
from krim_agent.workspace_missing_directory_restore import MissingDirectoryRestoreReceiptView
from krim_agent.workspace_tree_snapshot import _path

MISSING_DIRECTORY_RESTORE_STEP_TOOLS: dict[str, str] = {
    "prepare": "workspace_missing_directory_restore_prepare",
    "execute": "workspace_missing_directory_restore_apply",
    "verify": "workspace_missing_directory_restore_verify",
}


class MissingDirectoryRestoreStepInput(StrictModel):
    plan_id: str = Field(pattern=IDENTIFIER)
    operation_id: str = Field(pattern=IDENTIFIER)


class MissingDirectoryRestoreStepView(MissingDirectoryRestoreStepInput):
    source: Literal["workspace_missing_directory_restore_step"] = (
        "workspace_missing_directory_restore_step"
    )
    kind: DirectoryRestoreStepKind
    root_path: str = Field(min_length=1, max_length=512)
    target_fingerprint: str = Field(pattern=FINGERPRINT)
    before_absence_fingerprint: str = Field(pattern=FINGERPRINT)
    receipt: MissingDirectoryRestoreReceiptView | None

    @model_validator(mode="after")
    def original_step(self) -> MissingDirectoryRestoreStepView:
        _path(self.root_path)
        if (self.kind == "prepare") != (self.plan_id == self.operation_id):
            raise ValueError("缺失恢复准备与独立执行必须保留原操作身份")
        if (self.kind == "prepare") != (self.receipt is None):
            raise ValueError("缺失准备只声明真实before，执行与核实须保留原回执")
        if self.receipt is not None and (
            self.receipt.root_path != self.root_path
            or self.receipt.before_absence_fingerprint != self.before_absence_fingerprint
            or self.receipt.target_fingerprint != self.target_fingerprint
        ):
            raise ValueError("缺失恢复叶回程与原缺失材料不匹配")
        return self
