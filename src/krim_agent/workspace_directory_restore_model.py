"""原模型目录恢复选择器与安全查询；私有材料/来源/预算不属于模型参数。"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.models import StrictModel
from krim_agent.workspace_directory_restore import (
    IDENTIFIER,
    REQUEST_KEY,
    DirectoryRestoreOperationView,
    DirectoryRestoreView,
)
from krim_agent.workspace_directory_restore_public import DirectoryRestoreDiffPage

DirectoryRestoreQueryKind = Literal["id", "key", "history", "diff", "operation", "operation_by_key"]


class DirectoryRestoreLookup(StrictModel):
    plan_id: str | None = Field(default=None, pattern=IDENTIFIER)
    request_key: str | None = Field(default=None, pattern=REQUEST_KEY)

    @model_validator(mode="after")
    def original_identity(self) -> DirectoryRestoreLookup:
        if (self.plan_id is None) == (self.request_key is None):
            raise ValueError("必须选择一个原目录恢复计划ID或预览键")
        return self


class DirectoryRestoreOperationLookup(StrictModel):
    operation_id: str | None = Field(default=None, pattern=IDENTIFIER)
    request_key: str | None = Field(default=None, pattern=REQUEST_KEY)

    @model_validator(mode="after")
    def original_operation(self) -> DirectoryRestoreOperationLookup:
        if (self.operation_id is None) == (self.request_key is None):
            raise ValueError("必须选择一个原执行操作ID或execute键")
        return self


class DirectoryRestoreQuery(StrictModel):
    kind: DirectoryRestoreQueryKind
    plan_id: str | None = Field(default=None, pattern=IDENTIFIER)
    operation_id: str | None = Field(default=None, pattern=IDENTIFIER)
    request_key: str | None = Field(default=None, pattern=REQUEST_KEY)
    offset: int = Field(default=0, ge=0, le=5000)
    limit: int = Field(default=32, ge=1, le=128)

    @model_validator(mode="after")
    def one_selection(self) -> DirectoryRestoreQuery:
        selection = (
            self.plan_id is not None,
            self.operation_id is not None,
            self.request_key is not None,
        )
        wanted = {
            "id": (True, False, False),
            "diff": (True, False, False),
            "key": (False, False, True),
            "history": (False, False, False),
            "operation": (False, True, False),
            "operation_by_key": (False, False, True),
        }[self.kind]
        if (
            selection != wanted
            or self.kind == "history"
            and self.limit > 32
            or (self.kind not in {"history", "diff"} and (self.offset != 0 or self.limit != 32))
        ):
            raise ValueError("目录恢复查询种类、原身份与有界分页不一致")
        return self


class DirectoryRestoreQueryResult(StrictModel):
    kind: DirectoryRestoreQueryKind
    plan: DirectoryRestoreView | None = None
    operation: DirectoryRestoreOperationView | None = None
    diff: DirectoryRestoreDiffPage | None = None
    entries: list[DirectoryRestoreView] = Field(default_factory=list, max_length=32)

    @model_validator(mode="after")
    def selected_result(self) -> DirectoryRestoreQueryResult:
        if self.kind in {"id", "key"}:
            valid = (
                self.operation is None
                and self.diff is None
                and not self.entries
                and (self.kind == "key" or self.plan is not None)
            )
        elif self.kind in {"operation", "operation_by_key"}:
            valid = (
                self.plan is None
                and self.diff is None
                and not self.entries
                and (self.kind == "operation_by_key" or self.operation is not None)
            )
        elif self.kind == "diff":
            valid = (
                self.diff is not None
                and self.plan is None
                and self.operation is None
                and not self.entries
            )
        else:
            valid = self.plan is None and self.operation is None and self.diff is None
        if not valid:
            raise ValueError("目录恢复回程必须匹配原查询种类")
        return self
