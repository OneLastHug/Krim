"""缺失基线回退受控请求和严格独立查询投影。"""

from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.models import StrictModel
from krim_agent.workspace_directory_restore import IDENTIFIER, REQUEST_KEY
from krim_agent.workspace_missing_directory_rollback import (
    MissingDirectoryRollbackAttemptView,
    MissingDirectoryRollbackPage,
    MissingDirectoryRollbackView,
)
from krim_agent.workspace_tree_move import TreeMoveQueryKind


class MissingDirectoryRollbackCancel(StrictModel):
    proposal_id: str = Field(pattern=IDENTIFIER, max_length=36)


class MissingDirectoryRollbackQuery(StrictModel):
    kind: TreeMoveQueryKind
    identifier: str | None = Field(default=None, pattern=IDENTIFIER, max_length=36)
    request_key: str | None = Field(default=None, pattern=REQUEST_KEY)
    after: str | None = Field(default=None, pattern=IDENTIFIER, max_length=36)
    limit: int = Field(default=4, ge=1, le=4)

    @model_validator(mode="after")
    def original_selection(self) -> "MissingDirectoryRollbackQuery":
        if self.kind in {"proposal", "operation", "history"}:
            if self.identifier is None or self.request_key is not None:
                raise ValueError("回退单项查询须只提供原identifier")
        elif self.kind in {"by_key", "operation_by_key"}:
            if self.request_key is None or self.identifier is not None:
                raise ValueError("回退原键查询须只提供request_key")
        elif self.identifier is not None or self.request_key is not None:
            raise ValueError("回退列表只接受分页")
        if self.kind not in {"history", "proposals"} and (
            self.after is not None or self.limit != 4
        ):
            raise ValueError("回退单项不接受分页")
        return self


class MissingDirectoryRollbackHistory(StrictModel):
    items: list[MissingDirectoryRollbackAttemptView] = Field(max_length=4)
    next_after: str | None = Field(pattern=IDENTIFIER, max_length=36)


class MissingDirectoryRollbackQueryResult(StrictModel):
    kind: Literal["proposal", "operation", "by_key", "operation_by_key", "history", "proposals"]
    proposal: MissingDirectoryRollbackView | None = None
    operation: MissingDirectoryRollbackAttemptView | None = None
    history: MissingDirectoryRollbackHistory | None = None
    proposals: MissingDirectoryRollbackPage | None = None

    @model_validator(mode="after")
    def original_branch(self) -> "MissingDirectoryRollbackQueryResult":
        selected = {
            "by_key": "proposal",
            "operation_by_key": "operation",
        }.get(self.kind, self.kind)
        for name in ("proposal", "operation", "history", "proposals"):
            value = getattr(self, name)
            if name != selected and value is not None:
                raise ValueError("回退查询不能混入其他分支")
            if (
                name == selected
                and value is None
                and self.kind not in {"by_key", "operation_by_key"}
            ):
                raise ValueError("回退查询不能丢失原选定事实")
        if self.proposals is not None and len(self.proposals.items) > 4:
            raise ValueError("回退公共页超过输出数量边界")
        return self
