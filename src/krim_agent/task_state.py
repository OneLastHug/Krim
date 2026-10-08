"""计划/待办数据合约；不依赖运行时、仓储或工具模块。"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import Field, model_validator

from krim_agent.protocol.models import StrictModel

TaskKind = Literal["plan", "todo"]
TaskText = Annotated[str, Field(min_length=1, max_length=200)]


class TaskItemView(StrictModel):
    id: str
    content: str
    completed: bool = False
    position: int = Field(ge=0)


class TaskSnapshot(StrictModel):
    scope_id: str
    kind: TaskKind
    revision: int = Field(ge=0)
    items: list[TaskItemView] = Field(max_length=100)


class TaskMutation(StrictModel):
    operation: Literal["replace", "add", "complete", "remove", "update"]
    items: list[TaskText] = Field(default_factory=list, max_length=100)
    item_id: str = Field(default="", max_length=128)
    content: str = Field(default="", max_length=200)
    completed: bool | None = None
    expected_revision: int | None = Field(default=None, ge=0)
    request_key: str = Field(min_length=1, max_length=128)

    @model_validator(mode="after")
    def operation_fields(self) -> TaskMutation:
        if self.operation in {"add", "replace"} and self.item_id:
            raise ValueError("新增/替换任务不能指定既有item_id")
        if self.content and not self.content.strip():
            raise ValueError("任务内容不能为空白")
        if self.operation == "add" and not self.content.strip():
            raise ValueError("新增任务内容不能为空")
        if any(not item.strip() for item in self.items):
            raise ValueError("任务内容不能为空白")
        if self.operation in {"complete", "remove", "update"} and not self.item_id:
            raise ValueError("任务操作需要item_id")
        if self.operation == "update" and not self.content.strip() and self.completed is None:
            raise ValueError("任务更新需要内容或完成状态")
        return self


class TaskWriteRequest(TaskMutation):
    expected_revision: int = Field(ge=0)


class TaskOperationView(StrictModel):
    request_key: str
    revision: int
    operation: str
    run_id: str | None
    call_id: str | None
    created_at: datetime
