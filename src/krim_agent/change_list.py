"""Change List 有限数据合约；真实执行与不确定回执见 ADR-0035。"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Annotated, Literal

from pydantic import Field, JsonValue, model_validator

from .protocol.models import StrictModel, check_json, utc_now

ChangeID = Annotated[str, Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")]
ChangeAction = Literal["undo", "keep", "undo-all", "keep-all"]
ChangeItemStatus = Literal["pending", "executing", "undone", "kept", "uncertain"]


class ChangeItemInput(StrictModel):
    id: ChangeID
    title: str = Field(min_length=1, max_length=120)
    description: str = Field(default="", max_length=2048)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def bounded(self) -> ChangeItemInput:
        if not self.title.strip():
            raise ValueError("变更标题不能为空白")
        check_json(self.metadata, max_bytes=8192)
        return self


class ChangeItem(ChangeItemInput):
    status: ChangeItemStatus = "pending"


class ChangeRevision(StrictModel):
    revision: int = Field(ge=0, le=9007199254740991)


class ChangeSync(ChangeRevision):
    items: list[ChangeItemInput] = Field(default_factory=list, max_length=32)

    @model_validator(mode="after")
    def unique_items(self) -> ChangeSync:
        if len({item.id for item in self.items}) != len(self.items):
            raise ValueError("变更条目 ID 不能重复")
        check_json(self.model_dump(mode="json"), max_bytes=32768)
        return self


class ChangeListView(ChangeRevision):
    topic_id: ChangeID
    items: list[ChangeItem] = Field(default_factory=list, max_length=32)
    active_operation_id: ChangeID | None = None


class ChangeInvoke(ChangeRevision):
    action: ChangeAction
    item_id: ChangeID | None = None
    idempotency_key: ChangeID
    host_session_id: ChangeID | None = None
    timeout_seconds: float = Field(default=10, gt=0, le=300)

    @model_validator(mode="after")
    def target(self) -> ChangeInvoke:
        if (self.action in {"undo", "keep"}) != (self.item_id is not None):
            raise ValueError("单条动作需要 item_id，批量动作不能指定 item_id")
        return self

    def fingerprint(self) -> str:
        # 传输连接/超时不改变业务身份；原结果可在更换连接后查询，绝不自动重放。
        payload = self.model_dump(exclude={"host_session_id", "timeout_seconds"})
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


class ChangeExecution(ChangeRevision):
    operation_id: ChangeID
    topic_id: ChangeID
    action: ChangeAction
    items: list[ChangeItemInput] = Field(min_length=1, max_length=32)


class ChangeItemResult(StrictModel):
    item_id: ChangeID
    status: Literal["done", "error", "uncertain"]
    error_code: ChangeID | None = Field(default=None, max_length=64)

    @model_validator(mode="after")
    def outcome(self) -> ChangeItemResult:
        if (self.status == "done") != (self.error_code is None):
            raise ValueError("done 不含错误码，error/uncertain 必须包含稳定错误码")
        return self


class ChangeReceipt(ChangeRevision):
    operation_id: ChangeID
    topic_id: ChangeID
    action: ChangeAction
    results: list[ChangeItemResult] = Field(min_length=1, max_length=32)

    @model_validator(mode="after")
    def unique_results(self) -> ChangeReceipt:
        if len({item.item_id for item in self.results}) != len(self.results):
            raise ValueError("变更回执目标不能重复")
        return self

    def matches(self, execution: ChangeExecution) -> bool:
        return (
            self.operation_id == execution.operation_id
            and self.topic_id == execution.topic_id
            and self.revision == execution.revision
            and self.action == execution.action
            and {result.item_id for result in self.results} == {item.id for item in execution.items}
        )

    @classmethod
    def uncertain(cls, execution: ChangeExecution, code: str) -> ChangeReceipt:
        return cls(
            **execution.model_dump(exclude={"items"}),
            results=[
                ChangeItemResult(item_id=item.id, status="uncertain", error_code=code)
                for item in execution.items
            ],
        )


class ChangeOperation(StrictModel):
    idempotency_key: ChangeID
    execution: ChangeExecution
    status: Literal["executing", "done", "error", "uncertain"]
    receipt: ChangeReceipt | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class ChangeClick(ChangeRevision):
    item_id: ChangeID
    host_session_id: ChangeID | None = None


class ChangeItemClick(ChangeRevision):
    topic_id: ChangeID
    item: ChangeItem


class ChangeTopic(StrictModel):
    topic_id: ChangeID


class ChangeSyncRequest(ChangeSync, ChangeTopic):
    pass


class ChangeClearRequest(ChangeRevision, ChangeTopic):
    pass


class ChangeInvokeRequest(ChangeInvoke, ChangeTopic):
    pass


class ChangeClickRequest(ChangeClick, ChangeTopic):
    pass


class ChangeOperationRequest(ChangeTopic):
    operation_id: ChangeID
