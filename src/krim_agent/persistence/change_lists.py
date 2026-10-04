"""SQLite 清单/执行账本；回执和状态同事务保存，外部副作用由宿主执行。"""

from __future__ import annotations

from datetime import UTC
from uuid import uuid4

from pydantic import JsonValue, TypeAdapter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.change_list import (
    ChangeExecution,
    ChangeInvoke,
    ChangeItem,
    ChangeItemInput,
    ChangeListView,
    ChangeOperation,
    ChangeReceipt,
    ChangeSync,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import utc_now

from .database import ChangeListRow, ChangeOperationRow, Database, TopicRow


class ChangeListRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    @staticmethod
    async def _require_topic(session: AsyncSession, topic_id: str) -> None:
        if await session.get(TopicRow, topic_id) is None:
            raise ServiceError("topic_not_found", "话题不存在", 404)

    @staticmethod
    def _view(row: ChangeListRow) -> ChangeListView:
        return ChangeListView(
            topic_id=row.topic_id,
            revision=row.revision,
            items=TypeAdapter(list[ChangeItem]).validate_python(row.items),
            active_operation_id=row.active_operation_id,
        )

    @staticmethod
    def _operation(row: ChangeOperationRow) -> ChangeOperation:
        return ChangeOperation(
            idempotency_key=row.idempotency_key,
            execution=ChangeExecution.model_validate(row.execution),
            status=row.status,  # type: ignore[arg-type]
            receipt=ChangeReceipt.model_validate(row.receipt) if row.receipt else None,
            created_at=row.created_at.replace(tzinfo=UTC),
            updated_at=row.updated_at.replace(tzinfo=UTC),
        )

    @staticmethod
    def _bump(row: ChangeListRow) -> None:
        if row.revision >= 9007199254740991:
            raise ServiceError("change_revision_limit", "变更清单版本已达上限", 409)
        row.revision += 1
        row.updated_at = utc_now()

    @staticmethod
    def _editable(row: ChangeListRow, revision: int) -> None:
        if row.revision != revision:
            raise ServiceError("change_revision_conflict", "变更清单已更新，请重新读取", 409)
        if row.active_operation_id is not None:
            raise ServiceError("change_list_busy", "变更正在执行或结果待宿主核实", 409)

    async def get(self, topic_id: str) -> ChangeListView:
        async with self.database.write_lock, self.database.sessions() as session:
            await self._require_topic(session, topic_id)
            row = await session.get(ChangeListRow, topic_id)
            return self._view(row) if row else ChangeListView(topic_id=topic_id, revision=0)

    async def sync(self, topic_id: str, request: ChangeSync) -> ChangeListView:
        request = ChangeSync.model_validate(request.model_dump(mode="json"))
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self._require_topic(session, topic_id)
            row = await session.get(ChangeListRow, topic_id)
            if row is None:
                row = ChangeListRow(topic_id=topic_id, revision=0, items=[])
                session.add(row)
            self._editable(row, request.revision)
            # 完整同步是新列表版本；旧执行快照和回执仍留在账本。
            row.items = TypeAdapter(list[JsonValue]).validate_python(
                [ChangeItem(**item.model_dump()).model_dump(mode="json") for item in request.items]
            )
            self._bump(row)
            await session.flush()
            return self._view(row)

    async def operation(self, topic_id: str, operation_id: str) -> ChangeOperation:
        async with self.database.write_lock, self.database.sessions() as session:
            row = await session.get(ChangeOperationRow, operation_id)
            if row is None or row.topic_id != topic_id:
                raise ServiceError("change_operation_not_found", "变更操作不在该话题中", 404)
            return self._operation(row)

    async def operations(self, topic_id: str) -> list[ChangeOperation]:
        async with self.database.write_lock, self.database.sessions() as session:
            await self._require_topic(session, topic_id)
            rows = await session.scalars(
                select(ChangeOperationRow)
                .where(ChangeOperationRow.topic_id == topic_id)
                .order_by(ChangeOperationRow.created_at.desc(), ChangeOperationRow.id)
                .limit(100)
            )
            return [self._operation(row) for row in rows]

    async def history(self, topic_id: str, operation_id: str) -> list[ChangeReceipt]:
        async with self.database.write_lock, self.database.sessions() as session:
            row = await session.get(ChangeOperationRow, operation_id)
            if row is None or row.topic_id != topic_id:
                raise ServiceError("change_operation_not_found", "变更操作不在该话题中", 404)
            return TypeAdapter(list[ChangeReceipt]).validate_python(row.receipt_history)

    async def prepare(
        self, topic_id: str, request: ChangeInvoke, *, allow_new: bool = True
    ) -> tuple[ChangeOperation, bool]:
        request = ChangeInvoke.model_validate(request.model_dump(mode="json"))
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self._require_topic(session, topic_id)
            previous = await session.scalar(
                select(ChangeOperationRow).where(
                    ChangeOperationRow.topic_id == topic_id,
                    ChangeOperationRow.idempotency_key == request.idempotency_key,
                )
            )
            if previous is not None:
                if previous.request_hash != request.fingerprint():
                    raise ServiceError("idempotency_conflict", "幂等键已绑定其他变更请求", 409)
                return self._operation(previous), False
            if not allow_new:
                raise ServiceError("change_operation_limit", "同时执行的变更操作超过上限", 429)
            row = await session.get(ChangeListRow, topic_id)
            if row is None:
                raise ServiceError("change_list_empty", "话题没有变更清单", 409)
            self._editable(row, request.revision)
            if row.revision >= 9007199254740990:
                raise ServiceError("change_revision_limit", "变更清单版本无法容纳执行与回执", 409)
            view = self._view(row)
            targets = [item for item in view.items if item.status == "pending"]
            if request.item_id is not None:
                item = next((item for item in view.items if item.id == request.item_id), None)
                if item is None:
                    raise ServiceError("change_item_not_found", "变更条目不在该话题中", 404)
                if item.status != "pending":
                    raise ServiceError("change_item_resolved", "变更条目已经处理", 409)
                targets = [item]
            if not targets:
                raise ServiceError("change_list_empty", "没有待处理变更", 409)
            execution = ChangeExecution(
                operation_id=str(uuid4()),
                topic_id=topic_id,
                revision=request.revision,
                action=request.action,
                items=[
                    ChangeItemInput.model_validate(item.model_dump(exclude={"status"}))
                    for item in targets
                ],
            )
            operation = ChangeOperationRow(
                id=execution.operation_id,
                topic_id=topic_id,
                idempotency_key=request.idempotency_key,
                request_hash=request.fingerprint(),
                execution=TypeAdapter(dict[str, JsonValue]).validate_python(
                    execution.model_dump(mode="json")
                ),
                status="executing",
                receipt_history=[],
            )
            selected = {item.id for item in targets}
            for item in view.items:
                if item.id in selected:
                    item.status = "executing"
            row.items = TypeAdapter(list[JsonValue]).validate_python(
                [item.model_dump(mode="json") for item in view.items]
            )
            row.active_operation_id = operation.id
            self._bump(row)
            session.add(operation)
            await session.flush()
            return self._operation(operation), True

    async def settle(self, receipt: ChangeReceipt, *, resolve: bool = False) -> ChangeOperation:
        receipt = ChangeReceipt.model_validate(receipt.model_dump(mode="json"))
        async with self.database.write_lock, self.database.sessions.begin() as session:
            operation = await session.get(ChangeOperationRow, receipt.operation_id)
            if operation is None or operation.topic_id != receipt.topic_id:
                raise ServiceError("change_operation_not_found", "变更操作不在该话题中", 404)
            execution = ChangeExecution.model_validate(operation.execution)
            if not receipt.matches(execution):
                raise ServiceError("change_receipt_mismatch", "变更回执关联或目标不匹配", 409)
            if operation.status in {"done", "error"}:
                if operation.receipt == receipt.model_dump(mode="json"):
                    return self._operation(operation)
                raise ServiceError("change_operation_resolved", "操作已收到终态回执", 409)
            if (operation.status == "uncertain") != resolve:
                raise ServiceError("change_resolution_invalid", "回执不符合当前执行/核实阶段", 409)
            if resolve and operation.receipt:
                previous = ChangeReceipt.model_validate(operation.receipt)
                outcomes = {result.item_id: result for result in receipt.results}
                if any(
                    result.status != "uncertain" and result != outcomes[result.item_id]
                    for result in previous.results
                ):
                    raise ServiceError(
                        "change_resolution_invalid", "已确认的条目结果不能被核实覆盖", 409
                    )
            if len(operation.receipt_history) >= 16:
                raise ServiceError("change_receipt_limit", "操作回执历史已达上限", 409)
            row = await session.get(ChangeListRow, receipt.topic_id)
            if row is None or row.active_operation_id != operation.id:
                raise ServiceError("change_operation_conflict", "清单与执行账本关联无效", 409)
            self._apply(row, operation, receipt)
            await session.flush()
            return self._operation(operation)

    def _apply(
        self, row: ChangeListRow, operation: ChangeOperationRow, receipt: ChangeReceipt
    ) -> None:
        view = self._view(row)
        results = {result.item_id: result for result in receipt.results}
        for item in view.items:
            result = results.get(item.id)
            if result is None:
                continue
            if result.status == "done":
                item.status = "undone" if receipt.action.startswith("undo") else "kept"
            elif result.status == "error":
                item.status = "pending"
            else:
                item.status = "uncertain"
        operation.status = (
            "uncertain"
            if any(result.status == "uncertain" for result in receipt.results)
            else "error"
            if any(result.status == "error" for result in receipt.results)
            else "done"
        )
        operation.receipt = TypeAdapter(dict[str, JsonValue]).validate_python(
            receipt.model_dump(mode="json")
        )
        operation.receipt_history = [*operation.receipt_history, operation.receipt]
        operation.updated_at = utc_now()
        row.items = TypeAdapter(list[JsonValue]).validate_python(
            [item.model_dump(mode="json") for item in view.items]
        )
        row.active_operation_id = operation.id if operation.status == "uncertain" else None
        self._bump(row)

    async def recover_interrupted(self) -> int:
        """仅在没有活跃生产者的启动阶段调用；永不自动回放外部动作。"""
        async with self.database.write_lock, self.database.sessions.begin() as session:
            operations = list(
                await session.scalars(
                    select(ChangeOperationRow).where(ChangeOperationRow.status == "executing")
                )
            )
            for operation in operations:
                row = await session.get(ChangeListRow, operation.topic_id)
                if row is None or row.active_operation_id != operation.id:
                    raise ServiceError("change_operation_conflict", "清单与执行账本关联无效", 409)
                execution = ChangeExecution.model_validate(operation.execution)
                self._apply(
                    row, operation, ChangeReceipt.uncertain(execution, "process_interrupted")
                )
            return len(operations)
