"""宿主快照目录和执行账本；等待 Python callback 时不持有数据库事务。"""

from __future__ import annotations

from datetime import UTC
from uuid import uuid4

from pydantic import JsonValue, TypeAdapter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.checkpoint import (
    CheckpointCanRollback,
    CheckpointCatalog,
    CheckpointCheckResult,
    CheckpointCreate,
    CheckpointExecution,
    CheckpointInvoke,
    CheckpointOperation,
    CheckpointReceipt,
    CheckpointRollback,
    HostCheckpoint,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import utc_now

from .database import (
    Database,
    HostCheckpointOperationRow,
    HostCheckpointRow,
    HostCheckpointTopicRow,
    TopicRow,
)


class CheckpointRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    @staticmethod
    async def _require_topic(session: AsyncSession, topic_id: str) -> None:
        if await session.get(TopicRow, topic_id) is None:
            raise ServiceError("topic_not_found", "话题不存在", 404)

    @staticmethod
    def _operation(row: HostCheckpointOperationRow) -> CheckpointOperation:
        return CheckpointOperation(
            idempotency_key=row.idempotency_key,
            execution=CheckpointExecution.model_validate(row.execution),
            status=row.status,  # type: ignore[arg-type]
            receipt=CheckpointReceipt.model_validate(row.receipt) if row.receipt else None,
            created_at=row.created_at.replace(tzinfo=UTC),
            updated_at=row.updated_at.replace(tzinfo=UTC),
        )

    @staticmethod
    def _checkpoint(row: HostCheckpointRow) -> HostCheckpoint:
        return HostCheckpoint(
            id=row.id,
            topic_id=row.topic_id,
            title=row.title,
            metadata=TypeAdapter(dict[str, JsonValue]).validate_python(row.snapshot_metadata),
            snapshot_ref=row.snapshot_ref,
            status=row.status,  # type: ignore[arg-type]
            created_at=row.created_at.replace(tzinfo=UTC),
            updated_at=row.updated_at.replace(tzinfo=UTC),
        )

    @staticmethod
    def _bump(topic: HostCheckpointTopicRow) -> None:
        if topic.revision >= 9007199254740991:
            raise ServiceError("checkpoint_revision_limit", "Checkpoint 版本已达上限", 409)
        topic.revision += 1

    @staticmethod
    def _idle(topic: HostCheckpointTopicRow, revision: int) -> None:
        if topic.revision != revision:
            raise ServiceError(
                "checkpoint_revision_conflict", "Checkpoint 版本已更新，请重新读取", 409
            )
        if topic.active_operation_id is not None:
            raise ServiceError("checkpoint_busy", "已有 Checkpoint 操作执行中或待核实", 409)

    async def catalog(self, topic_id: str) -> CheckpointCatalog:
        async with self.database.write_lock, self.database.sessions() as session:
            await self._require_topic(session, topic_id)
            topic = await session.get(HostCheckpointTopicRow, topic_id)
            rows = await session.scalars(
                select(HostCheckpointRow)
                .where(HostCheckpointRow.topic_id == topic_id)
                .order_by(HostCheckpointRow.created_at, HostCheckpointRow.id)
            )
            return CheckpointCatalog(
                topic_id=topic_id,
                revision=topic.revision if topic else 0,
                active_operation_id=topic.active_operation_id if topic else None,
                checkpoints=[self._checkpoint(row) for row in rows],
            )

    async def checkpoint(self, topic_id: str, checkpoint_id: str) -> HostCheckpoint:
        async with self.database.write_lock, self.database.sessions() as session:
            row = await session.get(HostCheckpointRow, checkpoint_id)
            if row is None or row.topic_id != topic_id:
                raise ServiceError("checkpoint_not_found", "Checkpoint 不在该话题中", 404)
            return self._checkpoint(row)

    async def operation(self, topic_id: str, operation_id: str) -> CheckpointOperation:
        async with self.database.write_lock, self.database.sessions() as session:
            row = await session.get(HostCheckpointOperationRow, operation_id)
            if row is None or row.topic_id != topic_id:
                raise ServiceError("checkpoint_operation_not_found", "Checkpoint 操作不存在", 404)
            return self._operation(row)

    async def operations(self, topic_id: str) -> list[CheckpointOperation]:
        async with self.database.write_lock, self.database.sessions() as session:
            await self._require_topic(session, topic_id)
            rows = await session.scalars(
                select(HostCheckpointOperationRow)
                .where(HostCheckpointOperationRow.topic_id == topic_id)
                .order_by(
                    HostCheckpointOperationRow.created_at.desc(), HostCheckpointOperationRow.id
                )
                .limit(100)
            )
            return [self._operation(row) for row in rows]

    async def history(self, topic_id: str, operation_id: str) -> list[CheckpointReceipt]:
        async with self.database.write_lock, self.database.sessions() as session:
            row = await session.get(HostCheckpointOperationRow, operation_id)
            if row is None or row.topic_id != topic_id:
                raise ServiceError("checkpoint_operation_not_found", "Checkpoint 操作不存在", 404)
            return TypeAdapter(list[CheckpointReceipt]).validate_python(row.receipt_history)

    async def prepare_create(
        self, topic_id: str, request: CheckpointCreate, *, allow_new: bool = True
    ) -> tuple[CheckpointOperation, bool]:
        return await self._prepare(
            topic_id, CheckpointInvoke(action="create", **request.model_dump()), allow_new=allow_new
        )

    async def prepare_rollback(
        self, topic_id: str, request: CheckpointRollback, *, allow_new: bool = True
    ) -> tuple[CheckpointOperation, bool]:
        return await self._prepare(
            topic_id,
            CheckpointInvoke(action="rollback", title="资源回滚", **request.model_dump()),
            allow_new=allow_new,
        )

    async def _prepare(
        self, topic_id: str, request: CheckpointInvoke, *, allow_new: bool = True
    ) -> tuple[CheckpointOperation, bool]:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self._require_topic(session, topic_id)
            previous = await session.scalar(
                select(HostCheckpointOperationRow).where(
                    HostCheckpointOperationRow.topic_id == topic_id,
                    HostCheckpointOperationRow.idempotency_key == request.idempotency_key,
                )
            )
            if previous is not None:
                if previous.request_hash != request.fingerprint():
                    raise ServiceError(
                        "idempotency_conflict", "Checkpoint 幂等键已绑定其他请求", 409
                    )
                return self._operation(previous), False
            if not allow_new:
                raise ServiceError(
                    "checkpoint_operation_limit", "活跃 Checkpoint 操作超过上限", 429
                )
            topic = await session.get(HostCheckpointTopicRow, topic_id)
            if topic is None:
                topic = HostCheckpointTopicRow(topic_id=topic_id, revision=0)
                session.add(topic)
                await session.flush()
            self._idle(topic, request.revision)
            if topic.revision >= 9007199254740990:
                raise ServiceError(
                    "checkpoint_revision_limit", "Checkpoint 版本无法容纳执行与回执", 409
                )
            checkpoint: HostCheckpointRow | None = None
            if request.action == "rollback":
                checkpoint = await session.get(HostCheckpointRow, request.checkpoint_id)
                if checkpoint is None or checkpoint.topic_id != topic_id:
                    raise ServiceError("checkpoint_not_found", "Checkpoint 不在该话题中", 404)
                if checkpoint.status not in {"available", "rolled_back"}:
                    raise ServiceError(
                        "checkpoint_not_rollbackable", "Checkpoint 当前不可回滚", 409
                    )
            checkpoint_id = request.checkpoint_id or str(uuid4())
            operation_id = str(uuid4())
            execution = CheckpointExecution(
                operation_id=operation_id,
                topic_id=topic_id,
                checkpoint_id=checkpoint_id,
                revision=request.revision,
                action=request.action,
                title=request.title
                if request.action == "create"
                else (checkpoint.title if checkpoint else "资源回滚"),
                metadata=request.metadata,
                snapshot_metadata=checkpoint.snapshot_metadata if checkpoint else request.metadata,
                snapshot_ref=checkpoint.snapshot_ref if checkpoint else None,
            )
            if checkpoint is None:
                checkpoint = HostCheckpointRow(
                    id=checkpoint_id,
                    topic_id=topic_id,
                    title=execution.title,
                    snapshot_metadata=dict(execution.metadata),
                    snapshot_ref=None,
                    status="creating",
                )
                session.add(checkpoint)
            else:
                checkpoint.status = "rolling_back"
                checkpoint.updated_at = utc_now()
            await session.flush()
            operation = HostCheckpointOperationRow(
                id=operation_id,
                topic_id=topic_id,
                checkpoint_id=checkpoint_id,
                idempotency_key=request.idempotency_key,
                request_hash=request.fingerprint(),
                execution=TypeAdapter(dict[str, JsonValue]).validate_python(
                    execution.model_dump(mode="json")
                ),
                status="executing",
                receipt_history=[],
            )
            topic.active_operation_id = operation_id
            self._bump(topic)
            session.add(operation)
            await session.flush()
            return self._operation(operation), True

    async def settle(
        self, receipt: CheckpointReceipt, *, resolve: bool = False
    ) -> CheckpointOperation:
        receipt = CheckpointReceipt.model_validate(receipt.model_dump(mode="json"))
        async with self.database.write_lock, self.database.sessions.begin() as session:
            operation = await session.get(HostCheckpointOperationRow, receipt.operation_id)
            if operation is None or operation.topic_id != receipt.topic_id:
                raise ServiceError("checkpoint_operation_not_found", "Checkpoint 操作不存在", 404)
            execution = CheckpointExecution.model_validate(operation.execution)
            if not receipt.matches(execution):
                raise ServiceError(
                    "checkpoint_receipt_mismatch", "Checkpoint 回执与执行快照不匹配", 409
                )
            if operation.status in {"done", "error"}:
                if operation.receipt == receipt.model_dump(mode="json"):
                    return self._operation(operation)
                raise ServiceError(
                    "checkpoint_operation_resolved", "Checkpoint 操作已经有终态", 409
                )
            if (operation.status == "uncertain") != resolve:
                raise ServiceError(
                    "checkpoint_resolution_invalid", "Checkpoint 回执不符合当前状态", 409
                )
            checkpoint = await session.get(HostCheckpointRow, execution.checkpoint_id)
            topic = await session.get(HostCheckpointTopicRow, execution.topic_id)
            if checkpoint is None or topic is None or topic.active_operation_id != operation.id:
                raise ServiceError("checkpoint_operation_conflict", "Checkpoint 状态关联无效", 409)
            if len(operation.receipt_history) >= 16:
                raise ServiceError("checkpoint_receipt_limit", "Checkpoint 回执历史已达上限", 409)
            if receipt.status == "done":
                checkpoint.snapshot_ref = receipt.snapshot_ref
                checkpoint.status = "available" if receipt.action == "create" else "rolled_back"
            elif receipt.status == "error":
                checkpoint.status = "failed" if receipt.action == "create" else "available"
            else:
                checkpoint.status = "uncertain"
            checkpoint.updated_at = utc_now()
            operation.status = (
                "uncertain"
                if receipt.status == "uncertain"
                else "error"
                if receipt.status == "error"
                else "done"
            )
            operation.receipt = TypeAdapter(dict[str, JsonValue]).validate_python(
                receipt.model_dump(mode="json")
            )
            operation.receipt_history = [*operation.receipt_history, operation.receipt]
            operation.updated_at = utc_now()
            topic.active_operation_id = operation.id if operation.status == "uncertain" else None
            self._bump(topic)
            await session.flush()
            return self._operation(operation)

    async def can_rollback_local(
        self, topic_id: str, request: CheckpointCanRollback
    ) -> CheckpointCheckResult:
        checkpoint = await self.checkpoint(topic_id, request.checkpoint_id)
        catalog = await self.catalog(topic_id)
        if catalog.revision != request.revision:
            raise ServiceError("checkpoint_revision_conflict", "Checkpoint 版本已经更新", 409)
        return CheckpointCheckResult(
            topic_id=topic_id,
            checkpoint_id=checkpoint.id,
            revision=catalog.revision,
            can_rollback=checkpoint.status in {"available", "rolled_back"}
            and catalog.active_operation_id is None,
            reason=""
            if checkpoint.status in {"available", "rolled_back"}
            else "checkpoint_not_available",
        )

    async def recover_interrupted(self) -> int:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            rows = list(
                await session.scalars(
                    select(HostCheckpointOperationRow).where(
                        HostCheckpointOperationRow.status == "executing"
                    )
                )
            )
            for operation in rows:
                execution = CheckpointExecution.model_validate(operation.execution)
                receipt = CheckpointReceipt.uncertain(execution, "process_interrupted")
                checkpoint = await session.get(HostCheckpointRow, execution.checkpoint_id)
                topic = await session.get(HostCheckpointTopicRow, execution.topic_id)
                if checkpoint is None or topic is None:
                    raise ServiceError(
                        "checkpoint_operation_conflict", "Checkpoint 状态关联无效", 409
                    )
                checkpoint.status = "uncertain"
                operation.status = "uncertain"
                operation.receipt = TypeAdapter(dict[str, JsonValue]).validate_python(
                    receipt.model_dump(mode="json")
                )
                operation.receipt_history = [*operation.receipt_history, operation.receipt]
                topic.active_operation_id = operation.id
                self._bump(topic)
                checkpoint.updated_at = utc_now()
                operation.updated_at = utc_now()
            return len(rows)
