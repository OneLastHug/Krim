"""宿主 Checkpoint 编排；业务状态只由绑定 Python callback 的回执推进。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from krim_agent.checkpoint import (
    CheckpointCanRollback,
    CheckpointCheck,
    CheckpointCheckResult,
    CheckpointCreate,
    CheckpointExecution,
    CheckpointOperation,
    CheckpointReceipt,
    CheckpointRollback,
)
from krim_agent.persistence.checkpoints import CheckpointRepository
from krim_agent.protocol.domain import ServiceError

CheckpointExecutor = Callable[[CheckpointExecution], Awaitable[CheckpointReceipt]]
CheckpointCanExecutor = Callable[[CheckpointCheck], Awaitable[CheckpointCheckResult]]


class CheckpointService:
    def __init__(self, repository: CheckpointRepository) -> None:
        self.repository = repository
        self._tasks: dict[str, asyncio.Task[CheckpointOperation]] = {}
        self._executions: dict[str, CheckpointExecution] = {}
        self._start_lock = asyncio.Lock()
        self._closed = False

    async def create(
        self, topic_id: str, request: CheckpointCreate, executor: CheckpointExecutor
    ) -> CheckpointOperation:
        return await self._invoke(topic_id, request, executor)

    async def rollback(
        self, topic_id: str, request: CheckpointRollback, executor: CheckpointExecutor
    ) -> CheckpointOperation:
        return await self._invoke(topic_id, request, executor)

    async def _invoke(
        self,
        topic_id: str,
        request: CheckpointCreate | CheckpointRollback,
        executor: CheckpointExecutor,
    ) -> CheckpointOperation:
        async with self._start_lock:
            if self._closed:
                raise ServiceError("checkpoint_service_closed", "Checkpoint 服务已关闭", 410)
            operation, created = (
                await self.repository.prepare_create(
                    topic_id, request, allow_new=len(self._tasks) < 32
                )
                if isinstance(request, CheckpointCreate)
                else await self.repository.prepare_rollback(
                    topic_id, request, allow_new=len(self._tasks) < 32
                )
            )
            identifier = operation.execution.operation_id
            if created:
                task = asyncio.create_task(
                    self._execute(operation.execution, request.timeout_seconds, executor)
                )
                self._tasks[identifier] = task
                self._executions[identifier] = operation.execution

                def finished(value: asyncio.Task[CheckpointOperation]) -> None:
                    self._tasks.pop(identifier, None)
                    self._executions.pop(identifier, None)
                    if not value.cancelled():
                        value.exception()

                task.add_done_callback(finished)
            running = self._tasks.get(identifier)
        return await asyncio.shield(running) if running is not None else operation

    async def _execute(
        self,
        execution: CheckpointExecution,
        timeout_seconds: float,
        executor: CheckpointExecutor,
    ) -> CheckpointOperation:
        try:
            async with asyncio.timeout(timeout_seconds):
                receipt = await executor(execution.model_copy(deep=True))
            receipt = CheckpointReceipt.model_validate(receipt.model_dump(mode="json"))
            if not receipt.matches(execution):
                receipt = CheckpointReceipt.uncertain(execution, "receipt_mismatch")
        except asyncio.CancelledError:
            receipt = CheckpointReceipt.uncertain(execution, "cancelled")
        except TimeoutError:
            receipt = CheckpointReceipt.uncertain(execution, "executor_timeout")
        except ServiceError as error:
            code = (
                error.code
                if error.code
                in {
                    "host_timeout",
                    "host_cancelled",
                    "host_session_closed",
                    "host_not_ready",
                    "host_session_not_found",
                }
                else "executor_failed"
            )
            receipt = CheckpointReceipt.uncertain(execution, code)
        except Exception:
            receipt = CheckpointReceipt.uncertain(execution, "executor_failed")
        task = asyncio.create_task(self.repository.settle(receipt))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            return await task
        except Exception:
            current = await self.repository.operation(execution.topic_id, execution.operation_id)
            if current.status != "executing":
                return current
            return await self.repository.settle(
                CheckpointReceipt.uncertain(execution, "receipt_store_failed")
            )

    async def can_rollback(
        self, topic_id: str, request: CheckpointCanRollback, executor: CheckpointCanExecutor
    ) -> CheckpointCheckResult:
        local = await self.repository.can_rollback_local(topic_id, request)
        if not local.can_rollback:
            return local
        checkpoint = await self.repository.checkpoint(topic_id, request.checkpoint_id)
        check = CheckpointCheck(
            topic_id=topic_id,
            checkpoint_id=request.checkpoint_id,
            revision=local.revision,
            metadata=request.metadata,
            snapshot_metadata=checkpoint.metadata,
            snapshot_ref=checkpoint.snapshot_ref,
        )
        try:
            async with asyncio.timeout(request.timeout_seconds):
                answer = await executor(check.model_copy(deep=True))
            answer = CheckpointCheckResult.model_validate(answer.model_dump(mode="json"))
            if (
                answer.topic_id != topic_id
                or answer.checkpoint_id != request.checkpoint_id
                or answer.revision != local.revision
            ):
                return local.model_copy(
                    update={"can_rollback": False, "reason": "receipt_mismatch"}
                )
        except TimeoutError:
            return local.model_copy(update={"can_rollback": False, "reason": "host_timeout"})
        except Exception:
            return local.model_copy(update={"can_rollback": False, "reason": "host_check_failed"})
        current = await self.repository.catalog(topic_id)
        if current.revision != local.revision or current.active_operation_id is not None:
            return local.model_copy(update={"can_rollback": False, "reason": "checkpoint_changed"})
        return answer

    async def resolve(self, receipt: CheckpointReceipt) -> CheckpointOperation:
        return await self.repository.settle(receipt, resolve=True)

    async def cancel(self, topic_id: str, operation_id: str) -> CheckpointOperation:
        operation = await self.repository.operation(topic_id, operation_id)
        task = self._tasks.get(operation_id)
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            operation = await self.repository.operation(topic_id, operation_id)
        if operation.status == "executing":
            return await self.repository.settle(
                CheckpointReceipt.uncertain(operation.execution, "cancelled")
            )
        return operation

    async def close(self) -> None:
        async with self._start_lock:
            self._closed = True
            tasks = [
                (task, self._executions[identifier]) for identifier, task in self._tasks.items()
            ]
        for task, _execution in tasks:
            task.cancel()
        await asyncio.gather(*(task for task, _execution in tasks), return_exceptions=True)
        for _task, execution in tasks:
            current = await self.repository.operation(execution.topic_id, execution.operation_id)
            if current.status == "executing":
                await self.repository.settle(CheckpointReceipt.uncertain(execution, "cancelled"))
