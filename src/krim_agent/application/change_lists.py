"""持久动作编排；可信 Python callback 完成后才消费业务回执。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from krim_agent.change_list import ChangeExecution, ChangeInvoke, ChangeOperation, ChangeReceipt
from krim_agent.persistence.change_lists import ChangeListRepository
from krim_agent.protocol.domain import ServiceError

ChangeExecutor = Callable[[ChangeExecution], Awaitable[ChangeReceipt]]


class ChangeListService:
    def __init__(self, repository: ChangeListRepository) -> None:
        self.repository = repository
        self._tasks: dict[str, asyncio.Task[ChangeOperation]] = {}
        self._executions: dict[str, ChangeExecution] = {}
        self._closed = False
        self._start_lock = asyncio.Lock()

    async def invoke(
        self, topic_id: str, request: ChangeInvoke, executor: ChangeExecutor
    ) -> ChangeOperation:
        async with self._start_lock:
            if self._closed:
                raise ServiceError("change_service_closed", "变更执行服务已关闭", 410)
            operation, created = await self.repository.prepare(
                topic_id, request, allow_new=len(self._tasks) < 32
            )
            identifier = operation.execution.operation_id
            if created:
                task = asyncio.create_task(self._execute(operation.execution, request, executor))
                self._tasks[identifier] = task
                self._executions[identifier] = operation.execution

                def finished(value: asyncio.Task[ChangeOperation]) -> None:
                    self._tasks.pop(identifier, None)
                    self._executions.pop(identifier, None)
                    if not value.cancelled():
                        value.exception()

                task.add_done_callback(finished)
            running = self._tasks.get(identifier)
        # 相同请求可以等待现有执行；恢复/失败/不确定状态永不隐式重新执行。
        return await asyncio.shield(running) if running is not None else operation

    async def _save(self, receipt: ChangeReceipt) -> ChangeOperation:
        task = asyncio.create_task(self.repository.settle(receipt))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def _execute(
        self, execution: ChangeExecution, request: ChangeInvoke, executor: ChangeExecutor
    ) -> ChangeOperation:
        try:
            async with asyncio.timeout(request.timeout_seconds):
                receipt = await executor(execution.model_copy(deep=True))
            receipt = ChangeReceipt.model_validate(receipt.model_dump(mode="json"))
            if not receipt.matches(execution):
                receipt = ChangeReceipt.uncertain(execution, "receipt_mismatch")
        except asyncio.CancelledError:
            await self._save(ChangeReceipt.uncertain(execution, "cancelled"))
            raise
        except TimeoutError:
            receipt = ChangeReceipt.uncertain(execution, "executor_timeout")
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
            receipt = ChangeReceipt.uncertain(execution, code)
        except Exception:
            receipt = ChangeReceipt.uncertain(execution, "executor_failed")
        try:
            return await self._save(receipt)
        except asyncio.CancelledError:
            raise
        except Exception:
            current = await self.repository.operation(execution.topic_id, execution.operation_id)
            if current.status != "executing":
                return current
            return await self._save(ChangeReceipt.uncertain(execution, "receipt_store_failed"))

    async def cancel(self, topic_id: str, operation_id: str) -> ChangeOperation:
        operation = await self.repository.operation(topic_id, operation_id)
        task = self._tasks.get(operation_id)
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            current = await self.repository.operation(topic_id, operation_id)
            if current.status == "executing":
                return await self.repository.settle(
                    ChangeReceipt.uncertain(current.execution, "cancelled")
                )
            return current
        if operation.status == "executing":
            return await self.repository.settle(
                ChangeReceipt.uncertain(operation.execution, "cancelled")
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
                await self.repository.settle(ChangeReceipt.uncertain(execution, "cancelled"))
