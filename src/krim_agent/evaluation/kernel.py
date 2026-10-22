"""唯一Eval任务所有者；由原EvaluationService抽取，后端只转换契约。"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from krim_agent.contracts import ServiceError, fingerprint

from .domain import clone
from .lifecycle import settled, task_done
from .ports import AgentAdapter, EvalRepository, method_identities
from .runner import ItemWork, execute_item


class EvalKernel:
    def __init__(self, repository: EvalRepository) -> None:
        self.repository = repository
        self.tasks: dict[str, asyncio.Task[None]] = {}
        self.current: dict[str, ItemWork] = {}
        self._admission = asyncio.Lock()
        self._close_task: asyncio.Task[None] | None = None
        self._finalizing: set[str] = set()
        self._cancelled: set[str] = set()
        self.closed = False
        self.cleanup_unknown = False

    def _external(self) -> None:
        task = asyncio.current_task()
        if task in self.tasks.values() or any(
            task in (w.admission, w.scorer) for w in self.current.values()
        ):
            raise ServiceError("self_wait")

    async def submit(self, request: dict[str, Any]) -> dict[str, Any]:
        frozen = clone(request)
        return await settled(self._admit(frozen))

    async def _admit(self, request: dict[str, Any]) -> dict[str, Any]:
        async with self._admission:
            if self.closed:
                raise ServiceError("closed", 503)
            if not self.repository.healthy or self.cleanup_unknown:
                raise ServiceError("storage_unavailable", 503)
            old = await self.repository.by_key(request)
            if old is not None:
                if old["request_hash"] != fingerprint(request):
                    raise ServiceError("key_conflict")
                return old
            if any(not t.done() for t in self.tasks.values()):
                raise ServiceError("capacity", 429)
            adapter = self.repository.adapter(request)
            identities = method_identities(adapter)

            def guard() -> None:
                if (
                    self.repository.adapter(request) is not adapter
                    or method_identities(adapter) != identities
                ):
                    raise ServiceError("target_binding_changed")

            snapshot, created = await self.repository.begin(request)
            if created:
                try:
                    task = asyncio.create_task(
                        self._drive(
                            snapshot["id"],
                            adapter,
                            guard,
                            float(snapshot["execution"]["batch_timeout_seconds"]),
                        )
                    )
                    self.tasks[snapshot["id"]] = task
                    task.add_done_callback(task_done)
                except Exception:
                    await self.repository.finish(snapshot["id"], "failed", "execution_error")
                    raise ServiceError("execution_error") from None
            return snapshot

    async def _drive(
        self,
        task_id: str,
        adapter: AgentAdapter,
        guard: Callable[[], None],
        batch_seconds: float,
    ) -> None:
        failure: tuple[str, str] | None = None
        try:
            async with asyncio.timeout(batch_seconds):
                snapshot = await self.repository.snapshot(task_id)
                plan = self.repository.plan(snapshot)
                if plan.batch_timeout != batch_seconds:
                    raise ServiceError("facts_corrupt")
                await self.repository.set_running(task_id)
                guard()
                for trial in plan.trials:
                    work = ItemWork(adapter)
                    self.current[task_id] = work
                    try:
                        await execute_item(
                            trial,
                            plan.rules,
                            self.repository,
                            work,
                            self.repository.metrics(),
                            item_timeout_seconds=plan.item_timeout,
                            scorer_timeout_seconds=plan.scorer_timeout,
                            guard=guard,
                        )
                    finally:
                        if work.cleanup_unknown:
                            self.cleanup_unknown = True
                        self.current.pop(task_id, None)
        except asyncio.CancelledError:
            failure = ("aborted", "cancelled")
        except TimeoutError:
            failure = ("failed", "run_timeout")
        except ServiceError as error:
            failure = ("failed", error.code)
        except Exception:
            failure = ("failed", "execution_error")
        finally:
            self._finalizing.add(task_id)
            try:
                if failure is not None:
                    await self.repository.finish(task_id, *failure)
            finally:
                self._finalizing.discard(task_id)
                self._cancelled.discard(task_id)

    async def wait(self, task_id: str) -> dict[str, Any]:
        self._external()
        task = self.tasks.get(task_id)
        if task is not None:
            await asyncio.shield(task)
        return await self.repository.snapshot(task_id)

    async def cancel(self, task_id: str) -> dict[str, Any]:
        self._external()
        return await settled(self._cancel(task_id))

    async def _cancel(self, task_id: str) -> dict[str, Any]:
        task = self.tasks.get(task_id)
        if task is None or task.done():
            return await self.repository.snapshot(task_id)
        error: Exception | None = None
        try:
            await self.repository.stop(task_id)
        except Exception as failure:
            error = failure
        finally:
            if (
                not task.done()
                and task_id not in self._finalizing
                and task_id not in self._cancelled
            ):
                self._cancelled.add(task_id)
                task.cancel()
            await settled(task)
        if error is not None:
            raise error
        return await self.repository.snapshot(task_id)

    async def close(self) -> None:
        self._external()
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await settled(self._close_task)

    async def _close(self) -> None:
        async with self._admission:
            self.closed = True
        outcomes = await asyncio.gather(
            *(self.cancel(key) for key, task in list(self.tasks.items()) if not task.done()),
            return_exceptions=True,
        )
        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                raise outcome
