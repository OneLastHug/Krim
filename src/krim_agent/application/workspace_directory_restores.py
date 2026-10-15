"""目录恢复受控Host编排；准备、真实发布和只读核实沿原Registry与预算。"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Coroutine
from concurrent.futures import TimeoutError as FutureTimeout
from dataclasses import dataclass, field
from threading import Event
from typing import Any

from krim_agent.persistence.workspace_directory_restores import WorkspaceDirectoryRestoreStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.tools.core import (
    ExecutionBudgetBase,
    ToolContext,
    ToolError,
    ToolExecutionBudget,
    execution_budget_base,
    same_execution_budget,
)
from krim_agent.tools.directory_restore_guard import validate_directory_restore_parent
from krim_agent.tools.workspace import workspace_io
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_directory_restore import (
    DirectoryRestoreExecute,
    DirectoryRestoreFence,
    DirectoryRestorePreview,
    DirectoryRestoreReceiptView,
    DirectoryRestoreView,
)
from krim_agent.workspace_directory_restore_control import (
    DIRECTORY_RESTORE_STEP_TOOLS,
    DirectoryRestoreStepInput,
    DirectoryRestoreStepKind,
    DirectoryRestoreStepView,
)
from krim_agent.workspace_directory_restore_writer import (
    DirectoryRestoreEffect,
    WorkspaceDirectoryRestoreWriter,
)
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

from .workspace_changes import WorkspaceChangeService, _wait
from .workspace_execution import (
    enter_workspace_execution,
    inside_workspace_execution,
    leave_workspace_execution,
)

RestoreAuthorize = Callable[
    [str, DirectoryRestoreStepKind], Coroutine[Any, Any, tuple[str, WorkspaceBinding]]
]
RestoreCall = Callable[[str, DirectoryRestoreFence, bool], Coroutine[Any, Any, object]]


def _error(kind: str, text: str, status: int = 409) -> ServiceError:
    return ServiceError("workspace_directory_restore_" + kind, text, status)


@dataclass(frozen=True)
class _Pending:
    fence: DirectoryRestoreFence
    result: DirectoryRestoreStepView
    budget: ExecutionBudgetBase = field(repr=False)
    effect: DirectoryRestoreEffect | None = field(default=None, repr=False)


class WorkspaceDirectoryRestoreService:
    def __init__(
        self,
        store: WorkspaceDirectoryRestoreStore,
        changes: WorkspaceChangeService,
        authorize: RestoreAuthorize,
        call: RestoreCall,
        *,
        max_active: int = 16,
    ) -> None:
        if type(max_active) is not int or not 1 <= max_active <= 64:
            raise ValueError("目录恢复并发上限无效")
        self.store, self.changes, self.authorize, self.call = store, changes, authorize, call
        self.max_active = max_active
        self._tasks: dict[str, tuple[str, asyncio.Task[None]]] = {}
        self._aborts: dict[str, Event] = {}
        self._callbacks: dict[str, set[asyncio.Task[Any]]] = {}
        self._pending: dict[str, _Pending] = {}
        self._start_lock = asyncio.Lock()
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None
        self.model_close_guard: Callable[[], None] | None = None

    def _admission(self) -> None:
        if self._closed:
            raise _error("closed", "目录恢复服务已关闭", 503)

    def _capacity(self) -> None:
        if sum(not task.done() for _, task in self._tasks.values()) >= self.max_active:
            raise _error("busy", "目录恢复并发已满", 429)

    @staticmethod
    def _confirmation(confirmed: bool) -> None:
        if type(confirmed) is not bool:
            raise _error("input", "目录恢复确认必须为布尔值", 422)

    def _spawn(
        self,
        agent_id: str,
        scope: str,
        binding: WorkspaceBinding,
        fence: DirectoryRestoreFence,
        confirmed: bool,
    ) -> None:
        identifier = fence.operation_id
        abort = Event()
        task = asyncio.create_task(self._run(agent_id, scope, binding, fence, abort, confirmed))
        self._tasks[identifier] = (scope, task)
        self._aborts[identifier] = abort

        def done(result: asyncio.Task[None]) -> None:
            current = self._tasks.get(identifier)
            if current is not None and current[1] is result:
                self._tasks.pop(identifier, None)
            if self._aborts.get(identifier) is abort:
                self._aborts.pop(identifier, None)
            if not result.cancelled():
                result.exception()

        task.add_done_callback(done)

    async def preview(
        self, agent_id: str, request: DirectoryRestorePreview, *, confirmed: bool = False
    ) -> DirectoryRestoreView:
        self._confirmation(confirmed)
        request = DirectoryRestorePreview.model_validate(request.model_dump(mode="json"))

        async def admit() -> DirectoryRestoreView:
            scope, binding = await self.authorize(agent_id, "prepare")
            async with self._start_lock:
                self._admission()
                if await self.store.by_key(scope, request.request_key) is None:
                    self._capacity()
                view, created = await self.store.reserve(
                    scope, binding, request, qualify=self._admission
                )
                if created:
                    self._spawn(
                        agent_id,
                        scope,
                        binding,
                        await self.store.preparation(scope, view.plan_id),
                        confirmed,
                    )
                return view

        return await _wait(asyncio.create_task(admit()), lambda: None)

    async def start(
        self, agent_id: str, request: DirectoryRestoreExecute, *, confirmed: bool = False
    ) -> DirectoryRestoreView:
        self._confirmation(confirmed)
        request = DirectoryRestoreExecute.model_validate(request.model_dump(mode="json"))

        async def admit() -> DirectoryRestoreView:
            scope, binding = await self.authorize(agent_id, "execute")
            async with self._start_lock:
                self._admission()
                if (await self.store.get(scope, request.plan_id)).execute_key is None:
                    self._capacity()
                operation, created = await self.store.start(
                    scope, binding, request, qualify=self._admission
                )
                if created:
                    self._spawn(
                        agent_id,
                        scope,
                        binding,
                        await self.store.execution(scope, operation.operation_id),
                        confirmed,
                    )
                return await self.store.get(scope, request.plan_id)

        return await _wait(asyncio.create_task(admit()), lambda: None)

    async def verify(
        self, agent_id: str, operation_id: str, *, confirmed: bool = False
    ) -> DirectoryRestoreView:
        self._confirmation(confirmed)

        async def admit() -> DirectoryRestoreView:
            scope, binding = await self.authorize(agent_id, "verify")
            async with self._start_lock:
                self._admission()
                current = self._tasks.get(operation_id)
                if current is not None and not current[1].done():
                    raise _error("busy", "原目录操作仍在执行，不能并行核实")
                previous = await self.store.operation(scope, operation_id)
                if previous.status == "uncertain":
                    self._capacity()
                operation, fence = await self.store.claim_verify(
                    scope, operation_id, binding, qualify=self._admission
                )
                if fence is not None:
                    self._spawn(agent_id, scope, binding, fence, confirmed)
                return await self.store.get(scope, operation.plan_id)

        return await _wait(asyncio.create_task(admit()), lambda: None)

    def _qualify(self, context: ToolContext) -> None:
        self._admission()
        fence = context.directory_restore
        if type(fence) is not DirectoryRestoreFence:
            raise _error("context", "缺少原私有目录恢复围栏", 403)
        try:
            validate_directory_restore_parent(
                DIRECTORY_RESTORE_STEP_TOOLS.get(fence.kind, ""), context
            )
        except ToolError as error:
            raise ServiceError(error.code, str(error), 403) from error
        entry = self._tasks.get(fence.operation_id)
        abort = self._aborts.get(fence.operation_id)
        if (
            context.workspace_directory_restores is not self
            or entry is None
            or entry[0] != "session:" + str(context.session_id)
            or not inside_workspace_execution(self._tasks)
            or abort is None
            or abort.is_set()
            or type(context.execution_timeouts) is not tuple
            or not context.execution_timeouts
            or any(type(item) is not ToolExecutionBudget for item in context.execution_timeouts)
        ):
            raise _error("context", "目录恢复必须沿原受管工具调用与预算", 403)
        loop = asyncio.get_running_loop()
        for budget in context.execution_timeouts:
            deadline = budget.deadline.when()
            if (
                not budget.active
                or budget.deadline.expired()
                or deadline is not None
                and loop.time() >= deadline
            ):
                raise _error("timeout", "目录恢复原工具预算已结束", 408)

    async def _context(
        self, kind: str, payload: dict[str, Any], context: ToolContext
    ) -> tuple[str, WorkspaceBinding, DirectoryRestoreFence, DirectoryRestoreView]:
        self._qualify(context)
        wanted = DirectoryRestoreStepInput.model_validate(payload)
        fence = context.directory_restore
        assert fence is not None
        if kind != fence.kind or (wanted.plan_id, wanted.operation_id) != (
            fence.plan_id,
            fence.operation_id,
        ):
            raise _error("context", "目录叶必须引用原计划/操作与阶段", 403)
        scope, binding = await self.changes._guard(context)
        if kind == "prepare":
            await self.store.assert_preparation(
                scope, fence, binding, qualify=lambda: self._qualify(context)
            )
        else:
            await self.store.assert_execution(
                scope, fence, binding, qualify=lambda: self._qualify(context)
            )
        view = await self.store.get(scope, fence.plan_id)
        self._qualify(context)
        return scope, binding, fence, view

    def _bridge(
        self,
        identifier: str,
        operation: Callable[..., Coroutine[Any, Any, Any]],
        abort: Event,
        deadline: float,
    ) -> Callable[..., Any]:
        loop = asyncio.get_running_loop()

        async def invoke(*args: object) -> Any:
            task = asyncio.current_task()
            assert task is not None
            callbacks = self._callbacks.setdefault(identifier, set())
            callbacks.add(task)
            try:
                return await operation(*args)
            finally:
                callbacks.discard(task)

        def call(*args: object) -> Any:
            if abort.is_set() or time.monotonic() >= deadline:
                raise _error("stopped", "目录物理操作已取消或超过原5秒资格")
            coroutine = invoke(*args)
            try:
                future = asyncio.run_coroutine_threadsafe(coroutine, loop)
            except RuntimeError:
                coroutine.close()
                raise _error("unavailable", "目录原资格loop不可用", 503) from None
            try:
                while True:
                    remaining = deadline - time.monotonic()
                    if abort.is_set() or remaining <= 0:
                        raise _error("stopped", "目录物理操作已取消或超过原5秒资格")
                    try:
                        result = future.result(timeout=min(0.05, remaining))
                        if abort.is_set() or time.monotonic() >= deadline:
                            raise _error("stopped", "原目录资格等待已超出物理截止")
                        return result
                    except FutureTimeout:
                        continue
            finally:
                if not future.done():
                    future.cancel()

        return call

    async def tool_step(
        self, kind: str, payload: dict[str, Any], context: ToolContext
    ) -> dict[str, object]:
        try:
            scope, binding, fence, view = await self._context(kind, payload, context)
            base = execution_budget_base(context.execution_timeouts)
            abort = self._aborts[fence.operation_id]

            def qualify() -> None:
                self._qualify(context)
                if not same_execution_budget(
                    base, execution_budget_base(context.execution_timeouts)
                ):
                    raise _error("context", "原目录工具预算被替换或延长", 403)

            async def check() -> None:
                current, actual, same, _ = await self._context(kind, payload, context)
                if current != scope or actual is not binding or same != fence:
                    raise _error("context", "目录恢复原Scope/资源/围栏被替换", 403)
                qualify()

            effect = None
            if kind == "prepare":
                deadline = time.monotonic() + 5
                snapshot = await _wait(
                    asyncio.create_task(
                        workspace_io(
                            WorkspaceTreeSnapshotReader(binding.workspace).capture,
                            view.root_path,
                            authorize=self._bridge(fence.operation_id, check, abort, deadline),
                            cancelled=abort.is_set,
                        )
                    ),
                    abort.set,
                )
                await check()
                view = await self.store.record_baseline(
                    scope, fence, binding, snapshot, qualify=qualify
                )
            elif kind == "execute":
                target, before, token = await self.store.inputs(
                    scope, fence, binding, qualify=qualify
                )
                writer = WorkspaceDirectoryRestoreWriter(binding.workspace)
                deadline = time.monotonic() + 5
                change = await _wait(
                    asyncio.create_task(
                        workspace_io(
                            writer.prepare,
                            target,
                            expected_before=before.fingerprint,
                            token=token,
                            authorize=self._bridge(fence.operation_id, check, abort, deadline),
                            record_stage=self._bridge(
                                fence.operation_id,
                                lambda value: self.store.record_stage(
                                    scope, fence, binding, value, qualify=qualify
                                ),
                                abort,
                                deadline,
                            ),
                            cancelled=abort.is_set,
                        )
                    ),
                    abort.set,
                )
                await self.store.record_prepared(scope, fence, binding, change, qualify=qualify)
                deadline = time.monotonic() + 5
                effect = await _wait(
                    asyncio.create_task(
                        workspace_io(
                            writer.apply,
                            change,
                            authorize=self._bridge(fence.operation_id, check, abort, deadline),
                            record_intent=self._bridge(
                                fence.operation_id,
                                lambda value: self.store.record_intent(
                                    scope, fence, binding, value, qualify=qualify
                                ),
                                abort,
                                deadline,
                            ),
                            cancelled=abort.is_set,
                        )
                    ),
                    abort.set,
                )
            elif kind == "verify":
                change = await self.store.verification_input(scope, fence, binding, qualify=qualify)
                await check()
                effect = await _wait(
                    asyncio.create_task(
                        workspace_io(
                            WorkspaceDirectoryRestoreWriter(binding.workspace).reconcile,
                            change,
                        )
                    ),
                    abort.set,
                )
            else:
                raise _error("context", "目录恢复阶段无效", 403)
            await check()
            if view.backup is None:
                raise _error("unverified", "目录恢复缺少原完整备份")
            result = DirectoryRestoreStepView(
                plan_id=view.plan_id,
                operation_id=fence.operation_id,
                kind=fence.kind,
                root_path=view.root_path,
                target_fingerprint=view.target_snapshot.snapshot_fingerprint,
                baseline_fingerprint=view.backup.snapshot_fingerprint,
                receipt=DirectoryRestoreReceiptView.model_validate(effect.receipt.__dict__)
                if effect is not None
                else None,
            )
            if fence.operation_id in self._pending:
                raise _error("unverified", "原目录叶回程不能覆盖")
            self._pending[fence.operation_id] = _Pending(fence, result, base, effect)
            return result.model_dump(mode="json")
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error

    async def validate_step(self, kind: str, value: Any, context: ToolContext) -> None:
        try:
            scope, binding, fence, _ = await self._context(kind, context.arguments, context)
            result = DirectoryRestoreStepView.model_validate(value)
            pending = self._pending.get(fence.operation_id)
            if pending is None or pending.fence != fence or result != pending.result:
                raise _error("unverified", "目录恢复须保留原真实完整材料回程")

            def qualify() -> None:
                self._qualify(context)
                if not same_execution_budget(
                    pending.budget, execution_budget_base(context.execution_timeouts)
                ):
                    raise _error("context", "目录恢复后置Hook改变了原预算", 403)

            if kind == "prepare":
                await self.store.ready(scope, fence, binding, qualify=qualify)
            else:
                if pending.effect is None:
                    raise _error("unverified", "目录恢复缺少原真实effect")
                await self.store.settle(scope, fence, binding, pending.effect, qualify=qualify)
            self._pending.pop(fence.operation_id, None)
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error

    async def _renew(
        self,
        agent_id: str,
        scope: str,
        binding: WorkspaceBinding,
        fence: DirectoryRestoreFence,
        worker: asyncio.Task[None],
    ) -> None:
        try:
            while True:
                await asyncio.sleep(max(0.02, min(0.2, self.store.lease_seconds / 4)))
                await self.authorize(agent_id, fence.kind)
                await self.store.renew(scope, fence, binding, qualify=self._admission)
        except asyncio.CancelledError:
            raise
        except Exception:
            try:
                view = await self.store.get(scope, fence.plan_id)
                if view.phase != ("preparing" if fence.kind == "prepare" else "executing"):
                    return
            except Exception:
                pass
            if not worker.done() and not worker.cancelling():
                worker.cancel()

    async def _run(
        self,
        agent_id: str,
        scope: str,
        binding: WorkspaceBinding,
        fence: DirectoryRestoreFence,
        abort: Event,
        confirmed: bool,
    ) -> None:
        worker = asyncio.current_task()
        assert worker is not None
        marker = enter_workspace_execution(self._tasks, fence.operation_id)
        heartbeat = asyncio.create_task(self._renew(agent_id, scope, binding, fence, worker))

        async def fail(code: str) -> None:
            heartbeat.cancel()
            abort.set()
            try:
                await _wait(asyncio.create_task(self.store.fail(scope, fence, code)), lambda: None)
            except ServiceError:
                # 已取消/到期/失去owner时只能保持原事实，不能换token重新生产。
                pass

        try:
            await self.call(agent_id, fence, confirmed)
            view = await self.store.get(scope, fence.plan_id)
            if view.phase in {"preparing", "executing"}:
                raise _error("unverified", "目录叶没有完成原后置资格")
        except asyncio.CancelledError:
            await fail("workspace_directory_restore_cancelled")
        except ServiceError as error:
            await fail(error.code)
        except Exception:
            await fail("workspace_directory_restore_failed")
        finally:
            abort.set()
            heartbeat.cancel()

            async def drain() -> None:
                await asyncio.gather(heartbeat, return_exceptions=True)
                await asyncio.gather(
                    *tuple(self._callbacks.get(fence.operation_id, ())), return_exceptions=True
                )

            await _wait(asyncio.create_task(drain()), lambda: None)
            self._callbacks.pop(fence.operation_id, None)
            self._pending.pop(fence.operation_id, None)
            leave_workspace_execution(marker)

    async def wait(
        self, scope: str, identifier: str, *, timeout_seconds: float = 30
    ) -> DirectoryRestoreView:
        if type(timeout_seconds) not in {int, float} or not 0 <= timeout_seconds <= 300:
            raise _error("timeout", "等待须在0–300秒之间", 422)
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while True:
            view = await self.store.get(scope, identifier)
            remaining = deadline - asyncio.get_running_loop().time()
            if view.phase not in {"preparing", "executing"} or remaining <= 0:
                return view
            await asyncio.sleep(min(0.05, remaining))

    async def cancel(self, scope: str, identifier: str) -> DirectoryRestoreView:
        internal = inside_workspace_execution()

        async def stop() -> DirectoryRestoreView:
            view = await self.store.cancel(scope, identifier)
            entry = self._tasks.get(view.operation_id)
            if entry is not None and entry[0] == scope:
                abort = self._aborts.get(view.operation_id)
                if abort is not None:
                    abort.set()
                if not entry[1].done() and not entry[1].cancelling():
                    entry[1].cancel()
                if not internal:
                    await asyncio.gather(entry[1], return_exceptions=True)
            return await self.store.get(scope, identifier)

        return await _wait(asyncio.create_task(stop()), lambda: None)

    def assert_external_close(self) -> None:
        if self.model_close_guard is not None:
            self.model_close_guard()
        if inside_workspace_execution(self._tasks):
            raise _error("reentrant", "目录恢复回调只能取消原任务，不能关闭服务")

    async def close(self) -> None:
        self.assert_external_close()
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await _wait(self._close_task, lambda: None)

    async def _close(self) -> None:
        async with self._start_lock:
            self._closed = True
            entries = list(self._tasks.items())
        for identifier, (scope, task) in entries:
            try:
                async with self.store.database.sessions() as sql:
                    from krim_agent.persistence.database import WorkspaceDirectoryRestoreAttemptRow

                    attempt = await sql.get(WorkspaceDirectoryRestoreAttemptRow, identifier)
                    plan_id = attempt.plan_id if attempt is not None else identifier
                await self.store.cancel(scope, plan_id)
            except Exception:
                # 持久层故障仍须关闭本地资格并收束原线程，不能以回执未知重放。
                pass
            finally:
                abort = self._aborts.get(identifier)
                if abort is not None:
                    abort.set()
                if not task.done() and not task.cancelling():
                    task.cancel()

        async def drain() -> None:
            await asyncio.gather(*(entry[1][1] for entry in entries), return_exceptions=True)

        await _wait(asyncio.create_task(drain()), lambda: None)
