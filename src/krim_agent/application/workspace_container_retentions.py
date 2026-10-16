"""空事务容器受控Host编排；物理意图先行，全部Hook后才保存独立回执。"""

from __future__ import annotations

import asyncio
import secrets
import time
from collections.abc import Callable, Coroutine
from concurrent.futures import TimeoutError as FutureTimeout
from contextvars import ContextVar
from dataclasses import dataclass, field
from threading import Event
from typing import Any
from uuid import uuid4

from sqlalchemy import select

from krim_agent.persistence.database import WorkspaceContainerRetentionRow
from krim_agent.persistence.workspace_container_retentions import WorkspaceContainerRetentionStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.tools.container_retention_guard import validate_container_retention_parent
from krim_agent.tools.core import (
    ExecutionBudgetBase,
    ToolContext,
    ToolError,
    ToolExecutionBudget,
    execution_budget_base,
    same_execution_budget,
)
from krim_agent.tools.workspace import workspace_io
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_container_retention import (
    ContainerRetentionFence,
    ContainerRetentionPreview,
    ContainerRetentionReceipt,
    ContainerRetentionRequest,
    ContainerRetentionView,
    StoredContainerRetentionSource,
)
from krim_agent.workspace_container_retention_control import (
    CONTAINER_RETENTION_STEP_TOOLS,
    ContainerRetentionInspectInput,
    ContainerRetentionInspection,
    ContainerRetentionOperationInput,
    ContainerRetentionStepView,
    RetentionStepKind,
)
from krim_agent.workspace_empty_container_retention import (
    EmptyContainerObservation,
    EmptyContainerReceipt,
    WorkspaceEmptyContainerRetention,
)

from .workspace_changes import WorkspaceChangeService, _wait
from .workspace_execution import (
    enter_workspace_execution,
    inside_workspace_execution,
    leave_workspace_execution,
)

Selector = ContainerRetentionInspection | ContainerRetentionFence
RetentionAuthorize = Callable[
    [str, RetentionStepKind], Coroutine[Any, Any, tuple[str, WorkspaceBinding]]
]
RetentionCall = Callable[[str, Selector, bool], Coroutine[Any, Any, object]]
RetentionScope = Callable[[str], Coroutine[Any, Any, str]]
_current_retention: ContextVar[str | None] = ContextVar("krim_container_retention", default=None)


def _error(kind: str, text: str, status: int = 409) -> ServiceError:
    return ServiceError("workspace_container_retention_" + kind, text, status)


@dataclass(frozen=True)
class _Job:
    agent_id: str
    scope: str
    binding: WorkspaceBinding = field(repr=False)
    selector: Selector = field(repr=False)


@dataclass(frozen=True)
class _Pending:
    selector: Selector
    result: ContainerRetentionPreview | ContainerRetentionStepView
    budget: ExecutionBudgetBase = field(repr=False)
    effect: EmptyContainerReceipt | EmptyContainerObservation = field(repr=False)


class WorkspaceContainerRetentionService:
    def __init__(
        self,
        store: WorkspaceContainerRetentionStore,
        changes: WorkspaceChangeService,
        authorize: RetentionAuthorize,
        call: RetentionCall,
        read_scope: RetentionScope,
        *,
        max_active: int = 16,
    ) -> None:
        if type(max_active) is not int or not 1 <= max_active <= 64:
            raise ValueError("回收并发上限无效")
        self.store, self.changes = store, changes
        self.authorize, self.call, self.read_scope = authorize, call, read_scope
        self.max_active = max_active
        self._tasks: dict[str, tuple[str, asyncio.Task[None]]] = {}
        self._jobs: dict[str, _Job] = {}
        self._aborts: dict[str, Event] = {}
        self._callbacks: dict[str, set[asyncio.Task[Any]]] = {}
        self._pending: dict[str, _Pending] = {}
        self._used: set[str] = set()
        self._previews: dict[str, ContainerRetentionPreview] = {}
        self._start_lock = asyncio.Lock()
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None

    def _admission(self) -> None:
        if self._closed:
            raise _error("closed", "回收服务已关闭", 503)

    def _capacity(self) -> None:
        self._admission()
        if sum(not task.done() for _, task in self._tasks.values()) >= self.max_active:
            raise _error("busy", "回收并发已满", 429)

    @staticmethod
    def _confirmation(confirmed: bool) -> None:
        if type(confirmed) is not bool:
            raise _error("parameters", "回收确认须为实际布尔值", 422)

    @staticmethod
    def _identifier(selector: Selector) -> str:
        if type(selector) is ContainerRetentionInspection:
            return selector.inspection_id
        assert type(selector) is ContainerRetentionFence
        return selector.operation_id

    def _stop(self, identifier: str) -> None:
        abort = self._aborts.get(identifier)
        if abort is not None:
            abort.set()
        entry = self._tasks.get(identifier)
        if entry is not None and not entry[1].done() and not entry[1].cancelling():
            entry[1].cancel()

    def _spawn(self, job: _Job, confirmed: bool) -> asyncio.Task[None]:
        identifier = self._identifier(job.selector)
        abort = Event()
        self._jobs[identifier], self._aborts[identifier] = job, abort
        task = asyncio.create_task(self._run(identifier, job, abort, confirmed))
        self._tasks[identifier] = job.scope, task

        def done(result: asyncio.Task[None]) -> None:
            current = self._tasks.get(identifier)
            if current is not None and current[1] is result:
                self._tasks.pop(identifier, None)
                self._jobs.pop(identifier, None)
                self._aborts.pop(identifier, None)
            if not result.cancelled():
                result.exception()

        task.add_done_callback(done)
        return task

    async def preview(
        self, agent_id: str, source_plan_id: str, *, confirmed: bool = False
    ) -> ContainerRetentionPreview:
        self._confirmation(confirmed)
        source_plan_id = ContainerRetentionInspectInput(
            source_plan_id=source_plan_id
        ).source_plan_id
        self._admission()
        scope, binding = await self.authorize(agent_id, "inspect")
        selector = ContainerRetentionInspection(str(uuid4()), source_plan_id, secrets.token_hex(32))
        async with self._start_lock:
            self._capacity()
            task = self._spawn(_Job(agent_id, scope, binding, selector), confirmed)
        try:
            await _wait(task, lambda: self._stop(selector.inspection_id))
            result = self._previews.pop(selector.inspection_id, None)
            if result is None:
                raise _error("unverified", "预览未保留原真实回程")
            return result
        finally:
            self._previews.pop(selector.inspection_id, None)

    async def start(
        self, agent_id: str, request: ContainerRetentionRequest, *, confirmed: bool = False
    ) -> ContainerRetentionView:
        self._confirmation(confirmed)
        request = ContainerRetentionRequest.model_validate(request.model_dump(mode="json"))
        return await _wait(
            asyncio.create_task(self._start(agent_id, request, confirmed)), lambda: None
        )

    async def _start(
        self, agent_id: str, request: ContainerRetentionRequest, confirmed: bool
    ) -> ContainerRetentionView:
        scope = await self.read_scope(agent_id)
        async with self._start_lock:
            previous = await self.store.by_key(scope, request.request_key)
            if previous is not None:
                if previous.request != request:
                    raise _error("key_conflict", "原回收键已绑定不同请求")
                return previous
            self._capacity()
            actual_scope, binding = await self.authorize(agent_id, "remove")
            if actual_scope != scope:
                raise _error("context", "回收准入须保留原会话", 403)
            self._admission()
            view, created = await self.store.reserve(
                scope, binding, request, qualify=self._admission
            )
            if created:
                fence = await self.store.execution(scope, view.operation_id)
                self._spawn(_Job(agent_id, scope, binding, fence), confirmed)
            return view

    async def verify(
        self,
        agent_id: str,
        operation_id: str,
        *,
        expected_revision: int,
        request_key: str,
        confirmed: bool = False,
    ) -> ContainerRetentionView:
        self._confirmation(confirmed)
        return await _wait(
            asyncio.create_task(
                self._verify(agent_id, operation_id, expected_revision, request_key, confirmed)
            ),
            lambda: None,
        )

    async def _verify(
        self,
        agent_id: str,
        operation_id: str,
        expected_revision: int,
        request_key: str,
        confirmed: bool,
    ) -> ContainerRetentionView:
        scope, binding = await self.authorize(agent_id, "verify")
        async with self._start_lock:
            self._admission()

            def qualify() -> None:
                entry = self._tasks.get(operation_id)
                if entry is not None and not entry[1].done():
                    raise _error("busy", "原线程尚未收束，不能并行核实")
                self._capacity()

            view, fence = await self.store.claim_verify(
                scope,
                operation_id,
                binding,
                expected_revision=expected_revision,
                request_key=request_key,
                qualify=qualify,
            )
            if fence is not None:
                self._spawn(_Job(agent_id, scope, binding, fence), confirmed)
            return view

    def _qualify(self, context: ToolContext) -> None:
        self._admission()
        selector = context.container_retention
        if type(selector) not in {ContainerRetentionInspection, ContainerRetentionFence}:
            raise _error("context", "回收缺少原实际围栏", 403)
        assert selector is not None
        try:
            validate_container_retention_parent(
                CONTAINER_RETENTION_STEP_TOOLS.get(selector.kind, ""), context
            )
        except ToolError as error:
            raise ServiceError(error.code, str(error), 403) from error
        identifier = self._identifier(selector)
        job, entry = self._jobs.get(identifier), self._tasks.get(identifier)
        abort = self._aborts.get(identifier)
        if (
            job is None
            or entry is None
            or entry[0] != job.scope
            or job.scope != "session:" + str(context.session_id)
            or job.agent_id != context.agent_id
            or context.workspace is not job.binding
            or selector != job.selector
            or context.workspace_container_retentions is not self
            or not inside_workspace_execution(self._tasks)
            or _current_retention.get() != identifier
            or abort is None
            or abort.is_set()
            or type(context.execution_timeouts) is not tuple
            or not context.execution_timeouts
            or any(type(value) is not ToolExecutionBudget for value in context.execution_timeouts)
        ):
            raise _error("context", "回收须保留原受管调用、资源与预算", 403)
        loop = asyncio.get_running_loop()
        for budget in context.execution_timeouts:
            deadline = budget.deadline.when()
            if (
                not budget.active
                or budget.deadline.expired()
                or deadline is not None
                and loop.time() >= deadline
            ):
                raise _error("timeout", "原回收工具预算已结束", 408)
        job.binding.require()

    async def _context(
        self, kind: str, payload: dict[str, Any], context: ToolContext
    ) -> tuple[
        str, _Job, StoredContainerRetentionSource, ContainerRetentionView | None, str | None
    ]:
        self._qualify(context)
        selector = context.container_retention
        assert type(selector) in {ContainerRetentionInspection, ContainerRetentionFence}
        assert selector is not None
        identifier = self._identifier(selector)
        job = self._jobs[identifier]
        selector = job.selector
        if kind != selector.kind:
            raise _error("context", "回收阶段与原围栏不同", 403)
        if type(selector) is ContainerRetentionInspection:
            if ContainerRetentionInspectInput.model_validate(payload).source_plan_id != (
                selector.source_plan_id
            ):
                raise _error("context", "预览须保留原来源", 403)
        elif ContainerRetentionOperationInput.model_validate(payload).operation_id != identifier:
            raise _error("context", "回收须保留原操作", 403)
        async with self.store.database.write_lock, self.store.database.sessions() as sql:
            scope, binding = await self.changes._guard(
                context, writing=kind == "remove", sql_session=sql
            )
            if scope != job.scope or binding is not job.binding:
                raise _error("context", "回收Scope或工作区改变", 403)
            if type(selector) is ContainerRetentionInspection:
                source = await self.store.sources.read(
                    sql,
                    scope,
                    selector.source_plan_id,
                    workspace_key=binding.key,
                    resource_fingerprint=binding.fingerprint,
                    workspace_fingerprint=binding.workspace.fingerprint,
                )
                existing = await sql.scalar(
                    select(WorkspaceContainerRetentionRow.operation_id).where(
                        WorkspaceContainerRetentionRow.active_source_id == selector.source_plan_id,
                        WorkspaceContainerRetentionRow.scope_id == scope,
                    )
                )
                self._qualify(context)
                return identifier, job, source, None, existing
        assert type(selector) is ContainerRetentionFence
        source = await self.store.inputs(
            job.scope, selector, job.binding, qualify=lambda: self._qualify(context)
        )
        view = await self.store.get(job.scope, identifier)
        self._qualify(context)
        return identifier, job, source, view, identifier

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

        def check() -> None:
            if abort.is_set() or time.monotonic() >= deadline:
                raise _error("stopped", "回收已取消或达到原5秒物理截止")

        def call(*args: object) -> Any:
            check()
            coroutine = invoke(*args)
            try:
                future = asyncio.run_coroutine_threadsafe(coroutine, loop)
            except RuntimeError:
                coroutine.close()
                raise _error("unavailable", "原回收loop不可用", 503) from None
            try:
                while True:
                    check()
                    try:
                        value = future.result(timeout=min(0.05, deadline - time.monotonic()))
                        check()
                        return value
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
            identifier, job, source, view, existing = await self._context(kind, payload, context)
            if identifier in self._used:
                raise _error("unverified", "原回收叶不能重复物理阶段")
            self._used.add(identifier)
            base = execution_budget_base(context.execution_timeouts)
            abort = self._aborts[identifier]

            def qualify() -> None:
                self._qualify(context)
                if not same_execution_budget(
                    base, execution_budget_base(context.execution_timeouts)
                ):
                    raise _error("context", "回收原预算被改变", 403)

            async def check() -> None:
                current, actual, origin, _, _ = await self._context(kind, payload, context)
                if current != identifier or actual is not job or origin != source:
                    raise _error("context", "原回收资格或Source已改变", 403)
                qualify()

            core = WorkspaceEmptyContainerRetention(job.binding.workspace)
            deadline = time.monotonic() + 5
            authorize = self._bridge(identifier, check, abort, deadline)
            effect: EmptyContainerReceipt | EmptyContainerObservation
            result: ContainerRetentionPreview | ContainerRetentionStepView
            if kind == "remove":
                assert view is not None and type(job.selector) is ContainerRetentionFence
                fence = job.selector
                effect = await _wait(
                    asyncio.create_task(
                        workspace_io(
                            core.remove,
                            source.stage,
                            expected_fingerprint=view.request.expected_container_fingerprint,
                            confirmed=True,
                            authorize=authorize,
                            record_intent=self._bridge(
                                identifier,
                                lambda observed: self.store.record_intent(
                                    job.scope, fence, job.binding, observed, qualify=qualify
                                ),
                                abort,
                                deadline,
                            ),
                            cancelled=abort.is_set,
                        )
                    ),
                    abort.set,
                )
                result = ContainerRetentionStepView(
                    kind="remove",
                    operation_id=identifier,
                    receipt=ContainerRetentionReceipt(
                        outcome=effect.status,
                        expected_fingerprint=effect.expected_fingerprint,
                        observed_fingerprint=effect.observed_fingerprint,
                        changed=effect.changed,
                        recovery_required=effect.recovery_required,
                        error_code=effect.error_code,
                    ),
                    observation=None,
                )
            else:
                effect = await _wait(
                    asyncio.create_task(
                        workspace_io(core.inspect, source.stage, authorize=authorize)
                    ),
                    abort.set,
                )
                observed = ContainerRetentionPreview(
                    source=source.view,
                    status=effect.status,
                    fingerprint=effect.fingerprint,
                    error_code=effect.error_code,
                    existing_operation_id=existing,
                )
                result = (
                    observed
                    if kind == "inspect"
                    else ContainerRetentionStepView(
                        kind="verify", operation_id=identifier, receipt=None, observation=observed
                    )
                )
            await check()
            self._pending[identifier] = _Pending(job.selector, result, base, effect)
            return result.model_dump(mode="json")
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error

    async def validate_step(self, kind: str, value: Any, context: ToolContext) -> None:
        try:
            identifier, job, source, _, existing = await self._context(
                kind, context.arguments, context
            )
            pending = self._pending.get(identifier)
            result = (
                ContainerRetentionPreview.model_validate(value)
                if kind == "inspect"
                else ContainerRetentionStepView.model_validate(value)
            )
            if pending is None or pending.selector != job.selector or pending.result != result:
                raise _error("unverified", "须保留本次真实回收候选")

            def qualify() -> None:
                self._qualify(context)
                if not same_execution_budget(
                    pending.budget, execution_budget_base(context.execution_timeouts)
                ):
                    raise _error("context", "后置Hook改变了原回收预算", 403)

            qualify()
            if type(job.selector) is ContainerRetentionInspection:
                assert type(result) is ContainerRetentionPreview
                if result.source != source.view or result.existing_operation_id != existing:
                    raise _error("unverified", "预览来源或原回收占位已变化")
                self._previews[identifier] = result
            elif kind == "remove":
                assert type(pending.effect) is EmptyContainerReceipt
                assert type(job.selector) is ContainerRetentionFence
                await self.store.settle(
                    job.scope, job.selector, job.binding, pending.effect, qualify=qualify
                )
            else:
                assert type(pending.effect) is EmptyContainerObservation
                assert type(job.selector) is ContainerRetentionFence
                await self.store.finish_verify(
                    job.scope, job.selector, job.binding, pending.effect, qualify=qualify
                )
            self._pending.pop(identifier, None)
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error

    async def _renew(self, job: _Job, worker: asyncio.Task[None]) -> None:
        assert type(job.selector) is ContainerRetentionFence
        try:
            while True:
                await asyncio.sleep(max(0.02, min(5, self.store.lease_seconds / 4)))
                scope, binding = await self.authorize(job.agent_id, job.selector.kind)
                if scope != job.scope or binding is not job.binding:
                    raise _error("context", "续租须保留原Scope和资源", 403)
                await self.store.renew(
                    job.scope, job.selector, job.binding, qualify=self._admission
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            try:
                if (await self.store.get(job.scope, job.selector.operation_id)).status != "running":
                    return
            except Exception:
                pass
            if not worker.done() and not worker.cancelling():
                worker.cancel()

    async def _run(self, identifier: str, job: _Job, abort: Event, confirmed: bool) -> None:
        marker = enter_workspace_execution(self._tasks, identifier)
        current = _current_retention.set(identifier)
        worker = asyncio.current_task()
        assert worker is not None
        heartbeat = (
            asyncio.create_task(self._renew(job, worker))
            if type(job.selector) is ContainerRetentionFence
            else None
        )

        async def drain() -> None:
            if heartbeat is not None:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
            await asyncio.gather(
                *tuple(self._callbacks.get(identifier, ())), return_exceptions=True
            )

        async def fail(code: str) -> None:
            abort.set()
            for callback in tuple(self._callbacks.get(identifier, ())):
                if not callback.done() and not callback.cancelling():
                    callback.cancel()

            async def finish() -> None:
                await drain()
                if type(job.selector) is ContainerRetentionFence:
                    try:
                        await self.store.fail(job.scope, job.selector, code)
                    except ServiceError:
                        # 旧epoch/失主不得换token覆盖新核实；原键保留可读事实。
                        pass

            await _wait(asyncio.create_task(finish()), lambda: None)

        try:
            await self.call(job.agent_id, job.selector, confirmed)
            if type(job.selector) is ContainerRetentionInspection:
                if identifier not in self._previews:
                    raise _error("unverified", "原预览没有通过后置核验")
            elif (await self.store.get(job.scope, identifier)).status == "running":
                raise _error("unverified", "原回收没有通过后置核验")
        except asyncio.CancelledError:
            await fail("workspace_container_retention_cancelled")
            if type(job.selector) is ContainerRetentionInspection:
                raise
        except Exception as error:
            await fail(
                error.code
                if isinstance(error, ServiceError)
                else "workspace_container_retention_failed"
            )
            if type(job.selector) is ContainerRetentionInspection:
                raise
        finally:
            abort.set()
            try:
                await _wait(asyncio.create_task(drain()), lambda: None)
            finally:
                self._callbacks.pop(identifier, None)
                self._pending.pop(identifier, None)
                self._used.discard(identifier)
                leave_workspace_execution(marker)
                _current_retention.reset(current)

    async def wait(
        self, scope: str, operation_id: str, *, timeout_seconds: float = 30
    ) -> ContainerRetentionView:
        if type(timeout_seconds) not in {int, float} or not 0 <= timeout_seconds <= 300:
            raise _error("parameters", "等待须在0–300秒之间", 422)
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while True:
            view = await self.store.get(scope, operation_id)
            remaining = deadline - asyncio.get_running_loop().time()
            if view.status != "running" or remaining <= 0:
                return view
            await asyncio.sleep(min(0.05, remaining))

    async def cancel(
        self, agent_id: str, operation_id: str, *, expected_revision: int
    ) -> ContainerRetentionView:
        internal = inside_workspace_execution()

        async def stop() -> ContainerRetentionView:
            scope, binding = await self.authorize(agent_id, "verify")
            view = await self.store.cancel(
                scope,
                operation_id,
                binding,
                expected_revision=expected_revision,
                qualify=self._admission,
            )
            entry = self._tasks.get(operation_id)
            if entry is not None and entry[0] == scope:
                self._stop(operation_id)
                if not internal:
                    await asyncio.gather(entry[1], return_exceptions=True)
            return await self.store.get(scope, view.operation_id)

        return await _wait(asyncio.create_task(stop()), lambda: None)

    def assert_external_close(self) -> None:
        if inside_workspace_execution(self._tasks):
            raise _error("reentrant", "回收回调只能取消原任务，不能关闭自身服务")

    async def close(self) -> None:
        self.assert_external_close()
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await _wait(self._close_task, lambda: None)

    async def _close(self) -> None:
        async with self._start_lock:
            self._closed = True
            entries = list(self._tasks.items())
            jobs = dict(self._jobs)
        for identifier, _entry in entries:
            self._stop(identifier)
            job = jobs[identifier]
            if type(job.selector) is ContainerRetentionFence:
                try:
                    view = await self.store.get(job.scope, identifier)
                    await self.store.cancel(
                        job.scope,
                        identifier,
                        job.binding,
                        expected_revision=view.revision,
                        qualify=lambda: None,
                    )
                except Exception:
                    # 即使SQL回程未知，仍先停止物理资格并等待真实线程；不再生产。
                    try:
                        await self.store.fail(
                            job.scope, job.selector, "workspace_container_retention_closed"
                        )
                    except Exception:
                        pass
        await _wait(
            asyncio.create_task(self._drain_tasks([entry[1] for _, entry in entries])), lambda: None
        )

    @staticmethod
    async def _drain_tasks(tasks: list[asyncio.Task[None]]) -> None:
        await asyncio.gather(*tasks, return_exceptions=True)
