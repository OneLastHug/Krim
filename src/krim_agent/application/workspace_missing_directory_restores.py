"""目录恢复受控Host编排；准备、真实发布和只读核实沿原Registry与预算。"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Coroutine
from concurrent.futures import TimeoutError as FutureTimeout
from dataclasses import dataclass, field
from threading import Event
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.persistence.workspace_missing_directory_restores import (
    WorkspaceMissingDirectoryRestoreStore,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.tools.core import (
    ExecutionBudgetBase,
    ToolContext,
    ToolError,
    ToolExecutionBudget,
    execution_budget_base,
    same_execution_budget,
)
from krim_agent.tools.missing_directory_restore_guard import (
    validate_missing_directory_restore_parent,
)
from krim_agent.tools.workspace import workspace_io
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader
from krim_agent.workspace_directory_restore import directory_restore_model_source
from krim_agent.workspace_directory_restore_control import DirectoryRestoreStepKind
from krim_agent.workspace_missing_directory_restore import (
    MissingDirectoryRestoreExecute,
    MissingDirectoryRestoreFence,
    MissingDirectoryRestorePreview,
    MissingDirectoryRestoreReceiptView,
    MissingDirectoryRestoreView,
)
from krim_agent.workspace_missing_directory_restore_control import (
    MISSING_DIRECTORY_RESTORE_STEP_TOOLS,
    MissingDirectoryRestoreStepInput,
    MissingDirectoryRestoreStepView,
)
from krim_agent.workspace_missing_directory_writer import (
    MissingDirectoryRestoreEffect,
    WorkspaceMissingDirectoryRestoreWriter,
)

from .workspace_changes import WorkspaceChangeService, _wait
from .workspace_execution import (
    enter_workspace_execution,
    inside_workspace_execution,
    leave_workspace_execution,
)

RestoreAuthorize = Callable[
    [str, DirectoryRestoreStepKind], Coroutine[Any, Any, tuple[str, WorkspaceBinding]]
]
RestoreCall = Callable[[str, MissingDirectoryRestoreFence, bool], Coroutine[Any, Any, object]]


@dataclass(frozen=True)
class MissingDirectoryRestoreInvocation:
    source: CheckpointModelSource
    authorize: RestoreAuthorize
    call: RestoreCall
    qualify: Callable[[], None] = field(repr=False)


def _error(kind: str, text: str, status: int = 409) -> ServiceError:
    return ServiceError("workspace_missing_directory_restore_" + kind, text, status)


@dataclass(frozen=True)
class _Pending:
    fence: MissingDirectoryRestoreFence
    result: MissingDirectoryRestoreStepView
    budget: ExecutionBudgetBase = field(repr=False)
    effect: MissingDirectoryRestoreEffect | None = field(default=None, repr=False)


class WorkspaceMissingDirectoryRestoreService:
    def __init__(
        self,
        store: WorkspaceMissingDirectoryRestoreStore,
        changes: WorkspaceChangeService,
        authorize: RestoreAuthorize,
        call: RestoreCall,
        *,
        max_active: int = 16,
        max_observations: int = 128,
    ) -> None:
        if type(max_active) is not int or not 1 <= max_active <= 64:
            raise ValueError("目录恢复并发上限无效")
        self.store, self.changes, self.authorize, self.call = store, changes, authorize, call
        if type(max_observations) is not int or not 1 <= max_observations <= 128:
            raise ValueError("缺失恢复原观察保留上限无效")
        self.max_active, self.max_observations = max_active, max_observations
        self._observation_slots: dict[str, tuple[str, float]] = {}
        self._readers: dict[str, WorkspaceDirectoryAbsenceReader] = {}
        self._writers: dict[str, tuple[str, WorkspaceMissingDirectoryRestoreWriter]] = {}
        self._tasks: dict[str, tuple[str, asyncio.Task[None]]] = {}
        self._aborts: dict[str, Event] = {}
        self._fences: dict[str, MissingDirectoryRestoreFence] = {}
        self._invocations: dict[str, MissingDirectoryRestoreInvocation | None] = {}
        self._callbacks: dict[str, set[asyncio.Task[Any]]] = {}
        self._pending: dict[str, _Pending] = {}
        self._start_lock = asyncio.Lock()
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None
        self.model_close_guard: Callable[[], None] | None = None

    def _admission(self) -> None:
        if self._closed:
            raise _error("closed", "目录恢复服务已关闭", 503)

    def _release_plan(self, identifier: str) -> None:
        reader = self._readers.pop(identifier, None)
        self._observation_slots.pop(identifier, None)
        for operation_id, (plan_id, _writer) in tuple(self._writers.items()):
            if plan_id == identifier:
                self._writers.pop(operation_id, None)
        if reader is not None:
            reader.close()

    def _prune_observations(self) -> None:
        active = {fence.plan_id for fence in self._fences.values()}
        retained = {plan_id for plan_id, _writer in self._writers.values()}
        for identifier, (_scope, expires_at) in tuple(self._observation_slots.items()):
            if expires_at <= time.time() and identifier not in active | retained:
                self._release_plan(identifier)

    def _capacity(self, *, reserve_observation: bool = False) -> None:
        self._prune_observations()
        if sum(not task.done() for _, task in self._tasks.values()) >= self.max_active:
            raise _error("busy", "缺失恢复并发已满", 429)
        if reserve_observation and len(self._observation_slots) >= self.max_observations:
            raise _error("busy", "原缺失观察保留已达上限，请先收束原计划", 429)

    async def _release_finished(self, scope: str, identifier: str) -> None:
        try:
            state = await self.store.get(scope, identifier)
        except Exception:
            # 无法读取原事实时不继续保留发布准入；真实SQL材料和未知不重放。
            self._release_plan(identifier)
            return
        if state.phase not in {"preparing", "ready", "executing", "uncertain"}:
            self._release_plan(identifier)

    @staticmethod
    def _confirmation(confirmed: bool) -> None:
        if type(confirmed) is not bool:
            raise _error("input", "目录恢复确认必须为布尔值", 422)

    def _job_qualify(self, invocation: MissingDirectoryRestoreInvocation | None) -> None:
        self._admission()
        if invocation is not None:
            if (
                type(invocation) is not MissingDirectoryRestoreInvocation
                or directory_restore_model_source(invocation.source) is None
                or not all(
                    callable(value)
                    for value in (invocation.authorize, invocation.call, invocation.qualify)
                )
            ):
                raise _error("model_context", "模型恢复必须保留原私有父调用资格", 403)
            invocation.qualify()

    def _spawn(
        self,
        agent_id: str,
        scope: str,
        binding: WorkspaceBinding,
        fence: MissingDirectoryRestoreFence,
        confirmed: bool,
        invocation: MissingDirectoryRestoreInvocation | None = None,
    ) -> None:
        identifier = fence.operation_id
        abort = Event()
        task = asyncio.create_task(
            self._run(agent_id, scope, binding, fence, abort, confirmed, invocation)
        )
        self._tasks[identifier] = (scope, task)
        self._aborts[identifier] = abort
        self._fences[identifier] = fence
        self._invocations[identifier] = invocation

        def done(result: asyncio.Task[None]) -> None:
            current = self._tasks.get(identifier)
            if current is not None and current[1] is result:
                self._tasks.pop(identifier, None)
            if self._aborts.get(identifier) is abort:
                self._aborts.pop(identifier, None)
            if self._fences.get(identifier) is fence:
                self._fences.pop(identifier, None)
                self._invocations.pop(identifier, None)
            if not result.cancelled():
                result.exception()

        task.add_done_callback(done)

    async def preview(
        self, agent_id: str, request: MissingDirectoryRestorePreview, *, confirmed: bool = False
    ) -> MissingDirectoryRestoreView:
        self._confirmation(confirmed)
        request = MissingDirectoryRestorePreview.model_validate(request.model_dump(mode="json"))
        return await _wait(
            asyncio.create_task(self._preview(agent_id, request, confirmed)), lambda: None
        )

    async def _preview(
        self,
        agent_id: str,
        request: MissingDirectoryRestorePreview,
        confirmed: bool,
        invocation: MissingDirectoryRestoreInvocation | None = None,
    ) -> MissingDirectoryRestoreView:
        self._job_qualify(invocation)
        authority = invocation.authorize if invocation is not None else self.authorize
        scope, binding = await authority(agent_id, "prepare")
        async with self._start_lock:
            self._job_qualify(invocation)
            if await self.store.by_key(scope, request.request_key) is None:
                self._capacity(reserve_observation=True)
            view, created = await self.store.reserve(
                scope,
                binding,
                request,
                model_source=invocation.source if invocation is not None else None,
                qualify=lambda: self._job_qualify(invocation),
            )
            if created:
                self._job_qualify(invocation)
                self._observation_slots[view.plan_id] = (scope, view.expires_at)
                self._spawn(
                    agent_id,
                    scope,
                    binding,
                    await self.store.preparation(scope, view.plan_id),
                    confirmed,
                    invocation,
                )
            return view

    async def preview_model(
        self,
        agent_id: str,
        request: MissingDirectoryRestorePreview,
        invocation: MissingDirectoryRestoreInvocation,
    ) -> MissingDirectoryRestoreView:
        self._job_qualify(invocation)
        request = MissingDirectoryRestorePreview.model_validate(request.model_dump(mode="json"))
        return await _wait(
            asyncio.create_task(self._preview(agent_id, request, False, invocation)), lambda: None
        )

    async def start(
        self, agent_id: str, request: MissingDirectoryRestoreExecute, *, confirmed: bool = False
    ) -> MissingDirectoryRestoreView:
        self._confirmation(confirmed)
        request = MissingDirectoryRestoreExecute.model_validate(request.model_dump(mode="json"))
        return await _wait(
            asyncio.create_task(self._start(agent_id, request, confirmed)), lambda: None
        )

    async def _start(
        self,
        agent_id: str,
        request: MissingDirectoryRestoreExecute,
        confirmed: bool,
        invocation: MissingDirectoryRestoreInvocation | None = None,
    ) -> MissingDirectoryRestoreView:
        self._job_qualify(invocation)
        authority = invocation.authorize if invocation is not None else self.authorize
        scope, binding = await authority(agent_id, "execute")
        async with self._start_lock:
            self._job_qualify(invocation)
            if (await self.store.get(scope, request.plan_id)).execute_key is None:
                self._capacity()
            operation, created = await self.store.start(
                scope,
                binding,
                request,
                model_source=invocation.source if invocation is not None else None,
                qualify=lambda: self._job_qualify(invocation),
            )
            if created:
                self._job_qualify(invocation)
                self._spawn(
                    agent_id,
                    scope,
                    binding,
                    await self.store.execution(scope, operation.operation_id),
                    confirmed,
                    invocation,
                )
            return await self.store.get(scope, request.plan_id)

    async def start_model(
        self,
        agent_id: str,
        request: MissingDirectoryRestoreExecute,
        invocation: MissingDirectoryRestoreInvocation,
    ) -> MissingDirectoryRestoreView:
        self._job_qualify(invocation)
        request = MissingDirectoryRestoreExecute.model_validate(request.model_dump(mode="json"))
        return await _wait(
            asyncio.create_task(self._start(agent_id, request, False, invocation)), lambda: None
        )

    async def verify(
        self, agent_id: str, operation_id: str, *, confirmed: bool = False
    ) -> MissingDirectoryRestoreView:
        self._confirmation(confirmed)
        return await _wait(
            asyncio.create_task(self._verify(agent_id, operation_id, confirmed)), lambda: None
        )

    async def _verify(
        self,
        agent_id: str,
        operation_id: str,
        confirmed: bool,
        invocation: MissingDirectoryRestoreInvocation | None = None,
    ) -> MissingDirectoryRestoreView:
        self._job_qualify(invocation)
        authority = invocation.authorize if invocation is not None else self.authorize
        scope, binding = await authority(agent_id, "verify")
        async with self._start_lock:
            self._job_qualify(invocation)
            previous = await self.store.operation(scope, operation_id)
            current = self._tasks.get(operation_id)
            if current is not None and current[0] == scope and not current[1].done():
                raise _error("busy", "原目录操作仍在执行，不能并行核实")
            if previous.status == "uncertain":
                self._capacity()
            operation, fence = await self.store.claim_verify(
                scope,
                operation_id,
                binding,
                model_source=invocation.source if invocation is not None else None,
                qualify=lambda: self._job_qualify(invocation),
            )
            if fence is not None:
                self._job_qualify(invocation)
                self._spawn(agent_id, scope, binding, fence, confirmed, invocation)
            return await self.store.get(scope, operation.plan_id)

    async def verify_model(
        self, agent_id: str, operation_id: str, invocation: MissingDirectoryRestoreInvocation
    ) -> MissingDirectoryRestoreView:
        self._job_qualify(invocation)
        return await _wait(
            asyncio.create_task(self._verify(agent_id, operation_id, False, invocation)),
            lambda: None,
        )

    def _qualify(self, context: ToolContext) -> None:
        self._admission()
        fence = context.directory_restore
        if type(fence) is not MissingDirectoryRestoreFence or type(fence.kind) is not str:
            raise _error("context", "缺少原私有目录恢复围栏", 403)
        try:
            validate_missing_directory_restore_parent(
                MISSING_DIRECTORY_RESTORE_STEP_TOOLS.get(fence.kind, ""), context
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
        if context.directory_restore_qualify is not None:
            context.directory_restore_qualify()
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
    ) -> tuple[str, WorkspaceBinding, MissingDirectoryRestoreFence, MissingDirectoryRestoreView]:
        self._qualify(context)
        wanted = MissingDirectoryRestoreStepInput.model_validate(payload)
        fence = context.directory_restore
        assert type(fence) is MissingDirectoryRestoreFence
        if kind != fence.kind or (wanted.plan_id, wanted.operation_id) != (
            fence.plan_id,
            fence.operation_id,
        ):
            raise _error("context", "目录叶必须引用原计划/操作与阶段", 403)
        assert context.session_id is not None
        scope = "session:" + context.session_id
        binding = context.workspace
        if binding is None:
            raise ServiceError("workspace_unbound", "当前会话没有工作区授权", 409)

        async def authorize(sql: AsyncSession) -> None:
            actual_scope, actual = await self.changes._guard(context, sql_session=sql)
            if actual_scope != scope or actual is not binding:
                raise _error("context", "目录资格必须保留原主体与资源", 403)

        if kind == "prepare":
            view = await self.store.preparation_view(
                scope, fence, binding, qualify=lambda: self._qualify(context), authorize=authorize
            )
        else:
            view = await self.store.execution_view(
                scope, fence, binding, qualify=lambda: self._qualify(context), authorize=authorize
            )
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

            if fence.operation_id in self._pending:
                raise _error("unverified", "原缺失叶不得再次运行生产阶段")
            effect = None
            if kind == "prepare":
                if fence.plan_id in self._readers:
                    raise _error("unverified", "原缺失观察不得重新捕获")
                reader = WorkspaceDirectoryAbsenceReader(binding.workspace)
                self._readers[fence.plan_id] = reader
                deadline = time.monotonic() + 5
                snapshot = await _wait(
                    asyncio.create_task(
                        workspace_io(
                            reader.capture,
                            view.root_path,
                            authorize=self._bridge(fence.operation_id, check, abort, deadline),
                            cancelled=abort.is_set,
                        )
                    ),
                    abort.set,
                )
                await check()
                view = await self.store.record_absence(
                    scope, fence, binding, snapshot, observation_reader=reader, qualify=qualify
                )
            elif kind == "execute":
                target, before, token = await self.store.inputs(
                    scope, fence, binding, qualify=qualify
                )
                original_reader = self._readers.get(fence.plan_id)
                if original_reader is None:
                    raise _error("observation_lost", "原缺失观察已丢失，须新预览")
                writer = WorkspaceMissingDirectoryRestoreWriter(binding.workspace, original_reader)
                self._writers[fence.operation_id] = fence.plan_id, writer
                deadline = time.monotonic() + 5
                change = await _wait(
                    asyncio.create_task(
                        workspace_io(
                            writer.prepare,
                            target,
                            before,
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
                retained = self._writers.get(fence.operation_id)
                temporary = None
                if retained is not None:
                    if retained[0] != fence.plan_id:
                        raise _error("context", "原Writer与缺失计划不匹配", 403)
                    actual_writer = retained[1]
                else:
                    # 冷历史只建无capture的临时对象；没有原Writer证明继续unknown。
                    temporary = WorkspaceDirectoryAbsenceReader(binding.workspace)
                    actual_writer = WorkspaceMissingDirectoryRestoreWriter(
                        binding.workspace, temporary
                    )
                try:
                    effect = await _wait(
                        asyncio.create_task(workspace_io(actual_writer.reconcile, change)),
                        abort.set,
                    )
                finally:
                    if temporary is not None:
                        temporary.close()
            else:
                raise _error("context", "目录恢复阶段无效", 403)
            await check()
            if view.baseline is None:
                raise _error("unverified", "目录恢复缺少原完整备份")
            result = MissingDirectoryRestoreStepView(
                plan_id=view.plan_id,
                operation_id=fence.operation_id,
                kind=fence.kind,
                root_path=view.root_path,
                target_fingerprint=view.target_snapshot.snapshot_fingerprint,
                before_absence_fingerprint=view.baseline.fingerprint,
                receipt=MissingDirectoryRestoreReceiptView.model_validate(effect.receipt.__dict__)
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
            result = MissingDirectoryRestoreStepView.model_validate(value)
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
        fence: MissingDirectoryRestoreFence,
        worker: asyncio.Task[None],
        invocation: MissingDirectoryRestoreInvocation | None = None,
    ) -> None:
        try:
            while True:
                await asyncio.sleep(max(0.02, min(5, self.store.lease_seconds / 4)))
                authority = invocation.authorize if invocation is not None else self.authorize
                await authority(agent_id, fence.kind)
                await self.store.renew(
                    scope, fence, binding, qualify=lambda: self._job_qualify(invocation)
                )
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
        fence: MissingDirectoryRestoreFence,
        abort: Event,
        confirmed: bool,
        invocation: MissingDirectoryRestoreInvocation | None = None,
    ) -> None:
        worker = asyncio.current_task()
        assert worker is not None
        marker = enter_workspace_execution(self._tasks, fence.operation_id)
        heartbeat = asyncio.create_task(
            self._renew(agent_id, scope, binding, fence, worker, invocation)
        )

        async def fail(code: str) -> None:
            heartbeat.cancel()
            abort.set()

            async def drain_and_record() -> None:
                callbacks = tuple(self._callbacks.get(fence.operation_id, ()))
                for callback in callbacks:
                    if not callback.done() and not callback.cancelling():
                        callback.cancel()
                await asyncio.gather(heartbeat, *callbacks, return_exceptions=True)
                try:
                    await self.store.fail(scope, fence, code)
                except ServiceError:
                    # 已取消/到期/失去owner只能保持原事实，不换token重新生产。
                    pass

            await _wait(asyncio.create_task(drain_and_record()), lambda: None)

        try:
            call = invocation.call if invocation is not None else self.call
            await call(agent_id, fence, confirmed)
            view = await self.store.get(scope, fence.plan_id)
            if view.phase in {"preparing", "executing"}:
                raise _error("unverified", "目录叶没有完成原后置资格")
        except asyncio.CancelledError:
            await fail("workspace_missing_directory_restore_cancelled")
        except ServiceError as error:
            await fail(error.code)
        except Exception:
            await fail("workspace_missing_directory_restore_failed")
        finally:
            abort.set()
            heartbeat.cancel()

            async def drain() -> None:
                await asyncio.gather(heartbeat, return_exceptions=True)
                await asyncio.gather(
                    *tuple(self._callbacks.get(fence.operation_id, ())), return_exceptions=True
                )
                await self._release_finished(scope, fence.plan_id)

            try:
                await _wait(asyncio.create_task(drain()), lambda: None)
            finally:
                self._callbacks.pop(fence.operation_id, None)
                self._pending.pop(fence.operation_id, None)
                leave_workspace_execution(marker)

    async def wait(
        self, scope: str, identifier: str, *, timeout_seconds: float = 30
    ) -> MissingDirectoryRestoreView:
        if type(timeout_seconds) not in {int, float} or not 0 <= timeout_seconds <= 300:
            raise _error("timeout", "等待须在0–300秒之间", 422)
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while True:
            view = await self.store.get(scope, identifier)
            remaining = deadline - asyncio.get_running_loop().time()
            if view.phase not in {"preparing", "executing"} or remaining <= 0:
                return view
            await asyncio.sleep(min(0.05, remaining))

    async def cancel(self, scope: str, identifier: str) -> MissingDirectoryRestoreView:
        internal = inside_workspace_execution()

        async def stop() -> MissingDirectoryRestoreView:
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
            if entry is None or entry[1].done():
                await self._release_finished(scope, identifier)
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
                    from krim_agent.persistence.database import (
                        WorkspaceMissingDirectoryRestoreAttemptRow,
                    )

                    attempt = await sql.get(WorkspaceMissingDirectoryRestoreAttemptRow, identifier)
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
            for identifier, (scope, _expires_at) in tuple(self._observation_slots.items()):
                try:
                    await self.store.cancel(scope, identifier)
                except Exception:
                    pass
                finally:
                    self._release_plan(identifier)
            # 冷verify不分配观察slot，仍须释放可能残留的私有内存引用。
            for identifier in tuple(self._readers):
                self._release_plan(identifier)
            self._writers.clear()

        await _wait(asyncio.create_task(drain()), lambda: None)
