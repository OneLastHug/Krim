"""检查点条件恢复：先捕获备份，再沿原工具链执行冻结文件提案。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass

from krim_agent.persistence.workspace_restores import WorkspaceRestoreStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_change import WorkspaceChangeAction
from krim_agent.workspace_checkpoint import (
    CheckpointModelSource,
    WorkspaceCheckpointCreate,
    WorkspaceCheckpointView,
)
from krim_agent.workspace_restore import (
    CheckpointRestoreDispatch,
    CheckpointRestoreExecute,
    CheckpointRestoreFence,
    CheckpointRestorePreparation,
    CheckpointRestorePreview,
    CheckpointRestoreView,
)
from krim_agent.workspace_writer import WorkspaceWriter

from .workspace_changes import _wait
from .workspace_checkpoints import CaptureGuard, CheckpointInvocation, WorkspaceCheckpointService
from .workspace_execution import (
    enter_workspace_execution,
    inside_workspace_execution,
    leave_workspace_execution,
)

ExecuteRestore = Callable[
    [str, WorkspaceChangeAction, CheckpointRestoreFence, bool], Awaitable[object]
]


@dataclass(frozen=True)
class RestoreInvocation:
    source: CheckpointModelSource
    authorize: CaptureGuard
    baseline: Callable[[str], CheckpointInvocation]
    execute: ExecuteRestore
    qualify: Callable[[], None]


class WorkspaceRestoreService:
    def __init__(
        self,
        store: WorkspaceRestoreStore,
        checkpoints: WorkspaceCheckpointService,
        authorize: CaptureGuard,
        execute: ExecuteRestore,
        *,
        max_active: int = 16,
    ) -> None:
        if not 1 <= max_active <= 64:
            raise ValueError("恢复计划并发上限无效")
        self.store, self.checkpoints = store, checkpoints
        self.authorize, self.execute = authorize, execute
        self.max_active = max_active
        self._tasks: dict[str, tuple[str, asyncio.Task[None]]] = {}
        self._start_lock = asyncio.Lock()
        self._closed = False

    def _spawn(self, scope: str, key: str, job: Coroutine[None, None, None]) -> None:
        task = asyncio.create_task(job)
        self._tasks[key] = (scope, task)

        def done(result: asyncio.Task[None]) -> None:
            self._tasks.pop(key, None)
            if not result.cancelled():
                result.exception()

        task.add_done_callback(done)

    async def preview(
        self, agent_id: str, request: CheckpointRestorePreview, *, confirmed: bool = False
    ) -> CheckpointRestoreView:
        request = CheckpointRestorePreview.model_validate(request.model_dump())
        return await _wait(
            asyncio.create_task(self._preview(agent_id, request, confirmed)), lambda: None
        )

    async def _preview(
        self,
        agent_id: str,
        request: CheckpointRestorePreview,
        confirmed: bool,
        invocation: RestoreInvocation | None = None,
    ) -> CheckpointRestoreView:
        authority = invocation.authorize if invocation is not None else self.authorize
        scope, binding = await authority(agent_id)
        target = await self.checkpoints.store.get(scope, request.checkpoint_id)
        if request.baseline_capture_format == "bytes" or target.capture_format == "bytes":
            if self.checkpoints.require_bytes is None:
                raise ServiceError(
                    "workspace_checkpoint_bytes_unavailable", "字节基线未显式启用", 403
                )
            await self.checkpoints.require_bytes(agent_id)
        async with self._start_lock:
            if self._closed:
                raise ServiceError("workspace_restore_closed", "恢复服务已关闭", 503)
            if (
                len(self._tasks) >= self.max_active
                and await self.store.by_key(scope, request.request_key) is None
            ):
                raise ServiceError("workspace_restore_busy", "恢复计划并发已达上限", 429)
            if invocation is None:
                view, created = await self.store.reserve(
                    scope, request, binding.key, binding.fingerprint, binding.workspace.fingerprint
                )
            else:
                view, created = await self.store.reserve(
                    scope,
                    request,
                    binding.key,
                    binding.fingerprint,
                    binding.workspace.fingerprint,
                    model_source=invocation.source,
                    authorize=invocation.qualify,
                )
            if created:
                preparation = await self.store.preparation(scope, view.plan_id)
                self._spawn(
                    scope,
                    "prepare:" + view.plan_id,
                    self._prepare(agent_id, scope, preparation, binding, confirmed, invocation),
                )
            return view

    async def start(
        self, agent_id: str, request: CheckpointRestoreExecute, *, confirmed: bool = False
    ) -> CheckpointRestoreView:
        request = CheckpointRestoreExecute.model_validate(request.model_dump())
        return await _wait(
            asyncio.create_task(self._start(agent_id, request, confirmed)), lambda: None
        )

    async def _start(
        self,
        agent_id: str,
        request: CheckpointRestoreExecute,
        confirmed: bool,
        invocation: RestoreInvocation | None = None,
    ) -> CheckpointRestoreView:
        authority = invocation.authorize if invocation is not None else self.authorize
        scope, binding = await authority(agent_id)
        async with self._start_lock:
            if self._closed:
                raise ServiceError("workspace_restore_closed", "恢复服务已关闭", 503)
            if len(self._tasks) >= self.max_active:
                previous = await self.store.get(scope, request.plan_id)
                if previous.execute_key is None:
                    raise ServiceError("workspace_restore_busy", "恢复计划并发已达上限", 429)
            if invocation is None:
                view, created = await self.store.start(
                    scope, request, binding.key, binding.fingerprint, binding.workspace.fingerprint
                )
            else:
                view, created = await self.store.start(
                    scope,
                    request,
                    binding.key,
                    binding.fingerprint,
                    binding.workspace.fingerprint,
                    model_source=invocation.source,
                    authorize=invocation.qualify,
                )
            if created:
                dispatch = await self.store.execution(scope, view.plan_id)
                self._spawn(
                    scope,
                    "execute:" + view.plan_id,
                    self._run(agent_id, scope, dispatch, confirmed, invocation),
                )
            return view

    async def preview_model(
        self, agent_id: str, request: CheckpointRestorePreview, invocation: RestoreInvocation
    ) -> CheckpointRestoreView:
        request = CheckpointRestorePreview.model_validate(request.model_dump())
        return await _wait(
            asyncio.create_task(self._preview(agent_id, request, False, invocation)), lambda: None
        )

    async def start_model(
        self, agent_id: str, request: CheckpointRestoreExecute, invocation: RestoreInvocation
    ) -> CheckpointRestoreView:
        request = CheckpointRestoreExecute.model_validate(request.model_dump())
        return await _wait(
            asyncio.create_task(self._start(agent_id, request, False, invocation)), lambda: None
        )

    async def _renew(
        self,
        scope: str,
        identifier: str,
        token: str,
        phase: str,
        worker: asyncio.Task[None],
        invocation: RestoreInvocation | None = None,
        agent_id: str | None = None,
    ) -> None:
        try:
            while True:
                await asyncio.sleep(max(0.02, min(0.2, self.store.lease_seconds / 4)))
                if invocation is not None:
                    assert agent_id is not None
                    await invocation.authorize(agent_id)
                await self.store.renew(scope, identifier, token, phase)
        except asyncio.CancelledError:
            raise
        except Exception:
            if not worker.done() and not worker.cancelling():
                worker.cancel()

    async def _prepare(
        self,
        agent_id: str,
        scope: str,
        preparation: CheckpointRestorePreparation,
        binding: WorkspaceBinding,
        confirmed: bool,
        invocation: RestoreInvocation | None = None,
    ) -> None:
        identifier, token = preparation.view.plan_id, preparation.token
        worker = asyncio.current_task()
        assert worker is not None
        marker = enter_workspace_execution(self._tasks, "prepare:" + identifier)
        heartbeat = asyncio.create_task(
            self._renew(scope, identifier, token, "preparing", worker, invocation, agent_id)
        )
        ready = False
        code = None

        async def capture_backup() -> WorkspaceCheckpointView:
            request = WorkspaceCheckpointCreate(
                title="恢复前备份",
                paths=[item.path for item in preparation.view.items],
                request_key=preparation.baseline_key,
                capture_format=preparation.view.baseline_capture_format,
            )
            if invocation is None:
                backup = await self.checkpoints.create(agent_id, request, confirmed=confirmed)
            else:
                backup = await self.checkpoints.create_model(
                    agent_id, request, invocation.baseline(identifier)
                )
            try:
                if invocation is None:
                    await self.store.attach_baseline(scope, identifier, token, backup.checkpoint_id)
                else:
                    await self.store.attach_baseline(
                        scope,
                        identifier,
                        token,
                        backup.checkpoint_id,
                        model_source=invocation.source,
                    )
            except BaseException:
                await self.checkpoints.cancel(scope, backup.checkpoint_id)
                raise
            return backup

        try:
            # 接受捕获与关联必须先收束，避免取消后留下无法定位的后台备份。
            backup = await _wait(asyncio.create_task(capture_backup()), lambda: None)
            while backup.status == "capturing":
                backup = await self.checkpoints.wait(scope, backup.checkpoint_id, timeout_seconds=1)
            if backup.status != "available":
                raise ServiceError(
                    backup.error_code or "workspace_restore_baseline", "恢复前备份未完成", 409
                )
            authority = invocation.authorize if invocation is not None else self.authorize
            actual_scope, actual = await authority(agent_id)
            if actual_scope != scope or actual.manifest() != binding.manifest():
                raise ServiceError("workspace_binding_changed", "恢复计划资源已变化", 403)
            current = await self.checkpoints.store.captures(scope, backup.checkpoint_id)
            target = await self.checkpoints.store.captures(scope, preparation.view.checkpoint_id)
            writer = WorkspaceWriter(binding.workspace)
            prepared = tuple(
                writer.prepare_checkpoint_restore(old, desired)
                for old, desired in zip(current, target, strict=True)
            )
            if invocation is None:
                await self.store.ready(scope, identifier, token, prepared)
            else:
                await self.store.ready(
                    scope,
                    identifier,
                    token,
                    prepared,
                    model_source=invocation.source,
                    authorize=invocation.qualify,
                )
            ready = True
        except asyncio.CancelledError:
            code = "workspace_restore_cancelled"
        except ServiceError as error:
            code = error.code
        except Exception:
            code = "workspace_restore_prepare_failed"
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)

            async def settle() -> None:
                if not ready:
                    view = await self.store.get(scope, identifier)
                    if view.baseline_checkpoint_id is not None:
                        await self.checkpoints.cancel(scope, view.baseline_checkpoint_id)
                    await self.store.finish(scope, identifier, token, code)

            try:
                await _wait(asyncio.create_task(settle()), lambda: None)
            finally:
                leave_workspace_execution(marker)

    async def _run(
        self,
        agent_id: str,
        scope: str,
        dispatch: tuple[CheckpointRestoreDispatch, ...],
        confirmed: bool,
        invocation: RestoreInvocation | None = None,
    ) -> None:
        first = dispatch[0].fence
        worker = asyncio.current_task()
        assert worker is not None
        marker = enter_workspace_execution(self._tasks, "execute:" + first.plan_id)
        heartbeat = asyncio.create_task(
            self._renew(
                scope, first.plan_id, first.token, "executing", worker, invocation, agent_id
            )
        )
        failure = None
        try:
            for child in dispatch:
                code = None
                cancelled = False
                try:
                    # Hook返回值仅供工具链使用；真实结果始终读取原attempt关联。
                    if invocation is None:
                        await self.execute(agent_id, child.request, child.fence, confirmed)
                    else:
                        await invocation.authorize(agent_id)
                        await invocation.execute(agent_id, child.request, child.fence, False)
                except asyncio.CancelledError:
                    cancelled, code = True, "workspace_restore_cancelled"
                except ServiceError as error:
                    code = error.code
                except Exception:
                    code = "workspace_restore_execution_failed"
                await _wait(
                    asyncio.create_task(self.store.finish_child(scope, child.fence, code)),
                    lambda: None,
                )
                if cancelled:
                    failure = code
                    break
                if (await self.store.get(scope, first.plan_id)).phase != "executing":
                    break
        except asyncio.CancelledError:
            failure = "workspace_restore_cancelled"
        except ServiceError as error:
            failure = error.code
        except Exception:
            failure = "workspace_restore_execution_failed"
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            try:
                await _wait(
                    asyncio.create_task(
                        self.store.finish(scope, first.plan_id, first.token, failure)
                    ),
                    lambda: None,
                )
            finally:
                leave_workspace_execution(marker)

    async def cancel(self, scope: str, identifier: str) -> CheckpointRestoreView:
        internal = inside_workspace_execution()

        async def stop() -> CheckpointRestoreView:
            view = await self.store.cancel(scope, identifier)
            if view.baseline_checkpoint_id is not None and view.phase != "ready":
                await self.checkpoints.cancel(scope, view.baseline_checkpoint_id)
            pending = []
            for phase in ("prepare:", "execute:"):
                entry = self._tasks.get(phase + identifier)
                if entry is not None and entry[0] == scope:
                    task = entry[1]
                    if not task.done() and not task.cancelling():
                        task.cancel()
                    pending.append(task)
            if not internal:
                await asyncio.gather(*pending, return_exceptions=True)
            return await self.store.get(scope, identifier)

        return await _wait(asyncio.create_task(stop()), lambda: None)

    async def wait(
        self, scope: str, identifier: str, *, timeout_seconds: float = 30
    ) -> CheckpointRestoreView:
        if not 0 <= timeout_seconds <= 300:
            raise ServiceError("workspace_restore_timeout", "恢复等待时间须在0–300秒之间", 422)
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while True:
            value = await self.store.get(scope, identifier)
            remaining = deadline - asyncio.get_running_loop().time()
            if value.phase not in {"preparing", "executing"} or remaining <= 0:
                return value
            await asyncio.sleep(min(0.05, remaining))

    async def close(self) -> None:
        if inside_workspace_execution():
            raise ServiceError(
                "workspace_restore_close_reentrant", "文件回调须取消计划，不能关闭服务", 409
            )
        async with self._start_lock:
            self._closed = True
            entries = list(self._tasks.items())
        for key, (scope, task) in entries:
            try:
                await self.store.cancel(scope, key.split(":", 1)[1], include_ready=False)
            except Exception:
                pass
            finally:
                if not task.done() and not task.cancelling():
                    task.cancel()

        async def drain() -> None:
            await asyncio.gather(*(entry[1][1] for entry in entries), return_exceptions=True)

        await _wait(asyncio.create_task(drain()), lambda: None)
