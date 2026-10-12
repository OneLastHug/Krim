"""账本先行的文件执行编排；只从可信ToolContext取得权限与工作区。"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from threading import Event
from typing import Any, Literal

from pydantic import JsonValue

from krim_agent.persistence.agent_sessions import assert_session_open, guard_agent_topic
from krim_agent.persistence.database import RunRow
from krim_agent.persistence.workspace_edits import StoredWorkspaceAttempt, WorkspaceEditStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.tools.core import ToolContext, ToolError
from krim_agent.tools.workspace import workspace_io
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_change import (
    FileChangeReceipt,
    WorkspaceAttemptView,
    WorkspaceChangeAction,
    WorkspaceChangeView,
    WorkspaceOperationRequest,
    WorkspacePreviewRequest,
)
from krim_agent.workspace_writer import FileEffect, PreparedFileChange, WorkspaceWriter


async def _wait[T](task: asyncio.Task[T], on_cancel: Callable[[], None]) -> T:
    """取消只发信号；反复取消也须等待文件动作与持久回执收束。"""
    cancelled = False
    while True:
        try:
            result = await asyncio.shield(task)
            break
        except asyncio.CancelledError:
            if task.cancelled():
                raise
            cancelled = True
            on_cancel()
        except Exception:
            if cancelled:
                raise asyncio.CancelledError from None
            raise
    if cancelled:
        raise asyncio.CancelledError
    return result


class WorkspaceChangeService:
    def __init__(self, store: WorkspaceEditStore, *, max_operations: int = 64) -> None:
        if not 1 <= max_operations <= 64:
            raise ValueError("工作区执行并发须在1–64之间")
        self.store = store
        self.max_operations = max_operations
        self._operations: dict[asyncio.Task[WorkspaceAttemptView], Event] = {}
        self._active_operation_ids: set[str] = set()
        self._closed = False

    async def _guard(
        self, context: ToolContext, *, writing: bool = True
    ) -> tuple[str, WorkspaceBinding]:
        if self._closed:
            raise ServiceError("workspace_service_closed", "工作区修改服务已关闭", 503)
        binding = context.workspace
        if binding is None:
            raise ServiceError("workspace_unbound", "当前会话没有工作区授权", 409)
        if writing and not binding.require().writable:
            raise ServiceError("workspace_read_only", "宿主未授权工作区写能力", 403)
        database = self.store.database
        async with database.write_lock, database.sessions() as session:
            row = await guard_agent_topic(
                session, context.topic_id, context.session_id, context.agent_id, self.store.owner_id
            )
            if row is not None:
                await assert_session_open(session, row)
                if row.permissions.get("workspace") != binding.manifest():
                    raise ServiceError("workspace_binding_changed", "工作区授权版本已变化", 403)
            if context.run_id != "local":
                run = await session.get(RunRow, context.run_id)
                if run is None or run.topic_id != context.topic_id:
                    raise ServiceError("workspace_run_mismatch", "修改Run不属于当前话题", 403)
                if run.status not in {"queued", "running"} or run.stop_requested:
                    raise ServiceError("workspace_run_closed", "修改所属Run已关闭", 409)
            elif context.origin != "host":
                raise ServiceError("workspace_run_mismatch", "模型修改需要原运行上下文", 403)
        binding.require()
        scope = (
            f"session:{context.session_id}" if context.session_id else f"topic:{context.topic_id}"
        )
        return scope, binding

    async def tool_operation(
        self, operation: str, payload: dict[str, Any], context: ToolContext
    ) -> dict[str, JsonValue]:
        try:
            if context.mixed_batch is not None and (
                context.mixed_batch.source != "file" or operation != "change"
            ):
                raise ServiceError(
                    "workspace_mixed_batch_action", "混合子项只能执行原来源的冻结动作", 403
                )
            if context.checkpoint_capture is not None:
                raise ServiceError(
                    "workspace_checkpoint_action", "检查点捕获上下文不能执行文件修改动作", 403
                )
            if context.workspace_batch is not None and operation != "change":
                raise ServiceError("workspace_batch_action", "批次子项只能执行原冻结修改动作", 403)
            if context.checkpoint_restore is not None and operation != "change":
                raise ServiceError(
                    "workspace_restore_action", "恢复子项只能执行原冻结修改动作", 403
                )
            if operation == "preview":
                value = await self._preview(
                    WorkspacePreviewRequest.model_validate(payload), context
                )
                return value.model_dump(mode="json")
            if self._closed:
                raise ServiceError("workspace_service_closed", "工作区修改服务已关闭", 503)
            if len(self._operations) >= self.max_operations:
                raise ServiceError("workspace_busy", "工作区修改服务不可用或任务已达上限", 429)
            abort = Event()
            if operation == "change":
                job = self._change(WorkspaceChangeAction.model_validate(payload), context, abort)
            elif operation == "reconcile":
                request = WorkspaceOperationRequest.model_validate(payload)
                job = self._reconcile(request.operation_id, context, abort)
            elif operation == "finalize":
                request = WorkspaceOperationRequest.model_validate(payload)
                job = self._finalize(request.operation_id, context, abort)
            else:
                raise ServiceError("workspace_operation_invalid", "工作区操作无效", 422)
            task = asyncio.create_task(job)
            self._operations[task] = abort

            def finished(completed: asyncio.Task[WorkspaceAttemptView]) -> None:
                self._operations.pop(completed, None)
                if not completed.cancelled():
                    completed.exception()

            task.add_done_callback(finished)
            return (await _wait(task, abort.set)).model_dump(mode="json")
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error

    async def _preview(
        self, request: WorkspacePreviewRequest, context: ToolContext
    ) -> WorkspaceChangeView:
        scope, binding = await self._guard(context)
        previous = await self.store.find_preview(
            scope, request.request_key, request.spec, binding.fingerprint
        )
        if previous is not None:
            return previous.view
        prepared = await workspace_io(WorkspaceWriter(binding.workspace).prepare, request.spec)
        await self._guard(context)
        stored, _ = await self.store.preview(
            scope,
            binding.key,
            binding.fingerprint,
            request.request_key,
            prepared,
            ttl_seconds=request.ttl_seconds,
            run_id=context.run_id if context.run_id != "local" else None,
        )
        return stored.view

    async def _renew(self, context: ToolContext, operation_id: str, abort: Event) -> None:
        try:
            while True:
                await asyncio.sleep(max(0.02, min(0.2, self.store.lease_seconds / 4)))
                await self.store.renew(operation_id)
                await self._guard(context)
        except asyncio.CancelledError:
            raise
        except Exception:
            abort.set()

    async def _authorize_publication(
        self, context: ToolContext, scope: str, operation_id: str
    ) -> None:
        await self._guard(context)
        await self.store.assert_execution(scope, operation_id)

    @staticmethod
    def _effect(
        prepared: PreparedFileChange,
        status: Literal["done", "rejected", "uncertain"],
        code: str | None = None,
    ) -> FileEffect:
        return FileEffect(
            FileChangeReceipt(
                path=prepared.spec.path,
                status=status,
                before_sha256=prepared.before_sha256,
                after_sha256=prepared.after_sha256,
                changed=status == "uncertain",
                error_code=code,
                recovery_required=status == "uncertain",
            )
        )

    async def _save(
        self, scope: str, attempt: StoredWorkspaceAttempt, effect: FileEffect
    ) -> WorkspaceAttemptView:
        try:
            return (
                await self.store.settle(
                    scope,
                    attempt.view.operation_id,
                    effect.receipt,
                    effect.recovery,
                    observed_after=effect.observed_after,
                )
            ).view
        except Exception:
            # COMMIT可能成功后才向调用方报错；先读取实际账本，不重放文件动作。
            try:
                stored = await self.store.operation(scope, attempt.view.operation_id)
                if stored.view.status in {"done", "rejected", "uncertain"}:
                    return stored.view
                unknown = self._effect(attempt.prepared, "uncertain", "workspace_receipt_pending")
                if attempt.view.action == "keep":
                    unknown = self._keep_effect(attempt.prepared, unknown)
                return (
                    await self.store.settle(
                        scope,
                        attempt.view.operation_id,
                        unknown.receipt,
                        effect.recovery or attempt.recovery,
                    )
                ).view
            except Exception as error:
                raise ServiceError(
                    "workspace_receipt_pending",
                    f"文件结果待核实，保留操作 {attempt.view.operation_id} 与恢复材料",
                    503,
                ) from error

    @staticmethod
    def _keep_effect(original: PreparedFileChange, verified: FileEffect) -> FileEffect:
        # 保留不写文件；证明指向链尾，旧回执仍描述被接受的原提案。
        return FileEffect(
            verified.receipt.model_copy(
                update={
                    "path": original.spec.path,
                    "before_sha256": original.before_sha256,
                    "after_sha256": original.after_sha256,
                    "changed": False,
                }
            )
        )

    async def _change(
        self, request: WorkspaceChangeAction, context: ToolContext, abort: Event
    ) -> WorkspaceAttemptView:
        scope, binding = await self._guard(context)
        previous = await self.store.find_action(
            scope,
            request,
            binding.fingerprint,
            batch_fence=context.workspace_batch,
            restore_fence=context.checkpoint_restore,
            mixed_fence=context.mixed_batch,
            tree_fence=context.tree_fence,
            run_id=context.run_id if context.run_id != "local" else None,
        )
        if previous is not None:
            return previous.view
        original = await self.store.get(scope, request.proposal_id)
        writer = WorkspaceWriter(binding.workspace)
        prepared = original.prepared
        if request.action == "undo":
            prepared = await workspace_io(writer.prepare_restore, prepared)
        await self._guard(context)
        attempt, created = await self.store.reserve(
            scope,
            request,
            binding.fingerprint,
            prepared,
            run_id=context.run_id if context.run_id != "local" else None,
            call_id=context.source_call_id or context.call_id,
            batch_fence=context.workspace_batch,
            restore_fence=context.checkpoint_restore,
            mixed_fence=context.mixed_batch,
            tree_fence=context.tree_fence,
        )
        if not created:
            return attempt.view
        self._active_operation_ids.add(attempt.view.operation_id)
        renew = asyncio.create_task(self._renew(context, attempt.view.operation_id, abort))
        loop = asyncio.get_running_loop()

        def authorize() -> None:
            # 只由Writer线程在文件锁内调用；原事件循环负责检查SQL和会话事实。
            if loop.is_closed():
                raise ServiceError("workspace_publication_unavailable", "发布资格无法核实", 503)
            check = self._authorize_publication(context, scope, attempt.view.operation_id)
            try:
                future = asyncio.run_coroutine_threadsafe(check, loop)
            except RuntimeError:
                check.close()
                raise ServiceError(
                    "workspace_publication_unavailable", "发布资格无法核实", 503
                ) from None
            try:
                future.result(timeout=writer.lock_timeout)
            except ServiceError:
                raise
            except Exception:
                future.cancel()
                raise ServiceError(
                    "workspace_publication_unavailable", "发布资格无法核实", 503
                ) from None

        try:
            if abort.is_set():
                effect = self._effect(prepared, "rejected", "workspace_write_cancelled")
            elif request.action == "keep":
                try:
                    await self._guard(context)
                    target, expected = await self.store.keep_target(
                        scope, attempt.view.operation_id
                    )
                    verified = await workspace_io(
                        writer.verify,
                        target,
                        None,
                        keeping=True,
                        expected_after=expected,
                        authorize=authorize,
                    )
                    await self._guard(context)
                    effect = self._keep_effect(prepared, verified)
                except Exception:
                    effect = self._keep_effect(
                        prepared,
                        self._effect(prepared, "uncertain", "workspace_reconcile_inconclusive"),
                    )
            else:
                try:
                    await self._guard(context)
                    effect = await workspace_io(
                        writer.apply,
                        prepared,
                        abort=lambda: abort.is_set() or not binding.available or self._closed,
                        authorize=authorize,
                    )
                except ServiceError as error:
                    if error.code in {"workspace_write_cancelled", "workspace_parent_changed"}:
                        # 这两种未捕获异常只来自发布前；发布后的错误由Writer返回真实回执。
                        effect = self._effect(prepared, "rejected", error.code)
                    else:
                        effect = self._effect(
                            prepared, "uncertain", "workspace_execution_uncertain"
                        )
                        effect = FileEffect(effect.receipt, attempt.recovery)
                except Exception:
                    # 不把未拿到回执等同于文件没变；仅保存未知效果并保留逻辑锁。
                    effect = self._effect(prepared, "uncertain", "workspace_execution_uncertain")
                    effect = FileEffect(effect.receipt, attempt.recovery)
            # 保存真实效果后才能返回/传播取消，清理由单独显式动作进行。
            return await self._save(scope, attempt, effect)
        finally:
            self._active_operation_ids.discard(attempt.view.operation_id)
            renew.cancel()
            await asyncio.gather(renew, return_exceptions=True)

    async def _attempt(
        self, scope: str, operation_id: str, binding: WorkspaceBinding
    ) -> StoredWorkspaceAttempt:
        attempt = await self.store.operation(scope, operation_id)
        proposal = await self.store.get(scope, attempt.view.proposal_id)
        if (
            proposal.view.workspace_key != binding.key
            or proposal.view.resource_fingerprint != binding.fingerprint
            or attempt.prepared.workspace_fingerprint != binding.workspace.fingerprint
        ):
            raise ServiceError("workspace_binding_changed", "操作不属于当前工作区版本", 403)
        return attempt

    async def _reconcile(
        self, operation_id: str, context: ToolContext, abort: Event
    ) -> WorkspaceAttemptView:
        scope, binding = await self._guard(context)
        if operation_id in self._active_operation_ids:
            raise ServiceError("workspace_operation_active", "活跃文件操作不可核实或接管", 409)
        await self.store.recover_interrupted()
        attempt = await self._attempt(scope, operation_id, binding)
        if attempt.view.status == "running":
            raise ServiceError("workspace_operation_active", "活跃文件操作不可核实或接管", 409)
        if attempt.view.status != "uncertain":
            return attempt.view
        if abort.is_set():
            return attempt.view
        writer = WorkspaceWriter(binding.workspace)
        target, expected = attempt.prepared, None
        if attempt.view.action == "keep":
            target, expected = await self.store.keep_target(scope, operation_id)
        effect = await workspace_io(
            writer.verify,
            target,
            attempt.recovery,
            keeping=attempt.view.action == "keep",
            expected_after=expected,
        )
        if attempt.view.action == "keep":
            effect = self._keep_effect(attempt.prepared, effect)
        await self._guard(context)
        if effect.receipt.status == "uncertain":
            return attempt.view
        return (
            await self.store.settle(
                scope,
                operation_id,
                effect.receipt,
                effect.recovery,
                resolve=True,
                observed_after=effect.observed_after,
            )
        ).view

    async def _finalize(
        self, operation_id: str, context: ToolContext, abort: Event
    ) -> WorkspaceAttemptView:
        scope, binding = await self._guard(context)
        attempt = await self._attempt(scope, operation_id, binding)
        if attempt.view.status not in {"done", "rejected"}:
            raise ServiceError("workspace_cleanup_state", "未核实文件操作不可清理", 409)
        if not attempt.view.cleanup_pending or attempt.recovery is None or abort.is_set():
            return attempt.view
        await workspace_io(
            WorkspaceWriter(binding.workspace).finalize,
            attempt.prepared,
            attempt.recovery,
            rejected=attempt.view.status == "rejected",
        )
        await self._guard(context)
        return (await self.store.mark_cleaned(scope, operation_id)).view

    async def close(self) -> None:
        self._closed = True
        pending = list(self._operations)
        for abort in self._operations.values():
            abort.set()
        if pending:

            async def drain() -> None:
                await asyncio.gather(*pending, return_exceptions=True)

            await _wait(asyncio.create_task(drain()), lambda: None)
