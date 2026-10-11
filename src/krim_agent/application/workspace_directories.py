"""目录事务Worker：同Registry上下文、发布前持久意图与原回执收束。"""

from __future__ import annotations

import asyncio
import json
from contextvars import ContextVar
from dataclasses import replace
from threading import Event
from typing import Any

from pydantic import JsonValue

from krim_agent.persistence.workspace_directories import WorkspaceDirectoryStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.tools.core import ToolContext, ToolError
from krim_agent.tools.workspace import workspace_io
from krim_agent.workspace_directory import (
    DirectoryAttemptView,
    DirectoryChangeAction,
    DirectoryChangeReceipt,
    DirectoryEffect,
    DirectoryOperationRequest,
    DirectoryPreviewRequest,
    DirectoryRecovery,
    PreparedDirectoryChange,
    WorkspaceDirectoryWriter,
)

from .workspace_changes import WorkspaceChangeService, _wait

_current: ContextVar[asyncio.Task[Any] | None] = ContextVar(
    "krim_directory_operation", default=None
)


class DirectoryChangeService:
    def __init__(self, changes: WorkspaceChangeService) -> None:
        self.changes = changes
        self.store = WorkspaceDirectoryStore(
            changes.store.database, owner_id=changes.store.owner_id
        )
        self._operations: dict[asyncio.Task[DirectoryAttemptView], Event] = {}
        self._active: set[str] = set()
        self._closed = False

    def assert_external_close(self) -> None:
        task = _current.get()
        if task is not None and not task.done():
            raise ServiceError(
                "workspace_directory_self_close", "目录回调不能等待或关闭自身服务", 409
            )

    async def tool_operation(
        self, operation: str, payload: dict[str, Any], context: ToolContext
    ) -> dict[str, JsonValue]:
        marker = _current.set(asyncio.current_task())
        try:
            return await self._tool_operation(operation, payload, context)
        finally:
            _current.reset(marker)

    async def _tool_operation(
        self, operation: str, payload: dict[str, Any], context: ToolContext
    ) -> dict[str, JsonValue]:
        if self._closed:
            raise ToolError("workspace_directory_closed", "目录事务服务已关闭")
        try:
            if operation == "preview":
                request = DirectoryPreviewRequest.model_validate(payload)
                scope, binding = await self.changes._guard(context)
                original = await self.store.find_preview(
                    scope,
                    binding.key,
                    binding.fingerprint,
                    request.request_key,
                    request.spec,
                    ttl_seconds=request.ttl_seconds,
                )
                if original is None:
                    prepared = await workspace_io(
                        WorkspaceDirectoryWriter(binding.require()).prepare, request.spec
                    )
                    await self.changes._guard(context)
                    original, _ = await self.store.preview(
                        scope,
                        binding.key,
                        binding.fingerprint,
                        request.request_key,
                        prepared,
                        ttl_seconds=request.ttl_seconds,
                        run_id=context.run_id if context.run_id != "local" else None,
                    )
                preview_value = original.view.model_dump(mode="json")
                context._directory_response = (operation, json.dumps(preview_value, sort_keys=True))
                return preview_value
            if len(self._operations) >= 64:
                raise ServiceError("workspace_directory_busy", "目录操作并发已达上限", 429)
            abort = Event()
            if operation == "change":
                job = self._change(DirectoryChangeAction.model_validate(payload), context, abort)
            elif operation == "reconcile":
                job = self._reconcile(
                    DirectoryOperationRequest.model_validate(payload).operation_id, context
                )
            elif operation == "finalize":
                job = self._finalize(
                    DirectoryOperationRequest.model_validate(payload).operation_id, context
                )
            else:
                raise ServiceError("workspace_directory_operation", "目录操作无效", 422)
            task = asyncio.create_task(job)
            self._operations[task] = abort
            try:
                result = await _wait(task, abort.set)
                value = result.model_dump(mode="json")
                context._directory_response = (operation, json.dumps(value, sort_keys=True))
                return value
            finally:
                self._operations.pop(task, None)
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error

    async def validate_result(self, operation: str, value: Any, context: ToolContext) -> None:
        if context._directory_response != (operation, json.dumps(value, sort_keys=True)):
            raise ToolError(
                "workspace_directory_receipt_unverified", "目录回程未保留原后端事实，请查询原操作"
            )

    @staticmethod
    def _unknown(change: PreparedDirectoryChange, code: str) -> DirectoryEffect:
        return DirectoryEffect(
            DirectoryChangeReceipt(
                change.spec.path,
                "uncertain",
                change.before.fingerprint if change.before else None,
                None,
                True,
                code,
                True,
                change.token,
            )
        )

    async def _renew(self, scope: str, identifier: str, context: ToolContext, abort: Event) -> None:
        try:
            while True:
                await asyncio.sleep(min(0.2, self.store.lease_seconds / 4))
                await self.store.renew(scope, identifier)
                await self.changes._guard(context)
        except asyncio.CancelledError:
            raise
        except Exception:
            abort.set()

    async def _change(
        self, request: DirectoryChangeAction, context: ToolContext, abort: Event
    ) -> DirectoryAttemptView:
        scope, binding = await self.changes._guard(context)
        previous = await self.store.find_action(scope, request, binding.fingerprint)
        if previous is not None:
            return previous.view
        original = await self.store.get(scope, request.proposal_id)
        if (
            original.view.workspace_key != binding.key
            or original.view.resource_fingerprint != binding.fingerprint
        ):
            raise ServiceError("workspace_binding_changed", "目录提案不属于当前工作区资源", 403)
        prepared = original.prepared
        writer = WorkspaceDirectoryWriter(binding.require())
        if request.action == "undo":
            history = await self.store.history(scope, request.proposal_id)
            applied = next(
                (
                    item
                    for item in reversed(history)
                    if item.view.action == "apply" and item.view.status == "done"
                ),
                None,
            )
            if applied is None:
                raise ServiceError("workspace_directory_state", "目录尚无原应用事实", 409)
            effect = await self.store.effect(scope, applied.view.operation_id)
            prepared = await workspace_io(writer.prepare_undo, prepared, effect)
        attempt, created = await self.store.reserve(
            scope,
            request,
            binding.fingerprint,
            prepared,
            run_id=context.run_id if context.run_id != "local" else None,
            call_id=context.call_id,
        )
        if not created:
            return attempt.view
        identifier = attempt.view.operation_id
        self._active.add(identifier)
        renew = asyncio.create_task(self._renew(scope, identifier, context, abort))
        loop = asyncio.get_running_loop()

        async def allowed() -> None:
            await self.changes._guard(context)
            await self.store.assert_execution(scope, identifier)

        def authorize() -> None:
            future = asyncio.run_coroutine_threadsafe(allowed(), loop)
            try:
                future.result(timeout=writer.lock_timeout)
            except Exception:
                future.cancel()
                raise ServiceError(
                    "workspace_directory_authority", "目录发布资格无法核实", 409
                ) from None

        def record(reference: DirectoryRecovery) -> None:
            future = asyncio.run_coroutine_threadsafe(
                self.store.record_intent(scope, identifier, reference), loop
            )
            try:
                future.result(timeout=writer.lock_timeout)
            except Exception:
                future.cancel()
                raise ServiceError(
                    "workspace_directory_intent", "目录意图尚未可靠保存", 503
                ) from None

        try:
            if request.action == "keep":
                history = await self.store.history(scope, request.proposal_id)
                source = next(
                    item
                    for item in reversed(history)
                    if item.view.action == "apply" and item.view.status == "done"
                )
                source_effect = await self.store.effect(scope, source.view.operation_id)
                actual = await workspace_io(writer.capture, prepared.spec.path)
                if actual != source_effect.observed_after:
                    effect = DirectoryEffect(
                        DirectoryChangeReceipt(
                            prepared.spec.path,
                            "rejected",
                            prepared.before.fingerprint if prepared.before else None,
                            None,
                            False,
                            "workspace_directory_conflict",
                            False,
                            prepared.token,
                        )
                    )
                else:
                    effect = replace(
                        source_effect, receipt=replace(source_effect.receipt, changed=False)
                    )
            else:
                try:
                    effect = await workspace_io(
                        writer.apply,
                        prepared,
                        abort=lambda: abort.is_set() or not binding.available or self._closed,
                        authorize=authorize,
                        record_intent=record,
                    )
                except Exception:
                    effect = self._unknown(prepared, "workspace_directory_execution_unknown")
            try:
                return (await self.store.settle(scope, identifier, effect)).view
            except Exception:
                saved = await self.store.operation(scope, identifier)
                if saved.view.status in {"done", "rejected", "uncertain"}:
                    return saved.view
                pending = replace(
                    effect,
                    receipt=replace(
                        effect.receipt,
                        status="uncertain",
                        error_code="workspace_directory_receipt_pending",
                        recovery_required=True,
                    ),
                )
                return (await self.store.settle(scope, identifier, pending)).view
        finally:
            self._active.discard(identifier)
            renew.cancel()
            await asyncio.gather(renew, return_exceptions=True)

    async def _reconcile(self, identifier: str, context: ToolContext) -> DirectoryAttemptView:
        scope, binding = await self.changes._guard(context)
        if identifier in self._active:
            raise ServiceError("workspace_directory_active", "活跃目录操作不可接管", 409)
        attempt = await self.store.operation(scope, identifier)
        proposal = await self.store.get(scope, attempt.view.proposal_id)
        if (
            proposal.view.workspace_key != binding.key
            or proposal.view.resource_fingerprint != binding.fingerprint
        ):
            raise ServiceError("workspace_binding_changed", "目录操作不属于当前工作区资源", 403)
        if attempt.view.status == "running":
            raise ServiceError("workspace_directory_active", "原目录owner仍活跃", 409)
        if attempt.view.status != "uncertain":
            return attempt.view
        if attempt.recovery is None:
            return attempt.view
        effect = await workspace_io(
            WorkspaceDirectoryWriter(binding.require()).verify, attempt.prepared, attempt.recovery
        )
        return (await self.store.settle(scope, identifier, effect, resolving=True)).view

    async def _finalize(self, identifier: str, context: ToolContext) -> DirectoryAttemptView:
        scope, binding = await self.changes._guard(context)
        attempt = await self.store.operation(scope, identifier)
        proposal = await self.store.get(scope, attempt.view.proposal_id)
        if (
            proposal.view.workspace_key != binding.key
            or proposal.view.resource_fingerprint != binding.fingerprint
        ):
            raise ServiceError("workspace_binding_changed", "目录操作不属于当前工作区资源", 403)
        if attempt.view.status not in {"done", "rejected"}:
            raise ServiceError("workspace_directory_state", "未确认目录操作不能清理", 409)
        if attempt.recovery is not None:
            await workspace_io(
                WorkspaceDirectoryWriter(binding.require()).finalize,
                attempt.prepared,
                attempt.recovery,
                rejected=attempt.view.status == "rejected",
            )
        return (await self.store.mark_cleaned(scope, identifier)).view

    async def close(self) -> None:
        self.assert_external_close()
        self._closed = True
        tasks = list(self._operations)
        for event in self._operations.values():
            event.set()
        await asyncio.gather(*tasks, return_exceptions=True)
