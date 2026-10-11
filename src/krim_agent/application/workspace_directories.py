"""目录事务Worker：同Registry上下文、发布前持久意图与原回执收束。"""

from __future__ import annotations

import asyncio
import json
from contextvars import ContextVar
from dataclasses import replace
from threading import Event
from typing import Any, cast

from pydantic import JsonValue

from krim_agent.execution.binding import ExecutionBinding
from krim_agent.execution.directory_publication import (
    CommandDirectoryChangesRequest,
    CommandDirectoryProposalRequest,
)
from krim_agent.persistence.agent_sessions import guard_agent_topic
from krim_agent.persistence.command_directory_proposals import (
    CommandDirectoryProposalStore,
    directory_entry,
)
from krim_agent.persistence.workspace_directories import WorkspaceDirectoryStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.tools.core import ToolContext, ToolError
from krim_agent.tools.workspace import workspace_io
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_directory import (
    ArtifactDirectorySpec,
    DirectoryAttemptView,
    DirectoryChangeAction,
    DirectoryChangeReceipt,
    DirectoryEffect,
    DirectoryInspectRequest,
    DirectoryInspectView,
    DirectoryOperationRequest,
    DirectoryPreviewRequest,
    DirectoryQueryRequest,
    DirectoryQueryResult,
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
        self.command_sources = CommandDirectoryProposalStore(self.store)

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
            if operation == "source_query":
                catalog_request = CommandDirectoryChangesRequest.model_validate(payload)
                scope, catalog_binding, catalog_execution = await self._source_guard(
                    context, history=True
                )
                source = await self.command_sources.source(scope, catalog_request.command_id)
                if (
                    source.workspace != catalog_binding.manifest()
                    or source.snapshot.resource != catalog_execution.resource
                ):
                    raise ServiceError(
                        "execution_proposal_source", "目录来源与原资源绑定不一致", 403
                    )
                catalog_value = await self.command_sources.page(scope, catalog_request)
                if catalog_value.source_hash != source.source_hash:
                    raise ServiceError("execution_source_corrupt", "目录来源查阅期间变化", 409)
                await self._source_guard(context, history=True)
                source_value = catalog_value.model_dump(mode="json")
                context._directory_response = (operation, json.dumps(source_value, sort_keys=True))
                return source_value
            if operation == "source_preview":
                source_request = CommandDirectoryProposalRequest.model_validate(payload)
                scope, source_binding, execution = await self._source_guard(context)
                previous = await self.command_sources.find(
                    scope,
                    source_request,
                    source_binding,
                    run_id=context.run_id if context.run_id != "local" else None,
                )
                if previous is None:
                    source = await self.command_sources.source(scope, source_request.command_id)
                    if (
                        source.workspace != source_binding.manifest()
                        or source.snapshot.resource != execution.resource
                    ):
                        raise ServiceError(
                            "execution_proposal_source", "目录来源与当前工作区/执行资源不匹配", 403
                        )
                    if source.outcome != "completed" and not source_request.accept_failed:
                        raise ServiceError(
                            "execution_failed_artifacts", "失败命令目录须显式accept_failed", 409
                        )
                    entry = directory_entry(source, source_request.path)
                    if not entry.creatable:
                        raise ServiceError(
                            "execution_artifact_mode", "目录模式不支持本宿主受控创建", 422
                        )
                    spec = ArtifactDirectorySpec(
                        path=entry.path,
                        operation="create",
                        mode=entry.mode,
                        command_id=source_request.command_id,
                        source_hash=source.source_hash,
                    )
                    prepared = await workspace_io(
                        WorkspaceDirectoryWriter(source_binding.require()).prepare_artifact, spec
                    )
                    await self._source_guard(context)
                    previous = await self.command_sources.create(
                        scope,
                        source_request,
                        source_binding,
                        prepared,
                        run_id=context.run_id if context.run_id != "local" else None,
                    )
                await self._source_guard(context)
                source_value = previous.model_dump(mode="json")
                context._directory_response = (
                    operation,
                    json.dumps(source_value, sort_keys=True),
                )
                return source_value
            if operation == "inspect":
                inspection = DirectoryInspectRequest.model_validate(payload)
                _, read_binding = await self.changes._guard(context, writing=False)
                snapshot = await workspace_io(
                    WorkspaceDirectoryWriter(read_binding.require()).capture, inspection.path
                )
                await self.changes._guard(context, writing=False)
                result_view = DirectoryInspectView(
                    path=inspection.path,
                    found=snapshot is not None,
                    fingerprint=snapshot.fingerprint if snapshot else None,
                    mode=snapshot.mode if snapshot else None,
                    empty=snapshot.empty if snapshot else None,
                )
                read_value = result_view.model_dump(mode="json")
                context._directory_response = (operation, json.dumps(read_value, sort_keys=True))
                return read_value
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
            if operation == "query":
                query_request = DirectoryQueryRequest.model_validate(payload)
                scope, _binding = await self._query_guard(context)
                if query_request.kind == "proposal":
                    value = DirectoryQueryResult(
                        kind=query_request.kind,
                        proposal=(
                            await self.store.get(scope, cast(str, query_request.identifier))
                        ).view,
                    )
                elif query_request.kind == "operation":
                    value = DirectoryQueryResult(
                        kind=query_request.kind,
                        operation=(
                            await self.store.operation(scope, cast(str, query_request.identifier))
                        ).view,
                    )
                elif query_request.kind == "history":
                    proposal = await self.store.get(scope, cast(str, query_request.identifier))
                    history_rows, history_after = await self.store.history_page(
                        scope,
                        proposal.view.proposal_id,
                        after=query_request.after,
                        limit=query_request.limit,
                    )
                    value = DirectoryQueryResult(
                        kind=query_request.kind,
                        history=[item.view for item in history_rows],
                        next_after=history_after,
                    )
                elif query_request.kind == "proposals":
                    rows, after = await self.store.proposals(
                        scope, after=query_request.after, limit=query_request.limit
                    )
                    value = DirectoryQueryResult(
                        kind=query_request.kind,
                        proposals=[item.view for item in rows],
                        next_after=after,
                    )
                elif query_request.kind == "proposal_by_key":
                    found = await self.store.proposal_by_key(
                        scope, cast(str, query_request.request_key)
                    )
                    value = DirectoryQueryResult(
                        kind=query_request.kind, proposal=found.view if found else None
                    )
                else:
                    found_operation = await self.store.operation_by_key(
                        scope, cast(str, query_request.request_key)
                    )
                    value = DirectoryQueryResult(
                        kind=query_request.kind,
                        operation=found_operation.view if found_operation else None,
                    )
                proposals = [*value.proposals]
                if value.proposal is not None:
                    proposals.append(value.proposal)
                if query_request.kind == "history":
                    proposals.append(proposal.view)
                if value.operation is not None:
                    proposals.append(
                        (await self.store.get(scope, value.operation.proposal_id)).view
                    )
                if any(
                    entry.workspace_key != _binding.key
                    or entry.resource_fingerprint != _binding.fingerprint
                    for entry in proposals
                ):
                    raise ServiceError(
                        "workspace_binding_changed", "目录查询事实不属于原资源版本", 403
                    )
                # 查库期间可能关闭会话或改动绑定；只读回程也要核对原资源。
                _, current_binding = await self._query_guard(context)
                if current_binding.manifest() != _binding.manifest():
                    raise ServiceError("workspace_binding_changed", "目录查询资源版本已变化", 403)
                query_value = value.model_dump(mode="json")
                context._directory_response = (operation, json.dumps(query_value, sort_keys=True))
                return query_value
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
                operation_value = result.model_dump(mode="json")
                context._directory_response = (
                    operation,
                    json.dumps(operation_value, sort_keys=True),
                )
                return operation_value
            finally:
                self._operations.pop(task, None)
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error

    async def validate_result(self, operation: str, value: Any, context: ToolContext) -> None:
        if context._directory_response != (operation, json.dumps(value, sort_keys=True)):
            raise ToolError(
                "workspace_directory_receipt_unverified", "目录回程未保留原后端事实，请查询原操作"
            )

    async def _source_guard(
        self, context: ToolContext, *, history: bool = False
    ) -> tuple[str, WorkspaceBinding, ExecutionBinding]:
        scope, binding = (
            await self._query_guard(context) if history else await self.changes._guard(context)
        )
        execution = context.execution
        if execution is None:
            raise ServiceError("execution_unbound", "目录来源需要原执行资源授权", 409)
        if not history or context.origin != "host" or context.run_id != "local":
            execution.require()
        async with self.store.database.write_lock, self.store.database.sessions() as session:
            row = await guard_agent_topic(
                session, context.topic_id, context.session_id, context.agent_id, self.store.owner_id
            )
            if row is not None and row.permissions.get(
                "execution"
            ) != execution.resource.model_dump(mode="json"):
                raise ServiceError("execution_binding_changed", "目录来源执行资源已变化", 403)
        return scope, binding, execution

    async def _query_guard(self, context: ToolContext) -> tuple[str, WorkspaceBinding]:
        if context.origin != "host" or context.run_id != "local":
            return await self.changes._guard(context, writing=False)
        if self._closed or context.workspace is None:
            raise ServiceError("workspace_unbound", "目录查询没有原工作区绑定", 409)
        binding = context.workspace
        async with self.store.database.write_lock, self.store.database.sessions() as session:
            row = await guard_agent_topic(
                session, context.topic_id, context.session_id, context.agent_id, self.store.owner_id
            )
            if row is None:
                raise ServiceError("agent_session_topic", "目录查询没有会话绑定", 403)
            if row.permissions.get("workspace") != binding.manifest():
                raise ServiceError("workspace_binding_changed", "目录查询资源版本已变化", 403)
        return (
            f"session:{context.session_id}" if context.session_id else f"topic:{context.topic_id}"
        ), binding

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
