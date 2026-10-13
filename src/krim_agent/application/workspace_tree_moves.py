"""原非空树账本、Registry上下文与有界Writer的同调用编排。"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from contextvars import ContextVar
from threading import Event
from typing import Any, cast

from pydantic import JsonValue

from krim_agent.generated_tree_undo import GeneratedTreeUndoSource
from krim_agent.persistence.workspace_tree_moves import WorkspaceTreeMoveStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.tools.core import ToolContext, ToolError
from krim_agent.tools.workspace import workspace_io
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_tree_move import (
    TreeMoveAction,
    TreeMoveAttemptView,
    TreeMoveInspectRequest,
    TreeMoveInspectView,
    TreeMoveModelSource,
    TreeMoveOperationRequest,
    TreeMovePreviewRequest,
    TreeMovePreviewView,
    TreeMoveQueryRequest,
    TreeMoveQueryResult,
)
from krim_agent.workspace_tree_writer import (
    PreparedTreeMove,
    TreeMoveEffect,
    TreeMoveReceipt,
    WorkspaceTreeWriter,
)

from .workspace_changes import WorkspaceChangeService, _wait
from .workspace_directories import DirectoryChangeService

_current: ContextVar[tuple[object, asyncio.Task[Any] | None] | None] = ContextVar(
    "krim_tree_move_operation", default=None
)


class TreeMoveService:
    def __init__(
        self, changes: WorkspaceChangeService, directories: DirectoryChangeService
    ) -> None:
        self.changes, self.directories = changes, directories
        self.store = WorkspaceTreeMoveStore(changes.store)
        self._operations: dict[asyncio.Task[dict[str, JsonValue]], Event] = {}
        self._active: dict[str, tuple[str, asyncio.Task[Any], Event]] = {}
        self._closed = False

    def _inside(self) -> bool:
        marker = _current.get()
        return (
            marker is not None
            and marker[0] is self
            and marker[1] is not None
            and not marker[1].done()
        )

    def assert_external_close(self) -> None:
        if self._inside():
            raise ServiceError("workspace_tree_move_self_close", "树移动回调不能关闭自身服务", 409)

    @asynccontextmanager
    async def call_scope(self) -> AsyncIterator[None]:
        """覆盖原Registry的审批/Hook/执行/回程，先拒绝自身关闭再释放SDK/DB。"""
        marker = _current.set((self, asyncio.current_task()))
        try:
            yield
        finally:
            _current.reset(marker)

    def _source(self, context: ToolContext) -> TreeMoveModelSource | None:
        if self._closed:
            raise ServiceError("workspace_tree_move_closed", "树移动服务已关闭", 503)
        if any(
            value is not None
            for value in (
                context.tree_fence,
                context.mixed_batch,
                context.workspace_batch,
                context.checkpoint_capture,
                context.checkpoint_restore,
                context.command_capture,
                context.source_call_id,
            )
        ):
            raise ServiceError("workspace_tree_move_context", "其他事务上下文不能开启树移动", 403)
        if context.origin == "host" and context.run_id == "local":
            return None
        if (
            context.origin != "model"
            or context.run_id == "local"
            or context.session_id is None
            or context.agent_id is None
        ):
            raise ServiceError(
                "workspace_tree_move_model_context", "树移动需要原Agent/session/Run/call", 403
            )
        return TreeMoveModelSource(context.run_id, context.call_id)

    async def _guard(
        self, context: ToolContext, *, writing: bool = True
    ) -> tuple[str, WorkspaceBinding]:
        self._source(context)
        return await self.changes._guard(context, writing=writing)

    async def tool_operation(
        self, operation: str, payload: dict[str, Any], context: ToolContext
    ) -> dict[str, JsonValue]:
        marker = _current.set((self, asyncio.current_task()))
        try:
            self._source(context)
            if len(self._operations) >= 64:
                raise ServiceError("workspace_tree_move_busy", "树移动调用已达上限", 429)
            abort = Event()
            task = asyncio.create_task(self._execute(operation, payload, context, abort))
            self._operations[task] = abort
            try:
                value = await _wait(task, abort.set)
                context._tree_move_response = (operation, json.dumps(value, sort_keys=True))
                return value
            finally:
                self._operations.pop(task, None)
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error
        finally:
            _current.reset(marker)

    async def validate_result(self, operation: str, value: Any, context: ToolContext) -> None:
        if context._tree_move_response != (operation, json.dumps(value, sort_keys=True)):
            raise ToolError(
                "workspace_tree_move_unverified", "树移动回程未保留原后端事实，请查询原操作"
            )

    def _bridge(
        self,
        loop: asyncio.AbstractEventLoop,
        timeout: float,
        operation: Callable[[], Coroutine[Any, Any, None]],
        code: str,
    ) -> Callable[[], None]:
        def call() -> None:
            future = asyncio.run_coroutine_threadsafe(operation(), loop)
            try:
                future.result(timeout=timeout)
            except Exception:
                future.cancel()
                raise ServiceError(code, "原树资格或意图无法可靠核实", 409) from None

        return call

    async def _execute(
        self, operation: str, payload: dict[str, Any], context: ToolContext, abort: Event
    ) -> dict[str, JsonValue]:
        if operation.startswith("generated_undo_"):
            from .generated_tree_undo import GeneratedTreeUndoOperations

            return await GeneratedTreeUndoOperations(self).execute(
                operation, payload, context, abort
            )
        if operation == "query":
            return (
                await self._query(TreeMoveQueryRequest.model_validate(payload), context)
            ).model_dump(mode="json")
        if operation == "change":
            return (
                await self._change(TreeMoveAction.model_validate(payload), context, abort)
            ).model_dump(mode="json")
        if operation == "reconcile":
            lookup = TreeMoveOperationRequest.model_validate(payload)
            return (await self._reconcile(lookup.operation_id, context, abort)).model_dump(
                mode="json"
            )
        if operation == "preview":
            return (
                await self._preview(TreeMovePreviewRequest.model_validate(payload), context, abort)
            ).model_dump(mode="json")
        scope, binding = await self._guard(context, writing=operation != "inspect")
        writer = WorkspaceTreeWriter(binding.require())
        authorize = self._bridge(
            asyncio.get_running_loop(),
            writer.lock_timeout,
            lambda: self._read_authority(context, writing=operation != "inspect"),
            "workspace_tree_move_authority",
        )
        if operation == "inspect":
            inspection = TreeMoveInspectRequest.model_validate(payload)
            present = await workspace_io(writer.directories.capture, inspection.path)
            snapshot = (
                None
                if present is None
                else await workspace_io(
                    writer.reader.capture,
                    inspection.path,
                    authorize=authorize,
                    cancelled=abort.is_set,
                )
            )
            await self._guard(context, writing=False)
            root = snapshot.entries[0].directory if snapshot else None
            return TreeMoveInspectView(
                path=inspection.path,
                found=snapshot is not None,
                fingerprint=snapshot.fingerprint if snapshot else None,
                entry_count=len(snapshot.entries) if snapshot else 0,
                directory_count=sum(entry.directory is not None for entry in snapshot.entries)
                if snapshot
                else 0,
                file_count=sum(entry.file is not None for entry in snapshot.entries)
                if snapshot
                else 0,
                byte_size=snapshot.total_bytes if snapshot else 0,
                mode=root.mode if root else None,
            ).model_dump(mode="json")
        raise ServiceError("workspace_tree_move_operation", "树移动操作无效", 422)

    async def _preview(
        self,
        request: TreeMovePreviewRequest,
        context: ToolContext,
        abort: Event,
        *,
        generated_origin: GeneratedTreeUndoSource | None = None,
        qualify: Callable[[], None] | None = None,
    ) -> TreeMovePreviewView:
        scope, binding = await self._guard(context)
        if qualify is not None:
            qualify()
        writer = WorkspaceTreeWriter(binding.require())
        authorize = self._bridge(
            asyncio.get_running_loop(),
            writer.lock_timeout,
            lambda: self._read_authority(context, qualify=qualify),
            "workspace_tree_move_authority",
        )
        prepared = None
        if await self.store.proposal_by_key(scope, request.request_key) is None:
            prepared = await workspace_io(
                writer.prepare,
                request.source_path,
                request.target_path,
                expected_fingerprint=request.expected_fingerprint,
                authorize=authorize,
            )
        await self._guard(context)
        if qualify is not None:
            qualify()
        if abort.is_set():
            raise ServiceError("workspace_write_cancelled", "原树准备已取消", 409)
        stored, _ = await self.store.preview(
            scope,
            binding,
            request,
            prepared,
            model_source=self._source(context),
            generated_origin=generated_origin,
        )
        return stored.view

    async def _read_authority(
        self,
        context: ToolContext,
        *,
        writing: bool = True,
        qualify: Callable[[], None] | None = None,
    ) -> None:
        await self._guard(context, writing=writing)
        if qualify is not None:
            qualify()

    @staticmethod
    def _bound(view: TreeMovePreviewView, binding: WorkspaceBinding) -> None:
        if view.workspace_key != binding.key or view.resource_fingerprint != binding.fingerprint:
            raise ServiceError("workspace_binding_changed", "树移动事实不属于原工作区版本", 403)

    async def _query(
        self, request: TreeMoveQueryRequest, context: ToolContext
    ) -> TreeMoveQueryResult:
        self._source(context)
        scope, binding = await self.directories._query_guard(context)
        value = TreeMoveQueryResult(kind=request.kind)
        identifier = cast(str, request.identifier)
        if request.kind == "proposal":
            value.proposal = (await self.store.proposal(scope, identifier)).view
        elif request.kind == "operation":
            value.operation = (await self.store.operation(scope, identifier)).view
        elif request.kind == "by_key":
            proposal = await self.store.proposal_by_key(scope, cast(str, request.request_key))
            value.proposal = proposal.view if proposal else None
        elif request.kind == "operation_by_key":
            attempt = await self.store.operation_by_key(scope, cast(str, request.request_key))
            value.operation = attempt.view if attempt else None
        elif request.kind == "history":
            self._bound((await self.store.proposal(scope, identifier)).view, binding)
            value.history = await self.store.history_page(
                scope, identifier, after=request.after, limit=request.limit
            )
        else:
            value.proposals = await self.store.list(scope, after=request.after, limit=request.limit)
        for entry in ([value.proposal] if value.proposal else []) + (
            value.proposals.items if value.proposals else []
        ):
            self._bound(entry, binding)
        if value.operation is not None:
            self._bound(
                (await self.store.proposal(scope, value.operation.proposal_id)).view, binding
            )
        _, current = await self.directories._query_guard(context)
        if current.manifest() != binding.manifest():
            raise ServiceError("workspace_binding_changed", "树移动查询期间原资源已变化", 403)
        return value

    async def _applied(self, scope: str, proposal_id: str) -> TreeMoveEffect:
        history = await self.store.history(scope, proposal_id)
        applied = next(
            (entry for entry in history if entry.action == "apply" and entry.status == "done"), None
        )
        if applied is None:
            raise ServiceError("workspace_tree_move_state", "树移动尚无原真实apply", 409)
        effect = (await self.store.operation(scope, applied.operation_id)).effect
        if effect is None:
            raise ServiceError("workspace_tree_move_corrupt", "原完整after材料缺失", 409)
        return effect

    @staticmethod
    def _unknown(change: PreparedTreeMove, code: str) -> TreeMoveEffect:
        return TreeMoveEffect(
            TreeMoveReceipt(
                change.before.root_path,
                change.target_path,
                "uncertain",
                change.before.fingerprint,
                None,
                change.token,
                True,
                code,
                True,
            )
        )

    async def _renew(
        self,
        scope: str,
        identifier: str,
        context: ToolContext,
        abort: Event,
        *,
        qualify: Callable[[], None] | None = None,
    ) -> None:
        try:
            while True:
                await asyncio.sleep(min(0.2, self.store.lease_seconds / 4))
                await self._guard(context)
                if qualify is not None:
                    qualify()
                await self.store.renew(scope, identifier)
                if qualify is not None:
                    qualify()
        except asyncio.CancelledError:
            raise
        except Exception:
            abort.set()

    async def _change(
        self,
        request: TreeMoveAction,
        context: ToolContext,
        abort: Event,
        *,
        qualify: Callable[[], None] | None = None,
    ) -> TreeMoveAttemptView:
        scope, binding = await self._guard(context)
        if qualify is not None:
            qualify()
        source = self._source(context)
        previous = await self.store.find_action(
            scope, request, binding.fingerprint, model_source=source
        )
        if previous is not None:
            return previous.view
        original = await self.store.proposal(scope, request.proposal_id)
        self._bound(original.view, binding)
        writer = WorkspaceTreeWriter(binding.require())
        prepared = original.prepared
        applied = (
            await self._applied(scope, request.proposal_id) if request.action != "apply" else None
        )
        if request.action == "undo":
            assert applied is not None
            prepared = await workspace_io(writer.prepare_undo, prepared, applied)
        await self._guard(context)
        if qualify is not None:
            qualify()
        if abort.is_set():
            raise ServiceError("workspace_write_cancelled", "原树动作在准入前取消", 409)
        attempt, created = await self.store.reserve(
            scope, request, binding.fingerprint, prepared, model_source=source
        )
        if not created:
            return attempt.view
        identifier = attempt.view.operation_id
        task = asyncio.current_task()
        assert task is not None
        self._active[request.proposal_id] = (scope, task, abort)
        renewal = (
            self._renew(scope, identifier, context, abort)
            if qualify is None
            else self._renew(scope, identifier, context, abort, qualify=qualify)
        )
        heartbeat = asyncio.create_task(renewal)
        loop = asyncio.get_running_loop()

        async def allowed() -> None:
            await self._guard(context)
            if qualify is not None:
                qualify()
            await self.store.assert_execution(scope, identifier)
            if qualify is not None:
                qualify()

        authorize = self._bridge(
            loop, writer.lock_timeout, allowed, "workspace_tree_move_authority"
        )

        def intent(change: PreparedTreeMove) -> None:
            self._bridge(
                loop,
                writer.lock_timeout,
                lambda: self.store.record_intent(scope, identifier, change),
                "workspace_tree_move_intent",
            )()

        try:
            try:
                if request.action == "keep":
                    assert applied is not None
                    effect = await workspace_io(
                        writer.verify_keep,
                        prepared,
                        applied,
                        cancelled=abort.is_set,
                        authorize=authorize,
                    )
                else:
                    effect = await workspace_io(
                        writer.apply,
                        prepared,
                        cancelled=abort.is_set,
                        authorize=authorize,
                        record_intent=intent,
                    )
            except Exception:
                effect = self._unknown(prepared, "workspace_tree_move_execution_unknown")
            try:
                return (await self.store.settle(scope, identifier, effect)).view
            except Exception:
                # 原回执无法可靠保存时停止producer；保留原材料/锁，同operation核实。
                return (await self.store.mark_uncertain(scope, identifier)).view
        finally:
            if self._active.get(request.proposal_id) == (scope, task, abort):
                self._active.pop(request.proposal_id, None)
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)

    async def _reconcile(
        self,
        identifier: str,
        context: ToolContext,
        abort: Event,
        *,
        qualify: Callable[[], None] | None = None,
    ) -> TreeMoveAttemptView:
        scope, binding = await self._guard(context)
        if qualify is not None:
            qualify()
        attempt = await self.store.operation(scope, identifier)
        proposal_id = attempt.view.proposal_id
        if proposal_id in self._active or attempt.view.status == "running":
            raise ServiceError("workspace_tree_move_active", "原树producer仍活跃，不能接管", 409)
        self._bound((await self.store.proposal(scope, proposal_id)).view, binding)
        if attempt.view.status != "uncertain":
            return attempt.view
        writer = WorkspaceTreeWriter(binding.require())
        if attempt.view.action == "keep":
            applied = await self._applied(scope, proposal_id)
            effect = await workspace_io(
                writer.verify_keep, attempt.prepared, applied, cancelled=abort.is_set
            )
        else:
            effect = await workspace_io(writer.reconcile, attempt.prepared)
            if effect.receipt.status == "done" and not attempt.intent_recorded:
                effect = self._unknown(attempt.prepared, "workspace_tree_move_intent_missing")
        await self._guard(context)
        if qualify is not None:
            qualify()
        if abort.is_set():
            return attempt.view
        return (await self.store.settle(scope, identifier, effect, resolving=True)).view

    async def cancel(self, scope: str, proposal_id: str) -> TreeMovePreviewView:
        cancelled = await self.store.cancel(scope, proposal_id)
        active = self._active.get(proposal_id)
        if active is not None and active[0] == scope:
            active[2].set()
            if not self._inside():
                await _wait(active[1], active[2].set)
        return (await self.store.proposal(scope, cancelled.view.proposal_id)).view

    async def close(self) -> None:
        self.assert_external_close()
        self._closed = True
        for event in self._operations.values():
            event.set()
        for proposal_id, (scope, _task, _event) in tuple(self._active.items()):
            try:
                await self.store.cancel(scope, proposal_id)
            except Exception:
                # 关闭仍须等待线程；租约与Writer最后资格保留fail-closed。
                pass
        tasks = tuple(self._operations)

        async def drain() -> None:
            await asyncio.gather(*tasks, return_exceptions=True)

        await _wait(asyncio.create_task(drain()), lambda: None)
