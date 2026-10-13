"""原生成根独立编排；复用同一个Move producer/取消/关闭与物理账本。"""

from __future__ import annotations

import asyncio
from threading import Event
from typing import TYPE_CHECKING, Any, cast

from pydantic import JsonValue

from krim_agent.generated_tree_undo import (
    GeneratedTreeUndoAction,
    GeneratedTreeUndoActionKind,
    GeneratedTreeUndoAttemptPage,
    GeneratedTreeUndoAttemptView,
    GeneratedTreeUndoCancelRequest,
    GeneratedTreeUndoPreviewRequest,
    GeneratedTreeUndoPreviewView,
    GeneratedTreeUndoQueryResult,
    GeneratedTreeUndoSource,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.tools.core import ToolContext
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_tree_move import (
    TreeMoveAction,
    TreeMoveActionKind,
    TreeMoveAttemptView,
    TreeMoveOperationRequest,
    TreeMovePreviewRequest,
    TreeMoveQueryRequest,
)

if TYPE_CHECKING:
    from .workspace_tree_moves import TreeMoveService


class GeneratedTreeUndoOperations:
    def __init__(self, moves: TreeMoveService) -> None:
        self.moves = moves
        self.store = moves.store

    @staticmethod
    def budget(context: ToolContext) -> None:
        if not context.execution_timeouts:
            raise ServiceError(
                "workspace_generated_undo_context", "生成根工具须原Registry执行预算", 403
            )
        now = asyncio.get_running_loop().time()
        for execution in context.execution_timeouts:
            when = execution.deadline.when()
            if (
                not execution.active
                or execution.deadline.expired()
                or when is not None
                and when <= now
            ):
                raise ServiceError(
                    "workspace_generated_undo_deadline", "原生成根执行预算已到期", 409
                )

    async def execute(
        self, operation: str, payload: dict[str, Any], context: ToolContext, abort: Event
    ) -> dict[str, JsonValue]:
        self.budget(context)
        if operation == "generated_undo_preview":
            value = await self.preview(
                GeneratedTreeUndoPreviewRequest.model_validate(payload), context, abort
            )
        elif operation == "generated_undo_query":
            return (
                await self.query(TreeMoveQueryRequest.model_validate(payload), context)
            ).model_dump(mode="json")
        elif operation == "generated_undo_change":
            return (
                await self.change(GeneratedTreeUndoAction.model_validate(payload), context, abort)
            ).model_dump(mode="json")
        elif operation == "generated_undo_reconcile":
            lookup = TreeMoveOperationRequest.model_validate(payload)
            scope, binding = await self.moves._guard(context)
            original = await self.store.operation(scope, lookup.operation_id)
            await self.proposal(scope, binding, original.view.proposal_id)
            actual = await self.moves._reconcile(
                lookup.operation_id, context, abort, qualify=lambda: self.budget(context)
            )
            return (await self.attempt(scope, binding, actual)).model_dump(mode="json")
        elif operation == "generated_undo_cancel":
            cancel_request = GeneratedTreeUndoCancelRequest.model_validate(payload)
            scope, binding = await self.moves._guard(context)
            await self.proposal(scope, binding, cancel_request.proposal_id)
            self.budget(context)
            await self.moves.cancel(scope, cancel_request.proposal_id)
            value = await self.proposal(scope, binding, cancel_request.proposal_id)
        else:
            raise ServiceError("workspace_generated_undo_operation", "生成根工具操作无效", 422)
        return value.model_dump(mode="json")

    async def proposal(
        self, scope: str, binding: WorkspaceBinding, identifier: str
    ) -> GeneratedTreeUndoPreviewView:
        value = await self.store.generated_proposal(scope, identifier)
        self.moves._bound(value.move, binding)
        return value

    async def attempt(
        self, scope: str, binding: WorkspaceBinding, original: TreeMoveAttemptView
    ) -> GeneratedTreeUndoAttemptView:
        parent = await self.proposal(scope, binding, original.proposal_id)
        kind = cast(
            GeneratedTreeUndoActionKind,
            {"apply": "remove_root", "undo": "restore_root", "keep": "keep"}[original.action],
        )
        return GeneratedTreeUndoAttemptView(
            source_plan_id=parent.source_plan_id, action=kind, operation=original
        )

    async def preview(
        self, request: GeneratedTreeUndoPreviewRequest, context: ToolContext, abort: Event
    ) -> GeneratedTreeUndoPreviewView:
        scope, binding = await self.moves._guard(context)
        async with self.store.database.write_lock, self.store.database.sessions() as sql:
            sealed = await self.store.seals.get_material_in_transaction(
                sql,
                scope,
                request.plan_id,
                workspace_key=binding.key,
                resource_fingerprint=binding.fingerprint,
            )
        if sealed is None:
            raise ServiceError(
                "workspace_generated_undo_seal_required", "生成根撤销须原首次完整seal", 409
            )
        mapped = TreeMovePreviewRequest(
            source_path=sealed.origin.snapshot.root_path,
            target_path=request.preserve_path,
            expected_fingerprint=request.expected_fingerprint,
            request_key=request.request_key,
            ttl_seconds=request.ttl_seconds,
        )
        actual = await self.moves._preview(
            mapped,
            context,
            abort,
            generated_origin=GeneratedTreeUndoSource(
                request.plan_id, request.previous_restore_operation_id
            ),
            qualify=lambda: self.budget(context),
        )
        return await self.proposal(scope, binding, actual.proposal_id)

    async def change(
        self, request: GeneratedTreeUndoAction, context: ToolContext, abort: Event
    ) -> GeneratedTreeUndoAttemptView:
        scope, binding = await self.moves._guard(context)
        await self.proposal(scope, binding, request.proposal_id)
        kind = cast(
            TreeMoveActionKind,
            {"remove_root": "apply", "restore_root": "undo", "keep": "keep"}[request.action],
        )
        mapped = TreeMoveAction(
            proposal_id=request.proposal_id,
            action=kind,
            expected_revision=request.expected_revision,
            request_key=request.request_key,
        )
        actual = await self.moves._change(
            mapped, context, abort, qualify=lambda: self.budget(context)
        )
        return await self.attempt(scope, binding, actual)

    async def query(
        self, request: TreeMoveQueryRequest, context: ToolContext
    ) -> GeneratedTreeUndoQueryResult:
        self.moves._source(context)
        scope, binding = await self.moves.directories._query_guard(context)
        value = GeneratedTreeUndoQueryResult(kind=request.kind)
        identifier = cast(str, request.identifier)
        if request.kind == "proposal":
            value.proposal = await self.proposal(scope, binding, identifier)
        elif request.kind == "by_key":
            selected = await self.store.proposal_by_key(scope, cast(str, request.request_key))
            value.proposal = (
                await self.proposal(scope, binding, selected.view.proposal_id) if selected else None
            )
        elif request.kind in {"operation", "operation_by_key"}:
            attempt = (
                await self.store.operation(scope, identifier)
                if request.kind == "operation"
                else await self.store.operation_by_key(scope, cast(str, request.request_key))
            )
            value.operation = await self.attempt(scope, binding, attempt.view) if attempt else None
        elif request.kind == "history":
            await self.proposal(scope, binding, identifier)
            page = await self.store.history_page(
                scope, identifier, after=request.after, limit=request.limit
            )
            value.history = GeneratedTreeUndoAttemptPage(
                items=[await self.attempt(scope, binding, item) for item in page.items],
                next_after=page.next_after,
            )
        else:
            value.proposals = await self.store.generated_list(
                scope, after=request.after, limit=request.limit
            )
            for item in value.proposals.items:
                self.moves._bound(item.move, binding)
        _, current = await self.moves.directories._query_guard(context)
        if current.manifest() != binding.manifest():
            raise ServiceError("workspace_binding_changed", "原生成根查询期间资源改变", 403)
        self.budget(context)
        return value
