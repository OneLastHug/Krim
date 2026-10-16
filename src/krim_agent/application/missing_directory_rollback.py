"""缺失基线独立编排；同一原Move服务/任务/取消/关闭及物理副作用。"""

from __future__ import annotations

from threading import Event
from typing import TYPE_CHECKING, Any, cast

from pydantic import JsonValue

from krim_agent.protocol.domain import ServiceError
from krim_agent.tools.core import ToolContext
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_missing_directory_rollback import (
    MissingDirectoryRollbackAction,
    MissingDirectoryRollbackActionKind,
    MissingDirectoryRollbackAttemptView,
    MissingDirectoryRollbackPreview,
    MissingDirectoryRollbackSource,
    MissingDirectoryRollbackView,
)
from krim_agent.workspace_missing_directory_rollback_control import (
    MissingDirectoryRollbackCancel,
    MissingDirectoryRollbackHistory,
    MissingDirectoryRollbackQuery,
    MissingDirectoryRollbackQueryResult,
)
from krim_agent.workspace_tree_move import (
    TreeMoveAction,
    TreeMoveActionKind,
    TreeMoveAttemptView,
    TreeMoveOperationRequest,
    TreeMovePreviewRequest,
)

from .tree_move_budget import TreeMoveBudget

if TYPE_CHECKING:
    from .workspace_tree_moves import TreeMoveService


class MissingDirectoryRollbackOperations:
    def __init__(self, moves: TreeMoveService) -> None:
        self.moves, self.store = moves, moves.store
        self._budget = TreeMoveBudget("workspace_missing_directory_rollback")

    def budget(self, context: ToolContext) -> None:
        self._budget.check(context)

    async def execute(
        self, operation: str, payload: dict[str, Any], context: ToolContext, abort: Event
    ) -> dict[str, JsonValue]:
        # 权限/审批已在原Registry完成；持有物理资格后不再接受预算替换。
        self._budget.start(context)
        if operation == "missing_rollback_preview":
            result = await self.preview(
                MissingDirectoryRollbackPreview.model_validate(payload), context, abort
            )
        elif operation == "missing_rollback_change":
            return (
                await self.change(
                    MissingDirectoryRollbackAction.model_validate(payload), context, abort
                )
            ).model_dump(mode="json")
        elif operation == "missing_rollback_query":
            return (
                await self.query(MissingDirectoryRollbackQuery.model_validate(payload), context)
            ).model_dump(mode="json")
        elif operation == "missing_rollback_reconcile":
            lookup = TreeMoveOperationRequest.model_validate(payload)
            scope, binding = await self.moves._guard(context)
            original = await self.store.operation(scope, lookup.operation_id)
            await self.proposal(scope, binding, original.view.proposal_id)
            actual = await self.moves._reconcile(
                lookup.operation_id, context, abort, qualify=lambda: self.budget(context)
            )
            return (await self.attempt(scope, binding, actual, context)).model_dump(mode="json")
        elif operation == "missing_rollback_cancel":
            wanted = MissingDirectoryRollbackCancel.model_validate(payload)
            scope, binding = await self.moves._guard(context)
            await self.proposal(scope, binding, wanted.proposal_id)
            self.budget(context)
            await self.moves.cancel(scope, wanted.proposal_id)
            result = await self.proposal(scope, binding, wanted.proposal_id)
        else:
            raise ServiceError(
                "workspace_missing_directory_rollback_operation", "缺失回退操作无效", 422
            )
        await self.moves._guard(context)
        self.budget(context)
        return result.model_dump(mode="json")

    async def proposal(
        self, scope: str, binding: WorkspaceBinding, identifier: str
    ) -> MissingDirectoryRollbackView:
        result = await self.store.missing_rollback_proposal(scope, identifier)
        self.moves._bound(result.move, binding)
        return result

    async def attempt(
        self,
        scope: str,
        binding: WorkspaceBinding,
        original: TreeMoveAttemptView,
        context: ToolContext,
    ) -> MissingDirectoryRollbackAttemptView:
        parent = await self.proposal(scope, binding, original.proposal_id)
        result = MissingDirectoryRollbackAttemptView(
            source_plan_id=parent.source_plan_id,
            source_operation_id=parent.source_operation_id,
            action=cast(
                MissingDirectoryRollbackActionKind,
                {
                    "apply": "return_to_absence",
                    "undo": "restore_tree",
                    "keep": "keep",
                }[original.action],
            ),
            operation=original,
        )
        self.budget(context)
        return result

    async def preview(
        self, request: MissingDirectoryRollbackPreview, context: ToolContext, abort: Event
    ) -> MissingDirectoryRollbackView:
        scope, binding = await self.moves._guard(context)
        # 来源读取不能触发新Store的观察失效收束，干扰原仍为ready的计划。
        async with self.store.database.write_lock, self.store.database.sessions() as sql:
            source, _, _ = await self.store.missing_rollbacks.restores._base(
                sql, scope, request.plan_id
            )
            if (source.workspace_key, source.resource_fingerprint) != (
                binding.key,
                binding.fingerprint,
            ):
                raise ServiceError("workspace_binding_changed", "原缺失计划不属于当前资源", 403)
            if source.phase != "finished":
                raise ServiceError(
                    "workspace_missing_directory_rollback_incomplete", "回退须原完成缺失恢复", 409
                )
            root_path = source.root_path
        mapped = TreeMovePreviewRequest(
            source_path=root_path,
            target_path=request.preserve_path,
            expected_fingerprint=request.expected_fingerprint,
            request_key=request.request_key,
            ttl_seconds=request.ttl_seconds,
        )
        actual = await self.moves._preview(
            mapped,
            context,
            abort,
            missing_origin=MissingDirectoryRollbackSource(request.plan_id),
            qualify=lambda: self.budget(context),
        )
        return await self.proposal(scope, binding, actual.proposal_id)

    async def change(
        self, request: MissingDirectoryRollbackAction, context: ToolContext, abort: Event
    ) -> MissingDirectoryRollbackAttemptView:
        scope, binding = await self.moves._guard(context)
        await self.proposal(scope, binding, request.proposal_id)
        mapped = TreeMoveAction(
            proposal_id=request.proposal_id,
            action=cast(
                TreeMoveActionKind,
                {"return_to_absence": "apply", "restore_tree": "undo", "keep": "keep"}[
                    request.action
                ],
            ),
            expected_revision=request.expected_revision,
            request_key=request.request_key,
        )
        actual = await self.moves._change(
            mapped, context, abort, qualify=lambda: self.budget(context)
        )
        return await self.attempt(scope, binding, actual, context)

    async def query(
        self, request: MissingDirectoryRollbackQuery, context: ToolContext
    ) -> MissingDirectoryRollbackQueryResult:
        self.moves._source(context)
        scope, binding = await self.moves.directories._query_guard(context)
        data: dict[str, Any] = {"kind": request.kind}
        identifier = cast(str, request.identifier)
        if request.kind == "proposal":
            data["proposal"] = await self.proposal(scope, binding, identifier)
        elif request.kind == "by_key":
            parent = await self.store.proposal_by_key(scope, cast(str, request.request_key))
            data["proposal"] = (
                await self.proposal(scope, binding, parent.view.proposal_id)
                if parent is not None
                else None
            )
        elif request.kind in {"operation", "operation_by_key"}:
            attempt = (
                await self.store.operation(scope, identifier)
                if request.kind == "operation"
                else await self.store.operation_by_key(scope, cast(str, request.request_key))
            )
            data["operation"] = (
                await self.attempt(scope, binding, attempt.view, context)
                if attempt is not None
                else None
            )
        elif request.kind == "history":
            await self.proposal(scope, binding, identifier)
            page = await self.store.history_page(
                scope, identifier, after=request.after, limit=request.limit
            )
            data["history"] = MissingDirectoryRollbackHistory(
                items=[await self.attempt(scope, binding, item, context) for item in page.items],
                next_after=page.next_after,
            )
        else:
            proposals = await self.store.missing_rollback_list(
                scope, after=request.after, limit=request.limit
            )
            for item in proposals.items:
                self.moves._bound(item.move, binding)
            data["proposals"] = proposals
        _, current = await self.moves.directories._query_guard(context)
        if current.manifest() != binding.manifest():
            raise ServiceError("workspace_binding_changed", "原回退查询期间资源改变", 403)
        self.budget(context)
        return MissingDirectoryRollbackQueryResult.model_validate(data)
