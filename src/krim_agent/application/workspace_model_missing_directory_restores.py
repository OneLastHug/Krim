"""缺失目录恢复始终在原模型父Run/call内，未知回程只关闭匹配的原准入。"""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from threading import Event
from typing import TYPE_CHECKING, Any, Literal

from pydantic import JsonValue

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StreamFrame
from krim_agent.tools.core import (
    ExecutionBudgetBase,
    TextContent,
    ToolContext,
    ToolError,
    ToolResult,
    execution_budget_base,
)
from krim_agent.tools.missing_directory_restore import missing_directory_restore_tools
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_directory_restore import directory_restore_model_source
from krim_agent.workspace_directory_restore_control import DirectoryRestoreStepKind
from krim_agent.workspace_missing_directory_restore import (
    MissingDirectoryRestoreExecute,
    MissingDirectoryRestoreFence,
    MissingDirectoryRestoreOperationView,
    MissingDirectoryRestorePreview,
    MissingDirectoryRestoreView,
)
from krim_agent.workspace_missing_directory_restore_control import (
    MISSING_DIRECTORY_RESTORE_STEP_TOOLS,
    MissingDirectoryRestoreStepInput,
)
from krim_agent.workspace_missing_directory_restore_model import (
    MissingDirectoryRestoreLookup,
    MissingDirectoryRestoreOperationLookup,
    MissingDirectoryRestoreQuery,
    MissingDirectoryRestoreQueryResult,
    bounded_missing_directory_restore_result,
    missing_directory_restore_diff_page,
)

from .workspace_changes import _wait
from .workspace_missing_directory_restores import (
    MissingDirectoryRestoreInvocation,
    WorkspaceMissingDirectoryRestoreService,
)

if TYPE_CHECKING:
    from .workspace_model_checkpoints import ModelCheckpointService


class ModelMissingDirectoryRestoreService:
    def __init__(
        self, parent: ModelCheckpointService, restores: WorkspaceMissingDirectoryRestoreService
    ) -> None:
        self.parent, self.restores, self.store = parent, restores, restores.store

    async def _guard(
        self, context: ToolContext, base: ExecutionBudgetBase, *, writing: bool
    ) -> tuple[str, WorkspaceBinding]:
        if context.workspace_missing_directory_restores is not self.restores:
            raise ServiceError(
                "workspace_missing_directory_restore_model_context",
                "缺失目录模型恢复缺少原Backend绑定",
                403,
            )
        return await self.parent._guard(context, base, writing=writing)

    async def _lookup(
        self, scope: str, request: MissingDirectoryRestoreLookup
    ) -> MissingDirectoryRestoreView:
        if request.plan_id is not None:
            return await self.store.get(scope, request.plan_id)
        assert request.request_key is not None
        value = await self.store.by_key(scope, request.request_key)
        if value is None:
            raise ServiceError(
                "workspace_missing_directory_restore_not_found", "原缺失目录恢复预览键不存在", 404
            )
        return value

    async def _operation(
        self, scope: str, request: MissingDirectoryRestoreOperationLookup
    ) -> MissingDirectoryRestoreOperationView:
        if request.operation_id is not None:
            return await self.store.operation(scope, request.operation_id)
        assert request.request_key is not None
        value = await self.store.operation_by_key(scope, request.request_key)
        if value is None:
            raise ServiceError(
                "workspace_missing_directory_restore_operation_not_found",
                "原缺失目录execute键不存在",
                404,
            )
        return value

    @staticmethod
    async def _drain(worker: asyncio.Task[None]) -> None:
        await asyncio.gather(worker, return_exceptions=True)

    def _invocation(
        self,
        context: ToolContext,
        source: CheckpointModelSource,
        base: ExecutionBudgetBase,
        scope: str,
        binding: WorkspaceBinding,
        kind: DirectoryRestoreStepKind,
        request: MissingDirectoryRestorePreview | MissingDirectoryRestoreExecute | str,
    ) -> MissingDirectoryRestoreInvocation:
        original = self.parent._invocation(context, source, base, scope, binding)
        assert original.qualify is not None

        def qualify() -> None:
            if context.workspace_missing_directory_restores is not self.restores:
                raise ServiceError(
                    "workspace_missing_directory_restore_model_context",
                    "原缺失目录恢复Backend被替换",
                    403,
                )
            assert original.qualify is not None
            original.qualify()

        async def authorize(
            agent_id: str, phase: DirectoryRestoreStepKind
        ) -> tuple[str, WorkspaceBinding]:
            qualify()
            if phase != kind:
                raise ServiceError(
                    "workspace_missing_directory_restore_model_context",
                    "原缺失目录父调用不能跨生产阶段",
                    403,
                )
            return await original.authorize(agent_id)

        async def call(
            agent_id: str, fence: MissingDirectoryRestoreFence, confirmed: bool
        ) -> object:
            await authorize(agent_id, fence.kind)
            if confirmed or fence.model_source != source or fence.kind != kind:
                raise ServiceError(
                    "workspace_missing_directory_restore_model_context",
                    "缺失目录叶未保留原父来源/阶段",
                    403,
                )
            if type(request) is MissingDirectoryRestorePreview:
                planned = await self.store.by_key(scope, request.request_key)
                valid = planned is not None and planned.plan_id == fence.plan_id
            elif type(request) is MissingDirectoryRestoreExecute:
                valid = request.plan_id == fence.plan_id
            else:
                valid = request == fence.operation_id
            if not valid:
                raise ServiceError(
                    "workspace_missing_directory_restore_model_context",
                    "缺失目录叶计划/操作被重定向",
                    403,
                )
            # 两层私有副本，Hook不能原地改坏worker的token或原模型来源。
            leaf_fence = replace(fence, model_source=replace(source))
            child = replace(
                context,
                directory_restore=leaf_fence,
                directory_restore_qualify=qualify,
                source_call_id=source.call_id,
                workspace_directory_restores=self.restores,
                workspace_missing_directory_restores=self.restores,
            )
            response = await child.registry.invoke(
                MISSING_DIRECTORY_RESTORE_STEP_TOOLS[kind],
                MissingDirectoryRestoreStepInput(
                    plan_id=fence.plan_id, operation_id=fence.operation_id
                ).model_dump(),
                child,
            )
            if response.is_error:
                raise ServiceError(
                    response.code or "workspace_missing_directory_restore_unverified",
                    "缺失目录叶未通过原工具链",
                    409,
                )
            return response.data

        return MissingDirectoryRestoreInvocation(source, authorize, call, qualify)

    def _owned_worker(
        self,
        scope: str,
        view: MissingDirectoryRestoreView,
        kind: DirectoryRestoreStepKind,
        source: CheckpointModelSource,
    ) -> tuple[asyncio.Task[None], Event | None] | None:
        entry = self.restores._tasks.get(view.operation_id)
        fence = self.restores._fences.get(view.operation_id)
        if (
            entry is None
            or entry[0] != scope
            or fence is None
            or fence.kind != kind
            or fence.model_source != source
        ):
            return None
        return entry[1], self.restores._aborts.get(view.operation_id)

    async def _operate(
        self,
        operation: Literal["preview", "start", "verify"],
        request: MissingDirectoryRestorePreview | MissingDirectoryRestoreExecute | str,
        context: ToolContext,
        source: CheckpointModelSource,
        base: ExecutionBudgetBase,
        scope: str,
        binding: WorkspaceBinding,
    ) -> MissingDirectoryRestoreView:
        assert context.agent_id is not None
        kind: DirectoryRestoreStepKind = (
            "prepare" if operation == "preview" else "execute" if operation == "start" else "verify"
        )
        expected = {item.name: item for item in missing_directory_restore_tools()}[
            MISSING_DIRECTORY_RESTORE_STEP_TOOLS[kind]
        ]
        try:
            if context.registry.get(expected.name) is not expected:
                raise ToolError(
                    "workspace_missing_directory_restore_unavailable", "原缺失目录恢复叶定义不匹配"
                )
        except ToolError:
            raise ServiceError(
                "workspace_missing_directory_restore_unavailable",
                "原冻结Agent未授此目录恢复叶",
                403,
            ) from None
        invocation = self._invocation(context, source, base, scope, binding, kind, request)
        if type(request) is MissingDirectoryRestorePreview:
            admission = self.restores.preview_model(context.agent_id, request, invocation)
        elif type(request) is MissingDirectoryRestoreExecute:
            admission = self.restores.start_model(context.agent_id, request, invocation)
        else:
            assert isinstance(request, str)
            admission = self.restores.verify_model(context.agent_id, request, invocation)
        task = asyncio.create_task(admission)
        view = None
        joining: asyncio.Task[None] | None = None
        own = None
        try:
            try:
                view = await asyncio.shield(task)
            except asyncio.CancelledError:
                view = await _wait(task, lambda: None)
                raise
            while view.phase in {"preparing", "executing"}:
                await invocation.authorize(context.agent_id, kind)
                own = self._owned_worker(scope, view, kind, source)
                if own is not None:
                    joining = asyncio.create_task(self._drain(own[0]))
                    await asyncio.shield(joining)
                else:
                    await asyncio.sleep(0.02)
                view = await self.store.get(scope, view.plan_id)
                if own is not None and own[0].done() and view.phase in {"preparing", "executing"}:
                    phase: Literal["preview", "execute", "verify"] = (
                        "preview" if kind == "prepare" else kind
                    )
                    candidate = await self.store.cancel_model_admission(
                        scope, source, phase, request
                    )
                    if candidate is not None:
                        view = candidate
            return view
        except BaseException:

            async def close_admission_and_worker() -> None:
                owned = own
                if owned is None:
                    for identifier, bound in tuple(self.restores._invocations.items()):
                        entry = self.restores._tasks.get(identifier)
                        if bound is invocation and entry is not None and entry[0] == scope:
                            owned = (entry[1], self.restores._aborts.get(identifier))
                            break
                try:
                    phase: Literal["preview", "execute", "verify"] = (
                        "preview" if kind == "prepare" else kind
                    )
                    candidate = await self.store.cancel_model_admission(
                        scope, source, phase, request
                    )
                    if candidate is not None and owned is None:
                        owned = self._owned_worker(scope, candidate, kind, source)
                except Exception:
                    # DB回程未知也不能重放；已知自己的本地worker仍须收束。
                    pass
                finally:
                    if owned is not None:
                        if owned[1] is not None:
                            owned[1].set()
                        if not owned[0].done() and not owned[0].cancelling():
                            owned[0].cancel()
                        await self._drain(owned[0])
                    elif joining is not None:
                        await joining

            await _wait(asyncio.create_task(close_admission_and_worker()), lambda: None)
            raise

    async def _cancel(
        self, scope: str, view: MissingDirectoryRestoreView
    ) -> MissingDirectoryRestoreView:
        entry = self.restores._tasks.get(view.operation_id)
        abort = self.restores._aborts.get(view.operation_id)

        async def stop() -> MissingDirectoryRestoreView:
            try:
                await self.restores.cancel(scope, view.plan_id)
            finally:
                if entry is not None and entry[0] == scope:
                    if abort is not None:
                        abort.set()
                    if not entry[1].done() and not entry[1].cancelling():
                        entry[1].cancel()
                    await self._drain(entry[1])
            return await self.store.get(scope, view.plan_id)

        return await _wait(asyncio.create_task(stop()), lambda: None)

    async def tool_operation(
        self, operation: str, payload: dict[str, Any], context: ToolContext
    ) -> dict[str, JsonValue]:
        source = directory_restore_model_source(self.parent._source(context))
        assert source is not None
        base = execution_budget_base(context.execution_timeouts)
        io = operation in {"preview", "start", "verify"}
        scope, binding = await self._guard(context, base, writing=io)
        if operation == "preview":
            result = await self._operate(
                "preview",
                MissingDirectoryRestorePreview.model_validate(payload),
                context,
                source,
                base,
                scope,
                binding,
            )
            value = result.model_dump(mode="json")
        elif operation == "start":
            result = await self._operate(
                "start",
                MissingDirectoryRestoreExecute.model_validate(payload),
                context,
                source,
                base,
                scope,
                binding,
            )
            value = result.model_dump(mode="json")
        elif operation == "verify":
            original = await self._operation(
                scope, MissingDirectoryRestoreOperationLookup.model_validate(payload)
            )
            result = await self._operate(
                "verify", original.operation_id, context, source, base, scope, binding
            )
            value = result.model_dump(mode="json")
        elif operation == "cancel":
            result = await self._lookup(
                scope, MissingDirectoryRestoreLookup.model_validate(payload)
            )
            value = (await self._cancel(scope, result)).model_dump(mode="json")
        elif operation == "query":
            query = MissingDirectoryRestoreQuery.model_validate(payload)
            selected: dict[str, Any] = {"kind": query.kind}
            if query.kind == "history":
                selected["entries"] = await self.store.list(
                    scope, offset=query.offset, limit=query.limit
                )
                selected["offset"], selected["limit"] = query.offset, query.limit
                following = query.offset + len(selected["entries"])
                selected["next_offset"] = (
                    following
                    if len(selected["entries"]) == query.limit
                    and await self.store.list(scope, offset=following, limit=1)
                    else None
                )
            elif query.kind in {"id", "key", "diff"}:
                if query.kind == "key":
                    assert query.request_key is not None
                    selected["plan"] = await self.store.by_key(scope, query.request_key)
                else:
                    assert query.plan_id is not None
                    planned = await self.store.get(scope, query.plan_id)
                    if query.kind == "id":
                        selected["plan"] = planned
                    else:
                        entries = await self.store.diff(
                            scope, query.plan_id, offset=query.offset, limit=query.limit
                        )
                        selected["diff"] = missing_directory_restore_diff_page(
                            planned, entries, query.offset, query.limit
                        )
            elif query.kind == "operation":
                assert query.operation_id is not None
                selected["operation"] = await self.store.operation(scope, query.operation_id)
            else:
                assert query.request_key is not None
                selected["operation"] = await self.store.operation_by_key(scope, query.request_key)

            def validate_envelope(page: dict[str, JsonValue]) -> None:
                normalized = ToolResult(
                    content=[
                        TextContent(text=json.dumps(page, ensure_ascii=False, allow_nan=False))
                    ],
                    data=page,
                )
                StreamFrame(
                    seq=0,
                    kind="chunk",
                    data={
                        "type": "tool_result",
                        "call_id": context.call_id,
                        "name": "workspace_missing_directory_checkpoint_restore_query",
                        **normalized.model_dump(mode="json"),
                    },
                )

            value = bounded_missing_directory_restore_result(
                MissingDirectoryRestoreQueryResult.model_validate(selected),
                validate_envelope=validate_envelope,
            ).model_dump(mode="json")
        else:
            raise ServiceError(
                "workspace_missing_directory_restore_model_operation", "模型目录恢复动作无效", 422
            )
        await self._guard(context, base, writing=io)
        context._checkpoint_control_response = (
            "missing_directory_restore_" + operation,
            json.dumps(value, sort_keys=True),
        )
        return value
