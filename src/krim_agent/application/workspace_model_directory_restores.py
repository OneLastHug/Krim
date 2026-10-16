"""目录恢复始终在原模型父Run/call内，未知回程只关闭匹配的原准入。"""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from threading import Event
from typing import TYPE_CHECKING, Any, Literal

from pydantic import JsonValue

from krim_agent.protocol.domain import ServiceError
from krim_agent.tools.core import ExecutionBudgetBase, ToolContext, ToolError, execution_budget_base
from krim_agent.tools.directory_restore import directory_restore_tools
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_directory_restore import (
    DirectoryRestoreExecute,
    DirectoryRestoreFence,
    DirectoryRestoreOperationView,
    DirectoryRestorePreview,
    DirectoryRestoreView,
    directory_restore_model_source,
)
from krim_agent.workspace_directory_restore_control import (
    DIRECTORY_RESTORE_STEP_TOOLS,
    DirectoryRestoreStepInput,
    DirectoryRestoreStepKind,
)
from krim_agent.workspace_directory_restore_model import (
    DirectoryRestoreLookup,
    DirectoryRestoreOperationLookup,
    DirectoryRestoreQuery,
    DirectoryRestoreQueryResult,
)
from krim_agent.workspace_directory_restore_public import directory_restore_diff_page

from .workspace_changes import _wait
from .workspace_directory_restores import (
    DirectoryRestoreInvocation,
    WorkspaceDirectoryRestoreService,
)

if TYPE_CHECKING:
    from .workspace_model_checkpoints import ModelCheckpointService


class ModelDirectoryRestoreService:
    def __init__(
        self, parent: ModelCheckpointService, restores: WorkspaceDirectoryRestoreService
    ) -> None:
        self.parent, self.restores, self.store = parent, restores, restores.store

    async def _guard(
        self, context: ToolContext, base: ExecutionBudgetBase, *, writing: bool
    ) -> tuple[str, WorkspaceBinding]:
        if context.workspace_directory_restores is not self.restores:
            raise ServiceError(
                "workspace_directory_restore_model_context", "目录模型恢复缺少原Backend绑定", 403
            )
        return await self.parent._guard(context, base, writing=writing)

    async def _lookup(self, scope: str, request: DirectoryRestoreLookup) -> DirectoryRestoreView:
        if request.plan_id is not None:
            return await self.store.get(scope, request.plan_id)
        assert request.request_key is not None
        value = await self.store.by_key(scope, request.request_key)
        if value is None:
            raise ServiceError(
                "workspace_directory_restore_not_found", "原目录恢复预览键不存在", 404
            )
        return value

    async def _operation(
        self, scope: str, request: DirectoryRestoreOperationLookup
    ) -> DirectoryRestoreOperationView:
        if request.operation_id is not None:
            return await self.store.operation(scope, request.operation_id)
        assert request.request_key is not None
        value = await self.store.operation_by_key(scope, request.request_key)
        if value is None:
            raise ServiceError(
                "workspace_directory_restore_operation_not_found", "原目录execute键不存在", 404
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
        request: DirectoryRestorePreview | DirectoryRestoreExecute | str,
    ) -> DirectoryRestoreInvocation:
        original = self.parent._invocation(context, source, base, scope, binding)
        assert original.qualify is not None

        def qualify() -> None:
            if context.workspace_directory_restores is not self.restores:
                raise ServiceError(
                    "workspace_directory_restore_model_context", "原目录恢复Backend被替换", 403
                )
            assert original.qualify is not None
            original.qualify()

        async def authorize(
            agent_id: str, phase: DirectoryRestoreStepKind
        ) -> tuple[str, WorkspaceBinding]:
            qualify()
            if phase != kind:
                raise ServiceError(
                    "workspace_directory_restore_model_context", "原目录父调用不能跨生产阶段", 403
                )
            return await original.authorize(agent_id)

        async def call(agent_id: str, fence: DirectoryRestoreFence, confirmed: bool) -> object:
            await authorize(agent_id, fence.kind)
            if confirmed or fence.model_source != source or fence.kind != kind:
                raise ServiceError(
                    "workspace_directory_restore_model_context", "目录叶未保留原父来源/阶段", 403
                )
            if type(request) is DirectoryRestorePreview:
                planned = await self.store.by_key(scope, request.request_key)
                valid = planned is not None and planned.plan_id == fence.plan_id
            elif type(request) is DirectoryRestoreExecute:
                valid = request.plan_id == fence.plan_id
            else:
                valid = request == fence.operation_id
            if not valid:
                raise ServiceError(
                    "workspace_directory_restore_model_context", "目录叶计划/操作被重定向", 403
                )
            # 两层私有副本，Hook不能原地改坏worker的token或原模型来源。
            leaf_fence = replace(fence, model_source=replace(source))
            child = replace(
                context,
                directory_restore=leaf_fence,
                directory_restore_qualify=qualify,
                source_call_id=source.call_id,
                workspace_directory_restores=self.restores,
            )
            response = await child.registry.invoke(
                DIRECTORY_RESTORE_STEP_TOOLS[kind],
                DirectoryRestoreStepInput(
                    plan_id=fence.plan_id, operation_id=fence.operation_id
                ).model_dump(),
                child,
            )
            if response.is_error:
                raise ServiceError(
                    response.code or "workspace_directory_restore_unverified",
                    "目录叶未通过原工具链",
                    409,
                )
            return response.data

        return DirectoryRestoreInvocation(source, authorize, call, qualify)

    def _owned_worker(
        self,
        scope: str,
        view: DirectoryRestoreView,
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
        request: DirectoryRestorePreview | DirectoryRestoreExecute | str,
        context: ToolContext,
        source: CheckpointModelSource,
        base: ExecutionBudgetBase,
        scope: str,
        binding: WorkspaceBinding,
    ) -> DirectoryRestoreView:
        assert context.agent_id is not None
        kind: DirectoryRestoreStepKind = (
            "prepare" if operation == "preview" else "execute" if operation == "start" else "verify"
        )
        expected = {item.name: item for item in directory_restore_tools()}[
            DIRECTORY_RESTORE_STEP_TOOLS[kind]
        ]
        try:
            if context.registry.get(expected.name) is not expected:
                raise ToolError("workspace_directory_restore_unavailable", "原目录恢复叶定义不匹配")
        except ToolError:
            raise ServiceError(
                "workspace_directory_restore_unavailable", "原冻结Agent未授此目录恢复叶", 403
            ) from None
        invocation = self._invocation(context, source, base, scope, binding, kind, request)
        if type(request) is DirectoryRestorePreview:
            admission = self.restores.preview_model(context.agent_id, request, invocation)
        elif type(request) is DirectoryRestoreExecute:
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

    async def _cancel(self, scope: str, view: DirectoryRestoreView) -> DirectoryRestoreView:
        entry = self.restores._tasks.get(view.operation_id)
        abort = self.restores._aborts.get(view.operation_id)

        async def stop() -> DirectoryRestoreView:
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
                DirectoryRestorePreview.model_validate(payload),
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
                DirectoryRestoreExecute.model_validate(payload),
                context,
                source,
                base,
                scope,
                binding,
            )
            value = result.model_dump(mode="json")
        elif operation == "verify":
            original = await self._operation(
                scope, DirectoryRestoreOperationLookup.model_validate(payload)
            )
            result = await self._operate(
                "verify", original.operation_id, context, source, base, scope, binding
            )
            value = result.model_dump(mode="json")
        elif operation == "cancel":
            result = await self._lookup(scope, DirectoryRestoreLookup.model_validate(payload))
            value = (await self._cancel(scope, result)).model_dump(mode="json")
        elif operation == "query":
            query = DirectoryRestoreQuery.model_validate(payload)
            selected: dict[str, Any] = {"kind": query.kind}
            if query.kind == "history":
                selected["entries"] = await self.store.list(
                    scope, offset=query.offset, limit=query.limit
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
                        selected["diff"] = directory_restore_diff_page(
                            planned, entries, query.offset, query.limit
                        )
            elif query.kind == "operation":
                assert query.operation_id is not None
                selected["operation"] = await self.store.operation(scope, query.operation_id)
            else:
                assert query.request_key is not None
                selected["operation"] = await self.store.operation_by_key(scope, query.request_key)
            value = DirectoryRestoreQueryResult.model_validate(selected).model_dump(mode="json")
        else:
            raise ServiceError(
                "workspace_directory_restore_model_operation", "模型目录恢复动作无效", 422
            )
        await self._guard(context, base, writing=io)
        context._checkpoint_control_response = (
            "directory_restore_" + operation,
            json.dumps(value, sort_keys=True),
        )
        return value
