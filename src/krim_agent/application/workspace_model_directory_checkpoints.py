"""复用原模型控制资格，目录生产者始终留在原父Run/call与预算内。"""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from pydantic import JsonValue

from krim_agent.protocol.domain import ServiceError
from krim_agent.tools.core import ExecutionBudgetBase, ToolContext, ToolError, execution_budget_base
from krim_agent.tools.directory_checkpoint_capture import directory_checkpoint_capture_tool
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_checkpoint_control import CheckpointLookup, CheckpointQuery
from krim_agent.workspace_directory_checkpoint import (
    DirectoryCheckpointCaptureFence,
    DirectoryCheckpointCreate,
    DirectoryCheckpointView,
    directory_checkpoint_model_source,
)
from krim_agent.workspace_directory_checkpoint_control import DirectoryCheckpointQueryResult

from .workspace_changes import _wait
from .workspace_directory_checkpoints import (
    DirectoryCheckpointInvocation,
    WorkspaceDirectoryCheckpointService,
)

if TYPE_CHECKING:
    from .workspace_model_checkpoints import ModelCheckpointService


class ModelDirectoryCheckpointService:
    def __init__(
        self, parent: ModelCheckpointService, directories: WorkspaceDirectoryCheckpointService
    ) -> None:
        self.parent, self.directories, self.store = parent, directories, directories.store

    @staticmethod
    async def _drain(worker: asyncio.Task[None]) -> None:
        # 子生产者取消是原任务事实，不能冒充父模型Task的取消。
        await asyncio.gather(worker, return_exceptions=True)

    async def _guard(
        self, context: ToolContext, base: ExecutionBudgetBase, *, writing: bool
    ) -> tuple[str, WorkspaceBinding]:
        if context.workspace_directory_checkpoints is not self.directories:
            raise ServiceError(
                "workspace_directory_capture_model_context", "目录模型控制缺少原Backend绑定", 403
            )
        return await self.parent._guard(context, base, writing=writing)

    async def _lookup(self, scope: str, request: CheckpointLookup) -> DirectoryCheckpointView:
        if request.checkpoint_id is not None:
            return await self.store.get(scope, request.checkpoint_id)
        assert request.request_key is not None
        value = await self.store.by_key(scope, request.request_key)
        if value is None:
            raise ServiceError("workspace_directory_capture_not_found", "原目录捕获键不存在", 404)
        return value

    def _invocation(
        self,
        context: ToolContext,
        source: CheckpointModelSource,
        base: ExecutionBudgetBase,
        scope: str,
        binding: WorkspaceBinding,
    ) -> DirectoryCheckpointInvocation:
        original = self.parent._invocation(context, source, base, scope, binding)
        assert original.qualify is not None

        def qualify() -> None:
            if context.workspace_directory_checkpoints is not self.directories:
                raise ServiceError(
                    "workspace_directory_capture_model_context", "原目录控制Backend被替换", 403
                )
            assert original.qualify is not None
            original.qualify()

        async def authorize(agent_id: str) -> tuple[str, WorkspaceBinding]:
            qualify()
            return await original.authorize(agent_id)

        async def read(
            agent_id: str, root_path: str, fence: DirectoryCheckpointCaptureFence, confirmed: bool
        ) -> object:
            await authorize(agent_id)
            if confirmed or fence.model_source != source:
                raise ServiceError(
                    "workspace_directory_capture_model_context", "目录捕获未保留原父调用", 403
                )
            child = replace(
                context,
                directory_checkpoint_capture=fence,
                directory_checkpoint_qualify=qualify,
                source_call_id=source.call_id,
                workspace_directory_checkpoints=self.directories,
            )
            response = await child.registry.invoke(
                "workspace_directory_checkpoint_capture", {"root_path": root_path}, child
            )
            if response.is_error:
                raise ServiceError(
                    response.code or "workspace_directory_capture_unverified",
                    "目录读取未通过原工具链",
                    409,
                )
            return response.data

        return DirectoryCheckpointInvocation(source, authorize, read, qualify)

    async def _create(
        self,
        request: DirectoryCheckpointCreate,
        context: ToolContext,
        source: CheckpointModelSource,
        base: ExecutionBudgetBase,
        scope: str,
        binding: WorkspaceBinding,
    ) -> DirectoryCheckpointView:
        assert context.agent_id is not None
        try:
            leaf = context.registry.get("workspace_directory_checkpoint_capture")
        except ToolError:
            raise ServiceError(
                "workspace_directory_capture_unavailable", "原冻结Agent未授目录捕获叶能力", 403
            ) from None
        if leaf is not directory_checkpoint_capture_tool():
            raise ServiceError(
                "workspace_directory_capture_unavailable", "原目录捕获定义身份不匹配", 403
            )
        invocation = self._invocation(context, source, base, scope, binding)
        task = asyncio.create_task(
            self.directories.create_model(context.agent_id, request, invocation)
        )
        view = None
        joining: asyncio.Task[None] | None = None
        try:
            try:
                view = await asyncio.shield(task)
            except asyncio.CancelledError:
                view = await _wait(task, lambda: None)
                raise
            while view.status == "capturing":
                await invocation.authorize(context.agent_id)
                entry = self.directories._tasks.get(view.checkpoint_id)
                if entry is not None and entry[0] == scope:
                    joining = asyncio.create_task(self._drain(entry[1]))
                    await asyncio.shield(joining)
                else:
                    await asyncio.sleep(0.02)
                view = await self.store.get(scope, view.checkpoint_id)
                if entry is not None and entry[1].done() and view.status == "capturing":
                    view = await self.directories.cancel(scope, view.checkpoint_id)
            return view
        except BaseException:
            if view is None:
                candidate = await self.store.by_key(scope, request.request_key)
                if (
                    candidate is not None
                    and await self.store.source(scope, candidate.checkpoint_id) == source
                ):
                    view = candidate
            if view is not None:
                await _wait(
                    asyncio.create_task(self.directories.cancel(scope, view.checkpoint_id)),
                    lambda: None,
                )
                entry = self.directories._tasks.get(view.checkpoint_id)
                if joining is not None:
                    await _wait(joining, lambda: None)
                elif entry is not None and entry[0] == scope:
                    await _wait(asyncio.create_task(self._drain(entry[1])), lambda: None)
            raise

    async def tool_operation(
        self, operation: str, payload: dict[str, Any], context: ToolContext
    ) -> dict[str, JsonValue]:
        try:
            source = directory_checkpoint_model_source(self.parent._source(context))
            assert source is not None
            base = execution_budget_base(context.execution_timeouts)
            scope, binding = await self._guard(context, base, writing=operation == "create")
            if operation == "create":
                result = await self._create(
                    DirectoryCheckpointCreate.model_validate(payload),
                    context,
                    source,
                    base,
                    scope,
                    binding,
                )
                value = result.model_dump(mode="json")
            elif operation == "cancel":
                original = await self._lookup(scope, CheckpointLookup.model_validate(payload))
                result = await self.directories.cancel(scope, original.checkpoint_id)
                entry = self.directories._tasks.get(original.checkpoint_id)
                if entry is not None and entry[0] == scope:
                    await _wait(asyncio.create_task(self._drain(entry[1])), lambda: None)
                value = (await self.store.get(scope, result.checkpoint_id)).model_dump(mode="json")
            elif operation == "query":
                query = CheckpointQuery.model_validate(payload)
                page = DirectoryCheckpointQueryResult(kind=query.kind)
                if query.kind == "history":
                    page.entries = await self.store.list(
                        scope, offset=query.offset, limit=query.limit
                    )
                elif query.kind == "id":
                    assert query.checkpoint_id is not None
                    page.checkpoint = await self.store.get(scope, query.checkpoint_id)
                else:
                    assert query.request_key is not None
                    page.checkpoint = await self.store.by_key(scope, query.request_key)
                value = page.model_dump(mode="json")
            else:
                raise ServiceError(
                    "workspace_directory_capture_model_operation", "目录模型控制动作无效", 422
                )
            await self._guard(context, base, writing=operation == "create")
            context._checkpoint_control_response = (
                "directory_" + operation,
                json.dumps(value, sort_keys=True),
            )
            return value
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error
