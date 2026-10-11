"""模型混合批次在原工具调用内执行，继承原context/权限/审批/deadline。"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, replace
from typing import Any
from uuid import uuid4

from pydantic import JsonValue

from krim_agent.persistence.workspace_mixed_batches import MixedBatchStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.tools.core import ToolContext, ToolError
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_mixed_batch import (
    MixedBatchModelSource,
    MixedBatchRequest,
    MixedBatchView,
)

from .workspace_changes import WorkspaceChangeService, _wait
from .workspace_directories import DirectoryChangeService
from .workspace_execution import (
    enter_workspace_execution,
    inside_workspace_execution,
    leave_workspace_execution,
)


@dataclass
class _Invocation:
    request: MixedBatchRequest
    context: ToolContext
    batch_id: str | None = None
    token: str | None = None
    result: MixedBatchView | None = None
    created: bool = False


class ModelMixedBatchService:
    def __init__(
        self, changes: WorkspaceChangeService, directories: DirectoryChangeService
    ) -> None:
        self.changes = changes
        self.store = MixedBatchStore(changes.store, directories.store)
        self._tasks: dict[str, tuple[str, asyncio.Task[None]]] = {}
        self._closed = False

    async def _cancel_parent(self, scope: str, state: _Invocation) -> None:
        """先关闭持久父准入，再等待已经被接纳的原子动作收束。"""
        if not state.created or state.batch_id is None:
            return
        state.result = await self.store.cancel(scope, state.batch_id)

    async def tool_start(
        self, payload: dict[str, Any], context: ToolContext
    ) -> dict[str, JsonValue]:
        try:
            if self._closed:
                raise ServiceError("workspace_mixed_model_closed", "模型混合批次服务已关闭", 503)
            if (
                context.origin != "model"
                or context.run_id == "local"
                or context.session_id is None
                or context.agent_id is None
            ):
                raise ServiceError(
                    "workspace_mixed_model_context", "模型批次需要原Agent/session/Run/call", 403
                )
            if len(self._tasks) >= 64:
                raise ServiceError("workspace_mixed_batch_busy", "模型批次调用已达上限", 429)
            scope, binding = await self.changes._guard(context)
            state = _Invocation(MixedBatchRequest.model_validate(payload), context)
            identifier = str(uuid4())
            task = asyncio.create_task(self._run(identifier, state, scope, binding))
            self._tasks[identifier] = (scope, task)

            def stop() -> None:
                if not task.done() and not task.cancelling():
                    task.cancel()

            try:
                await _wait(task, stop)
                assert state.result is not None
                value = state.result.model_dump(mode="json")
                context._mixed_write_response = json.dumps(value, sort_keys=True)
                return value
            finally:
                self._tasks.pop(identifier, None)
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error

    async def _renew(self, state: _Invocation, worker: asyncio.Task[None]) -> None:
        assert state.batch_id is not None and state.token is not None
        try:
            while True:
                await asyncio.sleep(max(0.02, min(0.2, self.store.lease_seconds / 4)))
                await self.store.renew(state.batch_id, state.token)
                await self.changes._guard(state.context)
        except asyncio.CancelledError:
            raise
        except Exception:
            if not worker.done() and not worker.cancelling():
                worker.cancel()

    async def _run(
        self, identifier: str, state: _Invocation, scope: str, binding: WorkspaceBinding
    ) -> None:
        worker = asyncio.current_task()
        assert worker is not None
        marker = enter_workspace_execution(self._tasks, identifier)
        heartbeat = None
        cancelled = False
        accept = None
        try:

            async def accept_request() -> tuple[MixedBatchView, bool]:
                value, created = await self.store.reserve(
                    scope,
                    binding.key,
                    binding.fingerprint,
                    state.request,
                    model_source=MixedBatchModelSource(state.context.run_id, state.context.call_id),
                )
                state.batch_id, state.result, state.created = value.batch_id, value, created
                return value, created

            accept = asyncio.create_task(accept_request())
            try:
                view, created = await asyncio.shield(accept)
            except asyncio.CancelledError:
                view, created = await _wait(accept, lambda: None)
                state.batch_id, state.result = view.batch_id, view
                cancelled = True
                raise
            state.batch_id, state.result = view.batch_id, view
            if not created:
                return
            execution = await self.store.execution(scope, view.batch_id)
            state.token = execution.token
            heartbeat = asyncio.create_task(self._renew(state, worker))
            for child in execution.children:
                await self.changes._guard(state.context)
                nested = replace(
                    state.context,
                    mixed_batch=child.fence,
                    source_call_id=state.context.call_id,
                )
                name = "workspace_change" if child.fence.source == "file" else "directory_change"
                child_task = asyncio.create_task(
                    nested.registry.invoke(name, child.request.model_dump(), nested)
                )
                try:
                    result = await asyncio.shield(child_task)
                except asyncio.CancelledError:
                    cancelled = True
                    # 取消可能发生在原Writer已经取得资格之后；父账本必须先关闭，
                    # 之后才等待原文件/目录任务写入其真实回执。
                    await self._cancel_parent(scope, state)
                    child_task.cancel()

                    def cancel_child(task: asyncio.Task[Any] = child_task) -> None:
                        task.cancel()

                    try:
                        await _wait(child_task, cancel_child)
                    except asyncio.CancelledError:
                        pass
                    raise
                await self.store.finish_child(
                    scope, child.fence, error_code=result.code if result.is_error else None
                )
                if (await self.store.get(scope, view.batch_id)).producer_status != "running":
                    break
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            try:
                if heartbeat is not None:
                    heartbeat.cancel()
                    await asyncio.gather(heartbeat, return_exceptions=True)
                if (
                    state.created
                    and state.batch_id is not None
                    and state.result is not None
                    and state.result.producer_status == "running"
                ):
                    if cancelled or state.token is None:
                        await self._cancel_parent(scope, state)
                    else:
                        try:
                            state.result = await _wait(
                                asyncio.create_task(
                                    self.store.finish(scope, state.batch_id, state.token)
                                ),
                                lambda: None,
                            )
                        except ServiceError:
                            state.result = await _wait(
                                asyncio.create_task(self.store.cancel(scope, state.batch_id)),
                                lambda: None,
                            )
            finally:
                leave_workspace_execution(marker)

    async def validate_start(self, value: Any, context: ToolContext) -> None:
        try:
            parsed = MixedBatchView.model_validate_json(json.dumps(value)).model_dump(mode="json")
            if (
                context._mixed_write_response is None
                or json.dumps(parsed, sort_keys=True) != context._mixed_write_response
            ):
                raise ValueError()
        except (TypeError, ValueError):
            raise ToolError(
                "workspace_mixed_batch_write_unverified", "模型批次回程未关联本次原账本"
            ) from None

    def assert_external_close(self) -> None:
        if inside_workspace_execution():
            raise ServiceError(
                "workspace_mixed_model_reentrant", "模型批次回调不能关闭当前执行服务", 409
            )

    async def close(self) -> None:
        self.assert_external_close()
        self._closed = True
        tasks = [task for _scope, task in self._tasks.values()]
        for task in tasks:
            if not task.done() and not task.cancelling():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
