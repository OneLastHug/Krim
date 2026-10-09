"""Agent命令编排：同Registry授权完整输入，原任务查询与取消，不发布原文件。"""

from __future__ import annotations

import asyncio
import base64
import copy
import json
from contextvars import ContextVar
from dataclasses import dataclass, replace
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, JsonValue

from krim_agent.execution.agent_models import (
    AgentCommandRequest,
    CommandLookup,
    CommandRead,
    CommandSlice,
    CommandSummary,
)
from krim_agent.execution.binding import CommandCapture, ExecutionBinding
from krim_agent.execution.linux import _settle
from krim_agent.execution.models import SandboxFile
from krim_agent.execution.records import CommandRequest, CommandView
from krim_agent.execution.service import DurableCommandHandle, DurableCommandService
from krim_agent.persistence.agent_sessions import assert_session_open, guard_agent_topic
from krim_agent.persistence.commands import CommandStore
from krim_agent.persistence.database import RunRow
from krim_agent.persistence.workspace_checkpoints import encode_capture
from krim_agent.protocol.domain import ServiceError
from krim_agent.tools.core import ToolContext, ToolError
from krim_agent.tools.workspace import ReadFileInput, workspace_io, workspace_tools
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_writer import CapturedFile, WorkspaceWriter

_current: ContextVar[asyncio.Task[Any] | None] = ContextVar("krim_agent_command", default=None)


async def _cleanup[T](task: asyncio.Task[T]) -> T:
    while True:
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.cancelled():
                raise


@dataclass
class _Capture:
    context: ToolContext
    workspace: WorkspaceBinding
    path: str
    scope: str
    run_id: str
    captured: CapturedFile | None = None
    expected: dict[str, object] | None = None


class AgentCommandService:
    def __init__(self, store: CommandStore) -> None:
        self.store = store
        self._services: dict[int, DurableCommandService] = {}
        self._operations: set[asyncio.Task[dict[str, JsonValue]]] = set()
        self._captures: dict[str, _Capture] = {}
        self._closed = False

    def assert_external_close(self) -> None:
        task = _current.get()
        if task is not None and not task.done():
            raise ServiceError("execution_command_reentrant", "命令回调不能关闭自身执行服务", 409)

    def _service(self, binding: ExecutionBinding) -> DurableCommandService:
        if id(binding) not in self._services:
            self._services[id(binding)] = DurableCommandService(
                self.store,
                binding.require(),
                key=binding.resource.key,
                version=binding.resource.version,
            )
        return self._services[id(binding)]

    async def _guard(
        self, context: ToolContext, *, history: bool = False
    ) -> tuple[str, ExecutionBinding, WorkspaceBinding | None]:
        current = _current.get()
        if current is not None and current.cancelling():
            raise ServiceError("execution_cancelled", "原命令调用已取消", 409)
        if self._closed or context.execution is None:
            raise ServiceError("execution_unbound", "当前会话没有可用的命令服务", 409)
        history = history and context.origin == "host" and context.run_id == "local"
        if not history:
            context.execution.require()
        if context.workspace is not None and not history:
            context.workspace.require()
        database = self.store.database
        async with database.write_lock, database.sessions() as session:
            row = await guard_agent_topic(
                session, context.topic_id, context.session_id, context.agent_id, self.store.owner_id
            )
            if row is not None:
                if not history:
                    await assert_session_open(session, row)
                if row.permissions.get("execution") != context.execution.resource.model_dump(
                    mode="json"
                ):
                    raise ServiceError("execution_binding_changed", "会话执行资源已变化", 403)
                if (
                    context.workspace is not None
                    and row.permissions.get("workspace") != context.workspace.manifest()
                ):
                    raise ServiceError("workspace_binding_changed", "会话工作区授权已变化", 403)
            if context.run_id != "local":
                run = await session.get(RunRow, context.run_id)
                if run is None or run.topic_id != context.topic_id:
                    raise ServiceError("execution_run_mismatch", "命令Run不属于当前话题", 403)
                if run.status not in {"queued", "running"} or run.stop_requested:
                    raise ServiceError("execution_run_closed", "命令所属Run已停止", 409)
            elif context.origin != "host":
                raise ServiceError("execution_run_mismatch", "模型命令需要原Run上下文", 403)
        scope = (
            f"session:{context.session_id}" if context.session_id else f"topic:{context.topic_id}"
        )
        return scope, context.execution, context.workspace

    async def tool_operation(
        self, operation: str, payload: dict[str, Any], context: ToolContext
    ) -> dict[str, JsonValue]:
        if context.command_capture is not None or context.checkpoint_capture is not None:
            raise ToolError("execution_input_context", "文件捕获上下文不能执行命令动作")
        if self._closed or len(self._operations) >= 64:
            raise ToolError("execution_busy", "命令服务已关闭或操作已达上限")

        async def work() -> dict[str, JsonValue]:
            marker = _current.set(asyncio.current_task())
            try:
                if operation == "execute":
                    result = await self._execute(
                        AgentCommandRequest.model_validate(payload), context
                    )
                else:
                    scope, binding, _ = await self._guard(
                        context, history=operation in {"get", "read"}
                    )
                    request = (
                        CommandRead.model_validate(payload)
                        if operation == "read"
                        else CommandLookup.model_validate(payload)
                    )
                    view = await self._lookup(scope, request)
                    if operation == "cancel":
                        view = await self._service(binding).stop_and_wait(scope, view.command_id)
                    if operation == "read":
                        assert isinstance(request, CommandRead)
                        result_slice = await self._read(scope, view, request)
                        data = result_slice.model_dump(mode="json")
                        context._command_response = (operation, json.dumps(data, sort_keys=True))
                        return data
                    result = await self._summary(scope, view)
                data = result.model_dump(mode="json")
                context._command_response = (operation, json.dumps(data, sort_keys=True))
                return data
            except ServiceError as error:
                raise ToolError(error.code, str(error)) from error
            finally:
                _current.reset(marker)

        task = asyncio.create_task(work())
        self._operations.add(task)
        try:
            return await task
        finally:
            self._operations.discard(task)

    async def validate_result(self, operation: str, value: Any, context: ToolContext) -> None:
        raw = value.model_dump(mode="json") if isinstance(value, BaseModel) else value
        try:
            actual = json.dumps(raw, sort_keys=True, allow_nan=False)
        except (ValueError, TypeError):
            actual = ""
        if context._command_response != (operation, actual):
            raise ToolError(
                "execution_receipt_unverified", "命令Hook没有保留原后端回执，请按原键查询"
            )

    async def _lookup(self, scope: str, request: CommandLookup) -> CommandView:
        if request.command_id is not None:
            return await self.store.get(scope, request.command_id)
        assert request.request_key is not None
        view = await self.store.by_key(scope, request.request_key)
        if view is None:
            raise ServiceError("execution_not_found", "原命令请求不存在", 404)
        return view

    async def capture_read(
        self, payload: dict[str, Any], context: ToolContext
    ) -> dict[str, object]:
        try:
            marker = context.command_capture
            frame = self._captures.get(marker.token) if marker else None
            request = ReadFileInput.model_validate(payload)
            if (
                frame is None
                or marker is None
                or frame.captured is not None
                or request.path != frame.path
                or marker.path != frame.path
                or request.start_line != 1
                or request.max_lines != 200
            ):
                raise ServiceError("execution_input_target", "命令输入目标或读取范围已变化", 403)
            scope, _execution, workspace = await self._guard(context)
            if (
                scope != frame.scope
                or workspace is not frame.workspace
                or context.run_id != frame.run_id
                or context.registry is not frame.context.registry
            ):
                raise ServiceError("execution_input_context", "命令输入绑定已变化", 403)
            loop = asyncio.get_running_loop()

            def authorize() -> None:
                future = asyncio.run_coroutine_threadsafe(self._guard(context), loop)
                try:
                    future.result(timeout=5)
                except ServiceError:
                    raise
                except Exception:
                    future.cancel()
                    raise ServiceError(
                        "execution_input_unavailable", "命令输入资格无法核实", 503
                    ) from None

            capture = await workspace_io(
                WorkspaceWriter(frame.workspace.require()).capture_file,
                frame.path,
                require_writable=False,
                authorize=authorize,
            )
            await self._guard(context)
            if capture.snapshot is None:
                raise ServiceError("workspace_not_found", "命令输入文件不存在", 404)
            if capture.snapshot.mode & ~0o777:
                raise ServiceError("execution_input_file", "命令输入不接受特殊权限位", 422)
            frame.captured = capture
            frame.expected = frame.workspace.workspace.file_view(
                capture.path, capture.snapshot.data, capture.snapshot.data.decode("utf-8")
            ).model_dump(mode="json")
            return copy.deepcopy(frame.expected)
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error

    async def _existing(
        self,
        scope: str,
        request: AgentCommandRequest,
        context: ToolContext,
        binding: ExecutionBinding,
        workspace: WorkspaceBinding | None,
    ) -> CommandView | None:
        view = await self.store.by_key(scope, request.request_key)
        if view is None:
            return None
        origin = await self.store.origin(scope, view.command_id)
        run_id = None if context.run_id == "local" else context.run_id
        if (
            origin is None
            or origin.get("request") != request.model_dump(mode="json")
            or view.request
            != CommandRequest(request_key=request.request_key, command=request.command)
            or origin.get("workspace") != (workspace.manifest() if workspace else None)
            or view.resource != binding.resource
            or view.run_id != run_id
            or view.call_id != (context.call_id if run_id else None)
        ):
            raise ServiceError("execution_key_conflict", "原键已绑定其他命令意图或资源", 409)
        return view

    async def _execute(self, request: AgentCommandRequest, context: ToolContext) -> CommandSummary:
        scope, binding, workspace = await self._guard(context)
        service = self._service(binding)
        original = await self._existing(scope, request, context, binding, workspace)
        handle: DurableCommandHandle | None = None

        async def check() -> None:
            current_scope, current, current_workspace = await self._guard(context)
            if (
                current_scope != scope
                or current is not binding
                or current_workspace is not workspace
            ):
                raise ServiceError("execution_binding_changed", "命令资源在执行中变化", 403)

        try:
            if original is not None:
                handle = service.handle(scope, original.command_id)
            else:
                captures: list[CapturedFile] = []
                files: dict[str, SandboxFile] = {}
                total = 0
                for path in request.input_paths:
                    await check()
                    if workspace is None:
                        raise ServiceError("workspace_unbound", "命令输入没有工作区授权", 403)
                    if context.registry.get("workspace_read") is not workspace_tools()[0]:
                        raise ServiceError(
                            "execution_input_tool", "命令输入须使用原工作区读取工具", 403
                        )
                    marker = CommandCapture(str(uuid4()), path, context.call_id)
                    frame = _Capture(context, workspace, path, scope, context.run_id)
                    self._captures[marker.token] = frame
                    try:
                        read = await context.registry.invoke(
                            "workspace_read",
                            {"path": path},
                            replace(context, command_capture=marker),
                        )
                        if read.is_error:
                            raise ToolError(
                                read.code or "execution_input_failed", read.content[0].text
                            )
                        if frame.captured is None or frame.expected != read.data:
                            raise ServiceError(
                                "execution_input_not_captured",
                                "读取Hook未产生一致的真实完整输入",
                                403,
                            )
                        snapshot = frame.captured.snapshot
                        assert snapshot is not None
                        total += len(snapshot.data)
                        if (
                            len(snapshot.data) > binding.runner.limits.file_bytes
                            or total > binding.runner.limits.artifact_bytes
                        ):
                            raise ServiceError(
                                "execution_input_limit", "命令完整输入超过执行资源容量", 413
                            )
                        captures.append(frame.captured)
                        files[path] = SandboxFile(snapshot.data, snapshot.mode)
                    finally:
                        self._captures.pop(marker.token, None)
                if captures:
                    assert workspace is not None
                    await workspace_io(
                        WorkspaceWriter(workspace.require()).verify_captured,
                        tuple(captures),
                        require_writable=False,
                    )
                await check()
                origin: dict[str, JsonValue] = {
                    "request": request.model_dump(mode="json"),
                    "workspace": workspace.manifest() if workspace else None,
                    # 纳秒和inode保留为私有JSON字符串，不损失64位精度或冒充JS安全整数。
                    "captures": [
                        json.dumps(encode_capture(item), sort_keys=True) for item in captures
                    ],
                }
                admission = asyncio.create_task(
                    service.start(
                        scope,
                        CommandRequest(request_key=request.request_key, command=request.command),
                        files,
                        directories=tuple(request.directories),
                        run_id=None if context.run_id == "local" else context.run_id,
                        call_id=None if context.run_id == "local" else context.call_id,
                        authorize=check,
                        origin=origin,
                    )
                )
                try:
                    handle = await asyncio.shield(admission)
                except asyncio.CancelledError:
                    handle = await _cleanup(admission)
                    raise
                except ServiceError as error:
                    if error.code != "execution_key_conflict":
                        raise
                    original = await self._existing(scope, request, context, binding, workspace)
                    if original is None:
                        raise
                    handle = service.handle(scope, original.command_id)
            while True:
                await check()
                view = await handle.view()
                if view.terminal:
                    return await self._summary(scope, view)
                await asyncio.sleep(0.05)
        except BaseException:
            if handle is not None:
                await _cleanup(asyncio.create_task(service.stop_and_wait(scope, handle.command_id)))
            raise

    async def _summary(self, scope: str, view: CommandView) -> CommandSummary:
        view, stdout, stderr, files = await self.store.result_snapshot(scope, view.command_id)
        return CommandSummary(
            command_id=view.command_id,
            revision=view.revision,
            phase=view.phase,
            outcome=view.outcome,
            dispatched=view.dispatched,
            started=view.started,
            processes_reaped=view.processes_reaped,
            exit_code=view.exit_code,
            error_code=view.error_code,
            stdout_text=stdout[:1024].decode("utf-8", errors="replace"),
            stderr_text=stderr[:1024].decode("utf-8", errors="replace"),
            stdout_base64=base64.b64encode(stdout[:4096]).decode(),
            stderr_base64=base64.b64encode(stderr[:4096]).decode(),
            stdout_bytes=len(stdout),
            stderr_bytes=len(stderr),
            output_truncated=view.output_truncated or len(stdout) > 1024 or len(stderr) > 1024,
            input_files=view.input_files,
            artifact_files=view.artifact_files,
            artifacts=files[:32],
            artifacts_truncated=len(files) > 32,
            artifacts_complete=view.artifacts_complete,
        )

    async def _read(self, scope: str, view: CommandView, request: CommandRead) -> CommandSlice:
        if request.source == "artifact":
            assert request.path is not None
            data = (await self.store.artifact(scope, view.command_id, request.path)).data
        else:
            output = await self.store.output(scope, view.command_id)
            data = output[0 if request.source == "stdout" else 1]
        offset = min(request.offset, len(data))
        part = data[offset : offset + request.limit]
        return CommandSlice(
            command_id=view.command_id,
            source=request.source,
            path=request.path,
            offset=offset,
            next_offset=offset + len(part),
            byte_size=len(data),
            data_base64=base64.b64encode(part).decode(),
            text=part.decode("utf-8", errors="replace"),
            truncated=offset + len(part) < len(data),
        )

    async def close(self) -> None:
        self.assert_external_close()

        async def drain() -> None:
            self._closed = True
            tasks = list(self._operations)
            for task in tasks:
                if not task.cancelling():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.gather(*(service.close() for service in self._services.values()))

        await _settle(asyncio.create_task(drain()))
