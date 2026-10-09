"""可信宿主的持久执行编排；不绕过未来模型工具与工作区授权入口。"""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Mapping
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from typing import Literal
from uuid import uuid4

from pydantic import JsonValue

from krim_agent.persistence.commands import CommandStore, freeze_files
from krim_agent.protocol.domain import ServiceError

from .linux import Authorize, ExecutionHandle, LinuxSandboxRunner, _settle
from .models import ExecutionResult, SandboxFile
from .records import CommandRequest, CommandResource, CommandView

_current: ContextVar[asyncio.Task[None] | None] = ContextVar("krim_durable_command", default=None)


def _reject_reentry() -> None:
    task = _current.get()
    if task is not None and not task.done():
        raise ServiceError("execution_service_reentrant", "命令资格回调不能等待或关闭命令服务", 409)


class DurableCommandHandle:
    def __init__(self, store: CommandStore, scope: str, identifier: str) -> None:
        self._store, self._scope, self._identifier = store, scope, identifier

    @property
    def command_id(self) -> str:
        return self._identifier

    async def view(self) -> CommandView:
        return await self._store.get(self._scope, self.command_id)

    async def wait(self) -> CommandView:
        _reject_reentry()
        while True:
            result = await self.view()
            if result.terminal:
                return result
            await asyncio.sleep(0.05)

    async def cancel(self) -> CommandView:
        return await self._store.cancel(self._scope, self.command_id)

    async def output(self) -> tuple[bytes, bytes]:
        return await self._store.output(self._scope, self.command_id)

    async def files(
        self, kind: Literal["input", "artifact"] = "artifact"
    ) -> dict[str, SandboxFile]:
        return await self._store.files(self._scope, self.command_id, kind)


@dataclass
class _Running:
    stop: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task[None] | None = None


class DurableCommandService:
    def __init__(
        self,
        store: CommandStore,
        runner: LinuxSandboxRunner,
        *,
        key: str = "local",
        version: str = "1",
        owned: bool = False,
    ) -> None:
        self.store, self.runner, self.owned = store, runner, owned
        self._resource = CommandResource(key=key, version=version, fingerprint=runner.fingerprint)
        self._active: dict[str, _Running] = {}
        self._lock = asyncio.Lock()
        self._closed = False

    @property
    def resource(self) -> CommandResource:
        return self._resource

    def handle(self, scope: str, identifier: str) -> DurableCommandHandle:
        return DurableCommandHandle(self.store, scope, identifier)

    async def start(
        self,
        scope: str,
        request: CommandRequest,
        files: Mapping[str, SandboxFile | bytes] | None = None,
        *,
        directories: tuple[str, ...] = (),
        run_id: str | None = None,
        call_id: str | None = None,
        authorize: Authorize | None = None,
        origin: dict[str, JsonValue] | None = None,
    ) -> DurableCommandHandle:
        request = CommandRequest.model_validate(request.model_dump())
        values = freeze_files(files or {}, self.runner.limits)
        directories = tuple(directories)
        origin = copy.deepcopy(origin)

        async def accept() -> DurableCommandHandle:
            async with self._lock:
                if self._closed:
                    raise ServiceError("execution_service_closed", "命令服务已关闭", 503)
                # 原键查询即使满额也可读；新任务通过runner自身的实际进程准入。
                reservation = await self.store.reserve(
                    scope,
                    request,
                    self.resource,
                    self.runner.limits,
                    values,
                    directories=directories,
                    run_id=run_id,
                    call_id=call_id,
                    origin=origin,
                )
                identifier = reservation.view.command_id
                if reservation.token is not None:
                    entry = _Running()
                    self._active[identifier] = entry
                    entry.task = asyncio.create_task(
                        self._work(
                            entry,
                            scope,
                            identifier,
                            reservation.token,
                            request,
                            values,
                            directories,
                            authorize,
                        )
                    )
                    entry.task.add_done_callback(lambda task: self._finished(identifier, task))
                return self.handle(scope, identifier)

        # 准入提交与后台任务登记连续完成；调用者取消后仍可用原键查询。
        return await _settle(asyncio.create_task(accept()))

    def _finished(self, identifier: str, task: asyncio.Task[None]) -> None:
        self._active.pop(identifier, None)
        if not task.cancelled():
            task.exception()

    async def _work(
        self,
        entry: _Running,
        scope: str,
        identifier: str,
        token: str,
        request: CommandRequest,
        files: dict[str, SandboxFile],
        directories: tuple[str, ...],
        authorize: Authorize | None,
    ) -> None:
        marker = _current.set(asyncio.current_task())
        handle: ExecutionHandle | None = None
        started: asyncio.Task[bool] | None = None
        bound = asyncio.Event()
        result: ExecutionResult | None = None
        supervision_error: str | None = None

        async def guard() -> None:
            await bound.wait()
            if entry.stop.is_set():
                raise ServiceError("execution_cancelled", "命令服务已请求停止", 409)
            if authorize is not None:
                await authorize()
            await self.store.authorize(scope, identifier, token)

        async def dispatch() -> None:
            await self.store.authorize(scope, identifier, token, dispatch=True)

        try:
            handle = await self.runner.start(
                request.command,
                files,
                directories=directories,
                authorize=guard,
                before_dispatch=dispatch,
            )
            try:
                await self.store.authorize(scope, identifier, token, sandbox_id=handle.execution_id)
            finally:
                bound.set()
            started = asyncio.create_task(handle.wait_started())
            while not handle.done:
                if entry.stop.is_set():
                    handle.cancel()
                try:
                    stdout, stderr = handle.output()
                    await self.store.progress(
                        scope,
                        identifier,
                        token,
                        stdout,
                        stderr,
                        started=started.done() and started.result(),
                    )
                except Exception as error:
                    supervision_error = (
                        error.code
                        if isinstance(error, ServiceError)
                        else "execution_supervision_failed"
                    )
                    handle.cancel()
                    break
                await asyncio.sleep(min(0.1, self.store.lease_seconds / 4))
            result = await handle.wait()
        except BaseException as error:
            bound.set()
            supervision_error = (
                error.code if isinstance(error, ServiceError) else "execution_service_failed"
            )
            if handle is not None:
                handle.cancel()
                result = await _settle(asyncio.create_task(handle.wait()))
            else:
                result = ExecutionResult(
                    str(uuid4()),
                    "setup_failed",
                    False,
                    False,
                    None,
                    b"",
                    b"",
                    False,
                    (),
                    False,
                    0,
                    error.code if isinstance(error, ServiceError) else "execution_service_failed",
                    0,
                )
        finally:
            if started is not None:
                started.cancel()
                await asyncio.gather(started, return_exceptions=True)
            try:
                if result is not None:
                    if supervision_error is not None:
                        result = replace(result, error_code=supervision_error)

                    # 保存回执可幂等重试；只重复SQL，不重复命令或产物发布。
                    async def save() -> None:
                        for attempt in range(3):
                            try:
                                await self.store.finish(scope, identifier, token, result)
                                return
                            except Exception:
                                if attempt == 2:
                                    raise
                                await asyncio.sleep(0.05)

                    await _settle(asyncio.create_task(save()))
                else:
                    await self.store.interrupt(scope, identifier, token)
            except BaseException:
                try:
                    await _settle(
                        asyncio.create_task(self.store.interrupt(scope, identifier, token))
                    )
                except BaseException:
                    # DB不可写时保留原租约，下一次读取按过期转为interrupted，不能伪造成功。
                    pass
            finally:
                _current.reset(marker)

    async def close(self) -> None:
        _reject_reentry()

        async def drain() -> None:
            # 连等待准入事务结束也属于关闭动作，调用者取消不能丢弃这一阶段。
            async with self._lock:
                self._closed = True
                entries = list(self._active.values())
            for entry in entries:
                entry.stop.set()
            await asyncio.gather(
                *(entry.task for entry in entries if entry.task is not None), return_exceptions=True
            )
            if self.owned:
                await self.runner.close()

        await _settle(asyncio.create_task(drain()))

    async def stop_and_wait(self, scope: str, identifier: str) -> CommandView:
        _reject_reentry()
        await self.store.cancel(scope, identifier)
        entry = self._active.get(identifier)
        if entry is not None and entry.task is not None:
            entry.stop.set()
            await _settle(asyncio.create_task(self._drain_task(entry.task)))
        return await self.store.get(scope, identifier)

    @staticmethod
    async def _drain_task(task: asyncio.Task[None]) -> None:
        await asyncio.gather(task, return_exceptions=True)

    async def __aenter__(self) -> DurableCommandService:
        return self

    async def __aexit__(self, *_arguments: object) -> None:
        await self.close()
