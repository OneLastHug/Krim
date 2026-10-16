"""Host公开恢复准入；同Agent执行/排队互斥，取消/关闭先收束后端SQL。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from typing import TYPE_CHECKING

from krim_agent.application.workspace_changes import _wait
from krim_agent.conversation_recovery import (
    ConversationRecoveryPreview,
    ConversationRecoveryRecord,
    ConversationRecoveryRequest,
)
from krim_agent.persistence.conversation_recovery import (
    ConversationRecoveryScope,
    ConversationRecoveryStore,
)
from krim_agent.protocol.domain import ServiceError

if TYPE_CHECKING:
    from krim_agent.application.agents import AgentService


class ConversationRecoveryController:
    def __init__(self, service: AgentService) -> None:
        self.service = service
        self.store = ConversationRecoveryStore(service.runtime.repository)
        self.operations: set[asyncio.Task[object]] = set()
        self.active: set[str] = set()
        self.closed = False
        self._inside: ContextVar[bool] = ContextVar("krim_conversation_recovery", default=False)

    def assert_execution_allowed(self, agent_id: str) -> None:
        if self.closed or self.service._closed:
            raise ServiceError("agent_service_closed", "Agent服务已关闭", 503)
        if agent_id in self.active:
            raise ServiceError("agent_busy", "Agent正在审阅或建立会话恢复分支", 409)

    def assert_external_close(self) -> None:
        if self._inside.get() or asyncio.current_task() in self.operations:
            raise ServiceError(
                "conversation_recovery_self_close", "恢复调用内部不能关闭自身服务", 409
            )

    async def _call[T](
        self,
        agent_id: str,
        operation: Callable[[ConversationRecoveryScope], Awaitable[T]],
        *,
        idle: bool,
    ) -> T:
        async with self.service._start_lock:
            if self.closed or self.service._closed:
                raise ServiceError("agent_service_closed", "Agent服务已关闭", 503)
            if len(self.operations) >= 64:
                raise ServiceError("conversation_recovery_busy", "会话恢复操作已达上限", 429)
            handle = self.service._handle(agent_id)
            background = self.service._tasks.get(agent_id)
            if idle:
                if handle.destroyed or handle._closing:
                    raise ServiceError("agent_session_closed", "Agent正在关闭或已销毁", 410)
                if (
                    agent_id in self.active
                    or handle._execute_lock.locked()
                    or handle.pending_start
                    or background is not None
                    and not background[1].done()
                ):
                    raise ServiceError("agent_busy", "Agent有活跃、排队或后台执行", 409)
                # 准入时锁已空闲；原start_lock阻止在检查和占锁之间插入后台输入。
                await handle._execute_lock.acquire()
                self.active.add(agent_id)
            scope = ConversationRecoveryScope(
                handle.topic_id, handle.session_id, handle.agent_id, self.service.manager.owner_id
            )

            async def run() -> T:
                token = self._inside.set(True)
                try:
                    return await operation(scope)
                finally:
                    self._inside.reset(token)

            task: asyncio.Task[T] = asyncio.create_task(run())
            self.operations.add(task)

        def cancel_once() -> None:
            if not task.done() and not task.cancelling():
                task.cancel()

        try:
            return await _wait(task, cancel_once)
        finally:
            self.operations.discard(task)
            if idle:
                self.active.discard(agent_id)
                handle._execute_lock.release()

    async def preview(self, agent_id: str) -> ConversationRecoveryPreview:
        return await self._call(agent_id, self.store.preview, idle=True)

    async def apply(
        self, agent_id: str, request: ConversationRecoveryRequest, *, confirmed: bool = False
    ) -> ConversationRecoveryRecord:
        if type(confirmed) is not bool:
            raise ServiceError("conversation_recovery_confirmation", "恢复确认必须为实际bool", 422)
        try:
            request = ConversationRecoveryRequest.model_validate(request.model_dump(mode="json"))
            if request.expected_session_revision is None:
                raise ValueError("Agent恢复必须明确原会话版本")
        except (TypeError, ValueError, AttributeError):
            raise ServiceError(
                "conversation_recovery_parameters", "Agent恢复请求或版本无效", 422
            ) from None
        return await self._call(
            agent_id, lambda scope: self.store.apply(scope, request, confirmed=confirmed), idle=True
        )

    async def by_key(self, agent_id: str, request_key: str) -> ConversationRecoveryRecord | None:
        return await self._call(
            agent_id, lambda scope: self.store.by_key(scope, request_key), idle=False
        )

    async def close(self) -> None:
        self.assert_external_close()
        self.closed = True
        operations = list(self.operations)
        for task in operations:
            if not task.done() and not task.cancelling():
                task.cancel()
        await asyncio.gather(*operations, return_exceptions=True)
