"""Agent 目录与当前进程管理 API；不把 callable/provider 序列化到 HTTP。"""

from __future__ import annotations

import asyncio
import time
from dataclasses import replace
from typing import Literal

from pydantic import Field, field_validator

from krim_agent.agents import (
    AgentCatalog,
    AgentDefinition,
    AgentDescriptor,
    AgentError,
    AgentHandle,
    AgentManager,
    ExecResult,
)
from krim_agent.config import Settings
from krim_agent.context_engine import ContextRunConfig
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.runtime import ChatRuntime
from krim_agent.runtime.chat import RunState
from krim_agent.tools import ToolError

AgentViewStatus = Literal[
    "created", "queued", "running", "completed", "failed", "aborted", "interrupted", "destroyed"
]


class AgentSpawnRequest(StrictModel):
    name: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
    persistent: bool = False


class AgentPromptRequest(StrictModel):
    prompt: str = Field(min_length=1, max_length=8192)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=128)

    @field_validator("prompt")
    @classmethod
    def nonempty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Agent prompt 不能只有空白")
        return value


class AgentView(StrictModel):
    agent_id: str
    name: str
    topic_id: str
    status: AgentViewStatus
    current_run_id: str | None = None
    turn: int
    persistent: bool
    parent_agent_id: str | None = None
    parent_tool_call_id: str | None = None
    result: ExecResult | None = None


class AgentService:
    """当前进程句柄门面；查询读持久 Run，不修改执行句柄的 current_run 指针。"""

    def __init__(self, runtime: ChatRuntime, settings: Settings) -> None:
        self.runtime = runtime
        self.manager = AgentManager(
            runtime.repository,
            max_agents=settings.agent_max_agents,
            max_concurrent=settings.agent_max_concurrent,
            max_recursion_depth=settings.agent_max_recursion_depth,
        )
        self.catalog = AgentCatalog(self.manager)
        self._results: dict[str, tuple[str | None, int, ExecResult]] = {}
        self.catalog.register(
            "default",
            AgentDefinition(
                name="default",
                provider=runtime.provider,
                tools=runtime.tools,
                max_turns=settings.agent_max_turns,
                max_steps=settings.max_steps,
                timeout_seconds=settings.run_timeout_seconds,
                tool_timeout_seconds=settings.tool_timeout_seconds,
                policy=runtime.tool_policy,
                context_config=ContextRunConfig.from_settings(settings),
            ),
            description="复用当前本地 Runtime provider 和工具权限的 Agent",
            expose_as_tool=False,
        )

    def _handle(self, agent_id: str) -> AgentHandle:
        handle = self.manager._handles.get(agent_id)
        if handle is None:
            raise ServiceError("agent_not_found", "Agent 句柄不存在或已跨进程失效", 404)
        return handle

    async def view(self, handle: AgentHandle, *, destroyed: bool = False) -> AgentView:
        current = handle.current_run
        turn = handle.turn
        status: AgentViewStatus = (
            "destroyed"
            if destroyed or handle.destroyed
            else "queued"
            if handle.pending_start
            else "created"
        )
        result: ExecResult | None = None
        if current is not None:
            run = await self.runtime.repository.get_run(current.id)
            if not destroyed and not handle.destroyed:
                status = run.status
            if run.status in {"completed", "failed", "aborted", "interrupted"}:
                messages = await self.runtime.repository.run_messages(run.id)
                usage = await self.runtime.repository.usage_view(run.id)
                text = next(
                    (
                        message.content
                        for message in reversed(messages)
                        if message.role == "assistant"
                    ),
                    "",
                )
                result = ExecResult(
                    agent_id=handle.agent_id,
                    run_id=run.id,
                    status=run.status,
                    text=text,
                    error=run.error,
                    code=run.error_code,
                    parent_tool_call_id=handle.parent_tool_call_id,
                    turn=turn,
                    token_usage=usage.token_usage,
                    usage_complete=usage.usage_complete,
                )
        recent = self._results.get(handle.agent_id)
        if (
            recent is not None
            and not handle.pending_start
            and recent[:2] == (current.id if current else None, turn)
            and (result is not None or current is None)
        ):
            result = recent[2].model_copy(deep=True)
            if not destroyed and not handle.destroyed:
                status = result.status
        if handle.pending_start and not destroyed and not handle.destroyed:
            status = "queued"
        return AgentView(
            agent_id=handle.agent_id,
            name=handle.definition.name,
            topic_id=handle.topic_id,
            status=status,
            current_run_id=current.id if current is not None else None,
            turn=turn,
            persistent=handle.definition.persistent,
            parent_agent_id=handle.definition.parent_agent_id,
            parent_tool_call_id=handle.definition.parent_tool_call_id,
            result=result,
        )

    async def spawn(self, name: str, *, persistent: bool = False) -> AgentView:
        try:
            entry = self.catalog.get(name)
            handle = await self.manager.spawn(
                replace(entry.definition, agent_id="", persistent=persistent)
            )
        except ToolError as exc:
            raise ServiceError(exc.code, str(exc), 404) from exc
        except AgentError as exc:
            raise ServiceError(
                exc.code, str(exc), 429 if exc.code == "agent_limit" else 409
            ) from exc
        return await self.view(handle)

    async def restore_persistent(self) -> int:
        restored = 0
        stored = await self.manager.store.list()
        for item in stored:
            if not item.persistent:
                continue
            try:
                entry = self.catalog.get(item.name)
                handle = await self.manager.restore(entry.definition, item)
                if handle.current_run is not None and handle.current_run.status in {
                    "queued",
                    "running",
                }:
                    handle.current_run = await self.runtime.repository.get_run(
                        handle.current_run.id
                    )
                restored += 1
            except (AgentError, ServiceError, ToolError):
                continue
        return restored

    async def assert_inventory_available(self) -> None:
        """启动恢复前先拒绝有效的其它 owner，不能中断仍在生产的进程。"""
        now = time.time()
        for item in await self.manager.store.list():
            if (
                item.owner_id is not None
                and item.owner_id != self.manager.owner_id
                and item.lease_expires_at is not None
                and item.lease_expires_at > now
            ):
                raise ServiceError(
                    "agent_inventory_busy", "持久 Agent 仍由另一进程拥有，禁止启动恢复", 409
                )

    def catalog_views(self) -> list[AgentDescriptor]:
        return self.catalog.descriptors()

    async def views(self) -> list[AgentView]:
        handles = tuple(self.manager._handles.values())
        return [await self.view(handle) for handle in handles]

    async def get(self, agent_id: str) -> AgentView:
        return await self.view(self._handle(agent_id))

    def _remember(self, handle: AgentHandle, result: ExecResult) -> None:
        if handle.agent_id in self.manager._handles:
            self._results[handle.agent_id] = (
                handle.current_run.id if handle.current_run else None,
                handle.turn,
                result,
            )

    async def execute(self, agent_id: str, request: AgentPromptRequest) -> AgentView:
        handle = self._handle(agent_id)
        try:
            result = await handle.execute(request.prompt, request.idempotency_key)
        except asyncio.CancelledError:
            caller = asyncio.current_task()
            if caller is not None and caller.cancelling():
                raise
            result = ExecResult(
                agent_id=handle.agent_id,
                run_id=handle.current_run.id if handle.current_run else None,
                status="aborted",
                error="Agent 执行已停止",
                code="cancelled",
                turn=handle.turn,
                parent_tool_call_id=handle.parent_tool_call_id,
            )
        except AgentError as exc:
            raise ServiceError(exc.code, str(exc), 409) from exc
        self._remember(handle, result)
        return await self.view(handle)

    async def resume(self, agent_id: str, request: AgentPromptRequest) -> AgentView:
        handle = self._handle(agent_id)
        try:
            result = await handle.resume(request.prompt, request.idempotency_key)
        except asyncio.CancelledError:
            caller = asyncio.current_task()
            if caller is not None and caller.cancelling():
                raise
            result = ExecResult(
                agent_id=handle.agent_id,
                run_id=handle.current_run.id if handle.current_run else None,
                status="aborted",
                error="Agent 执行已停止",
                code="cancelled",
                turn=handle.turn,
                parent_tool_call_id=handle.parent_tool_call_id,
            )
        except AgentError as exc:
            raise ServiceError(exc.code, str(exc), 409) from exc
        self._remember(handle, result)
        return await self.view(handle)

    async def stop(self, agent_id: str) -> AgentView:
        handle = self._handle(agent_id)
        try:
            result = await handle.stop()
        except AgentError as exc:
            raise ServiceError(exc.code, str(exc), 409) from exc
        self._remember(handle, result)
        return await self.view(handle)

    async def destroy(self, agent_id: str) -> AgentView:
        handle = self._handle(agent_id)
        try:
            await handle.destroy()
        except AgentError as exc:
            raise ServiceError(exc.code, str(exc), 409) from exc
        value = await self.view(handle, destroyed=True)
        self._results.pop(handle.agent_id, None)
        return value

    async def stream_state(self, agent_id: str, from_seq: int) -> tuple[AgentHandle, RunState]:
        handle = self._handle(agent_id)
        if handle.current_run is None:
            raise ServiceError("agent_not_started", "Agent 尚未启动运行", 409)
        state = await handle.runtime.stream_state(handle.current_run.id, from_seq)
        return handle, state

    async def close(self) -> None:
        await self.manager.close()
        self._results.clear()
