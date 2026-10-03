"""本地 SubAgent 管理器；复用 ChatRuntime 的持久化、SSE、工具权限和审计。"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import replace
from typing import cast

from krim_agent.config import Settings
from krim_agent.persistence import KrimRepository
from krim_agent.protocol.domain import TERMINAL_STATUSES, ChatRequest, RunView
from krim_agent.protocol.models import StreamFrame
from krim_agent.runtime import ChatRuntime
from krim_agent.tools import ToolContext, ToolRegistry
from krim_agent.tools.core import Decision, Policy, ToolDefinition

from .types import AgentDefinition, AgentError, AgentStatus, ExecResult


def copy_tools(source: ToolRegistry) -> ToolRegistry:
    """复制定义本身，避免 Agent 销毁时关闭宿主拥有的 registry。"""
    return source.fork()


def inherited_policy(source: Policy | None, parent: Policy | None) -> Policy:
    async def decide(definition: ToolDefinition, context: ToolContext) -> Decision:
        caller: Decision = (
            await parent(definition, context)
            if parent is not None
            else "allow"
            if definition.risk == "read"
            else "deny"
        )
        if caller not in {"allow", "audit"}:
            return caller
        own = await source(definition, context) if source is not None else caller
        if own not in {"allow", "audit"}:
            return own
        return "audit" if caller == "audit" or own == "audit" else "allow"

    return decide


class AgentHandle:
    def __init__(
        self,
        manager: AgentManager,
        definition: AgentDefinition,
        runtime: ChatRuntime,
        topic_id: str,
    ) -> None:
        self.manager = manager
        self.definition = definition
        self.runtime = runtime
        self.topic_id = topic_id
        self.agent_id = definition.agent_id
        self.turn = 0
        self.current_run: RunView | None = None
        self.destroyed = False
        self._execute_lock = asyncio.Lock()
        self._consumer: asyncio.Task[ExecResult] | None = None
        self._destroy_task: asyncio.Task[ExecResult] | None = None
        self._production: asyncio.Task[None] | None = None

    @property
    def parent_tool_call_id(self) -> str | None:
        return self.definition.parent_tool_call_id

    async def execute(self, prompt: str, idempotency_key: str | None = None) -> ExecResult:
        if self._execute_lock.locked():
            raise AgentError("agent_busy", "Agent 已有活跃或排队执行")
        async with self._execute_lock:
            operation = asyncio.create_task(self._consume(prompt, idempotency_key))
            self._consumer = operation
            try:
                return await operation
            finally:
                self._consumer = None

    async def _consume(self, prompt: str, idempotency_key: str | None) -> ExecResult:
        try:
            return await self._execute(prompt, idempotency_key)
        except asyncio.CancelledError:
            if self.current_run is not None and not self.definition.persistent:
                self.current_run = await asyncio.shield(self.runtime.stop(self.current_run.id))
            raise

    async def _execute(self, prompt: str, idempotency_key: str | None) -> ExecResult:
        if self.destroyed:
            raise AgentError("agent_destroyed", "Agent 已销毁")
        if self.current_run is not None:
            self.current_run = await self.runtime.repository.get_run(self.current_run.id)
        active = self.current_run
        if active is not None and active.status not in TERMINAL_STATUSES:
            raise AgentError("agent_busy", "Agent 已有活跃执行")
        requested_key = idempotency_key or f"agent-{self.agent_id}-{self.turn + 1}"
        if (
            active is not None
            and active.status in TERMINAL_STATUSES
            and active.idempotency_key == requested_key
        ):
            await self.runtime.start(
                self.topic_id, ChatRequest(text=prompt, idempotency_key=requested_key)
            )
            return await self._completed_result()
        if self.turn >= self.definition.max_turns:
            return self._result(active, "failed", "Agent 超过轮次预算", "max_turns")
        started_at = asyncio.get_running_loop().time()
        try:
            await asyncio.wait_for(
                self.manager._semaphore.acquire(), timeout=self.definition.timeout_seconds
            )
        except TimeoutError:
            return self._result(None, "aborted", "Agent 排队超过时间预算", "timeout")
        try:
            if self.destroyed:
                raise AgentError("agent_destroyed", "Agent 已销毁")
            request = ChatRequest(
                text=prompt,
                idempotency_key=requested_key,
            )
            previous_id = self.current_run.id if self.current_run else None
            start = asyncio.create_task(self.runtime.start(self.topic_id, request))
            cancelled = False
            try:
                self.current_run = await asyncio.shield(start)
            except asyncio.CancelledError:
                self.current_run = await start
                cancelled = True
            if self.current_run.id != previous_id:
                self.turn += 1
            self._production = self.runtime.production_task(self.current_run.id)
            state = await self.runtime.stream_state(self.current_run.id, 0)
            self._production = state.task
            if cancelled:
                raise asyncio.CancelledError
            if state.task is None:
                raise AgentError("run_unavailable", "Agent 运行任务不可用")
            try:
                await asyncio.wait_for(
                    asyncio.shield(state.task),
                    timeout=max(
                        0.001,
                        self.definition.timeout_seconds
                        - (asyncio.get_running_loop().time() - started_at),
                    ),
                )
            except TimeoutError:
                await self.runtime.stop(self.current_run.id)
                return await self._completed_result("aborted", "Agent 超过时间预算", "timeout")
            if self.current_run is not None:
                latest = await self.runtime.repository.get_run(self.current_run.id)
                if latest.error_code == "run_timeout":
                    return await self._completed_result("aborted", "Agent 超过时间预算", "timeout")
            return await self._completed_result()
        finally:
            if self._production is not None and not self._production.done():
                self._production.add_done_callback(lambda _task: self.manager._semaphore.release())
            else:
                self.manager._semaphore.release()

    async def resume(self, prompt: str, idempotency_key: str | None = None) -> ExecResult:
        if self.current_run is None:
            raise AgentError("agent_not_started", "没有可以 resume 的 Agent 运行")
        if self.destroyed:
            raise AgentError("agent_destroyed", "Agent 已销毁")
        self.current_run = await self.runtime.repository.get_run(self.current_run.id)
        if self.current_run.status not in TERMINAL_STATUSES:
            raise AgentError("agent_busy", "Agent 当前运行尚未终态")
        return await self.execute(prompt, idempotency_key)

    async def wait(self) -> ExecResult:
        if self.current_run is None:
            raise AgentError("agent_not_started", "Agent 尚未开始")
        state = await self.runtime.stream_state(self.current_run.id, 0)
        if state.task is not None:
            await asyncio.shield(state.task)
        return await self._completed_result()

    async def stop(self) -> ExecResult:
        if self.destroyed:
            return self._result(None, "destroyed", "Agent 已销毁", "agent_destroyed")
        if self.current_run is None:
            return self._result(None, "aborted", "Agent 没有运行", "not_started")
        run = await self.runtime.stop(self.current_run.id)
        self.current_run = run
        return await self._completed_result()

    async def events(self, from_seq: int = 0) -> AsyncIterator[StreamFrame]:
        if self.current_run is None:
            raise AgentError("agent_not_started", "没有可以读取的 Agent 运行")
        state = await self.runtime.stream_state(self.current_run.id, from_seq)
        async for frame in self.runtime.subscribe(state, from_seq):
            if frame is not None:
                yield frame

    async def destroy(self) -> ExecResult:
        if asyncio.current_task() is self._production:
            raise AgentError("agent_self_destroy", "不能在 Agent 自己的生产任务内等待销毁")
        if self._destroy_task is not None:
            return await asyncio.shield(self._destroy_task)
        if self.destroyed:
            return self._result(None, "destroyed", "Agent 已销毁", "agent_destroyed")
        self.destroyed = True
        consumer = self._consumer
        if consumer is asyncio.current_task():
            consumer = None
        if consumer is not None and consumer is not asyncio.current_task():
            consumer.cancel()
        self._destroy_task = asyncio.create_task(self._destroy(consumer))
        try:
            return await asyncio.shield(self._destroy_task)
        except asyncio.CancelledError:
            await self._destroy_task
            raise

    async def _destroy(self, consumer: asyncio.Task[ExecResult] | None) -> ExecResult:
        if consumer is not None and consumer is not asyncio.current_task():
            await asyncio.gather(consumer, return_exceptions=True)
        if self.current_run is not None and self.current_run.status not in TERMINAL_STATUSES:
            self.current_run = await self.runtime.stop(self.current_run.id)
        await self.runtime.close()
        self.manager._handles.pop(self.agent_id, None)
        return self._result(self.current_run, "destroyed", "Agent 已销毁", "destroyed")

    async def _completed_result(
        self,
        forced_status: AgentStatus | None = None,
        forced_error: str | None = None,
        forced_code: str | None = None,
    ) -> ExecResult:
        if self.current_run is None:
            return self._result(None, "failed", "Agent 尚未运行", "not_started")
        run = await self.runtime.repository.get_run(self.current_run.id)
        self.current_run = run
        messages = await self.runtime.repository.run_messages(run.id)
        reply = next(
            (message.content for message in reversed(messages) if message.role == "assistant"),
            "",
        )
        status: AgentStatus = forced_status or cast(AgentStatus, run.status)
        return ExecResult(
            agent_id=self.agent_id,
            run_id=run.id,
            status=status,
            text=reply,
            error=forced_error or run.error,
            code=forced_code or run.error_code,
            parent_tool_call_id=self.parent_tool_call_id,
            turn=self.turn,
        )

    def _result(
        self, run: RunView | None, status: AgentStatus, error: str, code: str
    ) -> ExecResult:
        return ExecResult(
            agent_id=self.agent_id,
            run_id=run.id if run else None,
            status=status,
            error=error,
            code=code,
            parent_tool_call_id=self.parent_tool_call_id,
            turn=self.turn,
        )


class AgentManager:
    def __init__(
        self,
        repository: KrimRepository,
        *,
        max_agents: int = 32,
        max_concurrent: int = 8,
        max_recursion_depth: int = 4,
    ) -> None:
        if not 1 <= max_agents <= 1024 or not 1 <= max_concurrent <= 64:
            raise ValueError("Agent 数量和并发必须为有效正数")
        if not 1 <= max_recursion_depth <= 16:
            raise ValueError("Agent 递归上限必须在 1–16 之间")
        self.repository = repository
        self.max_agents = max_agents
        self.max_recursion_depth = max_recursion_depth
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._handles: dict[str, AgentHandle] = {}
        self._spawn_lock = asyncio.Lock()
        self._closed = False

    async def spawn(
        self, definition: AgentDefinition, *, parent_context: ToolContext | None = None
    ) -> AgentHandle:
        async with self._spawn_lock:
            if parent_context is not None:
                definition = replace(
                    definition,
                    parent_agent_id=parent_context.agent_id,
                    parent_tool_call_id=parent_context.call_id,
                    policy=inherited_policy(definition.policy, parent_context.policy),
                    tools=definition.tools.fork(controls=parent_context.registry),
                    timeout_seconds=min(definition.timeout_seconds, parent_context.timeout),
                    tool_timeout_seconds=min(
                        definition.tool_timeout_seconds,
                        definition.tools.default_timeout,
                        parent_context.timeout,
                    ),
                )
            return await self._spawn(definition)

    async def _spawn(self, definition: AgentDefinition) -> AgentHandle:
        if self._closed:
            raise AgentError("manager_closed", "AgentManager 已关闭")
        if len(self._handles) >= self.max_agents:
            raise AgentError("agent_limit", "Agent 数量超过限制")
        definition = definition.with_id()
        if definition.agent_id in self._handles:
            raise AgentError("agent_id_conflict", "Agent ID 已存在")
        depth = 1
        parent = definition.parent_agent_id
        while parent:
            parent_handle = self._handles.get(parent)
            if parent_handle is None or parent_handle.destroyed:
                raise AgentError("parent_agent_missing", "父 Agent 不存在")
            depth += 1
            parent = parent_handle.definition.parent_agent_id
        if depth > self.max_recursion_depth:
            raise AgentError("recursion_limit", "Agent 递归深度超过限制")
        database = self.repository.database
        settings = Settings(
            database_url=str(database.engine.url),
            max_steps=definition.max_steps,
            run_timeout_seconds=definition.timeout_seconds,
            tool_timeout_seconds=definition.tool_timeout_seconds,
        )
        runtime = ChatRuntime(
            self.repository,
            definition.provider,
            copy_tools(definition.tools),
            settings,
            tool_policy=definition.policy,
            agent_id=definition.agent_id,
        )
        topic = await self.repository.create_topic(f"Agent: {definition.name}")
        handle = AgentHandle(self, definition, runtime, topic.id)
        self._handles[handle.agent_id] = handle
        return handle

    async def destroy_all(self) -> None:
        for handle in list(self._handles.values()):
            await handle.destroy()

    async def close_tool_scope(self, parent_tool_call_id: str) -> None:
        for handle in list(self._handles.values()):
            if (
                handle.parent_tool_call_id == parent_tool_call_id
                and not handle.definition.persistent
            ):
                await handle.destroy()

    async def close(self) -> None:
        async with self._spawn_lock:
            self._closed = True
        await self.destroy_all()
