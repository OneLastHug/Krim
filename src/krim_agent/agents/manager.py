"""本地 SubAgent 管理器；复用 ChatRuntime 的持久化、SSE、工具权限和审计。"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections.abc import AsyncIterator
from dataclasses import replace
from typing import cast
from uuid import uuid4

from pydantic import JsonValue

from krim_agent.application.commands import AgentCommandService
from krim_agent.application.workspace_changes import WorkspaceChangeService
from krim_agent.application.workspace_directories import DirectoryChangeService
from krim_agent.application.workspace_mixed_queries import MixedBatchQueryService
from krim_agent.config import Settings
from krim_agent.context_engine import ContextRunConfig
from krim_agent.execution.binding import COMMAND_TOOL_NAMES, ExecutionBinding
from krim_agent.observability import Tracer
from krim_agent.persistence import KrimRepository
from krim_agent.persistence.agent_sessions import AgentSessionStore
from krim_agent.persistence.agents import AgentHandleStore, StoredAgentHandle
from krim_agent.persistence.commands import CommandStore
from krim_agent.persistence.tasks import AgentTaskStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.protocol.domain import TERMINAL_STATUSES, ChatRequest, RunView, ServiceError
from krim_agent.protocol.models import StreamFrame
from krim_agent.providers.openai_compatible import OpenAICompatibleProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.tools import ToolContext, ToolRegistry
from krim_agent.tools.core import Decision, Policy, ToolDefinition
from krim_agent.workspace import WORKSPACE_TOOL_NAMES, WORKSPACE_WRITE_TOOL_NAMES, WorkspaceBinding

from .permissions import profile_policy
from .resources import provider_fingerprint
from .sessions import AgentSessionView, SessionStatus
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
        if caller not in {"allow", "audit", "ask"}:
            return caller
        own = await source(definition, context) if source is not None else caller
        if own not in {"allow", "audit", "ask"}:
            return own
        if caller == "ask" or own == "ask":
            return "ask"
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
        self.session_id = definition.agent_id
        self.turn = 0
        self.current_run: RunView | None = None
        self.destroyed = False
        self._execute_lock = asyncio.Lock()
        self._consumer: asyncio.Task[ExecResult] | None = None
        self._destroy_task: asyncio.Task[ExecResult] | None = None
        self._production: asyncio.Task[None] | None = None
        self._session_lock = asyncio.Lock()
        self._expiry: asyncio.Task[None] | None = None
        self.pending_start = False
        self._accepted = asyncio.Event()
        self._durable_revision: int | None = None
        self._durable_lock = asyncio.Lock()
        self._heartbeat: asyncio.Task[None] | None = None
        self._lease_lost = False
        self._closing = False

    @property
    def parent_tool_call_id(self) -> str | None:
        return self.definition.parent_tool_call_id

    async def execute(self, prompt: str, idempotency_key: str | None = None) -> ExecResult:
        if self._execute_lock.locked():
            raise AgentError("agent_busy", "Agent 已有活跃或排队执行")
        async with self._execute_lock:
            self._accepted.clear()
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
        if self._lease_lost:
            raise AgentError("agent_lease_lost", "Agent 持久句柄租约已丢失")
        if self.destroyed or self._closing:
            raise AgentError("agent_destroyed", "Agent 已销毁")
        if self.current_run is not None:
            self.current_run = await self.runtime.repository.get_run(self.current_run.id)
        active = self.current_run
        if active is not None and active.status not in TERMINAL_STATUSES:
            raise AgentError("agent_busy", "Agent 已有活跃执行")
        requested_key = idempotency_key or f"agent-{self.agent_id}-{self.turn + 1}"
        previous = await self.manager.sessions.find_turn(self.session_id, requested_key)
        if previous is not None:
            historical, historical_turn = previous
            if historical.status not in TERMINAL_STATUSES:
                raise AgentError("agent_busy", "Agent 已有活跃执行")
            await self.runtime.start(
                self.topic_id, ChatRequest(text=prompt, idempotency_key=requested_key)
            )
            return await self._completed_result(read_run=historical, read_turn=historical_turn)
        session = await self.session()
        if session.status != "open":
            raise ServiceError(f"agent_session_{session.status}", "Agent 会话不可执行", 410)
        if self.turn >= self.definition.max_turns:
            return self._result(active, "failed", "Agent 超过轮次预算", "max_turns")
        started_at = asyncio.get_running_loop().time()
        self.pending_start = True
        try:
            await asyncio.wait_for(
                self.manager._semaphore.acquire(), timeout=self.definition.timeout_seconds
            )
        except TimeoutError:
            self.pending_start = False
            return self._result(None, "aborted", "Agent 排队超过时间预算", "timeout")
        except BaseException:
            self.pending_start = False
            raise
        try:
            if self.destroyed:
                raise AgentError("agent_destroyed", "Agent 已销毁")
            if self._durable_revision is not None:
                async with self._durable_lock:
                    await self.manager.store.renew(
                        self.agent_id, self.manager.owner_id, self._durable_revision
                    )
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
            self._accepted.set()
            self._production = self.runtime.production_task(self.current_run.id)
            try:
                await self.manager.persist(self, "running")
            except BaseException:
                await self.runtime.stop(self.current_run.id)
                raise
            self.pending_start = False
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
            self.pending_start = False
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
        if asyncio.current_task() is self._production:
            raise AgentError("agent_self_stop", "不能在 Agent 自己的生产任务内等待停止")
        if self.destroyed:
            return self._result(None, "destroyed", "Agent 已销毁", "agent_destroyed")
        if self.pending_start and self._consumer is not None:
            consumer = self._consumer
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)
            if self.current_run is None:
                return self._result(None, "aborted", "Agent 排队或启动等待已停止", "cancelled")
        if self.current_run is None:
            return self._result(None, "aborted", "Agent 没有运行", "not_started")
        run = await self.runtime.stop(self.current_run.id)
        self.current_run = run
        return await self._completed_result()

    async def events(
        self, from_seq: int = 0, *, run_id: str | None = None
    ) -> AsyncIterator[StreamFrame]:
        selected = run_id or (self.current_run.id if self.current_run is not None else None)
        if selected is None:
            raise AgentError("agent_not_started", "没有可以读取的 Agent 运行")
        await self.manager.sessions.assert_run(self.session_id, selected)
        state = await self.runtime.stream_state(selected, from_seq)
        async for frame in self.runtime.subscribe(state, from_seq):
            if frame is not None:
                yield frame

    async def session(self) -> AgentSessionView:
        return await self.manager.sessions.get(self.session_id)

    async def _session_change(
        self, status: SessionStatus, expected_revision: int
    ) -> AgentSessionView:
        async with self._session_lock:
            if self._lease_lost or self.destroyed:
                raise AgentError("agent_unavailable", "Agent 句柄不可操作")
            value = await self.manager.sessions.transition(
                self.session_id, self.manager.owner_id, expected_revision, status
            )
            if status != "open":
                for child in list(self.manager._handles.values()):
                    child_session = await child.session()
                    if child_session.parent_session_id == self.session_id and not child.destroyed:
                        await child.close_session(child_session.revision)
                await self.stop()
            return value

    async def close_session(self, expected_revision: int) -> AgentSessionView:
        task = asyncio.create_task(self._session_change("closed", expected_revision))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def reopen_session(self, expected_revision: int) -> AgentSessionView:
        return await self._session_change("open", expected_revision)

    async def _expire(self, expires_at: float) -> None:
        try:
            await asyncio.sleep(max(0, expires_at - time.time()))
            value = await self.session()
            if not self.destroyed and not self._closing:
                await self._session_change("expired", value.revision)
        except asyncio.CancelledError:
            raise
        except (AgentError, ServiceError):
            # 并发关闭/销毁由其调用负责收束；TTL 的事务检查仍拒绝后续 Run。
            pass

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
        await self._close_expiry()
        async with self._session_lock:
            if not self._lease_lost:
                value = await self.session()
                if value.status != "destroyed":
                    await self.manager.sessions.transition(
                        self.session_id, self.manager.owner_id, value.revision, "destroyed"
                    )
        for child in list(self.manager._handles.values()):
            if not child.destroyed and (await child.session()).parent_session_id == self.session_id:
                await child.destroy()
        if consumer is not None and consumer is not asyncio.current_task():
            await asyncio.gather(consumer, return_exceptions=True)
        if self.current_run is not None and self.current_run.status not in TERMINAL_STATUSES:
            self.current_run = await self.runtime.stop(self.current_run.id)
        await self.runtime.close()
        if not self._lease_lost:
            await self.manager.sessions.release(self.session_id, self.manager.owner_id)
        await self._close_heartbeat()
        if self._durable_revision is not None and not self._lease_lost:
            async with self._durable_lock:
                await self.manager.store.delete(
                    self.agent_id, self.manager.owner_id, self._durable_revision
                )
        self.manager._handles.pop(self.agent_id, None)
        return self._result(self.current_run, "destroyed", "Agent 已销毁", "destroyed")

    async def _close_heartbeat(self) -> None:
        if self._heartbeat is not None:
            self._heartbeat.cancel()
            await asyncio.gather(self._heartbeat, return_exceptions=True)
            self._heartbeat = None

    async def _close_expiry(self) -> None:
        if self._expiry is not None and self._expiry is not asyncio.current_task():
            self._expiry.cancel()
            await asyncio.gather(self._expiry, return_exceptions=True)
            self._expiry = None

    async def detach(self) -> None:
        """正常进程关闭：收束当前生产，但保留 persistent 的稳定句柄身份。"""
        self._closing = True
        await self.stop()
        await self.runtime.close()
        await self._close_expiry()
        await self._close_heartbeat()
        if not self._lease_lost:
            await self.manager.sessions.release(self.session_id, self.manager.owner_id)
            await self.manager.persist(self, "detached", release=True)
        self.destroyed = True
        self.manager._handles.pop(self.agent_id, None)

    async def _completed_result(
        self,
        forced_status: AgentStatus | None = None,
        forced_error: str | None = None,
        forced_code: str | None = None,
        *,
        read_run: RunView | None = None,
        read_turn: int | None = None,
    ) -> ExecResult:
        selected = read_run or self.current_run
        if selected is None:
            return self._result(None, "failed", "Agent 尚未运行", "not_started")
        run = await self.runtime.repository.get_run(selected.id)
        if read_run is None:
            self.current_run = run
        messages = await self.runtime.repository.run_messages(run.id)
        reply = next(
            (message.content for message in reversed(messages) if message.role == "assistant"),
            "",
        )
        status: AgentStatus = forced_status or cast(AgentStatus, run.status)
        usage = await self.runtime.repository.usage_view(run.id)
        result = ExecResult(
            agent_id=self.agent_id,
            run_id=run.id,
            status=status,
            text=reply,
            error=forced_error or run.error,
            code=forced_code or run.error_code,
            parent_tool_call_id=self.parent_tool_call_id,
            turn=read_turn if read_turn is not None else self.turn,
            token_usage=usage.token_usage,
            usage_complete=usage.usage_complete,
        )
        if read_run is None:
            await self.manager.persist(self, run.status)
        return result

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
        tracer: Tracer | None = None,
    ) -> None:
        if not 1 <= max_agents <= 1024 or not 1 <= max_concurrent <= 64:
            raise ValueError("Agent 数量和并发必须为有效正数")
        if not 1 <= max_recursion_depth <= 16:
            raise ValueError("Agent 递归上限必须在 1–16 之间")
        self.repository = repository
        self.tracer = tracer
        self.max_agents = max_agents
        self.max_recursion_depth = max_recursion_depth
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._handles: dict[str, AgentHandle] = {}
        self._spawn_lock = asyncio.Lock()
        self._closed = False
        self.owner_id = str(uuid4())
        self.store = AgentHandleStore(repository.database)
        self.sessions = AgentSessionStore(repository.database)
        self.tasks = AgentTaskStore(repository.database, owner_id=self.owner_id)
        self.workspace_changes = WorkspaceChangeService(
            WorkspaceEditStore(repository.database, owner_id=self.owner_id)
        )
        self.directory_changes = DirectoryChangeService(self.workspace_changes)
        self.mixed_queries = MixedBatchQueryService(self.workspace_changes, self.directory_changes)
        self.commands = AgentCommandService(
            CommandStore(repository.database, owner_id=self.owner_id), self.workspace_changes
        )

    @staticmethod
    def definition_hash(definition: AgentDefinition) -> str:
        """只冻结公开定义；恢复仍要求宿主重供可信 provider/tools/policy。"""
        provider_class = type(definition.provider)
        payload = {
            "name": definition.name,
            "provider": definition.provider.name,
            "provider_class": f"{provider_class.__module__}.{provider_class.__qualname__}",
            "tools": [item.model_dump(mode="json") for item in definition.tools.list_tools()],
            "max_turns": definition.max_turns,
            "max_steps": definition.max_steps,
            "timeout_seconds": definition.timeout_seconds,
            "tool_timeout_seconds": definition.tool_timeout_seconds,
        }
        if isinstance(definition.provider, OpenAICompatibleProvider):
            payload["provider_configuration"] = provider_fingerprint(definition.provider)
        if (
            definition.context_config is not None
            and definition.context_config != ContextRunConfig()
        ):
            context = definition.context_config.model_dump(mode="json")
            defaults = ContextRunConfig()
            # 新确认字段的缺省值保持旧语义，不能使 0017 已保存的同一定义失效。
            # 显式启用/调整后仍进入指纹，禁止持久句柄静默扩大或切换配置。
            for field in (
                "compact_mode",
                "proposal_ttl_seconds",
                "notice_messages",
                "trigger_ratio",
            ):
                if context[field] == getattr(defaults, field):
                    del context[field]
            payload["context_config"] = context
        if definition.usage_budget is not None:
            payload["usage_budget"] = definition.usage_budget.model_dump(mode="json")
        if definition.profile_revision is not None:
            payload["profile_revision"] = definition.profile_revision
        if definition.instructions:
            payload["instructions"] = definition.instructions
        if definition.resource_version:
            payload["resource_version"] = definition.resource_version
        if definition.workspace is not None:
            payload["workspace"] = definition.workspace.manifest()
        if definition.execution is not None:
            payload["execution"] = definition.execution.resource.model_dump(mode="json")
        if definition.session_ttl_seconds is not None:
            payload["session_ttl_seconds"] = definition.session_ttl_seconds
        if definition.mode != "agent" and definition.profile_revision is None:
            payload["mode"] = definition.mode
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()

    async def persist(self, handle: AgentHandle, status: str, *, release: bool = False) -> None:
        if handle._durable_revision is None:
            return
        async with handle._durable_lock:
            try:
                stored = await self.store.save(
                    handle.agent_id,
                    self.owner_id,
                    handle._durable_revision,
                    turn=handle.turn,
                    current_run_id=handle.current_run.id if handle.current_run else None,
                    status=status,
                    release=release,
                )
            except Exception:
                handle._lease_lost = True
                raise
            handle._durable_revision = stored.revision

    async def _renew(self, handle: AgentHandle) -> None:
        try:
            while not handle.destroyed:
                await asyncio.sleep(self.store.lease_seconds / 3)
                async with handle._durable_lock:
                    if handle._durable_revision is not None:
                        await self.store.renew(
                            handle.agent_id, self.owner_id, handle._durable_revision
                        )
        except asyncio.CancelledError:
            raise
        except Exception:
            handle._lease_lost = True
            if handle._consumer is not None:
                handle._consumer.cancel()
            if handle.current_run is not None:
                await handle.runtime.stop(handle.current_run.id)

    async def restore(self, definition: AgentDefinition, stored: StoredAgentHandle) -> AgentHandle:
        async with self._spawn_lock:
            if (
                stored.name != definition.name
                or not stored.persistent
                or stored.definition_hash != self.definition_hash(definition)
            ):
                raise AgentError("agent_definition_changed", "持久 Agent 的可信定义不匹配")
            if stored.parent_agent_id is not None:
                parent = self._handles.get(stored.parent_agent_id)
                if parent is None or parent.destroyed:
                    raise AgentError("parent_agent_missing", "父 Agent 尚未恢复")
                self._restrict_workspace(
                    definition.workspace,
                    parent.runtime.workspace,
                    definition.tools,
                    parent.runtime.tools,
                )
                self._restrict_execution(
                    definition.execution,
                    parent.runtime.execution,
                    definition.tools,
                    parent.runtime.tools,
                )
                definition = replace(
                    definition,
                    policy=inherited_policy(definition.policy, parent.runtime.tool_policy),
                    tools=definition.tools.fork(controls=parent.runtime.tools),
                )
            return await self._spawn(
                replace(
                    definition,
                    agent_id=stored.agent_id,
                    persistent=True,
                    parent_agent_id=stored.parent_agent_id,
                    parent_tool_call_id=stored.parent_tool_call_id,
                ),
                stored=stored,
            )

    async def spawn(
        self,
        definition: AgentDefinition,
        *,
        parent_context: ToolContext | None = None,
        budget_account_ids: tuple[str, ...] = (),
    ) -> AgentHandle:
        async with self._spawn_lock:
            if parent_context is None and definition.parent_agent_id is not None:
                parent_handle = self._handles.get(definition.parent_agent_id)
                if parent_handle is None or parent_handle.destroyed:
                    raise AgentError("parent_agent_missing", "父 Agent 不存在")
                parent_context = ToolContext(
                    parent_handle.runtime.tools,
                    agent_id=parent_handle.agent_id,
                    session_id=parent_handle.session_id,
                    policy=parent_handle.runtime.tool_policy,
                    timeout=parent_handle.definition.timeout_seconds,
                    context_config=parent_handle.definition.context_config.model_dump(mode="json")
                    if parent_handle.definition.context_config
                    else None,
                    budget_account_ids=await parent_handle.runtime.ensure_usage_budget_binding(),
                    workspace=parent_handle.runtime.workspace,
                    execution=parent_handle.runtime.execution,
                )
            if parent_context is not None:
                self._restrict_workspace(
                    definition.workspace,
                    parent_context.workspace,
                    definition.tools,
                    parent_context.registry,
                )
                self._restrict_execution(
                    definition.execution,
                    parent_context.execution,
                    definition.tools,
                    parent_context.registry,
                )
                context_config = definition.context_config
                if parent_context.context_config is not None:
                    try:
                        parent_config = ContextRunConfig.model_validate(
                            parent_context.context_config
                        )
                        context_config = (context_config or parent_config).restricted_by(
                            parent_config
                        )
                    except ValueError as error:
                        raise AgentError("agent_context_budget", "父子上下文预算不兼容") from error
                definition = replace(
                    definition,
                    parent_agent_id=parent_context.agent_id,
                    parent_tool_call_id=parent_context.call_id,
                    policy=inherited_policy(definition.policy, parent_context.policy),
                    tools=definition.tools.fork(controls=parent_context.registry),
                    context_config=context_config,
                    timeout_seconds=definition.timeout_seconds
                    if parent_context.approval_handler is not None
                    and any(budget.active for budget in parent_context.execution_timeouts)
                    else min(definition.timeout_seconds, parent_context.timeout),
                    tool_timeout_seconds=min(
                        definition.tool_timeout_seconds,
                        definition.tools.default_timeout,
                        parent_context.timeout,
                    ),
                )
            return await self._spawn(
                definition, parent_context, budget_account_ids=budget_account_ids
            )

    @staticmethod
    def _restrict_workspace(
        child: WorkspaceBinding | None,
        parent: WorkspaceBinding | None,
        child_tools: ToolRegistry,
        parent_tools: ToolRegistry,
    ) -> None:
        if child is not None and (
            parent is None or child.fingerprint != parent.fingerprint or not parent.available
        ):
            raise AgentError("agent_workspace_forbidden", "子Agent不能扩大父工作区授权")
        if {item.name for item in child_tools.definitions if item.name in WORKSPACE_TOOL_NAMES} - {
            item.name for item in parent_tools.definitions if item.name in WORKSPACE_TOOL_NAMES
        }:
            raise AgentError("agent_workspace_forbidden", "子Agent不能扩大父文件工具授权")

    async def _spawn(
        self,
        definition: AgentDefinition,
        parent_context: ToolContext | None = None,
        *,
        stored: StoredAgentHandle | None = None,
        budget_account_ids: tuple[str, ...] = (),
    ) -> AgentHandle:
        if self._closed:
            raise AgentError("manager_closed", "AgentManager 已关闭")
        if definition.execution is not None:
            try:
                definition.execution.require()
            except ServiceError as error:
                raise AgentError(error.code, str(error)) from error
        elif any(item.name in COMMAND_TOOL_NAMES for item in definition.tools.definitions):
            raise AgentError("execution_unbound", "Agent命令工具缺少执行资源授权")
        if definition.workspace is not None:
            try:
                definition.workspace.require()
            except ServiceError as error:
                raise AgentError(error.code, str(error)) from error
            if not definition.workspace.workspace.writable and any(
                item.name in WORKSPACE_WRITE_TOOL_NAMES for item in definition.tools.definitions
            ):
                raise AgentError("workspace_read_only", "只读工作区不可登记修改工具")
        elif any(item.name in WORKSPACE_TOOL_NAMES for item in definition.tools.definitions):
            raise AgentError("workspace_unbound", "Agent文件工具缺少工作区授权")
        if any(item.name == "command_propose" for item in definition.tools.definitions) and (
            definition.workspace is None or not definition.workspace.workspace.writable
        ):
            raise AgentError("workspace_read_only", "命令产物提案需要可写工作区授权")
        if len(self._handles) >= self.max_agents:
            raise AgentError("agent_limit", "Agent 数量超过限制")
        definition = replace(definition.with_id(), tools=copy_tools(definition.tools))
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
        settings = Settings.model_validate(
            {
                "database_url": str(database.engine.url),
                "max_steps": definition.max_steps,
                "run_timeout_seconds": definition.timeout_seconds,
                "tool_timeout_seconds": definition.tool_timeout_seconds,
                "usage_budget": definition.usage_budget.model_dump(mode="json")
                if definition.usage_budget
                else None,
                **(
                    definition.context_config.settings_values() if definition.context_config else {}
                ),
            }
        )
        runtime = ChatRuntime(
            self.repository,
            definition.provider,
            copy_tools(definition.tools),
            settings,
            tool_policy=profile_policy(definition.mode, definition.policy),
            agent_id=definition.agent_id,
            agent_session_id=definition.agent_id,
            agent_owner_id=self.owner_id,
            task_store=self.tasks,
            workspace_changes=self.workspace_changes,
            directory_changes=self.directory_changes,
            workspace_mixed_queries=self.mixed_queries,
            workspace=definition.workspace,
            execution=definition.execution,
            commands=self.commands,
            allow_skill_tool=definition.profile_revision is None
            or any(tool.name == "read_skill" for tool in definition.tools.definitions),
            system_prompt=definition.instructions,
            agent_definition_hash=self.definition_hash(definition),
            approval_handler=parent_context.approval_handler if parent_context else None,
            question_handler=parent_context.question_handler if parent_context else None,
            interaction_publisher=parent_context.interaction_publisher
            if parent_context and not definition.persistent
            else None,
            execution_timeouts=parent_context.execution_timeouts
            if parent_context and not definition.persistent
            else (),
            budget_account_ids=tuple(
                dict.fromkeys(
                    (
                        *budget_account_ids,
                        *(parent_context.budget_account_ids if parent_context else ()),
                    )
                )
            ),
            budget_owner_kind="agent",
            budget_owner_id=definition.agent_id,
            tracer=parent_context.tracer
            if parent_context and parent_context.tracer
            else self.tracer,
        )
        topic = (
            await self.repository.get_topic(stored.topic_id)
            if stored
            else await self.repository.create_topic(f"Agent: {definition.name}")
        )
        if parent_context is not None and parent_context.skill_snapshot is not None:
            for batch in parent_context.skill_snapshot.batches():
                await self.repository.inject_skills(topic.id, batch)
        handle = AgentHandle(self, definition, runtime, topic.id)
        await runtime.ensure_usage_budget_binding()
        if definition.persistent:
            try:
                saved = (
                    await self.store.claim(stored.agent_id, self.owner_id, stored.revision)
                    if stored
                    else await self.store.create(
                        StoredAgentHandle(
                            agent_id=handle.agent_id,
                            name=definition.name,
                            topic_id=topic.id,
                            persistent=True,
                            turn=0,
                            current_run_id=None,
                            status="created",
                            definition_hash=self.definition_hash(definition),
                            revision=0,
                            owner_id=None,
                            lease_expires_at=None,
                            parent_agent_id=definition.parent_agent_id,
                            parent_tool_call_id=definition.parent_tool_call_id,
                            profile_revision=definition.profile_revision,
                        ),
                        self.owner_id,
                    )
                )
                handle._durable_revision = saved.revision
                handle.turn = saved.turn
                if saved.current_run_id is not None:
                    handle.current_run = await self.repository.get_run(saved.current_run_id)
                    if handle.current_run.topic_id != topic.id:
                        raise AgentError("agent_run_mismatch", "持久 Agent 的运行话题不匹配")
                handle._heartbeat = asyncio.create_task(self._renew(handle))
            except BaseException:
                await runtime.close()
                raise
        permissions: dict[str, JsonValue] = {
            "mode": definition.mode,
            "max_turns": definition.max_turns,
            "max_steps": definition.max_steps,
            "tool_names": [item.name for item in definition.tools.definitions],
            "context_config": definition.context_config.model_dump(mode="json")
            if definition.context_config
            else None,
            "usage_budget": definition.usage_budget.model_dump(mode="json")
            if definition.usage_budget
            else None,
            "resources": definition.resource_manifest,
        }
        if definition.workspace is not None:
            permissions["workspace"] = definition.workspace.manifest()
        if definition.execution is not None:
            permissions["execution"] = definition.execution.resource.model_dump(mode="json")
        try:
            if stored is not None:
                value = await self.sessions.attach(
                    handle.session_id, self.owner_id, self.definition_hash(definition), permissions
                )
                # Run 与 turn 先于 handle.current_run 持久更新，恢复以原子账本为准。
                handle.turn = max(handle.turn, value.turn)
                if value.current_run_id is not None:
                    handle.current_run = await self.repository.get_run(value.current_run_id)
            else:
                expires_at = (
                    time.time() + definition.session_ttl_seconds
                    if definition.session_ttl_seconds is not None
                    else None
                )
                parent_session_id = parent_context.session_id if parent_context else None
                if parent_session_id is None and definition.parent_agent_id is not None:
                    parent_session_id = self._handles[definition.parent_agent_id].session_id
                if parent_session_id is not None:
                    parent_value = await self.sessions.get(parent_session_id)
                    if parent_value.status != "open":
                        raise AgentError("agent_parent_session_closed", "父 Agent 会话不可执行")
                    if parent_value.expires_at is not None:
                        expires_at = min(
                            expires_at or parent_value.expires_at, parent_value.expires_at
                        )
                value = await self.sessions.create(
                    session_id=handle.session_id,
                    agent_id=handle.agent_id,
                    topic_id=topic.id,
                    name=definition.name,
                    persistent=definition.persistent,
                    profile_revision=definition.profile_revision,
                    definition_hash=self.definition_hash(definition),
                    parent_session_id=parent_session_id,
                    owner_id=self.owner_id,
                    expires_at=expires_at,
                    permissions=permissions,
                )
        except BaseException:
            await handle._close_heartbeat()
            await runtime.close()
            if handle._durable_revision is not None:
                await self.persist(handle, "detached", release=True)
            raise
        self._handles[handle.agent_id] = handle
        if value.expires_at is not None and value.status != "destroyed":
            handle._expiry = asyncio.create_task(handle._expire(value.expires_at))
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
        self.commands.assert_external_close()
        self.directory_changes.assert_external_close()
        async with self._spawn_lock:
            self._closed = True
        await self.commands.close()
        await self.directory_changes.close()
        await self.workspace_changes.close()
        for handle in list(self._handles.values()):
            if handle.definition.persistent:
                await handle.detach()
            else:
                await handle.destroy()

    @staticmethod
    def _restrict_execution(
        child: ExecutionBinding | None,
        parent: ExecutionBinding | None,
        child_tools: ToolRegistry,
        parent_tools: ToolRegistry,
    ) -> None:
        if child is not None and (
            parent is None or child.resource != parent.resource or not parent.available
        ):
            raise AgentError("agent_execution_forbidden", "子Agent不能扩大父执行资源授权")
        if {item.name for item in child_tools.definitions if item.name in COMMAND_TOOL_NAMES} - {
            item.name for item in parent_tools.definitions if item.name in COMMAND_TOOL_NAMES
        }:
            raise AgentError("agent_execution_forbidden", "子Agent不能扩大父命令工具权限")
