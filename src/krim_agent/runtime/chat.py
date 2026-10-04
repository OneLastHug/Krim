"""独立于订阅者的生产任务；取消后等待消息、审计与终态持久化。"""

from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict
from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Literal

from pydantic import JsonValue

from krim_agent.application.context import PersistentContext
from krim_agent.application.interactions import InteractionService
from krim_agent.config import Settings
from krim_agent.context_engine import (
    ContextCompactRequest,
    ContextCompactResponse,
    ContextEngine,
    ContextError,
    ContextOptions,
)
from krim_agent.persistence import KrimRepository
from krim_agent.protocol.domain import (
    TERMINAL_STATUSES,
    ChatRequest,
    EditRequest,
    RegenerateRequest,
    RunStatus,
    RunView,
    ServiceError,
)
from krim_agent.protocol.models import StreamFrame
from krim_agent.providers import ModelProvider, ModelText, ModelToolCall, ProviderError
from krim_agent.runtime.skills import definition as skill_tool_definition
from krim_agent.skills import SkillReadResult, SkillSnapshot
from krim_agent.tools import ToolContext, ToolRegistry
from krim_agent.tools.core import (
    ApprovalHandler,
    Decision,
    InteractionPublisher,
    Policy,
    QuestionHandler,
    ToolDefinition,
    ToolExecutionBudget,
)
from krim_agent.tools.discovery import ToolDiscovery

logger = logging.getLogger(__name__)


@dataclass
class RunState:
    run: RunView
    frames: list[StreamFrame] = field(default_factory=list)
    event_writer: Callable[[StreamFrame], Awaitable[StreamFrame]] | None = None
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)
    started: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task[None] | None = None
    finalizing: bool = False
    terminal: bool = False
    cancelled: bool = False
    draft: str = ""
    draft_calls: list[ModelToolCall] = field(default_factory=list)
    pending_calls: list[ModelToolCall] = field(default_factory=list)
    seen_calls: set[str] = field(default_factory=set)
    publish_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    step: int = 0
    discovery: ToolDiscovery | None = None
    skill_snapshot: SkillSnapshot | None = None

    async def publish(
        self, kind: Literal["start", "chunk", "end", "error"], data: JsonValue = None
    ) -> None:
        async with self.publish_lock:
            frame = StreamFrame(stream_id=self.run.id, seq=len(self.frames), kind=kind, data=data)

            async def commit() -> None:
                stored = await self.event_writer(frame) if self.event_writer is not None else frame
                async with self.condition:
                    self.frames.append(stored)
                    self.condition.notify_all()

            write = asyncio.create_task(commit())
            try:
                await asyncio.shield(write)
            except asyncio.CancelledError:
                await write
                raise


class ChatRuntime:
    def __init__(
        self,
        repository: KrimRepository,
        provider: ModelProvider,
        tools: ToolRegistry,
        settings: Settings,
        *,
        tool_policy: Policy | None = None,
        agent_id: str | None = None,
        approval_handler: ApprovalHandler | None = None,
        question_handler: QuestionHandler | None = None,
        interaction_publisher: InteractionPublisher | None = None,
        execution_timeouts: tuple[ToolExecutionBudget, ...] = (),
    ) -> None:
        self.repository = repository
        self.provider = provider
        self.tools = tools
        self.settings = settings
        self.tool_policy = tool_policy
        if self.tool_policy is None and settings.interactive_tools:
            self.tool_policy = self._confirm_writes
        self.agent_id = agent_id
        self.interactions = InteractionService(
            repository, seconds=settings.interaction_timeout_seconds
        )
        self.approval_handler = approval_handler or self.interactions.approve
        self.question_handler = question_handler or self.interactions.question
        self.interaction_publisher = interaction_publisher
        self.execution_timeouts = execution_timeouts
        self._lock = asyncio.Lock()
        self._runs: OrderedDict[str, RunState] = OrderedDict()
        self._closed = False
        self.context_engine = ContextEngine()
        self.persistent_context = PersistentContext(repository, self.context_engine)
        existing_skill_tool = next(
            (
                definition
                for definition in self.tools.definitions
                if definition.name == "read_skill"
            ),
            None,
        )
        if existing_skill_tool is not None and existing_skill_tool is not skill_tool_definition():
            raise ValueError("read_skill 是运行时保留工具名称")
        if existing_skill_tool is None:
            self.tools.register(skill_tool_definition())

    async def start(self, topic_id: str, request: ChatRequest) -> RunView:
        return await self._start(topic_id, request)

    @staticmethod
    async def _confirm_writes(definition: ToolDefinition, _context: ToolContext) -> Decision:
        return "allow" if definition.risk == "read" else "ask"

    async def edit(self, topic_id: str, message_id: str, request: EditRequest) -> RunView:
        return await self._start(topic_id, request, action="edit", source_message_id=message_id)

    async def regenerate(
        self, topic_id: str, message_id: str, request: RegenerateRequest
    ) -> RunView:
        return await self._start(
            topic_id, request, action="regenerate", source_message_id=message_id
        )

    async def compact(
        self, topic_id: str, request: ContextCompactRequest
    ) -> ContextCompactResponse:
        if self._closed:
            raise ServiceError("runtime_closed", "运行时已经关闭", 503)
        budgets = (request.max_messages, request.max_chars, request.max_tokens)
        if all(value is None for value in budgets):
            budgets = (
                self.settings.context_max_messages,
                self.settings.context_max_chars,
                self.settings.context_max_tokens,
            )
            if all(value is None for value in budgets):
                budgets = (20, None, None)
        outcome = await self.persistent_context.compact(
            topic_id,
            ContextOptions(
                strategy=request.strategy,
                policy=request.policy,
                max_messages=budgets[0],
                max_chars=budgets[1],
                max_tokens=budgets[2],
            ),
            confirm=request.confirm,
            expected_branch_key=request.expected_branch_key,
            timeout_seconds=self.settings.run_timeout_seconds,
        )
        sources = outcome.result.source_message_ids
        return ContextCompactResponse(
            topic_id=topic_id,
            status=outcome.status,
            branch_key=outcome.branch_key,
            source_branch_key=outcome.source_branch_key,
            source_until_message_id=sources[-1] if sources else None,
            reason=outcome.reason,
            persisted=outcome.persisted,
            options=outcome.options,
            **outcome.result.model_dump(),
        )

    async def _start(
        self,
        topic_id: str,
        request: ChatRequest | EditRequest | RegenerateRequest,
        *,
        action: Literal["send", "edit", "regenerate"] = "send",
        source_message_id: str | None = None,
    ) -> RunView:
        async with self._lock:
            if self._closed:
                raise ServiceError("runtime_closed", "运行时已经关闭", 503)
            if self.persistent_context.is_busy(topic_id):
                raise ServiceError("context_busy", "话题正在生成手动压缩结果", 409)
            active = sum(
                state.task is not None and not state.terminal for state in self._runs.values()
            )
            run, created = await self.repository.begin_run(
                topic_id,
                request,
                allow_new=active < self.settings.max_active_runs,
                action=action,
                source_message_id=source_message_id,
            )
            if not created:
                return run
            state = RunState(run, event_writer=self.repository.append_event)
            self._runs[run.id] = state
            state.task = asyncio.create_task(self._drive(state), name=f"krim-run-{run.id}")
            await state.started.wait()
            self._evict()
            return await self.repository.get_run(run.id)

    def _evict(self) -> None:
        for run_id, state in list(self._runs.items()):
            if len(self._runs) <= 128:
                break
            if state.terminal:
                del self._runs[run_id]

    async def _drive(self, state: RunState) -> None:
        status: RunStatus = "completed"
        error: str | None = None
        error_code: str | None = None
        state.started.set()
        try:
            await self.repository.set_running(state.run.id)
            await state.publish("start", {"provider": self.provider.name})
            snapshot = asyncio.create_task(self.repository.run_skill_snapshot(state.run.id))
            try:
                state.skill_snapshot = await asyncio.shield(snapshot)
            except asyncio.CancelledError:
                await snapshot
                raise
            async with asyncio.timeout(self.settings.run_timeout_seconds):
                await self._loop(state)
        except asyncio.CancelledError:
            status, error = "aborted", "运行已停止"
            error_code = "cancelled"
        except TimeoutError:
            status, error = "failed", "运行超过时间预算"
            error_code = "run_timeout"
        except ServiceError as exc:
            status, error = "failed", str(exc)
            error_code = exc.code
        except ProviderError as exc:
            status, error = "failed", str(exc)
            error_code = exc.code
        except ContextError as exc:
            status, error = "failed", str(exc)
            error_code = exc.code
        except Exception:
            logger.exception("运行失败: %s", state.run.id)
            status, error = "failed", "模型或运行时执行失败"
            error_code = "execution_error"
        finally:
            state.finalizing = True
            try:
                if state.draft_calls:
                    await self.repository.add_message(
                        state.run,
                        "assistant",
                        state.draft,
                        {
                            "partial": True,
                            "step": state.step,
                            "tool_calls": [
                                call.model_dump(mode="json") for call in state.draft_calls
                            ],
                        },
                    )
                    state.draft = ""
                    state.pending_calls.extend(state.draft_calls)
                    state.draft_calls.clear()
                for pending_call in state.pending_calls:
                    await self.repository.add_message(
                        state.run,
                        "tool",
                        "工具未完成",
                        {
                            "call_id": pending_call.id,
                            "name": pending_call.name,
                            "is_error": True,
                            "code": "cancelled" if status == "aborted" else "incomplete",
                        },
                    )
                state.pending_calls.clear()
                if state.draft:
                    await self.repository.add_message(
                        state.run,
                        "assistant",
                        state.draft,
                        {"partial": status != "completed", "step": state.step},
                    )
                    state.draft = ""
                audits = self.tools.audits_for_run(state.run.id)
                await self.repository.finish(
                    state.run.id, status, error, audits, error_code=error_code
                )
                self.tools.clear_run_audits(state.run.id)
                await state.publish("end", {"status": status, "error": error, "code": error_code})
            finally:
                state.terminal = True
                async with state.condition:
                    state.condition.notify_all()

    async def _loop(self, state: RunState) -> None:
        assert state.skill_snapshot is not None
        try:
            definitions = [
                definition
                for definition in self.tools.definitions
                if definition.name != "read_skill" or state.skill_snapshot.skills
            ]
            state.discovery = ToolDiscovery(definitions)
        except ValueError as error:
            raise ServiceError(
                "tool_catalog_conflict", "工具目录与按需搜索名称冲突", 409
            ) from error
        for _step in range(self.settings.max_steps):
            state.step = _step
            history = await self.repository.list_messages(state.run.topic_id)
            context = await self.persistent_context.prepare(
                state.run.topic_id,
                history,
                ContextOptions(
                    strategy=self.settings.context_strategy,
                    policy=self.settings.context_policy,
                    max_messages=self.settings.context_max_messages,
                    max_chars=self.settings.context_max_chars,
                    max_tokens=self.settings.context_max_tokens,
                ),
            )
            messages = context.messages
            if context.compacted:
                await state.publish(
                    "chunk",
                    {
                        "type": "context_compact",
                        "original_count": context.original_count,
                        "kept_count": len(context.messages),
                        "omitted_count": context.omitted_count,
                        "strategy": context.strategy,
                        "summary": context.summary,
                        "status": "completed",
                        "persisted": (
                            context.summary is not None
                            and context.strategy in {"summarize_then_truncate", "annotated_compact"}
                            and context.policy != "never"
                        ),
                        "source_count": len(context.source_message_ids),
                        "source_until_message_id": (
                            context.source_message_ids[-1] if context.source_message_ids else None
                        ),
                        "source_branch_key": (
                            self.repository.context_branch_key(
                                history[: len(context.source_message_ids)]
                            )
                            if context.source_message_ids
                            else None
                        ),
                    },
                )
            calls: list[ModelToolCall] = []
            assert state.discovery is not None
            tool_step = state.discovery.prepare()
            provider_stream = self.provider.stream(messages, list(tool_step.definitions.values()))
            try:
                async for delta in provider_stream:
                    if isinstance(delta, ModelText):
                        if len(state.draft) + len(delta.text) > 8192:
                            raise ServiceError("output_limit", "单轮输出超过限制")
                        state.draft += delta.text
                        await state.publish("chunk", {"type": "text_delta", "text": delta.text})
                    else:
                        if delta.id in state.seen_calls:
                            raise ServiceError("duplicate_tool_call", "工具调用 ID 重复")
                        state.seen_calls.add(delta.id)
                        if len(calls) >= 8:
                            raise ServiceError("tool_limit", "单轮工具调用超过限制")
                        calls.append(delta)
                        state.draft_calls.append(delta)
                        await state.publish(
                            "chunk", {"type": "tool_call", **delta.model_dump(mode="json")}
                        )
                    if len(state.frames) >= 2048:
                        raise ServiceError("stream_limit", "流事件超过限制")
            finally:
                close_stream = getattr(provider_stream, "aclose", None)
                if close_stream is not None:
                    await close_stream()
            if not calls:
                if not state.draft:
                    raise ServiceError("no_output", "模型没有输出文本或工具请求")
                await self.repository.add_message(
                    state.run, "assistant", state.draft, {"step": _step, "partial": False}
                )
                state.draft = ""
                await state.publish(
                    "chunk", {"type": "step_finish", "step": _step, "finish_reason": "stop"}
                )
                return
            await self.repository.add_message(
                state.run,
                "assistant",
                state.draft,
                {"tool_calls": [call.model_dump(mode="json") for call in calls], "step": _step},
            )
            state.draft = ""
            state.draft_calls.clear()
            state.pending_calls = list(calls)

            async def publish_interaction(data: dict[str, JsonValue]) -> None:
                await state.publish("chunk", data)
                if self.interaction_publisher is not None:
                    await self.interaction_publisher(data)

            for call in calls:
                result = await self.tools.invoke(
                    call.name,
                    call.arguments,
                    ToolContext(
                        self.tools,
                        run_id=state.run.id,
                        topic_id=state.run.topic_id,
                        timeout=self.settings.tool_timeout_seconds,
                        call_id=call.id,
                        policy=self.tool_policy,
                        agent_id=self.agent_id,
                        approval_handler=self.approval_handler,
                        question_handler=self.question_handler,
                        interaction_publisher=publish_interaction,
                        execution_timeouts=self.execution_timeouts,
                        model_tools=tool_step.bindings(),
                        skill_snapshot=state.skill_snapshot.model_copy(deep=True)
                        if state.skill_snapshot
                        else None,
                    ),
                )
                await self.repository.add_message(
                    state.run,
                    "tool",
                    result.content[0].text,
                    {"call_id": call.id, "name": call.name, **result.model_dump(mode="json")},
                )
                state.pending_calls.remove(call)
                added = state.discovery.consume(tool_step, call.id, call.name, result)
                await state.publish(
                    "chunk",
                    {
                        "type": "tool_result",
                        "call_id": call.id,
                        "name": call.name,
                        **result.model_dump(mode="json"),
                    },
                )
                if added:
                    await state.publish(
                        "chunk",
                        {
                            "type": "tool_discovery",
                            "tools": list(added),
                            "step": _step,
                            "call_id": call.id,
                        },
                    )
                if (
                    call.name == "read_skill"
                    and not result.is_error
                    and isinstance(result.data, dict)
                ):
                    loaded = SkillReadResult.model_validate(result.data)
                    try:
                        expected = state.skill_snapshot.read(loaded.name, loaded.resource)
                    except KeyError:
                        expected = None
                    if expected == loaded:
                        await state.publish(
                            "chunk",
                            {
                                "type": "skill_loaded",
                                "name": loaded.name,
                                "resource": loaded.resource,
                                "step": _step,
                                "call_id": call.id,
                            },
                        )
            await state.publish(
                "chunk", {"type": "step_finish", "step": _step, "finish_reason": "tool-calls"}
            )
        raise ServiceError("step_limit", "模型步骤超过预算")

    async def stop(self, run_id: str) -> RunView:
        async with self._lock:
            run = await self.repository.get_run(run_id)
            state = self._runs.get(run_id)
            if run.status in TERMINAL_STATUSES:
                if state is None or state.task is None:
                    return run
                task = state.task
            else:
                if state is None or state.task is None:
                    raise ServiceError("run_unavailable", "运行不在当前进程中", 409)
                if not state.cancelled and not state.finalizing:
                    await self.repository.request_stop(run_id)
                    state.cancelled = True
                    if not state.finalizing:
                        state.task.cancel()
                task = state.task
        await asyncio.shield(task)
        return await self.repository.get_run(run_id)

    def production_task(self, run_id: str) -> asyncio.Task[None] | None:
        """可信组合层只读借用生产任务，用于资源名额的真实生命周期绑定。"""
        state = self._runs.get(run_id)
        return state.task if state is not None else None

    async def stream_state(self, run_id: str, from_seq: int) -> RunState:
        run = await self.repository.get_run(run_id)
        state = self._runs.get(run_id)
        if state is None or state.task is None:
            frames = await self.repository.ensure_terminal_event(run_id)
            if not frames and run.status in TERMINAL_STATUSES:
                raise ServiceError("stream_gone", "该运行没有可恢复的持久事件", 410)
            state = RunState(
                run,
                frames=frames,
                event_writer=self.repository.append_event,
                terminal=bool(frames and frames[-1].kind in {"end", "error"}),
            )
            state.started.set()
            self._runs[run_id] = state
            self._evict()
        summary = await self.repository.event_summary(run_id)
        if from_seq < 0 or from_seq > max(len(state.frames), summary.count):
            raise ServiceError("invalid_cursor", "流游标超出已有事件", 416)
        return state

    async def subscribe(
        self, state: RunState, from_seq: int = 0, heartbeat: float = 15
    ) -> AsyncGenerator[StreamFrame | None, None]:
        cursor = from_seq
        heartbeat_at = asyncio.get_running_loop().time()
        while True:
            if state.task is None and not state.terminal:
                state.frames = await self.repository.ensure_terminal_event(state.run.id)
                current_run = await self.repository.get_run(state.run.id)
                state.terminal = bool(state.frames and state.frames[-1].kind in {"end", "error"})
                if not state.frames and current_run.status in TERMINAL_STATUSES:
                    raise ServiceError("stream_gone", "该运行没有可恢复的持久事件", 410)
            async with state.condition:
                if cursor >= len(state.frames) and not state.terminal:
                    try:
                        await asyncio.wait_for(
                            state.condition.wait(),
                            timeout=heartbeat if state.task else min(heartbeat, 0.1),
                        )
                    except TimeoutError:
                        pass
                frames = state.frames[cursor:]
                finished = state.terminal
            for frame in frames:
                cursor = frame.seq + 1
                heartbeat_at = asyncio.get_running_loop().time()
                yield frame
            if finished:
                return
            if not frames and asyncio.get_running_loop().time() - heartbeat_at >= heartbeat:
                heartbeat_at = asyncio.get_running_loop().time()
                yield None

    async def close(self) -> None:
        self._closed = True
        for state in list(self._runs.values()):
            if state.task is not None and not state.terminal:
                await self.stop(state.run.id)
        await self.tools.close()
