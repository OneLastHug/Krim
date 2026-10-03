"""独立于订阅者的生产任务；取消后等待消息、审计与终态持久化。"""

from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Literal

from pydantic import JsonValue

from krim_agent.config import Settings
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
from krim_agent.providers import ModelProvider, ModelText, ModelToolCall
from krim_agent.tools import ToolContext, ToolRegistry

logger = logging.getLogger(__name__)


@dataclass
class RunState:
    run: RunView
    frames: list[StreamFrame] = field(default_factory=list)
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

    async def publish(
        self, kind: Literal["start", "chunk", "end", "error"], data: JsonValue = None
    ) -> None:
        frame = StreamFrame(stream_id=self.run.id, seq=len(self.frames), kind=kind, data=data)
        async with self.condition:
            self.frames.append(frame)
            self.condition.notify_all()


class ChatRuntime:
    def __init__(
        self,
        repository: KrimRepository,
        provider: ModelProvider,
        tools: ToolRegistry,
        settings: Settings,
    ) -> None:
        self.repository = repository
        self.provider = provider
        self.tools = tools
        self.settings = settings
        self._lock = asyncio.Lock()
        self._runs: OrderedDict[str, RunState] = OrderedDict()
        self._closed = False

    async def start(self, topic_id: str, request: ChatRequest) -> RunView:
        return await self._start(topic_id, request)

    async def edit(self, topic_id: str, message_id: str, request: EditRequest) -> RunView:
        return await self._start(topic_id, request, action="edit", source_message_id=message_id)

    async def regenerate(
        self, topic_id: str, message_id: str, request: RegenerateRequest
    ) -> RunView:
        return await self._start(
            topic_id, request, action="regenerate", source_message_id=message_id
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
            active = sum(not state.terminal for state in self._runs.values())
            run, created = await self.repository.begin_run(
                topic_id,
                request,
                allow_new=active < self.settings.max_active_runs,
                action=action,
                source_message_id=source_message_id,
            )
            if not created:
                return run
            state = RunState(run)
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
        state.started.set()
        try:
            await self.repository.set_running(state.run.id)
            await state.publish("start", {"provider": self.provider.name})
            async with asyncio.timeout(self.settings.run_timeout_seconds):
                await self._loop(state)
        except asyncio.CancelledError:
            status, error = "aborted", "运行已停止"
        except TimeoutError:
            status, error = "failed", "运行超过时间预算"
        except ServiceError as exc:
            status, error = "failed", str(exc)
        except Exception:
            logger.exception("运行失败: %s", state.run.id)
            status, error = "failed", "模型或运行时执行失败"
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
                        state.run, "assistant", state.draft, {"partial": status != "completed"}
                    )
                    state.draft = ""
                audits = self.tools.audits_for_run(state.run.id)
                await self.repository.finish(state.run.id, status, error, audits)
                self.tools.clear_run_audits(state.run.id)
                await state.publish("end", {"status": status, "error": error})
            finally:
                state.terminal = True
                async with state.condition:
                    state.condition.notify_all()

    async def _loop(self, state: RunState) -> None:
        for _step in range(self.settings.max_steps):
            messages = await self.repository.list_messages(state.run.topic_id)
            calls: list[ModelToolCall] = []
            async for delta in self.provider.stream(messages, self.tools.definitions):
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
            if not calls:
                return
            await self.repository.add_message(
                state.run,
                "assistant",
                state.draft,
                {"tool_calls": [call.model_dump(mode="json") for call in calls]},
            )
            state.draft = ""
            state.draft_calls.clear()
            state.pending_calls = list(calls)
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
                    ),
                )
                await self.repository.add_message(
                    state.run,
                    "tool",
                    result.content[0].text,
                    {"call_id": call.id, "name": call.name, **result.model_dump(mode="json")},
                )
                state.pending_calls.remove(call)
                await state.publish(
                    "chunk",
                    {
                        "type": "tool_result",
                        "call_id": call.id,
                        "name": call.name,
                        **result.model_dump(mode="json"),
                    },
                )
        raise ServiceError("step_limit", "模型步骤超过预算")

    async def stop(self, run_id: str) -> RunView:
        async with self._lock:
            run = await self.repository.get_run(run_id)
            if run.status in TERMINAL_STATUSES:
                return run
            state = self._runs.get(run_id)
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

    async def stream_state(self, run_id: str, from_seq: int) -> RunState:
        await self.repository.get_run(run_id)
        state = self._runs.get(run_id)
        if state is None:
            raise ServiceError("stream_gone", "流缓冲已释放；请读取持久化消息", 410)
        if from_seq < 0 or from_seq > len(state.frames):
            raise ServiceError("invalid_cursor", "流游标超出已有事件", 416)
        return state

    async def subscribe(
        self, state: RunState, from_seq: int = 0, heartbeat: float = 15
    ) -> AsyncIterator[StreamFrame | None]:
        cursor = from_seq
        while True:
            async with state.condition:
                if cursor >= len(state.frames) and not state.terminal:
                    try:
                        await asyncio.wait_for(state.condition.wait(), timeout=heartbeat)
                    except TimeoutError:
                        pass
                frames = state.frames[cursor:]
                finished = state.terminal
            for frame in frames:
                cursor = frame.seq + 1
                yield frame
            if finished:
                return
            if not frames:
                yield None

    async def close(self) -> None:
        self._closed = True
        for state in list(self._runs.values()):
            if not state.terminal:
                await self.stop(state.run.id)
        await self.tools.close()
