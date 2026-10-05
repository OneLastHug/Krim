"""Vercel AI SDK 的文本/完整流/结果语义，复用 Krim Python 运行时。"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from types import TracebackType
from uuid import uuid4

from pydantic import BaseModel, JsonValue, TypeAdapter, ValidationError

from krim_agent.config import Settings
from krim_agent.context_engine import ContextCompactRequest, ContextCompactResponse
from krim_agent.persistence import Database, KrimRepository
from krim_agent.prompt_context import MentionOccurrence
from krim_agent.prompt_nodes import PromptNode
from krim_agent.protocol.domain import ChatRequest, MessageView, RunView, ServiceError
from krim_agent.protocol.models import StreamFrame, check_json
from krim_agent.providers import ModelProvider, ModelToolCall, create_provider
from krim_agent.providers.factory import OwnedProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.skills import SkillBatch, SkillCatalogView, SkillReadResult
from krim_agent.tools import ToolRegistry
from krim_agent.tools.builtins import builtin_registry
from krim_agent.tools.core import ToolResult

from .types import (
    ContextCompactPart,
    FinishReason,
    GenerateResult,
    InteractionPart,
    SDKPart,
    SkillLoadedPart,
    StepFinish,
    StepResult,
    StreamError,
    StreamFinish,
    StreamStart,
    TextDelta,
    ToolCallPart,
    ToolDiscoveryPart,
    ToolResultPart,
)


class SDKOutputError(ValueError):
    """运行结果未能通过调用方声明的 Pydantic 输出校验。"""


def _finish(frame: StreamFrame) -> StreamFinish:
    data = frame.data if isinstance(frame.data, dict) else {}
    status = str(data.get("status", "failed"))
    reasons: dict[str, FinishReason] = {
        "completed": "stop",
        "aborted": "abort",
        "interrupted": "interrupted",
        "failed": "error",
    }
    reason = reasons.get(status, "error")
    code = data.get("code")
    error = data.get("error")
    if isinstance(code, str) and code in {
        "step_limit",
        "output_limit",
        "stream_limit",
        "tool_limit",
        "provider_output_limit",
    }:
        reason = "length"
    return StreamFinish(
        finish_reason=reason,
        status=status,
        error=error if isinstance(error, str) else None,
        code=code if isinstance(code, str) else None,
    )


def _part(frame: StreamFrame) -> SDKPart:
    data = frame.data if isinstance(frame.data, dict) else {}
    if frame.kind == "start":
        return StreamStart(provider=str(data.get("provider", "unknown")))
    if frame.kind == "end":
        return _finish(frame)
    if frame.kind == "error":
        return StreamError(
            code=str(data.get("code", "stream_error")), message=str(data.get("error", "流失败"))
        )
    kind = data.get("type")
    if kind == "skill_loaded":
        return SkillLoadedPart.model_validate(
            {key: data.get(key) for key in ("name", "resource", "step", "call_id")}
        )
    if kind == "tool_discovery":
        return ToolDiscoveryPart.model_validate(
            {key: data.get(key) for key in ("tools", "step", "call_id")}
        )
    if kind == "interaction_request":
        return InteractionPart.model_validate({"interaction": data.get("interaction")})
    if kind == "text_delta":
        return TextDelta.model_validate({"text": data.get("text")})
    if kind == "tool_call":
        return ToolCallPart.model_validate(
            {
                "tool_call_id": data.get("id"),
                "tool_name": data.get("name"),
                "input": data.get("arguments"),
            }
        )
    if kind == "tool_result":
        result = ToolResult.model_validate(
            {key: value for key, value in data.items() if key in ToolResult.model_fields}
        )
        return ToolResultPart.model_validate(
            {
                "tool_call_id": data.get("call_id"),
                "tool_name": data.get("name"),
                "result": result,
            }
        )
    if kind == "context_compact":
        return ContextCompactPart.model_validate(
            {
                "original_count": data.get("original_count"),
                "kept_count": data.get("kept_count"),
                "omitted_count": data.get("omitted_count"),
                "strategy": data.get("strategy"),
                "summary": data.get("summary"),
                "status": data.get("status", "completed"),
                "persisted": data.get("persisted", False),
                "source_count": data.get("source_count", 0),
                "source_until_message_id": data.get("source_until_message_id"),
                "source_branch_key": data.get("source_branch_key"),
            }
        )
    if kind == "step_finish":
        return StepFinish.model_validate(
            {"step": data.get("step"), "finish_reason": data.get("finish_reason")}
        )
    raise ServiceError("unsupported_sdk_event", "SDK 无法识别当前事件部件", 409)


def _steps(messages: list[MessageView]) -> list[StepResult]:
    steps: list[StepResult] = []
    current: StepResult | None = None
    for message in messages:
        if message.role == "assistant":
            calls_data = message.details.get("tool_calls", [])
            calls = (
                [ModelToolCall.model_validate(value) for value in calls_data]
                if isinstance(calls_data, list)
                else []
            )
            step = message.details.get("step")
            current = StepResult(
                step=step if isinstance(step, int) else len(steps),
                text=message.content,
                tool_calls=calls,
                finish_reason="tool-calls" if calls else "stop",
                partial=bool(message.details.get("partial", False)),
            )
            steps.append(current)
        elif message.role == "tool" and current is not None:
            result_data = {
                key: value
                for key, value in message.details.items()
                if key in ToolResult.model_fields
            }
            if "content" not in result_data:
                result_data["content"] = [{"type": "text", "text": message.content}]
            result = ToolResult.model_validate(result_data)
            current.tool_results.append(result)
            if result.code in {"cancelled", "incomplete"}:
                current.partial = True
                current.finish_reason = "error"
    return steps


class StreamTextResult:
    """持久事件的类型化异步视图；多次读取是回放，停止需显式 abort。"""

    def __init__(self, runtime: ChatRuntime, run: RunView) -> None:
        self.runtime = runtime
        self.run = run

    async def full_stream(self, from_seq: int = 0) -> AsyncGenerator[SDKPart, None]:
        state = await self.runtime.stream_state(self.run.id, from_seq)
        subscription = self.runtime.subscribe(state, from_seq)
        try:
            async for frame in subscription:
                if frame is not None:
                    yield _part(frame)
        finally:
            await subscription.aclose()

    async def text_stream(self, from_seq: int = 0) -> AsyncGenerator[str, None]:
        subscription = self.full_stream(from_seq)
        try:
            async for part in subscription:
                if isinstance(part, TextDelta):
                    yield part.text
        finally:
            await subscription.aclose()

    async def abort(self) -> RunView:
        return await self.runtime.stop(self.run.id)

    async def collect(self) -> GenerateResult:
        finish: StreamFinish | None = None
        async for part in self.full_stream():
            if isinstance(part, StreamFinish):
                finish = part
        if finish is None:
            raise ServiceError("incomplete_sdk_stream", "生成流没有终帧", 409)
        repository = self.runtime.repository
        messages = await repository.run_messages(self.run.id)
        steps = _steps(messages)
        response_messages = [message for message in messages if message.role != "user"]
        return GenerateResult(
            text=steps[-1].text if steps else "",
            run=await repository.get_run(self.run.id),
            finish_reason=finish.finish_reason,
            steps=steps,
            response_messages=response_messages,
            audits=await repository.audits(self.run.id),
            error=finish.error,
            code=finish.code,
        )

    async def output[Output: BaseModel](self, schema: type[Output]) -> Output:
        result = await self.collect()
        if result.finish_reason != "stop":
            raise SDKOutputError("只有正常完成的文本可读取结构化结果")
        try:
            check_json(TypeAdapter(JsonValue).validate_json(result.text))
            return schema.model_validate_json(result.text, strict=True)
        except (ValueError, ValidationError) as error:
            raise SDKOutputError("生成结果不是有效 JSON 或不符合输出 Schema") from error


class PythonAI:
    """借用或装配 Python ChatRuntime 的类型化生成门面。"""

    def __init__(
        self,
        runtime: ChatRuntime,
        *,
        owned_database: Database | None = None,
        owned_provider: OwnedProvider | None = None,
    ) -> None:
        self.runtime = runtime
        self._owned_database = owned_database
        self._owned_provider = owned_provider
        self._closed = False

    @classmethod
    async def create(
        cls,
        *,
        settings: Settings | None = None,
        provider: ModelProvider | None = None,
        tools: ToolRegistry | None = None,
    ) -> PythonAI:
        config = settings or Settings(database_url="sqlite+aiosqlite:///:memory:")
        database = Database(config.database_url)
        owned_provider: OwnedProvider | None = None
        try:
            if provider is None:
                owned_provider = create_provider(config)
                selected_provider: ModelProvider = owned_provider
            else:
                selected_provider = provider
            await database.initialize()
            repository = KrimRepository(database)
            await repository.recover_interrupted()
            runtime = ChatRuntime(
                repository,
                selected_provider,
                tools or builtin_registry(config.tool_timeout_seconds),
                config,
            )
            return cls(runtime, owned_database=database, owned_provider=owned_provider)
        except BaseException:
            try:
                await database.close()
            finally:
                if owned_provider is not None:
                    await owned_provider.close()
            raise

    async def __aenter__(self) -> PythonAI:
        if self._closed:
            raise ServiceError("sdk_closed", "SDK 已经关闭", 503)
        return self

    async def __aexit__(
        self,
        _type: type[BaseException] | None,
        _value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        await self.close()

    async def stream_text(
        self,
        prompt: str,
        *,
        topic_id: str | None = None,
        idempotency_key: str | None = None,
        parent_message_id: str | None = None,
        mentions: list[MentionOccurrence] | None = None,
        skill_names: list[str] | None = None,
        nodes: list[PromptNode] | None = None,
    ) -> StreamTextResult:
        if self._closed:
            raise ServiceError("sdk_closed", "SDK 已经关闭", 503)
        request = ChatRequest(
            text=prompt,
            idempotency_key=idempotency_key or str(uuid4()),
            parent_message_id=parent_message_id,
            mentions=mentions or [],
            skill_names=skill_names or [],
            nodes=nodes or [],
        )
        topic = topic_id or (await self.runtime.repository.create_topic(prompt[:80])).id
        run = await self.runtime.start(topic, request)
        return StreamTextResult(self.runtime, run)

    async def generate_text(
        self,
        prompt: str,
        *,
        topic_id: str | None = None,
        idempotency_key: str | None = None,
        parent_message_id: str | None = None,
        mentions: list[MentionOccurrence] | None = None,
        skill_names: list[str] | None = None,
        nodes: list[PromptNode] | None = None,
    ) -> GenerateResult:
        stream = await self.stream_text(
            prompt,
            topic_id=topic_id,
            idempotency_key=idempotency_key,
            parent_message_id=parent_message_id,
            mentions=mentions,
            skill_names=skill_names,
            nodes=nodes,
        )
        return await stream.collect()

    async def skills(self, topic_id: str) -> SkillCatalogView:
        return await self.runtime.repository.skill_catalog(topic_id)

    async def inject_skills(self, topic_id: str, request: SkillBatch) -> SkillCatalogView:
        return await self.runtime.repository.inject_skills(topic_id, request)

    async def remove_skill(self, topic_id: str, name: str | None = None) -> SkillCatalogView:
        return await self.runtime.repository.remove_skill(topic_id, name)

    async def read_skill(
        self, topic_id: str, name: str, resource: str | None = None
    ) -> SkillReadResult:
        return await self.runtime.repository.read_skill(topic_id, name, resource)

    async def compact(
        self, topic_id: str, request: ContextCompactRequest | None = None
    ) -> ContextCompactResponse:
        if self._closed:
            raise ServiceError("sdk_closed", "SDK 已经关闭", 503)
        return await self.runtime.compact(topic_id, request or ContextCompactRequest())

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owned_database is not None:
            try:
                await self.runtime.close()
            finally:
                try:
                    await self._owned_database.close()
                finally:
                    if self._owned_provider is not None:
                        await self._owned_provider.close()
