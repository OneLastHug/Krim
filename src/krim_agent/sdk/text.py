"""Vercel AI SDK 的文本/完整流/结果语义，复用 Krim Python 运行时。"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections.abc import AsyncGenerator, Sequence
from types import TracebackType
from typing import TYPE_CHECKING
from uuid import uuid4

from pydantic import BaseModel, ValidationError

from krim_agent.application.streams import MultiplexReader
from krim_agent.config import Settings
from krim_agent.context_engine import (
    ContextCompactRequest,
    ContextCompactResponse,
    ContextProposalDecision,
    ContextProposalView,
)
from krim_agent.persistence import Database, KrimRepository
from krim_agent.prompt_context import MentionOccurrence
from krim_agent.prompt_nodes import PromptNode
from krim_agent.protocol.domain import (
    TERMINAL_STATUSES,
    ChatRequest,
    MessageView,
    RunView,
    ServiceError,
)
from krim_agent.protocol.json_data import parse_json
from krim_agent.protocol.models import StreamFrame
from krim_agent.providers import ModelProvider, ModelToolCall, create_provider
from krim_agent.providers.factory import OwnedProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.skills import SkillBatch, SkillCatalogView, SkillReadResult
from krim_agent.streams import ActiveRunView, MultiplexRequest, RunStreamFrame
from krim_agent.tools import ToolRegistry
from krim_agent.tools.builtins import builtin_registry
from krim_agent.tools.core import ToolResult
from krim_agent.tools.output import OutputContract, OutputSchema

from .types import (
    ContextBudgetPart,
    ContextCompactPart,
    ContextNoticePart,
    ContextProposalPart,
    FinishReason,
    GenerateObjectResult,
    GenerateResult,
    InteractionPart,
    ModelStepPart,
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
    UsageBudgetPart,
    UsagePart,
)

if TYPE_CHECKING:
    from krim_agent.application.agents import AgentService
    from krim_agent.evaluation import EvalConfig, EvalDataset, EvalReport
    from krim_agent.evaluation.scorers import EvalScorer
    from krim_agent.evaluation.service import EvalRunHandle, EvaluationService


class SDKOutputError(ValueError):
    """运行结果未能通过调用方声明的 Pydantic 输出校验。"""

    def __init__(self, message: str, *, result: GenerateResult | None = None) -> None:
        super().__init__(message)
        self.result = result


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
        return StreamStart.model_validate(
            {
                "provider": str(data.get("provider", "unknown")),
                "context_config": data.get("context_config"),
                "trace_context": data.get("trace_context"),
            }
        )
    if frame.kind == "end":
        return _finish(frame)
    if frame.kind == "error":
        return StreamError(
            code=str(data.get("code", "stream_error")), message=str(data.get("error", "流失败"))
        )
    kind = data.get("type")
    if kind == "context_compact_proposal":
        return ContextProposalPart.model_validate(
            {"phase": data.get("phase"), "proposal": data.get("proposal")}
        )
    if kind == "context_notice":
        return ContextNoticePart.model_validate(
            {key: data.get(key) for key in ("step", "message_count", "threshold")}
        )
    if kind == "usage_budget":
        return UsageBudgetPart.model_validate({"request": data.get("request")})
    if kind == "context_budget":
        return ContextBudgetPart.model_validate(
            {key: value for key, value in data.items() if key != "type"}
        )
    if kind == "model_step":
        return ModelStepPart.model_validate(
            {key: data.get(key) for key in ("step", "phase", "provider", "completed")}
        )
    if kind == "token_usage":
        return UsagePart.model_validate(
            {
                "step": data.get("step"),
                "usage": data.get("usage"),
                "final": data.get("final", False),
            }
        )
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
                "tokenizer": data.get("tokenizer", "approx-v1"),
                "estimated_tokens": data.get("estimated_tokens"),
                "reserved_output_tokens": data.get("reserved_output_tokens", 0),
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
        from .snapshots import read_generation_result

        return await read_generation_result(self.runtime.repository, self.run.id)

    async def output[Output: BaseModel](self, schema: type[Output]) -> Output:
        result = await self.collect()
        if result.finish_reason != "stop":
            raise SDKOutputError("只有正常完成的文本可读取结构化结果")
        try:
            parse_json(result.text)
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
        self._evaluation_service: EvaluationService | None = None
        self._agent_service: AgentService | None = None

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
            from krim_agent.persistence.agents import AgentHandleStore

            if any(
                item.owner_id is not None and (item.lease_expires_at or 0) > time.time()
                for item in await AgentHandleStore(database).list()
            ):
                raise ServiceError("agent_inventory_busy", "持久Agent仍由其他生产者持有", 409)
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
        budget_account_ids: tuple[str, ...] = (),
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
        run = await self.runtime.start(topic, request, budget_account_ids=budget_account_ids)
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
        budget_account_ids: tuple[str, ...] = (),
    ) -> GenerateResult:
        stream = await self.stream_text(
            prompt,
            topic_id=topic_id,
            idempotency_key=idempotency_key,
            parent_message_id=parent_message_id,
            mentions=mentions,
            skill_names=skill_names,
            nodes=nodes,
            budget_account_ids=budget_account_ids,
        )
        return await stream.collect()

    async def read_result(self, run_id: str) -> GenerateResult:
        """只读取已终态的持久生成结果，不再次调用模型或执行工具。"""
        if self._closed:
            raise ServiceError("sdk_closed", "SDK 已经关闭", 503)
        run = await self.runtime.repository.get_run(run_id)
        if run.status not in TERMINAL_STATUSES:
            raise ServiceError("result_not_terminal", "运行尚未终态，不能只读评估结果", 409)
        from .snapshots import read_generation_result

        return await read_generation_result(self.runtime.repository, run.id)

    async def generate_object(
        self,
        prompt: str,
        *,
        schema: OutputSchema,
        topic_id: str | None = None,
        idempotency_key: str | None = None,
        budget_account_ids: tuple[str, ...] = (),
    ) -> GenerateObjectResult:
        """声明 JSON 输出、执行一次真实生成并校验；失败保留 Run，不自动修复/重试。"""
        if self._closed:
            raise ServiceError("sdk_closed", "SDK 已经关闭", 503)
        if not isinstance(prompt, str) or not prompt.strip():
            raise ValueError("结构化生成需要非空文本提示")
        contract = OutputContract(schema)
        declaration = json.dumps(
            contract.schema(), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        request = (
            "请只输出符合以下 JSON Schema 的 JSON，不加代码围栏。\n"
            f"Schema：{declaration}\n请求：{prompt}"
        )
        if len(request) > 8192:
            raise ValueError("结构化提示与 Schema 超过输入容量")
        start = asyncio.create_task(
            self.stream_text(
                request,
                topic_id=topic_id,
                idempotency_key=idempotency_key,
                budget_account_ids=budget_account_ids,
            )
        )
        stream: StreamTextResult | None = None
        try:
            stream = await asyncio.shield(start)
            result = await stream.collect()
        except BaseException:

            async def cleanup() -> None:
                nonlocal stream
                if stream is None:
                    try:
                        stream = await start
                    except Exception:
                        return
                await stream.abort()

            closing = asyncio.create_task(cleanup())
            try:
                await asyncio.shield(closing)
            except asyncio.CancelledError:
                await closing
            raise
        if result.finish_reason != "stop":
            raise SDKOutputError("结构化生成没有正常完成", result=result)
        try:
            value = parse_json(result.text)
            contract.validate(value)
            if isinstance(schema, type) and issubclass(schema, BaseModel):
                schema.model_validate_json(result.text, strict=True)
        except ValueError:
            raise SDKOutputError("生成结果不符合声明的 JSON 输出合约", result=result) from None
        return GenerateObjectResult(
            value=value,
            generation=result,
            schema_fingerprint=hashlib.sha256(declaration.encode("utf-8")).hexdigest(),
        )

    async def evaluate(
        self,
        dataset: EvalDataset,
        *,
        scorers: Sequence[EvalScorer] | None = None,
        config: EvalConfig | None = None,
        version: str = "1",
        prompt_prefix: str = "",
        conversation: bool = False,
    ) -> EvalReport:
        """隔离每个样本，复用真实生成/工具链并在超时或取消时停止生产。"""
        from krim_agent.evaluation import ConversationTarget, GenerationTarget, evaluate

        if self._closed:
            raise ServiceError("sdk_closed", "SDK 已经关闭", 503)
        return await evaluate(
            dataset,
            (ConversationTarget if conversation else GenerationTarget)(
                self, version=version, prompt_prefix=prompt_prefix
            ),
            scorers=scorers,
            config=config,
        )

    def evaluation_service(self) -> EvaluationService:
        from krim_agent.evaluation.service import EvaluationService

        if self._closed:
            raise ServiceError("sdk_closed", "SDK 已经关闭", 503)
        if self._evaluation_service is None:
            self._evaluation_service = EvaluationService(self.runtime.repository.database)
        return self._evaluation_service

    def agents(self) -> AgentService:
        """借用同一 Runtime 的配置与实例管理；持久恢复须显式调用。"""
        from krim_agent.application.agents import AgentService

        if self._closed:
            raise ServiceError("sdk_closed", "SDK 已关闭", 503)
        if self._agent_service is None:
            self._agent_service = AgentService(self.runtime, self.runtime.settings)
        return self._agent_service

    async def start_evaluation(
        self,
        dataset: EvalDataset,
        *,
        scorers: Sequence[EvalScorer] | None = None,
        config: EvalConfig | None = None,
        version: str = "1",
        prompt_prefix: str = "",
        request_key: str | None = None,
        conversation: bool = False,
    ) -> EvalRunHandle:
        from krim_agent.evaluation import ConversationTarget, GenerationTarget

        return await self.evaluation_service().start(
            dataset,
            (ConversationTarget if conversation else GenerationTarget)(
                self, version=version, prompt_prefix=prompt_prefix
            ),
            scorers=scorers,
            config=config,
            request_key=request_key,
        )

    def evaluation(self, job_id: str) -> EvalRunHandle:
        return self.evaluation_service().handle(job_id)

    async def resume_evaluation(
        self,
        job_id: str,
        *,
        scorers: Sequence[EvalScorer] | None = None,
        version: str = "1",
        prompt_prefix: str = "",
        retry_uncertain: bool = False,
        skip_uncertain: bool = False,
        conversation: bool = False,
    ) -> EvalRunHandle:
        from krim_agent.evaluation import ConversationTarget, GenerationTarget
        from krim_agent.evaluation.jobs import EvalResumeRequest

        return await self.evaluation_service().resume(
            job_id,
            target=(ConversationTarget if conversation else GenerationTarget)(
                self, version=version, prompt_prefix=prompt_prefix
            ),
            scorers=scorers,
            request=EvalResumeRequest(
                retry_uncertain=retry_uncertain, skip_uncertain=skip_uncertain
            ),
        )

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

    async def active_runs(self) -> list[ActiveRunView]:
        if self._closed:
            raise ServiceError("sdk_closed", "SDK 已经关闭", 503)
        return [
            ActiveRunView(run=run, events=await self.runtime.repository.event_summary(run.id))
            for run in await self.runtime.repository.active_runs()
        ]

    async def multiplex_events(
        self, request: MultiplexRequest
    ) -> AsyncGenerator[RunStreamFrame | None]:
        if self._closed:
            raise ServiceError("sdk_closed", "SDK 已经关闭", 503)
        reader = MultiplexReader(self.runtime)
        prepared = await reader.prepare(request)
        async for item in reader.subscribe(prepared):
            yield item

    async def context_proposals(
        self, topic_id: str, *, pending_only: bool = False
    ) -> list[ContextProposalView]:
        if self._closed:
            raise ServiceError("sdk_closed", "SDK 已经关闭", 503)
        return await self.runtime.context_proposals(topic_id, pending_only=pending_only)

    async def context_proposal(self, topic_id: str, proposal_id: str) -> ContextProposalView:
        if self._closed:
            raise ServiceError("sdk_closed", "SDK 已经关闭", 503)
        return await self.runtime.context_proposal(topic_id, proposal_id)

    async def decide_context_proposal(
        self, topic_id: str, proposal_id: str, request: ContextProposalDecision
    ) -> ContextProposalView:
        if self._closed:
            raise ServiceError("sdk_closed", "SDK 已经关闭", 503)
        return await self.runtime.decide_context_proposal(topic_id, proposal_id, request)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._evaluation_service is not None:
            await self._evaluation_service.close()
        if self._agent_service is not None:
            await self._agent_service.close()
        if self._owned_database is not None:
            try:
                await self.runtime.close()
            finally:
                try:
                    await self._owned_database.close()
                finally:
                    if self._owned_provider is not None:
                        await self._owned_provider.close()
