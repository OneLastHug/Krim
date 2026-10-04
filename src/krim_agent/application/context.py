"""持久上下文组合：完整来源前缀、分支隔离、确认与压缩 Hook。"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Literal

from krim_agent.context_engine import ContextEngine, ContextError, ContextOptions, ContextResult
from krim_agent.context_engine.engine import SummaryFn, summary_message
from krim_agent.persistence import ContextSummaryRow, KrimRepository
from krim_agent.prompt_context import PromptSnapshot
from krim_agent.protocol.domain import MessageView, ServiceError
from krim_agent.skills import materialize_skills

logger = logging.getLogger(__name__)
CompactTrigger = Literal["automatic", "manual"]
CompactStatus = Literal["proposed", "completed", "unchanged"]


@dataclass(frozen=True)
class PreCompactEvent:
    topic_id: str
    trigger: CompactTrigger
    options: ContextOptions
    messages: list[MessageView]


@dataclass(frozen=True)
class PreCompactDecision:
    allowed: bool = True
    options: ContextOptions | None = None


@dataclass(frozen=True)
class PostCompactEvent:
    topic_id: str
    trigger: CompactTrigger
    result: ContextResult
    persisted: bool


PreCompact = Callable[[PreCompactEvent], PreCompactDecision | Awaitable[PreCompactDecision]]
PostCompact = Callable[[PostCompactEvent], None | Awaitable[None]]


@dataclass(frozen=True)
class PersistentCompactResult:
    result: ContextResult
    options: ContextOptions
    branch_key: str
    source_branch_key: str | None
    status: CompactStatus
    reason: str | None = None
    persisted: bool = False


class PersistentContext:
    def __init__(
        self,
        repository: KrimRepository,
        engine: ContextEngine,
        summarizer: SummaryFn | None = None,
        *,
        pre_compact: PreCompact | None = None,
        post_compact: PostCompact | None = None,
        observer_timeout_seconds: float = 5,
    ) -> None:
        if not 0 < observer_timeout_seconds <= 60:
            raise ValueError("PostCompact 超时必须大于 0 且不超过 60 秒")
        self.repository, self.engine, self.summarizer = repository, engine, summarizer
        self.pre_compact, self.post_compact = pre_compact, post_compact
        self.observer_timeout_seconds = observer_timeout_seconds
        self._compacting: set[str] = set()

    def is_busy(self, topic_id: str) -> bool:
        return topic_id in self._compacting

    async def _compute(
        self,
        topic_id: str,
        messages: list[MessageView],
        options: ContextOptions,
        trigger: CompactTrigger,
    ) -> tuple[ContextResult, ContextOptions]:
        messages = [
            materialize_skills(self._materialize_prompt_context(message)) for message in messages
        ]
        stored = await self.repository.match_context_summary(topic_id, messages)
        consumed = len(stored.source_message_ids) if stored else 0
        prior = (
            (stored.summary, [str(value) for value in stored.source_message_ids])
            if stored
            else None
        )
        prepared = messages[consumed:]
        combined = [summary_message(prior[0], prior[1]), *prepared] if prior else prepared
        used = options.model_copy(deep=True)
        hooked = self.pre_compact is not None and self.engine.needs_compaction(combined, used)
        if hooked and self.pre_compact is not None:
            try:
                decision = self.pre_compact(
                    PreCompactEvent(
                        topic_id,
                        trigger,
                        used.model_copy(deep=True),
                        [message.model_copy(deep=True) for message in messages],
                    )
                )
                decision = await decision if inspect.isawaitable(decision) else decision
            except ContextError:
                raise
            except Exception as exc:
                raise ContextError("PreCompact 执行失败", code="context_hook_failed") from exc
            if not isinstance(decision, PreCompactDecision):
                raise ContextError("PreCompact 返回值无效", code="context_hook_invalid")
            if not decision.allowed:
                raise ContextError("PreCompact 拒绝压缩", code="context_denied")
            if decision.options is not None:
                try:
                    used = ContextOptions.model_validate(decision.options.model_dump())
                except Exception as exc:
                    raise ContextError("PreCompact 预算无效", code="context_hook_invalid") from exc
        if trigger == "manual" and used.strategy not in {
            "summarize_then_truncate",
            "annotated_compact",
        }:
            raise ContextError("手动压缩需要摘要策略", code="context_strategy_invalid")
        result = await self.engine.prepare_async(
            prepared, used, self.summarizer, existing_summary=prior
        )
        if hooked:
            result.processor_trace.insert(0, "pre_compact")
        return result, used

    @staticmethod
    def _materialize_prompt_context(message: MessageView) -> MessageView:
        raw = message.details.get("prompt_context")
        if (
            message.role != "user"
            or raw is None
            or message.details.get("_prompt_context_materialized") is True
        ):
            return message
        snapshot = PromptSnapshot.model_validate(raw)
        payload = json.dumps(snapshot.model_dump(mode="json"), ensure_ascii=False, sort_keys=True)
        content = message.content + "\n\n[Krim Agent 用户上下文数据]\n" + payload
        details = {
            **message.details,
            "_prompt_context_materialized": True,
            "_prompt_context_raw_text": message.content,
        }
        return message.model_copy(update={"content": content, "details": details}, deep=True)

    async def _save(
        self,
        topic_id: str,
        messages: list[MessageView],
        result: ContextResult,
        options: ContextOptions,
        *,
        manual: bool,
    ) -> ContextSummaryRow:
        sources = list(result.source_message_ids)
        prefix = messages[: len(sources)]
        if not sources or [message.id for message in prefix] != sources or result.summary is None:
            raise ContextError("Context 摘要来源不是完整活动前缀", code="context_source_invalid")
        save = asyncio.create_task(
            self.repository.save_context_summary(
                topic_id,
                self.repository.context_branch_key(prefix),
                sources,
                result.summary,
                options.strategy,
                options.policy,
                require_idle=manual,
                expected_path_key=self.repository.context_branch_key(messages) if manual else None,
            )
        )
        try:
            return await asyncio.shield(save)
        except asyncio.CancelledError:
            # 保存已开始时先明确事务结果，避免返回后仍有后台数据库写入。
            await save
            raise

    async def _observe(
        self,
        topic_id: str,
        trigger: CompactTrigger,
        result: ContextResult,
        persisted: bool,
    ) -> ContextResult:
        if self.post_compact is not None and result.compacted:
            try:
                async with asyncio.timeout(self.observer_timeout_seconds):
                    value = self.post_compact(
                        PostCompactEvent(topic_id, trigger, result.model_copy(deep=True), persisted)
                    )
                    if inspect.isawaitable(value):
                        await value
            except Exception:
                logger.warning("PostCompact 观测失败；保留已完成的压缩状态")
                result.processor_trace.append("post_compact_failed")
            else:
                result.processor_trace.append("post_compact")
        return result

    async def prepare(
        self, topic_id: str, messages: Sequence[MessageView], options: ContextOptions
    ) -> ContextResult:
        original = list(messages)
        result, used = await self._compute(topic_id, original, options, "automatic")
        persisted = (
            result.compacted
            and result.summary is not None
            and used.policy != "never"
            and used.strategy in {"summarize_then_truncate", "annotated_compact"}
        )
        if persisted:
            await self._save(topic_id, original, result, used, manual=False)
        return await self._observe(topic_id, "automatic", result, persisted)

    async def compact(
        self,
        topic_id: str,
        options: ContextOptions,
        *,
        confirm: bool = False,
        expected_branch_key: str | None = None,
        timeout_seconds: float = 60,
    ) -> PersistentCompactResult:
        if self.is_busy(topic_id):
            raise ServiceError("context_busy", "话题正在生成手动压缩结果", 409)
        self._compacting.add(topic_id)
        try:
            await self.repository.assert_topic_idle(topic_id)
            messages = await self.repository.list_messages(topic_id)
            branch_key = self.repository.context_branch_key(messages)
            if expected_branch_key is not None and expected_branch_key != branch_key:
                raise ServiceError("summary_branch_changed", "确认时活动分支已经变化", 409)
            try:
                async with asyncio.timeout(timeout_seconds):
                    result, used = await self._compute(topic_id, messages, options, "manual")
            except TimeoutError as exc:
                raise ServiceError("context_timeout", "手动摘要生成超过时间预算", 504) from exc
            sources = list(result.source_message_ids)
            prefix = messages[: len(sources)]
            if sources and [message.id for message in prefix] != sources:
                raise ContextError(
                    "Context 摘要来源不是完整活动前缀", code="context_source_invalid"
                )
            source_key = self.repository.context_branch_key(prefix) if sources else None
            status: CompactStatus = "unchanged"
            persisted = False
            reason: str | None = (
                "policy_never" if used.policy == "never" else "no_compaction_needed"
            )
            if result.compacted and result.summary is not None and used.policy != "never":
                if used.strategy not in {"summarize_then_truncate", "annotated_compact"}:
                    raise ContextError("手动压缩需要摘要策略", code="context_strategy_invalid")
                status, reason = "proposed", None
                if confirm:
                    await self._save(topic_id, messages, result, used, manual=True)
                    status, persisted = "completed", True
                    result = await self._observe(topic_id, "manual", result, True)
            # 预览/无变化也复核状态，不能对已经变化的会话返回有效提案。
            if not persisted:
                await self.repository.assert_topic_idle(topic_id)
                current = await self.repository.list_messages(topic_id)
                if self.repository.context_branch_key(current) != branch_key:
                    raise ServiceError(
                        "summary_branch_changed", "摘要计算期间活动路径已经变化", 409
                    )
            return PersistentCompactResult(
                result=result,
                options=used,
                branch_key=branch_key,
                source_branch_key=source_key,
                status=status,
                reason=reason,
                persisted=persisted,
            )
        finally:
            self._compacting.discard(topic_id)
