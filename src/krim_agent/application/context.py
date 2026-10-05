"""持久上下文组合：完整来源前缀、分支隔离、确认与压缩 Hook。"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Literal

from krim_agent.context_engine import (
    ContextEngine,
    ContextError,
    ContextOptions,
    ContextProposalDecision,
    ContextProposalView,
    ContextResult,
)
from krim_agent.context_engine.engine import SummaryFn, summary_message
from krim_agent.persistence import ContextSummaryRow, KrimRepository
from krim_agent.persistence.context_proposals import (
    ContextProposalStore,
    fingerprint,
    message_snapshot,
)
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
    proposal_id: str | None = None
    proposal_revision: int | None = None
    proposal_expires_at: float | None = None


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
        self.proposals = ContextProposalStore(repository)

    def is_busy(self, topic_id: str) -> bool:
        return topic_id in self._compacting

    async def _compute(
        self,
        topic_id: str,
        messages: list[MessageView],
        options: ContextOptions,
        trigger: CompactTrigger,
        engine: ContextEngine | None = None,
    ) -> tuple[ContextResult, ContextOptions]:
        engine = engine or self.engine
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
        hooked = self.pre_compact is not None and engine.needs_compaction(combined, used)
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
        used = used.model_copy(
            update={
                "reserved_output_tokens": max(
                    used.reserved_output_tokens, options.reserved_output_tokens
                )
            }
        )
        if trigger == "manual" and used.strategy not in {
            "summarize_then_truncate",
            "annotated_compact",
        }:
            raise ContextError("手动压缩需要摘要策略", code="context_strategy_invalid")
        result = await engine.prepare_async(prepared, used, self.summarizer, existing_summary=prior)
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
        self,
        topic_id: str,
        messages: Sequence[MessageView],
        options: ContextOptions,
        *,
        engine: ContextEngine | None = None,
    ) -> ContextResult:
        original = list(messages)
        result, used = await self._compute(topic_id, original, options, "automatic", engine)
        persisted = (
            result.compacted
            and result.summary is not None
            and used.policy != "never"
            and used.strategy in {"summarize_then_truncate", "annotated_compact"}
        )
        if persisted:
            await self._save(topic_id, original, result, used, manual=False)
        return await self._observe(topic_id, "automatic", result, persisted)

    @staticmethod
    async def _commit[Result](operation: Awaitable[Result]) -> Result:
        task = asyncio.ensure_future(operation)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def decide(
        self,
        topic_id: str,
        proposal_id: str,
        request: ContextProposalDecision,
        *,
        config_hash: str,
        engine: ContextEngine | None = None,
    ) -> ContextProposalView:
        proposal = await self.proposals.get(topic_id, proposal_id)
        if proposal.status == "proposed" and request.action == "confirm":
            if proposal.run_id is None:
                fits = (engine or self.engine)._fits(proposal.result.messages, proposal.options)
            else:
                # 根 API 可能回应子 Agent 的提案。使用计算时冻结的实际投影计数，
                # 所属生产任务仍用自己的编码器/Provider 投影复核后才能派发。
                saved, limits = proposal.result, proposal.options
                fits = (
                    (limits.max_messages is None or len(saved.messages) <= limits.max_messages)
                    and (limits.max_chars is None or saved.estimated_chars <= limits.max_chars)
                    and (
                        limits.max_tokens is None
                        or saved.estimated_tokens + limits.reserved_output_tokens
                        <= limits.max_tokens
                    )
                )
            if not fits:
                raise ServiceError("proposal_budget_changed", "保存的提案不满足当前编码预算", 409)
        decided, changed = await self._commit(
            self.proposals.decide(
                topic_id,
                proposal_id,
                request.revision,
                confirm=request.action == "confirm",
                config_hash=config_hash,
            )
        )
        expected = "completed" if request.action == "confirm" else "rejected"
        if decided.status != expected:
            raise ServiceError("proposal_" + decided.status, "提案已过期/变化或不能确认", 409)
        # Run 的 PostCompact 由所属生产任务执行，避免根 API 使用错误的子 Agent Hook。
        if changed and decided.status == "completed" and decided.run_id is None:
            result = await self._observe(
                topic_id, "manual", decided.result.model_copy(deep=True), True
            )
            decided = decided.model_copy(update={"result": result})
        return decided

    async def prepare_confirmation(
        self,
        topic_id: str,
        messages: list[MessageView],
        options: ContextOptions,
        *,
        engine: ContextEngine,
        run_id: str,
        step: int,
        config_hash: str,
        ttl_seconds: float,
    ) -> ContextResult | ContextProposalView:
        prior_hash = await self.proposals.prior(topic_id, messages)
        result, used = await self._compute(topic_id, messages, options, "automatic", engine)
        if not result.compacted or result.summary is None or used.policy == "never":
            return result
        return await self._commit(
            self.proposals.create(
                topic_id,
                messages,
                used,
                result,
                prior_hash=prior_hash,
                config_hash=config_hash,
                run_id=run_id,
                step=step,
                ttl_seconds=ttl_seconds,
            )
        )

    async def compact(
        self,
        topic_id: str,
        options: ContextOptions,
        *,
        confirm: bool = False,
        expected_branch_key: str | None = None,
        timeout_seconds: float = 60,
        engine: ContextEngine | None = None,
        proposal_id: str | None = None,
        proposal_revision: int | None = None,
        config_hash: str = "",
        ttl_seconds: float = 300,
    ) -> PersistentCompactResult:
        if self.is_busy(topic_id):
            raise ServiceError("context_busy", "话题正在生成手动压缩结果", 409)
        self._compacting.add(topic_id)
        engine = engine or self.engine
        guard = config_hash or fingerprint(
            {"tokenizer": engine.tokenizer.name if engine.tokenizer else "custom"}
        )
        try:
            if proposal_id is not None:
                if not confirm or proposal_revision is None:
                    raise ServiceError(
                        "proposal_request_invalid", "提案确认需要 confirm/revision", 422
                    )
                decided = await self.decide(
                    topic_id,
                    proposal_id,
                    ContextProposalDecision(action="confirm", revision=proposal_revision),
                    config_hash=guard,
                    engine=engine,
                )
                return self._proposal_result(decided, decided.result, persisted=True)
            await self.repository.assert_topic_idle(topic_id)
            messages = await self.repository.list_messages(topic_id)
            branch_key = self.repository.context_branch_key(messages)
            if expected_branch_key is not None and expected_branch_key != branch_key:
                raise ServiceError("summary_branch_changed", "确认时活动分支已经变化", 409)
            prior_hash = await self.proposals.prior(topic_id, messages)
            try:
                async with asyncio.timeout(timeout_seconds):
                    result, used = await self._compute(
                        topic_id, messages, options, "manual", engine
                    )
            except TimeoutError as exc:
                raise ServiceError("context_timeout", "手动摘要生成超过时间预算", 504) from exc
            if result.compacted and result.summary is not None and used.policy != "never":
                pending = asyncio.create_task(
                    self.proposals.create(
                        topic_id,
                        messages,
                        used,
                        result,
                        prior_hash=prior_hash,
                        config_hash=guard,
                        ttl_seconds=ttl_seconds,
                    )
                )
                try:
                    proposal = await asyncio.shield(pending)
                except asyncio.CancelledError:
                    await pending
                    raise
                if confirm:
                    decided = await self.decide(
                        topic_id,
                        proposal.id,
                        ContextProposalDecision(action="confirm", revision=proposal.revision),
                        config_hash=guard,
                        engine=engine,
                    )
                    return self._proposal_result(decided, decided.result, persisted=True)
                return self._proposal_result(proposal, result, persisted=False)
            await self.repository.assert_topic_idle(topic_id)
            current = await self.repository.list_messages(topic_id)
            if message_snapshot(current) != message_snapshot(messages):
                raise ServiceError("summary_branch_changed", "摘要计算期间活动路径发生变化", 409)
            sources = result.source_message_ids
            return PersistentCompactResult(
                result=result,
                options=used,
                branch_key=branch_key,
                source_branch_key=self.repository.context_branch_key(messages[: len(sources)])
                if sources
                else None,
                status="unchanged",
                reason="policy_never" if used.policy == "never" else "no_compaction_needed",
            )
        finally:
            self._compacting.discard(topic_id)

    def _proposal_result(
        self, proposal: ContextProposalView, result: ContextResult, *, persisted: bool
    ) -> PersistentCompactResult:
        sources = result.source_message_ids
        return PersistentCompactResult(
            result=result,
            options=proposal.options,
            branch_key=proposal.branch_key,
            source_branch_key=proposal.source_branch_key if sources else None,
            status="completed" if persisted else "proposed",
            persisted=persisted,
            proposal_id=proposal.id,
            proposal_revision=proposal.revision,
            proposal_expires_at=proposal.expires_at,
        )
