"""上下文分组、预算和摘要注入；工具组不从中间拆开，摘要不提升为可信指令。"""

from __future__ import annotations

import inspect
import json
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from uuid import uuid4

from pydantic import JsonValue

from krim_agent.protocol.domain import MessageView

from .types import ContextOptions, ContextResult

SummaryFn = Callable[[list[MessageView]], str | Awaitable[str]]
TokenCounter = Callable[[Sequence[MessageView]], int]


class ContextError(ValueError):
    def __init__(self, message: str, *, code: str = "context_invalid") -> None:
        super().__init__(message)
        self.code = code


def _wire_text(message: MessageView) -> str:
    body: dict[str, JsonValue] = {"role": message.role, "content": message.content}
    if message.role == "assistant" and message.details.get("tool_calls"):
        body["tool_calls"] = message.details["tool_calls"]
    if message.role == "tool":
        body["tool_call_id"] = message.details.get("call_id")
    return json.dumps(body, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def _chars(messages: Sequence[MessageView]) -> int:
    return sum(len(_wire_text(message)) for message in messages)


def _groups(messages: Sequence[MessageView]) -> list[list[MessageView]]:
    groups: list[list[MessageView]] = []
    current: list[MessageView] = []
    for message in messages:
        if message.role == "user" and current:
            groups.append(current)
            current = []
        current.append(message)
    if current:
        groups.append(current)
    return groups


def _valid_group(group: list[MessageView]) -> bool:
    pending: set[str] = set()
    seen: set[str] = set()
    for message in group:
        if pending and message.role != "tool":
            return False
        calls = message.details.get("tool_calls", [])
        if message.role == "assistant":
            if not isinstance(calls, list):
                return False
            for call in calls:
                identifier = call.get("id") if isinstance(call, dict) else None
                if not isinstance(identifier, str) or not identifier or identifier in seen:
                    return False
                seen.add(identifier)
                pending.add(identifier)
        if message.role == "tool":
            identifier = message.details.get("call_id")
            if not isinstance(identifier, str) or identifier not in pending:
                return False
            pending.remove(identifier)
    return not pending


def _default_summary(messages: list[MessageView]) -> str:
    snippets = []
    for message in messages:
        content = " ".join(message.content.split())
        if content:
            snippets.append(f"{message.role}: {content[:160]}")
    return f"已压缩 {len(messages)} 条历史消息：" + "；".join(snippets[-8:])


def summary_message(
    text: str, source_ids: list[str] | None = None, *, summary_id: str | None = None
) -> MessageView:
    """历史摘要作为 user 数据，保留来源与不可信标记，不冒充 system 指令。"""
    return MessageView(
        id=summary_id or str(uuid4()),
        topic_id="context",
        run_id="context",
        sequence=0,
        role="user",
        content="[不可信历史摘要]\n" + text + "\n[摘要结束]",
        details={"context_summary": True, "source_message_ids": list(source_ids or [])},
        created_at=datetime.now(UTC),
    )


def _source_ids(messages: Sequence[MessageView]) -> list[str]:
    result: list[str] = []
    for message in messages:
        if message.details.get("context_summary"):
            values = message.details.get("source_message_ids", [])
            if isinstance(values, list):
                result.extend(value for value in values if isinstance(value, str))
        else:
            result.append(message.id)
    return list(dict.fromkeys(result))


class ContextEngine:
    def __init__(self, token_counter: TokenCounter | None = None) -> None:
        self.token_counter = token_counter

    def _tokens(self, messages: Sequence[MessageView]) -> int:
        value = self.token_counter(messages) if self.token_counter else (_chars(messages) + 3) // 4
        if type(value) is not int or value < 0:
            raise ContextError("token 计数器必须返回非负整数", code="context_token_counter_invalid")
        return value

    def _fits(self, messages: Sequence[MessageView], options: ContextOptions) -> bool:
        return (
            (options.max_messages is None or len(messages) <= options.max_messages)
            and (options.max_chars is None or _chars(messages) <= options.max_chars)
            and (options.max_tokens is None or self._tokens(messages) <= options.max_tokens)
        )

    def needs_compaction(self, messages: Sequence[MessageView], options: ContextOptions) -> bool:
        return (
            options.policy != "never"
            and any(
                value is not None
                for value in (options.max_messages, options.max_chars, options.max_tokens)
            )
            and (options.policy == "always" or not self._fits(messages, options))
        )

    async def prepare_async(
        self,
        messages: Sequence[MessageView],
        options: ContextOptions,
        summarizer: SummaryFn | None = None,
        existing_summary: tuple[str, list[str]] | None = None,
    ) -> ContextResult:
        original = list(messages)
        prior = (
            summary_message(existing_summary[0], existing_summary[1])
            if existing_summary is not None
            else None
        )
        covered = len(existing_summary[1]) if existing_summary is not None else 0
        combined_input = [prior, *original] if prior is not None else original
        result = self.prepare(combined_input, options)
        if prior is not None:
            result = result.model_copy(
                update={
                    "summary": existing_summary[0] if existing_summary else None,
                    "processor_trace": ["persisted_summary", *result.processor_trace],
                    "source_message_ids": list(existing_summary[1]) if existing_summary else [],
                    "original_count": covered + len(original),
                    "omitted_count": covered,
                }
            )
        if not result.compacted:
            return result
        # 摘要与真实消息分开处理，确保再次压缩只产生一个摘要且来源仍是完整前缀。
        retained = [message for message in result.messages if message is not prior]
        if options.strategy in {"truncate", "sliding_window"}:
            combined = [prior, *retained] if prior is not None else retained
            while not self._fits(combined, options):
                groups = _groups(retained)
                if len(groups) <= 1:
                    raise ContextError(
                        "持久摘要与最新完整消息组超过上下文预算", code="context_budget"
                    )
                retained = [message for group in groups[1:] for message in group]
                combined = [prior, *retained] if prior is not None else retained
            return result.model_copy(
                update={
                    "messages": combined,
                    "compacted": len(retained) < len(original),
                    "omitted_count": covered + len(original) - len(retained),
                    "estimated_chars": _chars(combined),
                    "estimated_tokens": self._tokens(combined),
                }
            )
        for _pass in range(len(_groups(original)) + 1):
            kept_ids = {message.id for message in retained}
            omitted = [message for message in original if message.id not in kept_ids]
            summary_input = [prior, *omitted] if prior is not None else omitted
            try:
                value = (
                    _default_summary(summary_input)
                    if summarizer is None
                    else summarizer(summary_input)
                )
                summary = await value if inspect.isawaitable(value) else value
            except Exception as exc:
                raise ContextError("历史摘要生成失败", code="context_summary_failed") from exc
            if (
                not isinstance(summary, str)
                or not summary.strip()
                or len(summary.encode("utf-8")) > 65536
            ):
                raise ContextError(
                    "summarizer 必须返回有限非空字符串摘要", code="context_summary_invalid"
                )
            source_ids = _source_ids(summary_input)
            injected = summary_message(summary, source_ids)
            combined = [injected, *retained]
            if self._fits(combined, options):
                return result.model_copy(
                    update={
                        "messages": combined,
                        "summary": summary,
                        "omitted_count": len(source_ids),
                        "estimated_chars": _chars(combined),
                        "estimated_tokens": self._tokens(combined),
                        "source_message_ids": source_ids,
                    }
                )
            groups = _groups(retained)
            if len(groups) <= 1:
                raise ContextError("摘要与最新完整消息组超过上下文预算", code="context_budget")
            retained = [message for group in groups[1:] for message in group]
        raise ContextError("无法生成满足预算的完整上下文", code="context_budget")

    def prepare(self, messages: Sequence[MessageView], options: ContextOptions) -> ContextResult:
        original = list(messages)
        limits = any(
            value is not None
            for value in (options.max_messages, options.max_chars, options.max_tokens)
        )
        compact = options.policy == "always" or (
            options.policy == "auto" and not self._fits(original, options)
        )
        if options.policy == "never" or not compact or not limits:
            return ContextResult(
                messages=original,
                original_count=len(original),
                omitted_count=0,
                estimated_chars=_chars(original),
                estimated_tokens=self._tokens(original),
                strategy=options.strategy,
                policy=options.policy,
                processor_trace=["message_cleanup"],
            )
        groups = _groups(original)
        if any(not _valid_group(group) for group in groups):
            raise ContextError(
                "历史工具组未闭合或调用 ID 重复，不能安全压缩", code="context_tool_group"
            )
        kept: list[list[MessageView]] = []
        for group in reversed(groups):
            proposed = [message for selected in reversed([*kept, group]) for message in selected]
            if not self._fits(proposed, options):
                if not kept:
                    raise ContextError("最新完整消息组本身超过上下文预算", code="context_budget")
                break
            kept.append(group)
        retained = [message for group in reversed(kept) for message in group]
        trace = ["message_cleanup", "history_truncate", "token_budget"]
        if options.strategy == "truncate":
            trace = ["message_cleanup", "history_truncate"]
        elif options.strategy == "sliding_window":
            trace.append("sliding_window")
        else:
            trace.append("summary_injector")
            if options.strategy == "annotated_compact":
                trace.append("annotated_compact")
        return ContextResult(
            messages=retained,
            original_count=len(original),
            omitted_count=len(original) - len(retained),
            compacted=len(retained) < len(original),
            estimated_chars=_chars(retained),
            estimated_tokens=self._tokens(retained),
            strategy=options.strategy,
            policy=options.policy,
            processor_trace=trace,
        )
