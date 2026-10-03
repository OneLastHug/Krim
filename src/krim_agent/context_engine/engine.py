"""上下文分组、预算和摘要注入；工具组永远不被从中间拆开。"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from uuid import uuid4

from krim_agent.protocol.domain import MessageView

from .types import ContextOptions, ContextResult

SummaryFn = Callable[[list[MessageView]], str | Awaitable[str]]


class ContextError(ValueError):
    pass


def _chars(messages: Sequence[MessageView]) -> int:
    return sum(len(message.content) for message in messages)


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
    for message in group:
        calls = message.details.get("tool_calls", [])
        if message.role == "assistant" and isinstance(calls, list):
            for call in calls:
                call_id = call.get("id") if isinstance(call, dict) else None
                if isinstance(call_id, str):
                    pending.add(call_id)
        if message.role == "tool":
            call_id = message.details.get("call_id")
            if isinstance(call_id, str):
                if call_id not in pending:
                    return False
                pending.discard(call_id)
            else:
                return False
    return not pending


def _default_summary(messages: list[MessageView]) -> str:
    snippets = []
    for message in messages:
        content = " ".join(message.content.split())
        if content:
            snippets.append(f"{message.role}: {content[:160]}")
    if not snippets:
        return f"已压缩 {len(messages)} 条历史消息。"
    return f"已压缩 {len(messages)} 条历史消息：" + "；".join(snippets[-8:])


def _summary_message(text: str) -> MessageView:
    return MessageView(
        id=str(uuid4()),
        topic_id="context",
        run_id="context",
        sequence=0,
        role="system",
        content=text,
        details={"context_summary": True},
        created_at=datetime.now(UTC),
    )


class ContextEngine:
    """可测试的上下文处理器；默认近似 token=字符/4，真实 provider 可替换估算。"""

    async def prepare_async(
        self,
        messages: Sequence[MessageView],
        options: ContextOptions,
        summarizer: SummaryFn | None = None,
    ) -> ContextResult:
        result = self.prepare(messages, options)
        if result.compacted and options.strategy in {
            "summarize_then_truncate",
            "annotated_compact",
        }:
            omitted = [
                message
                for message in messages
                if message.id not in {item.id for item in result.messages}
            ]
            if summarizer is None:
                summary = _default_summary(omitted)
            else:
                value = summarizer(omitted)
                summary = await value if inspect.isawaitable(value) else value
            injected = _summary_message(summary)
            result = result.model_copy(
                update={
                    "messages": [injected, *result.messages],
                    "summary": summary,
                    "estimated_chars": result.estimated_chars + len(summary),
                    "estimated_tokens": (result.estimated_chars + len(summary) + 3) // 4,
                },
            )
        return result

    def prepare(self, messages: Sequence[MessageView], options: ContextOptions) -> ContextResult:
        original = list(messages)
        chars = _chars(original)
        tokens = (chars + 3) // 4
        limits = [
            options.max_messages is not None and len(original) > options.max_messages,
            options.max_chars is not None and chars > options.max_chars,
            options.max_tokens is not None and tokens > options.max_tokens,
        ]
        compact = options.policy == "always" or (options.policy == "auto" and any(limits))
        if (
            options.policy == "never"
            or not compact
            or not any(
                value is not None
                for value in (options.max_messages, options.max_chars, options.max_tokens)
            )
        ):
            return ContextResult(
                messages=original,
                original_count=len(original),
                omitted_count=0,
                estimated_chars=chars,
                estimated_tokens=tokens,
                strategy=options.strategy,
                policy=options.policy,
                processor_trace=["message_cleanup"],
            )
        groups = _groups(original)
        if any(not _valid_group(group) for group in groups):
            raise ContextError("历史工具组未闭合，不能安全压缩")
        budget_messages = options.max_messages or len(original)
        budget_chars = options.max_chars or 2**31
        budget_tokens = options.max_tokens or 2**31
        kept: list[list[MessageView]] = []
        total_messages = total_chars = 0
        for group in reversed(groups):
            group_chars = _chars(group)
            projected_messages = total_messages + len(group)
            projected_chars = total_chars + group_chars
            projected_tokens = (projected_chars + 3) // 4
            if kept and (
                projected_messages > budget_messages
                or projected_chars > budget_chars
                or projected_tokens > budget_tokens
            ):
                break
            if not kept and (
                projected_messages > budget_messages
                or projected_chars > budget_chars
                or projected_tokens > budget_tokens
            ):
                raise ContextError("最新完整消息组本身超过上下文预算")
            kept.append(group)
            total_messages, total_chars = projected_messages, projected_chars
        kept.reverse()
        retained = [message for group in kept for message in group]
        if not retained and original:
            retained = groups[-1]
        omitted_count = len(original) - len(retained)
        trace = ["message_cleanup", "history_truncate", "token_budget"]
        if options.strategy == "truncate":
            trace = ["message_cleanup", "history_truncate"]
        elif options.strategy == "sliding_window":
            trace.append("sliding_window")
        elif options.strategy in {"summarize_then_truncate", "annotated_compact"}:
            trace.extend(["summary_injector", "annotated_compact"])
        return ContextResult(
            messages=retained,
            original_count=len(original),
            omitted_count=omitted_count,
            compacted=omitted_count > 0,
            estimated_chars=_chars(retained),
            estimated_tokens=(_chars(retained) + 3) // 4,
            strategy=options.strategy,
            policy=options.policy,
            processor_trace=trace,
        )
