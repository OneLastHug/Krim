"""纯历史投影/输入预算。M1不静默截断、不生成伪摘要。"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from pydantic import JsonValue

from krim_agent.context.counter import DEFAULT_COUNTER, TOKEN_WINDOW_VERSION, TokenCounter
from krim_agent.context.serialization import byte_size, encoded, fingerprint
from krim_agent.contracts import Message, ServiceError
from krim_agent.source_fingerprints import source_digest

from .types import ContextPolicy, ContextSnapshot, HistorySource

if TYPE_CHECKING:
    from krim_agent.runtime.contracts import RunSnapshot

VERSION = "complete-turns/1"


def identity() -> str:
    return source_digest(__file__)


def binding_token() -> tuple[object, ...]:
    return id(prepare), id(transcript), id(check_budget), id(validate_groups), identity()


def transcript(snapshot: RunSnapshot) -> list[Message]:
    """只投影完整工具组；不把中途text当作最终答复。"""
    first = Message(role="user", content=snapshot.request.text)
    result = [first]
    text: dict[int, str] = {}
    tools = {
        e.data.get("provider_call_id"): e.data for e in snapshot.events if e.kind == "tool_end"
    }
    for event in snapshot.events:
        step = cast(int, event.data.get("step_index", 0))
        if event.kind == "text":
            text[step] = text.get(step, "") + cast(str, event.data["text"])
        elif event.kind == "model_end":
            calls = cast(list[dict[str, JsonValue]], event.data["tool_calls"])
            if calls and any(
                c["id"] not in tools or tools[c["id"]].get("result_available") is not True
                for c in calls
            ):
                break
            result.append(Message(role="assistant", content=text.get(step, ""), tool_calls=calls))
            for call in calls:
                result.append(
                    Message(
                        role="tool",
                        call_id=cast(str, call["id"]),
                        content=encoded(tools[call["id"]].get("value")),
                    )
                )
    if snapshot.run.status != "completed" or len(result) == 1 or result[-1].role != "assistant":
        return [
            first,
            Message(
                role="user",
                content="[不可信历史状态数据] 上一轮"
                + snapshot.run.status
                + "，原因："
                + (snapshot.run.code or "unknown")
                + "。未完成的工具结果未知，不重执行。",
            ),
        ]
    return result


def validate_groups(history: list[Message]) -> None:
    pending: set[str] = set()
    for message in history:
        if message.role == "system" or pending and message.role != "tool":
            raise ServiceError("facts_corrupt")
        if message.role == "assistant":
            ids = [call.get("id") for call in message.tool_calls]
            if any(not isinstance(value, str) or not value for value in ids) or len(
                set(ids)
            ) != len(ids):
                raise ServiceError("facts_corrupt")
            pending = set(cast(list[str], ids))
        elif message.role == "tool":
            if message.call_id not in pending:
                raise ServiceError("facts_corrupt")
            pending.remove(message.call_id)
    if pending:
        raise ServiceError("facts_corrupt")


def check_budget(
    messages: list[Message],
    tools: list[dict[str, JsonValue]],
    policy: ContextPolicy,
    request_limit: int | None,
    counter: TokenCounter | None = None,
) -> int:
    """冻结/发送前的准入检查；返回规范JSON字节数。

    ``request_limit`` 是宿主/传输层显式声明的请求字节容量，None表示未声明；
    ``policy.max_bytes`` 是可选字节预算，None表示未配置。两者都只在显式
    给出时比较，不引入新的默认字节cap。token窗口代另按计数器执行
    ``max_tokens`` 窗口（输入加预留输出不得超过窗口）。``counter`` 缺省时
    使用模块级分段估计器。
    """
    if not messages or messages[0].role != "system":
        raise ServiceError("facts_corrupt")
    validate_groups(messages[1:])
    size = byte_size({"messages": [m.model_dump(mode="json") for m in messages], "tools": tools})
    over = (
        len(messages) > policy.max_messages
        if policy.max_messages is not None
        else False
    )
    if request_limit is not None:
        over = over or size + 1024 > request_limit
    if policy.max_bytes is not None:
        # legacy字节代保留旧输出预留折算（reserved*4）；token代只比较序列化
        # 输入字节本身，输出预留已在token窗口(max_tokens-reserved)内计量。
        if policy.counter_version == TOKEN_WINDOW_VERSION:
            over = over or size + 1024 > policy.max_bytes
        else:
            over = over or size + 1024 + policy.reserved_output_tokens * 4 > policy.max_bytes
    if not over and policy.counter_version == TOKEN_WINDOW_VERSION:
        tokens = (counter or DEFAULT_COUNTER).count_request(messages, tools)
        over = tokens + policy.reserved_output_tokens > policy.max_tokens
    if over:
        raise ServiceError("context_budget", 422)
    return size


def count_request_tokens(
    messages: list[Message],
    tools: list[dict[str, JsonValue]],
    counter: TokenCounter | None = None,
) -> int:
    """真实token计数端口：触发点判定与展示共用同一计数来源。"""
    return (counter or DEFAULT_COUNTER).count_request(messages, tools)


def prepare(
    system: str,
    history: list[Message],
    sources: list[HistorySource],
    current: str,
    tools: list[dict[str, JsonValue]],
    policy: ContextPolicy,
    request_limit: int | None,
    extensions: list[Message] | None = None,
    extension_snapshot: dict[str, JsonValue] | None = None,
    counter: TokenCounter | None = None,
) -> ContextSnapshot:
    extra = extensions or []
    messages = [
        Message(role="system", content=system),
        *history,
        *extra,
        Message(role="user", content=current),
    ]
    size = check_budget(messages, tools, policy, request_limit, counter)
    if policy.counter_version == TOKEN_WINDOW_VERSION:
        estimated_tokens = (counter or DEFAULT_COUNTER).count_request(messages, tools)
    else:
        estimated_tokens = (size + 3) // 4
    return ContextSnapshot(
        messages=tuple(messages),
        sources=tuple(sources),
        extensions=extension_snapshot or {},
        policy=policy,
        tool_fingerprint=fingerprint(tools),
        builder_fingerprint=identity(),
        input_bytes=size,
        estimated_tokens=estimated_tokens,
        source_fingerprint=fingerprint(
            {
                "history": [m.model_dump(mode="json") for m in history],
                "sources": [v.model_dump(mode="json") for v in sources],
            }
            | ({"extensions": extension_snapshot} if extension_snapshot else {})
        ),
    )
