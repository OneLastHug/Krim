"""纯历史投影/输入预算。M1不静默截断、不生成伪摘要。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from pydantic import JsonValue

from krim_agent.context.counter import DEFAULT_COUNTER, TOKEN_WINDOW_VERSION, TokenCounter
from krim_agent.context.serialization import byte_size, encoded, fingerprint
from krim_agent.contracts import Message, RunEvent, ServiceError
from krim_agent.source_fingerprints import source_digest

from .types import ContextPolicy, ContextSnapshot, HistorySource

if TYPE_CHECKING:
    from krim_agent.runtime.contracts import RunSnapshot

VERSION = "complete-turns/1"


def identity() -> str:
    return source_digest(__file__)


def binding_token() -> tuple[object, ...]:
    return id(prepare), id(transcript), id(check_budget), id(validate_groups), identity()


@dataclass(frozen=True)
class _EventIndex:
    steps: tuple[tuple[int, str, tuple[dict[str, JsonValue], ...]], ...]
    calls: dict[str, tuple[int, dict[str, JsonValue], RunEvent]]
    starts: dict[str, RunEvent]
    ends: dict[str, RunEvent]
    intents: frozenset[str]


def _facts_corrupt() -> ServiceError:
    return ServiceError("facts_corrupt")


def _step_index(event: RunEvent) -> int:
    value = event.data.get("step_index")
    if type(value) is not int or value < 0:
        raise _facts_corrupt()
    return value


def _event_index(events: tuple[RunEvent, ...], run_id: str | None = None) -> _EventIndex:
    calls: dict[str, tuple[int, dict[str, JsonValue], RunEvent]] = {}
    starts: dict[str, RunEvent] = {}
    ends: dict[str, RunEvent] = {}
    intents: set[str] = set()
    intent_events: dict[str, RunEvent] = {}
    host_call_ids: set[str] = set()
    text_by_step: dict[int, str] = {}
    finished_steps: set[int] = set()
    steps: list[tuple[int, str, tuple[dict[str, JsonValue], ...]]] = []
    prior_seq = -1

    for event in events:
        if (run_id is not None and event.run_id != run_id) or event.seq <= prior_seq:
            raise _facts_corrupt()
        prior_seq = event.seq
        data = event.data
        if event.kind == "summary_text" or data.get("purpose") == "summary":
            continue
        if event.kind == "text":
            step = _step_index(event)
            value = data.get("text")
            if step in finished_steps or not isinstance(value, str):
                raise _facts_corrupt()
            text_by_step[step] = text_by_step.get(step, "") + value
        elif event.kind == "model_end":
            step = _step_index(event)
            raw_calls = data.get("tool_calls")
            if step in finished_steps or not isinstance(raw_calls, list):
                raise _facts_corrupt()
            normalized: list[dict[str, JsonValue]] = []
            for item in raw_calls:
                if not isinstance(item, dict):
                    raise _facts_corrupt()
                call_id, name, arguments = item.get("id"), item.get("name"), item.get("arguments")
                if (not isinstance(call_id, str) or not call_id or call_id in calls
                        or not isinstance(name, str) or not name
                        or not isinstance(arguments, dict)):
                    raise _facts_corrupt()
                call = item
                calls[call_id] = (step, call, event)
                normalized.append(call)
            steps.append((step, text_by_step.pop(step, ""), tuple(normalized)))
            finished_steps.add(step)
        elif event.kind == "tool_start":
            provider_id = data.get("provider_call_id")
            call_id = data.get("call_id")
            name = data.get("name")
            status = data.get("status")
            executor_started = data.get("executor_started")
            step = _step_index(event)
            if (not isinstance(provider_id, str) or not provider_id or provider_id in starts
                    or not isinstance(call_id, str) or not call_id or call_id in host_call_ids
                    or not isinstance(name, str) or not name
                    or not isinstance(status, str)
                    or executor_started is not None and type(executor_started) is not bool):
                raise _facts_corrupt()
            starts[provider_id] = event
            host_call_ids.add(call_id)
        elif event.kind == "tool_end":
            provider_id = data.get("provider_call_id")
            call_id = data.get("call_id")
            name = data.get("name")
            available = data.get("result_available")
            status = data.get("status")
            executor_started = data.get("executor_started")
            step = _step_index(event)
            if (not isinstance(provider_id, str) or not provider_id or provider_id in ends
                    or not isinstance(call_id, str) or not call_id
                    or not isinstance(name, str) or not name
                    or not isinstance(status, str)
                    or "result_available" not in data
                    or available is not None and type(available) is not bool
                    or status == "ok" and available is not True
                    or executor_started is not None and type(executor_started) is not bool
                    or available is True and (status == "unknown" or "value" not in data)):
                raise _facts_corrupt()
            ends[provider_id] = event
        elif event.kind == "tool_intent":
            call_id = data.get("call_id")
            if not isinstance(call_id, str) or not call_id or call_id in intents:
                raise _facts_corrupt()
            intents.add(call_id)
            intent_events[call_id] = event

    for provider_id, start in starts.items():
        call_info = calls.get(provider_id)
        data = start.data
        if call_info is None:
            raise _facts_corrupt()
        step, call, model_end = call_info
        host_call_id = cast(str, data["call_id"])
        intent = intent_events.get(host_call_id)
        if (data.get("name") != call.get("name") or _step_index(start) != step
                or start.seq <= model_end.seq
                or intent is not None and (
                    intent.seq <= start.seq or _step_index(intent) != step
                )):
            raise _facts_corrupt()
    for provider_id, end in ends.items():
        start_event = starts.get(provider_id)
        if start_event is None:
            raise _facts_corrupt()
        if (end.data.get("call_id") != start_event.data.get("call_id")
                or end.data.get("name") != start_event.data.get("name")
                or _step_index(end) != _step_index(start_event) or end.seq <= start_event.seq):
            raise _facts_corrupt()
    started_call_ids = {cast(str, event.data["call_id"]) for event in starts.values()}
    if not intents.issubset(started_call_ids):
        raise _facts_corrupt()
    # A completed model response may precede a rejected or interrupted admission
    # batch. Missing admission is not evidence of execution or of non-execution:
    # consumers must retain it as unknown, never as a successful result.
    return _EventIndex(tuple(steps), calls, starts, ends, frozenset(intents))


def transcript(snapshot: RunSnapshot) -> list[Message]:
    """Project every complete event-backed tool result, including failed runs.

    Missing results in an interrupted run receive a stable tool-call pairing with
    an explicit unknown projection. No synthetic tool_end event or success fact is
    written. A completed run missing its result ledger is inconsistent.
    """
    first = Message(role="user", content=snapshot.request.text)
    result = [first]
    index = _event_index(snapshot.events, snapshot.run.id)
    for _step, text, calls in index.steps:
        call_messages = list(calls)
        result.append(Message(role="assistant", content=text, tool_calls=call_messages))
        for call in calls:
            provider_id = cast(str, call["id"])
            end = index.ends.get(provider_id)
            if end is None:
                if snapshot.run.status == "completed":
                    raise _facts_corrupt()
                start = index.starts.get(provider_id)
                if start is None:
                    result.append(Message(
                        role="tool",
                        call_id=provider_id,
                        content=encoded({
                            "status": "unknown",
                            "source": "persisted_model_call_without_tool_start",
                            "executor_started": None,
                            "result_available": None,
                            "execution_outcome": "unknown",
                            "replay_forbidden": True,
                            "note": "工具准入记录缺失；不推断是否执行，未知副作用不得自动重放。",
                        }),
                    ))
                    continue
                result.append(
                    Message(
                        role="tool",
                        call_id=provider_id,
                        content=encoded({
                            "status": "unknown",
                            "source": "persisted_tool_start_without_tool_end",
                            "host_call_id": start.data["call_id"],
                            "tool_intent_recorded": start.data["call_id"] in index.intents,
                            "result_available": None,
                            "note": "执行结果未持久化；未知副作用不得自动重放。",
                        }),
                    )
                )
                continue
            data = end.data
            if data.get("result_available") is True:
                if "value" not in data:
                    raise _facts_corrupt()
                content = encoded(data["value"])
            else:
                unavailable: dict[str, JsonValue] = {
                    "status": data.get("status"),
                    "code": data.get("code"),
                    "decision": data.get("decision"),
                    "executor_started": data.get("executor_started"),
                    "result_available": data.get("result_available"),
                    "source": "persisted_tool_end",
                }
                if data.get("executor_started") is not False:
                    unavailable.update({
                        "execution_outcome": "unknown",
                        "replay_forbidden": True,
                        "note": "工具请求已结束，但执行结果未确认；未知副作用不得自动重放。",
                    })
                content = encoded(unavailable)
            result.append(Message(role="tool", call_id=provider_id, content=content))

    if snapshot.run.status != "completed":
        result.append(
            Message(
                role="user",
                content=(
                    "[不可信历史状态数据] 上一轮"
                    + snapshot.run.status
                    + "，原因："
                    + (snapshot.run.code or "unknown")
                    + "。以上仅包含已记录结果；缺失结果保持未知，不重执行未知副作用。"
                ),
            )
        )
    elif not index.steps:
        raise _facts_corrupt()
    validate_groups(result)
    return result


def deterministic_signature(events: tuple[RunEvent, ...]) -> str | None:
    """Hash an exact, fully-observed model/tool trace for stall detection."""
    index = _event_index(events)
    signature: list[dict[str, JsonValue]] = []
    for _step, text, calls in index.steps:
        normalized_calls: list[JsonValue] = []
        for call in calls:
            provider_id = cast(str, call["id"])
            start = index.starts.get(provider_id)
            end = index.ends.get(provider_id)
            if start is None or end is None:
                return None
            data = end.data
            status = data.get("status")
            available = data.get("result_available")
            if available is True:
                outcome: dict[str, JsonValue] = {
                    "status": cast(str, status), "value": data["value"]
                }
            elif (data.get("executor_started") is False
                  and start.data.get("call_id") not in index.intents
                  and status in {"denied", "failed", "cancelled"}):
                outcome = {
                    "status": cast(str, status), "code": data.get("code"),
                    "decision": data.get("decision"),
                }
            else:
                return None
            normalized_calls.append({
                "name": call["name"],
                "arguments": call["arguments"],
                "outcome": outcome,
            })
        if text or normalized_calls:
            signature.append({"text": text, "tool_calls": normalized_calls})
    return fingerprint(signature) if signature else None


def has_unknown_execution(events: tuple[RunEvent, ...]) -> bool:
    """Whether a declared tool has no reliable admission or terminal record."""
    index = _event_index(events)
    if not set(index.calls).issubset(index.starts):
        return True
    for provider_id, start in index.starts.items():
        end = index.ends.get(provider_id)
        host_call_id = start.data.get("call_id")
        if end is None:
            if host_call_id in index.intents:
                return True
            continue
        data = end.data
        if data.get("status") == "unknown":
            return True
        if data.get("executor_started") is not False and data.get("result_available") is not True:
            return True
    return False


def has_waiting_task(events: tuple[RunEvent, ...]) -> bool:
    """Detect explicit process/task handles that are still accepted or running."""
    index = _event_index(events)

    def waiting(value: JsonValue) -> bool:
        if isinstance(value, dict):
            if value.get("background") is True:
                return True
            if any(
                isinstance(value.get(key), str)
                and value.get(key) in {"accepted", "running", "pending", "waiting"}
                for key in ("state", "status", "phase")
            ):
                return True
            return any(waiting(item) for item in value.values())
        if isinstance(value, list):
            return any(waiting(item) for item in value)
        return False

    return any(
        end.data.get("result_available") is True and waiting(end.data.get("value"))
        for end in index.ends.values()
    )


def validate_groups(history: list[Message]) -> None:
    pending: set[str] = set()
    for message in history:
        if message.role == "system" or pending and message.role != "tool":
            raise ServiceError("facts_corrupt")
        if message.role == "assistant":
            if any(
                not isinstance(call, dict)
                or not isinstance(call.get("id"), str)
                or not call["id"]
                or not isinstance(call.get("name"), str)
                or not call["name"]
                or not isinstance(call.get("arguments"), dict)
                for call in message.tool_calls
            ):
                raise ServiceError("facts_corrupt")
            ids = [cast(str, call["id"]) for call in message.tool_calls]
            if len(set(ids)) != len(ids):
                raise ServiceError("facts_corrupt")
            pending = set(ids)
        elif message.tool_calls:
            raise ServiceError("facts_corrupt")
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
