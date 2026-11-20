"""Python AI SDK 的 Chat Completions 接入及 Krim 出站/响应边界。

SDK 负责模型请求和流式模型事件；完整 finish/DONE 和工具 JSON 校验后才释放调用。
"""

from __future__ import annotations

import asyncio
import logging
import re
import traceback
from collections.abc import AsyncGenerator, AsyncIterator, Sequence
from contextvars import ContextVar
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, Literal, Self, cast
from urllib.parse import urlsplit, urlunsplit

import ai
import httpx
import openai
from ai.providers.openai import OpenAIChatCompletionsProtocol
from ai.providers.openai.protocol import OpenAIResponsesProtocol
from ai.types.messages import FilePart, TextPart, ToolCallPart, ToolResultPart
from ai.types.messages import Message as SDKMessage
from ai.types.tools import Tool, ToolSpec
from pydantic import Field, JsonValue, SecretStr, field_validator, model_validator

from krim_agent.agent_sdk.host_media import HostImageMessage, validate_host_images
from krim_agent.contracts import (
    Message,
    ServiceError,
    StrictModel,
    encoded,
    fingerprint,
    parse_json,
)
from krim_agent.runtime.contracts import settled
from krim_agent.usage import TokenUsage

from .types import (
    ModelEvent,
    ModelProgress,
    ModelRequestBudget,
    ModelStreamEvent,
    ModelText,
    ModelToolCall,
    ModelUsage,
)


class ProviderError(ServiceError):
    """仅本地稳定码，不携原URL、上游正文或异常链。"""


class OpenAICompatibleConfig(StrictModel):
    base_url: str = Field(min_length=1, max_length=2048, repr=False)
    model: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
    wire_api: Literal["chat_completions", "responses"] = "chat_completions"
    api_key: SecretStr = Field(default=SecretStr(""), repr=False)
    # None = 请求不带max_tokens/max_output_tokens，输出上限交给供应商真实
    # 能力；显式数值仍是"单次请求的生成预算"，不是累计输出配额。
    max_output_tokens: int | None = Field(default=None, ge=1)
    reasoning_effort: Literal["xhigh"] | None = None
    thinking: Literal["disabled"] | None = None
    # None = 该传输阶段不设应用层硬超时；长连接的最终期限由运行层的
    # model_timeout/运行deadline执行。默认值不再压住大窗口请求的首字节等待。
    connect_timeout_seconds: float | None = Field(default=10.0, gt=0)
    read_timeout_seconds: float | None = Field(default=None, gt=0)
    write_timeout_seconds: float | None = Field(default=30.0, gt=0)
    pool_timeout_seconds: float | None = Field(default=30.0, gt=0)
    # None = 不设应用层请求字节上限；输入规模由token窗口判定（context策略），
    # 供应商真实限制按其响应原样暴露。
    max_request_bytes: int | None = Field(default=None, ge=256)
    allow_images: bool = Field(default=False, strict=True)
    max_response_bytes: int | None = Field(default=None, ge=1)
    max_event_bytes: int | None = Field(default=None, ge=1)
    max_events: int | None = Field(default=None, ge=1)
    max_arguments_bytes: int | None = Field(default=None, ge=2)
    max_tool_calls: int | None = Field(default=None, ge=1, strict=True)

    @field_validator("base_url")
    @classmethod
    def url(cls, value: str) -> str:
        if (
            not value.isascii()
            or any(c.isspace() or ord(c) < 33 for c in value)
            or any(c in value for c in "\\%?#")
        ):
            raise ValueError("无效Provider地址")
        try:
            parsed = urlsplit(value)
            port = parsed.port
        except ValueError:
            raise ValueError("无效Provider地址") from None
        host = parsed.hostname or ""
        if (
            parsed.scheme not in {"http", "https"}
            or not host
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise ValueError("无效Provider地址")
        if port is not None and not 1 <= port <= 65535:
            raise ValueError("无效Provider地址")
        if parsed.scheme == "http" and host not in {"127.0.0.1", "::1"}:
            raise ValueError("明文Provider须为固定本机地址")
        if host != "::1" and re.fullmatch(r"[A-Za-z0-9.-]+", host) is None:
            raise ValueError("无效Provider主机")
        path = parsed.path.rstrip("/")
        if (
            re.fullmatch(r"[A-Za-z0-9/_-]*", path) is None
            or "//" in path
            or any(p in {".", ".."} for p in path.split("/"))
        ):
            raise ValueError("无效Provider路径")
        authority = ("[" + host + "]" if host == "::1" else host.lower()) + (
            ":" + str(port) if port is not None else ""
        )
        return urlunsplit((parsed.scheme, authority, path, "", ""))

    @field_validator("api_key")
    @classmethod
    def key(cls, value: SecretStr) -> SecretStr:
        raw = value.get_secret_value()
        if len(raw) > 2048 or any(not 33 <= ord(c) <= 126 for c in raw):
            raise ValueError("无效Provider密钥")
        return value

    @model_validator(mode="after")
    def bounds(self) -> Self:
        # Arguments may span many events; a finite aggregate argument bound need
        # not fit one event. Only two explicitly finite wire bounds are related.
        if (self.max_event_bytes is not None and self.max_response_bytes is not None
                and self.max_event_bytes > self.max_response_bytes):
            raise ValueError("Provider容量关系无效")
        return self

async def _sse_data(response: httpx.Response, config: OpenAICompatibleConfig) -> AsyncIterator[str]:
    """按真实字节计帧；延迟CR分派以完整计入跨TCP块的CRLF，不分发残帧。"""
    line = bytearray()
    lines: list[bytes] = []
    response_size = event_size = count = 0
    pending_cr = has_frame = False

    def event_limit() -> ProviderError:
        logging.getLogger(__name__).warning(
            "Provider SSE limit: event_bytes=%d/%s events=%d/%s response_bytes=%d/%s",
            event_size,
            config.max_event_bytes,
            count,
            config.max_events,
            response_size,
            config.max_response_bytes,
        )
        return ProviderError("provider_event_limit")

    def finish_line() -> str | None:
        nonlocal event_size, count, has_frame
        raw = bytes(line)
        line.clear()
        if raw:
            has_frame = True
            try:
                raw.decode("utf-8", errors="strict")
            except UnicodeError:
                raise ProviderError("provider_protocol") from None
            if raw.startswith(b"data:"):
                lines.append(raw[6:] if raw[5:6] == b" " else raw[5:])
            elif raw == b"data":
                lines.append(b"")
            return None
        if has_frame:
            count += 1
            if config.max_events is not None and count > config.max_events:
                raise event_limit()
        try:
            payload = b"\n".join(lines).decode("utf-8", errors="strict") if lines else None
        except UnicodeError:
            raise ProviderError("provider_protocol") from None
        lines.clear()
        event_size, has_frame = 0, False
        return payload

    async for chunk in response.aiter_raw():
        response_size += len(chunk)
        if config.max_response_bytes is not None and response_size > config.max_response_bytes:
            raise ProviderError("provider_response_limit")
        for byte in chunk:
            if pending_cr:
                pending_cr = False
                if byte == 10:
                    event_size += 1
                    if config.max_event_bytes is not None and event_size > config.max_event_bytes:
                        raise event_limit()
                    payload = finish_line()
                    if payload is not None:
                        yield payload
                    continue
                payload = finish_line()
                if payload is not None:
                    yield payload
            event_size += 1
            if config.max_event_bytes is not None and event_size > config.max_event_bytes:
                raise event_limit()
            if byte == 13:
                pending_cr = True
            elif byte == 10:
                payload = finish_line()
                if payload is not None:
                    yield payload
            else:
                line.append(byte)
    # 单CR本身是完整换行；只分派已由空行闭合的帧，普通EOF尾串不分派。
    if pending_cr:
        payload = finish_line()
        if payload is not None:
            yield payload


@dataclass
class _ToolBuffer:
    identifier: str = ""
    name: str = ""
    arguments: str = ""

    def append(self, delta: dict[str, Any], config: OpenAICompatibleConfig) -> None:
        if delta.get("type", "function") not in (None, "function"):
            raise ProviderError("provider_protocol")
        function = delta.get("function", {})
        if not isinstance(function, dict):
            raise ProviderError("provider_protocol")
        for field, value in [
            ("identifier", delta.get("id")),
            ("name", function.get("name")),
            ("arguments", function.get("arguments")),
        ]:
            if value is not None:
                if not isinstance(value, str):
                    raise ProviderError("provider_protocol")
                current = getattr(self, field)
                if field == "arguments":
                    setattr(self, field, current + value)
                elif value and value != current:
                    # 完整元数据重发幂等；仍接受既有的工具名/ID分片。
                    setattr(self, field, current + value)
        if len(self.identifier) > 128 or len(self.name) > 64:
            raise ProviderError("provider_protocol")
        if (config.max_arguments_bytes is not None
                and len(self.arguments.encode()) > config.max_arguments_bytes):
            raise ProviderError("provider_tool_arguments")

    def collect(self) -> ModelToolCall:
        try:
            value = parse_json(self.arguments)
            if not isinstance(value, dict):
                raise ValueError
            return ModelToolCall(id=self.identifier, name=self.name, arguments=value)
        except (ValueError, RecursionError):
            raise ProviderError("provider_tool_arguments") from None


def _messages(
    messages: Sequence[Message], *, allow_images: bool = False
) -> list[dict[str, JsonValue]]:
    result: list[dict[str, JsonValue]] = []
    pending: set[str] = set()
    for index, message in enumerate(messages):
        if isinstance(message, HostImageMessage) and (
            not allow_images or message.role != "user" or index != len(messages) - 1
        ):
            raise ServiceError("invalid_input")
        item: dict[str, JsonValue] = {"role": message.role, "content": message.content}
        if message.role == "assistant" and message.tool_calls:
            if pending:
                raise ProviderError("provider_protocol")
            calls = []
            for raw in message.tool_calls:
                try:
                    call = ModelToolCall.model_validate(raw)
                except ValueError:
                    raise ProviderError("provider_protocol") from None
                if call.id in pending:
                    raise ProviderError("provider_protocol")
                pending.add(call.id)
                calls.append(
                    {
                        "id": call.id,
                        "type": "function",
                        "function": {"name": call.name, "arguments": encoded(call.arguments)},
                    }
                )
            item["tool_calls"] = cast(JsonValue, calls)
        elif message.role == "tool":
            if message.call_id not in pending:
                raise ProviderError("provider_protocol")
            pending.remove(message.call_id)
            item["tool_call_id"] = message.call_id
        elif pending:
            raise ProviderError("provider_protocol")
        result.append(item)
    if pending:
        raise ProviderError("provider_protocol")
    return result


def _usage(value: Any) -> TokenUsage | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ProviderError("provider_usage_conflict")
    fields = {
        target: value[source]
        for source, target in [
            ("prompt_tokens", "input_tokens"),
            ("completion_tokens", "output_tokens"),
            ("total_tokens", "total_tokens"),
        ]
        if value.get(source) is not None
    }
    # 只保留协议中明确报告的子计数；不保存opaque usage/cost原文。
    for source, targets in (
        ("prompt_tokens_details", (("cached_tokens", "cached_input_tokens"),)),
        ("completion_tokens_details", (("reasoning_tokens", "reasoning_output_tokens"),)),
    ):
        details = value.get(source)
        if details is None:
            continue
        if not isinstance(details, dict):
            raise ProviderError("provider_usage_conflict")
        for field, target in targets:
            if details.get(field) is not None:
                fields[target] = details[field]
    if not fields:
        return None
    try:
        return TokenUsage.model_validate(fields)
    except ValueError:
        raise ProviderError("provider_usage_conflict") from None


def _texts(value: str) -> list[str]:
    result, current, size = [], "", 0
    for character in value:
        length = len(character.encode())
        if size + length > 4096:
            result.append(current)
            current, size = "", 0
        current += character
        size += length
    if current:
        result.append(current)
    return result


def _sdk_messages(messages: Sequence[Message], *, allow_images: bool = False) -> list[SDKMessage]:
    # 在 SDK 的历史修复之前检查 Krim 的工具结果配对，避免静默修补无效历史。
    _messages(messages, allow_images=allow_images)
    result: list[SDKMessage] = []
    names: dict[str, str] = {}
    for message in messages:
        parts: list[Any] = []
        if message.role == "tool":
            parts.append(
                ToolResultPart(
                    tool_call_id=message.call_id or "",
                    tool_name=names[message.call_id or ""],
                    result=message.content,
                    model_input=message.content,
                )
            )
        else:
            if message.content or not message.tool_calls:
                parts.append(TextPart(text=message.content))
            if isinstance(message, HostImageMessage):
                parts.extend(
                    FilePart(
                        data=payload,
                        media_type=image.media_type,
                        filename=image.filename,
                    )
                    for image, payload in zip(
                        message.images, validate_host_images(message.images), strict=True
                    )
                )
            for raw in message.tool_calls:
                call = ModelToolCall.model_validate(raw)
                names[call.id] = call.name
                parts.append(
                    ToolCallPart(
                        tool_call_id=call.id,
                        tool_name=call.name,
                        tool_args=encoded(call.arguments),
                    )
                )
        result.append(SDKMessage(role=message.role, parts=parts))
    return result


@dataclass
class _StreamBoundary:
    """保留 Krim 的资源与完整性约束；模型事件的解析和聚合交给 SDK。"""

    config: OpenAICompatibleConfig
    max_tool_calls: int | None
    calls: dict[int, _ToolBuffer] = dataclass_field(default_factory=dict)
    sdk_ids: dict[int, str] = dataclass_field(default_factory=dict)
    snapshots: list[TokenUsage] = dataclass_field(default_factory=list)
    prior: TokenUsage | None = None
    finish: str | None = None
    done: bool = False
    error: ProviderError | None = None
    response: httpx.Response | None = None

    def accept(self, payload: str) -> str:
        if payload.strip() == "[DONE]":
            self.done = True
            self.complete()
            return "[DONE]"
        event = parse_json(payload)
        if not isinstance(event, dict) or "error" in event:
            raise ProviderError("provider_protocol")
        event_reasoning = event.get("reasoning_content")
        if event_reasoning is not None and not isinstance(event_reasoning, str):
            raise ProviderError("provider_protocol")
        if "reasoning_content" in event:
            event.pop("reasoning_content")
        usage = _usage(event.get("usage"))
        if usage is not None:
            if self.prior is not None and not usage.follows(self.prior):
                raise ProviderError("provider_usage_conflict")
            if usage != self.prior:
                self.snapshots.append(usage)
                self.prior = usage
        choices = event.get("choices")
        if choices == []:
            if self.finish is None or usage is None:
                raise ProviderError("provider_protocol")
            if "reasoning_content" in event:
                event.pop("reasoning_content")
            return encoded(event)
        if not isinstance(choices, list) or len(choices) != 1 or not isinstance(choices[0], dict):
            raise ProviderError("provider_protocol")
        choice = choices[0]
        delta = choice.get("delta", {})
        if (
            type(choice.get("index")) is not int
            or choice["index"] != 0
            or not isinstance(delta, dict)
            or self.finish is not None
        ):
            raise ProviderError("provider_protocol")
        allowed = {"role", "content", "tool_calls", "refusal", "reasoning_content"}
        reasoning = delta.get("reasoning_content")
        if reasoning is not None and not isinstance(reasoning, str):
            raise ProviderError("provider_protocol")
        if (
            set(delta) - allowed
            or delta.get("role", "assistant") != "assistant"
            or delta.get("refusal")
        ):
            raise ProviderError("provider_protocol")
        if delta.get("content") is not None and not isinstance(delta["content"], str):
            raise ProviderError("provider_protocol")
        tool_deltas = delta.get("tool_calls", [])
        if not isinstance(tool_deltas, list):
            raise ProviderError("provider_protocol")
        for raw in tool_deltas:
            if not isinstance(raw, dict):
                raise ProviderError("provider_protocol")
            index = raw.get("index")
            if type(index) is not int or index < 0:
                raise ProviderError("provider_protocol")
            buffer = self.calls.setdefault(index, _ToolBuffer())
            buffer.append(raw, self.config)
            # SDK 在 ToolStart 时固定内部 ID；外部元数据仍可分片到达。
            function = cast(dict[str, Any], raw.setdefault("function", {}))
            if index not in self.sdk_ids and buffer.identifier:
                self.sdk_ids[index] = buffer.identifier
                function["arguments"] = buffer.arguments
            if index in self.sdk_ids:
                raw["id"] = self.sdk_ids[index]
                if buffer.name:
                    function["name"] = buffer.name
            else:
                function["arguments"] = ""
        reason = choice.get("finish_reason")
        if reason == "length":
            raise ProviderError("provider_output_limit")
        if reason is not None:
            if reason not in ("stop", "tool_calls"):
                raise ProviderError("provider_protocol")
            self.finish = reason
        # Auxiliary reasoning is validated but never forwarded to the SDK or user-facing events.
        if "reasoning_content" in delta:
            delta.pop("reasoning_content")
        return encoded(event)

    def complete(self) -> list[ModelToolCall]:
        if any(index != expected for expected, index in enumerate(sorted(self.calls))):
            raise ProviderError("provider_protocol")
        if not self.done or self.finish is None:
            raise ProviderError("provider_incomplete_stream")
        if bool(self.calls) != (self.finish == "tool_calls"):
            raise ProviderError("provider_protocol")
        complete = [self.calls[index].collect() for index in sorted(self.calls)]
        if len({call.id for call in complete}) != len(complete):
            raise ProviderError("provider_protocol")
        if self.max_tool_calls is not None and len(complete) > self.max_tool_calls:
            raise ProviderError("tool_limit")
        return complete


@dataclass
class _ResponsesBoundary:
    """Bound Responses SSE; SDK still owns message conversion and tool hydration."""

    config: OpenAICompatibleConfig
    max_tool_calls: int | None
    calls: dict[str, _ToolBuffer] = dataclass_field(default_factory=dict)
    finished: set[str] = dataclass_field(default_factory=set)
    indices: dict[int, str] = dataclass_field(default_factory=dict)
    delta_seen: set[str] = dataclass_field(default_factory=set)
    sdk_ids: dict[int, str] = dataclass_field(default_factory=dict)
    snapshots: list[TokenUsage] = dataclass_field(default_factory=list)
    prior: TokenUsage | None = None
    done: bool = False
    error: ProviderError | None = None
    response: httpx.Response | None = None
    completed_calls: list[ModelToolCall] = dataclass_field(default_factory=list)

    def _item(self, item: Any) -> tuple[str, _ToolBuffer]:
        if not isinstance(item, dict) or item.get("type") != "function_call":
            raise ProviderError("provider_protocol")
        identifier = item.get("id")
        if not isinstance(identifier, str) or not 1 <= len(identifier) <= 128:
            raise ProviderError("provider_protocol")
        buffer = _ToolBuffer()
        buffer.append(
            {
                "id": item.get("call_id"),
                "function": {
                    "name": item.get("name"),
                    "arguments": item.get("arguments", ""),
                },
            },
            self.config,
        )
        if not buffer.identifier or not buffer.name:
            raise ProviderError("provider_protocol")
        return identifier, buffer

    def accept(self, payload: str) -> str:
        event = parse_json(payload)
        if not isinstance(event, dict) or not isinstance(event.get("type"), str) or self.done:
            raise ProviderError("provider_protocol")
        kind = event["type"]
        if kind == "response.incomplete":
            raise ProviderError("provider_output_limit")
        if kind in {"error", "response.failed"}:
            raise ProviderError("provider_protocol")
        if kind == "response.output_item.added":
            item = event.get("item")
            if not isinstance(item, dict) or item.get("type") not in {
                "function_call",
                "message",
                "reasoning",
            }:
                raise ProviderError("provider_protocol")
            if item["type"] == "function_call":
                key, buffer = self._item(item)
                if key in self.calls:
                    raise ProviderError("provider_protocol")
                if self.max_tool_calls is not None and len(self.calls) >= self.max_tool_calls:
                    raise ProviderError("tool_limit")
                index = event.get("output_index")
                if type(index) is not int or index < 0 or index in self.indices:
                    raise ProviderError("provider_protocol")
                self.indices[index] = key
                self.calls[key] = buffer
        elif kind in {
            "response.function_call_arguments.delta",
            "response.function_call_arguments.done",
        }:
            delta_key = event.get("item_id")
            output_index = event.get("output_index")
            indexed_key = self.indices.get(output_index) if type(output_index) is int else None
            if delta_key is None:
                delta_key = indexed_key
            elif indexed_key is not None and indexed_key != delta_key:
                raise ProviderError("provider_protocol")
            if not isinstance(delta_key, str) or delta_key not in self.calls:
                raise ProviderError("provider_protocol")
            delta_buffer = self.calls[delta_key]
            if delta_key in self.finished:
                raise ProviderError("provider_protocol")
            if kind == "response.function_call_arguments.delta":
                delta = event.get("delta")
                if not isinstance(delta, str):
                    raise ProviderError("provider_protocol")
                delta_buffer.append({"function": {"arguments": delta}}, self.config)
                self.delta_seen.add(delta_key)
            else:
                arguments = event.get("arguments")
                if not isinstance(arguments, str):
                    raise ProviderError("provider_protocol")
                if delta_key not in self.delta_seen and not delta_buffer.arguments:
                    delta_buffer.append({"function": {"arguments": arguments}}, self.config)
                if arguments != delta_buffer.arguments:
                    raise ProviderError("provider_protocol")
        elif kind == "response.output_item.done":
            item = event.get("item")
            if not isinstance(item, dict):
                raise ProviderError("provider_protocol")
            if item.get("type") == "function_call":
                key, buffer = self._item(item)
                if self.calls.get(key) != buffer or key in self.finished:
                    raise ProviderError("provider_protocol")
                self.finished.add(key)
        elif kind == "response.completed":
            response = event.get("response")
            if (
                not isinstance(response, dict)
                or response.get("status") != "completed"
                or response.get("error")
            ):
                raise ProviderError("provider_protocol")
            output = response.get("output")
            if not isinstance(output, list):
                raise ProviderError("provider_protocol")
            keys = []
            for item in output:
                if not isinstance(item, dict) or item.get("type") not in {
                    "function_call",
                    "message",
                    "reasoning",
                }:
                    raise ProviderError("provider_protocol")
                if item["type"] == "function_call":
                    key, buffer = self._item(item)
                    if key not in self.finished or self.calls.get(key) != buffer:
                        raise ProviderError("provider_protocol")
                    keys.append(key)
                    self.completed_calls.append(buffer.collect())
            if len(keys) != len(set(keys)) or set(keys) != set(self.calls):
                raise ProviderError("provider_protocol")
            if len({call.id for call in self.completed_calls}) != len(keys):
                raise ProviderError("provider_protocol")
            self.sdk_ids = {index: call.id for index, call in enumerate(self.completed_calls)}
            usage = response.get("usage")
            if usage is not None:
                if not isinstance(usage, dict):
                    raise ProviderError("provider_usage_conflict")
                self.prior = _usage(
                    {
                        "prompt_tokens": usage.get("input_tokens"),
                        "completion_tokens": usage.get("output_tokens"),
                        "total_tokens": usage.get("total_tokens"),
                        "prompt_tokens_details": usage.get("input_tokens_details"),
                        "completion_tokens_details": usage.get("output_tokens_details"),
                    }
                )
                if self.prior is not None:
                    self.snapshots.append(self.prior)
            self.done = True
        return encoded(event)

    def complete(self) -> list[ModelToolCall]:
        if not self.done:
            raise ProviderError("provider_incomplete_stream")
        return self.completed_calls


class _GuardedStream(httpx.AsyncByteStream):
    def __init__(
        self, response: httpx.Response, boundary: _StreamBoundary | _ResponsesBoundary
    ) -> None:
        self.raw = httpx.Response(
            response.status_code,
            headers=response.headers,
            stream=response.stream,
            request=response.request,
        )
        self.boundary = boundary

    async def __aiter__(self) -> AsyncIterator[bytes]:
        try:
            async for payload in _sse_data(self.raw, self.boundary.config):
                if not payload.strip():
                    continue
                normalized = self.boundary.accept(payload)
                yield ("data: " + normalized + "\n\n").encode()
                if self.boundary.done:
                    return
            self.boundary.complete()
        except ProviderError as error:
            logging.getLogger(__name__).warning(
                "Provider boundary rejected: code=%s frames=%s",
                error.code,
                [
                    (frame.f_code.co_name, line)
                    for frame, line in traceback.walk_tb(error.__traceback__)
                ],
            )
            self.boundary.error = error
            raise
        except (ValueError, RecursionError):
            self.boundary.error = ProviderError("provider_protocol")
            raise self.boundary.error from None

    async def aclose(self) -> None:
        await settled(self.raw.aclose())


class OpenAICompatibleProvider:
    def __init__(
        self, config: OpenAICompatibleConfig, *, max_connections: int | None = None,
    ) -> None:
        if max_connections is not None and (
            type(max_connections) is not int or max_connections < 1
        ):
            raise ProviderError("provider_config", 422)
        # 拒绝绕过Pydantic构造/修改后的配置；错误不暴露输入。
        try:
            self.config = OpenAICompatibleConfig.model_validate(config.model_dump())
        except ValueError:
            raise ProviderError("provider_config", 422) from None
        self._connections = max_connections
        self._client = httpx.AsyncClient(
            base_url=self.config.base_url + "/",
            trust_env=False,
            follow_redirects=False,
            # None分量表示该阶段不设应用层硬超时；整体期限由运行层deadline执行。
            timeout=httpx.Timeout(
                connect=self.config.connect_timeout_seconds,
                read=self.config.read_timeout_seconds,
                write=self.config.write_timeout_seconds,
                pool=self.config.pool_timeout_seconds,
            ),
            limits=httpx.Limits(
                max_connections=max_connections, max_keepalive_connections=max_connections
            ),
        )
        self._owned_client = self._client
        self._boundary: ContextVar[_StreamBoundary | _ResponsesBoundary] = ContextVar(
            "krim_model_boundary"
        )
        self._client.event_hooks["request"].append(self._guard_request)
        self._client.event_hooks["response"].append(self._guard_response)
        self._sdk_client = openai.AsyncOpenAI(
            base_url=self.config.base_url + "/",
            api_key=self.config.api_key.get_secret_value() or "krim-local-no-auth",
            http_client=self._client,
            timeout=self._client.timeout,
            max_retries=0,
            _enforce_credentials=False,
        )
        self._sdk_provider = ai.get_provider(
            "openai",
            client=self._sdk_client,
            base_url=self.config.base_url,
            api_key=self.config.api_key.get_secret_value(),
            env={},
            protocol=(
                OpenAIResponsesProtocol()
                if self.config.wire_api == "responses"
                else OpenAIChatCompletionsProtocol()
            ),
        )
        self._sdk_model = ai.Model(id=self.config.model, provider=self._sdk_provider)
        self._sealed = self.binding_token()
        self._close_task: asyncio.Task[None] | None = None
        self.calls = self.active = self.streams_closed = 0
        self.closed = False

    async def _guard_request(self, request: httpx.Request) -> None:
        boundary = self._boundary.get()
        if not self.config.api_key.get_secret_value():
            request.headers.pop("Authorization", None)
        request.headers["Accept-Encoding"] = "identity"
        request.headers["Accept"] = "text/event-stream"
        request.headers["User-Agent"] = self._client.headers["User-Agent"]
        for header in tuple(request.headers):
            if header.startswith("x-stainless-") or header in {
                "openai-organization",
                "openai-project",
            }:
                del request.headers[header]
        # SDK 会发送 description: null；既有兼容网关要求缺省描述直接省略。
        body = parse_json(request.content.decode())
        if isinstance(body, dict):
            for tool in cast(list[dict[str, Any]], body.get("tools", [])):
                function = tool if self.config.wire_api == "responses" else tool.get("function", {})
                if function.get("description") in (None, ""):
                    function.pop("description", None)
            if self.config.wire_api == "responses":
                body["store"] = False
            wire = encoded(body).encode()
            request._content = wire
            request.stream = httpx.ByteStream(wire)
            request.headers["Content-Length"] = str(len(wire))
        if self.config.max_request_bytes is not None and len(
            request.content
        ) > self.config.max_request_bytes:
            boundary.error = ProviderError("provider_request_limit")
            raise boundary.error

    async def _guard_response(self, response: httpx.Response) -> None:
        boundary = self._boundary.get()
        boundary.response = response
        code = None
        if 300 <= response.status_code < 400:
            code = "provider_redirect"
        elif response.status_code != 200:
            code = "provider_http_error"
        elif (
            response.headers.get("content-type", "").split(";")[0].strip().lower()
            != "text/event-stream"
            or response.headers.get("content-encoding", "identity").lower() != "identity"
        ):
            code = "provider_protocol"
        if code:
            boundary.error = ProviderError(code)
            await settled(response.aclose())
            raise boundary.error
        response.stream = _GuardedStream(response, boundary)

    def binding_token(self) -> tuple[object, ...]:
        """仅私有内存使用，含凭据的值绝不repr/序列化。"""
        config = self.config.model_dump(mode="json", exclude={"api_key", "base_url"})
        # Keep the historical no-image binding stable; opt-in is explicit and bound.
        if not self.config.allow_images:
            config.pop("allow_images", None)
        if self.config.wire_api == "chat_completions":
            config.pop("wire_api", None)
        return (
            id(self.config),
            self.config.base_url,
            self.config.api_key.get_secret_value(),
            encoded(config),
            id(self._client),
            id(self._sdk_client),
            id(self._sdk_client._client),
            str(self._sdk_client.base_url),
            self._sdk_client.api_key,
            self._sdk_client.max_retries,
            repr(self._sdk_client.timeout),
            id(self._sdk_provider),
            id(self._sdk_provider.client),
            id(self._sdk_model),
            id(self._sdk_model.protocol),
            self._sdk_model.id,
            id(self._sdk_model.provider),
            id(self._sdk_provider.protocol),
            tuple(
                tuple(id(hook) for hook in self._client.event_hooks[key])
                for key in ("request", "response")
            ),
            id(getattr(self._client.stream, "__func__", self._client.stream)),
            str(self._client.base_url),
            tuple(self._client.headers.multi_items()),
            tuple(self._client.timeout.as_dict().items()),
            self._client.follow_redirects,
            self._client.trust_env,
            id(self._client._transport),
            id(getattr(self._client.send, "__func__", self._client.send)),
            id(getattr(self._client.build_request, "__func__", self._client.build_request)),
            id(getattr(self.stream, "__func__", self.stream)),
            id(getattr(self.stream_with_budget, "__func__", self.stream_with_budget)),
            getattr(getattr(self._client._transport, "_pool", None), "_max_connections", None),
            getattr(
                getattr(self._client._transport, "_pool", None), "_max_keepalive_connections", None
            ),
        )

    def identity(self) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {
            "model": self.config.model,
            "endpoint_fingerprint": fingerprint(self.config.base_url),
        }
        if self.config.reasoning_effort is not None:
            result["reasoning_effort"] = self.config.reasoning_effort
        if self.config.thinking is not None:
            result["thinking"] = self.config.thinking
        if self.config.wire_api == "responses":
            result["wire_api"] = "responses"
        return result

    def execution_limits(self) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = self.config.model_dump(
            mode="json", exclude={"api_key", "base_url", "model"}, exclude_none=True
        )
        # Explicit None binds the absence of provider aggregate quotas on resume.
        result.update({name: getattr(self.config, name) for name in (
            "max_output_tokens", "max_request_bytes",
            "max_response_bytes", "max_event_bytes", "max_events",
            "max_arguments_bytes", "max_tool_calls",
            "connect_timeout_seconds", "read_timeout_seconds",
            "write_timeout_seconds", "pool_timeout_seconds",
        )})
        if not self.config.allow_images:
            result.pop("allow_images", None)
        if self.config.wire_api == "chat_completions":
            result.pop("wire_api", None)
        for key in (
            "connect_timeout_seconds",
            "read_timeout_seconds",
            "write_timeout_seconds",
            "pool_timeout_seconds",
        ):
            value = result[key]
            result[key] = None if value is None else float(cast(float, value))
        return result | {
            "schema_version": 1,
            "max_connections": self._connections,
            "max_keepalive_connections": self._connections,
            "max_call_id_chars": 128,
            "max_tool_name_chars": 64,
            "max_json_depth": 16,
            "max_safe_integer": 9007199254740991,
            "text_chunk_bytes": 4096,
        }

    async def stream(
        self, messages: Sequence[Message], tools: list[dict[str, JsonValue]]
    ) -> AsyncIterator[ModelEvent]:
        source = self.stream_with_budget(messages, tools, None)
        try:
            async for event in source:
                yield event
        finally:
            await settled(source.aclose())

    async def stream_with_diagnostics(
        self,
        messages: Sequence[Message],
        tools: list[dict[str, JsonValue]],
        budget: ModelRequestBudget | None,
    ) -> AsyncIterator[ModelStreamEvent]:
        source = self.stream_with_budget(messages, tools, budget, include_progress=True)
        try:
            async for event in source:
                yield event
        finally:
            await settled(source.aclose())

    async def stream_with_budget(
        self,
        messages: Sequence[Message],
        tools: list[dict[str, JsonValue]],
        budget: ModelRequestBudget | None,
        *,
        include_progress: bool = False,
    ) -> AsyncGenerator[ModelStreamEvent, None]:
        if self.closed:
            raise ProviderError("closed", 503)
        if self.binding_token() != self._sealed:
            raise ServiceError("target_binding_changed")
        max_tool_calls = self.config.max_tool_calls
        if budget is not None:
            step_cap = budget.max_tool_calls_this_step
            if step_cap is None:
                pass  # 步内批次不限：保留配置上限（或None=完全不裁）。
            elif max_tool_calls is None:
                max_tool_calls = step_cap
            else:
                max_tool_calls = min(max_tool_calls, step_cap)
        responses = self.config.wire_api == "responses"
        boundary = (
            _ResponsesBoundary(self.config, max_tool_calls)
            if responses
            else _StreamBoundary(self.config, max_tool_calls)
        )
        # Keep each protocol's native token field; Responses requests are stateless.
        # None leaves the output cap to the provider's real capability: no invented
        # default presses it down, and a provider-side reject surfaces as-is.
        extra: dict[str, Any] = (
            {"store": False}
            if responses
            else {"n": 1}
        )
        if self.config.max_output_tokens is not None:
            if responses:
                extra["max_output_tokens"] = self.config.max_output_tokens
            else:
                extra["max_tokens"] = self.config.max_output_tokens
        if self.config.reasoning_effort is not None:
            if responses:
                extra["reasoning"] = {"effort": self.config.reasoning_effort}
            else:
                extra["reasoning_effort"] = self.config.reasoning_effort
        if self.config.thinking is not None:
            if responses:
                raise ProviderError("provider_config", 422)
            extra["thinking"] = {"type": self.config.thinking}
        declarations = (
            [
                Tool(
                    kind="function",
                    name=str(tool["name"]),
                    spec=ToolSpec(
                        params=cast(dict[str, Any], tool["input_schema"]),
                        description=cast(str | None, tool.get("description")),
                    ),
                )
                for tool in tools
            ]
            if max_tool_calls is None or max_tool_calls > 0
            else []
        )
        if declarations and max_tool_calls == 1:
            extra["parallel_tool_calls"] = False
        projected = _sdk_messages(messages, allow_images=self.config.allow_images)
        self.calls += 1
        self.active += 1
        argument_bytes = 0
        streamed_arguments: dict[str, int] = {}
        try:
            async with ai.stream(
                self._sdk_model,
                projected,
                tools=declarations,
                params=ai.InferenceRequestParams(extra_body=extra),
            ) as stream:
                while True:
                    # 仅在 SDK 拉取事件期间绑定，不能跨 yield 保留 ContextVar token：
                    # Krim 的 settled(aclose) 会在独立清理任务中关闭生成器。
                    token = self._boundary.set(boundary)
                    try:
                        try:
                            event = await anext(stream)
                        except StopAsyncIteration:
                            break
                    finally:
                        self._boundary.reset(token)
                    while boundary.snapshots:
                        yield ModelUsage(usage=boundary.snapshots.pop(0), final=False)
                    if isinstance(event, ai.events.TextDelta):
                        for part in _texts(event.chunk):
                            if include_progress:
                                yield ModelProgress(phase="text", argument_bytes=argument_bytes)
                            yield ModelText(text=part)
                    elif isinstance(event, ai.events.ToolStart):
                        if include_progress:
                            yield ModelProgress(phase="tool_start", argument_bytes=argument_bytes)
                    elif isinstance(event, ai.events.ToolDelta):
                        call_id = event.tool_call_id
                        delta_bytes = len(event.chunk.encode("utf-8"))
                        streamed_arguments[call_id] = (
                            streamed_arguments.get(call_id, 0) + delta_bytes
                        )
                        argument_bytes += delta_bytes
                        if include_progress:
                            yield ModelProgress(
                                phase="tool_arguments", argument_bytes=argument_bytes,
                            )
                    elif isinstance(event, ai.events.ToolEnd):
                        call = event.tool_call
                        call_id = call.tool_call_id
                        complete_bytes = len(call.tool_args.encode("utf-8"))
                        streamed_bytes = streamed_arguments.get(call_id, 0)
                        if complete_bytes > streamed_bytes:
                            argument_bytes += complete_bytes - streamed_bytes
                            streamed_arguments[call_id] = complete_bytes
                        if include_progress:
                            yield ModelProgress(
                                phase="tool_complete", argument_bytes=argument_bytes,
                            )
                    elif include_progress:
                        # Includes provider-only/reasoning activity without retaining
                        # its payload, IDs, names, or any other content.
                        yield ModelProgress(phase="response", argument_bytes=argument_bytes)
                complete = boundary.complete()
                sdk_calls = {call.tool_call_id: call for call in stream.tool_calls}
                if len(sdk_calls) != len(complete):
                    raise ProviderError("provider_protocol")
                for index, call in enumerate(complete):
                    sdk_call = sdk_calls.get(boundary.sdk_ids[index])
                    if (
                        sdk_call is None
                        or parse_json(sdk_call.tool_args) != call.arguments
                        or responses
                        and sdk_call.tool_name != call.name
                    ):
                        raise ProviderError("provider_protocol")
                # SDK 的 ToolStart 只带最初名称；分片工具名仍使用边界完成校验的名称。
                # 调用 ID 和参数必须同时通过 SDK 聚合与 Krim 的严格 JSON 校验。
            if boundary.response is not None:
                await settled(boundary.response.aclose())
            for call in complete:
                yield call
            if boundary.prior is not None:
                yield ModelUsage(usage=boundary.prior, final=True)
        except Exception as error:
            if boundary.error is not None:
                raise boundary.error from None
            if isinstance(error, ServiceError):
                raise
            cause: BaseException | None = error
            for _ in range(16):
                if isinstance(cause, (httpx.TimeoutException, openai.APITimeoutError)):
                    raise ProviderError("provider_timeout") from None
                if isinstance(cause, (httpx.HTTPError, openai.APIConnectionError)):
                    raise ProviderError("provider_network") from None
                cause = cause.__cause__ if cause is not None else None
            logging.getLogger(__name__).warning(
                "Provider SDK rejected: exception_type=%s frames=%s",
                type(error).__name__,
                [
                    (frame.f_code.co_name, line)
                    for frame, line in traceback.walk_tb(error.__traceback__)
                ],
            )
            raise ProviderError("provider_protocol") from None
        finally:
            try:
                if boundary.response is not None:
                    await settled(boundary.response.aclose())
            finally:
                self.active -= 1
                self.streams_closed += 1

    async def close(self) -> None:
        self.closed = True
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._owned_client.aclose())
        await settled(self._close_task)
