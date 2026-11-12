"""Python AI SDK 的 Chat Completions 接入及 Krim 出站/响应边界。

SDK 负责模型请求和流式模型事件；完整 finish/DONE 和工具 JSON 校验后才释放调用。
"""

from __future__ import annotations

import asyncio
import os
import re
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
from ai.types.messages import Message as SDKMessage
from ai.types.messages import TextPart, ToolCallPart, ToolResultPart
from ai.types.tools import Tool, ToolSpec
from pydantic import Field, JsonValue, SecretStr, ValidationError, field_validator, model_validator

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

from .types import ModelEvent, ModelRequestBudget, ModelText, ModelToolCall, ModelUsage


class ProviderError(ServiceError):
    """仅本地稳定码，不携原URL、上游正文或异常链。"""


class OpenAICompatibleConfig(StrictModel):
    base_url: str = Field(min_length=1, max_length=2048, repr=False)
    model: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
    api_key: SecretStr = Field(default=SecretStr(""), repr=False)
    max_output_tokens: int = Field(default=512, ge=1, le=4096)
    reasoning_effort: Literal["xhigh"] | None = None
    thinking: Literal["disabled"] | None = None
    connect_timeout_seconds: float = Field(default=2.0, gt=0, le=2)
    read_timeout_seconds: float = Field(default=5.0, gt=0, le=90)
    write_timeout_seconds: float = Field(default=2.0, gt=0, le=2)
    pool_timeout_seconds: float = Field(default=2.0, gt=0, le=2)
    max_request_bytes: int = Field(default=32768, ge=256, le=32768)
    max_response_bytes: int = Field(default=131072, ge=256, le=131072)
    max_event_bytes: int = Field(default=16384, ge=128, le=16384)
    max_events: int = Field(default=1024, ge=1, le=1024)
    max_arguments_bytes: int = Field(default=4096, ge=2, le=4096)
    max_tool_calls: int = Field(default=1, ge=1, le=4, strict=True)

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
        if (
            self.max_event_bytes > self.max_response_bytes
            or self.max_arguments_bytes > self.max_event_bytes
        ):
            raise ValueError("Provider容量关系无效")
        return self

    @classmethod
    def from_env(cls) -> Self | None:
        names = (
            "BASE_URL",
            "MODEL",
            "API_KEY",
            "MAX_OUTPUT_TOKENS",
            "REASONING_EFFORT",
            "THINKING",
            "READ_TIMEOUT_SECONDS",
        )
        values = {
            name.lower(): os.environ["KRIM_AGENT_HTTP_" + name]
            for name in names
            if "KRIM_AGENT_HTTP_" + name in os.environ
        }
        if not values:
            return None
        try:
            if "max_output_tokens" in values:
                if re.fullmatch(r"[0-9]+", values["max_output_tokens"]) is None:
                    raise ValueError
            arguments: dict[str, Any] = dict(values)
            if "max_output_tokens" in values:
                arguments["max_output_tokens"] = int(values["max_output_tokens"])
            if "read_timeout_seconds" in values:
                if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", values["read_timeout_seconds"]) is None:
                    raise ValueError
                arguments["read_timeout_seconds"] = float(values["read_timeout_seconds"])
            return cls.model_validate(arguments)
        except (ValueError, ValidationError):
            raise ProviderError("provider_config", 422) from None


async def _sse_data(response: httpx.Response, config: OpenAICompatibleConfig) -> AsyncIterator[str]:
    """按真实字节计帧；延迟CR分派以完整计入跨TCP块的CRLF，不分发残帧。"""
    line = bytearray()
    lines: list[bytes] = []
    response_size = event_size = count = 0
    pending_cr = has_frame = False

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
            if count > config.max_events:
                raise ProviderError("provider_event_limit")
        try:
            payload = b"\n".join(lines).decode("utf-8", errors="strict") if lines else None
        except UnicodeError:
            raise ProviderError("provider_protocol") from None
        lines.clear()
        event_size, has_frame = 0, False
        return payload

    async for chunk in response.aiter_raw():
        response_size += len(chunk)
        if response_size > config.max_response_bytes:
            raise ProviderError("provider_response_limit")
        for byte in chunk:
            if pending_cr:
                pending_cr = False
                if byte == 10:
                    event_size += 1
                    if event_size > config.max_event_bytes:
                        raise ProviderError("provider_event_limit")
                    payload = finish_line()
                    if payload is not None:
                        yield payload
                    continue
                payload = finish_line()
                if payload is not None:
                    yield payload
            event_size += 1
            if event_size > config.max_event_bytes:
                raise ProviderError("provider_event_limit")
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
        if len(self.arguments.encode()) > config.max_arguments_bytes:
            raise ProviderError("provider_tool_arguments")

    def collect(self) -> ModelToolCall:
        try:
            value = parse_json(self.arguments)
            if not isinstance(value, dict):
                raise ValueError
            return ModelToolCall(id=self.identifier, name=self.name, arguments=value)
        except (ValueError, RecursionError):
            raise ProviderError("provider_tool_arguments") from None


def _messages(messages: Sequence[Message]) -> list[dict[str, JsonValue]]:
    result: list[dict[str, JsonValue]] = []
    pending: set[str] = set()
    for message in messages:
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


def _sdk_messages(messages: Sequence[Message]) -> list[SDKMessage]:
    # 在 SDK 的历史修复之前检查 Krim 的工具结果配对，避免静默修补无效历史。
    _messages(messages)
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
    max_tool_calls: int
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
        allowed = {"role", "content", "tool_calls", "refusal"}
        if self.config.thinking == "disabled":
            allowed.add("reasoning_content")
            if delta.get("reasoning_content") not in (None, ""):
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
            if type(index) is not int or not 0 <= index < self.config.max_response_bytes:
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
        return encoded(event)

    def complete(self) -> list[ModelToolCall]:
        if sorted(self.calls) != list(range(len(self.calls))):
            raise ProviderError("provider_protocol")
        if not self.done or self.finish is None:
            raise ProviderError("provider_incomplete_stream")
        if bool(self.calls) != (self.finish == "tool_calls"):
            raise ProviderError("provider_protocol")
        complete = [self.calls[index].collect() for index in sorted(self.calls)]
        if len({call.id for call in complete}) != len(complete):
            raise ProviderError("provider_protocol")
        if len(complete) > self.max_tool_calls:
            raise ProviderError("tool_limit")
        return complete


class _GuardedStream(httpx.AsyncByteStream):
    def __init__(self, response: httpx.Response, boundary: _StreamBoundary) -> None:
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
            self.boundary.error = error
            raise
        except (ValueError, RecursionError):
            self.boundary.error = ProviderError("provider_protocol")
            raise self.boundary.error from None

    async def aclose(self) -> None:
        await settled(self.raw.aclose())


class OpenAICompatibleProvider:
    def __init__(self, config: OpenAICompatibleConfig, *, max_connections: int = 4) -> None:
        if type(max_connections) is not int or not 1 <= max_connections <= 16:
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
        self._boundary: ContextVar[_StreamBoundary] = ContextVar("krim_model_boundary")
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
            protocol=OpenAIChatCompletionsProtocol(),
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
                function = tool.get("function", {})
                if function.get("description") in (None, ""):
                    function.pop("description", None)
            wire = encoded(body).encode()
            request._content = wire
            request.stream = httpx.ByteStream(wire)
            request.headers["Content-Length"] = str(len(wire))
        if len(request.content) > self.config.max_request_bytes:
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
        return (
            id(self.config),
            self.config.base_url,
            self.config.api_key.get_secret_value(),
            encoded(self.config.model_dump(mode="json", exclude={"api_key", "base_url"})),
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
        return result

    def execution_limits(self) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = self.config.model_dump(
            mode="json", exclude={"api_key", "base_url", "model"}, exclude_none=True
        )
        for key in (
            "connect_timeout_seconds",
            "read_timeout_seconds",
            "write_timeout_seconds",
            "pool_timeout_seconds",
        ):
            result[key] = float(cast(float, result[key]))
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

    async def stream_with_budget(
        self,
        messages: Sequence[Message],
        tools: list[dict[str, JsonValue]],
        budget: ModelRequestBudget | None,
    ) -> AsyncGenerator[ModelEvent, None]:
        if self.closed:
            raise ProviderError("closed", 503)
        if self.binding_token() != self._sealed:
            raise ServiceError("target_binding_changed")
        max_tool_calls = self.config.max_tool_calls
        if budget is not None:
            max_tool_calls = min(max_tool_calls, budget.max_tool_calls_this_step)
        boundary = _StreamBoundary(self.config, max_tool_calls)
        # 保留现有兼容端点的参数名，而不是 SDK 默认的 max_completion_tokens。
        extra: dict[str, Any] = {"max_tokens": self.config.max_output_tokens, "n": 1}
        if self.config.reasoning_effort is not None:
            extra["reasoning_effort"] = self.config.reasoning_effort
        if self.config.thinking is not None:
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
            if max_tool_calls
            else []
        )
        if declarations and max_tool_calls == 1:
            extra["parallel_tool_calls"] = False
        projected = _sdk_messages(messages)
        self.calls += 1
        self.active += 1
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
                            yield ModelText(text=part)
                complete = boundary.complete()
                sdk_calls = {call.tool_call_id: call for call in stream.tool_calls}
                if len(sdk_calls) != len(complete):
                    raise ProviderError("provider_protocol")
                for index, call in enumerate(complete):
                    sdk_call = sdk_calls.get(boundary.sdk_ids[index])
                    if sdk_call is None or parse_json(sdk_call.tool_args) != call.arguments:
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
