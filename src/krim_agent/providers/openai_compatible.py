"""有限HTTP/SSE适配；SRC-M3-01/02，固定3174b663旧解析逻辑适配，非官方完整协议。

只产模型事件；完整finish/DONE和全部工具JSON通过后才释放调用。无重试/重定向。
"""

from __future__ import annotations

import asyncio
import os
import re
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from typing import Any, Literal, Self, cast
from urllib.parse import urlsplit, urlunsplit

import httpx
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

from .types import ModelEvent, ModelText, ModelToolCall, ModelUsage


class ProviderError(ServiceError):
    """仅本地稳定码，不携原URL、上游正文或异常链。"""


class OpenAICompatibleConfig(StrictModel):
    base_url: str = Field(min_length=1, max_length=2048, repr=False)
    model: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
    api_key: SecretStr = Field(default=SecretStr(""), repr=False)
    max_output_tokens: int = Field(default=512, ge=1, le=4096)
    reasoning_effort: Literal["xhigh"] | None = None
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
        if delta.get("type", "function") != "function":
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
                setattr(self, field, getattr(self, field) + value)
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
        self._sealed = self.binding_token()
        self._close_task: asyncio.Task[None] | None = None
        self.calls = self.active = self.streams_closed = 0
        self.closed = False

    def binding_token(self) -> tuple[object, ...]:
        """仅私有内存使用，含凭据的值绝不repr/序列化。"""
        return (
            id(self.config),
            self.config.base_url,
            self.config.api_key.get_secret_value(),
            encoded(self.config.model_dump(mode="json", exclude={"api_key", "base_url"})),
            id(self._client),
            id(getattr(self._client.stream, "__func__", self._client.stream)),
            str(self._client.base_url),
            tuple(self._client.headers.multi_items()),
            tuple(self._client.timeout.as_dict().items()),
            self._client.follow_redirects,
            self._client.trust_env,
            id(self._client._transport),
            id(getattr(self._client.send, "__func__", self._client.send)),
            id(getattr(self._client.build_request, "__func__", self._client.build_request)),
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
        if self.closed:
            raise ProviderError("closed", 503)
        if self.binding_token() != self._sealed:
            raise ServiceError("target_binding_changed")
        body: dict[str, JsonValue] = {
            "model": self.config.model,
            "messages": cast(JsonValue, _messages(messages)),
            "stream": True,
            "n": 1,
            "max_tokens": self.config.max_output_tokens,
            "stream_options": {"include_usage": True},
        }
        if self.config.reasoning_effort is not None:
            body["reasoning_effort"] = self.config.reasoning_effort
        if tools:
            body["tools"] = [
                {
                    "type": "function",
                    "function": {"name": tool["name"], "parameters": tool["input_schema"]},
                }
                for tool in tools
            ]
        try:
            wire = encoded(body).encode()
        except (ValueError, RecursionError):
            raise ProviderError("provider_request_limit") from None
        if len(wire) > self.config.max_request_bytes:
            raise ProviderError("provider_request_limit")
        headers = {
            "Accept": "text/event-stream",
            "Content-Type": "application/json",
            "Accept-Encoding": "identity",
        }
        if self.config.api_key.get_secret_value():
            headers["Authorization"] = "Bearer " + self.config.api_key.get_secret_value()
        response = None
        self.calls += 1
        self.active += 1
        try:
            request = self._client.build_request(
                "POST", "chat/completions", content=wire, headers=headers
            )
            response = await self._client.send(request, stream=True)
            if 300 <= response.status_code < 400:
                raise ProviderError("provider_redirect")
            if response.status_code != 200:
                raise ProviderError("provider_http_error")
            if (
                response.headers.get("content-type", "").split(";")[0].strip().lower()
                != "text/event-stream"
                or response.headers.get("content-encoding", "identity").lower() != "identity"
            ):
                raise ProviderError("provider_protocol")
            calls: dict[int, _ToolBuffer] = {}
            finish = None
            prior: TokenUsage | None = None
            done = False
            async for payload in _sse_data(response, self.config):
                if payload.strip() == "[DONE]":
                    done = True
                    break
                if not payload.strip():
                    continue
                try:
                    event = parse_json(payload)
                except (ValueError, RecursionError):
                    raise ProviderError("provider_protocol") from None
                if not isinstance(event, dict) or "error" in event:
                    raise ProviderError("provider_protocol")
                usage = _usage(event.get("usage"))
                if usage is not None:
                    if prior is not None and not usage.follows(prior):
                        raise ProviderError("provider_usage_conflict")
                    if usage != prior:
                        yield ModelUsage(usage=usage, final=False)
                        prior = usage
                choices = event.get("choices")
                if choices == []:
                    if finish is None or usage is None:
                        raise ProviderError("provider_protocol")
                    continue
                if (
                    not isinstance(choices, list)
                    or len(choices) != 1
                    or not isinstance(choices[0], dict)
                ):
                    raise ProviderError("provider_protocol")
                choice = choices[0]
                delta = choice.get("delta", {})
                if (
                    type(choice.get("index")) is not int
                    or choice["index"] != 0
                    or not isinstance(delta, dict)
                    or finish is not None
                ):
                    raise ProviderError("provider_protocol")
                if (
                    set(delta) - {"role", "content", "tool_calls", "refusal"}
                    or delta.get("role", "assistant") != "assistant"
                    or delta.get("refusal")
                ):
                    raise ProviderError("provider_protocol")
                text = delta.get("content")
                if text is not None and not isinstance(text, str):
                    raise ProviderError("provider_protocol")
                if text:
                    for part in _texts(text):
                        yield ModelText(text=part)
                tool_deltas = delta.get("tool_calls", [])
                if not isinstance(tool_deltas, list):
                    raise ProviderError("provider_protocol")
                for raw in tool_deltas:
                    if not isinstance(raw, dict):
                        raise ProviderError("provider_protocol")
                    index = raw.get("index")
                    if type(index) is not int or not 0 <= index < self.config.max_tool_calls:
                        raise ProviderError("provider_protocol")
                    calls.setdefault(index, _ToolBuffer()).append(raw, self.config)
                reason = choice.get("finish_reason")
                if reason == "length":
                    raise ProviderError("provider_output_limit")
                if reason is not None:
                    if reason not in ("stop", "tool_calls"):
                        raise ProviderError("provider_protocol")
                    finish = reason
            if not done or finish is None:
                raise ProviderError("provider_incomplete_stream")
            if bool(calls) != (finish == "tool_calls"):
                raise ProviderError("provider_protocol")
            if sorted(calls) != list(range(len(calls))):
                raise ProviderError("provider_protocol")
            complete = [calls[index].collect() for index in sorted(calls)]
            if len({call.id for call in complete}) != len(complete):
                raise ProviderError("provider_protocol")
            await settled(response.aclose())
            response = None
            for call in complete:
                yield call
            if prior is not None:
                yield ModelUsage(usage=prior, final=True)
        except httpx.TimeoutException:
            raise ProviderError("provider_timeout") from None
        except httpx.HTTPError:
            raise ProviderError("provider_network") from None
        except (ValueError, RecursionError):
            raise ProviderError("provider_protocol") from None
        finally:
            try:
                if response is not None:
                    await settled(response.aclose())
            finally:
                self.active -= 1
                self.streams_closed += 1

    async def close(self) -> None:
        self.closed = True
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._owned_client.aclose())
        await settled(self._close_task)
