"""Python Chat Completions 文本/工具 SSE 子集；合约与来源见 ADR-0014。"""

from __future__ import annotations

import json
import re
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import httpx
from pydantic import ConfigDict, Field, SecretStr, ValidationError, field_validator

from krim_agent.protocol.domain import MessageView
from krim_agent.protocol.models import StrictModel, check_json
from krim_agent.tools.core import ToolDefinition
from krim_agent.usage import TokenUsage

from .types import ModelText, ModelToolCall, ModelUsage


class ProviderError(RuntimeError):
    """安全的本地错误消息和可持久保存的稳定错误码。"""

    def __init__(self, code: str, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code


class OpenAICompatibleConfig(StrictModel):
    model_config = ConfigDict(hide_input_in_errors=True)

    base_url: str = Field(min_length=1, max_length=2048)
    model: str = Field(min_length=1, max_length=256)
    api_key: SecretStr = SecretStr("")
    timeout_seconds: float = Field(default=60, gt=0, le=600)
    max_output_tokens: int | None = Field(default=None, ge=1, le=1_000_000)
    include_usage: bool = False
    max_request_bytes: int = Field(default=4_194_304, ge=1024, le=67_108_864)
    max_response_bytes: int = Field(default=4_194_304, ge=1024, le=67_108_864)
    max_event_bytes: int = Field(default=1_048_576, ge=256, le=16_777_216)
    max_tool_calls: int = Field(default=8, ge=1, le=128)

    @field_validator("base_url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        if any(character.isspace() or ord(character) < 32 for character in value):
            raise ValueError("provider base_url 不能包含空白或控制字符")
        try:
            parsed = urlsplit(value)
            _ = parsed.port
        except ValueError as error:
            raise ValueError("provider base_url 端口或 URL 格式无效") from error
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("provider base_url 必须是 http/https URL")
        if (
            parsed.username is not None
            or parsed.password is not None
            or "?" in value
            or "#" in value
        ):
            raise ValueError("provider base_url 不能包含凭证、查询参数或 fragment")
        if parsed.scheme == "http" and parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("非本机 provider 必须使用 HTTPS")
        return value.rstrip("/")

    @field_validator("api_key")
    @classmethod
    def validate_key(cls, value: SecretStr) -> SecretStr:
        if any(
            ord(character) < 32 or ord(character) >= 127 for character in value.get_secret_value()
        ):
            raise ValueError("provider api_key 必须是可打印 ASCII 字符")
        return value


def _tool_call(identifier: str, name: str, arguments: Any) -> ModelToolCall:
    if not identifier or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", name):
        raise ProviderError("provider_tool_call", "provider 工具调用缺少有效 ID 或名称")
    if not isinstance(arguments, dict):
        raise ProviderError("provider_tool_arguments", "provider 工具参数必须是 JSON 对象")
    try:
        check_json(arguments)
        return ModelToolCall(id=identifier, name=name, arguments=arguments)
    except (ValueError, RecursionError) as error:
        raise ProviderError("provider_tool_arguments", "provider 工具参数超出 JSON 限制") from error


def _messages(messages: Sequence[MessageView]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for message in messages:
        item: dict[str, Any] = {"role": message.role, "content": message.content}
        calls_data = message.details.get("tool_calls")
        if message.role == "assistant" and calls_data is not None:
            if not isinstance(calls_data, list):
                raise ProviderError("provider_messages", "assistant 工具历史必须是列表")
            calls = []
            for raw in calls_data:
                if not isinstance(raw, dict):
                    raise ProviderError("provider_messages", "assistant 工具历史必须是对象")
                identifier, name = raw.get("id"), raw.get("name")
                if not isinstance(identifier, str) or not isinstance(name, str):
                    raise ProviderError(
                        "provider_messages", "assistant 工具历史 ID/名称必须是字符串"
                    )
                try:
                    call = _tool_call(identifier, name, raw.get("arguments"))
                except ProviderError as error:
                    raise ProviderError("provider_messages", "assistant 工具历史无效") from error
                calls.append(
                    {
                        "id": call.id,
                        "type": "function",
                        "function": {
                            "name": call.name,
                            "arguments": json.dumps(
                                call.arguments, ensure_ascii=False, allow_nan=False
                            ),
                        },
                    }
                )
            if calls:
                item["tool_calls"] = calls
        if message.role == "tool":
            call_id = message.details.get("call_id")
            if not isinstance(call_id, str) or not call_id or len(call_id) > 128:
                raise ProviderError("provider_messages", "tool 历史缺少有效 call_id")
            item["tool_call_id"] = call_id
        result.append(item)
    return result


def _usage_report(value: object) -> TokenUsage | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ProviderError("provider_usage", "Provider 用量必须是对象")
    projected = {
        target: value.get(source)
        for source, target in (
            ("prompt_tokens", "input_tokens"),
            ("completion_tokens", "output_tokens"),
            ("total_tokens", "total_tokens"),
        )
        if value.get(source) is not None
    }
    for source, detail, target in (
        ("prompt_tokens_details", "cached_tokens", "cached_input_tokens"),
        ("completion_tokens_details", "reasoning_tokens", "reasoning_output_tokens"),
    ):
        details = value.get(source)
        if details is not None and not isinstance(details, dict):
            raise ProviderError("provider_usage", "Provider 用量细分必须是对象")
        if isinstance(details, dict) and details.get(detail) is not None:
            projected[target] = details[detail]
    if not projected:
        return None
    try:
        return TokenUsage.model_validate(projected)
    except ValidationError as error:
        raise ProviderError("provider_usage", "Provider 用量计数无效") from error


async def _sse_data(
    response: httpx.Response, *, max_response_bytes: int, max_event_bytes: int
) -> AsyncIterator[str]:
    """按字节限制 SSE，再按空行分发 data；支持多行和跨网络块 CR/LF。"""

    buffer = bytearray()
    event_lines: list[bytes] = []
    response_size = 0
    event_size = 0
    skip_lf = False
    async for chunk in response.aiter_bytes():
        response_size += len(chunk)
        if response_size > max_response_bytes:
            raise ProviderError("provider_response_limit", "provider SSE 响应超过大小限制")
        if skip_lf:
            if chunk.startswith(b"\n"):
                chunk = chunk[1:]
            skip_lf = False
        buffer.extend(chunk)
        while True:
            match = re.search(rb"[\r\n]", buffer)
            if match is None:
                if event_size + len(buffer) > max_event_bytes:
                    raise ProviderError("provider_event_limit", "provider SSE 事件超过大小限制")
                break
            end = match.start()
            line = bytes(buffer[:end])
            consumed = end + 1
            if buffer[end] == 13:
                if consumed < len(buffer) and buffer[consumed] == 10:
                    consumed += 1
                elif consumed == len(buffer):
                    skip_lf = True
            del buffer[:consumed]
            event_size += consumed
            if event_size > max_event_bytes:
                raise ProviderError("provider_event_limit", "provider SSE 事件超过大小限制")
            if not line:
                if event_lines:
                    try:
                        yield b"\n".join(event_lines).decode("utf-8", errors="strict")
                    except UnicodeDecodeError as error:
                        raise ProviderError(
                            "provider_response", "provider SSE 不是有效 UTF-8"
                        ) from error
                    event_lines.clear()
                event_size = 0
            elif line.startswith(b"data:"):
                value = line[5:]
                event_lines.append(value[1:] if value.startswith(b" ") else value)
            elif line == b"data":
                event_lines.append(b"")
    # EOF 不分发未闭合事件；上层必须看到完整 [DONE]，才认可终止。


@dataclass
class _ToolBuffer:
    identifier: str = ""
    name: str = ""
    arguments: str = ""

    def append(self, delta: dict[str, Any]) -> None:
        identifier = delta.get("id")
        if identifier is not None:
            if not isinstance(identifier, str):
                raise ProviderError("provider_tool_call", "provider 工具 ID 分片不是字符串")
            self.identifier += identifier
        function = delta.get("function", {})
        if not isinstance(function, dict) or delta.get("type", "function") != "function":
            raise ProviderError("provider_tool_call", "provider 只支持 function 工具调用")
        for key in ("name", "arguments"):
            fragment = function.get(key)
            if fragment is not None:
                if not isinstance(fragment, str):
                    raise ProviderError("provider_tool_call", "provider 工具分片不是字符串")
                setattr(self, key, getattr(self, key) + fragment)
        if len(self.identifier) > 128 or len(self.name) > 64:
            raise ProviderError("provider_tool_call", "provider 工具 ID 或名称超过限制")
        if len(self.arguments.encode("utf-8")) > 65536:
            raise ProviderError("provider_tool_arguments", "provider 工具参数超过大小限制")

    def collect(self) -> ModelToolCall:
        try:
            arguments = json.loads(self.arguments or "{}")
        except (ValueError, RecursionError) as error:
            raise ProviderError("provider_tool_arguments", "provider 工具参数 JSON 无效") from error
        return _tool_call(self.identifier, self.name, arguments)


class OpenAICompatibleProvider:
    name = "openai-compatible"

    @staticmethod
    def project_messages(messages: Sequence[MessageView]) -> list[dict[str, Any]]:
        """与实际请求复用同一消息投影，供本地 BPE 预算计数。"""
        return _messages(messages)

    def __init__(
        self, config: OpenAICompatibleConfig, *, transport: httpx.AsyncBaseTransport | None = None
    ) -> None:
        self.config = config
        headers = {"Content-Type": "application/json", "Accept": "text/event-stream"}
        key = config.api_key.get_secret_value()
        if key:
            headers["Authorization"] = f"Bearer {key}"
        self._http = httpx.AsyncClient(
            base_url=config.base_url + "/",
            headers=headers,
            timeout=config.timeout_seconds,
            trust_env=False,
            follow_redirects=False,
            transport=transport,
        )

    async def stream(
        self, messages: Sequence[MessageView], tools: Sequence[ToolDefinition]
    ) -> AsyncIterator[ModelText | ModelToolCall | ModelUsage]:
        if self._http.is_closed:
            raise ProviderError("provider_closed", "provider 已经关闭")
        body: dict[str, Any] = {
            "model": self.config.model,
            "messages": self.project_messages(messages),
            "stream": True,
            "n": 1,
        }
        if tools:
            body["tools"] = [
                {
                    "type": "function",
                    "function": {
                        "name": definition.name,
                        "description": definition.description,
                        "parameters": definition.parameters.model_json_schema(),
                    },
                }
                for definition in tools
            ]
        if self.config.max_output_tokens is not None:
            body["max_tokens"] = self.config.max_output_tokens
        if self.config.include_usage:
            body["stream_options"] = {"include_usage": True}
        try:
            check_json(body, max_bytes=self.config.max_request_bytes)
        except ValueError as error:
            raise ProviderError("provider_request_limit", "provider 请求超出 JSON 限制") from error
        calls: dict[int, _ToolBuffer] = {}
        finish_reason: str | None = None
        reported_usage: TokenUsage | None = None
        try:
            async with self._http.stream("POST", "chat/completions", json=body) as response:
                if 300 <= response.status_code < 400:
                    raise ProviderError(
                        "provider_redirect",
                        "provider 不允许重定向",
                        status_code=response.status_code,
                    )
                if response.status_code != 200:
                    raise ProviderError(
                        "provider_http_error",
                        f"provider 返回 HTTP {response.status_code}",
                        status_code=response.status_code,
                    )
                if (
                    response.headers.get("content-type", "").split(";")[0].strip().lower()
                    != "text/event-stream"
                ):
                    raise ProviderError("provider_content_type", "provider 未返回 SSE 内容")
                saw_done = False
                async for payload in _sse_data(
                    response,
                    max_response_bytes=self.config.max_response_bytes,
                    max_event_bytes=self.config.max_event_bytes,
                ):
                    if payload.strip() == "[DONE]":
                        saw_done = True
                        break
                    if not payload.strip():
                        continue
                    try:
                        event = json.loads(payload)
                    except (ValueError, RecursionError) as error:
                        raise ProviderError(
                            "provider_invalid_json", "provider SSE JSON 无效"
                        ) from error
                    if not isinstance(event, dict):
                        raise ProviderError("provider_response", "provider SSE 数据不是对象")
                    if "error" in event:
                        raise ProviderError("provider_error", "provider 返回错误")
                    reported = _usage_report(event.get("usage"))
                    if reported is not None:
                        if reported_usage is not None and not reported.follows(reported_usage):
                            raise ProviderError(
                                "provider_usage_conflict", "Provider 用量发生回退或丢失"
                            )
                        if reported != reported_usage:
                            reported_usage = reported
                            yield ModelUsage(usage=reported, final=False)
                    choices = event.get("choices")
                    if not isinstance(choices, list):
                        raise ProviderError("provider_response", "provider choices 不是列表")
                    if not choices:
                        continue
                    if len(choices) != 1 or not isinstance(choices[0], dict):
                        raise ProviderError(
                            "provider_response", "provider 必须返回单个 choice 对象"
                        )
                    choice = choices[0]
                    if type(choice.get("index", 0)) is not int or choice.get("index", 0) != 0:
                        raise ProviderError("provider_response", "provider choice 索引无效")
                    delta = choice.get("delta", {})
                    if not isinstance(delta, dict) or finish_reason is not None:
                        raise ProviderError(
                            "provider_response", "provider delta 无效或终止后仍有输出"
                        )
                    content = delta.get("content")
                    if content is not None and not isinstance(content, str):
                        raise ProviderError("provider_response", "provider 只支持字符串文本")
                    if delta.get("refusal"):
                        raise ProviderError("provider_content_filter", "provider 拒绝生成")
                    if content:
                        # 分块符合 Krim 单帧上限；SDK 和 HTTP 仍即时得到文本。
                        for position in range(0, len(content), 8192):
                            yield ModelText(text=content[position : position + 8192])
                    tool_deltas = delta.get("tool_calls")
                    if tool_deltas is None:
                        tool_deltas = []
                    if not isinstance(tool_deltas, list):
                        raise ProviderError("provider_tool_call", "provider tool_calls 不是列表")
                    for tool_delta in tool_deltas:
                        if (
                            not isinstance(tool_delta, dict)
                            or type(tool_delta.get("index")) is not int
                        ):
                            raise ProviderError("provider_tool_call", "provider 工具索引无效")
                        index = tool_delta["index"]
                        if not 0 <= index < self.config.max_tool_calls:
                            raise ProviderError(
                                "provider_tool_limit", "provider 工具调用超过数量限制"
                            )
                        calls.setdefault(index, _ToolBuffer()).append(tool_delta)
                    reason = choice.get("finish_reason")
                    if reason is not None:
                        if not isinstance(reason, str):
                            raise ProviderError(
                                "provider_response", "provider 终止原因必须是字符串"
                            )
                        if reason == "length":
                            raise ProviderError(
                                "provider_output_limit", "provider 输出达到模型上限"
                            )
                        if reason == "content_filter":
                            raise ProviderError("provider_content_filter", "provider 内容被过滤")
                        if reason not in {"stop", "tool_calls"}:
                            raise ProviderError("provider_response", "provider 终止原因不受支持")
                        finish_reason = reason
                if not saw_done or finish_reason is None:
                    raise ProviderError(
                        "provider_incomplete_stream", "provider SSE 缺少完整结束标记"
                    )
                if bool(calls) != (finish_reason == "tool_calls"):
                    raise ProviderError("provider_tool_call", "provider 工具调用与终止原因不一致")
                complete_calls = [calls[index].collect() for index in sorted(calls)]
                for call in complete_calls:
                    yield call
                if reported_usage is not None:
                    yield ModelUsage(usage=reported_usage, final=True)
        except httpx.TimeoutException as error:
            raise ProviderError("provider_timeout", "provider 请求超时") from error
        except httpx.HTTPError as error:
            raise ProviderError("provider_network", "provider 网络请求失败") from error
        except ValidationError as error:
            raise ProviderError("provider_response", "provider 输出不符合 Krim 类型约束") from error

    async def close(self) -> None:
        await self._http.aclose()
