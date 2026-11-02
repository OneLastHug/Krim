"""受限 Tavily Web 搜索/提取适配器。"""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import math
import re
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import cast
from urllib.parse import urlsplit, urlunsplit

import httpx
from pydantic import Field, JsonValue, SecretStr, field_validator

from krim_agent.contracts import ServiceError, StrictModel

_DEFAULT_ENDPOINT = "https://api.tavily.com"
_VERSION = "krim-tavily/1"
_CONCURRENCY = 4
_RESPONSE_BYTES = 2 * 1024 * 1024
_TOOL_JSON_BYTES = 60 * 1024
_MAX_RESULTS = 10
_MAX_CHARS = 15_000
_SEARCH_CONTENT_CHARS = 3_000
_OPERATION_SECONDS = 20.0
_RETRY_ATTEMPTS = 2
_CONNECT_TIMEOUT = 2.0
_READ_TIMEOUT = 8.0
_WRITE_TIMEOUT = 2.0
_POOL_TIMEOUT = 2.0
_TRUNCATION_MARKER = "[truncated]"
_KEY_PATTERN = re.compile(r"^[A-Za-z0-9._~+/-]+=*$")
_HOST_LABEL = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
_PERCENT_ESCAPE = re.compile(r"%(?![0-9a-fA-F]{2})")


class WebAccessConfig(StrictModel):
    """只保存服务端凭据；冻结配置的 repr 不展示密钥或endpoint。"""

    api_key: SecretStr | None = Field(default=None, repr=False)
    endpoint: str = Field(default=_DEFAULT_ENDPOINT, min_length=1, max_length=2048, repr=False)

    @field_validator("api_key")
    @classmethod
    def validate_key(cls, value: SecretStr | None) -> SecretStr | None:
        if value is None:
            return None
        raw = value.get_secret_value()
        if not raw or len(raw) > 4096 or _KEY_PATTERN.fullmatch(raw) is None:
            raise ValueError("无效 Web API 密钥")
        return value

    @field_validator("endpoint")
    @classmethod
    def validate_endpoint(cls, value: str) -> str:
        if (
            not value.isascii()
            or any(char.isspace() or ord(char) < 33 for char in value)
            or any(char in value for char in "\\%?#")
        ):
            raise ValueError("无效 Web 服务地址")
        try:
            parts = urlsplit(value)
            port = parts.port
        except ValueError:
            raise ValueError("无效 Web 服务地址") from None
        host = parts.hostname or ""
        if (
            parts.scheme not in {"http", "https"}
            or not host
            or parts.username is not None
            or parts.password is not None
            or "@" in parts.netloc
            or parts.query
            or parts.fragment
            or ("?" in value or "#" in value)
            or (port is not None and not 1 <= port <= 65535)
        ):
            raise ValueError("无效 Web 服务地址")

        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            normalized_host = host.lower()
            labels = normalized_host.rstrip(".").split(".")
            if (
                not normalized_host.isascii()
                or len(normalized_host) > 253
                or (len(labels) < 2 and normalized_host != "localhost")
                or any(not _HOST_LABEL.fullmatch(label) for label in labels)
            ):
                raise ValueError("无效 Web 服务主机") from None
            local_host = normalized_host.rstrip(".") == "localhost" or normalized_host.endswith(
                ".localhost"
            )
            authority_host = normalized_host
        else:
            normalized_host = address.compressed.lower()
            local_host = address.is_loopback
            authority_host = f"[{normalized_host}]" if address.version == 6 else normalized_host

        if parts.scheme == "http" and not local_host:
            raise ValueError("明文 Web 服务地址仅允许本机")
        path = parts.path.rstrip("/")
        if (
            len(path) > 512
            or re.fullmatch(r"[A-Za-z0-9/_-]*", path) is None
            or "//" in path
            or any(segment in {".", ".."} for segment in path.split("/"))
        ):
            raise ValueError("无效 Web 服务路径")
        authority = authority_host + (f":{port}" if port is not None else "")
        return urlunsplit((parts.scheme, authority, path, "", ""))


class TavilyWebAccess:
    """HTTPX 异步客户端；最多并发4次，仅对带Retry-After的429重试一次。"""

    def __init__(self, config: WebAccessConfig, *, client: httpx.AsyncClient | None = None):
        self._config = config.model_copy(deep=True)
        self._owns_client = client is None
        self._client = (
            client
            if client is not None
            else httpx.AsyncClient(
                trust_env=False,
                follow_redirects=False,
                timeout=self._timeout(),
                limits=httpx.Limits(
                    max_connections=_CONCURRENCY,
                    max_keepalive_connections=_CONCURRENCY,
                ),
            )
        )
        self._semaphore = asyncio.Semaphore(_CONCURRENCY)
        self._close_lock = asyncio.Lock()
        self._closed = False

    @property
    def configuration_digest(self) -> str:
        public_configuration = {
            "endpoint": self._config.endpoint,
            "version": _VERSION,
            "limits": {
                "concurrency": _CONCURRENCY,
                "response_bytes": _RESPONSE_BYTES,
                "tool_json_bytes": _TOOL_JSON_BYTES,
                "max_results": _MAX_RESULTS,
                "max_chars": _MAX_CHARS,
                "search_content_chars": _SEARCH_CONTENT_CHARS,
                "operation_seconds": _OPERATION_SECONDS,
                "retry_attempts": _RETRY_ATTEMPTS,
                "timeouts": [_CONNECT_TIMEOUT, _READ_TIMEOUT, _WRITE_TIMEOUT, _POOL_TIMEOUT],
            },
        }
        encoded = json.dumps(
            public_configuration,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    async def search(self, query: str, max_results: int = 5) -> dict[str, JsonValue]:
        secret = self._config.api_key.get_secret_value() if self._config.api_key else ""
        if not isinstance(query, str) or not query.strip() or (secret and secret in query):
            raise ServiceError("web_invalid_input", 422)
        try:
            query_size = len(query.encode("utf-8"))
        except UnicodeEncodeError:
            raise ServiceError("web_invalid_input", 422) from None
        if query_size > 4096 or any(ord(char) < 32 and char not in "\t\n\r" for char in query):
            raise ServiceError("web_invalid_input", 422)
        if type(max_results) is not int or not 1 <= max_results <= _MAX_RESULTS:
            raise ServiceError("web_invalid_input", 422)

        payload: dict[str, JsonValue] = {
            "query": query,
            "search_depth": "basic",
            "max_results": max_results,
            "include_answer": False,
            "include_raw_content": False,
            "include_usage": True,
        }
        response_data = await self._post("search", payload)
        raw_results = response_data.get("results")
        if not isinstance(raw_results, list):
            raise ServiceError("web_protocol", 502)
        results: list[dict[str, JsonValue]] = []
        truncated = len(raw_results) > max_results
        for item in raw_results[:max_results]:
            if not isinstance(item, dict):
                raise ServiceError("web_protocol", 502)
            title = item.get("title", "")
            url = item.get("url", "")
            content = item.get("content", "")
            if (
                not isinstance(title, str)
                or not isinstance(url, str)
                or not isinstance(content, str)
            ):
                raise ServiceError("web_protocol", 502)
            title = self._redact(title)
            url = self._redact(url)
            content, cut = self._clip(self._redact(content), _SEARCH_CONTENT_CHARS)
            truncated = truncated or cut
            results.append({"title": title, "url": url, "content": content})
        return self._fit_output("search", results, response_data, truncated)

    async def fetch(self, url: str, max_chars: int = _MAX_CHARS) -> dict[str, JsonValue]:
        if type(max_chars) is not int or not 1 <= max_chars <= _MAX_CHARS:
            raise ServiceError("web_invalid_input", 422)
        secret = self._config.api_key.get_secret_value() if self._config.api_key else ""
        if isinstance(url, str) and secret and secret in url:
            raise ServiceError("web_invalid_input", 422)
        safe_url = self._public_url(url)
        payload: dict[str, JsonValue] = {"urls": [safe_url], "format": "markdown"}
        response_data = await self._post("extract", payload)
        raw_results = response_data.get("results")
        failed_results = response_data.get("failed_results", [])
        if not isinstance(raw_results, list) or not isinstance(failed_results, list):
            raise ServiceError("web_protocol", 502)
        if failed_results or len(raw_results) != 1 or not isinstance(raw_results[0], dict):
            raise ServiceError("web_extract_failed", 502)

        item = raw_results[0]
        result_url = item.get("url")
        content = item.get("content", item.get("raw_content"))
        title = item.get("title", safe_url)
        if (
            not isinstance(result_url, str)
            or not isinstance(title, str)
            or not isinstance(content, str)
            or not self._same_public_url(result_url, safe_url)
        ):
            raise ServiceError("web_protocol", 502)
        content, truncated = self._clip(self._redact(content), max_chars)
        results: list[dict[str, JsonValue]] = [
            {
                "title": self._redact(title),
                "url": self._redact(result_url),
                "content": content,
            }
        ]
        return self._fit_output("fetch", results, response_data, truncated)

    async def close(self) -> None:
        async with self._close_lock:
            if self._closed:
                return
            self._closed = True
            if self._owns_client:
                await self._client.aclose()

    async def _post(self, operation: str, payload: dict[str, JsonValue]) -> dict[str, JsonValue]:
        if self._closed:
            raise ServiceError("web_closed", 409)
        secret = self._config.api_key.get_secret_value() if self._config.api_key else ""
        if not secret:
            raise ServiceError("web_config", 422)

        headers = {"Authorization": f"Bearer {secret}", "Accept": "application/json"}
        endpoint = f"{self._config.endpoint}/{operation}"
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _OPERATION_SECONDS
        try:
            async with asyncio.timeout_at(deadline):
                async with self._semaphore:
                    for attempt in range(_RETRY_ATTEMPTS):
                        status, retry_after, body = await self._request_once(
                            endpoint, headers, payload
                        )
                        if status == 200:
                            return self._decode_response(body)
                        if status == 429 and attempt == 0 and retry_after is not None:
                            remaining = deadline - loop.time()
                            if retry_after < remaining:
                                await asyncio.sleep(retry_after)
                                continue
                        raise self._http_error(status)
        except TimeoutError:
            raise ServiceError("web_timeout", 504) from None
        except httpx.TimeoutException:
            raise ServiceError("web_timeout", 504) from None
        except httpx.HTTPError:
            raise ServiceError("web_network", 503) from None
        raise ServiceError("web_http_error", 502)

    async def _request_once(
        self,
        endpoint: str,
        headers: dict[str, str],
        payload: dict[str, JsonValue],
    ) -> tuple[int, float | None, bytes]:
        timeout = self._timeout()
        async with self._client.stream(
            "POST",
            endpoint,
            headers=headers,
            json=payload,
            timeout=timeout,
            follow_redirects=False,
        ) as response:
            status = response.status_code
            if status != 200:
                return status, self._retry_after(response.headers.get("Retry-After")), b""
            body = bytearray()
            async for chunk in response.aiter_bytes():
                if len(body) + len(chunk) > _RESPONSE_BYTES:
                    raise ServiceError("web_response_too_large", 502)
                body.extend(chunk)
            return status, None, bytes(body)

    def _decode_response(self, body: bytes) -> dict[str, JsonValue]:
        try:
            decoded = json.loads(
                body.decode("utf-8"),
                object_pairs_hook=self._unique_object,
                parse_constant=self._reject_constant,
            )
        except (UnicodeError, ValueError, RecursionError):
            raise ServiceError("web_protocol", 502) from None
        if not isinstance(decoded, dict) or decoded.get("error") or decoded.get("success") is False:
            raise ServiceError("web_protocol", 502)
        return decoded

    @staticmethod
    def _unique_object(pairs: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    @staticmethod
    def _reject_constant(_value: str) -> None:
        raise ValueError("non-finite JSON number")

    @staticmethod
    def _timeout() -> httpx.Timeout:
        return httpx.Timeout(
            connect=_CONNECT_TIMEOUT,
            read=_READ_TIMEOUT,
            write=_WRITE_TIMEOUT,
            pool=_POOL_TIMEOUT,
        )

    @staticmethod
    def _retry_after(value: str | None) -> float | None:
        if value is None:
            return None
        try:
            seconds = float(value)
        except ValueError:
            try:
                date = parsedate_to_datetime(value)
                if date.tzinfo is None:
                    date = date.replace(tzinfo=UTC)
                seconds = (date - datetime.now(UTC)).total_seconds()
            except (TypeError, ValueError, OverflowError):
                return None
        if not math.isfinite(seconds):
            return None
        return max(0.0, seconds)

    @staticmethod
    def _http_error(status: int) -> ServiceError:
        codes = {
            401: "web_unauthorized",
            402: "web_payment_required",
            429: "web_rate_limited",
            432: "web_usage_limit",
            433: "web_credits_exhausted",
        }
        if 300 <= status < 400:
            code = "web_redirect"
        else:
            code = codes.get(status, "web_http_error")
        return ServiceError(code, status)

    def _redact(self, value: str) -> str:
        secret = self._config.api_key.get_secret_value() if self._config.api_key else ""
        return value.replace(secret, "[redacted]") if secret else value

    @staticmethod
    def _clip(value: str, limit: int) -> tuple[str, bool]:
        if len(value) <= limit:
            return value, False
        if limit >= len(_TRUNCATION_MARKER):
            return value[: limit - len(_TRUNCATION_MARKER)] + _TRUNCATION_MARKER, True
        return value[:limit], True

    def _fit_output(
        self,
        operation: str,
        results: list[dict[str, JsonValue]],
        response_data: dict[str, JsonValue],
        truncated: bool,
    ) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {
            "ok": True,
            "operation": operation,
            "results": cast(JsonValue, results),
            "usage": response_data.get("usage"),
            "request_id": response_data.get("request_id"),
            "truncated": truncated,
        }
        self._redact_json(result)
        while True:
            try:
                encoded = json.dumps(
                    result,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8")
            except (TypeError, ValueError, RecursionError):
                raise ServiceError("web_protocol", 502) from None
            if len(encoded) <= _TOOL_JSON_BYTES:
                return result
            shortened = False
            for item in results:
                content = item.get("content")
                if not isinstance(content, str) or not content:
                    continue
                prefix_length = len(content) // 2
                if prefix_length >= len(_TRUNCATION_MARKER):
                    item["content"] = content[: prefix_length - len(_TRUNCATION_MARKER)] + (
                        _TRUNCATION_MARKER
                    )
                else:
                    item["content"] = ""
                shortened = True
            if not shortened:
                raise ServiceError("web_output_too_large", 502)
            result["truncated"] = True

    def _redact_json(self, value: dict[str, JsonValue] | list[JsonValue]) -> None:
        secret = self._config.api_key.get_secret_value() if self._config.api_key else ""
        if not secret:
            return
        pending: list[dict[str, JsonValue] | list[JsonValue]] = [value]
        while pending:
            current = pending.pop()
            if isinstance(current, dict):
                for key, child in current.items():
                    if isinstance(child, str):
                        current[key] = child.replace(secret, "[redacted]")
                    elif isinstance(child, (dict, list)):
                        pending.append(child)
            else:
                for index, child in enumerate(current):
                    if isinstance(child, str):
                        current[index] = child.replace(secret, "[redacted]")
                    elif isinstance(child, (dict, list)):
                        pending.append(child)

    @classmethod
    def _public_url(cls, value: str) -> str:
        if (
            not isinstance(value, str)
            or not value
            or len(value) > 4096
            or any(char.isspace() or ord(char) < 33 for char in value)
            or "\\" in value
            or _PERCENT_ESCAPE.search(value)
        ):
            raise ServiceError("web_invalid_input", 422)
        try:
            parts = urlsplit(value)
            port = parts.port
        except ValueError:
            raise ServiceError("web_invalid_input", 422) from None
        if (
            parts.scheme.lower() not in {"http", "https"}
            or not parts.hostname
            or parts.username is not None
            or parts.password is not None
            or "@" in parts.netloc
            or (port is not None and not 1 <= port <= 65535)
        ):
            raise ServiceError("web_invalid_input", 422)
        host = parts.hostname
        if "%" in host:
            raise ServiceError("web_invalid_input", 422)
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            try:
                ascii_host = host.encode("idna").decode("ascii").lower().rstrip(".")
            except UnicodeError:
                raise ServiceError("web_invalid_input", 422) from None
            labels = ascii_host.split(".")
            if (
                len(ascii_host) > 253
                or len(labels) < 2
                or any(not _HOST_LABEL.fullmatch(label) for label in labels)
                or ascii_host == "localhost"
                or ascii_host.endswith((".localhost", ".local", ".internal", ".test"))
                or all(re.fullmatch(r"(?:[0-9]+|0x[0-9a-f]+)", label) for label in labels)
            ):
                raise ServiceError("web_invalid_input", 422) from None
            normalized_host = ascii_host
            authority_host = normalized_host
        else:
            if not address.is_global:
                raise ServiceError("web_invalid_input", 422)
            normalized_host = address.compressed.lower()
            authority_host = f"[{normalized_host}]" if address.version == 6 else normalized_host
        normalized_port = (
            None
            if (parts.scheme.lower() == "http" and port == 80)
            or (parts.scheme.lower() == "https" and port == 443)
            else port
        )
        authority = authority_host + (f":{normalized_port}" if normalized_port is not None else "")
        return urlunsplit(
            (
                parts.scheme.lower(),
                authority,
                parts.path or "/",
                parts.query,
                "",
            )
        )

    @classmethod
    def _same_public_url(cls, left: str, right: str) -> bool:
        try:
            return cls._public_url(left) == cls._public_url(right)
        except ServiceError:
            return False
