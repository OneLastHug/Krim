from __future__ import annotations

import asyncio
import base64
import copy
import json
import os
import re
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import Any, TextIO, cast
from urllib.parse import urlsplit

from pydantic import AnyUrl, JsonValue, ValidationError

from krim_agent.contracts import ServiceError
from krim_agent.mcp.types import MCPServerConfig, MCPTool, _encode_json
from krim_agent.network.egress_policy import EgressPolicy

_MAX_QUEUED_CALLS = 64
_MAX_CURSOR_LENGTH = 4096
_MAX_URI_LENGTH = 2048
_WORKER_COUNT = 8
_STOP = object()


def _policy_for_http_config(config: MCPServerConfig) -> EgressPolicy:
    parsed = urlsplit(config.url or "")
    host = parsed.hostname
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    if host is None:
        raise ValueError("MCP HTTP URL is missing a host")
    if parsed.scheme == "http":
        return EgressPolicy.authorize_loopback(config.allowed_hosts, {port})
    return EgressPolicy(config.allowed_hosts, {port})


_STDIO_ENV_ALLOWLIST = frozenset(
    {
        "APPDATA",
        "COMSPEC",
        "HOME",
        "HOMEDRIVE",
        "HOMEPATH",
        "LANG",
        "LC_ALL",
        "LOCALAPPDATA",
        "PATH",
        "PATHEXT",
        "SYSTEMROOT",
        "TEMP",
        "TMP",
        "TZ",
        "USERPROFILE",
        "WINDIR",
    }
)


def _stdio_environment(configured: dict[str, str] | None) -> dict[str, str]:
    """Retain runtime variables, clear inherited secrets, then add host values."""
    environment = {name: "" for name in os.environ}
    environment.update(
        (name, value)
        for name, value in os.environ.items()
        if name.upper() in _STDIO_ENV_ALLOWLIST
    )
    environment.update(configured or {})
    return environment


def _stderr_sink() -> TextIO:
    """只打开操作系统空设备，不接收任意路径或写入凭据文件。"""
    return open(os.devnull, "w", encoding="utf-8")


def _load_sdk() -> Any:
    """Load the optional official MCP SDK only when a connection is requested."""
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
    from mcp.client.streamable_http import streamable_http_client

    return ClientSession, StdioServerParameters, stdio_client, streamable_http_client


def _field(value: object, *names: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        for name in names:
            if name in value:
                return value[name]
        return default
    for name in names:
        if hasattr(value, name):
            return getattr(value, name)
    return default


def _clone_json(value: object) -> JsonValue:
    try:
        if hasattr(value, "model_dump"):
            value = value.model_dump(mode="json", by_alias=True, exclude_none=True)
        return cast(JsonValue, json.loads(_encode_json(value)))
    except (TypeError, ValueError, OverflowError, RecursionError):
        raise ServiceError("mcp_invalid_json", 502) from None


def _normalize_tool_result(result: object) -> JsonValue:
    if bool(_field(result, "isError", "is_error", default=False)):
        raise ServiceError("mcp_tool_failed", 502)

    structured = _field(result, "structuredContent", "structured_content", default=None)
    if structured is not None:
        return _clone_json(structured)
    content = _field(result, "content", default=()) or ()
    text_blocks: list[str] = []
    json_blocks: list[JsonValue] = []
    all_text = True
    for block in content:
        text = _field(block, "text", default=None)
        block_type = _field(block, "type", default=None)
        if block_type == "text" and isinstance(text, str):
            text_blocks.append(text)
            json_blocks.append(text)
        else:
            all_text = False
            json_blocks.append(_clone_json(block))
    return "".join(text_blocks) if all_text else json_blocks


def _normalize_resource_page(
    page: object, items_key: str, required_key: str
) -> dict[str, JsonValue]:
    """Clone one server listing page as JSON; keep nextCursor for caller-driven paging."""
    items: list[JsonValue] = []
    for item in _field(page, items_key, default=()) or ():
        cloned = _clone_json(item)
        if (
            not isinstance(cloned, dict)
            or not isinstance(cloned.get("name"), str)
            or not isinstance(cloned.get(required_key), str)
        ):
            raise ServiceError("mcp_tool_failed", 502)
        items.append(cloned)
    result: dict[str, JsonValue] = {items_key: items}
    next_cursor = _field(page, "nextCursor", "next_cursor", default=None)
    if next_cursor is not None:
        if not isinstance(next_cursor, str) or not next_cursor:
            raise ServiceError("mcp_cursor_invalid", 502)
        result["nextCursor"] = next_cursor
    return result


def _normalize_read_resource(result: object) -> dict[str, JsonValue]:
    """Keep text/blob as distinct kinds; a base64 blob is data, never claimed media."""
    contents: list[JsonValue] = []
    for block in _field(result, "contents", default=()) or ():
        raw_uri = _field(block, "uri", default=None)
        uri = raw_uri if isinstance(raw_uri, str) else str(raw_uri) if raw_uri is not None else ""
        mime = _field(block, "mimeType", "mime_type", default=None)
        text = _field(block, "text", default=None)
        blob = _field(block, "blob", default=None)
        text_entry = isinstance(text, str) and blob is None
        blob_entry = isinstance(blob, str) and text is None
        if not uri or not (text_entry or blob_entry):
            raise ServiceError("mcp_tool_failed", 502)
        entry: dict[str, JsonValue] = {"uri": uri}
        if isinstance(mime, str):
            entry["mimeType"] = mime
        if text_entry:
            entry["kind"] = "text"
            entry["text"] = text
        else:
            entry["kind"] = "blob"
            entry["blob"] = blob
        contents.append(entry)
    return {"contents": contents}


def _schema(value: object) -> dict[str, JsonValue]:
    cloned = _clone_json(value)
    if not isinstance(cloned, dict):
        raise ServiceError("mcp_discovery_failed", 502)
    return cloned


@dataclass(slots=True)
class _CallRequest:
    kind: str  # "tool" | "list_resources" | "list_resource_templates" | "read_resource"
    name: str = ""
    arguments: dict[str, JsonValue] = field(default_factory=dict)
    cursor: str | None = None
    uri: str = ""
    future: asyncio.Future[JsonValue] | None = None
    operation: asyncio.Task[JsonValue] | None = None


class MCPConnection:
    """Own one MCP SDK connection, its fixed tool set, and its shutdown task."""

    def __init__(
        self, config: MCPServerConfig, *, egress_policy: EgressPolicy | None = None
    ) -> None:
        if egress_policy is not None and config.transport != "streamable_http":
            raise ValueError("HTTP出站策略不能约束stdio子进程")
        self._config = config.model_copy(deep=True)
        self._egress_policy = (
            egress_policy
            if self._config.transport == "streamable_http" and egress_policy is not None
            else _policy_for_http_config(self._config)
            if self._config.transport == "streamable_http"
            else None
        )
        self._ready = asyncio.Event()
        self._shutdown = asyncio.Event()
        self._queue: asyncio.Queue[_CallRequest | object] = asyncio.Queue(maxsize=_MAX_QUEUED_CALLS)
        self._lifecycle_lock = asyncio.Lock()
        self._owner_task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._owner_error: ServiceError | None = None
        self._closing = False
        self._closed = False
        self._connected = False
        self._active_callers: set[asyncio.Task[Any]] = set()
        self._tool_records: dict[str, MCPTool] = {}
        self._discovered_records: dict[str, MCPTool] = {}
        self._resources_capable = False
        self._tools_capable = False
        self._pending: dict[asyncio.Future[JsonValue], _CallRequest] = {}

    async def connect(self) -> MCPConnection:
        async with self._lifecycle_lock:
            if self._closing or self._closed:
                raise ServiceError("mcp_connection_closed", 503)
            if self._owner_task is None:
                self._owner_task = asyncio.create_task(
                    self._run_owner(), name=f"mcp-owner-{self._config.name}"
                )
            owner = self._owner_task

        await self._ready.wait()
        if self._owner_error is not None:
            await asyncio.gather(owner, return_exceptions=True)
            raise self._owner_error from None
        if self._closing or self._closed or not self._connected:
            raise ServiceError("mcp_connection_closed", 503)
        return self

    @property
    def config(self) -> MCPServerConfig:
        return self._config.model_copy(deep=True)

    @property
    def tools(self) -> tuple[MCPTool, ...]:
        """Return fresh schema copies so callers cannot rewrite the connection snapshot."""
        return tuple(copy.deepcopy(tool) for tool in self._tool_records.values())

    @property
    def discovered_tools(self) -> tuple[MCPTool, ...]:
        """宿主可读完整发现目录；不会因此向模型授予调用权限。"""
        return tuple(copy.deepcopy(tool) for tool in self._discovered_records.values())

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def supports_resources(self) -> bool:
        """服务器是否在initialize时协商了resources能力；未协商的来源应被隐藏。"""
        return self._resources_capable

    async def call_tool(self, name: str, args: dict[str, JsonValue]) -> JsonValue:
        if not isinstance(name, str):
            raise ServiceError("mcp_tool_not_selected", 404)
        try:
            cloned_args = _clone_json(args)
        except ServiceError:
            raise ServiceError("mcp_invalid_json", 422) from None
        if not isinstance(cloned_args, dict) or self._contains_private(cloned_args):
            raise ServiceError("mcp_invalid_json", 422)
        return await self._enqueue(_CallRequest("tool", name=name, arguments=cloned_args))

    async def list_resources(self, cursor: str | None = None) -> dict[str, JsonValue]:
        """返回一页服务器资源；结果保留 nextCursor，翻页由调用方逐页推进。"""
        request = _CallRequest("list_resources", cursor=self._validate_cursor(cursor))
        return cast(
            "dict[str, JsonValue]", await self._enqueue(request, requires_resources=True)
        )

    async def list_resource_templates(self, cursor: str | None = None) -> dict[str, JsonValue]:
        """返回一页资源模板；同 list_resources，保留 nextCursor。"""
        request = _CallRequest("list_resource_templates", cursor=self._validate_cursor(cursor))
        return cast(
            "dict[str, JsonValue]", await self._enqueue(request, requires_resources=True)
        )

    async def read_resource(self, uri: str) -> dict[str, JsonValue]:
        """读取单个资源；text 与 base64 blob 分开标注，保留 URI 与 MIME。"""
        request = _CallRequest("read_resource", uri=self._validate_uri(uri))
        return cast(
            "dict[str, JsonValue]", await self._enqueue(request, requires_resources=True)
        )

    def _validate_cursor(self, cursor: str | None) -> str | None:
        if cursor is None:
            return None
        if (
            not isinstance(cursor, str)
            or not cursor
            or len(cursor) > _MAX_CURSOR_LENGTH
            or any(ord(char) < 0x20 or ord(char) == 0x7F for char in cursor)
        ):
            raise ServiceError("mcp_invalid_json", 422)
        return cursor

    def _validate_uri(self, uri: str) -> str:
        if (
            not isinstance(uri, str)
            or not uri
            or len(uri) > _MAX_URI_LENGTH
            or any(ord(char) < 0x20 or ord(char) == 0x7F for char in uri)
        ):
            raise ServiceError("mcp_invalid_json", 422)
        try:
            AnyUrl(uri)
        except ValidationError:
            raise ServiceError("mcp_invalid_json", 422) from None
        return uri

    async def _enqueue(
        self, request: _CallRequest, *, requires_resources: bool = False
    ) -> JsonValue:
        async with self._lifecycle_lock:
            if self._closing or self._closed or not self._connected:
                raise ServiceError("mcp_connection_closed", 503)
            if requires_resources and not self._resources_capable:
                raise ServiceError("mcp_unavailable", 501)
            if request.kind == "tool" and request.name not in self._tool_records:
                raise ServiceError("mcp_tool_not_selected", 404)
            current = asyncio.current_task()
            if current is not None:
                self._active_callers.add(current)
            future: asyncio.Future[JsonValue] = asyncio.get_running_loop().create_future()
            request.future = future
            self._pending[future] = request
            try:
                self._queue.put_nowait(request)
            except asyncio.QueueFull:
                self._pending.pop(future, None)
                if current is not None:
                    self._active_callers.discard(current)
                raise ServiceError("mcp_queue_full", 503) from None

        try:
            return await future
        except asyncio.CancelledError:
            future.cancel()
            operation = request.operation
            if operation is not None and not operation.done():
                operation.cancel()
                await asyncio.gather(operation, return_exceptions=True)
            raise
        finally:
            self._pending.pop(future, None)
            current = asyncio.current_task()
            if current is not None:
                self._active_callers.discard(current)

    async def close(self) -> None:
        async with self._lifecycle_lock:
            if self._close_task is None:
                self._closing = True
                self._close_task = asyncio.create_task(
                    self._close_impl(), name=f"mcp-close-{self._config.name}"
                )
            close_task = self._close_task
        await asyncio.shield(close_task)

    async def _close_impl(self) -> None:
        callers = [task for task in self._active_callers if task is not asyncio.current_task()]
        for task in callers:
            task.cancel()
        if callers:
            await asyncio.gather(*callers, return_exceptions=True)

        self._shutdown.set()
        owner = self._owner_task
        if owner is not None and owner is not asyncio.current_task():
            await asyncio.gather(owner, return_exceptions=True)
        self._closed = True
        self._connected = False

    async def _run_owner(self) -> None:
        try:
            try:
                ClientSession, StdioServerParameters, stdio_client, streamablehttp_client = (
                    _load_sdk()
                )
            except ImportError:
                self._owner_error = ServiceError("mcp_unavailable", 503)
                return

            async with AsyncExitStack() as stack:
                if self._config.transport == "stdio":
                    # Trusted-host argv boundary: the official MCP SDK starts
                    # command + args without a shell. These values must come
                    # from host configuration, never from model input.
                    params = StdioServerParameters(
                        command=self._config.command,
                        args=list(self._config.args),
                        env=_stdio_environment(copy.deepcopy(self._config.env)),
                    )
                    # 子进程原始stderr可能含凭据，不进入公共应用日志。
                    stderr_sink = stack.enter_context(_stderr_sink())
                    streams = await stack.enter_async_context(
                        stdio_client(params, errlog=stderr_sink)
                    )
                else:
                    secret_headers = {
                        key: value.get_secret_value() for key, value in self._config.headers.items()
                    }

                    from krim_agent.network.http_transport import make_http_client

                    assert self._egress_policy is not None
                    self._egress_policy.validate_target(self._config.url or "")
                    http_client = make_http_client(
                        self._egress_policy,
                        headers=secret_headers,
                        timeout=self._config.timeout_seconds,
                    )
                    client = await stack.enter_async_context(http_client)
                    streams = await stack.enter_async_context(
                        streamablehttp_client(
                            self._config.url,
                            http_client=client,
                            terminate_on_close=True,
                        )
                    )

                read_stream, write_stream = streams[0], streams[1]
                session = ClientSession(read_stream, write_stream)
                await stack.enter_async_context(session)
                try:
                    async with asyncio.timeout(self._config.timeout_seconds):
                        initialized = await session.initialize()
                        capabilities = _field(initialized, "capabilities", default=None)
                        self._resources_capable = (
                            _field(capabilities, "resources", default=None) is not None
                        )
                        self._tools_capable = (
                            _field(capabilities, "tools", default=None) is not None
                        )
                        discovered = await self._discover_tools(session)
                except TimeoutError:
                    self._owner_error = ServiceError("mcp_timeout", 504)
                    return
                except ServiceError as error:
                    self._owner_error = error
                    return
                except Exception:
                    self._owner_error = ServiceError("mcp_connect_failed", 502)
                    return

                self._tool_records = discovered
                self._connected = True
                self._ready.set()

                async with asyncio.TaskGroup() as group:
                    workers = [
                        group.create_task(self._worker(session), name=f"mcp-call-{index}")
                        for index in range(_WORKER_COUNT)
                    ]
                    await self._shutdown.wait()
                    for _ in workers:
                        await self._queue.put(_STOP)
                    await self._queue.join()
        except asyncio.CancelledError:
            raise
        except ImportError:
            self._owner_error = ServiceError("mcp_unavailable", 503)
        except Exception:
            self._owner_error = ServiceError("mcp_connect_failed", 502)
        finally:
            self._connected = False
            self._closed = True
            self._ready.set()
            for request in tuple(self._pending.values()):
                if request.future is not None and not request.future.done():
                    request.future.set_exception(ServiceError("mcp_connection_closed", 503))

    async def _discover_tools(self, session: Any) -> dict[str, MCPTool]:
        selected = set(self._config.selected_tools)
        if not self._tools_capable:
            # resource-only服务器：不发送其不支持的tools/list；
            # 声明选中工具即为配置错误，与unknown selection一致。
            if selected:
                raise ServiceError("mcp_tool_not_selected", 422)
            return {}

        discovered: dict[str, MCPTool] = {}
        used_cursors: set[str] = set()
        cursor: str | None = None

        while True:
            if cursor is not None:
                if cursor in used_cursors:
                    raise ServiceError("mcp_cursor_invalid", 502)
                used_cursors.add(cursor)
            try:
                page = await session.list_tools(cursor=cursor)
            except asyncio.CancelledError:
                raise
            except Exception:
                raise ServiceError("mcp_discovery_failed", 502) from None

            page_tools = _field(page, "tools", default=()) or ()
            for remote_tool in page_tools:
                name = _field(remote_tool, "name")
                # 官方工具名128字符约束与控制字符拒绝；受控失败，非数量配额。
                if (
                    not isinstance(name, str)
                    or not name
                    or len(name) > 128
                    or any(ord(char) < 0x20 or ord(char) == 0x7F for char in name)
                    or name in discovered
                ):
                    raise ServiceError("mcp_discovery_failed", 502)
                input_schema = _schema(
                    _field(remote_tool, "inputSchema", "input_schema", default={})
                )
                output_schema_raw = _field(
                    remote_tool, "outputSchema", "output_schema", default=None
                )
                output_schema = (
                    _clone_json(output_schema_raw) if output_schema_raw is not None else None
                )
                description = _field(remote_tool, "description", default="")
                if not isinstance(description, str):
                    description = ""
                if any(
                    self._contains_private(value)
                    for value in (name, description, input_schema, output_schema)
                ):
                    raise ServiceError("mcp_discovery_failed", 502)
                annotations = _field(remote_tool, "annotations", default=None)
                remote_read_only = _field(
                    annotations, "readOnlyHint", "read_only_hint", default=None
                )
                host_read_only = name in self._config.readonly_tools
                read_only = host_read_only and remote_read_only is not False
                discovered[name] = MCPTool(
                    name=name,
                    description=description,
                    input_schema=input_schema,
                    output_schema=output_schema,
                    read_only=read_only,
                )

            next_cursor = _field(page, "nextCursor", "next_cursor", default=None)
            if next_cursor is None:
                break
            if not isinstance(next_cursor, str) or not next_cursor or next_cursor in used_cursors:
                raise ServiceError("mcp_cursor_invalid", 502)
            cursor = next_cursor

        unknown_selected = selected - discovered.keys()
        unknown_readonly = self._config.readonly_tools - discovered.keys()
        if unknown_selected or unknown_readonly:
            raise ServiceError("mcp_tool_not_selected", 422)
        self._discovered_records = copy.deepcopy(discovered)
        return {name: copy.deepcopy(tool) for name, tool in discovered.items() if name in selected}

    async def _worker(self, session: Any) -> None:
        while True:
            entry = await self._queue.get()
            try:
                if entry is _STOP:
                    return
                request = entry
                assert isinstance(request, _CallRequest)
                if request.future is None or request.future.cancelled():
                    continue
                operation = asyncio.create_task(
                    self._execute_call(session, request),
                    name=f"mcp-call-{request.kind}-{request.name or request.uri}",
                )
                request.operation = operation
                try:
                    result = await operation
                except asyncio.CancelledError:
                    operation.cancel()
                    await asyncio.gather(operation, return_exceptions=True)
                    current = asyncio.current_task()
                    if current is not None and current.cancelling():
                        raise
                    continue
                except ServiceError as error:
                    if not request.future.done():
                        request.future.set_exception(error)
                except Exception:
                    if not request.future.done():
                        request.future.set_exception(ServiceError("mcp_tool_failed", 502))
                else:
                    if not request.future.done():
                        request.future.set_result(result)
                finally:
                    request.operation = None
            finally:
                self._queue.task_done()

    def _private_values(self) -> tuple[str, ...]:
        values = [item.get_secret_value() for item in self._config.headers.values()]
        values.extend(
            value
            for name, value in (self._config.env or {}).items()
            if name in self._config.secret_env_names
            or re.search(r"key|token|secret|password|credential|auth", name, re.I)
        )
        values.extend(value[7:] for value in tuple(values) if value.lower().startswith("bearer "))
        return tuple(sorted({value for value in values if value}, key=len, reverse=True))

    def _contains_private(self, value: object) -> bool:
        secrets = self._private_values()
        if not secrets:
            return False
        pending = [value]
        while pending:
            item = pending.pop()
            if isinstance(item, str) and any(
                item == secret or len(secret) >= 8 and secret in item for secret in secrets
            ):
                return True
            if isinstance(item, dict):
                pending.extend(item.keys())
                pending.extend(item.values())
            elif isinstance(item, list):
                pending.extend(item)
        return False

    def _redact_private(self, value: JsonValue) -> JsonValue:
        if isinstance(value, str):
            for secret in self._private_values():
                if len(secret) >= 8:
                    value = value.replace(secret, "[redacted]")
                elif value == secret:
                    value = "[redacted]"
            return value
        if isinstance(value, dict):
            return {
                str(self._redact_private(key)): self._redact_private(item)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [self._redact_private(item) for item in value]
        return value

    def _redact_blobs(self, value: JsonValue) -> JsonValue:
        """对base64解码字节直接扫描凭据，命中则整体剔除该blob，再做普通字符串替换。"""
        if isinstance(value, dict):
            blob = value.get("blob")
            if isinstance(blob, str) and blob != "[redacted]" and self._blob_has_private(blob):
                value = {**value, "blob": "[redacted]"}
            return {str(key): self._redact_blobs(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._redact_blobs(item) for item in value]
        return value

    def _blob_has_private(self, blob: str) -> bool:
        """凭据按文本凭据的UTF-8字节扫描；无额外二进制秘密输入。短密钥仅整串相等。"""
        try:
            decoded = base64.b64decode(blob)
        except ValueError:
            return False
        if not decoded:
            return False
        for secret in self._private_values():
            encoded = secret.encode("utf-8")
            if len(encoded) >= 8 and encoded in decoded:
                return True
            if decoded == encoded:
                return True
        return False

    async def _execute_call(self, session: Any, request: _CallRequest) -> JsonValue:
        try:
            async with asyncio.timeout(self._config.timeout_seconds):
                payload = await self._remote_call(session, request)
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            raise ServiceError("mcp_timeout", 504) from None
        except ServiceError:
            raise
        except Exception:
            raise ServiceError("mcp_tool_failed", 502) from None
        return self._redact_private(self._redact_blobs(payload))

    async def _remote_call(self, session: Any, request: _CallRequest) -> JsonValue:
        if request.kind == "tool":
            result = await session.call_tool(request.name, arguments=request.arguments)
            return _normalize_tool_result(result)
        if request.kind == "list_resources":
            page = await session.list_resources(cursor=request.cursor)
            return _normalize_resource_page(page, "resources", "uri")
        if request.kind == "list_resource_templates":
            page = await session.list_resource_templates(cursor=request.cursor)
            return _normalize_resource_page(page, "resourceTemplates", "uriTemplate")
        result = await session.read_resource(request.uri)
        return _normalize_read_resource(result)
