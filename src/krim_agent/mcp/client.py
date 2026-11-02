from __future__ import annotations

import asyncio
import copy
import json
import os
import re
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Any, TextIO, cast

from pydantic import JsonValue

from krim_agent.contracts import ServiceError
from krim_agent.mcp.types import MCPServerConfig, MCPTool, _encode_json

_MAX_DISCOVERED_TOOLS = 256
_MAX_DISCOVERY_PAGES = 32
_MAX_RESULT_BYTES = 64 * 1024
_MAX_QUEUED_CALLS = 64
_WORKER_COUNT = 8
_STOP = object()


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


def _serialized_size(value: JsonValue) -> int:
    try:
        return len(_encode_json(value).encode("utf-8"))
    except (TypeError, ValueError, OverflowError, RecursionError):
        raise ServiceError("mcp_invalid_json", 502) from None


def _normalize_tool_result(result: object) -> JsonValue:
    if bool(_field(result, "isError", "is_error", default=False)):
        raise ServiceError("mcp_tool_failed", 502)

    structured = _field(result, "structuredContent", "structured_content", default=None)
    if structured is not None:
        payload = _clone_json(structured)
    else:
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
        payload = "".join(text_blocks) if all_text else json_blocks

    if _serialized_size(payload) > _MAX_RESULT_BYTES:
        raise ServiceError("mcp_output_too_large", 502)
    return payload


def _schema(value: object) -> dict[str, JsonValue]:
    cloned = _clone_json(value)
    if not isinstance(cloned, dict):
        raise ServiceError("mcp_discovery_failed", 502)
    return cloned


@dataclass(slots=True)
class _CallRequest:
    name: str
    arguments: dict[str, JsonValue]
    future: asyncio.Future[JsonValue]
    operation: asyncio.Task[JsonValue] | None = None


class MCPConnection:
    """Own one MCP SDK connection, its fixed tool set, and its shutdown task."""

    def __init__(self, config: MCPServerConfig) -> None:
        self._config = config.model_copy(deep=True)
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

    async def call_tool(self, name: str, args: dict[str, JsonValue]) -> JsonValue:
        if not isinstance(name, str):
            raise ServiceError("mcp_tool_not_selected", 404)
        try:
            cloned_args = _clone_json(args)
        except ServiceError:
            raise ServiceError("mcp_invalid_json", 422) from None
        if not isinstance(cloned_args, dict) or self._contains_private(cloned_args):
            raise ServiceError("mcp_invalid_json", 422)

        async with self._lifecycle_lock:
            if self._closing or self._closed or not self._connected:
                raise ServiceError("mcp_connection_closed", 503)
            if name not in self._tool_records:
                raise ServiceError("mcp_tool_not_selected", 404)
            current = asyncio.current_task()
            if current is not None:
                self._active_callers.add(current)
            future: asyncio.Future[JsonValue] = asyncio.get_running_loop().create_future()
            request = _CallRequest(name, cloned_args, future)
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
                    params = StdioServerParameters(
                        command=self._config.command,
                        args=list(self._config.args),
                        env=copy.deepcopy(self._config.env),
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

                    import httpx

                    client = await stack.enter_async_context(
                        httpx.AsyncClient(
                            headers=secret_headers,
                            timeout=self._config.timeout_seconds,
                            trust_env=False,
                            follow_redirects=False,
                        )
                    )
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
                        await session.initialize()
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
                if not request.future.done():
                    request.future.set_exception(ServiceError("mcp_connection_closed", 503))

    async def _discover_tools(self, session: Any) -> dict[str, MCPTool]:
        discovered: dict[str, MCPTool] = {}
        selected = set(self._config.selected_tools)
        used_cursors: set[str] = set()
        cursor: str | None = None
        page_count = 0

        while True:
            if page_count >= _MAX_DISCOVERY_PAGES:
                raise ServiceError("mcp_discovery_limit", 502)
            if cursor is not None:
                if cursor in used_cursors:
                    raise ServiceError("mcp_cursor_invalid", 502)
                used_cursors.add(cursor)
            page_count += 1
            try:
                page = await session.list_tools(cursor=cursor)
            except asyncio.CancelledError:
                raise
            except Exception:
                raise ServiceError("mcp_discovery_failed", 502) from None

            page_tools = _field(page, "tools", default=()) or ()
            for remote_tool in page_tools:
                if len(discovered) >= _MAX_DISCOVERED_TOOLS:
                    raise ServiceError("mcp_discovery_limit", 502)
                name = _field(remote_tool, "name")
                if not isinstance(name, str) or not name or name in discovered:
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
                if request.future.cancelled():
                    continue
                operation = asyncio.create_task(
                    self._execute_call(session, request.name, request.arguments),
                    name=f"mcp-tool-{request.name}",
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

    async def _execute_call(
        self, session: Any, name: str, arguments: dict[str, JsonValue]
    ) -> JsonValue:
        try:
            async with asyncio.timeout(self._config.timeout_seconds):
                result = await session.call_tool(name, arguments=arguments)
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            raise ServiceError("mcp_timeout", 504) from None
        except Exception:
            raise ServiceError("mcp_tool_failed", 502) from None
        return self._redact_private(_normalize_tool_result(result))
