"""官方 SDK 上下文只在 owner task 内打开/关闭；跨任务请求经类型化队列。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import timedelta
from typing import Literal, Protocol, cast
from urllib.parse import urlsplit

import httpx
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.client.streamable_http import streamable_http_client
from mcp.types import CallToolResult, ListToolsResult, PaginatedRequestParams
from pydantic import JsonValue

from .auth import MCPOAuthAuth
from .types import MCPServerConfig


class MCPConnectionError(RuntimeError):
    def __init__(self, message: str, code: str = "mcp_connection_failed") -> None:
        super().__init__(message)
        self.code = code


_owned_connection: ContextVar[object | None] = ContextVar("krim-mcp-owner", default=None)


async def _finish_cleanup[Result](task: asyncio.Task[Result]) -> Result:
    """重复取消只能取消等待方，原 owner 清理必须完成。"""

    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    result = task.result()
    if cancelled:
        raise asyncio.CancelledError
    return result


class MCPSession(Protocol):
    async def initialize(self) -> object: ...
    async def list_tools(
        self, *, params: PaginatedRequestParams | None = None
    ) -> ListToolsResult: ...
    async def call_tool(self, name: str, arguments: dict[str, JsonValue]) -> CallToolResult: ...


SessionFactory = Callable[[], Awaitable[MCPSession]]
Response = ListToolsResult | CallToolResult


@dataclass
class _Request:
    kind: Literal["list", "call"]
    future: asyncio.Future[Response]
    cursor: str | None = None
    name: str = ""
    arguments: dict[str, JsonValue] | None = None


def validate_server_url(config: MCPServerConfig) -> None:
    if config.transport != "streamable_http" or config.url is None:
        return
    parsed = urlsplit(config.url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise MCPConnectionError("MCP URL 必须是 http/https 且包含主机")
    if parsed.username or parsed.password or parsed.fragment:
        raise MCPConnectionError("MCP URL 不能包含用户凭证或 fragment")
    host = parsed.hostname.lower().rstrip(".")
    allowed = {value.lower().rstrip(".") for value in config.allowed_hosts}
    if host not in allowed:
        raise MCPConnectionError("MCP URL 主机不在 allowlist")
    if parsed.scheme == "http" and host not in {"127.0.0.1", "localhost", "::1"}:
        raise MCPConnectionError("非本机 MCP 连接必须使用 HTTPS")


class MCPConnection:
    def __init__(
        self,
        config: MCPServerConfig,
        session_factory: SessionFactory | None = None,
        auth: httpx.Auth | None = None,
        request_guard: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        config = MCPServerConfig.model_validate(config.model_dump())
        self.config, self.session_factory, self.auth = (
            config.model_copy(deep=True),
            session_factory,
            auth,
        )
        self.request_guard = request_guard
        if auth is not None and (
            config.transport != "streamable_http" or session_factory is not None
        ):
            raise MCPConnectionError("OAuth Auth 需要真实 Streamable HTTP transport")
        if auth is not None and any(name.lower() == "authorization" for name in config.headers):
            raise MCPConnectionError("OAuth Auth 与静态 Authorization header 冲突")
        if isinstance(auth, MCPOAuthAuth) and (
            auth.server != config.name or not auth.matches_endpoint(config.url or "")
        ):
            raise MCPConnectionError("OAuth Auth 与 MCP server/endpoint 不一致")
        self._owner: asyncio.Task[None] | None = None
        self._queue: asyncio.Queue[_Request | None] = asyncio.Queue(config.max_pending_requests)
        self._ready = asyncio.Event()
        self._failure: str | None = None
        self._failure_code = "mcp_connection_failed"
        self._closed = False
        self._requests: set[asyncio.Task[None]] = set()
        self._close_task: asyncio.Task[None] | None = None
        self.connected = False

    async def connect(self) -> MCPConnection:
        if self._closed or not self.config.enabled:
            raise MCPConnectionError("MCP server 已关闭或禁用")
        if self._owner is None:
            if self.config.transport == "streamable_http":
                validate_server_url(self.config)
            self._owner = asyncio.create_task(self._serve(), name=f"krim-mcp-{self.config.name}")
        try:
            await asyncio.wait_for(
                self._ready.wait(),
                self.config.initialization_timeout_seconds
                if self.auth
                else self.config.request_timeout_seconds,
            )
        except (asyncio.CancelledError, TimeoutError):
            cleanup = asyncio.create_task(self.close())
            await _finish_cleanup(cleanup)
            raise
        if self._failure is not None or not self.connected:
            raise MCPConnectionError(self._failure or "MCP 初始化失败", self._failure_code)
        return self

    async def _serve(self) -> None:
        ownership = _owned_connection.set(self)
        try:
            async with AsyncExitStack() as stack:
                session: MCPSession
                if self.session_factory is not None:
                    session = await self.session_factory()
                else:
                    if self.config.transport == "stdio":
                        read, write = await stack.enter_async_context(
                            stdio_client(
                                StdioServerParameters(
                                    command=self.config.command or "",
                                    args=self.config.args,
                                    env=self.config.env,
                                    cwd=self.config.cwd,
                                )
                            )
                        )
                    else:
                        client = await stack.enter_async_context(
                            httpx.AsyncClient(
                                headers={
                                    name: value.get_secret_value()
                                    for name, value in self.config.headers.items()
                                },
                                trust_env=False,
                                follow_redirects=False,
                                timeout=self.config.request_timeout_seconds,
                                auth=self.auth,
                            )
                        )
                        read, write, _session_id = await stack.enter_async_context(
                            streamable_http_client(
                                self.config.url or "",
                                http_client=client,
                            )
                        )
                    session = cast(
                        MCPSession,
                        await stack.enter_async_context(
                            ClientSession(
                                read,
                                write,
                                read_timeout_seconds=timedelta(
                                    seconds=(
                                        self.config.initialization_timeout_seconds
                                        if self.auth
                                        else self.config.request_timeout_seconds
                                    )
                                ),
                            )
                        ),
                    )
                async with asyncio.timeout(
                    self.config.initialization_timeout_seconds
                    if self.auth
                    else self.config.request_timeout_seconds
                ):
                    await session.initialize()
                self.connected = True
                self._ready.set()
                try:
                    while True:
                        request = await self._queue.get()
                        if request is None:
                            break
                        if request.future.cancelled():
                            continue
                        if self._closed:
                            request.future.set_exception(MCPConnectionError("MCP server 正在关闭"))
                            continue
                        task = asyncio.create_task(self._dispatch(session, request))
                        self._requests.add(task)
                        task.add_done_callback(self._requests.discard)

                        def cancel_owned(
                            future: asyncio.Future[Response], owned: asyncio.Task[None] = task
                        ) -> None:
                            if future.cancelled():
                                owned.cancel()

                        request.future.add_done_callback(cancel_owned)
                finally:
                    for task in list(self._requests):
                        task.cancel()
                    await asyncio.gather(*self._requests, return_exceptions=True)
        except Exception:
            self._failure = "MCP 连接或初始化失败"
            if isinstance(self.auth, MCPOAuthAuth) and self.auth.last_error_code is not None:
                self._failure_code = self.auth.last_error_code
        finally:
            self.connected = False
            self._ready.set()
            while not self._queue.empty():
                request = self._queue.get_nowait()
                if request is not None and not request.future.done():
                    request.future.set_exception(MCPConnectionError("MCP 连接已结束"))
            _owned_connection.reset(ownership)

    async def _dispatch(self, session: MCPSession, request: _Request) -> None:
        try:
            if self._closed:
                raise MCPConnectionError("MCP server 正在关闭")
            async with asyncio.timeout(self.config.request_timeout_seconds):
                result: Response
                if request.kind == "list":
                    result = await session.list_tools(
                        params=PaginatedRequestParams(cursor=request.cursor)
                        if request.cursor
                        else None
                    )
                else:
                    if self.request_guard is not None:
                        await self.request_guard()
                    if self._closed:
                        raise MCPConnectionError("MCP server 正在关闭")
                    result = await session.call_tool(request.name, request.arguments or {})
            if not request.future.done():
                request.future.set_result(result)
        except asyncio.CancelledError:
            if not request.future.done():
                request.future.cancel()
        except MCPConnectionError as error:
            if not request.future.done():
                request.future.set_exception(error)
        except Exception:
            if not request.future.done():
                request.future.set_exception(MCPConnectionError("MCP 请求失败或超时"))

    async def _request(self, request: _Request) -> Response:
        if self._closed or not self.connected or self._owner is None or self._owner.done():
            raise MCPConnectionError("MCP server 未连接")
        async with asyncio.timeout(self.config.request_timeout_seconds):
            await self._queue.put(request)
            return await request.future

    async def list_tools(self, cursor: str | None = None) -> ListToolsResult:
        future: asyncio.Future[Response] = asyncio.get_running_loop().create_future()
        response = await self._request(_Request("list", future, cursor=cursor))
        if not isinstance(response, ListToolsResult):
            raise MCPConnectionError("MCP tools/list 响应类型错误")
        return response

    async def call_tool(self, name: str, arguments: dict[str, JsonValue]) -> CallToolResult:
        future: asyncio.Future[Response] = asyncio.get_running_loop().create_future()
        response = await self._request(_Request("call", future, name=name, arguments=arguments))
        if not isinstance(response, CallToolResult):
            raise MCPConnectionError("MCP tools/call 响应类型错误")
        return response

    def assert_close_allowed(self) -> None:
        if _owned_connection.get() is self:
            raise MCPConnectionError("不能从 MCP owner/请求回调等待自身关闭", "mcp_self_close")

    def revoke(self) -> None:
        self.assert_close_allowed()
        self._closed = True

    async def close(self) -> None:
        self.revoke()
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close_impl())
        await _finish_cleanup(self._close_task)

    async def _close_impl(self) -> None:
        if self._owner is not None and not self._owner.done():
            if not self.connected:
                self._owner.cancel()
            else:
                await self._queue.put(None)
            await asyncio.gather(self._owner, return_exceptions=True)
        self.connected = False
