"""MCP 工具桥：服务器隔离、原名保留、原 inputSchema 和现有权限链。"""

from __future__ import annotations

import asyncio
import re

import httpx
from mcp.types import CallToolResult, TextContent, Tool
from pydantic import BaseModel, JsonValue, TypeAdapter

from krim_agent.protocol.models import check_json
from krim_agent.tools import ToolContext, ToolRegistry
from krim_agent.tools.core import ToolDefinition, ToolError

from .client import (
    MCPConnection,
    MCPConnectionError,
    SessionFactory,
    _finish_cleanup,
    validate_server_url,
)
from .schema import arguments_model
from .types import MCPServerConfig, MCPServerStatus, MCPToolInfo


def _tool_result(result: CallToolResult) -> JsonValue:
    if result.structuredContent is not None:
        value: JsonValue = TypeAdapter(JsonValue).validate_python(result.structuredContent)
    elif all(isinstance(item, TextContent) for item in result.content):
        value = "\n".join(item.text for item in result.content if isinstance(item, TextContent))
    else:
        value = TypeAdapter(JsonValue).validate_python(
            [item.model_dump(mode="json", exclude_none=True) for item in result.content]
        )
    check_json(value)
    return value


class MCPBridge:
    def __init__(self, registry: ToolRegistry) -> None:
        self.registry = registry
        self._connections: dict[str, MCPConnection] = {}
        self._infos: dict[str, MCPToolInfo] = {}
        self._connection_tools: dict[str, dict[str, ToolDefinition]] = {}
        self._statuses: dict[str, MCPServerStatus] = {}
        self._retiring: set[str] = set()
        self._removals: dict[str, asyncio.Task[MCPServerStatus]] = {}
        self._close_task: asyncio.Task[None] | None = None
        self._closed = False

    async def add_server(
        self,
        config: MCPServerConfig,
        *,
        session_factory: SessionFactory | None = None,
        auth: httpx.Auth | None = None,
    ) -> MCPServerStatus:
        if self._closed:
            raise MCPConnectionError("MCP bridge 已关闭")
        config = MCPServerConfig.model_validate(config.model_dump()).model_copy(deep=True)
        namespace = config.namespace or config.name
        if (
            config.name in self._connections
            or config.name in self._retiring
            or config.name in self._removals
        ):
            raise MCPConnectionError("MCP server 名称已存在")
        if config.transport == "streamable_http":
            validate_server_url(config)
        if auth is not None and any(name.lower() == "authorization" for name in config.headers):
            raise MCPConnectionError("MCP OAuth Auth 不能与静态 Authorization header 同时使用")
        if auth is not None and config.transport != "streamable_http":
            raise MCPConnectionError("OAuth Auth 只支持 Streamable HTTP MCP")
        connection = MCPConnection(config, session_factory, auth)
        self._connections[config.name] = connection
        self._statuses[config.name] = MCPServerStatus(
            name=config.name, namespace=namespace, status="connecting"
        )
        if not config.enabled:
            status = MCPServerStatus(name=config.name, namespace=namespace, status="disabled")
            self._statuses[config.name] = status
            return status
        try:
            await connection.connect()
            tools: list[Tool] = []
            cursor: str | None = None
            seen_cursors: set[str] = set()
            for _page in range(32):
                result = await connection.list_tools(cursor)
                tools.extend(result.tools)
                if len(tools) > 256:
                    raise MCPConnectionError("MCP 工具数量超过限制")
                cursor = result.nextCursor
                if not cursor:
                    break
                if cursor in seen_cursors:
                    raise MCPConnectionError("MCP 工具分页游标重复")
                seen_cursors.add(cursor)
            else:
                raise MCPConnectionError("MCP 工具分页超过限制")
            prepared: list[tuple[ToolDefinition, MCPToolInfo]] = []
            names: set[str] = set()
            existing = {definition.name for definition in self.registry.definitions}
            for remote in tools:
                if re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]{0,63}", remote.name) is None:
                    raise MCPConnectionError("MCP 工具名不合法")
                local = f"{namespace}__{remote.name}".replace("-", "_").replace(".", "_").lower()
                if len(local) > 64 or local in names or local in existing:
                    raise MCPConnectionError("MCP 工具命名空间发生冲突")
                names.add(local)
                description = remote.description or f"MCP {config.name}.{remote.name}"
                readonly = remote.name in config.readonly_tools
                schema = TypeAdapter(dict[str, JsonValue]).validate_python(remote.inputSchema)
                definition = self._definition(
                    connection,
                    local,
                    remote.name,
                    description,
                    readonly,
                    schema,
                    TypeAdapter(dict[str, JsonValue]).validate_python(remote.outputSchema)
                    if remote.outputSchema is not None
                    else None,
                    remote.name in config.deferred_tools,
                    remote.name in config.always_load_tools,
                )
                prepared.append(
                    (
                        definition,
                        MCPToolInfo(
                            server=config.name,
                            remote_name=remote.name,
                            local_name=local,
                            description=description,
                            readonly=readonly,
                        ),
                    )
                )
            if (
                self._closed
                or self._connections.get(config.name) is not connection
                or config.name in self._retiring
            ):
                raise MCPConnectionError("MCP server 在发现期间已移除", "mcp_server_retiring")
            registered: dict[str, ToolDefinition] = {}
            try:
                for definition, info in prepared:
                    registered[definition.name] = definition
                    self.registry.register(definition)
                    self._infos[definition.name] = info
            except Exception:
                for name, definition in registered.items():
                    self.registry.unregister(name, expected=definition)
                    self._infos.pop(name, None)
                raise
            self._connection_tools[config.name] = registered
            status = MCPServerStatus(
                name=config.name,
                namespace=namespace,
                status="ready",
                tools=[info.local_name for _definition, info in prepared],
            )
        except asyncio.CancelledError:
            await connection.close()
            status = MCPServerStatus(
                name=config.name,
                namespace=namespace,
                status="closed",
                error="MCP 初始化已取消",
                error_code="cancelled",
            )
            caller = asyncio.current_task()
            if caller is not None and caller.cancelling():
                if (
                    self._connections.get(config.name) is connection
                    and config.name not in self._retiring
                ):
                    self._statuses[config.name] = status
                raise
        except Exception as error:
            await connection.close()
            if self._connections.get(config.name) is connection:
                self._withdraw(config.name)
            status = MCPServerStatus(
                name=config.name,
                namespace=namespace,
                status="failed",
                error="MCP 连接、工具发现或命名校验失败",
                error_code=error.code
                if isinstance(error, MCPConnectionError)
                else "mcp_connection_failed",
            )
        if self._connections.get(config.name) is not connection or config.name in self._retiring:
            return MCPServerStatus(name=config.name, namespace=namespace, status="closed")
        self._statuses[config.name] = status
        return status

    def _definition(
        self,
        connection: MCPConnection,
        local: str,
        remote: str,
        description: str,
        readonly: bool,
        schema: dict[str, JsonValue],
        output_schema: dict[str, JsonValue] | None = None,
        should_defer: bool = False,
        always_load: bool = False,
    ) -> ToolDefinition:
        parameters = arguments_model(f"MCP_{local}_Input", schema)

        async def execute(arguments: BaseModel, context: ToolContext) -> JsonValue:
            payload = TypeAdapter(dict[str, JsonValue]).validate_python(
                arguments.model_dump(by_alias=True)
            )
            try:
                result = await connection.call_tool(remote, payload)
            except MCPConnectionError as error:
                raise ToolError("mcp_request_failed", "MCP 工具连接不可用或请求失败") from error
            if result.isError:
                raise ToolError("mcp_tool_error", "MCP 服务报告工具执行失败")
            return _tool_result(result)

        return ToolDefinition(
            local,
            description,
            parameters,
            execute,
            "read" if readonly else "write",
            output_schema=output_schema,
            should_defer=should_defer,
            always_load=always_load,
        )

    def statuses(self) -> list[MCPServerStatus]:
        return [status.model_copy(deep=True) for status in self._statuses.values()]

    def tools(self) -> list[MCPToolInfo]:
        return [info.model_copy(deep=True) for info in self._infos.values()]

    def _withdraw(self, name: str) -> None:
        for local, definition in self._connection_tools.pop(name, {}).items():
            self.registry.unregister(local, expected=definition)
            self._infos.pop(local, None)

    async def remove_server(self, name: str) -> MCPServerStatus:
        """收束一个 MCP owner 并撤销其原定义；同名服务可随后明确重建。"""

        existing = self._removals.get(name)
        if existing is None:
            connection = self._connections.get(name)
            if connection is None:
                status = self._statuses.get(name)
                if status is None:
                    raise MCPConnectionError("MCP server 不存在", "mcp_server_not_found")
                return status.model_copy(deep=True)
            connection.revoke()
            self._retiring.add(name)
            self._withdraw(name)
            self._statuses[name] = self._statuses[name].model_copy(update={"status": "closing"})
            existing = asyncio.create_task(self._remove_server(name, connection))
            self._removals[name] = existing
        else:
            connection = self._connections.get(name)
            if connection is not None:
                connection.assert_close_allowed()
        try:
            return (await _finish_cleanup(existing)).model_copy(deep=True)
        finally:
            if existing.done() and self._removals.get(name) is existing:
                self._removals.pop(name, None)

    async def _remove_server(self, name: str, connection: MCPConnection) -> MCPServerStatus:
        try:
            await connection.close()
            self._connections.pop(name, None)
            status = self._statuses[name].model_copy(update={"status": "closed"})
            self._statuses[name] = status
            return status
        finally:
            self._retiring.discard(name)

    async def close(self) -> None:
        for connection in self._connections.values():
            connection.assert_close_allowed()
        if self._close_task is None:
            self._closed = True
            self._close_task = asyncio.create_task(self._close_impl())
        await _finish_cleanup(self._close_task)

    async def _close_impl(self) -> None:
        await asyncio.gather(
            *(self.remove_server(name) for name in list(self._connections)),
            return_exceptions=False,
        )
