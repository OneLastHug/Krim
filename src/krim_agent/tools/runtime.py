"""可信宿主的外部工具装配与资源所有权。"""

from __future__ import annotations

import asyncio
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast

from pydantic import SecretStr

from krim_agent.contracts import ServiceError, parse_json
from krim_agent.mcp import MCPConnection, MCPServerConfig
from krim_agent.runtime.contracts import settled
from krim_agent.web_access import TavilyWebAccess, WebAccessConfig

from .bindings import resolve_tool_overrides
from .builtins import demo_policy
from .core import Decision, Invocation, Policy
from .mcp import mcp_tools
from .registry import ToolRegistry
from .web import web_tools


@dataclass(frozen=True)
class ToolRuntimeConfig:
    web: WebAccessConfig | None = None
    mcp_servers: tuple[MCPServerConfig, ...] = ()
    tool_overrides: Mapping[str, str] = field(default_factory=dict)
    public_tools: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        servers = tuple(self.mcp_servers)
        if (
            len(servers) > 8
            or any(type(item) is not MCPServerConfig for item in servers)
            or len({item.name for item in servers}) != len(servers)
            or self.web is not None
            and type(self.web) is not WebAccessConfig
        ):
            raise ValueError("外部工具配置无效")
        copied = dict(self.tool_overrides)
        if len(copied) > 16 or any(
            not isinstance(k, str) or not isinstance(v, str) for k, v in copied.items()
        ):
            raise ValueError("工具覆盖配置无效")
        object.__setattr__(self, "mcp_servers", servers)
        object.__setattr__(self, "tool_overrides", MappingProxyType(copied))
        if self.public_tools is not None:
            names = tuple(self.public_tools)
            if len(names) > 16 or len(names) != len(set(names)):
                raise ValueError("公开工具选择无效")
            object.__setattr__(self, "public_tools", names)

    @classmethod
    def load(cls, path: Path, *, environment: Mapping[str, str] | None = None) -> ToolRuntimeConfig:
        """JSON只引用凭据变量名，不接受浏览器上传或配置内的明文Key。"""
        environment = os.environ if environment is None else environment
        try:
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_size > 65536:
                raise ValueError("配置必须是有界普通文件")
            body = parse_json(path.read_text(encoding="utf-8"))
            if not isinstance(body, dict) or set(body) - {
                "web",
                "mcp_servers",
                "tool_overrides",
                "public_tools",
            }:
                raise ValueError("配置字段无效")
            web = None
            if "web" in body:
                raw_web = body["web"]
                if not isinstance(raw_web, dict) or set(raw_web) - {"endpoint", "api_key_env"}:
                    raise ValueError("网页配置字段无效")
                key = _credential(
                    raw_web.get("api_key_env", "KRIM_AGENT_WEB_API_KEY"),
                    environment,
                    required=False,
                )
                web = WebAccessConfig(
                    api_key=SecretStr(key) if key else None,
                    endpoint=cast(str, raw_web.get("endpoint", "https://api.tavily.com")),
                )
            raw_servers = body.get("mcp_servers", [])
            if not isinstance(raw_servers, list) or len(raw_servers) > 8:
                raise ValueError("MCP配置无效")
            servers = []
            for raw in raw_servers:
                if not isinstance(raw, dict) or set(raw) & {"headers", "env"}:
                    raise ValueError("配置须引用凭据名而非明文值")
                values: dict[str, Any] = dict(raw)
                headers = _credential_map(values.pop("headers_env", {}), environment)
                env = _credential_map(values.pop("env_env", {}), environment)
                bearer = values.pop("bearer_env", None)
                if bearer is not None:
                    if any(name.lower() == "authorization" for name in headers):
                        raise ValueError("认证重复")
                    headers["Authorization"] = "Bearer " + _credential(bearer, environment)
                values["headers"] = {name: SecretStr(value) for name, value in headers.items()}
                if env:
                    values["env"] = env
                    values["secret_env_names"] = frozenset(env)
                for key in ("args", "selected_tools"):
                    if key in values:
                        if not isinstance(values[key], list):
                            raise ValueError("MCP列表字段无效")
                        values[key] = tuple(values[key])
                for key in ("readonly_tools", "allowed_hosts"):
                    if key in values:
                        if not isinstance(values[key], list):
                            raise ValueError("MCP集合字段无效")
                        values[key] = frozenset(values[key])
                servers.append(MCPServerConfig.model_validate(values))
            mapping = body.get("tool_overrides", {})
            public = body.get("public_tools")
            if not isinstance(mapping, dict) or any(
                not isinstance(v, str) for v in mapping.values()
            ):
                raise ValueError("覆盖字段无效")
            if public is not None and (
                not isinstance(public, list) or any(not isinstance(v, str) for v in public)
            ):
                raise ValueError("公开工具字段无效")
            return cls(
                web,
                tuple(servers),
                cast(dict[str, str], mapping),
                tuple(cast(list[str], public)) if public is not None else None,
            )
        except (OSError, ValueError, TypeError, KeyError):
            raise ServiceError("invalid_input", 422) from None


def _credential(name: object, env: Mapping[str, str], *, required: bool = True) -> str:
    if not isinstance(name, str) or re.fullmatch(r"[A-Z][A-Z0-9_]{0,127}", name) is None:
        raise ValueError("凭据变量名无效")
    if name in {"KRIM_AGENT_API_KEY", "KRIM_AGENT_HTTP_API_KEY"}:
        raise ValueError("不得复用实例或模型密钥")
    value = env.get(name, "")
    if (required and not value) or len(value) > 8192 or "\r" in value or "\n" in value:
        raise ValueError("凭据缺失或无效")
    return value


def _credential_map(value: object, env: Mapping[str, str]) -> dict[str, str]:
    if not isinstance(value, dict) or len(value) > 32:
        raise ValueError("凭据映射无效")
    return {str(key): _credential(name, env) for key, name in value.items()}


@dataclass
class ToolRuntime:
    registry: ToolRegistry
    policy: Policy | None
    connections: tuple[MCPConnection, ...] = ()
    web: TavilyWebAccess | None = None
    _close_task: asyncio.Task[None] | None = field(default=None, init=False, repr=False)

    @classmethod
    async def open(
        cls, base: ToolRegistry, config: ToolRuntimeConfig, policy: Policy | None
    ) -> ToolRuntime:
        base.assert_current()
        specs = [item for item, _ in base._frozen]
        connections: list[MCPConnection] = []
        web = None
        try:
            if config.web is not None or set(config.tool_overrides) & {"web_search", "web_fetch"}:
                web = TavilyWebAccess(config.web or WebAccessConfig())
                specs.extend(web_tools(web))
            for server in config.mcp_servers:
                connection = MCPConnection(server)
                connections.append(connection)
                await connection.connect()
                specs.extend(mcp_tools(connection))
            registry = resolve_tool_overrides(
                tuple(specs), config.tool_overrides, config.public_tools
            )
            effective = policy
            if policy is demo_policy:
                external = frozenset(item.name for item in specs if item.source is not None)

                async def external_policy(invocation: Invocation) -> Decision:
                    if invocation.name in external:
                        return "ask"
                    return await demo_policy(invocation)

                effective = external_policy
            return cls(registry, effective, tuple(connections), web)
        except BaseException:
            await settled(
                asyncio.gather(*(item.close() for item in connections), return_exceptions=True)
            )
            if web is not None:
                await settled(web.close())
            raise

    async def close(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await settled(self._close_task)

    async def _close(self) -> None:
        try:
            results = await asyncio.gather(
                *(item.close() for item in self.connections), return_exceptions=True
            )
            for result in results:
                if isinstance(result, BaseException):
                    raise result
        finally:
            if self.web is not None:
                await self.web.close()
