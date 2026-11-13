"""可信宿主的外部工具装配与资源所有权。"""

from __future__ import annotations

import asyncio
import importlib
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast
from urllib.parse import urlsplit

from pydantic import SecretStr

from krim_agent.contracts import ServiceError, parse_json
from krim_agent.mcp import MCPConnection, MCPServerConfig
from krim_agent.network.egress_policy import EgressPolicy, EgressViolation
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
        if len(copied) > 16:
            raise ServiceError("tool_limit", 422)
        if any(not isinstance(k, str) or not isinstance(v, str) for k, v in copied.items()):
            raise ValueError("工具覆盖配置无效")
        object.__setattr__(self, "mcp_servers", servers)
        object.__setattr__(self, "tool_overrides", MappingProxyType(copied))
        if self.public_tools is not None:
            names = tuple(self.public_tools)
            if len(names) > 16:
                raise ServiceError("tool_limit", 422)
            if len(names) != len(set(names)):
                raise ValueError("公开工具选择无效")
            object.__setattr__(self, "public_tools", names)

    @classmethod
    def load(cls, path: Path, *, environment: Mapping[str, str] | None = None) -> ToolRuntimeConfig:
        """JSON只引用凭据变量名，不接受浏览器上传或配置内的明文Key。"""
        environment = os.environ if environment is None else environment
        try:
            config_bytes = _read_runtime_config(path)
            body = parse_json(config_bytes.decode("utf-8"))
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
            servers: list[MCPServerConfig] = []
            for raw_server in raw_servers:
                if not isinstance(raw_server, dict) or set(raw_server) & {"headers", "env"}:
                    raise ValueError("配置须引用凭据名而非明文值")
                values: dict[str, Any] = dict(raw_server)
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


def _validate_windows_acl(descriptor: int) -> None:
    """Require a user-owned config without write ACEs for broad principals."""
    if os.name != "nt":
        return
    import ctypes
    from ctypes import wintypes

    win_dll = cast(Any, getattr(ctypes, "WinDLL"))
    msvcrt = cast(Any, importlib.import_module("msvcrt"))
    advapi32 = win_dll("Advapi32.dll", use_last_error=True)
    kernel32 = win_dll("Kernel32.dll", use_last_error=True)
    security_descriptor = ctypes.c_void_p()
    owner_sid = ctypes.c_void_p()
    dacl = ctypes.c_void_p()
    get_security_info = advapi32.GetSecurityInfo
    get_security_info.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.POINTER(ctypes.c_void_p),
    ]
    get_security_info.restype = wintypes.DWORD
    result = get_security_info(
        wintypes.HANDLE(cast(int, msvcrt.get_osfhandle(descriptor))),
        1,  # SE_FILE_OBJECT
        0x00000001 | 0x00000004,  # OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION
        ctypes.byref(owner_sid),
        None,
        ctypes.byref(dacl),
        None,
        ctypes.byref(security_descriptor),
    )
    if result != 0:
        raise ValueError("无法读取配置文件ACL")

    class _SidAndAttributes(ctypes.Structure):
        _fields_ = [("sid", ctypes.c_void_p), ("attributes", wintypes.DWORD)]

    class _TokenUser(ctypes.Structure):
        _fields_ = [("user", _SidAndAttributes)]

    class _AceHeader(ctypes.Structure):
        _fields_ = [
            ("ace_type", ctypes.c_ubyte),
            ("ace_flags", ctypes.c_ubyte),
            ("ace_size", wintypes.WORD),
        ]

    kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    advapi32.OpenProcessToken.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.HANDLE),
    ]
    advapi32.OpenProcessToken.restype = wintypes.BOOL
    advapi32.GetTokenInformation.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
    ]
    advapi32.GetTokenInformation.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    kernel32.LocalFree.argtypes = [ctypes.c_void_p]
    kernel32.LocalFree.restype = ctypes.c_void_p
    token = wintypes.HANDLE()
    allocated_sids: list[ctypes.c_void_p] = []
    try:
        if not advapi32.OpenProcessToken(
            kernel32.GetCurrentProcess(), 0x0008, ctypes.byref(token)
        ):
            raise ValueError("无法读取当前用户Token")
        size = wintypes.DWORD()
        advapi32.GetTokenInformation(token, 1, None, 0, ctypes.byref(size))
        if not size.value:
            raise ValueError("无法读取当前用户SID")
        token_buffer = ctypes.create_string_buffer(size.value)
        if not advapi32.GetTokenInformation(
            token, 1, token_buffer, size, ctypes.byref(size)
        ):
            raise ValueError("无法读取当前用户SID")
        current_sid = ctypes.cast(token_buffer, ctypes.POINTER(_TokenUser)).contents.user.sid

        get_security_descriptor_owner = advapi32.GetSecurityDescriptorOwner
        get_security_descriptor_owner.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.POINTER(wintypes.BOOL),
        ]
        get_security_descriptor_owner.restype = wintypes.BOOL
        owner_defaulted = wintypes.BOOL()
        if not get_security_descriptor_owner(
            security_descriptor, ctypes.byref(owner_sid), ctypes.byref(owner_defaulted)
        ):
            raise ValueError("无法读取配置文件所有者")
        equal_sid = advapi32.EqualSid
        equal_sid.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
        equal_sid.restype = wintypes.BOOL
        if not equal_sid(owner_sid, current_sid):
            raise ValueError("配置文件所有者不可信")

        # A null DACL grants full access to everyone, so it is never trusted.
        get_security_descriptor_dacl = advapi32.GetSecurityDescriptorDacl
        get_security_descriptor_dacl.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(wintypes.BOOL),
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.POINTER(wintypes.BOOL),
        ]
        get_security_descriptor_dacl.restype = wintypes.BOOL
        dacl_present = wintypes.BOOL()
        dacl_defaulted = wintypes.BOOL()
        if not get_security_descriptor_dacl(
            security_descriptor,
            ctypes.byref(dacl_present),
            ctypes.byref(dacl),
            ctypes.byref(dacl_defaulted),
        ) or not dacl_present.value or not dacl.value:
            raise ValueError("配置文件ACL缺失")

        class _Acl(ctypes.Structure):
            _fields_ = [
                ("revision", ctypes.c_ubyte),
                ("sbz1", ctypes.c_ubyte),
                ("size", wintypes.WORD),
                ("ace_count", wintypes.WORD),
                ("sbz2", wintypes.WORD),
            ]

        acl = ctypes.cast(dacl, ctypes.POINTER(_Acl)).contents
        get_ace = advapi32.GetAce
        get_ace.argtypes = [ctypes.c_void_p, wintypes.DWORD, ctypes.POINTER(ctypes.c_void_p)]
        get_ace.restype = wintypes.BOOL
        write_mask = 0x00000002 | 0x00000004 | 0x00000010 | 0x00000100
        write_mask |= 0x00010000 | 0x00040000 | 0x00080000 | 0x40000000 | 0x10000000
        allowed_ace_types = {0}  # Standard ACCESS_ALLOWED_ACE_TYPE.
        known_non_grant_types = {1, 2, 3, 6, 7, 8, 10, 12, 13, 14, 17, 18, 19, 20, 21}
        permitted_sids: list[ctypes.c_void_p] = [current_sid]
        convert_sid = advapi32.ConvertStringSidToSidW
        convert_sid.argtypes = [wintypes.LPCWSTR, ctypes.POINTER(ctypes.c_void_p)]
        convert_sid.restype = wintypes.BOOL
        for value in ("S-1-5-18", "S-1-5-32-544"):
            sid = ctypes.c_void_p()
            if not convert_sid(value, ctypes.byref(sid)):
                raise ValueError("无法读取系统ACL主体")
            allocated_sids.append(sid)
            permitted_sids.append(sid)

        for index in range(acl.ace_count):
            ace = ctypes.c_void_p()
            if not get_ace(dacl, index, ctypes.byref(ace)):
                raise ValueError("无法读取配置文件ACE")
            header = ctypes.cast(ace, ctypes.POINTER(_AceHeader)).contents
            if header.ace_type not in allowed_ace_types:
                if header.ace_type in known_non_grant_types:
                    continue
                raise ValueError("配置文件ACL含未知ACE类型")
            mask = ctypes.c_uint32.from_address(ace.value + ctypes.sizeof(_AceHeader)).value
            if not mask & write_mask:
                continue
            sid_offset = 8
            ace_sid = ctypes.c_void_p(ace.value + sid_offset)
            if not any(equal_sid(ace_sid, permitted) for permitted in permitted_sids):
                raise ValueError("配置文件ACL允许不可信主体写入")
    finally:
        for sid in allocated_sids:
            kernel32.LocalFree(sid)
        if token.value:
            kernel32.CloseHandle(token)
        if security_descriptor.value:
            kernel32.LocalFree(security_descriptor)


def _read_runtime_config(path: Path) -> bytes:
    if not isinstance(path, Path):
        raise ValueError("配置路径无效")
    path = path.absolute()
    if any(part.is_symlink() for part in (path, *path.parents)):
        raise ValueError("配置路径不得包含符号链接")
    known = path.lstat()
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NONBLOCK", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        info = os.fstat(descriptor)
        identity = (info.st_dev, info.st_ino, info.st_mode, info.st_nlink, info.st_size)
        known_identity = (
            known.st_dev, known.st_ino, known.st_mode, known.st_nlink, known.st_size
        )
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_nlink != 1
            or info.st_size > 65536
            or identity != known_identity
            or os.name == "posix"
            and (
                info.st_mode & 0o077
                or not hasattr(os, "geteuid")
                or info.st_uid != os.geteuid()
            )
        ):
            raise ValueError("配置必须是权限受限的有界普通文件")
        try:
            _validate_windows_acl(descriptor)
        except Exception:
            raise ValueError("配置文件ACL无效") from None
        chunks: list[bytes] = []
        remaining = 65537
        while remaining:
            chunk = os.read(descriptor, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
        path_after = path.lstat()
        if (
            len(raw) != info.st_size
            or len(raw) > 65536
            or (after.st_dev, after.st_ino, after.st_size, after.st_nlink)
            != (info.st_dev, info.st_ino, info.st_size, info.st_nlink)
            or (path_after.st_dev, path_after.st_ino) != (info.st_dev, info.st_ino)
            or any(part.is_symlink() for part in (path, *path.parents))
        ):
            raise ValueError("配置文件在读取期间发生变化")
        return raw
    finally:
        os.close(descriptor)


def _mcp_egress_policy(server: MCPServerConfig) -> EgressPolicy:
    try:
        parsed = urlsplit(server.url or "")
        host = parsed.hostname
        configured_port = parsed.port
        port = configured_port if configured_port is not None else (
            443 if parsed.scheme == "https" else 80
        )
        if host is None:
            raise ValueError("MCP endpoint is missing a host")
        policy_type = EgressPolicy.authorize_loopback if parsed.scheme == "http" else EgressPolicy
        # The host list is an explicit redirect boundary. Keep every configured
        # host, but pin all of them to the endpoint's configured port.
        return policy_type(server.allowed_hosts, {port})
    except (EgressViolation, ValueError):
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
                if server.transport == "streamable_http":
                    connection = MCPConnection(
                        server, egress_policy=_mcp_egress_policy(server)
                    )
                else:
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
