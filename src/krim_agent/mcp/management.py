"""可信MCP资源的持久引用管理；公开入口不接受宿主配置，见ADR-0081。"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

import httpx
from pydantic import Field
from sqlalchemy import func, select, update
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.exc import SQLAlchemyError

from krim_agent.persistence.database import Database, MCPResourceProfileRow
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel, utc_now
from krim_agent.tools import ToolRegistry

from .auth import MCPOAuthAuth
from .bridge import MCPBridge
from .client import MCPConnectionError, SessionFactory, _finish_cleanup, validate_server_url
from .types import MCPServerConfig, MCPServerStatus

_NAME = r"^[a-z][a-z0-9_-]{0,31}$"
_HASH = r"^[a-f0-9]{64}$"


class MCPPublicDescription(StrictModel):
    display_name: str = Field(min_length=1, max_length=120)
    transport: Literal["stdio", "streamable_http"]
    namespace: str = Field(pattern=_NAME)
    readonly_tools: list[str] = Field(default_factory=list, max_length=256)


class MCPProfileView(StrictModel):
    name: str = Field(pattern=_NAME)
    revision: int = Field(ge=1, le=9007199254740991)
    enabled: bool
    resource_version: str
    resource_hash: str = Field(pattern=_HASH)
    description: MCPPublicDescription
    updated_at: datetime


class MCPResourceView(StrictModel):
    name: str = Field(pattern=_NAME)
    available: bool
    version: str | None
    resource_hash: str | None
    description: MCPPublicDescription
    profile: MCPProfileView | None
    connection: MCPServerStatus


class MCPControlRequest(StrictModel):
    expected_revision: int = Field(ge=0, lt=9007199254740991)
    resource_hash: str = Field(pattern=_HASH)


class MCPSelectionRequest(MCPControlRequest):
    enabled: bool


@dataclass(frozen=True)
class TrustedMCPServer:
    config: MCPServerConfig
    version: str
    display_name: str | None = None
    auto_connect: bool = False
    auth: httpx.Auth | None = field(default=None, repr=False)
    session_factory: SessionFactory | None = field(default=None, repr=False)


def _public_hash(spec: TrustedMCPServer) -> str:
    payload = spec.config.model_dump(mode="json", exclude={"headers", "env"})
    for key in ("allowed_hosts", "readonly_tools", "deferred_tools", "always_load_tools"):
        payload[key] = sorted(payload[key])
    payload["header_names"] = sorted(name.lower() for name in spec.config.headers)
    payload["environment_names"] = sorted(spec.config.env or {})
    payload["version"] = spec.version
    payload["auth_kind"] = type(spec.auth).__qualname__ if spec.auth is not None else None
    if isinstance(spec.auth, MCPOAuthAuth):
        configured = spec.auth.declared_config
        if configured is not None:
            authorization = configured.model_dump(mode="json", exclude={"client_secret"})
            authorization["allowed_hosts"] = sorted(authorization["allowed_hosts"])
            authorization["scopes"] = sorted(set(authorization["scopes"]))
            payload["authorization"] = authorization
        payload["subject"] = spec.auth.subject
        payload["resource"] = spec.auth.resource_url
        payload["issuer"] = spec.auth.issuer
        payload["redirect_uri"] = spec.auth.redirect_uri
        payload["scopes"] = sorted(set(spec.auth.scopes))
    return hashlib.sha256(json.dumps(payload, sort_keys=True, allow_nan=False).encode()).hexdigest()


class MCPProfileStore:
    def __init__(self, database: Database, scope: str = "api") -> None:
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}", scope) is None:
            raise ValueError("MCP管理scope无效")
        self.database, self.scope = database, scope

    @staticmethod
    def _view(row: MCPResourceProfileRow) -> MCPProfileView:
        return MCPProfileView(
            name=row.name,
            revision=row.revision,
            enabled=row.enabled,
            resource_version=row.resource_version,
            resource_hash=row.resource_hash,
            description=MCPPublicDescription.model_validate(row.public_metadata),
            updated_at=row.updated_at,
        )

    async def get(self, name: str) -> MCPProfileView | None:
        async with self.database.write_lock, self.database.sessions() as session:
            row = await session.get(MCPResourceProfileRow, (self.scope, name))
            return self._view(row) if row is not None else None

    async def list(self) -> list[MCPProfileView]:
        async with self.database.write_lock, self.database.sessions() as session:
            rows = await session.scalars(
                select(MCPResourceProfileRow)
                .where(MCPResourceProfileRow.scope == self.scope)
                .order_by(MCPResourceProfileRow.name)
            )
            return [self._view(row) for row in rows]

    async def put(
        self,
        name: str,
        request: MCPSelectionRequest,
        version: str,
        description: MCPPublicDescription,
    ) -> MCPProfileView:
        try:
            async with self.database.write_lock, self.database.sessions.begin() as session:
                values = dict(
                    revision=request.expected_revision + 1,
                    enabled=request.enabled,
                    resource_version=version,
                    resource_hash=request.resource_hash,
                    public_metadata=description.model_dump(mode="json"),
                    updated_at=utc_now(),
                )
                if request.expected_revision == 0:
                    # 先写再数，独立SQLite连接也不能同时越过资源引用容量。
                    await session.execute(
                        update(MCPResourceProfileRow)
                        .where(MCPResourceProfileRow.scope == self.scope)
                        .values(revision=MCPResourceProfileRow.revision)
                    )
                    count = await session.scalar(
                        select(func.count())
                        .select_from(MCPResourceProfileRow)
                        .where(MCPResourceProfileRow.scope == self.scope)
                    )
                    if (count or 0) >= 256:
                        raise ServiceError("mcp_profile_limit", "MCP引用选择已达容量上限", 429)
                    selected = await session.scalar(
                        insert(MCPResourceProfileRow)
                        .values(scope=self.scope, name=name, **values)
                        .on_conflict_do_nothing()
                        .returning(MCPResourceProfileRow.name)
                    )
                else:
                    selected = await session.scalar(
                        update(MCPResourceProfileRow)
                        .where(
                            MCPResourceProfileRow.scope == self.scope,
                            MCPResourceProfileRow.name == name,
                            MCPResourceProfileRow.revision == request.expected_revision,
                        )
                        .values(**values)
                        .returning(MCPResourceProfileRow.name)
                    )
                if selected is None:
                    raise ServiceError("mcp_profile_conflict", "MCP引用选择版本已变化", 409)
                row = await session.get(MCPResourceProfileRow, (self.scope, name))
                if row is None:
                    raise ServiceError("mcp_profile_missing", "MCP引用选择未保存", 503)
                return self._view(row)
        except SQLAlchemyError:
            raise ServiceError("mcp_profile_store_failed", "MCP引用存储操作失败", 503) from None


class MCPManager:
    def __init__(
        self,
        database: Database,
        registry: ToolRegistry,
        *,
        scope: str = "api",
        on_ready: Callable[[list[str], str], None] | None = None,
    ) -> None:
        self.store = MCPProfileStore(database, scope)
        self.registry, self.bridge = registry, MCPBridge(registry)
        self.on_ready = on_ready
        self._resources: dict[str, tuple[TrustedMCPServer, str, MCPPublicDescription]] = {}
        self._jobs: dict[str, tuple[int, str, asyncio.Task[None]]] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._errors: dict[str, str] = {}
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None
        self._controls: set[asyncio.Task[MCPResourceView]] = set()

    def register(self, spec: TrustedMCPServer) -> None:
        self._live()
        config = MCPServerConfig.model_validate(spec.config.model_dump()).model_copy(deep=True)
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", spec.version) is None:
            raise ValueError("MCP资源版本无效")
        if config.name in self._resources or len(self._resources) >= 32:
            raise ServiceError("mcp_resource_conflict", "MCP资源重复或超过32项", 409)
        validate_server_url(config)
        frozen = TrustedMCPServer(
            config,
            spec.version,
            spec.display_name,
            spec.auto_connect,
            spec.auth,
            spec.session_factory,
        )
        description = MCPPublicDescription(
            display_name=spec.display_name or config.name,
            transport=config.transport,
            namespace=config.namespace or config.name,
            readonly_tools=sorted(config.readonly_tools),
        )
        self._resources[config.name] = (frozen, _public_hash(frozen), description)

    def _live(self) -> None:
        if self._closed:
            raise ServiceError("mcp_manager_closed", "MCP管理已关闭", 503)

    def _external(self, name: str | None = None) -> None:
        for key, connection in self.bridge._connections.items():
            if name is None or key == name:
                connection.assert_close_allowed()

    def assert_external_close(self) -> None:
        self._external()

    async def view(self, name: str) -> MCPResourceView:
        profile, resource = await self.store.get(name), self._resources.get(name)
        if profile is None and resource is None:
            raise ServiceError("mcp_resource_missing", "MCP资源未登记", 404)
        if resource is None:
            assert profile is not None
            spec, digest, description = None, None, profile.description
        else:
            spec, digest, description = resource
        available = bool(spec and spec.config.enabled and not self._closed)
        status = next((value for value in self.bridge.statuses() if value.name == name), None)
        if status is None:
            status = MCPServerStatus(name=name, namespace=description.namespace, status="closed")
        job = self._jobs.get(name)
        if job is not None and not job[2].done() and status.status not in {"ready", "closing"}:
            status = status.model_copy(
                update={"status": "connecting", "error": None, "error_code": None}
            )
        if name in self._errors and status.status not in {"ready", "connecting", "closing"}:
            status = status.model_copy(
                update={
                    "error_code": self._errors[name],
                    "status": "failed",
                    "error": "MCP资源或引用版本不可用",
                }
            )
        return MCPResourceView(
            name=name,
            available=available,
            version=spec.version if spec else None,
            resource_hash=digest,
            description=description,
            profile=profile,
            connection=status,
        )

    async def list(self) -> list[MCPResourceView]:
        names = set(self._resources) | {item.name for item in await self.store.list()}
        return [await self.view(name) for name in sorted(names)]

    async def configure(self, name: str, request: MCPSelectionRequest) -> MCPResourceView:
        self._live()
        self._external(name)
        operation = asyncio.create_task(self._configure(name, request))
        self._controls.add(operation)
        operation.add_done_callback(self._controls.discard)
        return await _finish_cleanup(operation)

    async def _configure(self, name: str, request: MCPSelectionRequest) -> MCPResourceView:
        async with self._locks.setdefault(name, asyncio.Lock()):
            self._live()
            resource, previous = self._resources.get(name), await self.store.get(name)
            if resource is None:
                if request.enabled or previous is None:
                    raise ServiceError("mcp_resource_missing", "MCP资源未登记", 404)
                version, digest, description = (
                    previous.resource_version,
                    previous.resource_hash,
                    previous.description,
                )
            else:
                spec, digest, description = resource
                version = spec.version
                if request.enabled and not spec.config.enabled:
                    raise ServiceError("mcp_resource_disabled", "宿主已禁止该MCP资源连接", 403)
            if request.resource_hash != digest:
                raise ServiceError("mcp_resource_changed", "MCP资源版本已变化，请重新审阅", 409)
            await self.store.put(name, request, version, description)
            self._errors.pop(name, None)
            await self._stop(name)
        return await self.view(name)

    async def _check(self, name: str, request: MCPControlRequest) -> TrustedMCPServer:
        resource, profile = self._resources.get(name), await self.store.get(name)
        if resource is None or not resource[0].config.enabled:
            raise ServiceError("mcp_resource_missing", "MCP资源未登记或宿主已停用", 404)
        spec, digest, _description = resource
        if profile is None or not profile.enabled:
            raise ServiceError("mcp_resource_disabled", "MCP引用选择未启用", 409)
        if profile.revision != request.expected_revision:
            raise ServiceError("mcp_profile_conflict", "MCP引用选择版本已变化", 409)
        if request.resource_hash != digest or profile.resource_hash != digest:
            raise ServiceError("mcp_resource_changed", "MCP资源与已确认版本不一致", 409)
        return spec

    async def connect(self, name: str, request: MCPControlRequest) -> MCPResourceView:
        self._live()
        self._external(name)
        async with self._locks.setdefault(name, asyncio.Lock()):
            self._live()
            spec = await self._check(name, request)
            existing = self._jobs.get(name)
            if existing is not None and not existing[2].done():
                if existing[:2] != (request.expected_revision, request.resource_hash):
                    raise ServiceError("mcp_connect_busy", "MCP仍在连接其他版本", 409)
            elif (await self.view(name)).connection.status != "ready":
                self._errors.pop(name, None)
                task = asyncio.create_task(
                    self._connect(name, spec, request), name=f"krim-mcp-connect-{name}"
                )
                self._jobs[name] = (request.expected_revision, request.resource_hash, task)
        return await self.view(name)

    async def _guard(self, name: str, digest: str) -> None:
        resource, profile = self._resources.get(name), await self.store.get(name)
        if self._closed or profile is None or not profile.enabled:
            raise MCPConnectionError("MCP引用选择已停用", "mcp_resource_disabled")
        if resource is None or not resource[0].config.enabled:
            raise MCPConnectionError("MCP宿主资源不可用", "mcp_resource_disabled")
        if resource[1] != digest or profile.resource_hash != digest:
            raise MCPConnectionError("MCP资源版本已变化", "mcp_resource_changed")

    async def _connect(self, name: str, spec: TrustedMCPServer, request: MCPControlRequest) -> None:
        try:
            if name in self.bridge._connections:
                await self.bridge.remove_server(name)

            async def guard() -> None:
                await self._guard(name, request.resource_hash)

            async with asyncio.timeout(spec.config.initialization_timeout_seconds):
                status = await self.bridge.add_server(
                    spec.config,
                    auth=spec.auth,
                    session_factory=spec.session_factory,
                    request_guard=guard,
                )
                if status.status == "ready" and self.on_ready is not None:
                    self.on_ready(status.tools, request.resource_hash)
                await self._check(name, request)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._errors[name] = (
                error.code
                if isinstance(error, (ServiceError, MCPConnectionError))
                else "mcp_connect_timeout"
                if isinstance(error, TimeoutError)
                else "mcp_connect_failed"
            )
            if name in self.bridge._connections:
                await self.bridge.remove_server(name)

    async def _stop(self, name: str) -> None:
        job = self._jobs.get(name)
        if job is not None and not job[2].done():
            job[2].cancel()
            await asyncio.gather(job[2], return_exceptions=True)
        if name in self.bridge._connections:
            await self.bridge.remove_server(name)

    async def disconnect(self, name: str, request: MCPControlRequest) -> MCPResourceView:
        self._live()
        self._external(name)
        operation = asyncio.create_task(self._disconnect(name, request))
        self._controls.add(operation)
        operation.add_done_callback(self._controls.discard)
        return await _finish_cleanup(operation)

    async def _disconnect(self, name: str, request: MCPControlRequest) -> MCPResourceView:
        async with self._locks.setdefault(name, asyncio.Lock()):
            await self._check(name, request)
            await self._stop(name)
        return await self.view(name)

    async def wait(self, name: str, timeout_seconds: float = 30) -> MCPResourceView:
        if not math.isfinite(timeout_seconds) or not 0 <= timeout_seconds <= 300:
            raise ServiceError("mcp_wait_budget", "MCP等待预算须为0至300秒", 422)
        job = self._jobs.get(name)
        if job is not None and not job[2].done():
            await asyncio.wait([job[2]], timeout=timeout_seconds)
        return await self.view(name)

    async def initialize(self) -> None:
        self._live()
        for name, (spec, digest, _description) in self._resources.items():
            profile = await self.store.get(name)
            if (
                spec.auto_connect
                and spec.config.enabled
                and profile is not None
                and profile.enabled
                and profile.resource_hash == digest
            ):
                await self.connect(
                    name,
                    MCPControlRequest(expected_revision=profile.revision, resource_hash=digest),
                )
                job = self._jobs.get(name)
                if job is not None:
                    await asyncio.wait([job[2]], timeout=spec.config.initialization_timeout_seconds)

    async def close(self) -> None:
        self._external()
        if self._close_task is None:
            self._closed = True
            self._close_task = asyncio.create_task(self._close())
        await _finish_cleanup(self._close_task)

    async def _close(self) -> None:
        await asyncio.gather(*list(self._controls), return_exceptions=True)
        for _revision, _digest, job in self._jobs.values():
            if not job.done():
                job.cancel()
        await asyncio.gather(
            *(job for _revision, _digest, job in self._jobs.values()), return_exceptions=True
        )
        await self.bridge.close()


class MCPBootstrapServer(StrictModel):
    config: MCPServerConfig
    version: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
    display_name: str | None = Field(default=None, max_length=120)
    auto_connect: bool = False


class MCPBootstrap(StrictModel):
    servers: list[MCPBootstrapServer] = Field(max_length=32)

    def registrations(self) -> Sequence[TrustedMCPServer]:
        names = [value.config.name for value in self.servers]
        if len(names) != len(set(names)):
            raise ValueError("MCP启动配置包含重复名称")
        return [
            TrustedMCPServer(item.config, item.version, item.display_name, item.auto_connect)
            for item in self.servers
        ]
