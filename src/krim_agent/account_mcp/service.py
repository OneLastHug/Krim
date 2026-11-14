"""Owner-scoped MCP server configuration, credential storage, and turn snapshots."""

from __future__ import annotations

import asyncio
import hmac
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any, cast

from pydantic import Field, SecretStr, ValidationError, field_validator, model_validator
from sqlalchemy import delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.account_mcp.contracts import SecretContext, SecretVault
from krim_agent.account_mcp.secrets import MAX_PLAINTEXT_BYTES
from krim_agent.contracts import ServiceError, StrictModel, encoded, fingerprint, parse_json
from krim_agent.mcp.client import MCPConnection
from krim_agent.mcp.types import MCPServerConfig
from krim_agent.security import PrincipalContext
from krim_agent.storage.mcp_models import mcp_connections, mcp_requests, session_mcp, turn_mcp
from krim_agent.storage.models import sessions
from krim_agent.tools.core import Decision, Invocation
from krim_agent.tools.executor import ToolExecutor
from krim_agent.tools.mcp import mcp_tools
from krim_agent.tools.registry import MAX_TOOLS, ToolRegistry
from krim_agent.tools.runtime import _mcp_egress_policy

if TYPE_CHECKING:
    from krim_agent.accounts.service import IdentityService
    from krim_agent.storage.sqlite import SQLiteStore

MAX_ACCOUNT_SERVERS = 8
MAX_SELECTED_TOOLS = 8
MAX_CONNECTION_CONFIG_BYTES = 8192
_CREDENTIAL_PURPOSE = "mcp-credential"


class ConnectionCreate(StrictModel):
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[!-~]+$")
    name: str = Field(min_length=1, max_length=32, pattern=r"^[a-z][a-z0-9_-]{0,31}$")
    url: str = Field(min_length=1, max_length=2048)
    allowed_hosts: tuple[str, ...] = Field(min_length=1, max_length=16)
    selected_tools: tuple[str, ...] = Field(min_length=1, max_length=MAX_SELECTED_TOOLS)
    readonly_tools: tuple[str, ...] = Field(default=(), max_length=MAX_SELECTED_TOOLS)
    credentials: dict[str, SecretStr] = Field(default_factory=dict, max_length=32)
    timeout_seconds: float = Field(default=10.0, gt=0, le=60)

    @field_validator("allowed_hosts", "selected_tools", "readonly_tools")
    @classmethod
    def unique_values(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(value)) != len(value):
            raise ValueError("duplicate values")
        return value

    @model_validator(mode="after")
    def validate_configuration(self) -> ConnectionCreate:
        _validate_server(
            self.name,
            self.url,
            self.allowed_hosts,
            self.selected_tools,
            self.readonly_tools,
            self.timeout_seconds,
        )
        _validate_credentials(self.credentials)
        return self


class ConnectionUpdate(StrictModel):
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[!-~]+$")
    expected_revision: int = Field(ge=1)
    name: str = Field(min_length=1, max_length=32, pattern=r"^[a-z][a-z0-9_-]{0,31}$")
    url: str = Field(min_length=1, max_length=2048)
    allowed_hosts: tuple[str, ...] = Field(min_length=1, max_length=16)
    selected_tools: tuple[str, ...] = Field(min_length=1, max_length=MAX_SELECTED_TOOLS)
    readonly_tools: tuple[str, ...] = Field(default=(), max_length=MAX_SELECTED_TOOLS)
    credentials: dict[str, SecretStr] | None = Field(default=None, max_length=32)
    timeout_seconds: float = Field(default=10.0, gt=0, le=60)

    @field_validator("allowed_hosts", "selected_tools", "readonly_tools")
    @classmethod
    def unique_values(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(value)) != len(value):
            raise ValueError("duplicate values")
        return value

    @model_validator(mode="after")
    def validate_configuration(self) -> ConnectionUpdate:
        _validate_server(
            self.name,
            self.url,
            self.allowed_hosts,
            self.selected_tools,
            self.readonly_tools,
            self.timeout_seconds,
        )
        if self.credentials is not None:
            _validate_credentials(self.credentials)
        return self


class ConnectionDelete(StrictModel):
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[!-~]+$")
    expected_revision: int = Field(ge=1)


class SessionConnection(StrictModel):
    connection_id: str = Field(min_length=1, max_length=64)
    revision: int = Field(ge=1)


class SessionMCPSelection(StrictModel):
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[!-~]+$")
    expected_revision: int = Field(ge=0)
    connections: tuple[SessionConnection, ...] = Field(default=(), max_length=MAX_ACCOUNT_SERVERS)

    @model_validator(mode="after")
    def unique_connections(self) -> SessionMCPSelection:
        if len({item.connection_id for item in self.connections}) != len(self.connections):
            raise ValueError("duplicate connections")
        return self


def _validate_server(
    name: str,
    url: str,
    allowed_hosts: tuple[str, ...],
    selected_tools: tuple[str, ...],
    readonly_tools: tuple[str, ...],
    timeout_seconds: float,
) -> None:
    try:
        MCPServerConfig(
            name=name,
            transport="streamable_http",
            url=url,
            allowed_hosts=frozenset(allowed_hosts),
            selected_tools=selected_tools,
            readonly_tools=frozenset(readonly_tools),
            timeout_seconds=timeout_seconds,
        )
    except (ValidationError, ValueError, TypeError):
        raise ValueError("invalid MCP server configuration") from None
    if not set(readonly_tools) <= set(selected_tools):
        raise ValueError("readonly tools must be selected")


def _credential_values(credentials: Mapping[str, str | SecretStr]) -> dict[str, str]:
    return {
        name: value.get_secret_value() if isinstance(value, SecretStr) else value
        for name, value in credentials.items()
    }


def _validate_credentials(credentials: Mapping[str, object]) -> None:
    if type(credentials) is not dict or len(credentials) > 32:
        raise ValueError("invalid credentials")
    try:
        validated: list[tuple[str, str]] = []
        for name, raw in credentials.items():
            value = raw.get_secret_value() if isinstance(raw, SecretStr) else raw
            if type(name) is not str or type(value) is not str or len(value) > 8192:
                raise ValueError("invalid credential")
            if any(ord(char) < 32 or ord(char) == 127 for char in value):
                raise ValueError("invalid credential")
            validated.append((name, value))
        # Reuse the transport's strict header-name validation without retaining the values.
        MCPServerConfig(
            name="credential_check",
            transport="streamable_http",
            url="https://localhost/mcp",
            allowed_hosts=frozenset({"localhost"}),
            selected_tools=("check",),
            headers={name: SecretStr(value) for name, value in validated},
        )
    except (ValidationError, ValueError, TypeError):
        raise ValueError("invalid credentials") from None


def _public(row: Any) -> dict[str, Any]:
    config = cast(dict[str, Any], row["config"])
    return {
        "id": row["id"],
        "name": config["name"],
        "url": config["url"],
        "allowed_hosts": list(config["allowed_hosts"]),
        "selected_tools": list(config["selected_tools"]),
        "readonly_tools": list(config["readonly_tools"]),
        "timeout_seconds": config["timeout_seconds"],
        "revision": row["revision"],
        "credential_revision": row["credential_revision"],
        "credential_configured": row["credential_envelope"] is not None,
    }


def _config_document(body: ConnectionCreate | ConnectionUpdate) -> dict[str, Any]:
    return {
        "name": body.name,
        "transport": "streamable_http",
        "url": body.url,
        "allowed_hosts": list(body.allowed_hosts),
        "selected_tools": list(body.selected_tools),
        "readonly_tools": list(body.readonly_tools),
        "timeout_seconds": body.timeout_seconds,
    }


class AccountMCPService:
    def __init__(
        self,
        store: SQLiteStore,
        identities: IdentityService | None,
        vault: SecretVault | None,
    ) -> None:
        self.store, self.identities, self.vault = store, identities, vault
        self._request_locks: dict[tuple[str, str], asyncio.Lock] = {}

    def actor(self, actor: PrincipalContext) -> PrincipalContext:
        if self.store.schema_version < 12:
            raise ServiceError("schema_upgrade_required", 409)
        if self.identities is None or not self.identities.managed:
            raise ServiceError("account_management_unavailable", 409)
        return self.identities.validate(actor)

    def _secret_context(
        self, principal_id: str, connection_id: str, revision: int
    ) -> SecretContext:
        return SecretContext(principal_id, connection_id, revision, _CREDENTIAL_PURPOSE)

    def _seal(
        self,
        principal_id: str,
        connection_id: str,
        revision: int,
        credentials: Mapping[str, str | SecretStr],
    ) -> str | None:
        if not credentials:
            return None
        plaintext = encoded(_credential_values(credentials)).encode("utf-8")
        if len(plaintext) > MAX_PLAINTEXT_BYTES:
            raise ServiceError("input_limit", 422)
        vault = self.vault
        if vault is None:
            raise ServiceError("mcp_unavailable", 503)
        try:
            return vault.encrypt(
                self._secret_context(principal_id, connection_id, revision), plaintext
            )
        except ServiceError:
            raise
        except Exception:
            raise ServiceError("mcp_unavailable", 503) from None

    def _open_secret(
        self, principal_id: str, connection_id: str, revision: int, envelope: str | None
    ) -> dict[str, str]:
        if envelope is None:
            return {}
        vault = self.vault
        if vault is None:
            raise ServiceError("mcp_unavailable", 503)
        try:
            raw = vault.decrypt(
                self._secret_context(principal_id, connection_id, revision), envelope
            )
            value = parse_json(raw.decode("utf-8"))
            if type(value) is not dict:
                raise ValueError("invalid credential document")
            _validate_credentials(value)
            return cast(dict[str, str], value)
        except ServiceError:
            raise
        except (UnicodeDecodeError, ValueError, TypeError):
            raise ServiceError("facts_corrupt") from None

    async def _read(
        self, actor: PrincipalContext, operation: Callable[[AsyncConnection], Awaitable[Any]]
    ) -> Any:
        self.actor(actor)
        async with self.store.lock, self.store.engine.begin() as conn:
            result = await operation(conn)
        self.actor(actor)
        return result

    async def _mutate(
        self,
        actor: PrincipalContext,
        body: ConnectionCreate | ConnectionUpdate | ConnectionDelete | SessionMCPSelection,
        action: str,
        operation: Callable[
            [AsyncConnection],
            Awaitable[tuple[dict[str, Any], tuple[str, int, str | None] | None]],
        ],
        *,
        secret: Mapping[str, str | SecretStr] | None = None,
    ) -> dict[str, Any]:
        self.actor(actor)
        fields = body.model_dump(mode="json", exclude={"credentials"})
        secret_values = _credential_values(secret) if secret is not None else None
        secret_state = "absent" if secret is None else "empty" if not secret else "value"
        digest = fingerprint({"action": action, "body": fields, "secret_state": secret_state})

        async def known(conn: AsyncConnection) -> dict[str, Any] | None:
            self.actor(actor)
            row = (
                (
                    await conn.execute(
                        select(mcp_requests).where(
                            mcp_requests.c.principal_id == actor.principal_id,
                            mcp_requests.c.request_key == body.request_key,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                return None
            if row["request_hash"] != digest:
                raise ServiceError("key_conflict", 409)
            if secret_state == "value":
                if row["secret_envelope"] is None:
                    raise ServiceError("key_conflict", 409)
                stored = self._open_secret(
                    actor.principal_id,
                    row["secret_connection_id"],
                    row["secret_revision"],
                    row["secret_envelope"],
                )
                if not hmac.compare_digest(encoded(stored), encoded(secret_values)):
                    raise ServiceError("key_conflict", 409)
            elif secret_state == "empty" and row["secret_envelope"] is not None:
                raise ServiceError("key_conflict", 409)
            return cast(dict[str, Any], row["result"])

        async def write(conn: AsyncConnection) -> dict[str, Any]:
            previous = await known(conn)
            if previous is not None:
                return previous
            result, receipt_secret = await operation(conn)
            self.actor(actor)
            await conn.execute(
                insert(mcp_requests).values(
                    principal_id=actor.principal_id,
                    request_key=body.request_key,
                    request_hash=digest,
                    result=result,
                    secret_connection_id=receipt_secret[0] if receipt_secret else None,
                    secret_revision=receipt_secret[1] if receipt_secret else None,
                    secret_envelope=receipt_secret[2] if receipt_secret else None,
                )
            )
            return result

        async def confirm() -> dict[str, Any] | None:
            replayed: dict[str, Any] | None = await self._read(actor, known)
            return replayed

        lock_key = (actor.principal_id, body.request_key)
        lock = self._request_locks.setdefault(lock_key, asyncio.Lock())
        async with lock:
            if len(self._request_locks) > 256:
                for key, candidate in list(self._request_locks.items()):
                    if key != lock_key and not candidate.locked():
                        self._request_locks.pop(key, None)
                        if len(self._request_locks) <= 256:
                            break
            return await self.store._eval_write(write, confirm)

    async def list_connections(self, actor: PrincipalContext) -> list[dict[str, Any]]:
        async def read(conn: AsyncConnection) -> list[dict[str, Any]]:
            rows = (
                (
                    await conn.execute(
                        select(mcp_connections)
                        .where(mcp_connections.c.principal_id == actor.principal_id)
                        .order_by(mcp_connections.c.created_at, mcp_connections.c.id)
                    )
                )
                .mappings()
                .all()
            )
            return [_public(row) for row in rows]

        listed: list[dict[str, Any]] = await self._read(actor, read)
        return listed

    async def create_connection(
        self, actor: PrincipalContext, body: ConnectionCreate
    ) -> dict[str, Any]:
        connection_id = str(uuid.uuid4())
        config = _config_document(body)
        if len(encoded(config).encode("utf-8")) > MAX_CONNECTION_CONFIG_BYTES:
            raise ServiceError("input_limit", 422)
        if len(body.selected_tools) > MAX_SELECTED_TOOLS:
            raise ServiceError("tool_limit", 422)
        credential_revision = 1
        secret = body.credentials

        async def operation(
            conn: AsyncConnection,
        ) -> tuple[dict[str, Any], tuple[str, int, str | None] | None]:
            more = (
                await conn.execute(
                    select(mcp_connections.c.id)
                    .where(mcp_connections.c.principal_id == actor.principal_id)
                    .limit(MAX_ACCOUNT_SERVERS)
                )
            ).all()
            if len(more) >= MAX_ACCOUNT_SERVERS:
                raise ServiceError("input_limit", 422)
            duplicate = await conn.scalar(
                select(mcp_connections.c.id).where(
                    mcp_connections.c.principal_id == actor.principal_id,
                    mcp_connections.c.name == body.name,
                )
            )
            if duplicate is not None:
                raise ServiceError("revision_conflict", 409)
            envelope = self._seal(
                actor.principal_id, connection_id, credential_revision, body.credentials
            )
            await conn.execute(
                insert(mcp_connections).values(
                    id=connection_id,
                    principal_id=actor.principal_id,
                    name=body.name,
                    revision=1,
                    credential_revision=credential_revision,
                    config=config,
                    credential_envelope=envelope,
                    created_at=time.time(),
                    updated_at=time.time(),
                )
            )
            result = {
                "id": connection_id,
                "name": body.name,
                "revision": 1,
                "credential_configured": envelope is not None,
            }
            receipt = (
                (connection_id, credential_revision, envelope)
                if body.credentials is not None
                else None
            )
            return result, receipt

        return await self._mutate(actor, body, "create-connection", operation, secret=secret)

    async def update_connection(
        self, actor: PrincipalContext, connection_id: str, body: ConnectionUpdate
    ) -> dict[str, Any]:
        if not connection_id or len(connection_id) > 64:
            raise ServiceError("not_found", 404)
        config = _config_document(body)
        if len(encoded(config).encode("utf-8")) > MAX_CONNECTION_CONFIG_BYTES:
            raise ServiceError("input_limit", 422)
        if len(body.selected_tools) > MAX_SELECTED_TOOLS:
            raise ServiceError("tool_limit", 422)
        secret = body.credentials

        async def operation(
            conn: AsyncConnection,
        ) -> tuple[dict[str, Any], tuple[str, int, str | None] | None]:
            row = (
                (
                    await conn.execute(
                        select(mcp_connections).where(
                            mcp_connections.c.id == connection_id,
                            mcp_connections.c.principal_id == actor.principal_id,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                raise ServiceError("not_found", 404)
            if row["revision"] != body.expected_revision:
                raise ServiceError("revision_conflict", 409)
            duplicate = await conn.scalar(
                select(mcp_connections.c.id).where(
                    mcp_connections.c.principal_id == actor.principal_id,
                    mcp_connections.c.name == body.name,
                    mcp_connections.c.id != connection_id,
                )
            )
            if duplicate is not None:
                raise ServiceError("revision_conflict", 409)
            credential_revision = row["credential_revision"]
            envelope = row["credential_envelope"]
            if body.credentials is not None:
                credential_revision += 1
                envelope = self._seal(
                    actor.principal_id,
                    connection_id,
                    credential_revision,
                    body.credentials,
                )
            next_revision = row["revision"] + 1
            await conn.execute(
                update(mcp_connections)
                .where(
                    mcp_connections.c.id == connection_id,
                    mcp_connections.c.principal_id == actor.principal_id,
                    mcp_connections.c.revision == body.expected_revision,
                )
                .values(
                    name=body.name,
                    config=config,
                    revision=next_revision,
                    credential_revision=credential_revision,
                    credential_envelope=envelope,
                    updated_at=time.time(),
                )
            )
            result = {
                "id": connection_id,
                "name": body.name,
                "revision": next_revision,
                "credential_configured": envelope is not None,
            }
            receipt = (
                (connection_id, credential_revision, envelope)
                if body.credentials is not None
                else None
            )
            return result, receipt

        return await self._mutate(
            actor, body, "update-connection:" + connection_id, operation, secret=secret
        )

    async def delete_connection(
        self, actor: PrincipalContext, connection_id: str, body: ConnectionDelete
    ) -> dict[str, Any]:
        if not connection_id or len(connection_id) > 64:
            raise ServiceError("not_found", 404)

        async def operation(
            conn: AsyncConnection,
        ) -> tuple[dict[str, Any], tuple[str, int, str | None] | None]:
            row = (
                (
                    await conn.execute(
                        select(mcp_connections).where(
                            mcp_connections.c.id == connection_id,
                            mcp_connections.c.principal_id == actor.principal_id,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                raise ServiceError("not_found", 404)
            if row["revision"] != body.expected_revision:
                raise ServiceError("revision_conflict", 409)
            await conn.execute(
                delete(mcp_connections).where(
                    mcp_connections.c.id == connection_id,
                    mcp_connections.c.principal_id == actor.principal_id,
                    mcp_connections.c.revision == body.expected_revision,
                )
            )
            selections = (
                (
                    await conn.execute(
                        select(session_mcp)
                        .join(sessions, session_mcp.c.session_id == sessions.c.id)
                        .where(sessions.c.owner_id == actor.principal_id)
                    )
                )
                .mappings()
                .all()
            )
            for selection in selections:
                if any(
                    isinstance(item, dict) and item.get("connection_id") == connection_id
                    for item in selection["connections"]
                ):
                    await conn.execute(
                        update(session_mcp)
                        .where(session_mcp.c.session_id == selection["session_id"])
                        .values(
                            connections=[
                                item
                                for item in selection["connections"]
                                if item.get("connection_id") != connection_id
                            ],
                            revision=selection["revision"] + 1,
                        )
                    )
            return {"id": connection_id, "deleted": True}, None

        return await self._mutate(actor, body, "delete-connection:" + connection_id, operation)

    async def session_selection(
        self,
        actor: PrincipalContext,
        workspace: str,
        session_id: str,
        body: SessionMCPSelection | None = None,
    ) -> dict[str, Any]:
        async def read(conn: AsyncConnection) -> dict[str, Any]:
            await self._session(conn, actor, workspace, session_id)
            row = (
                (
                    await conn.execute(
                        select(session_mcp).where(session_mcp.c.session_id == session_id)
                    )
                )
                .mappings()
                .first()
            )
            return (
                {
                    "session_id": session_id,
                    "connections": row["connections"],
                    "revision": row["revision"],
                }
                if row
                else {"session_id": session_id, "connections": [], "revision": 0}
            )

        if body is None:
            current: dict[str, Any] = await self._read(actor, read)
            return current

        async def write(
            conn: AsyncConnection,
        ) -> tuple[dict[str, Any], tuple[str, int, str | None] | None]:
            await self._session(conn, actor, workspace, session_id, write=True)
            current = await read(conn)
            if current["revision"] != body.expected_revision:
                raise ServiceError("revision_conflict", 409)
            for selected in body.connections:
                row = (
                    (
                        await conn.execute(
                            select(mcp_connections).where(
                                mcp_connections.c.id == selected.connection_id,
                                mcp_connections.c.principal_id == actor.principal_id,
                            )
                        )
                    )
                    .mappings()
                    .first()
                )
                if row is None:
                    raise ServiceError("not_found", 404)
                if row["revision"] != selected.revision:
                    raise ServiceError("revision_conflict", 409)
            connections = [item.model_dump(mode="json") for item in body.connections]
            total_tools = 0
            for selected in body.connections:
                row = (
                    (
                        await conn.execute(
                            select(mcp_connections).where(
                                mcp_connections.c.id == selected.connection_id,
                                mcp_connections.c.principal_id == actor.principal_id,
                            )
                        )
                    )
                    .mappings()
                    .first()
                )
                if row is None:
                    raise ServiceError("not_found", 404)
                total_tools += len(cast(dict[str, Any], row["config"])["selected_tools"])
            if total_tools > MAX_SELECTED_TOOLS:
                raise ServiceError("tool_limit", 422)
            values = {"connections": connections, "revision": current["revision"] + 1}
            if current["revision"] == 0:
                await conn.execute(insert(session_mcp).values(session_id=session_id, **values))
            else:
                await conn.execute(
                    update(session_mcp)
                    .where(session_mcp.c.session_id == session_id)
                    .values(**values)
                )
            return {
                "session_id": session_id,
                "revision": values["revision"],
                "connections": connections,
            }, None

        return await self._mutate(actor, body, "session-mcp:" + session_id, write)

    async def _session(
        self,
        conn: AsyncConnection,
        actor: PrincipalContext,
        workspace: str,
        session_id: str,
        *,
        write: bool = False,
    ) -> Any:
        membership = self.actor(actor).member(workspace, write=write)
        row = (
            (
                await conn.execute(
                    select(sessions).where(
                        sessions.c.id == session_id, sessions.c.workspace_id == workspace
                    )
                )
            )
            .mappings()
            .first()
        )
        if row is None or (row["owner_id"] != actor.principal_id and membership.role != "admin"):
            raise ServiceError("not_found", 404)
        if write and row["owner_id"] != actor.principal_id:
            raise ServiceError("permission_denied", 403)
        if write and row["status"] != "open":
            raise ServiceError("session_closed", 409)
        return row

    async def freeze(
        self, conn: AsyncConnection, session: Any, turn_id: str, actor: PrincipalContext
    ) -> None:
        self.actor(actor)
        if actor.principal_id != session["owner_id"]:
            snapshot: dict[str, Any] = {
                "version": "account-mcp/1",
                "principal_id": session["owner_id"],
                "connections": [],
            }
        else:
            row = (
                (
                    await conn.execute(
                        select(session_mcp).where(session_mcp.c.session_id == session["id"])
                    )
                )
                .mappings()
                .first()
            )
            selections = row["connections"] if row is not None else []
            connections: list[dict[str, Any]] = []
            for selection in selections:
                selected = SessionConnection.model_validate(selection)
                found = (
                    (
                        await conn.execute(
                            select(mcp_connections).where(
                                mcp_connections.c.id == selected.connection_id,
                                mcp_connections.c.principal_id == actor.principal_id,
                            )
                        )
                    )
                    .mappings()
                    .first()
                )
                if found is None:
                    raise ServiceError("facts_corrupt")
                if found["revision"] != selected.revision:
                    raise ServiceError("revision_conflict", 409)
                config = cast(dict[str, Any], found["config"])
                connections.append(
                    {
                        "id": found["id"],
                        "revision": found["revision"],
                        "credential_revision": found["credential_revision"],
                        "config": config,
                        "credential_envelope": found["credential_envelope"],
                    }
                )
            snapshot = {
                "version": "account-mcp/1",
                "principal_id": actor.principal_id,
                "selection_revision": row["revision"] if row else 0,
                "connections": connections,
            }
        if len(encoded(snapshot).encode("utf-8")) > 131072:
            raise ServiceError("input_limit", 422)
        total_tools = sum(
            len(cast(dict[str, Any], item["config"]).get("selected_tools", []))
            for item in snapshot["connections"]
        )
        if total_tools > MAX_SELECTED_TOOLS:
            raise ServiceError("tool_limit", 422)
        await conn.execute(
            insert(turn_mcp).values(
                turn_id=turn_id, snapshot=snapshot, fingerprint=fingerprint(snapshot)
            )
        )

    async def build_runtime(
        self, configs: tuple[MCPServerConfig, ...], base: ToolExecutor
    ) -> tuple[ToolExecutor, tuple[MCPConnection, ...]]:
        """Extend the selected executor without dropping its host pipeline."""
        base.registry.assert_current()
        if not configs:
            return base, ()
        specs = [spec for spec, _ in base.registry._frozen]
        connections: list[MCPConnection] = []
        try:
            for server in configs:
                connection = MCPConnection(server, egress_policy=_mcp_egress_policy(server))
                connections.append(connection)
                await connection.connect()
                specs.extend(mcp_tools(connection))
            selected_tools = len(specs) - len(base.registry._frozen)
            if selected_tools > MAX_SELECTED_TOOLS:
                raise ServiceError("tool_limit", 422)
            registry = ToolRegistry(tuple(specs), max_tools=MAX_TOOLS)
            account_names = frozenset(spec.name for spec in specs[len(base.registry._frozen) :])

            async def policy(invocation: Invocation) -> Decision:
                if invocation.name in account_names:
                    return "ask"
                if base.policy is None:
                    return "deny"
                return await base.policy(invocation)

            return (
                ToolExecutor(
                    registry, policy, base.pre, base.post,
                    middleware=base.middleware, wrap=base.wrap,
                ),
                tuple(connections),
            )
        except BaseException:
            await asyncio.gather(*(item.close() for item in connections), return_exceptions=True)
            raise

    async def configurations_for_turn(self, turn_id: str) -> tuple[MCPServerConfig, ...]:
        async with self.store.lock, self.store.engine.begin() as conn:
            row = await conn.execute(select(turn_mcp).where(turn_mcp.c.turn_id == turn_id))
            found = row.mappings().first()
        if found is None:
            return ()
        snapshot = found["snapshot"]
        if (
            type(snapshot) is not dict
            or fingerprint(cast(dict[str, Any], snapshot)) != found["fingerprint"]
            or snapshot.get("version") != "account-mcp/1"
        ):
            raise ServiceError("facts_corrupt")
        snapshot = cast(dict[str, Any], snapshot)
        owner = snapshot.get("principal_id")
        if not isinstance(owner, str):
            raise ServiceError("facts_corrupt")
        configs = snapshot.get("connections")
        if type(configs) is not list:
            raise ServiceError("facts_corrupt")
        result: list[MCPServerConfig] = []
        count = 0
        seen_names: set[str] = set()
        seen_ids: set[str] = set()
        for item in configs:
            if type(item) is not dict:
                raise ServiceError("facts_corrupt")
            config = item.get("config")
            connection_id = item.get("id")
            revision = item.get("credential_revision")
            if (
                not isinstance(connection_id, str)
                or type(revision) is not int
                or revision < 1
                or type(config) is not dict
                or not isinstance(item.get("credential_envelope"), str)
                and item.get("credential_envelope") is not None
            ):
                raise ServiceError("facts_corrupt")
            credentials = self._open_secret(
                owner, connection_id, revision, item.get("credential_envelope")
            )
            headers = {name: SecretStr(value) for name, value in credentials.items()}
            # JSON快照里的集合字段是list；strict模式只接受frozenset/tuple本体，
            # 在此处还原类型，其余内容仍交由原校验完整核对。
            try:
                server = MCPServerConfig.model_validate(
                    {
                        **cast(dict[str, Any], config),
                        "allowed_hosts": frozenset(
                            cast(list[str], config.get("allowed_hosts", ()))
                        ),
                        "selected_tools": tuple(cast(list[str], config.get("selected_tools", ()))),
                        "readonly_tools": frozenset(
                            cast(list[str], config.get("readonly_tools", ()))
                        ),
                        "headers": headers,
                    }
                )
            except (ValidationError, ValueError, TypeError):
                raise ServiceError("facts_corrupt") from None
            if (
                server.name != config.get("name")
                or server.transport != "streamable_http"
                or server.name in seen_names
                or connection_id in seen_ids
            ):
                raise ServiceError("facts_corrupt")
            seen_names.add(server.name)
            seen_ids.add(connection_id)
            count += len(server.selected_tools)
            if count > MAX_SELECTED_TOOLS:
                raise ServiceError("tool_limit", 422)
            result.append(server)
        if len(result) > MAX_ACCOUNT_SERVERS:
            raise ServiceError("facts_corrupt")
        return tuple(result)
