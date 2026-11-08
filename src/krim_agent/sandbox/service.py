"""账号环境、项目归属与配额管理；执行后端未就绪时不伪报已限额。"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any, cast

from pydantic import BaseModel, JsonValue, ValidationError
from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.accounts.service import IdentityService
from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.runtime.contracts import settled
from krim_agent.sandbox.broker_client import BrokerClient
from krim_agent.sandbox.configuration import SandboxConfiguration
from krim_agent.sandbox.models import ConfigureQuotaBody, CreateProjectBody
from krim_agent.security import PrincipalContext
from krim_agent.storage.account_models import access_keys, account_workspaces, accounts
from krim_agent.storage.sandbox_models import (
    sandbox_environments as environments,
)
from krim_agent.storage.sandbox_models import (
    sandbox_operations as operations,
)
from krim_agent.storage.sandbox_models import (
    sandbox_projects as projects,
)
from krim_agent.storage.sqlite import SQLiteStore


class SandboxService:
    def __init__(
        self, store: SQLiteStore, identities: IdentityService, configuration: SandboxConfiguration
    ) -> None:
        if not identities.managed:
            raise ServiceError("accounts_not_initialized", 422)
        if store.schema_version < 8:
            raise ServiceError("schema_upgrade_required")
        self.store = store
        self.identities = identities
        self.configuration = SandboxConfiguration.model_validate_json(
            configuration.model_dump_json(exclude_unset=True)
        )
        self.broker = (
            BrokerClient(configuration.broker_socket, owner_uid=configuration.broker_owner_uid)
            if configuration.broker_socket is not None
            else None
        )
        self.closed = False

    async def close(self) -> None:
        self.closed = True

    def _live(self, actor: PrincipalContext) -> PrincipalContext:
        if self.closed:
            raise ServiceError("closed")
        return self.identities.validate(actor)

    async def _authorize(
        self, conn: AsyncConnection, actor: PrincipalContext, *, admin: bool = False
    ) -> None:
        self._live(actor)
        row = (
            (
                await conn.execute(
                    select(accounts).where(accounts.c.principal_id == actor.principal_id)
                )
            )
            .mappings()
            .first()
        )
        if (
            row is None
            or row["status"] != "active"
            or row["authorization_generation"] != actor.generation
        ):
            raise ServiceError("unauthorized", 401)
        keyed = await conn.scalar(
            select(access_keys.c.key_id)
            .where(
                access_keys.c.principal_id == actor.principal_id, access_keys.c.status == "active"
            )
            .limit(1)
        )
        if keyed is None:
            raise ServiceError("unauthorized", 401)
        if admin and row["platform_role"] != "platform_admin":
            raise ServiceError("permission_denied", 403)

    async def _read[T](
        self,
        actor: PrincipalContext,
        read: Callable[[AsyncConnection], Awaitable[T]],
        *,
        admin: bool = False,
    ) -> T:
        self._live(actor)
        try:
            async with self.store.lock, self.store.engine.begin() as conn:
                await self._authorize(conn, actor, admin=admin)
                return await read(conn)
        except ServiceError:
            raise
        except Exception:
            raise ServiceError("storage_unavailable", 503) from None

    @staticmethod
    def _parse[T: BaseModel](model: type[T], value: object) -> T:
        try:
            return model.model_validate(value)
        except (ValidationError, TypeError):
            raise ServiceError("invalid_input", 422) from None

    def _limits(self, row: Any | None) -> dict[str, Any]:
        try:
            quota = self.configuration.quotas.resolve(
                None if row is None else row["profile_name"],
                None if row is None else row["quota_override"],
            )
        except (ValueError, KeyError):
            raise ServiceError("sandbox_config_invalid", 422) from None
        return quota.model_dump(mode="json")

    def _environment(self, principal_id: str, row: Any | None) -> dict[str, Any]:
        return {
            "principal_id": principal_id,
            "environment_id": None if row is None else row["environment_id"],
            "revision": 0 if row is None else row["revision"],
            "status": "unprovisioned" if row is None else row["status"],
            "profile_name": None if row is None else row["profile_name"],
            "overrides": {} if row is None else row["quota_override"],
            "limits": self._limits(row),
            "usage": None,
            "enforcement": (
                "xfs_project_quota"
                if row is not None and row["status"] == "ready"
                else "not_provisioned"
            ),
            "runtime_available": False,
        }

    async def environment(
        self,
        actor: PrincipalContext,
        principal_id: str | None = None,
        *,
        require_admin: bool = False,
    ) -> dict[str, Any]:
        target = principal_id or actor.principal_id

        async def read(conn: AsyncConnection) -> dict[str, Any]:
            if (
                await conn.scalar(
                    select(accounts.c.principal_id).where(accounts.c.principal_id == target)
                )
                is None
            ):
                raise ServiceError("not_found", 404)
            row = (
                (
                    await conn.execute(
                        select(environments).where(environments.c.principal_id == target)
                    )
                )
                .mappings()
                .first()
            )
            return self._environment(target, row)

        result = await self._read(actor, read, admin=require_admin or target != actor.principal_id)
        if result["status"] == "ready":
            if self.broker is None:
                raise ServiceError("sandbox_backend_unavailable", 503)
            live = await self.broker.call("usage", environment_id=result["environment_id"])
            await self._read(actor, read, admin=require_admin or target != actor.principal_id)
            usage = live["usage"]
            result["usage"] = {
                "environment_bytes": usage["environment_bytes"],
                "environment_inodes": usage["environment_inodes"],
            }
            result["applied_limits"] = {
                "environment_bytes": usage["byte_limit"],
                "environment_inodes": usage["inode_limit"],
            }
            if self.configuration.command_image is not None:
                health = await self.broker.call("health")
                result["runtime_available"] = (
                    health.get("execution_available") is True
                    and health.get("image") == self.configuration.command_image
                )
                await self._read(actor, read, admin=require_admin or target != actor.principal_id)
        if self.store.schema_version >= 10:
            from krim_agent.storage.sandbox_models import sandbox_quota_state

            async def quota_state(conn: AsyncConnection) -> Any:
                return (
                    (
                        await conn.execute(
                            select(sandbox_quota_state).where(
                                sandbox_quota_state.c.principal_id == target
                            )
                        )
                    )
                    .mappings()
                    .first()
                )

            state = await self._read(
                actor, quota_state, admin=require_admin or target != actor.principal_id
            )
            if state is not None:
                result.update(
                    quota_state=state["state"],
                    desired_limits=state["desired"]["limits"],
                    desired_revision=state["desired_revision"],
                    applied_revision=state["applied_revision"],
                    quota_operation_id=state["operation_id"],
                )
        return result

    async def _visible_result(
        self, conn: AsyncConnection, actor: PrincipalContext, kind: str, result: dict[str, Any]
    ) -> None:
        if kind == "configure_quota" and result.get("principal_id") != actor.principal_id:
            await self._authorize(conn, actor, admin=True)
        if result.get("session_id") is not None:
            from krim_agent.sandbox.bindings import session_row

            await session_row(conn, actor, result["session_id"])
        project_id = result.get("project_id")
        if project_id is not None:
            row = (
                (
                    await conn.execute(
                        select(projects).where(
                            projects.c.project_id == project_id,
                            projects.c.principal_id == actor.principal_id,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if (
                row is None
                or await conn.scalar(
                    select(account_workspaces.c.workspace_id).where(
                        account_workspaces.c.principal_id == actor.principal_id,
                        account_workspaces.c.workspace_id == row["workspace_id"],
                    )
                )
                is None
            ):
                raise ServiceError("not_found", 404)

    async def _mutation(
        self,
        actor: PrincipalContext,
        request_key: str,
        payload: dict[str, JsonValue],
        perform: Callable[[AsyncConnection], Awaitable[dict[str, Any]]],
        *,
        admin: bool = False,
    ) -> dict[str, Any]:
        self._live(actor)
        if (
            type(request_key) is not str
            or not 1 <= len(request_key) <= 128
            or any(ord(c) < 33 or ord(c) > 126 for c in request_key)
        ):
            raise ServiceError("invalid_input", 422)
        digest = fingerprint(payload)
        prior_health = self.store.healthy

        async def lookup(conn: AsyncConnection) -> Any:
            return (
                (
                    await conn.execute(
                        select(operations).where(
                            operations.c.principal_id == actor.principal_id,
                            operations.c.request_key == request_key,
                        )
                    )
                )
                .mappings()
                .first()
            )

        async def write(conn: AsyncConnection) -> dict[str, Any]:
            await self._authorize(conn, actor, admin=admin)
            previous = await lookup(conn)
            if previous is not None:
                if previous["request_hash"] != digest:
                    raise ServiceError("key_conflict")
                if previous["status"] != "completed":
                    raise ServiceError("sandbox_operation_unknown")
                await self._visible_result(conn, actor, previous["kind"], previous["result"])
                return cast(dict[str, Any], previous["result"])
            result = await perform(conn)
            now = time.time()
            await conn.execute(
                insert(operations).values(
                    operation_id=str(uuid.uuid4()),
                    principal_id=actor.principal_id,
                    request_key=request_key,
                    request_hash=digest,
                    project_id=result.get("project_id"),
                    kind=payload["action"],
                    status="completed",
                    result=result,
                    created_at=now,
                    updated_at=now,
                )
            )
            return result

        try:
            return await settled(self.store._write(write))
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if isinstance(error, ServiceError) and error.code != "storage_unavailable":
                raise

            # 提交回程未知：只读取原键，不重新执行 perform。
            async def recover(conn: AsyncConnection) -> dict[str, Any] | None:
                found = await lookup(conn)
                if (
                    found is not None
                    and found["request_hash"] == digest
                    and found["status"] == "completed"
                ):
                    await self._visible_result(conn, actor, found["kind"], found["result"])
                    return cast(dict[str, Any], found["result"])
                return None

            try:
                found = await self._read(actor, recover, admin=admin)
                if found is not None:
                    self.store.healthy = prior_health
                    return found
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            raise ServiceError("sandbox_operation_unknown", 503) from None

    async def configure_quota(
        self, actor: PrincipalContext, principal_id: str, body: object, *, request_key: str
    ) -> dict[str, Any]:
        values = self._parse(ConfigureQuotaBody, body)
        patch = values.overrides.model_dump(mode="json", exclude_unset=True)
        try:
            self.configuration.quotas.resolve(values.profile_name, values.overrides)
        except (ValueError, KeyError):
            raise ServiceError("sandbox_config_invalid", 422) from None

        async def perform(conn: AsyncConnection) -> dict[str, Any]:
            if (
                await conn.scalar(
                    select(accounts.c.principal_id).where(accounts.c.principal_id == principal_id)
                )
                is None
            ):
                raise ServiceError("not_found", 404)
            row = (
                (
                    await conn.execute(
                        select(environments).where(environments.c.principal_id == principal_id)
                    )
                )
                .mappings()
                .first()
            )
            current_revision = 0 if row is None else row["revision"]
            if current_revision != values.expected_revision:
                raise ServiceError("revision_conflict")
            # Broker 的 apply 协议接入前，不修改已运行环境的硬配额。
            if row is not None and row["status"] != "unprovisioned":
                raise ServiceError("sandbox_backend_unavailable", 503)
            now = time.time()
            if row is None:
                await conn.execute(
                    insert(environments).values(
                        environment_id=str(uuid.uuid4()),
                        principal_id=principal_id,
                        profile_name=values.profile_name,
                        quota_override=patch,
                        revision=1,
                        status="unprovisioned",
                        created_at=now,
                        updated_at=now,
                    )
                )
            else:
                await conn.execute(
                    update(environments)
                    .where(
                        environments.c.environment_id == row["environment_id"],
                        environments.c.revision == current_revision,
                    )
                    .values(
                        profile_name=values.profile_name,
                        quota_override=patch,
                        revision=current_revision + 1,
                        updated_at=now,
                    )
                )
            saved = (
                (
                    await conn.execute(
                        select(environments).where(environments.c.principal_id == principal_id)
                    )
                )
                .mappings()
                .one()
            )
            return self._environment(principal_id, saved)

        return await self._mutation(
            actor,
            request_key,
            {
                "action": "configure_quota",
                "target": principal_id,
                "body": values.model_dump(mode="json", exclude_unset=True),
            },
            perform,
            admin=True,
        )

    @staticmethod
    def _project(row: Any) -> dict[str, Any]:
        return {
            name: row[name]
            for name in (
                "project_id",
                "environment_id",
                "principal_id",
                "workspace_id",
                "title",
                "generation",
                "status",
                "created_at",
                "updated_at",
            )
        }

    async def create_project(
        self, actor: PrincipalContext, body: object, *, request_key: str
    ) -> dict[str, Any]:
        values = self._parse(CreateProjectBody, body)

        async def perform(conn: AsyncConnection) -> dict[str, Any]:
            member = (
                (
                    await conn.execute(
                        select(account_workspaces).where(
                            account_workspaces.c.principal_id == actor.principal_id,
                            account_workspaces.c.workspace_id == values.workspace_id,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if member is None:
                raise ServiceError("not_found", 404)
            if member["role"] == "viewer":
                raise ServiceError("permission_denied", 403)
            row = (
                (
                    await conn.execute(
                        select(environments).where(
                            environments.c.principal_id == actor.principal_id
                        )
                    )
                )
                .mappings()
                .first()
            )
            project_count = await conn.scalar(
                select(func.count())
                .select_from(projects)
                .where(projects.c.principal_id == actor.principal_id)
            )
            if (
                project_count is None
                or project_count >= self.configuration.max_projects_per_account
            ):
                raise ServiceError("sandbox_limit")
            now = time.time()
            if row is None:
                environment_id = str(uuid.uuid4())
                await conn.execute(
                    insert(environments).values(
                        environment_id=environment_id,
                        principal_id=actor.principal_id,
                        profile_name=None,
                        quota_override={},
                        revision=1,
                        status="unprovisioned",
                        created_at=now,
                        updated_at=now,
                    )
                )
            else:
                self._limits(row)
                environment_id = row["environment_id"]
            project_id = str(uuid.uuid4())
            await conn.execute(
                insert(projects).values(
                    project_id=project_id,
                    environment_id=environment_id,
                    principal_id=actor.principal_id,
                    workspace_id=values.workspace_id,
                    title=values.title,
                    generation=0,
                    status="unprovisioned",
                    lease_id=None,
                    lease_expires_at=None,
                    created_at=now,
                    updated_at=now,
                )
            )
            saved = (
                (await conn.execute(select(projects).where(projects.c.project_id == project_id)))
                .mappings()
                .one()
            )
            return self._project(saved)

        return await self._mutation(
            actor,
            request_key,
            {
                "action": "create_project",
                "body": values.model_dump(mode="json"),
            },
            perform,
        )

    async def list_projects(
        self, actor: PrincipalContext, *, offset: int = 0, limit: int = 20
    ) -> dict[str, Any]:
        if (
            type(offset) is not int
            or not 0 <= offset <= 100000
            or type(limit) is not int
            or not 1 <= limit <= 100
        ):
            raise ServiceError("invalid_input", 422)

        async def read(conn: AsyncConnection) -> dict[str, Any]:
            visible = select(account_workspaces.c.workspace_id).where(
                account_workspaces.c.principal_id == actor.principal_id
            )
            conditions = (
                projects.c.principal_id == actor.principal_id,
                projects.c.workspace_id.in_(visible),
            )
            total = await conn.scalar(select(func.count()).select_from(projects).where(*conditions))
            rows = (
                await conn.execute(
                    select(projects)
                    .where(*conditions)
                    .order_by(projects.c.created_at, projects.c.project_id)
                    .offset(offset)
                    .limit(limit)
                )
            ).mappings()
            return {
                "items": [self._project(row) for row in rows],
                "total": total,
                "offset": offset,
                "limit": limit,
            }

        return await self._read(actor, read)

    async def project(
        self, actor: PrincipalContext, project_id: str, *, write: bool = False
    ) -> dict[str, Any]:
        async def read(conn: AsyncConnection) -> dict[str, Any]:
            row = (
                (
                    await conn.execute(
                        select(projects).where(
                            projects.c.project_id == project_id,
                            projects.c.principal_id == actor.principal_id,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                raise ServiceError("not_found", 404)
            if (
                await conn.scalar(
                    select(account_workspaces.c.workspace_id).where(
                        account_workspaces.c.principal_id == actor.principal_id,
                        account_workspaces.c.workspace_id == row["workspace_id"],
                    )
                )
                is None
            ):
                raise ServiceError("not_found", 404)
            if write:
                actor.member(row["workspace_id"], write=True)
            return self._project(row)

        return await self._read(actor, read)

    async def operation(self, actor: PrincipalContext, request_key: str) -> dict[str, Any]:
        async def read(conn: AsyncConnection) -> dict[str, Any]:
            row = (
                (
                    await conn.execute(
                        select(operations).where(
                            operations.c.principal_id == actor.principal_id,
                            operations.c.request_key == request_key,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                raise ServiceError("not_found", 404)
            await self._visible_result(conn, actor, row["kind"], row["result"])
            return {
                "operation_id": row["operation_id"],
                "request_key": row["request_key"],
                "kind": row["kind"],
                "status": row["status"],
                "result": row["result"],
            }

        return await self._read(actor, read)

    async def provision(
        self, actor: PrincipalContext, *, request_key: str, project_id: str | None = None
    ) -> dict[str, Any]:
        """持久化准备意图后调用Broker；这些准备动作按内部ID可核实地幂等。"""
        if self.broker is None:
            raise ServiceError("sandbox_backend_unavailable", 503)
        self._live(actor)
        if (
            type(request_key) is not str
            or not 1 <= len(request_key) <= 128
            or any(ord(c) < 33 or ord(c) > 126 for c in request_key)
        ):
            raise ServiceError("invalid_input", 422)
        if project_id is not None:
            await self.project(actor, project_id, write=True)
        digest = fingerprint({"action": "provision", "project_id": project_id})

        async def reserve(conn: AsyncConnection) -> dict[str, Any]:
            await self._authorize(conn, actor)
            existing = (
                (
                    await conn.execute(
                        select(operations).where(
                            operations.c.principal_id == actor.principal_id,
                            operations.c.request_key == request_key,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if existing is not None:
                if existing["request_hash"] != digest:
                    raise ServiceError("key_conflict")
                await self._visible_result(conn, actor, "provision", existing["result"])
                if existing["status"] != "completed":
                    accepted = existing["result"]
                    current = (
                        (
                            await conn.execute(
                                select(environments).where(
                                    environments.c.environment_id == accepted["environment_id"]
                                )
                            )
                        )
                        .mappings()
                        .one()
                    )
                    if (
                        current["revision"] != accepted["revision"]
                        or self._limits(current) != accepted["limits"]
                    ):
                        raise ServiceError("revision_conflict")
                return dict(existing)
            now = time.time()
            env = (
                (
                    await conn.execute(
                        select(environments).where(
                            environments.c.principal_id == actor.principal_id
                        )
                    )
                )
                .mappings()
                .first()
            )
            if env is None:
                environment_id = str(uuid.uuid4())
                await conn.execute(
                    insert(environments).values(
                        environment_id=environment_id,
                        principal_id=actor.principal_id,
                        profile_name=None,
                        quota_override={},
                        revision=1,
                        status="unprovisioned",
                        created_at=now,
                        updated_at=now,
                    )
                )
                env = (
                    (
                        await conn.execute(
                            select(environments).where(
                                environments.c.environment_id == environment_id
                            )
                        )
                    )
                    .mappings()
                    .one()
                )
            intent = {
                "environment_id": env["environment_id"],
                "project_id": project_id,
                "limits": self._limits(env),
                "revision": env["revision"],
            }
            if project_id is not None:
                project = (
                    (
                        await conn.execute(
                            select(projects).where(
                                projects.c.project_id == project_id,
                                projects.c.principal_id == actor.principal_id,
                            )
                        )
                    )
                    .mappings()
                    .first()
                )
                if project is None or project["environment_id"] != env["environment_id"]:
                    raise ServiceError("not_found", 404)
                actor.member(project["workspace_id"], write=True)
                if project["lease_id"] is not None or project["status"] not in {
                    "ready",
                    "unprovisioned",
                }:
                    raise ServiceError("workspace_busy")
                await conn.execute(
                    update(projects)
                    .where(projects.c.project_id == project_id)
                    .values(status="provisioning", updated_at=now)
                )
            operation_id = str(uuid.uuid4())
            await conn.execute(
                update(environments)
                .where(environments.c.environment_id == env["environment_id"])
                .values(status="provisioning")
            )
            await conn.execute(
                insert(operations).values(
                    operation_id=operation_id,
                    principal_id=actor.principal_id,
                    request_key=request_key,
                    request_hash=digest,
                    project_id=project_id,
                    kind="provision",
                    status="pending",
                    result=intent,
                    created_at=now,
                    updated_at=now,
                )
            )
            return {"operation_id": operation_id, "status": "pending", "result": intent}

        record = await settled(self.store._write(reserve))
        if record["status"] == "completed":
            return cast(dict[str, Any], record["result"])
        intent = record["result"]
        self._live(actor)
        await self.broker.call(
            "provision_environment",
            environment_id=intent["environment_id"],
            limits=intent["limits"],
        )
        if project_id is not None:
            self._live(actor)
            await self.broker.call(
                "provision_project", environment_id=intent["environment_id"], project_id=project_id
            )

        async def finish(conn: AsyncConnection) -> dict[str, Any]:
            await self._authorize(conn, actor)
            env = (
                (
                    await conn.execute(
                        select(environments).where(
                            environments.c.environment_id == intent["environment_id"]
                        )
                    )
                )
                .mappings()
                .one()
            )
            if env["revision"] != intent["revision"]:
                raise ServiceError("revision_conflict")
            if project_id is not None:
                project = (
                    (
                        await conn.execute(
                            select(projects).where(projects.c.project_id == project_id)
                        )
                    )
                    .mappings()
                    .one()
                )
                actor.member(project["workspace_id"], write=True)
                await conn.execute(
                    update(projects)
                    .where(projects.c.project_id == project_id)
                    .values(status="ready", updated_at=time.time())
                )
            await conn.execute(
                update(environments)
                .where(environments.c.environment_id == intent["environment_id"])
                .values(status="ready", updated_at=time.time())
            )
            result = {
                "environment_id": intent["environment_id"],
                "project_id": project_id,
                "status": "ready",
                "enforcement": "xfs_project_quota",
                "runtime_available": False,
            }
            await conn.execute(
                update(operations)
                .where(operations.c.operation_id == record["operation_id"])
                .values(status="completed", result=result, updated_at=time.time())
            )
            return result

        return await settled(self.store._write(finish))

    async def directory(
        self,
        actor: PrincipalContext,
        project_id: str,
        *,
        path: str = ".",
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        project = await self.project(actor, project_id)
        if self.broker is None or project["status"] not in {
            "ready",
            "running",
            "blocked",
            "restoring",
        }:
            raise ServiceError("sandbox_backend_unavailable", 503)
        result = await self.broker.call(
            "list",
            environment_id=project["environment_id"],
            project_id=project_id,
            path=path,
            limit=limit,
            offset=offset,
        )
        self._live(actor)
        current = await self.project(actor, project_id)
        return {
            **result,
            "project_id": project_id,
            "generation": current["generation"],
            "may_change": (
                project["status"] in {"running", "blocked", "restoring"}
                or current["status"] in {"running", "blocked", "restoring"}
                or current["generation"] != project["generation"]
            ),
        }

    async def read_file(
        self,
        actor: PrincipalContext,
        project_id: str,
        *,
        path: str,
        offset: int = 0,
        limit: int = 65536,
    ) -> dict[str, Any]:
        project = await self.project(actor, project_id)
        if self.broker is None or project["status"] not in {
            "ready",
            "running",
            "blocked",
            "restoring",
        }:
            raise ServiceError("sandbox_backend_unavailable", 503)
        result = await self.broker.call(
            "read",
            environment_id=project["environment_id"],
            project_id=project_id,
            path=path,
            offset=offset,
            limit=limit,
        )
        self._live(actor)
        current = await self.project(actor, project_id)
        return {
            **result,
            "project_id": project_id,
            "generation": current["generation"],
            "may_change": (
                project["status"] in {"running", "blocked", "restoring"}
                or current["status"] in {"running", "blocked", "restoring"}
                or current["generation"] != project["generation"]
            ),
        }

    async def reconcile_command(
        self,
        actor: PrincipalContext,
        project_id: str,
        operation_id: str,
        *,
        expected_generation: int,
        confirmed: bool,
    ) -> dict[str, Any]:
        """用户明确核实原任务；不提供command参数，不按超时自动夺锁。"""
        if type(expected_generation) is not int or confirmed is not True:
            raise ServiceError("invalid_input", 422)
        project = await self.project(actor, project_id, write=True)
        if self.broker is None or project["generation"] != expected_generation:
            raise ServiceError("input_revision_conflict")

        async def accepted(conn: AsyncConnection) -> Any:
            await self._authorize(conn, actor)
            row = (
                (
                    await conn.execute(
                        select(operations).where(
                            operations.c.operation_id == operation_id,
                            operations.c.principal_id == actor.principal_id,
                            operations.c.project_id == project_id,
                            operations.c.kind == "command",
                        )
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                raise ServiceError("not_found", 404)
            return dict(row)

        original = await self._read(actor, accepted)
        if original["status"] == "completed" and project["lease_id"] is None:
            return cast(dict[str, Any], original["result"]["receipt"])
        if project["lease_id"] != operation_id:
            raise ServiceError("sandbox_cleanup_required")
        result = await self.broker.call(
            "command_reconcile",
            environment_id=project["environment_id"],
            project_id=project_id,
            operation_id=operation_id,
        )
        if result["receipt"]["cleanup"] != "clean":
            raise ServiceError("sandbox_cleanup_required")
        receipt = {
            **result["receipt"],
            "operation_id": operation_id,
            "project_id": project_id,
            "generation": result["generation"],
            "verification": "not_evaluated",
        }

        async def finish(conn: AsyncConnection) -> dict[str, Any]:
            await self._authorize(conn, actor)
            current = (
                (await conn.execute(select(projects).where(projects.c.project_id == project_id)))
                .mappings()
                .one()
            )
            if current["generation"] != expected_generation or current["lease_id"] != operation_id:
                raise ServiceError("input_revision_conflict")
            await conn.execute(
                update(projects)
                .where(projects.c.project_id == project_id)
                .values(
                    status="ready",
                    generation=result["generation"],
                    lease_id=None,
                    lease_expires_at=None,
                    updated_at=time.time(),
                )
            )
            await conn.execute(
                update(operations)
                .where(operations.c.operation_id == operation_id)
                .values(
                    status="completed",
                    result={**original["result"], "receipt": receipt},
                    updated_at=time.time(),
                )
            )
            return receipt

        return await settled(self.store._write(finish))

    async def search_files(
        self,
        actor: PrincipalContext,
        project_id: str,
        *,
        query: str,
        path: str = ".",
        limit: int = 50,
    ) -> dict[str, Any]:
        project = await self.project(actor, project_id)
        if self.broker is None or project["status"] not in {
            "ready",
            "running",
            "blocked",
            "restoring",
        }:
            raise ServiceError("sandbox_backend_unavailable", 503)
        result = await self.broker.call(
            "search",
            environment_id=project["environment_id"],
            project_id=project_id,
            query=query,
            path=path,
            limit=limit,
        )
        current = await self.project(actor, project_id)
        return {
            **result,
            "project_id": project_id,
            "generation": current["generation"],
            "may_change": (
                project["status"] in {"running", "blocked", "restoring"}
                or current["status"] in {"running", "blocked", "restoring"}
                or current["generation"] != project["generation"]
            ),
        }
