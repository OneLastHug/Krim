"""Current project authorization from the existing SQLite identity/run store.

The guard uses a short immediate transaction for delivery acceptance. It does not
hold a transaction while a project command, API request or Git process runs.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError, encoded, fingerprint, parse_json
from krim_agent.development.contracts import AuthorizationRef, OperationIdentity
from krim_agent.project_access.git import GitGrant, GitOperation
from krim_agent.project_access.repository import AccessSnapshot
from krim_agent.project_access.types import (
    AccessIntent,
    ApprovalBinding,
    NetworkGrant,
    ProjectGrant,
    SecretRecord,
)
from krim_agent.security import Membership, PrincipalContext

from .account_models import access_keys, account_workspaces, accounts
from .development_models import development_operations
from .development_models import project_access_records as records
from .models import run_events, runs
from .sandbox_models import sandbox_projects

if TYPE_CHECKING:
    from .sqlite import SQLiteStore

AccessRecord = ProjectGrant | SecretRecord | NetworkGrant | GitGrant


def _key(record: AccessRecord) -> tuple[str, str]:
    if isinstance(record, ProjectGrant):
        return "grant", record.actor_id
    if isinstance(record, SecretRecord):
        return "secret", record.secret_id
    if isinstance(record, NetworkGrant):
        return "network", record.policy_id
    return "git", fingerprint(record.repository)


def _document(record: AccessRecord) -> dict[str, Any]:
    result = record.model_dump(mode="json")
    if isinstance(record, SecretRecord):
        # Explicitly persist authenticated ciphertext, never the plaintext.
        result["ciphertext"] = record.ciphertext
    return result


class SQLiteAccessRepository:
    """Trusted storage adapter; public callers must first bind the authenticated actor."""

    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    def require(self) -> None:
        if self.store.schema_version < 15:
            raise ServiceError("schema_upgrade_required")
        if self.store.closed or self.store.readonly:
            raise ServiceError("closed" if self.store.closed else "readonly")

    async def put(self, record: AccessRecord, *, expected_revision: int) -> int:
        """I management path supplies the authorized domain value and CAS revision."""
        self.require()
        kind, key = _key(record)
        revision = expected_revision + 1
        if (
            expected_revision < 0
            or isinstance(expected_revision, bool)
            or isinstance(record, (ProjectGrant, SecretRecord, NetworkGrant))
            and record.revision != revision
        ):
            raise ServiceError("revision_conflict")
        document = _document(record)

        async def write(conn: AsyncConnection) -> int:
            project = (
                (
                    await conn.execute(
                        select(sandbox_projects).where(
                            sandbox_projects.c.project_id == record.project_id,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if (
                project is None
                or project["principal_id"] != record.owner_id
                or isinstance(record, ProjectGrant)
                and record.workspace_id != project["workspace_id"]
            ):
                raise ServiceError("permission_denied")
            where = (
                records.c.project_id == record.project_id,
                records.c.kind == kind,
                records.c.resource_id == key,
            )
            previous = (await conn.execute(select(records).where(*where))).mappings().first()
            if previous is None:
                if expected_revision != 0:
                    raise ServiceError("revision_conflict")
                await conn.execute(
                    insert(records).values(
                        project_id=record.project_id,
                        owner_id=record.owner_id,
                        kind=kind,
                        resource_id=key,
                        revision=revision,
                        document=document,
                        updated_at=time.time(),
                    )
                )
            else:
                if (
                    previous["revision"] != expected_revision
                    or previous["owner_id"] != record.owner_id
                ):
                    raise ServiceError("revision_conflict")
                await conn.execute(
                    update(records)
                    .where(*where)
                    .values(
                        revision=revision,
                        document=document,
                        updated_at=time.time(),
                    )
                )
            return revision

        async def confirm() -> int | None:
            async with self.store.lock, self.store.engine.connect() as conn:
                row = (
                    (
                        await conn.execute(
                            select(records).where(
                                records.c.project_id == record.project_id,
                                records.c.owner_id == record.owner_id,
                                records.c.kind == kind,
                                records.c.resource_id == key,
                            )
                        )
                    )
                    .mappings()
                    .first()
                )
                return (
                    revision
                    if (
                        row is not None
                        and row["revision"] == revision
                        and row["document"] == document
                    )
                    else None
                )

        return await self.store._eval_write(write, confirm)

    async def _record(
        self,
        conn: AsyncConnection,
        owner: str,
        project: str,
        kind: str,
        key: str,
    ) -> AccessRecord | None:
        row = (
            (
                await conn.execute(
                    select(records).where(
                        records.c.owner_id == owner,
                        records.c.project_id == project,
                        records.c.kind == kind,
                        records.c.resource_id == key,
                    )
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            return None
        models: dict[str, type[ProjectGrant] | type[SecretRecord]
                     | type[NetworkGrant] | type[GitGrant]] = {
            "grant": ProjectGrant,
            "secret": SecretRecord,
            "network": NetworkGrant,
            "git": GitGrant,
        }
        value = models[kind].model_validate_json(encoded(row["document"]))
        if (
            value.owner_id != owner
            or value.project_id != project
            or _key(value) != (kind, key)
            or isinstance(value, (ProjectGrant, SecretRecord, NetworkGrant))
            and value.revision != row["revision"]
        ):
            raise ServiceError("facts_corrupt")
        return value

    async def _actor(self, conn: AsyncConnection, actor_id: str) -> PrincipalContext | None:
        account = (
            (
                await conn.execute(
                    select(accounts).where(
                        accounts.c.principal_id == actor_id,
                    )
                )
            )
            .mappings()
            .first()
        )
        active_key = await conn.scalar(
            select(access_keys.c.key_id)
            .where(
                access_keys.c.principal_id == actor_id,
                access_keys.c.status == "active",
            )
            .limit(1)
        )
        if account is None or account["status"] != "active" or active_key is None:
            return None
        memberships = (
            (
                await conn.execute(
                    select(account_workspaces).where(
                        account_workspaces.c.principal_id == actor_id,
                    )
                )
            )
            .mappings()
            .all()
        )
        return PrincipalContext(
            actor_id,
            account["authorization_generation"],
            tuple(
                Membership(
                    workspace_id=row["workspace_id"], role=row["role"], approve=row["approve"]
                )
                for row in memberships
            ),
            account["legacy"],
        )

    async def _approval(
        self,
        conn: AsyncConnection,
        ref: AuthorizationRef,
        intent: AccessIntent,
    ) -> ApprovalBinding | None:
        if ref.approval_id is None:
            return None
        identity_raw = (
            await conn.execute(
                select(development_operations.c.identity).where(
                    development_operations.c.operation_id == intent.operation_id,
                    development_operations.c.owner_id == intent.owner_id,
                    development_operations.c.project_id == intent.project_id,
                )
            )
        ).scalar_one_or_none()
        if identity_raw is None:
            return None
        identity = OperationIdentity.model_validate(identity_raw)
        run = (await conn.execute(select(runs.c.status, runs.c.stop_requested).where(
            runs.c.id == identity.ownership.run_id,
        ))).mappings().first()
        if (identity.operation_id != intent.operation_id
                or identity.ownership.owner_id != intent.owner_id
                or identity.environment.project_id != intent.project_id
                or run is None or run["stop_requested"] or run["status"] != "running"):
            return None
        events = (
            (
                await conn.execute(
                    select(run_events.c.kind, run_events.c.data)
                    .where(
                        run_events.c.run_id == identity.ownership.run_id,
                        run_events.c.kind.in_(["approval_request", "approval_reply"]),
                    )
                    .order_by(run_events.c.seq)
                )
            )
            .mappings()
            .all()
        )
        request = next(
            (
                row["data"]
                for row in events
                if row["kind"] == "approval_request"
                and row["data"].get("request_id") == ref.approval_id
            ),
            None,
        )
        reply = next(
            (
                row["data"]
                for row in events
                if row["kind"] == "approval_reply"
                and row["data"].get("request_id") == ref.approval_id
            ),
            None,
        )
        # Older unscoped approvals cannot silently authorize new project actions.
        if (
            request is None
            or reply is None
            or request.get("call_id") != identity.call_id
            or not isinstance(request.get("project_access"), dict)
        ):
            return None
        body = reply.get("reply")
        if not isinstance(body, dict) or any(
            body.get(key) != request.get(key) for key in ("call_id", "arguments_hash", "tool_hash")
        ):
            return None
        try:
            return ApprovalBinding.model_validate_json(
                encoded(
                    {
                        **request["project_access"],
                        "approval_id": ref.approval_id,
                        "expires_at": request["expires_at"],
                        "approved": body.get("allow") is True,
                    }
                )
            )
        except (ValueError, KeyError):
            return None

    @asynccontextmanager
    async def guard(
        self,
        ref: AuthorizationRef,
        intent: AccessIntent,
    ) -> AsyncIterator[AccessSnapshot]:
        self.require()
        if ref.project_id != intent.project_id:
            raise ServiceError("permission_denied")
        async with self.store.lock, self.store.engine.connect() as conn:
            await conn.execution_options(krim_immediate=True)
            async with conn.begin():
                project = (
                    (
                        await conn.execute(
                            select(sandbox_projects).where(
                                sandbox_projects.c.project_id == intent.project_id,
                            )
                        )
                    )
                    .mappings()
                    .first()
                )
                if (
                    project is None
                    or project["principal_id"] != intent.owner_id
                    or project["workspace_id"] != intent.workspace_id
                ):
                    raise ServiceError("permission_denied")
                actor = await self._actor(conn, ref.actor_id)
                grant = await self._record(
                    conn, intent.owner_id, intent.project_id, "grant", ref.actor_id
                )
                secrets = []
                for key in ref.secret_refs:
                    value = await self._record(
                        conn, intent.owner_id, intent.project_id, "secret", key
                    )
                    if isinstance(value, SecretRecord):
                        secrets.append(value)
                network = (
                    await self._record(
                        conn, intent.owner_id, intent.project_id, "network", ref.network_policy_ref
                    )
                    if ref.network_policy_ref
                    else None
                )
                git = None
                if intent.action.startswith("git."):
                    arguments = parse_json(intent.arguments_json)
                    if not isinstance(arguments, dict) or not isinstance(
                        arguments.get("git"), dict
                    ):
                        raise ServiceError("invalid_input", 422)
                    operation = GitOperation.model_validate_json(encoded(arguments["git"]))
                    git = await self._record(
                        conn,
                        intent.owner_id,
                        intent.project_id,
                        "git",
                        fingerprint(operation.repository),
                    )
                yield AccessSnapshot(
                    actor,
                    grant if isinstance(grant, ProjectGrant) else None,
                    await self._approval(conn, ref, intent),
                    tuple(secrets),
                    network if isinstance(network, NetworkGrant) else None,
                    git if isinstance(git, GitGrant) else None,
                )
