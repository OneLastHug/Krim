"""SQLite preview bindings, one-use credentials and nonrecursive route authority.

All decisions use short consistent database reads. No method calls Docker, E's
runtime checks or a network service while holding a transaction. Persistent
project services don't acquire a second execution approval when viewed.
"""

from __future__ import annotations

import hashlib
import math
import re
import time
from dataclasses import replace
from typing import TYPE_CHECKING, Any, Literal

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError, encoded
from krim_agent.dev_preview.repository import BrowserGrant, PreviewRecord
from krim_agent.dev_preview.service import binding, validate_record
from krim_agent.development.contracts import AuthorizationRef, PreviewHandle, ProcessHandle
from krim_agent.development.ports import ProcessRecord
from krim_agent.development.preview_ports import RouteBinding, ServiceInstance
from krim_agent.project_access.service import command_intent
from krim_agent.project_access.types import ProjectGrant
from krim_agent.security import Membership, PrincipalContext

from .development import DevelopmentStore, _process_record
from .development_models import development_processes
from .preview_models import preview_credentials as credentials
from .preview_models import preview_records as records
from .project_access import SQLiteAccessRepository
from .sandbox_models import sandbox_projects

if TYPE_CHECKING:
    from .sqlite import SQLiteStore

_ACTIVE = {"pending", "ready"}


def _document(record: PreviewRecord) -> dict[str, Any]:
    return {
        "handle": record.handle.model_dump(mode="json"),
        "process": record.process.model_dump(mode="json"),
        "authorization": record.authorization.model_dump(mode="json"),
        "instance": record.instance.model_dump(mode="json"),
        "request_digest": record.request_digest,
        "route_epoch": record.route_epoch,
        "revision": record.revision,
    }


def _record(raw: Any) -> PreviewRecord:
    try:
        value = raw["document"]
        record = PreviewRecord(
            handle=PreviewHandle.model_validate(value["handle"]),
            process=ProcessHandle.model_validate(value["process"]),
            authorization=AuthorizationRef.model_validate_json(encoded(value["authorization"])),
            instance=ServiceInstance.model_validate(value["instance"]),
            request_digest=value["request_digest"], route_epoch=value["route_epoch"],
            revision=value["revision"],
        )
        validate_record(record)
        if type(record.revision) is not int or type(record.route_epoch) is not int:
            raise ValueError("invalid preview revision")
        expected = _index(record)
        if any(raw[key] != result for key, result in expected.items()):
            raise ValueError("preview index mismatch")
        return record
    except (ValueError, KeyError, TypeError):
        raise ServiceError("facts_corrupt") from None


def _index(record: PreviewRecord) -> dict[str, Any]:
    handle = record.handle
    env = handle.environment
    return {
        "preview_id": handle.preview_id,
        "host_key": hashlib.sha256(handle.preview_id.encode()).hexdigest()[:40],
        "owner_id": env.owner_id, "project_id": env.project_id,
        "environment_id": env.environment_id, "generation": env.generation,
        "process_id": record.process.process_id, "port": handle.port,
        "state": handle.state, "revision": record.revision, "route_epoch": record.route_epoch,
    }


def _principal_json(value: PrincipalContext) -> dict[str, Any]:
    return {"principal_id": value.principal_id, "generation": value.generation,
            "memberships": [item.model_dump(mode="json") for item in value.memberships],
            "legacy": value.legacy}


def _principal(value: Any) -> PrincipalContext:
    try:
        if (not isinstance(value["principal_id"], str)
                or type(value["generation"]) is not int or value["generation"] < 1
                or type(value["legacy"]) is not bool):
            raise ValueError("invalid principal")
        return PrincipalContext(value["principal_id"], value["generation"],
                                tuple(Membership.model_validate(x) for x in value["memberships"]),
                                value["legacy"])
    except (KeyError, TypeError, ValueError):
        raise ServiceError("facts_corrupt") from None


class SQLitePreviewStore:
    def __init__(self, store: SQLiteStore) -> None:
        self.store = store
        self.development = DevelopmentStore(store)
        self.access = SQLiteAccessRepository(store)
        self.records = PreviewRecords(self)
        self.credentials = PreviewCredentials(self)
        self.authority = PreviewAuthority(self)

    def require(self, *, write: bool = False) -> None:
        if self.store.schema_version < 16:
            raise ServiceError("schema_upgrade_required")
        if self.store.closed or write and self.store.readonly:
            raise ServiceError("closed" if self.store.closed else "readonly")

    async def record(self, conn: AsyncConnection, preview_id: str) -> PreviewRecord:
        row = (await conn.execute(select(records).where(
            records.c.preview_id == preview_id,
        ))).mappings().first()
        if row is None:
            raise ServiceError("not_found", 404)
        return _record(row)

    async def process(self, conn: AsyncConnection, process: ProcessHandle) -> ProcessRecord:
        row = (await conn.execute(select(development_processes).where(
            development_processes.c.process_id == process.process_id,
        ))).mappings().first()
        if row is None:
            raise ServiceError("not_found", 404)
        try:
            current = _process_record(row)
            current.handle.require_binding(process)
        except ValueError:
            raise ServiceError("facts_corrupt") from None
        return current

    async def live(self, conn: AsyncConnection, record: PreviewRecord) -> ProcessRecord:
        validate_record(record)
        current = await self.process(conn, record.process)
        if (current.execution_id != record.instance.execution_id
                or current.request_digest != record.request_digest
                or current.request.authorization != record.authorization):
            raise ServiceError("tool_binding_changed")
        env = await self.development.current_environment(conn, record.handle.environment)
        if (env.environment.state != "ready" or current.handle.state != "running"
                or current.request.lifecycle != "project_service"):
            raise ServiceError("workspace_busy")
        return current


class PreviewRecords:
    def __init__(self, db: SQLitePreviewStore) -> None:
        self.db = db

    async def create(self, record: PreviewRecord) -> PreviewRecord:
        self.db.require(write=True)
        validate_record(record)
        if (type(record.revision) is not int or type(record.route_epoch) is not int
                or record.revision != 1 or record.route_epoch != 1
                or record.handle.state != "pending"):
            raise ServiceError("invalid_input", 422)

        async def write(conn: AsyncConnection) -> PreviewRecord:
            await self.db.live(conn, record)
            row = (await conn.execute(select(records.c.preview_id).where(
                (records.c.preview_id == record.handle.preview_id)
                | ((records.c.process_id == record.process.process_id)
                   & (records.c.port == record.handle.port)
                   & records.c.state.in_(_ACTIVE)),
            ))).first()
            if row is not None:
                raise ServiceError("key_conflict")
            await conn.execute(insert(records).values(
                **_index(record), document=_document(record),
                created_at=time.time(), updated_at=time.time(),
            ))
            return record

        async def confirm() -> PreviewRecord | None:
            found = await self.get(record.handle.preview_id)
            return found if found == record else None

        return await self.db.store._eval_write(write, confirm)

    async def get(self, preview_id: str) -> PreviewRecord:
        self.db.require()
        async with self.db.store.lock, self.db.store.engine.connect() as conn:
            return await self.db.record(conn, preview_id)

    async def get_by_host_key(self, key: str) -> PreviewRecord:
        self.db.require()
        if not re.fullmatch(r"[0-9a-f]{40}", key):
            raise ServiceError("not_found", 404)
        async with self.db.store.lock, self.db.store.engine.connect() as conn:
            row = (await conn.execute(select(records).where(
                records.c.host_key == key,
            ))).mappings().first()
            if row is None:
                raise ServiceError("not_found", 404)
            return _record(row)

    async def save(self, expected: PreviewRecord, updated: PreviewRecord) -> PreviewRecord:
        self.db.require(write=True)
        validate_record(expected)
        validate_record(updated)
        before, after = expected.handle.state, updated.handle.state
        allowed = {"pending": {"ready", "stopped", "revoked"},
                   "ready": {"stopped", "revoked"}, "stopped": {"revoked"}}
        epoch = expected.route_epoch + int(after in {"stopped", "revoked"})
        if (type(updated.revision) is not int or updated.revision != expected.revision + 1
                or type(updated.route_epoch) is not int
                or after not in allowed.get(before, set()) or updated.route_epoch != epoch
                or expected.handle.model_copy(update={"state": after}) != updated.handle
                or replace(expected, handle=updated.handle, revision=updated.revision,
                           route_epoch=updated.route_epoch) != updated):
            raise ServiceError("revision_conflict")

        async def write(conn: AsyncConnection) -> PreviewRecord:
            if await self.db.record(conn, expected.handle.preview_id) != expected:
                raise ServiceError("revision_conflict")
            if after in _ACTIVE:
                await self.db.live(conn, updated)
            await conn.execute(update(records).where(
                records.c.preview_id == expected.handle.preview_id,
                records.c.revision == expected.revision,
            ).values(**_index(updated), document=_document(updated), updated_at=time.time()))
            return updated

        async def confirm() -> PreviewRecord | None:
            found = await self.get(updated.handle.preview_id)
            return found if found == updated else None

        return await self.db.store._eval_write(write, confirm)


class PreviewAuthority:
    """Canonical accounts/grants, independent of E and B runtime callbacks."""

    def __init__(self, db: SQLitePreviewStore) -> None:
        self.db = db

    async def _check(
        self, conn: AsyncConnection, row: ProcessRecord, principal: PrincipalContext | None,
        *, write: bool = False,
    ) -> None:
        ref = row.request.authorization
        project = (await conn.execute(select(sandbox_projects).where(
            sandbox_projects.c.project_id == row.request.environment.project_id,
        ))).mappings().first()
        if (project is None or project["principal_id"] != row.request.ownership.owner_id
                or project["status"] != "ready"):
            raise ServiceError("permission_denied", 403)
        creator = await self.db.access._actor(conn, ref.actor_id)
        owner = project["principal_id"]
        workspace = project["workspace_id"]
        if creator is None or creator.generation != ref.actor_generation:
            raise ServiceError("permission_denied", 403)
        creator.owns(workspace, owner, write=True)
        grant = await self.db.access._record(
            conn, owner, project["project_id"], "grant", ref.actor_id,
        )
        if (not isinstance(grant, ProjectGrant) or grant.revoked
                or grant.expires_at <= time.time() or grant.actor_generation != creator.generation
                or grant.workspace_id != workspace or "shell.execute" not in grant.actions):
            raise ServiceError("permission_denied", 403)
        intent = command_intent(row.request, workspace_id=workspace)
        if intent.digest(ref) != ref.arguments_digest:
            raise ServiceError("tool_binding_changed")
        # The process has already passed command admission/approval. Do not ask
        # its originating run to remain running, or replay a one-use approval.
        if principal is not None:
            actor = await self.db.access._actor(conn, principal.principal_id)
            if actor is None or actor.generation != principal.generation:
                raise ServiceError("permission_denied", 403)
            actor.owns(workspace, owner, write=write)

    async def check(
        self, record: PreviewRecord, principal: PrincipalContext | None,
        action: Literal["register", "open", "serve", "stop"],
    ) -> None:
        self.db.require()
        if principal is None and action != "serve":
            raise ServiceError("permission_denied", 403)
        async with self.db.store.lock, self.db.store.engine.connect() as conn, conn.begin():
            if action != "register":
                current = await self.db.record(conn, record.handle.preview_id)
                if current != record:
                    raise ServiceError("tool_binding_changed")
                if action != "stop" and current.handle.state not in _ACTIVE:
                    raise ServiceError("permission_denied", 403)
            row = (await self.db.process(conn, record.process) if action == "stop"
                   else await self.db.live(conn, record))
            await self._check(conn, row, principal, write=action in {"register", "stop"})

    async def registration(
        self, process: ProcessHandle, port: int, principal: PrincipalContext,
    ) -> ProcessRecord:
        self.db.require()
        if type(port) is not int or not 1 <= port <= 65535 or principal is None:
            raise ServiceError("invalid_input", 422)
        async with self.db.store.lock, self.db.store.engine.connect() as conn, conn.begin():
            row = await self.db.process(conn, process)
            env = await self.db.development.current_environment(conn, row.request.environment)
            if (row.handle.state != "running" or row.request.lifecycle != "project_service"
                    or not row.execution_id or env.environment.state != "ready"):
                raise ServiceError("workspace_busy")
            await self._check(conn, row, principal, write=True)
            return row

    async def resolve(
        self, target: RouteBinding, principal: PrincipalContext | None,
    ) -> ProcessRecord:
        self.db.require()
        async with self.db.store.lock, self.db.store.engine.connect() as conn, conn.begin():
            record = await self.db.record(conn, target.preview_id)
            if record.handle.state not in _ACTIVE or binding(record) != target:
                raise ServiceError("tool_binding_changed")
            row = await self.db.live(conn, record)
            await self._check(conn, row, principal)
            return row


class PreviewCredentials:
    """Hashes only; consumed ticket tombstones prevent accidental key reuse."""

    def __init__(self, db: SQLitePreviewStore) -> None:
        self.db = db

    @staticmethod
    def _digest(value: str) -> None:
        if not re.fullmatch(r"[0-9a-f]{64}", value):
            raise ServiceError("invalid_input", 422)

    async def _put(self, digest: str, grant: BrowserGrant, kind: str) -> None:
        self.db.require(write=True)
        self._digest(digest)
        now = time.time()
        maximum = 30 if kind == "ticket" else 3600
        if not math.isfinite(grant.expires_at) or not now < grant.expires_at <= now + maximum:
            raise ServiceError("invalid_input", 422)
        principal = _principal_json(grant.principal)

        async def write(conn: AsyncConnection) -> bool:
            record = await self.db.record(conn, grant.preview_id)
            if record.handle.state not in _ACTIVE:
                raise ServiceError("permission_denied", 403)
            row = await self.db.live(conn, record)
            await self.db.authority._check(conn, row, grant.principal)
            if await conn.scalar(select(credentials.c.digest).where(
                credentials.c.digest == digest,
            )) is not None:
                raise ServiceError("key_conflict")
            await conn.execute(insert(credentials).values(
                digest=digest, kind=kind, preview_id=grant.preview_id, principal=principal,
                expires_at=grant.expires_at, consumed_at=None, created_at=now,
            ))
            return True

        async def confirm() -> bool | None:
            async with self.db.store.lock, self.db.store.engine.connect() as conn:
                row = (await conn.execute(select(credentials).where(
                    credentials.c.digest == digest,
                ))).mappings().first()
                return True if (row is not None and row["kind"] == kind
                                and row["preview_id"] == grant.preview_id
                                and row["principal"] == principal
                                and row["expires_at"] == grant.expires_at
                                and row["consumed_at"] is None) else None

        await self.db.store._eval_write(write, confirm)

    async def put_ticket(self, digest: str, grant: BrowserGrant) -> None:
        await self._put(digest, grant, "ticket")

    async def put_session(self, digest: str, grant: BrowserGrant) -> None:
        await self._put(digest, grant, "session")

    async def _get(
        self, conn: AsyncConnection, digest: str, preview_id: str, now: float, kind: str,
    ) -> BrowserGrant:
        self._digest(digest)
        if not math.isfinite(now):
            raise ServiceError("unauthorized", 401)
        row = (await conn.execute(select(credentials).where(
            credentials.c.digest == digest, credentials.c.kind == kind,
            credentials.c.preview_id == preview_id,
        ))).mappings().first()
        if (row is None or row["consumed_at"] is not None
                or row["expires_at"] <= max(now, time.time())):
            raise ServiceError("unauthorized", 401)
        record = await self.db.record(conn, preview_id)
        if record.handle.state not in _ACTIVE:
            raise ServiceError("permission_denied", 403)
        grant = BrowserGrant(preview_id, _principal(row["principal"]), row["expires_at"])
        current = await self.db.live(conn, record)
        await self.db.authority._check(conn, current, grant.principal)
        return grant

    async def consume_ticket(self, digest: str, preview_id: str, now: float) -> BrowserGrant:
        self.db.require(write=True)

        async def write(conn: AsyncConnection) -> BrowserGrant:
            grant = await self._get(conn, digest, preview_id, now, "ticket")
            changed = await conn.execute(update(credentials).where(
                credentials.c.digest == digest, credentials.c.consumed_at.is_(None),
            ).values(consumed_at=time.time()))
            if changed.rowcount != 1:
                raise ServiceError("unauthorized", 401)
            return grant

        async def uncertain() -> None:
            # A lost commit reply never licenses a second ticket acceptance.
            # The user can request a new ticket; the existing tombstone remains.
            return None

        return await self.db.store._eval_write(write, uncertain)

    async def get_session(self, digest: str, preview_id: str, now: float) -> BrowserGrant:
        self.db.require()
        async with self.db.store.lock, self.db.store.engine.connect() as conn, conn.begin():
            return await self._get(conn, digest, preview_id, now, "session")
