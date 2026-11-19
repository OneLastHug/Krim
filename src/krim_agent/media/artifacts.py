"""Immutable session-authorized media objects in the existing SQLite store.

No model-provided filesystem path is opened. Migration registration is owned by
the application host; unavailable tables are not silently created at runtime.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from typing import TYPE_CHECKING, Any, Protocol

from pydantic import JsonValue
from sqlalchemy import JSON, Column, Float, ForeignKey, LargeBinary, String, Table, insert, select
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError
from krim_agent.storage.models import metadata, sessions
from krim_agent.tools.session import SessionToolScope

from .formats import inspect_image
from .references import AuthorizedMedia, AuthorizedMediaRef, opaque_reference

if TYPE_CHECKING:
    from krim_agent.storage.sqlite import SQLiteStore

session_media_artifacts = Table(
    "session_media_artifacts", metadata,
    Column("reference", String(256), primary_key=True),
    Column("session_id", String(36), ForeignKey("sessions.id"), nullable=False),
    Column("owner_id", String(64), nullable=False),
    Column("workspace_id", String(64), nullable=False),
    Column("record", JSON, nullable=False),
    Column("provenance", JSON, nullable=False),
    Column("content", LargeBinary, nullable=False),
    Column("created_at", Float, nullable=False),
)
MEDIA_TABLES = (session_media_artifacts,)
MEDIA_TABLE_NAMES = frozenset(table.name for table in MEDIA_TABLES)


class MediaArtifactPort(Protocol):
    async def save(
        self, scope: SessionToolScope, data: bytes, media_type: str, *,
        path: str | None = None, provenance: dict[str, JsonValue] | None = None,
    ) -> AuthorizedMedia: ...

    async def read(self, scope: SessionToolScope, reference: str) -> AuthorizedMedia: ...


class SQLiteMediaArtifacts:
    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    async def _authorize(self, conn: AsyncConnection, scope: SessionToolScope) -> None:
        scope.check()
        scope.actor.member(scope.workspace_id)
        row = (await conn.execute(select(sessions.c.owner_id, sessions.c.workspace_id,
                                         sessions.c.status).where(
            sessions.c.id == scope.session_id,
        ))).mappings().first()
        if (row is None or row["owner_id"] != scope.actor.principal_id
                or row["workspace_id"] != scope.workspace_id):
            raise ServiceError("permission_denied", 403)
        if row["status"] != "open":
            raise ServiceError("session_closed", 409)

    async def save(
        self, scope: SessionToolScope, data: bytes, media_type: str, *,
        path: str | None = None, provenance: dict[str, JsonValue] | None = None,
    ) -> AuthorizedMedia:
        scope.check()
        inspect_image(data, media_type)
        frozen_provenance = json.loads(json.dumps(provenance or {}, allow_nan=False))
        reference = AuthorizedMediaRef(
            reference="media-" + uuid.uuid4().hex, owner_id=scope.actor.principal_id,
            workspace_id=scope.workspace_id, session_id=scope.session_id,
            project_id=scope.project_id, path=path, sha256=hashlib.sha256(data).hexdigest(),
            size=len(data), media_type=media_type,
        )
        value = AuthorizedMedia(reference, data)
        value.validate(scope)

        async def write(conn: AsyncConnection) -> AuthorizedMedia:
            await self._authorize(conn, scope)
            await conn.execute(insert(session_media_artifacts).values(
                reference=reference.reference, session_id=scope.session_id,
                owner_id=scope.actor.principal_id, workspace_id=scope.workspace_id,
                record=reference.model_dump(mode="json"), content=data,
                provenance=frozen_provenance, created_at=time.time(),
            ))
            return value

        async def confirm() -> AuthorizedMedia | None:
            try:
                found = await self.read(scope, reference.reference)
            except ServiceError as exc:
                if exc.code == "not_found":
                    return None
                raise
            if found != value:
                raise ServiceError("artifact_conflict")
            return found

        result = await self.store.work_write(write, confirm)
        scope.check()
        return result

    async def read(self, scope: SessionToolScope, reference: str) -> AuthorizedMedia:
        try:
            opaque_reference(reference)
        except ValueError:
            raise ServiceError("invalid_input", 422) from None

        async def read(conn: AsyncConnection) -> dict[str, Any]:
            await self._authorize(conn, scope)
            row = (await conn.execute(select(session_media_artifacts).where(
                session_media_artifacts.c.reference == reference,
                session_media_artifacts.c.session_id == scope.session_id,
                session_media_artifacts.c.owner_id == scope.actor.principal_id,
                session_media_artifacts.c.workspace_id == scope.workspace_id,
            ))).mappings().first()
            if row is None:
                raise ServiceError("not_found", 404)
            return dict(row)

        row = await self.store.work_read(read)
        scope.check()
        try:
            ref = AuthorizedMediaRef.model_validate(row["record"])
            if ref.reference != reference:
                raise ValueError
        except ValueError:
            raise ServiceError("artifact_invalid") from None
        result = AuthorizedMedia(ref, row["content"])
        result.validate(scope)
        inspect_image(result.data, ref.media_type)
        return result
