"""Reversible session archive/trash state and API behavior."""

from __future__ import annotations

import sqlite3

import pytest

from krim_agent.composition import open_application
from krim_agent.contracts import ServiceError
from krim_agent.security import LOCAL
from krim_agent.sessions.types import ControlRequest, SessionCreate
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.session_lifecycle_models import SESSION_LIFECYCLE_TABLES
from krim_agent.storage.sqlite import SCHEMA_VERSION, SQLiteStore
from tests.agent_core.test_account_api import client_for, initialize


async def exercise_closed_session_lifecycle(host) -> None:
    service, store = host.sessions, host.store
    assert service is not None
    actor, workspace = LOCAL, "local"
    session = await service.create(
        actor, workspace, SessionCreate(request_key="lifecycle-session")
    )
    close = await service.control(
        actor,
        workspace,
        session.id,
        "close",
        ControlRequest(request_key="lifecycle-close", expected_revision=0),
    )
    assert close.status == "completed"
    closed = await service.get(actor, workspace, session.id)
    assert closed.status == "closed"

    from krim_agent.storage.session_lifecycle import apply

    archived = await apply(
        store,
        principal_id=actor.principal_id,
        workspace=workspace,
        session_id=session.id,
        request_key="lifecycle-archive",
        expected_revision=0,
        action="archive",
    )
    assert archived.archived_at is not None and archived.deleted_at is None
    assert await service.list(actor, workspace) == []
    assert [item.id for item in await service.list(actor, workspace, view="archived")] == [
        session.id
    ]

    unarchived = await apply(
        store,
        principal_id=actor.principal_id,
        workspace=workspace,
        session_id=session.id,
        request_key="lifecycle-unarchive",
        expected_revision=archived.lifecycle_revision,
        action="unarchive",
    )
    assert unarchived.archived_at is None and unarchived.lifecycle_revision == 2
    assert [item.id for item in await service.list(actor, workspace)] == [session.id]

    trashed = await apply(
        store,
        principal_id=actor.principal_id,
        workspace=workspace,
        session_id=session.id,
        request_key="lifecycle-trash",
        expected_revision=unarchived.lifecycle_revision,
        action="trash",
    )
    assert trashed.deleted_at is not None and trashed.status == "closed"
    assert await service.list(actor, workspace) == []
    assert [item.id for item in await service.list(actor, workspace, view="trash")] == [
        session.id
    ]
    restored = await apply(
        store,
        principal_id=actor.principal_id,
        workspace=workspace,
        session_id=session.id,
        request_key="lifecycle-restore",
        expected_revision=trashed.lifecycle_revision,
        action="restore",
    )
    assert restored.deleted_at is None and restored.archived_at is None
    assert restored.status == "closed"
    assert [item.id for item in await service.list(actor, workspace)] == [session.id]
    assert await service.list(actor, workspace, view="trash") == []
    assert store.schema_version == SCHEMA_VERSION == 14


@pytest.mark.asyncio
async def test_archive_trash_restore_are_reversible_and_preserve_session(tmp_path):
    async with await open_application(tmp_path / "lifecycle.sqlite") as host:
        await exercise_closed_session_lifecycle(host)


@pytest.mark.asyncio
async def test_lifecycle_rejects_open_sessions_and_enforces_revision_idempotency(tmp_path):
    database = tmp_path / "lifecycle-open.sqlite"
    async with await open_application(database) as host:
        store, service = host.store, host.sessions
        assert service is not None
        session = await service.create(
            LOCAL, "local", SessionCreate(request_key="open-lifecycle")
        )
        from krim_agent.storage.session_lifecycle import apply

        with pytest.raises(ServiceError, match="session_must_be_closed"):
            await apply(
                store,
                principal_id=LOCAL.principal_id,
                workspace="local",
                session_id=session.id,
                request_key="open-archive",
                expected_revision=0,
                action="archive",
            )
        await service.control(
            LOCAL,
            "local",
            session.id,
            "close",
            ControlRequest(request_key="open-close", expected_revision=0),
        )
        result = await apply(
            store,
            principal_id=LOCAL.principal_id,
            workspace="local",
            session_id=session.id,
            request_key="retry-archive",
            expected_revision=0,
            action="archive",
        )
        replay = await apply(
            store,
            principal_id=LOCAL.principal_id,
            workspace="local",
            session_id=session.id,
            request_key="retry-archive",
            expected_revision=0,
            action="archive",
        )
        assert replay == result and result.lifecycle_revision == 1
        with pytest.raises(ServiceError, match="key_conflict"):
            await apply(
                store,
                principal_id=LOCAL.principal_id,
                workspace="local",
                session_id=session.id,
                request_key="retry-archive",
                expected_revision=0,
                action="trash",
            )
        with pytest.raises(ServiceError, match="revision_conflict"):
            await apply(
                store,
                principal_id=LOCAL.principal_id,
                workspace="local",
                session_id=session.id,
                request_key="stale-trash",
                expected_revision=0,
                action="trash",
            )


@pytest.mark.asyncio
async def test_session_lifecycle_api_lists_and_restores_trash(tmp_path):
    database = tmp_path / "lifecycle-api.sqlite"
    token = await initialize(database)
    async with client_for(database, token) as (app, client):
        actor = app.identities.authenticate("Bearer " + token)
        workspace = actor.memberships[0].workspace_id
        session = await app.sessions.create(
            actor, workspace, SessionCreate(request_key="lifecycle-api")
        )
        path = f"/v2/workspaces/{workspace}/sessions"
        close = await client.post(
            f"{path}/{session.id}/close",
            json={"request_key": "api-close", "expected_revision": 0},
        )
        assert close.status_code == 200, close.text
        trash = await client.post(
            f"{path}/{session.id}/lifecycle/trash",
            json={"request_key": "api-trash", "expected_revision": 0},
        )
        assert trash.status_code == 200, trash.text
        assert trash.json()["deleted_at"] is not None
        assert (await client.get(path)).json() == []
        contents = await client.get(path + "/lifecycle/trash")
        assert contents.status_code == 200, contents.text
        assert contents.json()[0]["id"] == session.id
        restore = await client.post(
            f"{path}/{session.id}/lifecycle/restore",
            json={"request_key": "api-restore", "expected_revision": 1},
        )
        assert restore.status_code == 200, restore.text
        assert restore.json()["deleted_at"] is None
        assert (await client.get(path)).json()[0]["id"] == session.id
        assert (await client.get(path + "/lifecycle/trash")).json() == []


@pytest.mark.asyncio
async def test_schema_13_readonly_then_upgrade_adds_only_lifecycle_tables(tmp_path):
    database = tmp_path / "lifecycle-migration.sqlite"
    async with await open_application(database) as host:
        session = await host.sessions.create(
            LOCAL, "local", SessionCreate(request_key="lifecycle-migration")
        )
        await host.close()
    with sqlite3.connect(database) as connection:
        for table in reversed(SESSION_LIFECYCLE_TABLES):
            connection.execute(f'DROP TABLE "{table.name}"')
        connection.execute("PRAGMA user_version=13")
        before = connection.execute("SELECT * FROM sessions").fetchall()
    unchanged = database.read_bytes()
    reader = await SQLiteStore.open(database, readonly=True)
    try:
        assert reader.schema_version == 13
        assert (await session_sql.get_session(reader, session.id)).id == session.id
    finally:
        await reader.close()
    assert database.read_bytes() == unchanged
    writer = await SQLiteStore.open(database)
    try:
        assert writer.schema_version == SCHEMA_VERSION == 14
        assert (await session_sql.get_session(writer, session.id)).id == session.id
        with sqlite3.connect(database) as connection:
            assert connection.execute("SELECT * FROM sessions").fetchall() == before
            tables = {
                row[0]
                for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
        assert {table.name for table in SESSION_LIFECYCLE_TABLES} <= tables
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_lifecycle_migration_preserves_historical_sandbox_tables(tmp_path):
    database = tmp_path / "lifecycle-sandbox-migration.sqlite"
    async with await open_application(database) as host:
        session = await host.sessions.create(
            LOCAL, "local", SessionCreate(request_key="lifecycle-sandbox-migration")
        )
        await host.close()
    with sqlite3.connect(database) as connection:
        for table in reversed(SESSION_LIFECYCLE_TABLES):
            connection.execute(f'DROP TABLE "{table.name}"')
        connection.execute("PRAGMA user_version=13")
        existing = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
    writer = await SQLiteStore.open(database)
    try:
        assert writer.schema_version == 14
        assert (await session_sql.get_session(writer, session.id)).id == session.id
        with sqlite3.connect(database) as connection:
            actual = {row[0] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
        assert existing <= actual
    finally:
        await writer.close()
