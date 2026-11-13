"""Schema upgrades preserve session sources and workspace safety gates."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import ServiceError
from krim_agent.evaluation.adapters.krim_source import KrimSourceReader
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.sqlite import SCHEMA_VERSION, SQLiteStore
from krim_agent.workspace import verification
from krim_agent.workspace.types import (
    Completion,
    ExecutionAccess,
    WorkspaceProfile,
    WorkspaceTurnRequest,
)


@pytest.mark.parametrize("version", [11, 12, 13, 14])
async def test_workspace_terminal_attempt_supports_feature_schema_versions(version):
    result = Mock()
    result.scalar_one_or_none.return_value = {"verification": {"schema": verification.SCHEMA}}
    conn = SimpleNamespace(
        execute=AsyncMock(return_value=result), scalar=AsyncMock(return_value=False)
    )
    store = SimpleNamespace(schema_version=version)
    attempt = await verification.terminal_attempt_same_conn(conn, store, "run-id", None)
    assert attempt is not None
    try:
        assert attempt.conn is conn and attempt.store is store and attempt.run_id == "run-id"
    finally:
        verification._TERMINAL_HELPERS[4](attempt)


@pytest.mark.parametrize("legacy", [False, True])
async def test_linked_session_source_works_after_current_schema_upgrade(tmp_path, legacy):
    async with await open_application(tmp_path / "source.sqlite") as app:
        assert app.store.schema_version == SCHEMA_VERSION == 14
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="source"))
        turn = await app.sessions.submit(
            LOCAL,
            "local",
            session.id,
            TurnRequest(
                text="/echo retained", request_key="turn", expected_revision=session.revision
            ),
        )
        finished = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
        assert finished.status == "completed" and finished.run_id is not None
        reader = KrimSourceReader(
            app.store,
            app.sessions,
            replace(LOCAL, legacy=legacy),
            workspace_id="local",
            scope="scope",
            allow_legacy=legacy,
        )
        source = await reader.read("scope", {"run_id": finished.run_id})
        assert source["source"]["session_id"] == session.id
        assert source["source"]["run_id"] == finished.run_id
        assert source["terminal"] is True


async def test_queued_workspace_requires_resource_before_recovery(tmp_path, monkeypatch):
    database = tmp_path / "queued-workspace.sqlite"
    profile = WorkspaceProfile(max_tools_per_step=1, environment={})
    store = await SQLiteStore.open(database)
    try:
        session = await session_sql.create_session(
            store,
            "local",
            LOCAL.principal_id,
            SessionCreate(request_key="workspace", target_key="target-workspace"),
            {"workspace_profile": profile.model_dump(mode="json")},
            ContextPolicy(),
            Settings(),
        )
        turn = await session_sql.submit(
            store,
            session.id,
            WorkspaceTurnRequest(
                request_key="queued", text="pending", expected_revision=0, completion=Completion()
            ),
            Settings(),
            access=ExecutionAccess(LOCAL, "local", session.id),
        )
        assert turn.status == "queued" and turn.run_id is None
    finally:
        await store.close()

    recovered = False
    original = SQLiteStore.recover

    async def tracked_recover(active_store):
        nonlocal recovered
        recovered = True
        await original(active_store)

    monkeypatch.setattr(SQLiteStore, "recover", tracked_recover)
    with pytest.raises(ServiceError, match="workspace_unavailable"):
        await open_application(database)
    assert recovered is False
    reader = await SQLiteStore.open(database, readonly=True)
    try:
        assert (await session_sql.get_session(reader, session.id)).dispatch_paused is False
        current = await session_sql.get_turn(reader, turn.id)
        assert current.status == "queued" and current.run_id is None
    finally:
        await reader.close()
