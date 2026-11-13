import sqlite3

import pytest

from krim_agent.contracts import RunRequest, ServiceError
from krim_agent.storage.extension_models import EXTENSION_TABLE_NAMES
from krim_agent.storage.mcp_models import MCP_TABLE_NAMES
from krim_agent.storage.session_lifecycle_models import SESSION_LIFECYCLE_TABLE_NAMES
from krim_agent.storage.sqlite import SCHEMA_VERSION, SQLiteStore


def table_names(path):
    with sqlite3.connect(path) as conn:
        return {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def rows(path, name):
    with sqlite3.connect(path) as conn:
        return conn.execute(f'SELECT * FROM "{name}"').fetchall()


MCP_V12_TABLE_NAMES = {"mcp_connections", "session_mcp", "turn_mcp", "mcp_requests"}


def test_integrated_feature_metadata():
    assert MCP_TABLE_NAMES == MCP_V12_TABLE_NAMES
    assert SESSION_LIFECYCLE_TABLE_NAMES == {"session_lifecycle", "session_lifecycle_requests"}


# Historical sets are explicit and independent of the production subtraction ladder.
HISTORICAL_TABLES = {1: {"runs", "run_events"}}
for version, additions in (
    (2, {"dataset_revisions", "evaluations", "eval_items"}),
    (3, {"sessions", "turns", "session_controls"}),
    (4, {"work_sessions", "work_turns", "compactions", "tool_effects", "artifact_revisions"}),
    (5, {"capture_meta", "capture_policies", "observation_outbox"}),
    (6, {"park_checkpoints"}),
    (
        7,
        {
            "accounts",
            "access_keys",
            "account_workspaces",
            "workspace_profiles",
            "account_events",
            "account_meta",
        },
    ),
    (8, {"sandbox_environments", "sandbox_projects", "sandbox_operations"}),
    (9, {"sandbox_session_bindings"}),
    (10, {"sandbox_quota_state"}),
    (
        11,
        {
            "skills",
            "skill_versions",
            "memories",
            "memory_preferences",
            "session_extensions",
            "turn_extensions",
            "extension_requests",
        },
    ),
    (12, MCP_V12_TABLE_NAMES),
    (13, {"sandbox_turn_contracts"}),
    (14, {"session_lifecycle", "session_lifecycle_requests"}),
):
    HISTORICAL_TABLES[version] = HISTORICAL_TABLES[version - 1] | additions


def downgrade_to_version(path, version):
    with sqlite3.connect(path) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        names = {
            row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        for name in sorted(names - HISTORICAL_TABLES[version]):
            conn.execute(f'DROP TABLE "{name}"')
        conn.execute(f"PRAGMA user_version={version}")


async def seed_run(path):
    store = await SQLiteStore.open(path)
    try:
        run, created = await store.begin(
            RunRequest(text="保留的运行", request_key="extension-migration"),
            {"source": "legacy"},
        )
        assert created is True
        return run
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_fresh_schema14_registers_all_feature_tables(tmp_path):
    database = tmp_path / "fresh.sqlite"
    store = await SQLiteStore.open(database)
    try:
        assert store.schema_version == SCHEMA_VERSION == 14
        assert table_names(database) == HISTORICAL_TABLES[14]
        assert EXTENSION_TABLE_NAMES <= table_names(database)
    finally:
        await store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [11, 12, 13])
async def test_feature_upgrade_preserves_data(tmp_path, version):
    database = tmp_path / f"feature-{version}.sqlite"
    run = await seed_run(database)
    before = rows(database, "runs")
    downgrade_to_version(database, version)
    assert table_names(database) == HISTORICAL_TABLES[version]
    store = await SQLiteStore.open(database)
    try:
        assert store.schema_version == SCHEMA_VERSION == 14
        assert table_names(database) == HISTORICAL_TABLES[14]
        assert rows(database, "runs") == before
        assert (await store.snapshot(run.id)).request.text == "保留的运行"
    finally:
        await store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("version", range(1, 15))
async def test_all_historical_versions_readonly_then_upgrade(tmp_path, version):
    database = tmp_path / f"historical-v{version}.sqlite"
    run = await seed_run(database)
    downgrade_to_version(database, version)
    expected = HISTORICAL_TABLES[version]
    assert table_names(database) == expected
    before_rows = {name: rows(database, name) for name in expected}
    with sqlite3.connect(database) as conn:
        before_schema = dict(conn.execute("SELECT name, sql FROM sqlite_master WHERE type='table'"))
    before_bytes = database.read_bytes()

    reader = await SQLiteStore.open(database, readonly=True)
    try:
        assert reader.schema_version == version
        assert (await reader.snapshot(run.id)).request.text == "保留的运行"
        assert table_names(database) == expected
    finally:
        await reader.close()
    assert database.read_bytes() == before_bytes

    writer = await SQLiteStore.open(database)
    try:
        assert writer.schema_version == SCHEMA_VERSION == 14
        assert table_names(database) == HISTORICAL_TABLES[14]
        assert (await writer.snapshot(run.id)).request.text == "保留的运行"
        for name in expected:
            assert rows(database, name) == before_rows[name]
        with sqlite3.connect(database) as conn:
            after_schema = dict(
                conn.execute("SELECT name, sql FROM sqlite_master WHERE type='table'")
            )
        assert {name: after_schema[name] for name in expected} == before_schema
    finally:
        await writer.close()

    reader = await SQLiteStore.open(database, readonly=True)
    try:
        assert reader.schema_version == 14
        assert (await reader.snapshot(run.id)).run.id == run.id
    finally:
        await reader.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "version,table",
    [
        (11, "skills"),
        (12, "mcp_connections"),
        (13, "sandbox_turn_contracts"),
        (14, "session_lifecycle"),
    ],
)
@pytest.mark.parametrize("damage", ["missing", "extra"])
async def test_invalid_historical_table_set_is_rejected(tmp_path, version, table, damage):
    database = tmp_path / f"invalid-v{version}-{damage}.sqlite"
    await seed_run(database)
    downgrade_to_version(database, version)
    with sqlite3.connect(database) as conn:
        if damage == "missing":
            conn.execute(f'DROP TABLE "{table}"')
        else:
            conn.execute("CREATE TABLE extension_surprise(value TEXT)")
    before = database.read_bytes()
    with pytest.raises(ServiceError, match="database_invalid"):
        await SQLiteStore.open(database)
    assert database.read_bytes() == before
