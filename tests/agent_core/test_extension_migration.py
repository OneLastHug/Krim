import sqlite3

import pytest

from krim_agent.contracts import RunRequest, ServiceError
from krim_agent.storage.extension_models import EXTENSION_TABLE_NAMES
from krim_agent.storage.sqlite import MCP_TABLE_NAMES, SCHEMA_VERSION, SQLiteStore


def table_names(path):
    with sqlite3.connect(path) as conn:
        return {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def rows(path, name):
    with sqlite3.connect(path) as conn:
        return conn.execute(f'SELECT * FROM "{name}"').fetchall()


MCP_V12_TABLE_NAMES = {"mcp_connections", "session_mcp", "turn_mcp", "mcp_requests"}


def test_mcp_schema12_metadata_when_integrated():
    if not MCP_TABLE_NAMES:
        pytest.skip("The independent account-MCP branch is not merged into this checkout")
    assert MCP_TABLE_NAMES == MCP_V12_TABLE_NAMES


# Historical table sets are explicit so this fixture does not share the production
# ladder's metadata subtraction and cannot accidentally validate the same mistake.
# With MCP absent, v12 exercises only the contract-table boundary; the test above
# identifies that checkout as lacking the full MCP-v12 integration coverage.
HISTORICAL_TABLES = {
    1: {"runs", "run_events"},
}
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
    (11, EXTENSION_TABLE_NAMES),
    (12, MCP_V12_TABLE_NAMES if MCP_TABLE_NAMES else set()),
    (13, {"sandbox_turn_contracts"}),
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


def downgrade_to_10(path):
    downgrade_to_version(path, 10)


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
async def test_fresh_schema13_registers_extension_and_sandbox_contract_tables(tmp_path):
    database = tmp_path / "fresh.sqlite"
    store = await SQLiteStore.open(database)
    try:
        assert SCHEMA_VERSION == 13
        assert store.schema_version == 13
        assert EXTENSION_TABLE_NAMES <= table_names(database)
        assert "sandbox_turn_contracts" in table_names(database)
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_schema12_upgrade_adds_sandbox_contract_table_and_preserves_data(tmp_path):
    database = tmp_path / "upgrade.sqlite"
    run = await seed_run(database)
    before = rows(database, "runs")
    downgrade_to_version(database, 12)

    store = await SQLiteStore.open(database)
    try:
        assert store.schema_version == SCHEMA_VERSION
        assert EXTENSION_TABLE_NAMES <= table_names(database)
        assert "sandbox_turn_contracts" in table_names(database)
        assert rows(database, "runs") == before
    finally:
        await store.close()
    assert run.id == before[0][0]


@pytest.mark.asyncio
async def test_readonly_schema12_does_not_migrate(tmp_path):
    database = tmp_path / "readonly12.sqlite"
    await seed_run(database)
    downgrade_to_version(database, 12)
    before = database.read_bytes()

    store = await SQLiteStore.open(database, readonly=True)
    try:
        assert store.schema_version == 12
        assert "sandbox_turn_contracts" not in table_names(database)
        assert EXTENSION_TABLE_NAMES <= table_names(database)
    finally:
        await store.close()
    assert database.read_bytes() == before


@pytest.mark.asyncio
@pytest.mark.parametrize("version", range(1, 14))
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
        assert writer.schema_version == SCHEMA_VERSION == 13
        assert "sandbox_turn_contracts" in table_names(database)
        assert table_names(database) == HISTORICAL_TABLES[13]
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
        assert reader.schema_version == 13
        assert (await reader.snapshot(run.id)).run.id == run.id
    finally:
        await reader.close()


@pytest.mark.asyncio
async def test_readonly_schema13_works(tmp_path):
    database = tmp_path / "readonly13.sqlite"
    writer = await SQLiteStore.open(database)
    await writer.close()
    before = database.read_bytes()

    reader = await SQLiteStore.open(database, readonly=True)
    try:
        assert reader.schema_version == SCHEMA_VERSION == 13
        assert EXTENSION_TABLE_NAMES <= table_names(database)
        assert "sandbox_turn_contracts" in table_names(database)
    finally:
        await reader.close()
    assert database.read_bytes() == before


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "extra"])
async def test_invalid_extension_table_set_is_rejected(tmp_path, damage):
    database = tmp_path / f"invalid-{damage}.sqlite"
    writer = await SQLiteStore.open(database)
    await writer.close()
    with sqlite3.connect(database) as conn:
        if damage == "missing":
            conn.execute('DROP TABLE "skills"')
        else:
            conn.execute("CREATE TABLE extension_surprise(value TEXT)")
        conn.commit()
    before = database.read_bytes()

    with pytest.raises(ServiceError, match="database_invalid"):
        await SQLiteStore.open(database)
    assert database.read_bytes() == before
