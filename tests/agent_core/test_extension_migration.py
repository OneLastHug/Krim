import sqlite3

import pytest

from krim_agent.contracts import RunRequest, ServiceError
from krim_agent.storage.extension_models import EXTENSION_TABLE_NAMES, EXTENSION_TABLES
from krim_agent.storage.sqlite import SCHEMA_VERSION, SQLiteStore


def table_names(path):
    with sqlite3.connect(path) as conn:
        return {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def rows(path, name):
    with sqlite3.connect(path) as conn:
        return conn.execute(f'SELECT * FROM "{name}"').fetchall()


def downgrade_to_10(path):
    with sqlite3.connect(path) as conn:
        for table in reversed(EXTENSION_TABLES):
            conn.execute(f'DROP TABLE "{table.name}"')
        conn.execute("PRAGMA user_version=10")


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
async def test_fresh_schema11_registers_extension_tables(tmp_path):
    database = tmp_path / "fresh.sqlite"
    store = await SQLiteStore.open(database)
    try:
        assert SCHEMA_VERSION == 11
        assert store.schema_version == 11
        assert EXTENSION_TABLE_NAMES <= table_names(database)
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_schema10_upgrade_adds_extension_tables_and_preserves_data(tmp_path):
    database = tmp_path / "upgrade.sqlite"
    run = await seed_run(database)
    before = rows(database, "runs")
    downgrade_to_10(database)

    store = await SQLiteStore.open(database)
    try:
        assert store.schema_version == SCHEMA_VERSION
        assert EXTENSION_TABLE_NAMES <= table_names(database)
        assert rows(database, "runs") == before
    finally:
        await store.close()
    assert run.id == before[0][0]


@pytest.mark.asyncio
async def test_readonly_schema10_does_not_migrate(tmp_path):
    database = tmp_path / "readonly10.sqlite"
    await seed_run(database)
    downgrade_to_10(database)
    before = database.read_bytes()

    store = await SQLiteStore.open(database, readonly=True)
    try:
        assert store.schema_version == 10
        assert not (EXTENSION_TABLE_NAMES & table_names(database))
    finally:
        await store.close()
    assert database.read_bytes() == before


@pytest.mark.asyncio
async def test_readonly_schema11_works(tmp_path):
    database = tmp_path / "readonly11.sqlite"
    writer = await SQLiteStore.open(database)
    await writer.close()
    before = database.read_bytes()

    reader = await SQLiteStore.open(database, readonly=True)
    try:
        assert reader.schema_version == SCHEMA_VERSION == 11
        assert EXTENSION_TABLE_NAMES <= table_names(database)
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
