"""沙箱schema升级：只读门、引用约束与既有账号/工作区数据保留。"""

import sqlite3

import pytest

from krim_agent.accounts import IdentityService
from krim_agent.contracts import RunRequest, ServiceError
from krim_agent.storage import sessions as session_sql
from krim_agent.storage import work as work_sql
from krim_agent.storage.account_models import ACCOUNT_TABLE_NAMES
from krim_agent.storage.models import metadata
from krim_agent.storage.sandbox_models import SANDBOX_TABLE_NAMES
from krim_agent.storage.sqlite import SCHEMA_VERSION, SQLiteStore


def raw(path, names):
    conn = sqlite3.connect(path)
    try:
        return {name: conn.execute(f'SELECT * FROM "{name}"').fetchall() for name in names}
    finally:
        conn.close()


def table_names(path):
    conn = sqlite3.connect(path)
    try:
        return {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    finally:
        conn.close()


def downgrade(path, version):
    """从当前完整表集逐层删除到目标版本的白名单集合；只读门与升级测试共用。"""
    conn = sqlite3.connect(path)
    try:
        conn.execute('DROP TABLE "sandbox_quota_state"')
        conn.execute('DROP TABLE "sandbox_session_bindings"')
        for name in ("sandbox_operations", "sandbox_projects", "sandbox_environments"):
            conn.execute(f'DROP TABLE "{name}"')
        if version < 7:
            for name in sorted(ACCOUNT_TABLE_NAMES):
                conn.execute(f'DROP TABLE "{name}"')
        if version < 6:
            conn.execute('DROP TABLE "park_checkpoints"')
        if version < 5:
            for name in ("observation_outbox", "capture_policies", "capture_meta"):
                conn.execute(f'DROP TABLE "{name}"')
        if version < 4:
            for name in (
                "work_sessions",
                "work_turns",
                "compactions",
                "tool_effects",
                "artifact_revisions",
            ):
                conn.execute(f'DROP TABLE "{name}"')
        if version < 3:
            for name in ("session_controls", "turns", "sessions"):
                conn.execute(f'DROP TABLE "{name}"')
        if version < 2:
            for name in ("eval_items", "evaluations", "dataset_revisions"):
                conn.execute(f'DROP TABLE "{name}"')
        conn.execute(f"PRAGMA user_version={version}")
        conn.commit()
    finally:
        conn.close()


async def seeded(tmp_path, name, request_key):
    store = await SQLiteStore.open(tmp_path / name)
    view, created = await store.begin(RunRequest(text="保留", request_key=request_key), {"v": 1})
    assert created is True
    await store.close()
    return view


async def test_fresh_store_has_current_schema_with_sandbox_tables(tmp_path):
    database = tmp_path / "fresh.sqlite"
    store = await SQLiteStore.open(database)
    try:
        assert SCHEMA_VERSION == 10
        assert store.schema_version == SCHEMA_VERSION
        assert SANDBOX_TABLE_NAMES <= set(metadata.tables)
        assert SANDBOX_TABLE_NAMES <= table_names(database)
        session_sql.require(store)
        work_sql.require(store)
    finally:
        await store.close()


async def test_sandbox_table_shapes_match_spec(tmp_path):
    database = tmp_path / "shape.sqlite"
    store = await SQLiteStore.open(database)
    await store.close()
    conn = sqlite3.connect(database)
    try:

        def columns(name):
            return {row[1]: row for row in conn.execute(f'PRAGMA table_info("{name}")')}

        def uniques(name):
            found = set()
            for row in conn.execute(f'PRAGMA index_list("{name}")').fetchall():
                if row[2]:
                    found.add(
                        tuple(
                            r[2] for r in conn.execute(f'PRAGMA index_info("{row[1]}")').fetchall()
                        )
                    )
            return found

        assert [row[1] for row in conn.execute('PRAGMA table_info("sandbox_environments")')] == [
            "environment_id",
            "principal_id",
            "profile_name",
            "quota_override",
            "revision",
            "status",
            "created_at",
            "updated_at",
        ]
        env = columns("sandbox_environments")
        assert env["environment_id"][5] == 1 and env["profile_name"][3] == 0
        assert env["principal_id"][3] == 1 and env["quota_override"][2].upper() == "JSON"
        assert uniques("sandbox_environments") == {("environment_id",), ("principal_id",)}

        project = columns("sandbox_projects")
        assert project["workspace_id"][2].upper() == "VARCHAR(128)"
        assert project["title"][2].upper() == "VARCHAR(128)"
        assert project["lease_id"][3] == 0 and project["lease_expires_at"][3] == 0
        assert project["generation"][3] == 1
        assert uniques("sandbox_projects") == {("project_id",)}
        foreign = {
            (row[3], row[2], row[4])
            for row in conn.execute('PRAGMA foreign_key_list("sandbox_projects")')
        }
        assert foreign == {
            ("environment_id", "sandbox_environments", "environment_id"),
            ("principal_id", "accounts", "principal_id"),
        }

        operation = columns("sandbox_operations")
        assert operation["request_hash"][2].upper() == "VARCHAR(64)"
        assert operation["kind"][2].upper() == "VARCHAR(64)"
        assert operation["status"][2].upper() == "VARCHAR(64)"
        assert operation["project_id"][3] == 0
        assert uniques("sandbox_operations") == {
            ("operation_id",),
            ("principal_id", "request_key"),
        }
        foreign = {
            (row[3], row[2], row[4])
            for row in conn.execute('PRAGMA foreign_key_list("sandbox_operations")')
        }
        assert foreign == {
            ("principal_id", "accounts", "principal_id"),
            ("project_id", "sandbox_projects", "project_id"),
        }
    finally:
        conn.close()


async def test_schema7_readonly_keeps_original_schema(tmp_path):
    database = tmp_path / "seven.sqlite"
    view = await seeded(tmp_path, "seven.sqlite", "keep:1")
    downgrade(database, 7)
    assert set(ACCOUNT_TABLE_NAMES) <= table_names(database)
    before = database.read_bytes()
    reader = await SQLiteStore.open(database, readonly=True)
    try:
        assert reader.schema_version == 7
        assert not (SANDBOX_TABLE_NAMES & table_names(database))
        assert (await reader.snapshot(view.id)).request.text == "保留"
    finally:
        await reader.close()
    assert database.read_bytes() == before
    writer = await SQLiteStore.open(database)
    try:
        assert writer.schema_version == SCHEMA_VERSION
        assert SANDBOX_TABLE_NAMES <= table_names(database)
        assert (await writer.snapshot(view.id)).request.text == "保留"
    finally:
        await writer.close()


async def test_schema7_upgrade_preserves_account_rows(tmp_path):
    database = tmp_path / "upgrade.sqlite"
    store = await SQLiteStore.open(database)
    service = await IdentityService.open(store)
    try:
        first = await service.bootstrap("管理员", request_key="boot:1")
        assert first.token is not None
    finally:
        await service.close()
        await store.close()
    await seeded(tmp_path, "upgrade.sqlite", "keep:1")
    downgrade(database, 7)
    names = ["accounts", "access_keys", "account_events", "account_meta", "runs", "run_events"]
    before = raw(database, names)
    writer = await SQLiteStore.open(database)
    try:
        assert writer.schema_version == SCHEMA_VERSION
        assert raw(database, names) == before
        assert SANDBOX_TABLE_NAMES <= table_names(database)
        service = await IdentityService.open(writer)
        try:
            assert service.managed is True
            assert service.authenticate("Bearer " + first.token) is not None
        finally:
            await service.close()
    finally:
        await writer.close()


@pytest.mark.parametrize("version", [1, 2, 3, 4, 5, 6])
async def test_older_versions_readonly_then_upgrade_preserves_rows(tmp_path, version):
    database = tmp_path / f"v{version}.sqlite"
    view = await seeded(tmp_path, f"v{version}.sqlite", "legacy:1")
    downgrade(database, version)
    before_rows = raw(database, ["runs", "run_events"])
    before = database.read_bytes()
    reader = await SQLiteStore.open(database, readonly=True)
    try:
        assert reader.schema_version == version
        assert not (SANDBOX_TABLE_NAMES & table_names(database))
        if version < 3:
            with pytest.raises(ServiceError, match="schema_upgrade_required"):
                session_sql.require(reader)
        if version < 4:
            with pytest.raises(ServiceError, match="schema_upgrade_required"):
                work_sql.require(reader)
        if version == 1:
            with pytest.raises(ServiceError, match="schema_upgrade_required"):
                await reader.dataset("absent", "1")
        if version >= 3:
            with pytest.raises(ServiceError, match="not_found"):
                await session_sql.get_session(reader, "absent")
    finally:
        await reader.close()
    assert database.read_bytes() == before
    writer = await SQLiteStore.open(database)
    try:
        assert writer.schema_version == SCHEMA_VERSION
        assert SANDBOX_TABLE_NAMES <= table_names(database)
        assert raw(database, ["runs", "run_events"]) == before_rows
        assert (await writer.snapshot(view.id)).request.text == "保留"
        session_sql.require(writer)
        work_sql.require(writer)
    finally:
        await writer.close()


@pytest.mark.parametrize(
    "damage",
    ["missing_table", "extra_column", "extra_unique", "unknown_table", "bad_version"],
)
async def test_damaged_sandbox_schema_is_rejected(tmp_path, damage):
    database = tmp_path / "invalid.sqlite"
    await seeded(tmp_path, "invalid.sqlite", "seed:1")
    conn = sqlite3.connect(database)
    try:
        if damage == "missing_table":
            conn.execute('DROP TABLE "sandbox_operations"')
        elif damage == "extra_column":
            conn.execute("ALTER TABLE sandbox_projects ADD COLUMN surprise TEXT")
        elif damage == "extra_unique":
            conn.execute("CREATE UNIQUE INDEX surprise_unique ON sandbox_operations(request_hash)")
        elif damage == "unknown_table":
            conn.execute("CREATE TABLE surprise(value TEXT)")
        else:
            conn.execute("PRAGMA user_version=11")
        conn.commit()
    finally:
        conn.close()
    before = database.read_bytes()
    with pytest.raises(ServiceError, match="database_invalid"):
        await SQLiteStore.open(database)
    assert database.read_bytes() == before


async def test_sandbox_foreign_keys_and_unique_keys_enforced(tmp_path):
    database = tmp_path / "constraints.sqlite"
    store = await SQLiteStore.open(database)
    await store.close()
    now = 1.0
    conn = sqlite3.connect(database)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(
        "INSERT INTO accounts (principal_id, display_name, status, platform_role,"
        " authorization_generation, revision, legacy, created_at, updated_at)"
        " VALUES ('alice', 'Alice', 'active', 'member', 0, 1, 0, ?, ?)",
        (now, now),
    )
    conn.execute(
        "INSERT INTO sandbox_environments (environment_id, principal_id, profile_name,"
        " quota_override, revision, status, created_at, updated_at)"
        " VALUES ('env-1', 'alice', NULL, '{}', 1, 'unprovisioned', ?, ?)",
        (now, now),
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO sandbox_environments (environment_id, principal_id, profile_name,"
            " quota_override, revision, status, created_at, updated_at)"
            " VALUES ('env-2', 'alice', NULL, '{}', 1, 'unprovisioned', ?, ?)",
            (now, now),
        )
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO sandbox_projects (project_id, environment_id, principal_id,"
            " workspace_id, title, generation, status, lease_id, lease_expires_at,"
            " created_at, updated_at)"
            " VALUES ('p-1', 'missing-env', 'alice', 'ws-1', '项目', 0, 'unprovisioned',"
            " NULL, NULL, ?, ?)",
            (now, now),
        )
    conn.execute(
        "INSERT INTO sandbox_projects (project_id, environment_id, principal_id,"
        " workspace_id, title, generation, status, lease_id, lease_expires_at,"
        " created_at, updated_at)"
        " VALUES ('p-1', 'env-1', 'alice', 'ws-1', '项目', 0, 'unprovisioned',"
        " NULL, NULL, ?, ?)",
        (now, now),
    )
    conn.execute(
        "INSERT INTO sandbox_operations (operation_id, principal_id, request_key,"
        " request_hash, project_id, kind, status, result, created_at, updated_at)"
        " VALUES ('op-1', 'alice', 'rk-1', 'hash-1', 'p-1', 'file_write', 'completed',"
        " '{}', ?, ?)",
        (now, now),
    )
    conn.execute(
        "INSERT INTO sandbox_operations (operation_id, principal_id, request_key,"
        " request_hash, project_id, kind, status, result, created_at, updated_at)"
        " VALUES ('op-2', 'alice', 'rk-2', 'hash-2', NULL, 'file_write', 'completed',"
        " '{}', ?, ?)",
        (now, now),
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO sandbox_operations (operation_id, principal_id, request_key,"
            " request_hash, project_id, kind, status, result, created_at, updated_at)"
            " VALUES ('op-3', 'alice', 'rk-1', 'hash-3', NULL, 'file_write', 'completed',"
            " '{}', ?, ?)",
            (now, now),
        )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO sandbox_operations (operation_id, principal_id, request_key,"
            " request_hash, project_id, kind, status, result, created_at, updated_at)"
            " VALUES ('op-4', 'bob', 'rk-4', 'hash-4', NULL, 'file_write', 'completed',"
            " '{}', ?, ?)",
            (now, now),
        )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO sandbox_operations (operation_id, principal_id, request_key,"
            " request_hash, project_id, kind, status, result, created_at, updated_at)"
            " VALUES ('op-5', 'alice', 'rk-5', 'hash-5', 'missing-project', 'file_write',"
            " 'completed', '{}', ?, ?)",
            (now, now),
        )
    conn.commit()
    conn.close()
    probe = await SQLiteStore.open(database)
    try:
        assert probe.schema_version == SCHEMA_VERSION
    finally:
        await probe.close()


async def test_schema9_readonly_and_quota_upgrade_preserve_binding(tmp_path):
    database = tmp_path / "nine.sqlite"
    store = await SQLiteStore.open(database)
    await store.close()
    with sqlite3.connect(database) as conn:
        conn.execute("DROP TABLE sandbox_quota_state")
        conn.execute("PRAGMA user_version=9")
    reader = await SQLiteStore.open(database, readonly=True)
    try:
        assert reader.schema_version == 9
        assert "sandbox_quota_state" not in table_names(database)
        assert "sandbox_session_bindings" in table_names(database)
    finally:
        await reader.close()
    writer = await SQLiteStore.open(database)
    try:
        assert writer.schema_version == SCHEMA_VERSION
        assert "sandbox_quota_state" in table_names(database)
    finally:
        await writer.close()
