"""SRC-OSS-007：接管已核验的 M1 基线，再执行随包分发的版本迁移。"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from pathlib import Path
from threading import Lock

from alembic import command
from alembic.config import Config
from sqlalchemy import inspect
from sqlalchemy.engine import Connection

_migration_state = Lock()
_migration_busy = False
_migration_waiters: set[tuple[asyncio.AbstractEventLoop, asyncio.Event]] = set()
_inside_migration: ContextVar[bool] = ContextVar("krim_database_migration", default=False)


@asynccontextmanager
async def migration_guard() -> AsyncIterator[None]:
    global _migration_busy
    if _inside_migration.get():
        raise RuntimeError("迁移上下文不能重入初始化")
    loop, changed = asyncio.get_running_loop(), asyncio.Event()
    waiter = (loop, changed)
    while True:
        # 线程锁只保护短状态；SQL等待与跨loop通知均在锁外。
        with _migration_state:
            if not _migration_busy:
                _migration_busy = True
                break
            _migration_waiters.add(waiter)
        try:
            await changed.wait()
        finally:
            with _migration_state:
                _migration_waiters.discard(waiter)
        changed.clear()
    marker = _inside_migration.set(True)
    try:
        yield
    finally:
        _inside_migration.reset(marker)
        with _migration_state:
            _migration_busy = False
            waiting = tuple(_migration_waiters)
        for target_loop, target_event in waiting:
            try:
                target_loop.call_soon_threadsafe(target_event.set)
            except RuntimeError:
                # 已关闭的loop不能继续等待，不影响其他初始化的原结果。
                with _migration_state:
                    _migration_waiters.discard((target_loop, target_event))


LEGACY_COLUMNS = {
    "topics": {"id", "title", "created_at", "updated_at"},
    "runs": {
        "id",
        "topic_id",
        "input_message_id",
        "idempotency_key",
        "input_hash",
        "status",
        "stop_requested",
        "error",
        "created_at",
        "updated_at",
        "finished_at",
    },
    "messages": {
        "id",
        "topic_id",
        "run_id",
        "sequence",
        "role",
        "content",
        "details",
        "created_at",
    },
    "tool_audits": {"call_id", "run_id", "payload", "created_at"},
}


def migration_config(connection: Connection) -> Config:
    config = Config()
    config.set_main_option("script_location", str(Path(__file__).parent / "migrations"))
    config.attributes["connection"] = connection
    return config


def upgrade_database(connection: Connection) -> None:
    inspector = inspect(connection)
    tables = set(inspector.get_table_names())
    config = migration_config(connection)
    if "alembic_version" not in tables and tables:
        valid = tables == set(LEGACY_COLUMNS)
        for table, columns in LEGACY_COLUMNS.items():
            if table not in tables:
                valid = False
                continue
            valid &= {column["name"] for column in inspector.get_columns(table)} == columns
            expected_pk = ["call_id", "run_id"] if table == "tool_audits" else ["id"]
            valid &= inspector.get_pk_constraint(table)["constrained_columns"] == expected_pk
        for table, unique_columns in (
            ("runs", {"topic_id", "idempotency_key"}),
            ("messages", {"topic_id", "sequence"}),
        ):
            if table in tables:
                valid &= any(
                    set(item["column_names"]) == unique_columns
                    for item in inspector.get_unique_constraints(table)
                )
        expected_fks = {
            "runs": {("topic_id", "topics", "id")},
            "messages": {("topic_id", "topics", "id"), ("run_id", "runs", "id")},
            "tool_audits": {("run_id", "runs", "id")},
        }
        for table, expected_foreign_keys in expected_fks.items():
            if table in tables:
                actual = {
                    (
                        item["constrained_columns"][0],
                        item["referred_table"],
                        item["referred_columns"][0],
                    )
                    for item in inspector.get_foreign_keys(table)
                    if len(item["constrained_columns"]) == 1
                }
                valid &= actual == expected_foreign_keys
        if not valid:
            raise RuntimeError("数据库不是已核验的 M1 schema；请备份并检查结构，禁止覆盖升级")
        command.stamp(config, "0001_m1")
    command.upgrade(config, "head")
