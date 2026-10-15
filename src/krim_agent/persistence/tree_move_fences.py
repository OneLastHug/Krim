"""非空树与原文件/目录双向命名空间锁；按物理身份，不按资源别名。"""

from __future__ import annotations

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_directory import (
    DirectoryChangeSpec,
    DirectoryMoveRestoreSpec,
    PreparedDirectoryChange,
)
from krim_agent.workspace_tree_writer import PreparedTreeMove
from krim_agent.workspace_writer import PreparedFileChange

from .database import (
    WorkspaceDirectoryAttemptRow,
    WorkspaceDirectoryLockRow,
    WorkspaceDirectoryRestoreLockRow,
    WorkspaceEditAttemptRow,
    WorkspaceFileLockRow,
    WorkspaceTreeExchangeLockRow,
    WorkspaceTreeMoveLockRow,
)
from .directory_fences import directory_key, entry_key

NamespaceKeys = set[tuple[str, str]]


def move_keys(change: PreparedTreeMove) -> NamespaceKeys:
    directories = {change.before.parent_identity, change.target_parent_identity}
    directories.update(
        entry.directory.identity for entry in change.before.entries if entry.directory is not None
    )
    return {
        ("entry", entry_key(change.before.parent_identity, change.before.root_path)),
        ("entry", entry_key(change.target_parent_identity, change.target_path)),
        *(("directory", directory_key(identity)) for identity in directories),
    }


def file_keys(change: PreparedFileChange) -> NamespaceKeys:
    return {
        ("entry", entry_key(change.parent_identity, change.spec.path)),
        ("directory", directory_key(change.parent_identity)),
    }


def directory_keys(change: PreparedDirectoryChange) -> NamespaceKeys:
    keys = {
        ("entry", entry_key(change.parent_identity, change.spec.path)),
        ("directory", directory_key(change.parent_identity)),
    }
    if change.before is not None:
        keys.add(("directory", directory_key(change.before.identity)))
    if change.target_parent_identity is not None:
        spec = change.spec
        if not isinstance(spec, (DirectoryChangeSpec, DirectoryMoveRestoreSpec)):
            raise ServiceError("workspace_directory_corrupt", "目录双父规格无效", 409)
        target_path = spec.target_path
        if target_path is None:
            raise ServiceError("workspace_directory_corrupt", "目录双父缺少原目标", 409)
        keys.add(("entry", entry_key(change.target_parent_identity, target_path)))
        keys.add(("directory", directory_key(change.target_parent_identity)))
    return keys


async def assert_no_move_lock(
    sql: AsyncSession, keys: NamespaceKeys, *, exclude_operation_id: str | None = None
) -> None:
    predicate = or_(
        *(
            and_(WorkspaceTreeMoveLockRow.kind == kind, WorkspaceTreeMoveLockRow.lock_key == key)
            for kind, key in keys
        )
    )
    query = select(WorkspaceTreeMoveLockRow.operation_id).where(predicate)
    if exclude_operation_id is not None:
        query = query.where(WorkspaceTreeMoveLockRow.operation_id != exclude_operation_id)
    if await sql.scalar(query) is not None:
        raise ServiceError(
            "workspace_tree_move_busy", "原树有执行中或未核实操作，禁止命名空间发布", 409
        )
    exchange_query = select(WorkspaceTreeExchangeLockRow.operation_id).where(
        or_(
            *(
                and_(
                    WorkspaceTreeExchangeLockRow.kind == kind,
                    WorkspaceTreeExchangeLockRow.lock_key == key,
                )
                for kind, key in keys
            )
        )
    )
    if exclude_operation_id is not None:
        exchange_query = exchange_query.where(
            WorkspaceTreeExchangeLockRow.operation_id != exclude_operation_id
        )
    if await sql.scalar(exchange_query) is not None:
        raise ServiceError(
            "workspace_tree_exchange_busy", "原两树有执行中或未核实交换，禁止命名空间发布", 409
        )
    restore_query = select(WorkspaceDirectoryRestoreLockRow.operation_id).where(
        or_(
            *(
                and_(
                    WorkspaceDirectoryRestoreLockRow.kind == kind,
                    WorkspaceDirectoryRestoreLockRow.lock_key == key,
                )
                for kind, key in keys
            )
        )
    )
    if exclude_operation_id is not None:
        restore_query = restore_query.where(
            WorkspaceDirectoryRestoreLockRow.operation_id != exclude_operation_id
        )
    if await sql.scalar(restore_query) is not None:
        raise ServiceError(
            "workspace_directory_restore_busy", "原目录有执行中或未核实恢复，禁止命名空间发布", 409
        )


async def assert_namespace_available(
    sql: AsyncSession, change: PreparedTreeMove, *, exclude_operation_id: str | None = None
) -> NamespaceKeys:
    keys = move_keys(change)
    await assert_namespace_keys_available(sql, keys, exclude_operation_id=exclude_operation_id)
    return keys


async def assert_namespace_keys_available(
    sql: AsyncSession, keys: NamespaceKeys, *, exclude_operation_id: str | None = None
) -> None:
    from .workspace_directories import decode
    from .workspace_edits import decode_prepared

    await assert_no_move_lock(sql, keys, exclude_operation_id=exclude_operation_id)
    files = await sql.scalars(
        select(WorkspaceEditAttemptRow).join(
            WorkspaceFileLockRow,
            WorkspaceFileLockRow.operation_id == WorkspaceEditAttemptRow.operation_id,
        )
    )
    for row in files:
        prepared = decode_prepared(row.prepared, row.before_content, row.after_content)
        if keys.intersection(file_keys(prepared)):
            raise ServiceError("workspace_file_busy", "原全树命名空间有未收束文件操作", 409)
    directories = await sql.scalars(
        select(WorkspaceDirectoryAttemptRow).join(
            WorkspaceDirectoryLockRow,
            WorkspaceDirectoryLockRow.operation_id == WorkspaceDirectoryAttemptRow.operation_id,
        )
    )
    for directory_row in directories:
        if keys.intersection(directory_keys(decode(directory_row.prepared))):
            raise ServiceError("workspace_directory_busy", "原全树命名空间有未收束目录操作", 409)
