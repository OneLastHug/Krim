"""目录恢复与原文件/目录/树操作共用实际命名空间身份。"""

from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.workspace_directory_restore_writer import PreparedDirectoryRestore
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshot

from .directory_fences import directory_key, entry_key
from .tree_move_fences import NamespaceKeys, assert_namespace_keys_available


def snapshot_keys(snapshot: WorkspaceTreeSnapshot) -> NamespaceKeys:
    directories = {snapshot.parent_identity}
    directories.update(
        entry.directory.identity for entry in snapshot.entries if entry.directory is not None
    )
    return {
        ("entry", entry_key(snapshot.parent_identity, snapshot.root_path)),
        *(("directory", directory_key(identity)) for identity in directories),
    }


def prepared_keys(prepared: PreparedDirectoryRestore) -> NamespaceKeys:
    return snapshot_keys(prepared.before) | snapshot_keys(prepared.staged)


async def assert_restore_namespace(
    sql: AsyncSession,
    snapshot: WorkspaceTreeSnapshot,
    prepared: PreparedDirectoryRestore | None = None,
    *,
    exclude_operation_id: str | None = None,
) -> NamespaceKeys:
    keys = snapshot_keys(snapshot)
    if prepared is not None:
        keys |= prepared_keys(prepared)
    await assert_namespace_keys_available(sql, keys, exclude_operation_id=exclude_operation_id)
    return keys
