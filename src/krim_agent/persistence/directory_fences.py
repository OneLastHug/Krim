"""目录与文件共用物理命名空间门禁；不依赖工作区别名或资源版本。"""

import hashlib
import json

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_writer import PreparedFileChange

from .database import WorkspaceDirectoryLockRow


def entry_key(parent: tuple[int, int], path: str) -> str:
    return hashlib.sha256(
        json.dumps(
            {"parent": list(parent), "name": path.split("/")[-1]},
            sort_keys=True,
            ensure_ascii=False,
        ).encode()
    ).hexdigest()


def directory_key(identity: tuple[int, int]) -> str:
    return hashlib.sha256(
        json.dumps({"directory": list(identity)}, sort_keys=True).encode()
    ).hexdigest()


async def assert_file_directory_fence(session: AsyncSession, prepared: PreparedFileChange) -> None:
    lock = await session.scalar(
        select(WorkspaceDirectoryLockRow.operation_id).where(
            or_(
                WorkspaceDirectoryLockRow.lock_key
                == entry_key(prepared.parent_identity, prepared.spec.path),
                WorkspaceDirectoryLockRow.target_lock_key
                == entry_key(prepared.parent_identity, prepared.spec.path),
                WorkspaceDirectoryLockRow.directory_key == directory_key(prepared.parent_identity),
            )
        )
    )
    if lock is not None:
        raise ServiceError(
            "workspace_directory_busy", "目录有执行中或未核实操作，禁止文件发布", 409
        )
