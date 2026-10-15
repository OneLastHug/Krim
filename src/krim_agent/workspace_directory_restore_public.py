"""目录恢复公开差异页；绑定原完整材料身份，不携带任何私有内容。"""

from __future__ import annotations

from pydantic import Field, model_validator

from krim_agent.protocol.models import StrictModel
from krim_agent.workspace_directory_restore import (
    FINGERPRINT,
    IDENTIFIER,
    DirectoryRestoreDiffEntry,
    DirectoryRestoreView,
)
from krim_agent.workspace_tree_snapshot import _path


class DirectoryRestoreDiffPage(StrictModel):
    plan_id: str = Field(pattern=IDENTIFIER)
    scope_id: str = Field(min_length=1, max_length=80)
    root_path: str = Field(min_length=1, max_length=512)
    workspace_key: str = Field(min_length=1, max_length=64)
    resource_fingerprint: str = Field(pattern=FINGERPRINT)
    target_fingerprint: str = Field(pattern=FINGERPRINT)
    baseline_fingerprint: str = Field(pattern=FINGERPRINT)
    offset: int = Field(ge=0, le=9007199254740991)
    limit: int = Field(ge=1, le=128)
    entries: list[DirectoryRestoreDiffEntry] = Field(max_length=128)

    @model_validator(mode="after")
    def original_page(self) -> DirectoryRestoreDiffPage:
        _path(self.root_path)
        paths = [item.path for item in self.entries]
        if (
            len(paths) > self.limit
            or len(set(paths)) != len(paths)
            or paths != sorted(paths)
            or any(
                path != self.root_path and not path.startswith(self.root_path + "/")
                for path in paths
            )
        ):
            raise ValueError("目录恢复差异页必须有界、唯一、排序且属于原根")
        return self


def directory_restore_diff_page(
    plan: DirectoryRestoreView, entries: list[DirectoryRestoreDiffEntry], offset: int, limit: int
) -> DirectoryRestoreDiffPage:
    if plan.backup is None:
        raise ValueError("目录恢复差异缺少原完整备份")
    return DirectoryRestoreDiffPage(
        plan_id=plan.plan_id,
        scope_id=plan.scope_id,
        root_path=plan.root_path,
        workspace_key=plan.workspace_key,
        resource_fingerprint=plan.resource_fingerprint,
        target_fingerprint=plan.target_snapshot.snapshot_fingerprint,
        baseline_fingerprint=plan.backup.snapshot_fingerprint,
        offset=offset,
        limit=limit,
        entries=entries,
    )
