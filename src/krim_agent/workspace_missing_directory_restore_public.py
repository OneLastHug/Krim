"""公开缺失差异页复用原Missing事实和可靠字节分页，不返回私有材料。"""

from __future__ import annotations

from krim_agent.workspace_directory_restore import DirectoryRestoreDiffEntry
from krim_agent.workspace_missing_directory_restore import MissingDirectoryRestoreView
from krim_agent.workspace_missing_directory_restore_model import (
    MissingDirectoryRestoreDiffPage,
    MissingDirectoryRestoreQueryResult,
    bounded_missing_directory_restore_result,
)
from krim_agent.workspace_missing_directory_restore_model import (
    missing_directory_restore_diff_page as original_diff_page,
)

__all__ = ["MissingDirectoryRestoreDiffPage", "missing_directory_restore_diff_page"]


def missing_directory_restore_diff_page(
    plan: MissingDirectoryRestoreView,
    entries: list[DirectoryRestoreDiffEntry],
    offset: int,
    limit: int,
) -> MissingDirectoryRestoreDiffPage:
    result = bounded_missing_directory_restore_result(
        MissingDirectoryRestoreQueryResult(
            kind="diff", diff=original_diff_page(plan, entries, offset, limit)
        ),
        # HTTP/SDK页无模型tool_result封装，仍保持原64KiB JSON边界。
        validate_envelope=lambda _value: None,
    )
    assert result.diff is not None
    return result.diff
