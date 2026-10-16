"""缺失恢复模型父的独立结果/差异；不把缺失baseline当完整树备份。"""

from __future__ import annotations

from collections.abc import Callable
from typing import Literal

from pydantic import Field, JsonValue, model_validator

from krim_agent.protocol.models import StrictModel, check_json
from krim_agent.workspace_directory_restore import (
    FINGERPRINT,
    IDENTIFIER,
    DirectoryRestoreDiffEntry,
)
from krim_agent.workspace_directory_restore_model import (
    DirectoryRestoreLookup,
    DirectoryRestoreOperationLookup,
    DirectoryRestoreQuery,
    DirectoryRestoreQueryKind,
)
from krim_agent.workspace_missing_directory_restore import (
    DirectoryAbsenceSummary,
    MissingDirectoryRestoreOperationView,
    MissingDirectoryRestoreView,
)
from krim_agent.workspace_tree_snapshot import _path


class MissingDirectoryRestoreLookup(DirectoryRestoreLookup):
    pass


class MissingDirectoryRestoreOperationLookup(DirectoryRestoreOperationLookup):
    pass


class MissingDirectoryRestoreQuery(DirectoryRestoreQuery):
    pass


class MissingDirectoryRestoreDiffPage(StrictModel):
    source: Literal["workspace_missing_directory_restore_diff"] = (
        "workspace_missing_directory_restore_diff"
    )
    plan_id: str = Field(pattern=IDENTIFIER)
    scope_id: str = Field(min_length=1, max_length=80)
    root_path: str = Field(min_length=1, max_length=512)
    workspace_key: str = Field(min_length=1, max_length=64)
    resource_fingerprint: str = Field(pattern=FINGERPRINT)
    target_fingerprint: str = Field(pattern=FINGERPRINT)
    baseline: DirectoryAbsenceSummary
    offset: int = Field(ge=0, le=9007199254740991)
    limit: int = Field(ge=1, le=128)
    total: int = Field(ge=1, le=1024)
    next_offset: int | None = Field(default=None, ge=0, le=9007199254740991)
    entries: list[DirectoryRestoreDiffEntry] = Field(max_length=128)

    @model_validator(mode="after")
    def original_absence(self) -> MissingDirectoryRestoreDiffPage:
        _path(self.root_path)
        paths = [entry.path for entry in self.entries]
        if (
            len(paths) > self.limit
            or len(paths) > max(0, self.total - self.offset)
            or not paths
            and self.offset < self.total
            or self.next_offset
            != (self.offset + len(paths) if self.offset + len(paths) < self.total else None)
            or self.offset == 0
            and paths
            and paths[0] != self.root_path
            or len(set(paths)) != len(paths)
            or paths != sorted(paths)
            or any(
                path != self.root_path and not path.startswith(self.root_path + "/")
                for path in paths
            )
            or any(
                entry.operation != "create" or entry.before is not None for entry in self.entries
            )
        ):
            raise ValueError("缺失差异必须唯一排序、有界且仅原根下的create/缺失before")
        return self


def missing_directory_restore_diff_page(
    plan: MissingDirectoryRestoreView,
    entries: list[DirectoryRestoreDiffEntry],
    offset: int,
    limit: int,
) -> MissingDirectoryRestoreDiffPage:
    if plan.baseline is None:
        raise ValueError("缺失差异须保留原真实缺失baseline")
    return MissingDirectoryRestoreDiffPage(
        plan_id=plan.plan_id,
        scope_id=plan.scope_id,
        root_path=plan.root_path,
        workspace_key=plan.workspace_key,
        resource_fingerprint=plan.resource_fingerprint,
        target_fingerprint=plan.target_snapshot.snapshot_fingerprint,
        baseline=plan.baseline,
        offset=offset,
        limit=limit,
        total=plan.target_snapshot.entry_count,
        next_offset=offset + len(entries)
        if offset + len(entries) < plan.target_snapshot.entry_count
        else None,
        entries=entries,
    )


class MissingDirectoryRestoreQueryResult(StrictModel):
    kind: DirectoryRestoreQueryKind
    plan: MissingDirectoryRestoreView | None = None
    operation: MissingDirectoryRestoreOperationView | None = None
    diff: MissingDirectoryRestoreDiffPage | None = None
    entries: list[MissingDirectoryRestoreView] = Field(default_factory=list, max_length=32)
    offset: int | None = Field(default=None, ge=0, le=5000)
    limit: int | None = Field(default=None, ge=1, le=32)
    next_offset: int | None = Field(default=None, ge=0, le=5000)

    @model_validator(mode="after")
    def original_result(self) -> MissingDirectoryRestoreQueryResult:
        if self.kind in {"id", "key"}:
            valid = (
                self.operation is None
                and self.diff is None
                and not self.entries
                and (self.kind == "key" or self.plan is not None)
            )
        elif self.kind in {"operation", "operation_by_key"}:
            valid = (
                self.plan is None
                and self.diff is None
                and not self.entries
                and (self.kind == "operation_by_key" or self.operation is not None)
            )
        elif self.kind == "diff":
            valid = (
                self.diff is not None
                and self.plan is None
                and self.operation is None
                and not self.entries
            )
        else:
            valid = (
                self.plan is None
                and self.operation is None
                and self.diff is None
                and self.offset is not None
                and self.limit is not None
                and len(self.entries) <= self.limit
                and (
                    self.next_offset is None
                    or bool(self.entries)
                    and self.next_offset == self.offset + len(self.entries)
                )
            )
        if self.kind != "history":
            valid = (
                valid and self.offset is None and self.limit is None and self.next_offset is None
            )
        if not valid:
            raise ValueError("缺失恢复查询分支须保留原缺失事实，不能混入完整before/备份")
        return self


def bounded_missing_directory_restore_result(
    result: MissingDirectoryRestoreQueryResult,
    *,
    validate_envelope: Callable[[dict[str, JsonValue]], None],
) -> MissingDirectoryRestoreQueryResult:
    """按原UTF-8 JSON边界返回完整前缀，并显式保留下一页位置。"""
    while True:
        try:
            value = result.model_dump(mode="json")
            check_json(value)
            validate_envelope(value)
            return result
        except ValueError:
            if result.kind == "diff" and result.diff is not None and len(result.diff.entries) > 1:
                diff = result.diff.model_dump(mode="json")
                diff["entries"] = result.diff.entries[:-1]
                diff["next_offset"] = result.diff.offset + len(result.diff.entries) - 1
                result.diff = MissingDirectoryRestoreDiffPage.model_validate(diff)
            elif result.kind == "history" and len(result.entries) > 1:
                result.entries = result.entries[:-1]
                assert result.offset is not None
                result.next_offset = result.offset + len(result.entries)
            else:
                raise
