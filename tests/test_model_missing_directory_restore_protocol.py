"""缺失模型结果不能混入完整before/备份，查询必须有界且身份唯一。"""

import copy

import pytest
from pydantic import ValidationError

from krim_agent.workspace_missing_directory_restore_model import (
    MissingDirectoryRestoreDiffPage,
    MissingDirectoryRestoreLookup,
    MissingDirectoryRestoreOperationLookup,
    MissingDirectoryRestoreQuery,
    MissingDirectoryRestoreQueryResult,
)


@pytest.mark.parametrize(
    "model,payload",
    [
        (MissingDirectoryRestoreLookup, {}),
        (MissingDirectoryRestoreLookup, {"plan_id": "p", "request_key": "k"}),
        (MissingDirectoryRestoreOperationLookup, {"operation_id": "p", "request_key": "k"}),
        (MissingDirectoryRestoreQuery, {"kind": "id"}),
        (MissingDirectoryRestoreQuery, {"kind": "key", "plan_id": "p", "request_key": "k"}),
        (MissingDirectoryRestoreQuery, {"kind": "history", "plan_id": "p"}),
        (MissingDirectoryRestoreQuery, {"kind": "history", "limit": 128}),
        (MissingDirectoryRestoreQuery, {"kind": "diff", "plan_id": "p", "limit": 129}),
        (MissingDirectoryRestoreQuery, {"kind": "diff", "plan_id": "p", "offset": True}),
        (MissingDirectoryRestoreQuery, {"kind": "history", "scope_id": "foreign"}),
    ],
)
def test_missing_model_query_requires_exact_original_selection(model, payload):
    with pytest.raises(ValidationError):
        model.model_validate(payload)


def page():
    node = {"kind": "directory", "mode": 0o755, "uid": 1, "gid": 1, "byte_size": 0, "sha256": None}
    return {
        "plan_id": "plan",
        "scope_id": "session:original",
        "root_path": "tree",
        "workspace_key": "files",
        "resource_fingerprint": "a" * 64,
        "target_fingerprint": "b" * 64,
        "baseline": {"kind": "absent", "fingerprint": "c" * 64},
        "offset": 0,
        "limit": 2,
        "total": 2,
        "next_offset": None,
        "entries": [
            {"path": "tree", "operation": "create", "before": None, "after": node},
            {"path": "tree/sub", "operation": "create", "before": None, "after": node},
        ],
    }


@pytest.mark.parametrize(
    "fault",
    ["present", "backup", "source", "delete", "outside", "duplicate", "order", "limit", "payload"],
)
def test_missing_diff_rejects_forged_before_full_backup_or_incomplete_page(fault):
    value = copy.deepcopy(page())
    if fault == "present":
        value["baseline"]["kind"] = "present"
    elif fault == "backup":
        value["baseline"] = {"snapshot_fingerprint": "c" * 64, "entry_count": 1}
    elif fault == "source":
        value["source"] = "workspace_directory_restore_diff"
    elif fault == "delete":
        value["entries"][0].update(
            operation="delete", before=value["entries"][0]["after"], after=None
        )
    elif fault == "outside":
        value["entries"][1]["path"] = "other/sub"
    elif fault == "duplicate":
        value["entries"][1] = value["entries"][0]
    elif fault == "order":
        value["entries"].reverse()
    elif fault == "limit":
        value["limit"] = 1
    else:
        value["payload"] = "private"
    with pytest.raises(ValidationError):
        MissingDirectoryRestoreDiffPage.model_validate(value)


@pytest.mark.parametrize("kind", ["id", "operation", "diff"])
def test_nonnullable_missing_result_branch_cannot_be_removed(kind):
    with pytest.raises(ValidationError):
        MissingDirectoryRestoreQueryResult(kind=kind)


def test_missing_diff_validates_explicit_absent_and_original_counts():
    value = MissingDirectoryRestoreDiffPage.model_validate(page())
    result = MissingDirectoryRestoreQueryResult(kind="diff", diff=value)
    assert result.diff.baseline.kind == "absent"
    assert [entry.path for entry in result.diff.entries] == ["tree", "tree/sub"]


@pytest.mark.parametrize("fault", ["missing-next", "skip-next", "empty", "total", "root", "bool"])
def test_missing_diff_cursor_cannot_skip_remaining_original_entries(fault):
    value = page()
    if fault == "missing-next":
        value["total"] = 3
    elif fault == "skip-next":
        value.update(total=4, next_offset=3)
    elif fault == "empty":
        value["entries"] = []
    elif fault == "total":
        value["total"] = 1
    elif fault == "root":
        value["entries"][0]["path"] = "tree/a"
    else:
        value["next_offset"] = True
    with pytest.raises(ValidationError):
        MissingDirectoryRestoreDiffPage.model_validate(value)
