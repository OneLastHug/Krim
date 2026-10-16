"""独立受控协议拒绝歧义/私有材料，最大UTF-8页保留原JSON上限。"""

import json

import pytest
from pydantic import ValidationError
from test_missing_baseline_rollback_control import setup

from krim_agent.protocol.models import check_json
from krim_agent.workspace_missing_directory_rollback import (
    MissingDirectoryRollbackPage,
    MissingDirectoryRollbackPreview,
    MissingDirectoryRollbackView,
)
from krim_agent.workspace_missing_directory_rollback_control import (
    MissingDirectoryRollbackQuery,
    MissingDirectoryRollbackQueryResult,
)


@pytest.mark.parametrize(
    "payload",
    [
        {"kind": "proposal"},
        {"kind": "operation", "request_key": "x"},
        {"kind": "by_key", "identifier": "x", "request_key": "x"},
        {"kind": "proposals", "identifier": "x"},
        {"kind": "proposals", "limit": 5},
        {"kind": "proposals", "limit": True},
        {"kind": "proposals", "scope_id": "forged"},
        {"kind": "proposal", "identifier": "x", "after": "x"},
        {"kind": "proposal", "identifier": "x", "limit": 1},
        {"kind": "by_key", "request_key": "x\0"},
    ],
)
def test_query_rejects_ambiguous_private_or_unbounded_requests(payload):
    with pytest.raises(ValidationError):
        MissingDirectoryRollbackQuery.model_validate(payload)


@pytest.mark.parametrize(
    "field", ["scope_id", "source_path", "missing_origin", "model_source", "prepared"]
)
def test_preview_cannot_upload_private_source_or_change_inferred_root(field):
    with pytest.raises(ValidationError):
        MissingDirectoryRollbackPreview.model_validate(
            {
                "plan_id": "original",
                "preserve_path": "retained",
                "expected_fingerprint": "a" * 64,
                "request_key": "preview",
                field: "forged",
            }
        )


@pytest.mark.parametrize("kind", ["proposal", "operation", "history", "proposals"])
def test_query_result_must_keep_selected_branch(kind):
    with pytest.raises(ValidationError):
        MissingDirectoryRollbackQueryResult(kind=kind)
    assert MissingDirectoryRollbackQueryResult(kind="by_key").proposal is None


async def test_maximum_unicode_page_fits_original_json_limit_without_truncation(runtime, tmp_path):
    service, agent, *_rest, request = await setup(runtime, tmp_path)
    try:
        actual = await service.missing_directory_rollback_preview(agent.agent_id, request)
        raw = actual.model_dump(mode="json")
        raw["move"].update(
            source_path="😀" * 512,
            target_path="界" * 512,
            workspace_key="😀" * 64,
            scope_id="😀" * 80,
        )
        value = MissingDirectoryRollbackView.model_validate_json(json.dumps(raw))
        result = MissingDirectoryRollbackQueryResult(
            kind="proposals",
            proposals=MissingDirectoryRollbackPage(items=[value] * 4, next_after=None),
        )
        check_json(result.model_dump(mode="json"))
        assert result.proposals.items[0].move.source_path == "😀" * 512
        with pytest.raises(ValidationError):
            MissingDirectoryRollbackQueryResult(
                kind="proposals",
                proposals=MissingDirectoryRollbackPage(items=[value] * 5, next_after=None),
            )
    finally:
        await service.close()
