"""混合批次候选合约：有界意图、原回执绑定与真实汇总，存储编排仍继续。"""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from krim_agent.workspace_directory import DirectoryAttemptView
from krim_agent.workspace_mixed_batch import (
    MixedBatchItemView,
    MixedBatchRequest,
    MixedBatchView,
)


def request():
    return {
        "action": "apply",
        "targets": [
            {"source": "file", "proposal_id": "file", "expected_revision": 0},
            {"source": "directory", "proposal_id": "directory", "expected_revision": 0},
        ],
        "request_key": "审阅/混合批次?key=1",
    }


def item(status="pending"):
    operation = None
    if status == "done":
        operation = DirectoryAttemptView(
            operation_id="operation",
            proposal_id="directory",
            scope_id="session:agent",
            action="apply",
            expected_revision=0,
            status="done",
            receipt=None,
            recovery_required=False,
            cleanup_pending=False,
            created_at=1.0,
            updated_at=1.0,
        )
    return MixedBatchItemView(
        source="directory",
        position=0,
        proposal_id="directory",
        expected_revision=0,
        path="empty",
        status=status,
        operation=operation,
        error_code=None,
    )


def view():
    return {
        "batch_id": "batch",
        "scope_id": "session:agent",
        "request_key": "batch-key",
        "snapshot_revision": "0" * 64,
        "action": "apply",
        "status": "running",
        "producer_status": "running",
        "items": [item()],
        "stop_reason": None,
        "created_at": datetime(2026, 10, 11, tzinfo=UTC),
        "updated_at": datetime(2026, 10, 11, tzinfo=UTC),
    }


def test_bounded_explicit_source_requests_accept_normal_keys_and_reject_duplicates():
    value = MixedBatchRequest.model_validate(request())
    assert [entry.source for entry in value.targets] == ["file", "directory"]
    assert MixedBatchRequest.model_validate_json(value.model_dump_json()) == value
    duplicate = request()
    duplicate["targets"] = [duplicate["targets"][0], duplicate["targets"][0]]
    with pytest.raises(ValidationError):
        MixedBatchRequest.model_validate(duplicate)


@pytest.mark.parametrize(
    "field,value",
    [("action", "write"), ("request_key", "\0"), ("targets", []), ("fence", "secret")],
)
def test_public_request_rejects_ambiguous_or_private_input(field, value):
    with pytest.raises(ValidationError):
        MixedBatchRequest.model_validate({**request(), field: value})


@pytest.mark.parametrize("status", ["running", "done", "uncertain"])
def test_unlinked_item_cannot_claim_operation_result(status):
    with pytest.raises(ValidationError):
        MixedBatchItemView.model_validate({**item().model_dump(), "status": status})


@pytest.mark.parametrize(
    "field,value",
    [
        ("source", "file"),
        ("proposal_id", "other"),
        ("expected_revision", 1),
        ("status", "rejected"),
    ],
)
def test_item_cannot_relabel_original_directory_operation(field, value):
    original = item("done")
    with pytest.raises(ValidationError):
        MixedBatchItemView.model_validate({**original.model_dump(), field: value})


def test_summary_cannot_claim_done_for_pending_or_foreign_child():
    original = view()
    assert MixedBatchView.model_validate(original).status == "running"
    with pytest.raises(ValidationError):
        MixedBatchView.model_validate({**original, "status": "done", "producer_status": "finished"})
    finished = {
        **original,
        "items": [item("done")],
        "status": "done",
        "producer_status": "finished",
    }
    assert MixedBatchView.model_validate(finished).status == "done"
    with pytest.raises(ValidationError):
        MixedBatchView.model_validate({**finished, "scope_id": "session:foreign"})
    with pytest.raises(ValidationError):
        MixedBatchView.model_validate({**finished, "action": "undo"})
