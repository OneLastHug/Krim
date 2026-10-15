"""公开摘要只接受同一原操作和完整效果，不接受合法字段拼出的假成功。"""

from copy import deepcopy

import pytest
from pydantic import ValidationError

from krim_agent.protocol.models import utc_now
from krim_agent.workspace_directory_restore import (
    DirectoryRestoreOperationView,
    DirectoryRestoreView,
)


def plan():
    now = utc_now()
    return {
        "plan_id": "original-plan",
        "scope_id": "topic:original",
        "target": {"kind": "checkpoint", "identifier": "original-checkpoint"},
        "root_path": "tree",
        "workspace_key": "files",
        "resource_fingerprint": "a" * 64,
        "target_snapshot": {
            "snapshot_fingerprint": "b" * 64,
            "entry_count": 1,
            "file_count": 0,
            "directory_count": 1,
            "byte_size": 0,
        },
        "backup": {
            "snapshot_fingerprint": "c" * 64,
            "entry_count": 1,
            "file_count": 0,
            "directory_count": 1,
            "byte_size": 0,
        },
        "request_key": "preview",
        "execute_key": None,
        "revision": 1,
        "phase": "ready",
        "status": "ready",
        "operation_id": "original-plan",
        "receipt": None,
        "cleanup_pending": False,
        "cancel_requested": False,
        "expires_at": 9007199254740000.0,
        "error_code": None,
        "created_at": now,
        "updated_at": now,
    }


@pytest.mark.parametrize(
    "fault", ["receipt_before_execute", "operation", "cleanup", "revision", "cancel_flag"]
)
def test_plan_refuses_contradictory_public_facts(fault):
    value = plan()
    assert DirectoryRestoreView.model_validate(deepcopy(value)).phase == "ready"
    if fault == "receipt_before_execute":
        value["receipt"] = {
            "root_path": "tree",
            "status": "rejected",
            "before_fingerprint": "c" * 64,
            "target_fingerprint": "b" * 64,
            "after_fingerprint": None,
            "retained_fingerprint": None,
            "changed": False,
            "error_code": "original_rejection",
            "recovery_required": False,
        }
    elif fault == "operation":
        value["operation_id"] = "unrelated-operation"
    elif fault == "cleanup":
        value["cleanup_pending"] = True
    elif fault == "revision":
        value["revision"] = 0
    else:
        value.update(phase="cancelled", status="cancelled", error_code="original_cancelled")
    with pytest.raises(ValidationError):
        DirectoryRestoreView.model_validate(value)


@pytest.mark.parametrize("kind", ["prepare", "execute"])
def test_operation_identity_cannot_switch_prepare_and_execute(kind):
    now = utc_now()
    value = {
        "operation_id": "original-plan" if kind == "prepare" else "original-execution",
        "plan_id": "original-plan",
        "scope_id": "topic:original",
        "kind": kind,
        "request_key": "original-key",
        "status": "running",
        "intent_recorded": False,
        "receipt": None,
        "error_code": None,
        "created_at": now,
        "updated_at": now,
    }
    assert DirectoryRestoreOperationView.model_validate(value).kind == kind
    value["operation_id"] = "foreign-preparation" if kind == "prepare" else "original-plan"
    with pytest.raises(ValidationError):
        DirectoryRestoreOperationView.model_validate(value)
