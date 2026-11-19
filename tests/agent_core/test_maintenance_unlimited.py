"""Maintenance reaches later records and retains legacy quota semantics."""

from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.sandbox import maintenance
from krim_agent.sandbox.quota import QuotaLimits

ENV = "11111111-1111-4111-8111-111111111111"
PROJECT = "22222222-2222-4222-8222-222222222222"
CHECKPOINT = "33333333-3333-4333-8333-333333333333"
OPERATION = "44444444-4444-4444-8444-444444444444"


def entries(*names):
    return nullcontext(iter(SimpleNamespace(name=name) for name in names))


def legacy_limits():
    return QuotaLimits().model_dump(mode="json", exclude={"resource_policy"})


def test_completed_restore_release_reaches_records_after_old_scan_limit(monkeypatch):
    import os

    restore = {
        "status": "completed",
        "before_protected_until": 1,
        "before_checkpoint": CHECKPOINT,
        "environment_id": ENV,
        "project_id": PROJECT,
        "operation_id": OPERATION,
    }
    checkpoint = SimpleNamespace(release_reference=Mock())
    broker = SimpleNamespace(
        state=Path("state"),
        _read=Mock(return_value=restore),
        _save=Mock(),
        recovery=SimpleNamespace(store=Mock(return_value=checkpoint)),
    )
    name = "restore-" + OPERATION + ".json"
    monkeypatch.setattr(os, "scandir", lambda path: entries(
        *("unrelated-" + str(index) for index in range(8193)), name,
    ))
    maintenance.release_completed_restore_roots(broker)
    checkpoint.release_reference.assert_called_once_with(
        ENV, PROJECT, CHECKPOINT, "restore-before", OPERATION,
    )
    assert restore["before_protected_until"] is None


def test_scheduled_cleanup_reaches_project_after_old_scan_limit(monkeypatch):
    import os

    saved = {}
    row = {"environment_id": ENV, "project_id": PROJECT}
    name = "project-" + PROJECT + ".json"
    broker = SimpleNamespace(
        state=Path("state"),
        _read=lambda key: row if key == name else saved.get(key),
        _save=lambda key, value: saved.update({key: value}),
        jobs=None,
        recovery=SimpleNamespace(active=False),
        _environment=lambda env: {"quota_operation": "held"},
    )
    monkeypatch.setattr(maintenance, "release_completed_restore_roots", lambda broker: None)
    monkeypatch.setattr(os, "scandir", lambda path: entries(
        *("unrelated-" + str(index) for index in range(8193)), name,
    ))
    assert maintenance.scheduled_cleanup(broker) == {"skipped": "quota_maintenance"}
    assert saved["maintenance-cursor.json"] == {"index": 1}


def test_checkpoint_later_pages_and_cleanup_preserve_legacy_policy(monkeypatch, tmp_path):
    seen = []

    class Checkpoints:
        def __init__(self, root):
            pass

        def list(self, env, project):
            return [{"checkpoint_id": str(index), "files": [], "excluded": []}
                    for index in range(1003)]

        def cleanup_preview(self, env, project, limits):
            seen.append(limits.resource_policy)
            return {"candidates": []}

    environment = {"limits": legacy_limits()}
    broker = SimpleNamespace(
        environments=tmp_path,
        _environment=lambda env: environment,
        _project=lambda env, project: {},
    )
    monkeypatch.setattr(maintenance, "ShadowCheckpoints", Checkpoints)
    result = maintenance.dispatch_maintenance(broker, "checkpoint_list", {
        "environment_id": ENV, "project_id": PROJECT, "offset": 1001, "limit": 1,
    })
    assert result["total"] == 1003 and result["items"][0]["checkpoint_id"] == "1001"
    maintenance.dispatch_maintenance(broker, "checkpoint_cleanup_preview", {
        "environment_id": ENV, "project_id": PROJECT,
    })
    assert seen == ["limited"]
    with pytest.raises(ServiceError, match="invalid_input"):
        maintenance.dispatch_maintenance(broker, "checkpoint_list", {
            "environment_id": ENV, "project_id": PROJECT, "offset": -1, "limit": 1,
        })


def test_automatic_cleanup_preserves_legacy_policy(monkeypatch, tmp_path):
    import os

    root = tmp_path / ENV / "checkpoints"
    root.mkdir(parents=True)
    seen = []

    class Checkpoints:
        def __init__(self, root):
            pass

        def cleanup_preview(self, env, project, limits):
            seen.append(limits.resource_policy)
            return {"candidates": []}

    name = "project-" + PROJECT + ".json"
    row = {"environment_id": ENV, "project_id": PROJECT}
    broker = SimpleNamespace(
        state=tmp_path,
        environments=tmp_path,
        _read=lambda key: row if key == name else None,
        _save=lambda *args: None,
        jobs=None,
        recovery=SimpleNamespace(active=False),
        _environment=lambda env: {"limits": legacy_limits()},
    )
    monkeypatch.setattr(maintenance, "release_completed_restore_roots", lambda broker: None)
    monkeypatch.setattr(maintenance, "ShadowCheckpoints", Checkpoints)
    monkeypatch.setattr(os, "scandir", lambda path: entries(name))
    assert maintenance.scheduled_cleanup(broker) == {"skipped": "no_candidates"}
    assert seen == ["limited"]
