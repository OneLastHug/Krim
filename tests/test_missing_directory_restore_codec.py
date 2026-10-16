"""真实缺失/准备/实际效果的持久材料回环、关联与严格公共事实。"""

import copy
import json
from dataclasses import asdict

import pytest
from pydantic import ValidationError
from test_missing_directory_writer import original

from krim_agent.persistence.missing_directory_restore_codec import (
    decode_absence,
    decode_effect,
    decode_prepared,
    encode_absence,
    encode_effect,
    encode_prepared,
)
from krim_agent.persistence.tree_exchange_codec import _digest
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import utc_now
from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader
from krim_agent.workspace_missing_directory_restore import MissingDirectoryRestoreView


def test_real_absence_prepared_and_effect_roundtrip_one_binary_blob_and_cold_refusal(tmp_path):
    with original(tmp_path) as (workspace, reader, writer, target, before, tree):
        missing = encode_absence(before)
        assert decode_absence(missing) == before
        reader.verify(decode_absence(missing))
        with WorkspaceDirectoryAbsenceReader(workspace) as cold:
            with pytest.raises(ServiceError) as denied:
                cold.verify(decode_absence(missing))
            assert denied.value.code == "workspace_directory_absence_unverifiable"
        prepared = writer.prepare(target, before, token="d" * 32)
        metadata, payload = encode_prepared(prepared)
        assert decode_prepared(metadata, payload) == prepared
        assert payload == b"".join(
            entry.file.data for entry in prepared.staged.entries if entry.file is not None
        )
        assert "original directory" not in json.dumps(metadata)
        actual = writer.apply(prepared, record_intent=lambda _value: None)
        assert actual.receipt.status == "done"
        effect = encode_effect(prepared, actual)
        assert decode_effect(prepared, effect) == actual
        assert "payload" not in effect
        assert effect["receipt"] == asdict(actual.receipt)
        assert effect["prepared_material_sha256"] == metadata["material_sha256"]
        assert (tree / "sub/raw.bin").read_bytes() == b"\0\xff"


@pytest.mark.parametrize(
    "fault", ["version-bool", "extra", "parent-bool", "observer", "kind", "fingerprint", "path"]
)
def test_absence_rehashed_corruption_still_refuses(tmp_path, fault):
    with original(tmp_path) as (_workspace, _reader, _writer, _target, before, _tree):
        data = encode_absence(before)
        if fault == "version-bool":
            data["version"] = True
        elif fault == "extra":
            data["private_token"] = "not-allowed"
        elif fault == "parent-bool":
            data["parent"]["inode"] = True
        elif fault == "observer":
            data["observation_id"] = "unbound"
        elif fault == "kind":
            data["kind"] = "workspace-tree-snapshot"
        elif fault == "fingerprint":
            data["fingerprint"] = "f" * 64
        else:
            data["root_path"] = "../outside"
        data["material_sha256"] = _digest(data)
        with pytest.raises(ServiceError) as denied:
            decode_absence(data)
        assert denied.value.code == "workspace_missing_directory_restore_corrupt"


@pytest.mark.parametrize(
    "fault",
    ["version", "extra", "payload", "container", "parent", "root", "same-epoch", "workspace"],
)
def test_prepared_historical_material_and_blob_cannot_be_reassociated(tmp_path, fault):
    with original(tmp_path) as (_workspace, _reader, writer, target, before, _tree):
        prepared = writer.prepare(target, before, token="e" * 32)
        metadata, payload = encode_prepared(prepared)
        data = copy.deepcopy(metadata)
        if fault == "version":
            data["version"] = 2
        elif fault == "extra":
            data["stage"]["credential"] = "no"
        elif fault == "payload":
            payload = payload[:-1] + bytes([payload[-1] ^ 1])
            data["payload_sha256"] = __import__("hashlib").sha256(payload).hexdigest()
        elif fault == "container":
            data["stage"]["container_identity"] = data["stage"]["parent_identity"]
        elif fault == "parent":
            data["stage"]["parent_identity"][1] += 1
        elif fault == "root":
            data["stage"]["root_path"] = "other"
        elif fault == "same-epoch":
            data["current_absence"] = copy.deepcopy(data["before"])
        else:
            data["before"]["workspace_fingerprint"] = "f" * 64
            data["before"]["material_sha256"] = _digest(data["before"])
        data["material_sha256"] = _digest(data)
        with pytest.raises(ServiceError):
            decode_prepared(data, payload)


@pytest.mark.parametrize(
    "fault", ["prepared", "root", "before", "target", "after", "intent", "extra"]
)
def test_actual_effect_rehashed_wrong_facts_cannot_be_success(tmp_path, fault):
    with original(tmp_path) as (_workspace, _reader, writer, target, before, _tree):
        prepared = writer.prepare(target, before, token="f" * 32)
        actual = writer.apply(prepared, record_intent=lambda _value: None)
        assert actual.receipt.status == "done"
        data = encode_effect(prepared, actual)
        if fault == "prepared":
            data["prepared_material_sha256"] = "f" * 64
        elif fault == "root":
            data["receipt"]["root_path"] = "other"
        elif fault == "before":
            data["receipt"]["before_absence_fingerprint"] = "f" * 64
        elif fault == "target":
            data["receipt"]["target_fingerprint"] = "f" * 64
        elif fault == "after":
            data["receipt"]["after_fingerprint"] = "f" * 64
        elif fault == "intent":
            data["receipt"]["changed"] = False
        else:
            data["observed_after"]["private_hash"] = "forged"
        data["material_sha256"] = _digest(data)
        with pytest.raises(ServiceError):
            decode_effect(prepared, data)


def view():
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
        "baseline": {"kind": "absent", "fingerprint": "c" * 64},
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
    "fault", ["baseline", "zero-bytes", "operation", "cleanup", "version", "private"]
)
def test_public_missing_facts_do_not_impersonate_tree_or_private_qualification(fault):
    value = view()
    actual = MissingDirectoryRestoreView.model_validate(copy.deepcopy(value))
    assert actual.baseline.kind == "absent"
    assert "observation_id" not in actual.model_dump_json()
    if fault == "baseline":
        value["baseline"] = {"kind": "tree", "fingerprint": "c" * 64}
    elif fault == "zero-bytes":
        value["target_snapshot"]["byte_size"] = 1
    elif fault == "operation":
        value["operation_id"] = "other"
    elif fault == "cleanup":
        value["cleanup_pending"] = True
    elif fault == "version":
        value["revision"] = 0
    else:
        value["baseline"]["observation_id"] = "d" * 32
    with pytest.raises(ValidationError):
        MissingDirectoryRestoreView.model_validate(value)
