"""新计划私有材料先严格回环；所有完成回执来自真实物理发布。"""

import copy
import json
from dataclasses import asdict, replace
from uuid import uuid4

import pytest
from test_directory_restore_writer import versions

from krim_agent.persistence.directory_restore_codec import (
    decode_effect,
    decode_prepared,
    encode_effect,
    encode_prepared,
)
from krim_agent.persistence.tree_exchange_codec import _digest
from krim_agent.persistence.tree_snapshot_codec import encode_snapshot
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_directory_restore_writer import WorkspaceDirectoryRestoreWriter


def original(root):
    workspace = Workspace(root, writable=True)
    writer = WorkspaceDirectoryRestoreWriter(workspace)
    target, before = versions(root, writer)
    prepared = writer.prepare(target, expected_before=before.fingerprint, token=uuid4().hex)
    return workspace, writer, prepared


def test_private_prepared_real_binary_and_dual_effect_roundtrip_without_second_blob(tmp_path):
    workspace, writer, prepared = original(tmp_path)
    try:
        metadata, payload = encode_prepared(prepared)
        assert decode_prepared(metadata, payload) == prepared
        assert payload == b"".join(
            entry.file.data
            for tree in (prepared.before, prepared.staged)
            for entry in tree.entries
            if entry.file is not None
        )
        assert "new before" not in json.dumps(metadata)
        actual = writer.apply(prepared, record_intent=lambda _value: None)
        assert actual.receipt.status == "done"
        effect = encode_effect(prepared, actual)
        assert decode_effect(prepared, effect) == actual
        assert "payload" not in effect and "new before" not in json.dumps(effect)
        assert effect["prepared_material_sha256"] == metadata["material_sha256"]
        assert effect["receipt"] == asdict(actual.receipt)
    finally:
        workspace.close()


@pytest.mark.parametrize(
    "fault",
    [
        "version_bool",
        "version",
        "extra",
        "parent",
        "container",
        "owner",
        "offset",
        "length",
        "payload",
        "missing",
        "root",
        "token",
        "material_hash",
    ],
)
def test_prepared_metadata_payload_and_parent_corruption_is_controlled(tmp_path, fault):
    workspace, _, prepared = original(tmp_path)
    try:
        metadata, payload = encode_prepared(prepared)
        value = copy.deepcopy(metadata)
        if fault == "version_bool":
            value["version"] = True
        elif fault == "version":
            value["version"] = 2
        elif fault == "extra":
            value["host_path"] = "/outside"
        elif fault == "parent":
            value["stage"]["parent_identity"][1] += 1
        elif fault == "container":
            value["stage"]["container_identity"] = value["stage"]["parent_identity"]
        elif fault == "owner":
            value["stage"]["container_owner"][0] = True
        elif fault == "offset":
            value["staged"]["offset"] += 1
        elif fault == "length":
            value["before"]["length"] += 1
        elif fault == "payload":
            payload = payload[:-1] + bytes([payload[-1] ^ 1])
        elif fault == "missing":
            value.pop("target_fingerprint")
        elif fault == "root":
            value["stage"]["root_path"] = "."
        elif fault == "token":
            value["stage"]["token"] = "a" * 31
        else:
            value["material_sha256"] = "a" * 64
        if fault not in {"payload", "material_hash"}:
            value["material_sha256"] = _digest(value)
        with pytest.raises(ServiceError) as error:
            decode_prepared(value, payload)
        assert error.value.code == "workspace_directory_restore_corrupt"
    finally:
        workspace.close()


@pytest.mark.parametrize(
    "fault",
    [
        "version_bool",
        "extra",
        "other_prepared",
        "target",
        "after_fingerprint",
        "after_inode",
        "after_missing",
        "root",
        "status",
        "changed",
        "private_field",
    ],
)
def test_effect_is_bound_to_original_material_and_actual_dual_after(tmp_path, fault):
    workspace, writer, prepared = original(tmp_path)
    try:
        actual = writer.apply(prepared, record_intent=lambda _value: None)
        assert actual.receipt.status == "done"
        value = copy.deepcopy(encode_effect(prepared, actual))
        if fault == "version_bool":
            value["version"] = True
        elif fault == "extra":
            value["extra"] = 1
        elif fault == "other_prepared":
            value["prepared_material_sha256"] = "a" * 64
        elif fault == "target":
            value["receipt"]["target_fingerprint"] = "a" * 64
        elif fault == "after_fingerprint":
            value["receipt"]["after_fingerprint"] = "a" * 64
        elif fault == "after_inode":
            observed = actual.observed_after
            first = observed.entries[0]
            other_inode = (
                max(
                    [observed.parent_identity[1], prepared.stage.container_identity[1]]
                    + [(entry.file or entry.directory).inode for entry in observed.entries]
                )
                + 1_000_000
            )
            forged = replace(
                observed,
                entries=(replace(first, directory=replace(first.directory, inode=other_inode)),)
                + observed.entries[1:],
            )
            value["observed_after"] = encode_snapshot(forged)[0]
            value["receipt"]["after_fingerprint"] = forged.fingerprint
        elif fault == "after_missing":
            value["observed_after"] = None
        elif fault == "root":
            value["receipt"]["root_path"] = "another"
        elif fault == "status":
            value["receipt"]["status"] = "rejected"
        elif fault == "changed":
            value["receipt"]["changed"] = False
        else:
            value["receipt"]["token"] = "private"
        value["material_sha256"] = _digest(value)
        with pytest.raises(ServiceError) as error:
            decode_effect(prepared, value)
        assert error.value.code == "workspace_directory_restore_corrupt"
    finally:
        workspace.close()


def test_rejected_and_unknown_effect_roundtrip_without_fabricated_after(tmp_path, monkeypatch):
    workspace, writer, prepared = original(tmp_path)
    try:
        rejected = writer.reconcile(prepared)
        assert rejected.receipt.status == "rejected"
        assert decode_effect(prepared, encode_effect(prepared, rejected)) == rejected
        unknown = replace(
            rejected,
            receipt=replace(
                rejected.receipt, status="uncertain", changed=True, recovery_required=True
            ),
        )
        assert decode_effect(prepared, encode_effect(prepared, unknown)) == unknown
        fabricated = replace(unknown, observed_after=prepared.staged)
        with pytest.raises(ServiceError):
            encode_effect(prepared, fabricated)
        with pytest.raises(ServiceError):
            encode_prepared(False)
        with pytest.raises(ServiceError):
            encode_effect(prepared, False)
    finally:
        workspace.close()
