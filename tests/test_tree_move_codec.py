"""原树私有metadata/BLOB严格往返、损坏/重定位/容量拒绝，不读外来代码。"""

import copy
import hashlib
import json
from dataclasses import replace

import pytest
from test_nonempty_tree_writer import prepare
from test_workspace_tree_snapshot import seed

import krim_agent.persistence.tree_move_codec as codec
from krim_agent.persistence.tree_move_codec import decode_move, encode_move
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_tree_snapshot import TreeSnapshotEntry
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter


def original(root):
    seed(root)
    (root / "dest").mkdir()
    with Workspace(root, writable=True) as workspace:
        return prepare(WorkspaceTreeWriter(workspace))


def test_exact_json_roundtrip_preserves_private_binary_all_identities_and_empty_offsets(tmp_path):
    change = original(tmp_path)
    metadata, payload = encode_move(change)
    parsed = json.loads(json.dumps(metadata))
    assert decode_move(parsed, payload) == change
    assert payload == b"\0\xff\x80" + "目录树原字节\n".encode()
    assert "目录树原字节" not in json.dumps(metadata, ensure_ascii=False)
    files = [item for item in metadata["tree"]["entries"] if item["kind"] == "file"]
    assert files[0]["offset"] == files[0]["byte_size"] == 0
    assert files[1]["offset"] == 0 and files[2]["offset"] == 3
    assert metadata["payload_sha256"] == hashlib.sha256(payload).hexdigest()


@pytest.mark.parametrize(
    "fault",
    [
        "version-bool",
        "version-string",
        "version-unknown",
        "extra",
        "node-extra",
        "token",
        "target-self",
        "target-absolute",
        "parent-bool",
        "cross-device",
        "tree-fingerprint",
        "payload-hash",
        "payload-size",
        "node-offset",
        "node-hash",
        "node-size",
        "file-link",
        "file-empty",
        "directory-payload",
        "duplicate",
        "parent-missing",
    ],
)
def test_corrupt_or_relocated_private_material_is_rejected(tmp_path, fault):
    metadata, payload = encode_move(original(tmp_path))
    value = copy.deepcopy(metadata)
    file = next(item for item in value["tree"]["entries"] if item["kind"] == "file")
    root = value["tree"]["entries"][0]
    if fault == "version-bool":
        value["version"] = True
    elif fault == "version-string":
        value["version"] = "1"
    elif fault == "version-unknown":
        value["version"] = 2
    elif fault == "extra":
        value["owner"] = "outside"
    elif fault == "node-extra":
        file["data"] = "outside"
    elif fault == "token":
        value["token"] = "invalid"
    elif fault == "target-self":
        value["target_path"] = "tree/sub"
    elif fault == "target-absolute":
        value["target_path"] = "/tmp/tree"
    elif fault == "parent-bool":
        value["tree"]["parent"]["inode"] = True
    elif fault == "cross-device":
        value["target_parent"]["device"] += 1
    elif fault == "tree-fingerprint":
        value["tree"]["fingerprint"] = "0" * 64
    elif fault == "payload-hash":
        value["payload_sha256"] = "0" * 64
    elif fault == "payload-size":
        value["payload_size"] += 1
    elif fault == "node-offset":
        file["offset"] = 1
    elif fault == "node-hash":
        file["sha256"] = "0" * 64
    elif fault == "node-size":
        file["byte_size"] = 1
    elif fault == "file-link":
        file["nlink"] = 2
    elif fault == "file-empty":
        file["empty"] = False
    elif fault == "directory-payload":
        root["offset"] = 0
    elif fault == "duplicate":
        value["tree"]["entries"].append(root.copy())
    elif fault == "parent-missing":
        file["path"] = "tree/missing/file"
    with pytest.raises(ServiceError) as denied:
        decode_move(value, payload)
    assert denied.value.code == "workspace_tree_move_corrupt"


@pytest.mark.parametrize("payload_fault", ["changed", "extra", "truncated", "bytearray"])
def test_private_blob_mutation_never_returns_partial_original_tree(tmp_path, payload_fault):
    metadata, payload = encode_move(original(tmp_path))
    damaged = {
        "changed": bytes([payload[0] ^ 255]) + payload[1:],
        "extra": payload + b"x",
        "truncated": payload[:-1],
        "bytearray": bytearray(payload),
    }[payload_fault]
    with pytest.raises(ServiceError) as denied:
        decode_move(metadata, damaged)
    assert denied.value.code == "workspace_tree_move_corrupt"


def test_changed_empty_flag_with_recomputed_checksum_still_fails_closed(tmp_path):
    change = original(tmp_path)
    root = change.before.entries[0]
    altered = TreeSnapshotEntry(root.path, directory=replace(root.directory, empty=True))
    fake = replace(
        change, before=replace(change.before, entries=(altered, *change.before.entries[1:]))
    )
    with pytest.raises(ServiceError) as denied:
        encode_move(fake)
    assert denied.value.code == "workspace_tree_move_corrupt"


def test_metadata_limit_closes_decode_before_schema_and_leaves_binary_untouched(tmp_path):
    metadata, payload = encode_move(original(tmp_path))
    metadata["oversize"] = "x" * codec.MAX_METADATA
    with pytest.raises(ServiceError) as denied:
        decode_move(metadata, payload)
    assert denied.value.code == "workspace_tree_move_corrupt" and payload.startswith(b"\0\xff\x80")
