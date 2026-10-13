"""独立完整snapshot严格材料与原Move/Exchange格式逐字节兼容。"""

import copy
import hashlib
import json
import subprocess
from dataclasses import replace

import pytest
from test_workspace_tree_snapshot import seed

import krim_agent.persistence.tree_snapshot_codec as codec
from krim_agent.persistence.tree_move_codec import encode_move
from krim_agent.persistence.tree_snapshot_codec import decode_snapshot, encode_snapshot
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader
from krim_agent.workspace_tree_writer import PreparedTreeMove


def original(root):
    seed(root)
    with Workspace(root, writable=True) as workspace:
        return WorkspaceTreeSnapshotReader(workspace).capture("tree")


def rehash(value):
    value["material_sha256"] = hashlib.sha256(
        json.dumps(
            {key: item for key, item in value.items() if key != "material_sha256"},
            sort_keys=True,
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def test_snapshot_json_binary_all_versions_roundtrip_has_no_operation_reference(tmp_path):
    snapshot = original(tmp_path)
    metadata, payload = encode_snapshot(snapshot)
    assert decode_snapshot(json.loads(json.dumps(metadata)), payload) == snapshot
    assert payload == b"\0\xff\x80" + "目录树原字节\n".encode()
    assert "token" not in metadata and "target_path" not in metadata
    assert "目录树原字节" not in json.dumps(metadata, ensure_ascii=False)


def test_empty_complete_root_has_a_real_directory_and_zero_blob(tmp_path):
    (tmp_path / "empty").mkdir()
    with Workspace(tmp_path) as workspace:
        snapshot = WorkspaceTreeSnapshotReader(workspace).capture("empty")
    metadata, blob = encode_snapshot(snapshot)
    assert blob == b"" and len(snapshot.entries) == 1 and snapshot.entries[0].directory.empty
    assert decode_snapshot(json.loads(json.dumps(metadata)), blob) == snapshot


@pytest.mark.parametrize(
    "fault",
    [
        "bool-version",
        "float-version",
        "kind",
        "extra",
        "missing",
        "null-node",
        "file-offset",
        "file-sha",
        "parent-closure",
        "tree-sha",
        "blob-sha",
        "blob-size",
        "material-sha",
        "mode",
        "device",
        "node-order",
    ],
)
def test_new_material_damage_or_recomputed_metadata_never_returns_partial_tree(tmp_path, fault):
    metadata, payload = encode_snapshot(original(tmp_path))
    value = copy.deepcopy(metadata)
    node = next(item for item in value["tree"]["entries"] if item["kind"] == "file")
    if fault == "bool-version":
        value["version"] = True
    elif fault == "float-version":
        value["version"] = 1.0
    elif fault == "kind":
        value["kind"] = "workspace-tree-snapshot\n"
    elif fault == "extra":
        value["target_path"] = "outside"
    elif fault == "missing":
        del value["tree"]["parent"]
    elif fault == "null-node":
        node["empty"] = None
    elif fault == "file-offset":
        node["offset"] += 1
    elif fault == "file-sha":
        node["sha256"] = "0" * 64
    elif fault == "parent-closure":
        node["path"] = "tree/missing/file"
    elif fault == "tree-sha":
        value["tree"]["fingerprint"] = "0" * 64
    elif fault == "blob-sha":
        value["payload_sha256"] = "0" * 64
    elif fault == "blob-size":
        value["payload_size"] += 1
    elif fault == "mode":
        node["mode"] = True
    elif fault == "device":
        node["device"] += 1
    elif fault == "node-order":
        value["tree"]["entries"].reverse()
    rehash(value)
    if fault == "material-sha":
        value["material_sha256"] = "0" * 64
    with pytest.raises(ServiceError) as rejected:
        decode_snapshot(value, payload)
    assert rejected.value.code == "workspace_tree_snapshot_corrupt"


@pytest.mark.parametrize("fault", ["changed", "tail", "short", "bytearray"])
def test_binary_blob_damage_fails_closed(tmp_path, fault):
    metadata, payload = encode_snapshot(original(tmp_path))
    bad = {
        "changed": b"x" + payload[1:],
        "tail": payload + b"x",
        "short": payload[:-1],
        "bytearray": bytearray(payload),
    }[fault]
    with pytest.raises(ServiceError):
        decode_snapshot(metadata, bad)


def test_snapshot_rejects_duplicated_physical_identity_even_when_fingerprint_is_recomputed(
    tmp_path,
):
    snapshot = original(tmp_path)
    root = snapshot.entries[0]
    entry = next(item for item in snapshot.entries[1:] if item.directory is not None)
    replaced = replace(entry, directory=replace(entry.directory, inode=root.directory.inode))
    altered = replace(
        snapshot,
        entries=tuple(replaced if item.path == entry.path else item for item in snapshot.entries),
    )
    with pytest.raises(ServiceError):
        encode_snapshot(altered)


@pytest.mark.parametrize("budget", ["nodes", "bytes", "metadata"])
def test_exact_bounds_and_over_budget_encode_and_decode(tmp_path, monkeypatch, budget):
    snapshot = original(tmp_path)
    metadata, payload = encode_snapshot(snapshot)
    size = {
        "nodes": len(snapshot.entries),
        "bytes": len(payload),
        "metadata": len(
            json.dumps(metadata, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode()
        ),
    }[budget]
    name = {"nodes": "MAX_NODES", "bytes": "MAX_PAYLOAD", "metadata": "MAX_METADATA"}[budget]
    monkeypatch.setattr(codec, name, size)
    assert encode_snapshot(snapshot) == (metadata, payload)
    monkeypatch.setattr(codec, name, size - 1)
    with pytest.raises(ServiceError) as full:
        encode_snapshot(snapshot)
    assert full.value.status == 413
    with pytest.raises(ServiceError):
        decode_snapshot(metadata, payload)


def test_extracted_helpers_preserve_real_published_move_metadata_and_blob_byte_for_byte(tmp_path):
    snapshot = original(tmp_path)
    change = PreparedTreeMove(snapshot, "dest", snapshot.parent_identity, "1" * 32)
    # 从实际已发布6f7fc32加载旧纯Codec；只用于测试比较，不在产品执行外来材料。
    old_source = subprocess.check_output(
        [
            "git",
            "show",
            "6f7fc32175b3302969f1adde26316f684eeec0c6:src/krim_agent/persistence/tree_move_codec.py",
        ],
        text=True,
        cwd="/workspace/Krim",
    )
    namespace = {"__name__": "krim_old_move_codec_check"}
    exec(compile(old_source, "published-tree-move-codec", "exec"), namespace)
    for model in ("_Identity", "_Node", "_Tree", "_Move"):
        namespace[model].model_rebuild(force=True, _types_namespace=namespace)
    assert encode_move(change) == namespace["encode_move"](change)
    metadata, payload = encode_move(change)
    assert namespace["decode_move"](metadata, payload) == change
