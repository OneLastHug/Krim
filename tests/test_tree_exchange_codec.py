"""真实双before/after私有往返、互反/合计界限与拼接损坏拒绝。"""

import copy
import hashlib
import json
import sys
from dataclasses import replace

import pytest

import krim_agent.persistence.tree_exchange_codec as codec
import krim_agent.workspace_tree_exchange as physical
from krim_agent.persistence.tree_exchange_codec import (
    decode_exchange,
    decode_exchange_effect,
    encode_exchange,
    encode_exchange_effect,
)
from krim_agent.persistence.tree_move_codec import encode_move
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_tree_exchange import PreparedTreeExchange, WorkspaceTreeExchangeWriter

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="真实Linux双树材料")


@pytest.fixture
def prepared(tmp_path):
    (tmp_path / "旧树/sub/empty").mkdir(parents=True)
    (tmp_path / "prepared/sub/empty").mkdir(parents=True)
    (tmp_path / "旧树/sub/blank.bin").write_bytes(b"")
    (tmp_path / "旧树/sub/blob.bin").write_bytes(b"\0\xff\x80")
    (tmp_path / "旧树/sub/text.txt").write_text("原完整树字节\n", encoding="utf-8")
    (tmp_path / "prepared/sub/blob.bin").write_bytes(b"\xff\0new")
    (tmp_path / "prepared/new.txt").write_text("新树\n", encoding="utf-8")
    (tmp_path / "旧树/sub/blob.bin").chmod(0o640)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeExchangeWriter(workspace)
        left = writer.trees.reader.capture("旧树")
        right = writer.trees.reader.capture("prepared")
        change = writer.prepare(
            left.root_path,
            right.root_path,
            expected_left=left.fingerprint,
            expected_right=right.fingerprint,
        )
        yield writer, change


def rehash(value):
    # 合成损坏会重新计算外层摘要，以证明互反/原事实校验不只依赖摘要。
    body = {key: item for key, item in value.items() if key != "material_sha256"}
    value["material_sha256"] = hashlib.sha256(
        json.dumps(body, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode()
    ).hexdigest()


def forbidden(*_args, **_kwargs):
    pytest.fail("私有Codec读取不得执行exchange/apply/命令")


def test_json_roundtrip_preserves_dual_private_binary_identity_versions_and_empty_ranges(
    prepared, monkeypatch
):
    _writer, change = prepared
    metadata, payload = encode_exchange(change)
    monkeypatch.setattr(physical, "rename_at", forbidden)
    monkeypatch.setattr(WorkspaceTreeExchangeWriter, "apply", forbidden)
    decoded = decode_exchange(json.loads(json.dumps(metadata)), payload)
    assert decoded == change and decoded.token == change.token
    expected_left = b"\0\xff\x80" + "原完整树字节\n".encode()
    expected_right = "新树\n".encode() + b"\xff\0new"
    assert payload == expected_left + expected_right
    assert metadata["right"]["offset"] == len(expected_left)
    assert "原完整树字节" not in json.dumps(metadata, ensure_ascii=False)
    files = [
        node for node in metadata["left"]["reference"]["tree"]["entries"] if node["kind"] == "file"
    ]
    assert files[0]["offset"] == files[0]["byte_size"] == 0
    assert files[1]["offset"] == 0 and files[2]["offset"] == 3
    assert metadata["payload_sha256"] == hashlib.sha256(payload).hexdigest()


@pytest.mark.parametrize(
    "fault",
    [
        "version-bool",
        "version-float",
        "version-string",
        "version-next",
        "kind",
        "extra",
        "missing-left",
        "missing-right",
        "nested-extra",
        "nested-missing",
        "nested-null",
        "nested-version",
        "nested-token",
        "top-token",
        "left-offset",
        "right-gap",
        "right-overlap",
        "offset-bool",
        "blob-size",
        "blob-digest",
        "material-digest",
        "target-path",
        "target-parent",
        "same-parent-identity",
        "workspace",
        "duplicate-inode",
    ],
)
def test_corrupt_mixed_or_noncanonical_prepared_material_fails_closed(prepared, fault):
    writer, change = prepared
    metadata, payload = encode_exchange(change)
    value = copy.deepcopy(metadata)
    left = value["left"]["reference"]
    right = value["right"]["reference"]
    if fault.startswith("version-"):
        value["version"] = {"bool": True, "float": 1.0, "string": "1", "next": 2}[fault[8:]]
    elif fault == "kind":
        value["kind"] = "workspace-tree-move"
    elif fault == "extra":
        value["operation"] = "outside"
    elif fault.startswith("missing-"):
        del value[fault[8:]]
    elif fault == "nested-extra":
        right["tree"]["entries"][0]["data"] = "outside"
    elif fault == "nested-missing":
        del right["tree"]["entries"][0]["uid"]
    elif fault == "nested-null":
        right["tree"]["entries"][0]["offset"] = None
    elif fault == "nested-version":
        right["version"] = True
    elif fault == "nested-token":
        right["token"] = "f" * 32 if change.token != "f" * 32 else "0" * 32
    elif fault == "top-token":
        value["token"] = "0" * 32 if change.token != "0" * 32 else "f" * 32
    elif fault == "left-offset":
        value["left"]["offset"] = 1
    elif fault == "right-gap":
        value["right"]["offset"] += 1
    elif fault == "right-overlap":
        value["right"]["offset"] -= 1
    elif fault == "offset-bool":
        value["left"]["offset"] = False
    elif fault == "blob-size":
        value["payload_size"] += 1
    elif fault == "blob-digest":
        value["payload_sha256"] = "0" * 64
    elif fault == "target-path":
        left["target_path"] = "other"
    elif fault == "target-parent":
        left["target_parent"]["inode"] += 1
    else:
        # 每份Move自身仍合法；双树容器必须进一步拒绝这些不可能的关联。
        right_change = writer._moves(change)[1]
        if fault == "same-parent-identity":
            different_parent = (
                change.right.parent_identity[0],
                change.right.parent_identity[1] + 1,
            )
            altered = replace(change.right, parent_identity=different_parent)
            right_change = replace(right_change, before=altered)
            left["target_parent"]["inode"] = different_parent[1]
        elif fault == "workspace":
            right_change = replace(
                right_change, before=replace(change.right, workspace_fingerprint="0" * 64)
            )
        elif fault == "duplicate-inode":
            entry = change.right.entries[0]
            duplicate = replace(entry.directory, inode=change.left.entries[0].directory.inode)
            altered = replace(
                change.right,
                entries=(replace(entry, directory=duplicate), *change.right.entries[1:]),
            )
            right_change = replace(right_change, before=altered)
        elif fault != "material-digest":
            raise AssertionError(fault)
        value["right"]["reference"], right_bytes = encode_move(right_change)
        assert right_bytes == payload[value["right"]["offset"] :]
    rehash(value)
    if fault == "material-digest":
        value["material_sha256"] = "0" * 64
    with pytest.raises(ServiceError) as denied:
        decode_exchange(value, payload)
    assert denied.value.code == "workspace_tree_exchange_corrupt"


@pytest.mark.parametrize("fault", ["mutation", "truncation", "tail", "bytearray"])
def test_dual_blob_damage_never_returns_one_valid_half(prepared, fault):
    _writer, change = prepared
    metadata, payload = encode_exchange(change)
    damaged = {
        "mutation": bytes([payload[0] ^ 255]) + payload[1:],
        "truncation": payload[:-1],
        "tail": payload + b"x",
        "bytearray": bytearray(payload),
    }[fault]
    with pytest.raises(ServiceError, match="私有材料损坏"):
        decode_exchange(metadata, damaged)


@pytest.mark.parametrize("limit", ["nodes", "payload", "metadata"])
def test_combined_limits_accept_exact_boundary_and_reject_two_individually_valid_halves(
    prepared, monkeypatch, limit
):
    _writer, change = prepared
    metadata, payload = encode_exchange(change)
    size = {
        "nodes": len(change.left.entries) + len(change.right.entries),
        "payload": len(payload),
        "metadata": len(
            json.dumps(metadata, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode()
        ),
    }[limit]
    name = {"nodes": "MAX_NODES", "payload": "MAX_PAYLOAD", "metadata": "MAX_METADATA"}[limit]
    monkeypatch.setattr(codec, name, size)
    assert encode_exchange(change) == (metadata, payload)
    assert decode_exchange(metadata, payload) == change
    monkeypatch.setattr(codec, name, size - 1)
    with pytest.raises(ServiceError) as over:
        encode_exchange(change)
    assert over.value.status == 413
    with pytest.raises(ServiceError) as corrupt:
        decode_exchange(metadata, payload)
    assert corrupt.value.code == "workspace_tree_exchange_corrupt"


def test_done_json_roundtrip_is_bound_to_original_before_and_actual_both_after(
    prepared, monkeypatch
):
    writer, change = prepared
    effect = writer.apply(change)
    assert effect.receipt.status == "done"
    metadata, payload = encode_exchange_effect(change, effect)
    monkeypatch.setattr(physical, "rename_at", forbidden)
    monkeypatch.setattr(WorkspaceTreeExchangeWriter, "apply", forbidden)
    decoded = decode_exchange_effect(change, json.loads(json.dumps(metadata)), payload)
    assert decoded == effect
    assert (
        decoded.observed_left.entries[0].directory.inode == change.right.entries[0].directory.inode
    )
    assert (
        decoded.observed_right.entries[0].directory.inode == change.left.entries[0].directory.inode
    )
    assert decoded.receipt.left_after_fingerprint != change.right.fingerprint
    assert writer.prepare_undo(change, decoded).left == decoded.observed_left
    with pytest.raises(ServiceError) as another:
        decode_exchange_effect(replace(change, token="f" * 32), metadata, payload)
    assert another.value.code == "workspace_tree_exchange_corrupt"


@pytest.mark.parametrize(
    "fault",
    [
        "half",
        "missing",
        "before",
        "after",
        "token",
        "changed",
        "recovery",
        "error",
        "workspace",
        "anchor",
        "version",
        "tail",
    ],
)
def test_done_requires_original_receipt_and_complete_actual_dual_after(prepared, fault):
    writer, change = prepared
    effect = writer.apply(change)
    metadata, payload = encode_exchange_effect(change, effect)
    value = copy.deepcopy(metadata)
    receipt = value["receipt"]
    if fault == "half":
        del value["observed"]["right"]
        rehash(value["observed"])
    elif fault == "missing":
        value["observed"] = None
    elif fault == "before":
        receipt["right_before_fingerprint"] = "0" * 64
    elif fault == "after":
        receipt["left_after_fingerprint"] = change.right.fingerprint
    elif fault == "token":
        receipt["operation_token"] = "f" * 32
    elif fault == "changed":
        receipt["changed"] = False
    elif fault == "recovery":
        receipt["recovery_required"] = True
    elif fault == "error":
        receipt["error_code"] = "pretended_failure"
    elif fault == "workspace":
        value["workspace_fingerprint"] = "0" * 64
    elif fault == "anchor":
        value["prepared_material_sha256"] = "0" * 64
    elif fault == "version":
        value["version"] = 1.0
    else:
        payload += b"x"
        value["payload_size"] = len(payload)
        value["payload_sha256"] = hashlib.sha256(payload).hexdigest()
    rehash(value)
    with pytest.raises(ServiceError) as denied:
        decode_exchange_effect(change, value, payload)
    assert denied.value.code == "workspace_tree_exchange_corrupt"


def test_encode_refuses_half_or_forged_after_even_when_its_fingerprints_are_consistent(prepared):
    writer, change = prepared
    effect = writer.apply(change)
    with pytest.raises(ServiceError):
        encode_exchange_effect(change, replace(effect, observed_right=None))
    position = next(
        i for i, entry in enumerate(effect.observed_left.entries) if entry.file is not None
    )
    entry = effect.observed_left.entries[position]
    altered_file = replace(entry.file, data=b"forged bytes")
    altered_left = replace(
        effect.observed_left,
        entries=tuple(
            replace(item, file=altered_file) if i == position else item
            for i, item in enumerate(effect.observed_left.entries)
        ),
    )
    fake = replace(
        effect,
        observed_left=altered_left,
        receipt=replace(effect.receipt, left_after_fingerprint=altered_left.fingerprint),
    )
    with pytest.raises(ServiceError) as denied:
        encode_exchange_effect(change, fake)
    assert denied.value.code == "workspace_tree_exchange_corrupt"


@pytest.mark.parametrize("status", ["rejected", "uncertain"])
def test_unfinished_effect_preserves_original_status_error_and_no_partial_after(prepared, status):
    writer, change = prepared
    effect = writer._effect(change, status, code="original_error", changed=status == "uncertain")
    metadata, payload = encode_exchange_effect(change, effect)
    assert payload == b"" and metadata["observed"] is None
    assert decode_exchange_effect(change, json.loads(json.dumps(metadata)), payload) == effect
    wrong = replace(
        effect, receipt=replace(effect.receipt, recovery_required=status != "uncertain")
    )
    with pytest.raises(ServiceError):
        encode_exchange_effect(change, wrong)
    with pytest.raises(ServiceError):
        encode_exchange_effect(change, replace(effect, observed_left=change.left))


def test_private_metadata_limits_also_cover_the_outer_effect_container(prepared, monkeypatch):
    writer, change = prepared
    effect = writer.apply(change)
    metadata, payload = encode_exchange_effect(change, effect)
    actual_size = len(
        json.dumps(metadata, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode()
    )
    monkeypatch.setattr(codec, "MAX_METADATA", actual_size)
    assert encode_exchange_effect(change, effect) == (metadata, payload)
    monkeypatch.setattr(codec, "MAX_METADATA", actual_size - 1)
    with pytest.raises(ServiceError) as over:
        encode_exchange_effect(change, effect)
    assert over.value.status == 413
    with pytest.raises(ServiceError):
        decode_exchange_effect(change, metadata, payload)


def test_swapping_two_complete_sides_without_original_correlation_is_rejected(prepared):
    writer, change = prepared
    effect = writer.apply(change)
    metadata, payload = encode_exchange_effect(change, effect)
    unrelated, replacement_payload = encode_exchange(
        PreparedTreeExchange(change.left, change.right, change.token)
    )
    value = copy.deepcopy(metadata)
    value["observed"] = unrelated
    value["receipt"]["left_after_fingerprint"] = change.left.fingerprint
    value["receipt"]["right_after_fingerprint"] = change.right.fingerprint
    value["payload_size"] = len(replacement_payload)
    value["payload_sha256"] = hashlib.sha256(replacement_payload).hexdigest()
    rehash(value)
    with pytest.raises(ServiceError) as denied:
        decode_exchange_effect(change, value, replacement_payload)
    assert denied.value.code == "workspace_tree_exchange_corrupt"
