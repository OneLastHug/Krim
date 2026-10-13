"""单完整实际树快照私有材料；不带Move/Exchange操作语义。"""

from __future__ import annotations

import hashlib
from typing import Literal

from pydantic import Field, ValidationError

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshot

from .tree_exchange_codec import _digest, _json, _Versioned
from .tree_move_codec import (
    MAX_METADATA,
    MAX_NODES,
    MAX_PAYLOAD,
    Digest,
    _decode_tree,
    _encode_tree,
    _Tree,
)


class _Snapshot(_Versioned):
    kind: Literal["workspace-tree-snapshot"]
    tree: _Tree
    payload_size: int = Field(ge=0, le=MAX_PAYLOAD)
    payload_sha256: Digest
    material_sha256: Digest


def _corrupt() -> ServiceError:
    return ServiceError("workspace_tree_snapshot_corrupt", "原完整树快照私有材料损坏或不完整", 409)


def _unique(snapshot: WorkspaceTreeSnapshot) -> None:
    identities = {snapshot.parent_identity}
    for entry in snapshot.entries:
        observed = entry.directory or entry.file
        assert observed is not None
        if observed.identity in identities:
            raise ValueError("完整树不能重复物理节点或包含原父身份")
        identities.add(observed.identity)


def encode_snapshot(snapshot: WorkspaceTreeSnapshot) -> tuple[dict[str, object], bytes]:
    if len(snapshot.entries) > MAX_NODES:
        raise ServiceError("workspace_tree_snapshot_metadata_limit", "完整树节点超过容量", 413)
    if snapshot.total_bytes > MAX_PAYLOAD:
        raise ServiceError("workspace_tree_snapshot_payload_limit", "完整树字节超过容量", 413)
    try:
        _unique(snapshot)
        tree, payload = _encode_tree(snapshot)
        metadata = {
            "version": 1,
            "kind": "workspace-tree-snapshot",
            "tree": tree,
            "payload_size": len(payload),
            "payload_sha256": hashlib.sha256(payload).hexdigest(),
        }
        metadata["material_sha256"] = _digest(metadata)
        if len(_json(metadata)) > MAX_METADATA:
            raise ServiceError(
                "workspace_tree_snapshot_metadata_limit", "完整树元数据超过容量", 413
            )
        if decode_snapshot(metadata, payload) != snapshot:
            raise ValueError()
        return metadata, payload
    except ServiceError as error:
        if error.code in {
            "workspace_tree_snapshot_metadata_limit",
            "workspace_tree_snapshot_payload_limit",
        }:
            raise
        raise _corrupt() from None
    except (ValidationError, ValueError, TypeError, UnicodeError, RecursionError):
        raise _corrupt() from None


def decode_snapshot(metadata: object, payload: bytes) -> WorkspaceTreeSnapshot:
    try:
        if (
            type(metadata) is not dict
            or type(payload) is not bytes
            or len(payload) > MAX_PAYLOAD
            or len(_json(metadata)) > MAX_METADATA
        ):
            raise ValueError()
        value = _Snapshot.model_validate(metadata)
        canonical = value.model_dump(mode="json", exclude_none=True)
        if (
            canonical != metadata
            or value.material_sha256 != _digest(canonical)
            or value.payload_size != len(payload)
            or value.payload_sha256 != hashlib.sha256(payload).hexdigest()
            or len(value.tree.entries) > MAX_NODES
        ):
            raise ValueError()
        snapshot = _decode_tree(value.tree, payload)
        _unique(snapshot)
        return snapshot
    except (ValidationError, ValueError, TypeError, UnicodeError, RecursionError, ServiceError):
        raise _corrupt() from None
