"""非空树移动私有metadata+BLOB；严格有界，原字节/指纹/父闭包核实。"""

from __future__ import annotations

import hashlib
import json
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_directory import DirectorySnapshot
from krim_agent.workspace_tree_snapshot import TreeSnapshotEntry, WorkspaceTreeSnapshot
from krim_agent.workspace_tree_writer import PreparedTreeMove, WorkspaceTreeWriter
from krim_agent.workspace_writer import FileSnapshot

Unsigned = Annotated[int, Field(ge=0, le=18_446_744_073_709_551_615)]
Timestamp = Annotated[int, Field(ge=-9_223_372_036_854_775_808, le=9_223_372_036_854_775_807)]
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
MAX_METADATA = 2_097_152
MAX_PAYLOAD = 134_217_728
MAX_NODES = 16384


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class _Identity(_Model):
    device: Unsigned
    inode: Unsigned


class _Node(_Model):
    path: str = Field(min_length=1, max_length=512)
    kind: Literal["directory", "file"]
    mode: int = Field(ge=0, le=511)
    device: Unsigned
    inode: Unsigned
    mtime_ns: Timestamp
    ctime_ns: Timestamp
    uid: Unsigned
    gid: Unsigned
    nlink: int = Field(ge=1, le=18_446_744_073_709_551_615)
    empty: bool | None = None
    sha256: Digest | None = None
    byte_size: int | None = Field(default=None, ge=0, le=8_388_608)
    offset: int | None = Field(default=None, ge=0, le=MAX_PAYLOAD)


class _Tree(_Model):
    root_path: str = Field(min_length=1, max_length=512)
    workspace_fingerprint: Digest
    parent: _Identity
    fingerprint: Digest
    entries: list[_Node] = Field(min_length=1, max_length=MAX_NODES)


class _Move(_Model):
    version: Literal[1]
    target_path: str = Field(min_length=1, max_length=512)
    target_parent: _Identity
    token: str = Field(pattern=r"^[0-9a-f]{32}$")
    tree: _Tree
    payload_size: int = Field(ge=0, le=MAX_PAYLOAD)
    payload_sha256: Digest

    @field_validator("version", mode="before")
    @classmethod
    def exact_version(cls, value: object) -> int:
        if type(value) is not int or value != 1:
            raise ValueError("私有树版本须为整数1")
        return value


def _corrupt() -> ServiceError:
    return ServiceError("workspace_tree_move_corrupt", "原树移动私有材料损坏或不完整", 409)


def _encode_tree(snapshot: WorkspaceTreeSnapshot) -> tuple[dict[str, object], bytes]:
    if not 1 <= len(snapshot.entries) <= MAX_NODES:
        raise ServiceError("workspace_tree_move_metadata_limit", "原树节点超过容量", 413)
    chunks = []
    nodes = []
    offset = 0
    for entry in snapshot.entries:
        node = entry.metadata()
        if entry.file is not None:
            node["offset"] = offset
            offset += len(entry.file.data)
            if offset > MAX_PAYLOAD:
                raise ServiceError("workspace_tree_move_payload_limit", "原树私有字节超过容量", 413)
            chunks.append(entry.file.data)
        nodes.append(node)
    payload = b"".join(chunks)
    tree: dict[str, object] = {
        "root_path": snapshot.root_path,
        "workspace_fingerprint": snapshot.workspace_fingerprint,
        "parent": {"device": snapshot.parent_identity[0], "inode": snapshot.parent_identity[1]},
        "fingerprint": snapshot.fingerprint,
        "entries": nodes,
    }
    return tree, payload


def _decode_tree(value: _Tree, payload: bytes) -> WorkspaceTreeSnapshot:
    entries = []
    offset = 0
    for node in value.entries:
        if node.kind == "directory":
            if node.empty is None or any(
                item is not None for item in (node.sha256, node.byte_size, node.offset)
            ):
                raise ValueError()
            directory = DirectorySnapshot(
                node.mode,
                node.device,
                node.inode,
                node.mtime_ns,
                node.ctime_ns,
                node.uid,
                node.gid,
                node.nlink,
                node.empty,
            )
            entries.append(TreeSnapshotEntry(node.path, directory=directory))
        else:
            if (
                node.empty is not None
                or node.sha256 is None
                or node.byte_size is None
                or node.offset != offset
                or node.nlink != 1
                or offset + node.byte_size > len(payload)
            ):
                raise ValueError()
            data = payload[offset : offset + node.byte_size]
            offset += node.byte_size
            if node.sha256 != hashlib.sha256(data).hexdigest():
                raise ValueError()
            file = FileSnapshot(
                data,
                node.mode,
                node.device,
                node.inode,
                node.mtime_ns,
                node.ctime_ns,
                node.uid,
                node.gid,
            )
            entries.append(TreeSnapshotEntry(node.path, file=file))
    if offset != len(payload):
        raise ValueError()
    snapshot = WorkspaceTreeSnapshot(
        value.root_path,
        value.workspace_fingerprint,
        (value.parent.device, value.parent.inode),
        tuple(entries),
    )
    paths = [entry.path for entry in snapshot.entries]
    if paths != sorted(paths, key=lambda path: tuple(path.split("/"))):
        raise ValueError()
    parent_paths = {path.rpartition("/")[0] for path in paths[1:]}
    for entry in snapshot.entries:
        observed = entry.directory or entry.file
        assert observed is not None
        if observed.device != snapshot.parent_identity[0]:
            raise ValueError()
        if entry.directory is not None and entry.directory.empty != (
            entry.path not in parent_paths
        ):
            raise ValueError()
    if snapshot.fingerprint != value.fingerprint:
        raise ValueError()
    return snapshot


def encode_move(change: PreparedTreeMove) -> tuple[dict[str, object], bytes]:
    tree, payload = _encode_tree(change.before)
    metadata = {
        "version": 1,
        "target_path": change.target_path,
        "target_parent": {
            "device": change.target_parent_identity[0],
            "inode": change.target_parent_identity[1],
        },
        "token": change.token,
        "tree": tree,
        "payload_size": len(payload),
        "payload_sha256": hashlib.sha256(payload).hexdigest(),
    }
    try:
        checked = _Move.model_validate(metadata)
        encoded = checked.model_dump(mode="json", exclude_none=True)
        if (
            len(json.dumps(encoded, ensure_ascii=True, separators=(",", ":")).encode())
            > MAX_METADATA
        ):
            raise ServiceError("workspace_tree_move_metadata_limit", "原树元数据超过容量", 413)
        if decode_move(encoded, payload) != change:
            raise _corrupt()
        return encoded, payload
    except (ValidationError, ValueError, TypeError):
        raise _corrupt() from None


def decode_move(metadata: object, payload: bytes) -> PreparedTreeMove:
    try:
        if type(payload) is not bytes or len(payload) > MAX_PAYLOAD:
            raise ValueError()
        if (
            len(
                json.dumps(
                    metadata, ensure_ascii=True, allow_nan=False, separators=(",", ":")
                ).encode()
            )
            > MAX_METADATA
        ):
            raise ValueError()
        value = _Move.model_validate(metadata)
        if (
            value.payload_size != len(payload)
            or value.payload_sha256 != hashlib.sha256(payload).hexdigest()
        ):
            raise ValueError()
        snapshot = _decode_tree(value.tree, payload)
        WorkspaceTreeWriter._paths(snapshot.root_path, value.target_path)
        if snapshot.parent_identity[0] != value.target_parent.device:
            raise ValueError()
        return PreparedTreeMove(
            snapshot,
            value.target_path,
            (value.target_parent.device, value.target_parent.inode),
            value.token,
        )
    except (
        ValidationError,
        ValueError,
        TypeError,
        KeyError,
        UnicodeError,
        RecursionError,
        ServiceError,
    ):
        raise _corrupt() from None
