"""目录恢复完整私有准备和双after；effect复用原BLOB，不再复制相同文件字节。"""

from __future__ import annotations

import hashlib
from dataclasses import asdict
from typing import Annotated, Literal

from pydantic import Field, JsonValue

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_directory_restore import DirectoryRestoreReceiptView
from krim_agent.workspace_directory_restore_writer import (
    DirectoryRestoreEffect,
    DirectoryRestoreReceipt,
    DirectoryRestoreStage,
    PreparedDirectoryRestore,
    WorkspaceDirectoryRestoreWriter,
)
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshot, _path

from .tree_exchange_codec import _digest, _json, _Model, _Versioned
from .tree_snapshot_codec import decode_snapshot, encode_snapshot

MAX_METADATA = 4_194_304
MAX_PAYLOAD = 67_108_864
Digest = Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]
Number = Annotated[int, Field(ge=0)]
Owner = Annotated[int, Field(ge=0, lt=2**32 - 1)]


class _Stage(_Model):
    root_path: str = Field(min_length=1, max_length=512)
    token: str = Field(pattern=r"^[a-f0-9]{32}$")
    parent_identity: list[Number] = Field(min_length=2, max_length=2)
    container_identity: list[Number] = Field(min_length=2, max_length=2)
    container_owner: list[Owner] = Field(min_length=2, max_length=2)


class _Side(_Model):
    metadata: dict[str, JsonValue]
    offset: int = Field(ge=0, le=MAX_PAYLOAD)
    length: int = Field(ge=0, le=33_554_432)


class _Prepared(_Versioned):
    kind: Literal["workspace-directory-restore"]
    stage: _Stage
    target_fingerprint: Digest
    before: _Side
    staged: _Side
    payload_size: int = Field(ge=0, le=MAX_PAYLOAD)
    payload_sha256: Digest
    material_sha256: Digest


class _StageRecord(_Versioned):
    kind: Literal["workspace-directory-restore-stage"]
    plan_hash: Digest
    stage: _Stage
    material_sha256: Digest


def encode_stage(stage: DirectoryRestoreStage, plan_hash: str) -> dict[str, object]:
    try:
        if type(stage) is not DirectoryRestoreStage or any(
            type(value) is not tuple or len(value) != 2
            for value in (stage.parent_identity, stage.container_identity, stage.container_owner)
        ):
            raise _corrupt()
        value: dict[str, object] = {
            "version": 1,
            "kind": "workspace-directory-restore-stage",
            "plan_hash": plan_hash,
            "stage": {
                "root_path": stage.root_path,
                "token": stage.token,
                "parent_identity": list(stage.parent_identity),
                "container_identity": list(stage.container_identity),
                "container_owner": list(stage.container_owner),
            },
        }
        value["material_sha256"] = _digest(value)
        if decode_stage(value, plan_hash) != stage:
            raise _corrupt()
        return value
    except (ValueError, TypeError, AttributeError, ServiceError):
        raise _corrupt() from None


def decode_stage(metadata: object, plan_hash: str) -> DirectoryRestoreStage:
    try:
        if type(metadata) is not dict or len(_json(metadata)) > 4096:
            raise _corrupt()
        value = _StageRecord.model_validate(metadata)
        canonical = value.model_dump(mode="json")
        if (
            canonical != metadata
            or value.material_sha256 != _digest(canonical)
            or value.plan_hash != plan_hash
        ):
            raise _corrupt()
        stage = value.stage
        _path(stage.root_path)
        if (
            stage.parent_identity == stage.container_identity
            or stage.parent_identity[0] != stage.container_identity[0]
        ):
            raise _corrupt()
        return DirectoryRestoreStage(
            stage.root_path,
            stage.token,
            (stage.parent_identity[0], stage.parent_identity[1]),
            (stage.container_identity[0], stage.container_identity[1]),
            (stage.container_owner[0], stage.container_owner[1]),
        )
    except (ValueError, TypeError, AttributeError, ServiceError):
        raise _corrupt() from None


class _Effect(_Versioned):
    kind: Literal["workspace-directory-restore-effect"]
    prepared_material_sha256: Digest
    receipt: DirectoryRestoreReceiptView
    observed_after: dict[str, JsonValue] | None
    retained_before: dict[str, JsonValue] | None
    material_sha256: Digest


def _corrupt() -> ServiceError:
    return ServiceError(
        "workspace_directory_restore_corrupt", "原目录恢复私有材料损坏或关联不完整", 409
    )


def _tree(snapshot: WorkspaceTreeSnapshot) -> set[tuple[int, int]]:
    if (
        type(snapshot) is not WorkspaceTreeSnapshot
        or len(snapshot.entries) > 1024
        or snapshot.total_bytes > 33_554_432
    ):
        raise _corrupt()
    base = len(_path(snapshot.root_path))
    result: set[tuple[int, int]] = set()
    for entry in snapshot.entries:
        if len(_path(entry.path)) - base + 1 > 64:
            raise _corrupt()
        if entry.file is not None and len(entry.file.data) > 8_388_608:
            raise _corrupt()
        item = entry.file or entry.directory
        assert item is not None
        result.add(item.identity)
    return result


def _pair(change: PreparedDirectoryRestore) -> None:
    if (
        type(change) is not PreparedDirectoryRestore
        or type(change.stage) is not DirectoryRestoreStage
    ):
        raise _corrupt()
    stage = change.stage
    for identity in (stage.parent_identity, stage.container_identity, stage.container_owner):
        if type(identity) is not tuple or len(identity) != 2:
            raise _corrupt()
    _Stage.model_validate(
        {
            "root_path": stage.root_path,
            "token": stage.token,
            "parent_identity": list(stage.parent_identity),
            "container_identity": list(stage.container_identity),
            "container_owner": list(stage.container_owner),
        }
    )
    before, staged = _tree(change.before), _tree(change.staged)
    if (
        stage.root_path != change.before.root_path
        or stage.root_path != change.staged.root_path
        or change.before.workspace_fingerprint != change.staged.workspace_fingerprint
        or stage.parent_identity != change.before.parent_identity
        or stage.container_identity != change.staged.parent_identity
        or stage.parent_identity == stage.container_identity
        or stage.parent_identity[0] != stage.container_identity[0]
        or before.intersection(staged)
        or {stage.parent_identity, stage.container_identity}.intersection(before | staged)
    ):
        raise _corrupt()


def encode_prepared(change: PreparedDirectoryRestore) -> tuple[dict[str, object], bytes]:
    try:
        _pair(change)
        before, left = encode_snapshot(change.before)
        staged, right = encode_snapshot(change.staged)
        stage = change.stage
        value: dict[str, object] = {
            "version": 1,
            "kind": "workspace-directory-restore",
            "stage": {
                "root_path": stage.root_path,
                "token": stage.token,
                "parent_identity": list(stage.parent_identity),
                "container_identity": list(stage.container_identity),
                "container_owner": list(stage.container_owner),
            },
            "target_fingerprint": change.target_fingerprint,
            "before": {"metadata": before, "offset": 0, "length": len(left)},
            "staged": {"metadata": staged, "offset": len(left), "length": len(right)},
            "payload_size": len(left) + len(right),
            "payload_sha256": hashlib.sha256(left + right).hexdigest(),
        }
        value["material_sha256"] = _digest(value)
        payload = left + right
        if decode_prepared(value, payload) != change:
            raise _corrupt()
        return value, payload
    except (ValueError, TypeError, AttributeError, AssertionError, RecursionError, ServiceError):
        raise _corrupt() from None


def decode_prepared(metadata: object, payload: bytes) -> PreparedDirectoryRestore:
    try:
        if (
            type(metadata) is not dict
            or type(payload) is not bytes
            or len(_json(metadata)) > MAX_METADATA
            or len(payload) > MAX_PAYLOAD
        ):
            raise _corrupt()
        value = _Prepared.model_validate(metadata)
        canonical = value.model_dump(mode="json")
        if (
            canonical != metadata
            or value.material_sha256 != _digest(canonical)
            or value.payload_size != len(payload)
            or value.payload_sha256 != hashlib.sha256(payload).hexdigest()
            or value.before.offset != 0
            or value.staged.offset != value.before.length
            or value.staged.offset + value.staged.length != len(payload)
        ):
            raise _corrupt()
        before = decode_snapshot(value.before.metadata, payload[: value.staged.offset])
        staged = decode_snapshot(value.staged.metadata, payload[value.staged.offset :])
        stage = value.stage
        result = PreparedDirectoryRestore(
            DirectoryRestoreStage(
                stage.root_path,
                stage.token,
                (stage.parent_identity[0], stage.parent_identity[1]),
                (stage.container_identity[0], stage.container_identity[1]),
                (stage.container_owner[0], stage.container_owner[1]),
            ),
            value.target_fingerprint,
            before,
            staged,
        )
        _pair(result)
        return result
    except (
        ValueError,
        TypeError,
        AttributeError,
        AssertionError,
        KeyError,
        RecursionError,
        ServiceError,
    ):
        raise _corrupt() from None


def _effect(change: PreparedDirectoryRestore, result: DirectoryRestoreEffect) -> None:
    if (
        type(result) is not DirectoryRestoreEffect
        or type(result.receipt) is not DirectoryRestoreReceipt
    ):
        raise _corrupt()
    receipt = DirectoryRestoreReceiptView.model_validate(asdict(result.receipt))
    if (
        receipt.root_path != change.stage.root_path
        or receipt.before_fingerprint != change.before.fingerprint
        or receipt.target_fingerprint != change.target_fingerprint
    ):
        raise _corrupt()
    if receipt.status == "done":
        after, retained = result.observed_after, result.retained_before
        if (
            after is None
            or retained is None
            or receipt.after_fingerprint != after.fingerprint
            or receipt.retained_fingerprint != retained.fingerprint
            or not WorkspaceDirectoryRestoreWriter._moved(
                change.staged, after, change.stage.parent_identity
            )
            or not WorkspaceDirectoryRestoreWriter._moved(
                change.before, retained, change.stage.container_identity
            )
        ):
            raise _corrupt()
    elif result.observed_after is not None or result.retained_before is not None:
        raise _corrupt()


def encode_effect(
    change: PreparedDirectoryRestore, result: DirectoryRestoreEffect
) -> dict[str, object]:
    try:
        prepared, _ = encode_prepared(change)
        _effect(change, result)
        after, retained = None, None
        if result.observed_after is not None and result.retained_before is not None:
            after, after_bytes = encode_snapshot(result.observed_after)
            retained, retained_bytes = encode_snapshot(result.retained_before)
            # 双after只变根位置/ctime，原文件字节须与私有准备完全相同。
            if (
                after_bytes != encode_snapshot(change.staged)[1]
                or retained_bytes != encode_snapshot(change.before)[1]
            ):
                raise _corrupt()
        value: dict[str, object] = {
            "version": 1,
            "kind": "workspace-directory-restore-effect",
            "prepared_material_sha256": prepared["material_sha256"],
            "receipt": asdict(result.receipt),
            "observed_after": after,
            "retained_before": retained,
        }
        value["material_sha256"] = _digest(value)
        if len(_json(value)) > MAX_METADATA or decode_effect(change, value) != result:
            raise _corrupt()
        return value
    except (ValueError, TypeError, AttributeError, AssertionError, RecursionError, ServiceError):
        raise _corrupt() from None


def decode_effect(change: PreparedDirectoryRestore, metadata: object) -> DirectoryRestoreEffect:
    try:
        prepared, payload = encode_prepared(change)
        if type(metadata) is not dict or len(_json(metadata)) > MAX_METADATA:
            raise _corrupt()
        value = _Effect.model_validate(metadata)
        canonical = value.model_dump(mode="json")
        if (
            canonical != metadata
            or value.material_sha256 != _digest(canonical)
            or value.prepared_material_sha256 != prepared["material_sha256"]
            or (value.observed_after is None) != (value.retained_before is None)
        ):
            raise _corrupt()
        after, retained = None, None
        if value.observed_after is not None and value.retained_before is not None:
            reference = _Prepared.model_validate(prepared)
            after = decode_snapshot(value.observed_after, payload[reference.staged.offset :])
            retained = decode_snapshot(value.retained_before, payload[: reference.staged.offset])
        result = DirectoryRestoreEffect(
            DirectoryRestoreReceipt(**value.receipt.model_dump()), after, retained
        )
        _effect(change, result)
        return result
    except (
        ValueError,
        TypeError,
        AttributeError,
        AssertionError,
        KeyError,
        RecursionError,
        ServiceError,
    ):
        raise _corrupt() from None
