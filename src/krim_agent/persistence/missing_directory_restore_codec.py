"""缺失事实/准备/效果的私有版本化材料；冷解码历史不重新产生活资格。"""

from __future__ import annotations

import hashlib
from dataclasses import asdict
from typing import Literal

from pydantic import Field, JsonValue

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_directory_absence import DirectoryAbsenceParent, WorkspaceDirectoryAbsence
from krim_agent.workspace_directory_restore_writer import (
    DirectoryRestoreStage,
    WorkspaceDirectoryRestoreWriter,
)
from krim_agent.workspace_missing_directory_restore import MissingDirectoryRestoreReceiptView
from krim_agent.workspace_missing_directory_writer import (
    MissingDirectoryRestoreEffect,
    MissingDirectoryRestoreReceipt,
    PreparedMissingDirectoryRestore,
)

from .directory_restore_codec import Digest, Owner, _Stage, _tree
from .tree_exchange_codec import _digest, _json, _Model, _Versioned
from .tree_snapshot_codec import decode_snapshot, encode_snapshot

MAX_METADATA = 4_194_304
MAX_PAYLOAD = 33_554_432


class _Parent(_Model):
    device: int = Field(ge=0)
    inode: int = Field(gt=0)
    mode: int = Field(ge=0, le=0o7777)
    uid: Owner
    gid: Owner
    mtime_ns: int
    ctime_ns: int
    nlink: int = Field(ge=1)


class _Absence(_Versioned):
    kind: Literal["workspace-directory-absence"]
    root_path: str = Field(min_length=1, max_length=512)
    workspace_fingerprint: Digest
    parent: _Parent
    observation_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    fingerprint: Digest
    material_sha256: Digest


class _Prepared(_Versioned):
    kind: Literal["workspace-missing-directory-restore"]
    stage: _Stage
    target_fingerprint: Digest
    before: _Absence
    current_absence: _Absence
    staged: dict[str, JsonValue]
    payload_size: int = Field(ge=0, le=MAX_PAYLOAD)
    payload_sha256: Digest
    material_sha256: Digest


class _Effect(_Versioned):
    kind: Literal["workspace-missing-directory-restore-effect"]
    prepared_material_sha256: Digest
    receipt: MissingDirectoryRestoreReceiptView
    observed_after: dict[str, JsonValue] | None
    material_sha256: Digest


def _corrupt() -> ServiceError:
    return ServiceError(
        "workspace_missing_directory_restore_corrupt", "原缺失恢复材料损坏或关联不完整", 409
    )


def encode_absence(value: WorkspaceDirectoryAbsence) -> dict[str, object]:
    try:
        if type(value) is not WorkspaceDirectoryAbsence:
            raise _corrupt()
        value.validate()
        result: dict[str, object] = {
            "version": 1,
            "kind": "workspace-directory-absence",
            "root_path": value.root_path,
            "workspace_fingerprint": value.workspace_fingerprint,
            "parent": value.parent.metadata(),
            "observation_id": value.observation_id,
            "fingerprint": value.fingerprint,
        }
        result["material_sha256"] = _digest(result)
        if decode_absence(result) != value:
            raise _corrupt()
        return result
    except (ValueError, TypeError, AttributeError, RecursionError, ServiceError):
        raise _corrupt() from None


def decode_absence(metadata: object) -> WorkspaceDirectoryAbsence:
    try:
        if type(metadata) is not dict or len(_json(metadata)) > 4096:
            raise _corrupt()
        value = _Absence.model_validate(metadata)
        canonical = value.model_dump(mode="json")
        if canonical != metadata or value.material_sha256 != _digest(canonical):
            raise _corrupt()
        result = WorkspaceDirectoryAbsence(
            value.root_path,
            value.workspace_fingerprint,
            DirectoryAbsenceParent(**value.parent.model_dump()),
            value.observation_id,
        )
        if result.fingerprint != value.fingerprint:
            raise _corrupt()
        return result
    except (ValueError, TypeError, AttributeError, KeyError, RecursionError, ServiceError):
        raise _corrupt() from None


def _pair(value: PreparedMissingDirectoryRestore) -> None:
    if (
        type(value) is not PreparedMissingDirectoryRestore
        or type(value.stage) is not DirectoryRestoreStage
        or type(value.before) is not WorkspaceDirectoryAbsence
        or type(value.current_absence) is not WorkspaceDirectoryAbsence
    ):
        raise _corrupt()
    value.before.validate()
    value.current_absence.validate()
    stage, before, current = value.stage, value.before, value.current_absence
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
    identities = _tree(value.staged)
    if (
        stage.root_path != before.root_path
        or stage.root_path != current.root_path
        or stage.root_path != value.staged.root_path
        or before.workspace_fingerprint != current.workspace_fingerprint
        or before.workspace_fingerprint != value.staged.workspace_fingerprint
        or stage.parent_identity != before.parent.identity
        or stage.parent_identity != current.parent.identity
        or stage.container_identity != value.staged.parent_identity
        or stage.parent_identity == stage.container_identity
        or stage.parent_identity[0] != stage.container_identity[0]
        or {stage.parent_identity, stage.container_identity}.intersection(identities)
        or before.observation_id == current.observation_id
        or any(
            getattr(before.parent, name) != getattr(current.parent, name)
            for name in ("device", "inode", "mode", "uid", "gid")
        )
    ):
        raise _corrupt()


def encode_prepared(
    change: PreparedMissingDirectoryRestore,
) -> tuple[dict[str, object], bytes]:
    try:
        _pair(change)
        tree, payload = encode_snapshot(change.staged)
        value: dict[str, object] = {
            "version": 1,
            "kind": "workspace-missing-directory-restore",
            "stage": {
                "root_path": change.stage.root_path,
                "token": change.stage.token,
                "parent_identity": list(change.stage.parent_identity),
                "container_identity": list(change.stage.container_identity),
                "container_owner": list(change.stage.container_owner),
            },
            "target_fingerprint": change.target_fingerprint,
            "before": encode_absence(change.before),
            "current_absence": encode_absence(change.current_absence),
            "staged": tree,
            "payload_size": len(payload),
            "payload_sha256": hashlib.sha256(payload).hexdigest(),
        }
        value["material_sha256"] = _digest(value)
        if decode_prepared(value, payload) != change:
            raise _corrupt()
        return value, payload
    except (ValueError, TypeError, AttributeError, AssertionError, RecursionError, ServiceError):
        raise _corrupt() from None


def decode_prepared(metadata: object, payload: bytes) -> PreparedMissingDirectoryRestore:
    try:
        if (
            type(metadata) is not dict
            or type(payload) is not bytes
            or len(payload) > MAX_PAYLOAD
            or len(_json(metadata)) > MAX_METADATA
        ):
            raise _corrupt()
        value = _Prepared.model_validate(metadata)
        canonical = value.model_dump(mode="json")
        if (
            canonical != metadata
            or value.material_sha256 != _digest(canonical)
            or value.payload_size != len(payload)
            or value.payload_sha256 != hashlib.sha256(payload).hexdigest()
        ):
            raise _corrupt()
        stage = value.stage
        result = PreparedMissingDirectoryRestore(
            DirectoryRestoreStage(
                stage.root_path,
                stage.token,
                (stage.parent_identity[0], stage.parent_identity[1]),
                (stage.container_identity[0], stage.container_identity[1]),
                (stage.container_owner[0], stage.container_owner[1]),
            ),
            value.target_fingerprint,
            decode_absence(value.before.model_dump(mode="json")),
            decode_absence(value.current_absence.model_dump(mode="json")),
            decode_snapshot(value.staged, payload),
        )
        _pair(result)
        return result
    except (ValueError, TypeError, AttributeError, KeyError, RecursionError, ServiceError):
        raise _corrupt() from None


def _effect(change: PreparedMissingDirectoryRestore, result: MissingDirectoryRestoreEffect) -> None:
    if (
        type(result) is not MissingDirectoryRestoreEffect
        or type(result.receipt) is not MissingDirectoryRestoreReceipt
    ):
        raise _corrupt()
    receipt = MissingDirectoryRestoreReceiptView.model_validate(asdict(result.receipt))
    if (
        receipt.root_path != change.stage.root_path
        or receipt.before_absence_fingerprint != change.before.fingerprint
        or receipt.target_fingerprint != change.target_fingerprint
    ):
        raise _corrupt()
    if receipt.status == "done":
        after = result.observed_after
        if (
            after is None
            or receipt.after_fingerprint != after.fingerprint
            or not WorkspaceDirectoryRestoreWriter._moved(
                change.staged, after, change.stage.parent_identity
            )
        ):
            raise _corrupt()
        _tree(after)
    elif result.observed_after is not None:
        raise _corrupt()


def encode_effect(
    change: PreparedMissingDirectoryRestore, result: MissingDirectoryRestoreEffect
) -> dict[str, object]:
    try:
        prepared, payload = encode_prepared(change)
        _effect(change, result)
        after = None
        if result.observed_after is not None:
            after, data = encode_snapshot(result.observed_after)
            if data != payload:
                raise _corrupt()
        value: dict[str, object] = {
            "version": 1,
            "kind": "workspace-missing-directory-restore-effect",
            "prepared_material_sha256": prepared["material_sha256"],
            "receipt": asdict(result.receipt),
            "observed_after": after,
        }
        value["material_sha256"] = _digest(value)
        if len(_json(value)) > MAX_METADATA or decode_effect(change, value) != result:
            raise _corrupt()
        return value
    except (ValueError, TypeError, AttributeError, AssertionError, RecursionError, ServiceError):
        raise _corrupt() from None


def decode_effect(
    change: PreparedMissingDirectoryRestore, metadata: object
) -> MissingDirectoryRestoreEffect:
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
        ):
            raise _corrupt()
        after = (
            decode_snapshot(value.observed_after, payload)
            if value.observed_after is not None
            else None
        )
        result = MissingDirectoryRestoreEffect(
            MissingDirectoryRestoreReceipt(**value.receipt.model_dump()), after
        )
        _effect(change, result)
        return result
    except (ValueError, TypeError, AttributeError, KeyError, RecursionError, ServiceError):
        raise _corrupt() from None
