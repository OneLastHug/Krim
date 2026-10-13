"""双完整树交换私有metadata+BLOB；严格互反、合计容量及原双after绑定。"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from krim_agent.persistence.tree_move_codec import (
    MAX_METADATA,
    MAX_NODES,
    MAX_PAYLOAD,
    Digest,
    _Move,
    decode_move,
    encode_move,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_tree_exchange import (
    PreparedTreeExchange,
    TreeExchangeEffect,
    TreeExchangeReceipt,
    WorkspaceTreeExchangeWriter,
)
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshot


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class _Versioned(_Model):
    version: Literal[1]

    @field_validator("version", mode="before")
    @classmethod
    def exact_version(cls, value: object) -> int:
        if type(value) is not int or value != 1:
            raise ValueError("私有交换版本须为整数1")
        return value


class _Side(_Model):
    offset: int = Field(ge=0, le=MAX_PAYLOAD)
    reference: _Move


class _Exchange(_Versioned):
    kind: Literal["workspace-tree-exchange"]
    token: str = Field(pattern=r"^[0-9a-f]{32}$")
    left: _Side
    right: _Side
    payload_size: int = Field(ge=0, le=MAX_PAYLOAD)
    payload_sha256: Digest
    material_sha256: Digest


class _Receipt(_Model):
    left_path: str = Field(min_length=1, max_length=512)
    right_path: str = Field(min_length=1, max_length=512)
    status: Literal["done", "rejected", "uncertain"]
    left_before_fingerprint: Digest
    right_before_fingerprint: Digest
    left_after_fingerprint: Digest | None
    right_after_fingerprint: Digest | None
    operation_token: str = Field(pattern=r"^[0-9a-f]{32}$")
    changed: bool
    error_code: str | None = Field(min_length=1, max_length=128)
    recovery_required: bool


class _Effect(_Versioned):
    kind: Literal["workspace-tree-exchange-effect"]
    prepared_material_sha256: Digest
    workspace_fingerprint: Digest
    receipt: _Receipt
    observed: _Exchange | None
    payload_size: int = Field(ge=0, le=MAX_PAYLOAD)
    payload_sha256: Digest
    material_sha256: Digest


def _corrupt() -> ServiceError:
    return ServiceError("workspace_tree_exchange_corrupt", "原双树交换私有材料损坏或不完整", 409)


def _json(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, ensure_ascii=True, allow_nan=False, separators=(",", ":")
    ).encode()


def _digest(metadata: dict[str, object]) -> str:
    return hashlib.sha256(
        _json({key: value for key, value in metadata.items() if key != "material_sha256"})
    ).hexdigest()


def _bounded(metadata: object, payload: bytes) -> None:
    if (
        type(metadata) is not dict
        or type(payload) is not bytes
        or len(payload) > MAX_PAYLOAD
        or len(_json(metadata)) > MAX_METADATA
    ):
        raise ValueError("双树交换材料越界或类型错误")


def _capacity(change: PreparedTreeExchange) -> None:
    if len(change.left.entries) + len(change.right.entries) > MAX_NODES:
        raise ServiceError("workspace_tree_exchange_metadata_limit", "两树合计节点超过容量", 413)
    if change.left.total_bytes + change.right.total_bytes > MAX_PAYLOAD:
        raise ServiceError("workspace_tree_exchange_payload_limit", "两树合计字节超过容量", 413)


def _pair(change: PreparedTreeExchange) -> None:
    left, right = change.left, change.right
    if (
        left.workspace_fingerprint != right.workspace_fingerprint
        or left.parent_identity[0] != right.parent_identity[0]
        or (
            left.root_path.rpartition("/")[0] == right.root_path.rpartition("/")[0]
            and left.parent_identity != right.parent_identity
        )
    ):
        raise ValueError("原两树工作区、设备或同父身份不匹配")
    parents = {left.parent_identity, right.parent_identity}
    identities: set[tuple[int, int]] = set()
    for tree in (left, right):
        for entry in tree.entries:
            snapshot = entry.directory or entry.file
            assert snapshot is not None
            if snapshot.identity in identities or snapshot.identity in parents:
                raise ValueError("原两树不能重复物理节点或包含原父身份")
            identities.add(snapshot.identity)


def _finish(metadata: dict[str, object], payload: bytes) -> tuple[dict[str, object], bytes]:
    metadata["material_sha256"] = _digest(metadata)
    if len(_json(metadata)) > MAX_METADATA:
        raise ServiceError("workspace_tree_exchange_metadata_limit", "双树交换元数据超过容量", 413)
    return metadata, payload


def encode_exchange(change: PreparedTreeExchange) -> tuple[dict[str, object], bytes]:
    """冻结一份双before；两reference仅作条件材料，不表示执行过两个move。"""
    _capacity(change)
    try:
        _pair(change)
        first, second = WorkspaceTreeExchangeWriter._moves(change)
        left, left_bytes = encode_move(first)
        right, right_bytes = encode_move(second)
        payload = left_bytes + right_bytes
        metadata, payload = _finish(
            {
                "version": 1,
                "kind": "workspace-tree-exchange",
                "token": change.token,
                "left": {"offset": 0, "reference": left},
                "right": {"offset": len(left_bytes), "reference": right},
                "payload_size": len(payload),
                "payload_sha256": hashlib.sha256(payload).hexdigest(),
            },
            payload,
        )
        if decode_exchange(metadata, payload) != change:
            raise ValueError()
        return metadata, payload
    except ServiceError as error:
        if error.code == "workspace_tree_exchange_metadata_limit":
            raise
        raise _corrupt() from None
    except (ValidationError, ValueError, TypeError, UnicodeError, RecursionError):
        raise _corrupt() from None


def decode_exchange(metadata: object, payload: bytes) -> PreparedTreeExchange:
    """完整核实后返回不可变双before；不访问工作区、不执行namespace。"""
    try:
        _bounded(metadata, payload)
        value = _Exchange.model_validate(metadata)
        canonical = value.model_dump(mode="json", exclude_none=True)
        if (
            canonical != metadata
            or value.material_sha256 != _digest(canonical)
            or value.payload_size != len(payload)
            or value.payload_sha256 != hashlib.sha256(payload).hexdigest()
            or value.left.offset != 0
            or value.right.offset != value.left.reference.payload_size
            or value.right.offset + value.right.reference.payload_size != len(payload)
            or len(value.left.reference.tree.entries) + len(value.right.reference.tree.entries)
            > MAX_NODES
        ):
            raise ValueError()
        left = decode_move(
            value.left.reference.model_dump(mode="json", exclude_none=True),
            payload[: value.right.offset],
        )
        right = decode_move(
            value.right.reference.model_dump(mode="json", exclude_none=True),
            payload[value.right.offset :],
        )
        if (
            left.token != value.token
            or right.token != value.token
            or left.target_path != right.before.root_path
            or right.target_path != left.before.root_path
            or left.target_parent_identity != right.before.parent_identity
            or right.target_parent_identity != left.before.parent_identity
        ):
            raise ValueError()
        change = PreparedTreeExchange(left.before, right.before, value.token)
        _pair(change)
        return change
    except (
        ValidationError,
        ValueError,
        TypeError,
        UnicodeError,
        RecursionError,
        ServiceError,
    ):
        raise _corrupt() from None


def _effect_metadata(value: _Effect) -> dict[str, object]:
    canonical = value.model_dump(mode="json", exclude_none=True)
    canonical["receipt"] = value.receipt.model_dump(mode="json")
    canonical["observed"] = (
        value.observed.model_dump(mode="json", exclude_none=True) if value.observed else None
    )
    return canonical


def _effect_pair(
    change: PreparedTreeExchange, value: _Effect, observed: PreparedTreeExchange | None
) -> TreeExchangeEffect:
    before_metadata, _payload = encode_exchange(change)
    receipt = value.receipt
    if (
        value.prepared_material_sha256 != before_metadata["material_sha256"]
        or value.workspace_fingerprint != change.left.workspace_fingerprint
        or receipt.operation_token != change.token
        or receipt.left_path != change.left.root_path
        or receipt.right_path != change.right.root_path
        or receipt.left_before_fingerprint != change.left.fingerprint
        or receipt.right_before_fingerprint != change.right.fingerprint
    ):
        raise ValueError("回程不属于原双before")
    left: WorkspaceTreeSnapshot | None = None
    right: WorkspaceTreeSnapshot | None = None
    if receipt.status == "done":
        if (
            observed is None
            or observed.token != change.token
            or not receipt.changed
            or receipt.recovery_required
            or receipt.error_code is not None
            or receipt.left_after_fingerprint != observed.left.fingerprint
            or receipt.right_after_fingerprint != observed.right.fingerprint
            or not WorkspaceTreeExchangeWriter._swapped(change, observed.left, observed.right)
        ):
            raise ValueError("done须原两份真实after")
        left, right = observed.left, observed.right
    elif (
        observed is not None
        or receipt.left_after_fingerprint is not None
        or receipt.right_after_fingerprint is not None
        or receipt.error_code is None
        or receipt.changed != (receipt.status == "uncertain")
        or receipt.recovery_required != (receipt.status == "uncertain")
    ):
        raise ValueError("未完成回程不能声称部分after或更改真实状态")
    return TreeExchangeEffect(TreeExchangeReceipt(**receipt.model_dump()), left, right)


def encode_exchange_effect(
    change: PreparedTreeExchange, effect: TreeExchangeEffect
) -> tuple[dict[str, object], bytes]:
    """done保存完整双after；其余状态不把部分观察保存为完成事实。"""
    try:
        before_metadata, _before_payload = encode_exchange(change)
        observed: dict[str, object] | None = None
        payload = b""
        if effect.observed_left is not None or effect.observed_right is not None:
            if effect.observed_left is None or effect.observed_right is None:
                raise ValueError("交换after不能缺一侧")
            observed, payload = encode_exchange(
                PreparedTreeExchange(effect.observed_left, effect.observed_right, change.token)
            )
        metadata, payload = _finish(
            {
                "version": 1,
                "kind": "workspace-tree-exchange-effect",
                "prepared_material_sha256": before_metadata["material_sha256"],
                "workspace_fingerprint": change.left.workspace_fingerprint,
                "receipt": asdict(effect.receipt),
                "observed": observed,
                "payload_size": len(payload),
                "payload_sha256": hashlib.sha256(payload).hexdigest(),
            },
            payload,
        )
        if decode_exchange_effect(change, metadata, payload) != effect:
            raise ValueError()
        return metadata, payload
    except ServiceError as error:
        if error.code in {
            "workspace_tree_exchange_metadata_limit",
            "workspace_tree_exchange_payload_limit",
        }:
            raise
        raise _corrupt() from None
    except (ValidationError, ValueError, TypeError, UnicodeError, RecursionError):
        raise _corrupt() from None


def decode_exchange_effect(
    change: PreparedTreeExchange, metadata: object, payload: bytes
) -> TreeExchangeEffect:
    """以原before核实完整回程；损坏不返回半份after，也不再次exchange。"""
    try:
        _bounded(metadata, payload)
        value = _Effect.model_validate(metadata)
        canonical = _effect_metadata(value)
        if (
            canonical != metadata
            or value.material_sha256 != _digest(canonical)
            or value.payload_size != len(payload)
            or value.payload_sha256 != hashlib.sha256(payload).hexdigest()
        ):
            raise ValueError()
        observed = (
            decode_exchange(value.observed.model_dump(mode="json", exclude_none=True), payload)
            if value.observed is not None
            else None
        )
        if observed is None and payload:
            raise ValueError()
        return _effect_pair(change, value, observed)
    except (
        ValidationError,
        ValueError,
        TypeError,
        UnicodeError,
        RecursionError,
        ServiceError,
    ):
        raise _corrupt() from None
