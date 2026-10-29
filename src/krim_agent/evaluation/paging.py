"""规模页的窄数据契约；游标只校验完整性，绝不是授权凭据。"""

from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass
from typing import Any

from krim_agent.contracts import ServiceError

from .domain import TrialSpec, neutral_encoded, neutral_fingerprint, neutral_parse


@dataclass(frozen=True)
class PageCursor:
    version: str
    repository_id: str
    scope: str
    task_id: str
    dataset_fingerprint: str
    snapshot_binding: str
    filter_fingerprint: str
    order: str
    last_key: list[str | int]
    last_row_hash: str
    checksum: str


@dataclass(frozen=True)
class Page:
    rows: list[dict[str, Any]]
    next_cursor: str | None
    binding: dict[str, Any]
    total_count: int


@dataclass(frozen=True)
class PlanHeader:
    task_id: str
    scope: str
    mode: str
    planned_count: int
    concurrency: int
    item_timeout: float
    scorer_timeout: float
    batch_timeout: float
    rules: list[dict[str, Any]]
    target: dict[str, Any]
    dataset_ref: dict[str, Any]
    plan_fingerprint: str
    layout: int


@dataclass(frozen=True)
class ExecutionPage:
    trials: list[TrialSpec]
    next_cursor: str | None
    plan_fingerprint: str


def encode_cursor(binding: dict[str, Any], key: list[str | int], row_hash: str) -> str:
    body = dict(binding, last_key=key, last_row_hash=row_hash)
    body["checksum"] = neutral_fingerprint(body)
    return base64.urlsafe_b64encode(neutral_encoded(body, max_bytes=16384).encode()).decode()


def decode_cursor(token: str, binding: dict[str, Any]) -> dict[str, Any]:
    try:
        if not isinstance(token, str) or len(token) > 24000:
            raise ValueError("cursor_size")
        body = neutral_parse(base64.b64decode(token, altchars=b"-_", validate=True).decode())
        if not isinstance(body, dict) or set(body) != set(binding) | {
            "last_key",
            "last_row_hash",
            "checksum",
        }:
            raise ValueError("cursor_shape")
        if body["checksum"] != neutral_fingerprint(
            {k: v for k, v in body.items() if k != "checksum"}
        ):
            raise ValueError("cursor_hash")
        if not isinstance(body["last_key"], list) or not isinstance(body["last_row_hash"], str):
            raise ValueError("cursor_key")
        for name, expected in binding.items():
            if body[name] != expected:
                if name == "snapshot_binding":
                    raise ServiceError("source_changed")
                raise ValueError("cursor_binding")
        return body
    except ServiceError:
        raise
    except (ValueError, TypeError, UnicodeError, binascii.Error):
        raise ServiceError("invalid_input", 422) from None
