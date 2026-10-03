"""Krim 内部 v2 合约；来源与上游差异见 docs/protocol/m1-contract.md。"""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator


def utc_now() -> datetime:
    return datetime.now(UTC)


def check_json(value: JsonValue, max_bytes: int = 65536) -> None:
    pending = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        if depth > 32:
            raise ValueError("JSON 嵌套深度超过 32")
        if isinstance(item, (int, float)) and (
            abs(item) > 9007199254740991 or not math.isfinite(item)
        ):
            raise ValueError("JSON 数字必须有限且位于跨语言安全范围内")
        if isinstance(item, dict):
            pending.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)
    encoded = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode(
        "utf-8"
    )
    if len(encoded) > max_bytes:
        raise ValueError("JSON 大小超过限制")


class StrictModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, allow_inf_nan=False, validate_default=True
    )


class ChannelResult(StrictModel):
    ok: bool
    data: JsonValue = None
    error: str | None = None

    @model_validator(mode="after")
    def validate_result(self) -> ChannelResult:
        if self.ok and self.error is not None:
            raise ValueError("成功结果不能包含 error")
        if not self.ok and (not self.error or self.data is not None):
            raise ValueError("失败结果必须包含 error 且不能包含 data")
        check_json(self.data)
        return self


class ProtocolEnvelope(StrictModel):
    protocol_version: Literal[2] = 2
    kind: Literal["call", "reply", "notify", "chunk", "end", "cancel"]
    id: str = Field(min_length=1, max_length=128)
    channel: str = Field(min_length=1, max_length=128)
    data: JsonValue = None
    stream_id: str | None = Field(default=None, min_length=1, max_length=128)
    seq: int | None = Field(default=None, ge=0, le=9007199254740991)

    @field_validator("seq", mode="before")
    @classmethod
    def whole_number(cls, value: object) -> object:
        if isinstance(value, float) and math.isfinite(value) and value.is_integer():
            return int(value)
        return value

    @model_validator(mode="after")
    def validate_frame(self) -> ProtocolEnvelope:
        check_json(self.data)
        if self.kind in {"chunk", "end"} and (self.stream_id is None or self.seq is None):
            raise ValueError("流帧必须包含 stream_id 和 seq")
        return self


class StreamFrame(StrictModel):
    stream_id: str = Field(default_factory=lambda: str(uuid4()))
    seq: int = Field(ge=0, le=9007199254740991)
    kind: Literal["start", "chunk", "end", "error"]
    data: JsonValue = None
    created_at: datetime = Field(default_factory=utc_now)

    @field_validator("created_at")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value

    @model_validator(mode="after")
    def validate_data(self) -> StreamFrame:
        check_json(self.data, max_bytes=131072)
        return self
