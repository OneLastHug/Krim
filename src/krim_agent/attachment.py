"""话题隔离的受限二进制附件合约；不执行、不解析、不自动进入模型上下文。"""

from __future__ import annotations

import base64
import binascii
import hashlib
import re
from datetime import datetime
from typing import Protocol
from uuid import UUID

from pydantic import Field, JsonValue, model_validator

from .protocol.models import StrictModel, utc_now

MAX_ATTACHMENT_BYTES = 2 * 1024 * 1024
MAX_ATTACHMENT_BASE64 = ((MAX_ATTACHMENT_BYTES + 2) // 3) * 4
MAX_TOPIC_ATTACHMENT_BYTES = 32 * 1024 * 1024
MAX_TOPIC_ATTACHMENTS = 64
ALLOWED_ATTACHMENT_TYPES = frozenset(
    {"application/pdf", "application/zip", "image/png", "image/jpeg", "image/gif", "image/webp"}
)
FILENAME_RE = re.compile(r"^[^/\\\x00-\x1f\x7f\u0085\u2028\u2029]{1,120}$")
MEDIA_TYPE_RE = re.compile(r"^[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+$")


def allowed_media_type(value: str) -> bool:
    return bool(MEDIA_TYPE_RE.fullmatch(value)) and (
        value.startswith("text/") or value in ALLOWED_ATTACHMENT_TYPES
    )


class AttachmentUpload(StrictModel):
    filename: str = Field(min_length=1, max_length=120)
    media_type: str = Field(min_length=1, max_length=120)
    content_base64: str = Field(min_length=4, max_length=MAX_ATTACHMENT_BASE64)
    idempotency_key: str = Field(min_length=1, max_length=128)

    @model_validator(mode="after")
    def bounded(self) -> AttachmentUpload:
        if (
            not self.filename.strip()
            or not FILENAME_RE.fullmatch(self.filename)
            or self.filename in {".", ".."}
        ):
            raise ValueError("附件文件名不能包含路径、控制符或换行")
        if ";" in self.media_type or not allowed_media_type(self.media_type.lower()):
            raise ValueError("附件媒体类型不在允许范围")
        try:
            raw = base64.b64decode(self.content_base64, validate=True)
        except (ValueError, binascii.Error) as error:
            raise ValueError("附件内容不是有效 base64") from error
        if not raw or len(raw) > MAX_ATTACHMENT_BYTES:
            raise ValueError("附件为空或超过 2 MiB 上限")
        if base64.b64encode(raw).decode("ascii") != self.content_base64:
            raise ValueError("附件 base64 必须使用规范编码")
        try:
            self.filename.encode("utf-8")
        except UnicodeError as error:
            raise ValueError("附件文件名必须是有效 Unicode") from error
        return self

    def decode(self) -> bytes:
        return base64.b64decode(self.content_base64, validate=True)


class AttachmentView(StrictModel):
    id: str = Field(min_length=1, max_length=36)
    topic_id: str = Field(min_length=1, max_length=128)
    filename: str
    media_type: str
    byte_size: int = Field(gt=0, le=MAX_ATTACHMENT_BYTES)
    sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    created_at: datetime = Field(default_factory=utc_now)


class AttachmentContent(StrictModel):
    attachment: AttachmentView
    content_base64: str = Field(min_length=4, max_length=MAX_ATTACHMENT_BASE64)

    @model_validator(mode="after")
    def matches_metadata(self) -> AttachmentContent:
        try:
            raw = base64.b64decode(self.content_base64, validate=True)
        except (ValueError, binascii.Error) as error:
            raise ValueError("附件下载内容编码无效") from error
        if (
            len(raw) != self.attachment.byte_size
            or hashlib.sha256(raw).hexdigest() != self.attachment.sha256
        ):
            raise ValueError("附件下载内容与大小/哈希不一致")
        return self


class AttachmentAccess(Protocol):
    async def list(self, topic_id: str) -> list[AttachmentView]: ...

    async def upload(self, topic_id: str, request: AttachmentUpload) -> AttachmentView: ...

    async def content(self, topic_id: str, attachment_identifier: str) -> AttachmentContent: ...

    async def delete(self, topic_id: str, attachment_identifier: str) -> None: ...


class AttachmentTopicRequest(StrictModel):
    topic_id: str = Field(min_length=1, max_length=128)


class AttachmentRequest(AttachmentUpload, AttachmentTopicRequest):
    pass


def attachment_id(value: str) -> str:
    try:
        return str(UUID(value))
    except ValueError as error:
        raise ValueError("attachment_id 无效") from error


def attachment_hash(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def validate_attachment_metadata(metadata: dict[str, JsonValue]) -> str | None:
    value = metadata.get("attachment_id")
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("attachment_id 必须是字符串")
    return attachment_id(value)
