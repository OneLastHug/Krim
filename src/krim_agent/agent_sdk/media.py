"""Host-authorized image material loading for ai 0.9.0 SDK messages."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import re
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass

from ai import messages as sdk
from ai.types import media as sdk_media

from krim_agent.contracts import ServiceError

type AuthorizeFile = Callable[[sdk.FilePart], Awaitable[None]]
type LoadFile = Callable[[str], Awaitable[sdk.FilePart]]
type ExpectedSha256 = Callable[[sdk.FilePart], str | None]

_ALLOWED_TYPES = frozenset({"image/png", "image/jpeg", "image/webp", "image/gif"})
_MAX_BATCH_BYTES = 512 * 1024
_MAX_IMAGES = 8
_REFERENCE_RE = re.compile(r"[A-Za-z0-9_-]{1,256}\Z")
_SHA256_RE = re.compile(r"[0-9a-fA-F]{64}\Z")
_DATA_URL_RE = re.compile(r"data:([^;,]+);base64,(.*)\Z", re.DOTALL)
_SCHEME_RE = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*:")


@dataclass(frozen=True, slots=True)
class MediaLimits:
    """Strict per-image, batch, and count bounds for model-bound media."""

    max_image_bytes: int = 256 * 1024
    max_batch_bytes: int = _MAX_BATCH_BYTES
    max_images: int = _MAX_IMAGES

    def __post_init__(self) -> None:
        if (type(self.max_image_bytes) is not int or type(self.max_batch_bytes) is not int
                or type(self.max_images) is not int or self.max_image_bytes < 1
                or self.max_image_bytes > _MAX_BATCH_BYTES or self.max_batch_bytes < 1
                or self.max_batch_bytes > _MAX_BATCH_BYTES or self.max_images < 1
                or self.max_images > _MAX_IMAGES):
            raise ValueError("invalid media limits")


DEFAULT_MEDIA_LIMITS = MediaLimits()


def _invalid() -> ServiceError:
    return ServiceError("invalid_input")


def _decode_payload(part: sdk.FilePart, limits: MediaLimits) -> tuple[bytes, str]:
    media_type = part.media_type
    if not isinstance(media_type, str) or media_type not in _ALLOWED_TYPES:
        raise _invalid()

    data = part.data
    if isinstance(data, bytes):
        payload = data
    elif isinstance(data, str):
        max_encoded = 4 * ((limits.max_image_bytes + 2) // 3)
        if len(data) > max_encoded + 64:
            raise _invalid()
        encoded = data
        if data.startswith("data:"):
            match = _DATA_URL_RE.fullmatch(data)
            if match is None:
                raise _invalid()
            embedded_type, encoded = match.groups()
            if embedded_type != media_type:
                raise _invalid()
        elif _SCHEME_RE.match(data):
            raise _invalid()
        try:
            encoded_bytes = encoded.encode("ascii")
        except UnicodeEncodeError as exc:
            raise _invalid() from exc
        max_encoded = 4 * ((limits.max_image_bytes + 2) // 3)
        if len(encoded_bytes) > max_encoded:
            raise _invalid()
        try:
            payload = base64.b64decode(encoded_bytes, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise _invalid() from exc
        if base64.b64encode(payload) != encoded_bytes:
            raise _invalid()
    else:
        raise _invalid()

    if not payload or len(payload) > limits.max_image_bytes:
        raise _invalid()
    detected = sdk_media.detect_image_media_type(payload)
    if detected not in _ALLOWED_TYPES or detected != media_type:
        raise _invalid()
    return payload, detected


def _check_digest(payload: bytes, expected: str | None) -> None:
    if expected is None:
        return
    if not isinstance(expected, str) or _SHA256_RE.fullmatch(expected) is None:
        raise _invalid()
    if not hmac.compare_digest(hashlib.sha256(payload).hexdigest(), expected.lower()):
        raise _invalid()


def _inspect_history(
    history: Sequence[sdk.Message],
    limits: MediaLimits,
    expected_sha256: ExpectedSha256 | None,
) -> list[sdk.FilePart]:
    files: list[sdk.FilePart] = []
    total_bytes = 0
    for message in history:
        for part in message.parts:
            if isinstance(part, sdk.FilePart):
                if len(files) >= limits.max_images:
                    raise _invalid()
                payload, _ = _decode_payload(part, limits)
                total_bytes += len(payload)
                if total_bytes > limits.max_batch_bytes:
                    raise _invalid()
                if expected_sha256 is not None:
                    _check_digest(payload, expected_sha256(part))
                files.append(part)
    return files


async def load_authorized_file(
    reference: str,
    *,
    load: LoadFile,
    authorize: AuthorizeFile,
    limits: MediaLimits = DEFAULT_MEDIA_LIMITS,
    expected_sha256: str | None = None,
) -> sdk.FilePart:
    """Resolve a host-scoped opaque reference and return its original SDK part."""
    if not isinstance(reference, str) or _REFERENCE_RE.fullmatch(reference) is None:
        raise _invalid()
    part = await load(reference)
    if not isinstance(part, sdk.FilePart):
        raise _invalid()
    payload, _ = _decode_payload(part, limits)
    _check_digest(payload, expected_sha256)
    await authorize(part)
    return part


async def authorize_history(
    history: Sequence[sdk.Message],
    authorize: AuthorizeFile | None,
    limits: MediaLimits = DEFAULT_MEDIA_LIMITS,
    *,
    expected_sha256: ExpectedSha256 | None = None,
) -> list[sdk.Message]:
    """Revalidate all persisted SDK image parts, preserving their native order."""
    files = _inspect_history(history, limits, expected_sha256)
    if files and authorize is None:
        raise ServiceError("unauthorized")
    if authorize is not None:
        for part in files:
            await authorize(part)
    return list(history)
