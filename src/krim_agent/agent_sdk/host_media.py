"""Transient host-authorized image messages for model requests.

Images in this module are admitted only through the host callback and never
become fields on the durable base Message/checkpoint representation.
"""

from __future__ import annotations

import base64
import binascii
from collections.abc import Awaitable, Callable

from ai import messages as sdk
from pydantic import Field

from krim_agent.browser.protocol import validate_png
from krim_agent.contracts import Message, ServiceError

MAX_IMAGE_BYTES = 512 * 1024
MAX_IMAGE_BATCH_BYTES = 512 * 1024
MAX_IMAGES = 8


class HostImageMessage(Message):
    """A request-local user message carrying host-authorized PNG parts."""

    images: list[sdk.FilePart] = Field(min_length=1, max_length=MAX_IMAGES)


HostMediaCallback = Callable[[list[Message]], Awaitable[list[Message]]]


def validate_host_images(images: list[sdk.FilePart]) -> list[bytes]:
    """Accept bounded, inline PNG bytes only; never pass URLs to the SDK."""
    if not 1 <= len(images) <= MAX_IMAGES:
        raise ServiceError("invalid_input")
    payloads: list[bytes] = []
    total = 0
    for part in images:
        if not isinstance(part, sdk.FilePart) or part.media_type != "image/png":
            raise ServiceError("invalid_input")
        data = part.data
        if isinstance(data, bytes):
            payload = data
        elif isinstance(data, str):
            # A FilePart string may represent a URL or data URL to ai. Never
            # delegate interpretation/download to the SDK; permit canonical b64 only.
            max_encoded = 4 * ((MAX_IMAGE_BYTES + 2) // 3)
            if data.startswith("data:") or ":" in data or len(data) > max_encoded:
                raise ServiceError("invalid_input")
            try:
                raw = data.encode("ascii")
                payload = base64.b64decode(raw, validate=True)
            except (UnicodeEncodeError, binascii.Error, ValueError):
                raise ServiceError("invalid_input") from None
            if base64.b64encode(payload) != raw:
                raise ServiceError("invalid_input")
        else:
            raise ServiceError("invalid_input")
        if not payload or len(payload) > MAX_IMAGE_BYTES:
            raise ServiceError("invalid_input")
        try:
            validate_png(payload)
        except ServiceError:
            raise ServiceError("invalid_input") from None
        total += len(payload)
        if total > MAX_IMAGE_BATCH_BYTES:
            raise ServiceError("invalid_input")
        payloads.append(payload)
    return payloads
