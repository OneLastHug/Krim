"""Transient host-authorized image messages for model requests.

Only a trusted host callback creates these messages. Durable tool history stores
references, never inline bytes. Limits, when present, come from the provider.
"""

from __future__ import annotations

import base64
import binascii
from collections.abc import Awaitable, Callable

from ai import messages as sdk
from pydantic import Field

from krim_agent.contracts import Message, ServiceError
from krim_agent.media.formats import inspect_image

from .media import DEFAULT_MEDIA_LIMITS, MediaLimits


class HostImageMessage(Message):
    """A request-local user message carrying host-authorized image parts."""

    images: list[sdk.FilePart] = Field(min_length=1)


HostMediaCallback = Callable[[list[Message]], Awaitable[list[Message]]]


def validate_host_images(
    images: list[sdk.FilePart], limits: MediaLimits = DEFAULT_MEDIA_LIMITS,
) -> list[bytes]:
    """Validate inline bytes; never let the SDK download or open a string."""
    if not images or (limits.max_images is not None and len(images) > limits.max_images):
        raise ServiceError("invalid_input")
    payloads: list[bytes] = []
    total = 0
    for part in images:
        if not isinstance(part, sdk.FilePart):
            raise ServiceError("invalid_input")
        data = part.data
        if isinstance(data, bytes):
            payload = data
        elif isinstance(data, str):
            if ":" in data:
                raise ServiceError("invalid_input")
            if (limits.max_image_bytes is not None
                    and len(data) > 4 * ((limits.max_image_bytes + 2) // 3)):
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
        if limits.max_image_bytes is not None and len(payload) > limits.max_image_bytes:
            raise ServiceError("invalid_input")
        inspect_image(payload, part.media_type)
        total += len(payload)
        if limits.max_batch_bytes is not None and total > limits.max_batch_bytes:
            raise ServiceError("invalid_input")
        payloads.append(payload)
    return payloads
