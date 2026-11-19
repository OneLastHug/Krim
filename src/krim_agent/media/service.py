"""Authorized original-image reads and provider-aware request preparation."""

from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import cast

from ai import messages as sdk
from pydantic import JsonValue

from krim_agent.agent_sdk.media import MediaLimits
from krim_agent.contracts import ServiceError
from krim_agent.tools.session import SessionScopeResolver, SessionToolScope, ToolBundle

from .formats import IMAGE_TYPES, ImageInfo, inspect_image
from .references import AuthorizedMedia, AuthorizedMediaRef, MediaSource


@dataclass(frozen=True, slots=True)
class ImageCapabilities:
    """Actual model/provider capacities supplied by the host, never the model."""

    vision: bool
    media_types: frozenset[str] = IMAGE_TYPES
    limits: MediaLimits = field(default_factory=MediaLimits)
    max_width: int | None = None
    max_height: int | None = None
    max_pixels: int | None = None
    animation: bool = False

    def __post_init__(self) -> None:
        if (type(self.vision) is not bool or type(self.animation) is not bool
                or not self.media_types or not self.media_types <= IMAGE_TYPES):
            raise ValueError("invalid image capabilities")
        for value in (self.max_width, self.max_height, self.max_pixels):
            if value is not None and (type(value) is not int or value < 1):
                raise ValueError("invalid image capabilities")

    def accepts(self, info: ImageInfo, size: int) -> bool:
        return bool(
            self.vision and info.media_type in self.media_types
            and (self.animation or info.frames == 1)
            and (self.max_width is None or info.width <= self.max_width)
            and (self.max_height is None or info.height <= self.max_height)
            and (self.max_pixels is None or info.width * info.height <= self.max_pixels)
            and (self.limits.max_image_bytes is None or size <= self.limits.max_image_bytes)
            and (self.limits.max_batch_bytes is None or size <= self.limits.max_batch_bytes)
        )


@dataclass(frozen=True, slots=True)
class ImageRendition:
    data: bytes
    media_type: str
    source_sha256: str
    frame: int | None = None


@dataclass(frozen=True, slots=True)
class PreparedImage:
    reference: AuthorizedMediaRef
    info: ImageInfo
    part: sdk.FilePart
    frame: int | None
    transformed: bool


# read must authorize BEFORE IO, reject symlink escapes, and preserve an immutable
# original under reference. This package never opens a host path or fetches URLs.
type ReadMedia = Callable[[SessionToolScope, MediaSource], Awaitable[AuthorizedMedia]]
type ResolveCapabilities = Callable[[SessionToolScope], Awaitable[ImageCapabilities]]
type TransformImage = Callable[
    [AuthorizedMedia, ImageInfo, ImageCapabilities, int | None], Awaitable[ImageRendition]
]


class MediaUnavailable(Exception):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


class MediaService:
    def __init__(
        self, *, read: ReadMedia, capabilities: ResolveCapabilities,
        transform: TransformImage | None = None,
    ) -> None:
        self.read = read
        self.capabilities = capabilities
        self.transform = transform

    def bundle(self, resolve: SessionScopeResolver) -> ToolBundle:
        from .tools import bundle
        return bundle(self, resolve)

    async def load(
        self, scope: SessionToolScope, source: MediaSource,
        *, expected: AuthorizedMediaRef | None = None,
    ) -> tuple[AuthorizedMedia, ImageInfo]:
        scope.check()
        scope.actor.member(scope.workspace_id)
        if source.path is not None and scope.project_id is None:
            raise ServiceError("workspace_unavailable", 422)
        if expected is not None:
            expected.check_scope(scope)
        value = await self.read(scope, source)
        scope.check()
        value.validate(scope)
        if expected is not None and value.reference != expected:
            raise ServiceError("source_changed", 409)
        if source.reference is not None and source.reference != value.reference.reference:
            raise ServiceError("source_changed", 409)
        if source.path is not None and (
            value.reference.path != source.path or value.reference.project_id != scope.project_id
        ):
            raise ServiceError("permission_denied", 403)
        return value, inspect_image(value.data, value.reference.media_type)

    async def prepare(
        self, scope: SessionToolScope, value: AuthorizedMedia, info: ImageInfo,
        capabilities: ImageCapabilities, frame: int | None = None,
    ) -> PreparedImage:
        value.validate(scope)
        if not capabilities.vision:
            raise MediaUnavailable("model_does_not_support_images")
        if frame is not None and (type(frame) is not int or not 0 <= frame < info.frames):
            raise ServiceError("invalid_input", 422)
        needs_transform = (not capabilities.accepts(info, len(value.data))
                           or frame is not None and info.frames > 1)
        if not needs_transform:
            rendition = ImageRendition(value.data, info.media_type, value.reference.sha256)
        else:
            if self.transform is None:
                raise MediaUnavailable("image_conversion_unavailable")
            rendition = await self.transform(value, info, capabilities, frame)
            scope.check()
            if rendition.source_sha256 != value.reference.sha256 or rendition.frame != frame:
                raise ServiceError("source_changed", 409)
            info = inspect_image(rendition.data, rendition.media_type)
            if not capabilities.accepts(info, len(rendition.data)):
                raise MediaUnavailable("image_exceeds_provider_capacity")
        return PreparedImage(
            value.reference, info,
            sdk.FilePart(data=rendition.data, media_type=rendition.media_type),
            frame, needs_transform,
        )

    async def view(
        self, scope: SessionToolScope, source: MediaSource, *, frame: int | None = None,
    ) -> dict[str, JsonValue]:
        scope.check()
        scope.actor.member(scope.workspace_id)
        caps = await self.capabilities(scope)
        scope.check()
        if not caps.vision:
            return {"status": "unavailable", "reason": "model_does_not_support_images",
                    "delivery": "not_attached"}
        value, info = await self.load(scope, source)
        result: dict[str, JsonValue] = {
            "media_ref": cast(JsonValue, value.reference.model_dump(mode="json")),
            "width": info.width, "height": info.height, "frames": info.frames, "frame": frame,
        }
        try:
            prepared = await self.prepare(scope, value, info, caps, frame)
        except MediaUnavailable as exc:
            return {**result, "status": "unavailable", "reason": exc.reason,
                    "delivery": "not_attached"}
        # Preparing bytes does not mean the provider/model has received them.
        return {
            **result, "status": "ready", "delivery": "pending_model_request",
            "image": {"media_type": prepared.info.media_type,
                      "width": prepared.info.width, "height": prepared.info.height,
                      "transformed": prepared.transformed,
                      "sha256": hashlib.sha256(cast(bytes, prepared.part.data)).hexdigest()},
        }
