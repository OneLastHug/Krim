"""Optional converter using an existing host-configured ffmpeg executable.

No shell, files, network or dependency installation is used. Input is a validated
raster container on stdin; output is one image on stdout. Deployments may instead
inject their own sandboxed processor via TransformImage.
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass

from krim_agent.contracts import ServiceError

from .formats import ImageInfo, inspect_image
from .references import AuthorizedMedia
from .service import ImageCapabilities, ImageRendition, MediaUnavailable


@dataclass(frozen=True, slots=True)
class FFmpegImageTransform:
    executable: str
    timeout: float | None = None

    async def __call__(
        self, value: AuthorizedMedia, info: ImageInfo, caps: ImageCapabilities,
        frame: int | None,
    ) -> ImageRendition:
        selected_frame = frame or 0
        if selected_frame >= info.frames:
            raise ServiceError("invalid_input", 422)
        target = next((mime for mime in ("image/png", "image/jpeg", "image/webp", "image/gif")
                       if mime in caps.media_types), None)
        if target is None:
            raise MediaUnavailable("image_conversion_unavailable")
        scale = min(
            1.0, (caps.max_width / info.width if caps.max_width else 1.0),
            (caps.max_height / info.height if caps.max_height else 1.0),
            (math.sqrt(caps.max_pixels / (info.width * info.height)) if caps.max_pixels else 1.0),
        )
        width, height = max(1, int(info.width * scale)), max(1, int(info.height * scale))
        while True:
            data = await self._convert(value.data, info, target, width, height, selected_frame)
            converted = inspect_image(data, target)
            if caps.accepts(converted, len(data)):
                return ImageRendition(data, target, value.reference.sha256, frame)
            if width == height == 1:
                raise MediaUnavailable("image_exceeds_provider_capacity")
            # Monotonically smaller renditions until the actual provider byte
            # capacity is met; this is not a product resolution/count quota.
            width, height = max(1, int(width * 0.75)), max(1, int(height * 0.75))

    async def _convert(
        self, data: bytes, info: ImageInfo, target: str, width: int, height: int, frame: int,
    ) -> bytes:
        demuxer = {"image/png": "apng" if info.frames > 1 else "png_pipe",
                   "image/jpeg": "jpeg_pipe", "image/webp": "webp_pipe", "image/gif": "gif"}[
            info.media_type
        ]
        codec = {"image/png": "png", "image/jpeg": "mjpeg", "image/webp": "libwebp",
                 "image/gif": "gif"}[target]
        filters = f"select=eq(n\\,{frame}),scale={width}:{height}"
        args = [self.executable, "-hide_banner", "-loglevel", "error", "-nostdin",
                "-protocol_whitelist", "pipe", "-f", demuxer, "-threads", "1", "-i", "pipe:0",
                "-map", "0:v:0", "-vf", filters, "-frames:v", "1", "-threads", "1",
                "-c:v", codec, "-f", "image2pipe", "pipe:1"]
        try:
            process = await asyncio.create_subprocess_exec(
                *args, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except OSError:
            raise MediaUnavailable("image_conversion_unavailable") from None
        try:
            async with asyncio.timeout(self.timeout):
                output, _error = await process.communicate(data)
        except BaseException:
            if process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
            await process.wait()
            raise
        if process.returncode or not output:
            raise MediaUnavailable("image_conversion_failed")
        return output
