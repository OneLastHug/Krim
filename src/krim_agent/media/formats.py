"""Container validation for inline raster images, without filesystem or URL IO.

Checks cover container framing, CRCs, dimensions and animation controls. Pixel
decoding/conversion belongs to a trusted processor port. PNG inflate validation
is optional and requires an explicit decoded-pixel work bound; source inspection
never expands untrusted compressed data. No product quota is imposed here.
"""

from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass

from krim_agent.contracts import ServiceError

IMAGE_TYPES = frozenset({"image/png", "image/jpeg", "image/gif", "image/webp"})


@dataclass(frozen=True, slots=True)
class ImageInfo:
    media_type: str
    width: int
    height: int
    frames: int = 1


def inspect_image(
    data: bytes, media_type: str | None = None, *,
    validate_pixels: bool = False, max_pixels: int | None = None,
) -> ImageInfo:
    if validate_pixels and (type(max_pixels) is not int or max_pixels < 1):
        raise ValueError("pixel validation requires a positive work bound")
    try:
        if type(data) is not bytes or not data:
            raise ValueError
        if data.startswith(b"\x89PNG\r\n\x1a\n"):
            info = _png(data, validate_pixels=validate_pixels, max_pixels=max_pixels)
        elif data.startswith(b"\xff\xd8"):
            info = _jpeg(data)
        elif data[:6] in {b"GIF87a", b"GIF89a"}:
            info = _gif(data)
        elif data.startswith(b"RIFF") and data[8:12] == b"WEBP":
            info = _webp(data)
        else:
            raise ValueError
        if (info.width < 1 or info.height < 1
                or media_type is not None and info.media_type != media_type):
            raise ValueError
        return info
    except (ValueError, IndexError, struct.error, zlib.error, OverflowError):
        raise ServiceError("invalid_input", 422) from None


def _png(
    data: bytes, *, validate_pixels: bool = False, max_pixels: int | None = None,
) -> ImageInfo:
    offset, width, height, depth, color, interlace, frames = 8, 0, 0, 0, 0, 0, 1
    compressed: list[bytes] = []
    ended = closed = palette = False
    animation = False
    sequence = controls = 0
    frame_data = False
    frame_uses_idat = False
    while offset < len(data):
        length = struct.unpack_from(">I", data, offset)[0]
        kind = data[offset + 4:offset + 8]
        end = offset + 12 + length
        if end > len(data) or length > 0x7FFFFFFF or len(kind) != 4:
            raise ValueError
        chunk = data[offset + 8:end - 4]
        crc = struct.unpack_from(">I", data, end - 4)[0]
        if zlib.crc32(kind + chunk) & 0xFFFFFFFF != crc:
            raise ValueError
        if offset == 8:
            if kind != b"IHDR" or length != 13:
                raise ValueError
            width, height, depth, color, method, filtering, interlace = struct.unpack(
                ">IIBBBBB", chunk
            )
            depths = {0: {1, 2, 4, 8, 16}, 2: {8, 16}, 3: {1, 2, 4, 8}, 4: {8, 16}, 6: {8, 16}}
            if (not 0 < width <= 0x7FFFFFFF or not 0 < height <= 0x7FFFFFFF
                    or depth not in depths.get(color, set()) or method or filtering
                    or interlace not in {0, 1}):
                raise ValueError
        elif kind == b"IHDR":
            raise ValueError
        elif kind == b"PLTE":
            if compressed or palette or not 3 <= length <= 768 or length % 3:
                raise ValueError
            palette = True
        elif kind == b"IDAT":
            if closed or (color == 3 and not palette):
                raise ValueError
            if animation and controls:
                if not frame_uses_idat:
                    raise ValueError
                frame_data = True
            compressed.append(chunk)
        elif kind == b"IEND":
            if (length or not compressed or end != len(data)
                    or animation and (controls != frames or not frame_data)):
                raise ValueError
            ended = True
        elif kind == b"acTL":
            if length != 8 or compressed or animation:
                raise ValueError
            frames = struct.unpack_from(">I", chunk)[0]
            if not frames:
                raise ValueError
            animation = True
        elif kind == b"fcTL":
            if not animation or length != 26 or controls and not frame_data:
                raise ValueError
            seq, w, h, x, y, _delay_n, _delay_d, dispose, blend = struct.unpack(
                ">IIIIIHHBB", chunk
            )
            if (seq != sequence or not w or not h or x + w > width or y + h > height
                    or dispose > 2 or blend > 1):
                raise ValueError
            if not compressed and (controls or (w, h, x, y) != (width, height, 0, 0)):
                raise ValueError
            frame_uses_idat = not compressed
            frame_data = False
            controls += 1
            sequence += 1
        elif kind == b"fdAT":
            if (not animation or not controls or frame_uses_idat or not compressed
                    or length <= 4 or struct.unpack_from(">I", chunk)[0] != sequence):
                raise ValueError
            sequence += 1
            frame_data = True
        elif kind[0] & 0x20 == 0:
            raise ValueError
        if compressed and kind != b"IDAT":
            closed = True
        offset = end
    if not ended:
        raise ValueError
    if not validate_pixels:
        return ImageInfo("image/png", width, height, frames)
    if max_pixels is None or width * height > max_pixels:
        raise ValueError
    channels = {0: 1, 2: 3, 3: 1, 4: 2, 6: 4}[color]
    passes = [(0, 0, 1, 1)] if not interlace else [
        (0, 0, 8, 8), (4, 0, 8, 8), (0, 4, 4, 8), (2, 0, 4, 4),
        (0, 2, 2, 4), (1, 0, 2, 2), (0, 1, 1, 2),
    ]
    expected = 0
    for x, y, dx, dy in passes:
        w, h = max(0, (width - x + dx - 1) // dx), max(0, (height - y + dy - 1) // dy)
        if w and h:
            expected += (((w * channels * depth + 7) // 8) + 1) * h
    decoder, actual = zlib.decompressobj(), 0
    for chunk in compressed:
        pending = chunk
        while pending:
            decoded = decoder.decompress(pending, min(65536, expected - actual + 1))
            actual += len(decoded)
            if actual > expected or decoder.unused_data:
                raise ValueError
            pending = decoder.unconsumed_tail
    if not decoder.eof or actual != expected:
        raise ValueError
    return ImageInfo("image/png", width, height, frames)


def _jpeg(data: bytes) -> ImageInfo:
    offset, width, height, scan = 2, 0, 0, False
    components: set[int] = set()
    while offset < len(data):
        if data[offset] != 0xFF:
            raise ValueError
        while offset < len(data) and data[offset] == 0xFF:
            offset += 1
        marker = data[offset]
        offset += 1
        if marker == 0xD9:
            if not width or not scan or offset != len(data):
                raise ValueError
            return ImageInfo("image/jpeg", width, height)
        if marker in {0x00, 0xD8} or 0xD0 <= marker <= 0xD7:
            raise ValueError
        if marker == 0x01:
            continue
        length = struct.unpack_from(">H", data, offset)[0]
        end = offset + length
        if length < 2 or end > len(data):
            raise ValueError
        if marker in {0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF}:
            if length < 8:
                raise ValueError
            height, width = struct.unpack_from(">HH", data, offset + 3)
            count = data[offset + 7]
            if not 1 <= count <= 255 or length != 8 + 3 * count:
                raise ValueError
            components = {data[offset + 8 + 3 * i] for i in range(count)}
            if len(components) != count:
                raise ValueError
        if marker == 0xDA:
            if not width or length < 6:
                raise ValueError
            count = data[offset + 2]
            if not 1 <= count <= min(4, len(components)) or length != 6 + 2 * count:
                raise ValueError
            selectors = [data[offset + 3 + 2 * i] for i in range(count)]
            if len(set(selectors)) != count or not set(selectors) <= components:
                raise ValueError
        offset = end
        if marker == 0xDA:
            scan = True
            while offset < len(data):
                if data[offset] != 0xFF:
                    offset += 1
                    continue
                start = offset
                while offset < len(data) and data[offset] == 0xFF:
                    offset += 1
                next_marker = data[offset]
                if next_marker == 0 or 0xD0 <= next_marker <= 0xD7:
                    offset += 1
                else:
                    offset = start
                    break
    raise ValueError


def _gif(data: bytes) -> ImageInfo:
    width, height, flags = struct.unpack_from("<HHB", data, 6)
    offset = 13 + (3 * (2 ** ((flags & 7) + 1)) if flags & 0x80 else 0)
    frames = 0

    def blocks(pos: int) -> int:
        while True:
            size = data[pos]
            pos += 1
            if not size:
                return pos
            pos += size
            if pos > len(data):
                raise ValueError

    while offset < len(data):
        kind = data[offset]
        offset += 1
        if kind == 0x3B:
            if offset != len(data) or not frames:
                raise ValueError
            return ImageInfo("image/gif", width, height, frames)
        if kind == 0x21:
            offset = blocks(offset + 1)
        elif kind == 0x2C:
            left, top, w, h, flags = struct.unpack_from("<HHHHB", data, offset)
            if not w or not h or left + w > width or top + h > height:
                raise ValueError
            offset += 9 + (3 * (2 ** ((flags & 7) + 1)) if flags & 0x80 else 0)
            if not 2 <= data[offset] <= 8:
                raise ValueError
            offset = blocks(offset + 1)
            frames += 1
        else:
            raise ValueError
    raise ValueError


def _webp_pixels(kind: bytes, chunk: bytes) -> tuple[int, int]:
    if kind == b"VP8 ":
        if len(chunk) < 10 or chunk[0] & 1 or chunk[3:6] != b"\x9d\x01\x2a":
            raise ValueError
        w, h = struct.unpack_from("<HH", chunk, 6)
        result = w & 0x3FFF, h & 0x3FFF
    elif kind == b"VP8L":
        if len(chunk) < 5 or chunk[0] != 0x2F:
            raise ValueError
        bits = int.from_bytes(chunk[1:5], "little")
        if bits >> 29:
            raise ValueError
        result = 1 + (bits & 0x3FFF), 1 + ((bits >> 14) & 0x3FFF)
    else:
        raise ValueError
    if not all(result):
        raise ValueError
    return result


def _webp(data: bytes) -> ImageInfo:
    if struct.unpack_from("<I", data, 4)[0] + 8 != len(data):
        raise ValueError
    offset, width, height, frames, pixels = 12, 0, 0, 0, False
    animated = animation_header = False
    while offset < len(data):
        kind = data[offset:offset + 4]
        length = struct.unpack_from("<I", data, offset + 4)[0]
        start, end = offset + 8, offset + 8 + length
        if end + (length & 1) > len(data):
            raise ValueError
        chunk = data[start:end]
        if kind == b"VP8X":
            if offset != 12 or length != 10 or chunk[0] & 0xC1 or any(chunk[1:4]):
                raise ValueError
            width = 1 + int.from_bytes(chunk[4:7], "little")
            height = 1 + int.from_bytes(chunk[7:10], "little")
            animated = bool(chunk[0] & 2)
        elif kind in {b"VP8 ", b"VP8L"}:
            if animated or pixels:
                raise ValueError
            w, h = _webp_pixels(kind, chunk)
            if width and (w, h) != (width, height):
                raise ValueError
            width, height, pixels = w, h, True
        elif kind == b"ANIM":
            if not animated or animation_header or length != 6 or frames:
                raise ValueError
            animation_header = True
        elif kind == b"ANMF":
            if not animation_header or length < 24:
                raise ValueError
            x, y = (2 * int.from_bytes(chunk[p:p + 3], "little") for p in (0, 3))
            w, h = (1 + int.from_bytes(chunk[p:p + 3], "little") for p in (6, 9))
            if x + w > width or y + h > height or chunk[15] & 0xFC:
                raise ValueError
            pos, frame_pixels, alpha = 16, False, False
            while pos < length:
                subkind = chunk[pos:pos + 4]
                size = struct.unpack_from("<I", chunk, pos + 4)[0]
                subend = pos + 8 + size
                if subend + (size & 1) > length:
                    raise ValueError
                payload = chunk[pos + 8:subend]
                if subkind == b"ALPH":
                    if alpha or frame_pixels or not size:
                        raise ValueError
                    alpha = True
                else:
                    if frame_pixels or (alpha and subkind == b"VP8L"):
                        raise ValueError
                    if _webp_pixels(subkind, payload) != (w, h):
                        raise ValueError
                    frame_pixels = True
                pos = subend + (size & 1)
            if not frame_pixels:
                raise ValueError
            frames += 1
            pixels = True
        offset = end + (length & 1)
    if not pixels or animated and not frames:
        raise ValueError
    return ImageInfo("image/webp", width, height, frames or 1)
