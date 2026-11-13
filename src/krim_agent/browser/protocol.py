"""Closed read-only protocol between the trusted host and an isolated worker."""

from __future__ import annotations

import base64
import hashlib
import struct
import zlib
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, Protocol

from krim_agent.contracts import ServiceError, fingerprint


@dataclass(frozen=True, slots=True)
class BrowserCommand:
    """The only commands the host may send to a browser worker."""

    operation: Literal["navigate", "snapshot", "screenshot", "close"]
    url: str | None = None

    def __post_init__(self) -> None:
        if self.operation not in {"navigate", "snapshot", "screenshot", "close"}:
            raise ServiceError("browser_invalid_input", 422)
        if self.operation == "navigate":
            if not isinstance(self.url, str) or not self.url or len(self.url) > 4096:
                raise ServiceError("browser_invalid_input", 422)
        elif self.url is not None:
            raise ServiceError("browser_invalid_input", 422)

    def wire(self) -> dict[str, str]:
        return {"operation": self.operation, **({"url": self.url} if self.url else {})}


@dataclass(frozen=True, slots=True)
class BrowserResult:
    """Bounded, credential-free result returned by a trusted worker adapter."""

    operation: Literal["navigate", "snapshot", "screenshot", "close"]
    final_url: str | None = None
    title: str | None = None
    text: str | None = None
    screenshot_png: bytes | None = None
    truncated: bool = False

    def __post_init__(self) -> None:
        if self.operation == "snapshot":
            if self.text is None or self.screenshot_png is not None:
                raise ServiceError("browser_protocol", 502)
            if len(self.text.encode("utf-8")) > 32768:
                raise ServiceError("browser_output_limit", 502)
        elif self.operation == "screenshot":
            if self.screenshot_png is None or self.text is not None:
                raise ServiceError("browser_protocol", 502)
            validate_png(self.screenshot_png)
        elif self.text is not None or self.screenshot_png is not None:
            raise ServiceError("browser_protocol", 502)
        if self.title is not None and len(self.title.encode("utf-8")) > 1024:
            raise ServiceError("browser_output_limit", 502)
        if self.final_url is not None and len(self.final_url) > 4096:
            raise ServiceError("browser_output_limit", 502)


def validate_png(data: bytes) -> None:
    """Accept a bounded PNG only after checking its chunks, CRCs and pixels."""
    signature = b"\x89PNG\r\n\x1a\n"
    if (
        type(data) is not bytes
        or not 33 <= len(data) <= 1_048_576
        or not data.startswith(signature)
    ):
        raise ServiceError("browser_artifact_invalid", 422)
    offset = len(signature)
    width = height = 0
    seen_header = seen_data = seen_end = idat_closed = False
    compressed = bytearray()
    while offset < len(data):
        if offset + 12 > len(data):
            raise ServiceError("browser_artifact_invalid", 422)
        length = struct.unpack_from(">I", data, offset)[0]
        kind = data[offset + 4 : offset + 8]
        end = offset + 12 + length
        if length > 1_048_576 or end > len(data):
            raise ServiceError("browser_artifact_invalid", 422)
        chunk = data[offset + 8 : offset + 8 + length]
        expected_crc = struct.unpack_from(">I", data, offset + 8 + length)[0]
        if zlib.crc32(kind + chunk) & 0xFFFFFFFF != expected_crc:
            raise ServiceError("browser_artifact_invalid", 422)
        if not seen_header:
            if kind != b"IHDR" or length != 13:
                raise ServiceError("browser_artifact_invalid", 422)
            width, height, depth, color, compression, filtering, interlace = struct.unpack(
                ">IIBBBBB", chunk
            )
            channels = {0: 1, 2: 3, 3: 1, 4: 2, 6: 4}.get(color)
            if (
                color not in {2, 6}
                or depth != 8
                or channels is None
                or compression != 0
                or filtering != 0
                or interlace != 0
                or not 1 <= width <= 4096
                or not 1 <= height <= 4096
                or width * height > 8_000_000
            ):
                raise ServiceError("browser_artifact_invalid", 422)
            seen_header = True
        elif kind == b"IHDR":
            raise ServiceError("browser_artifact_invalid", 422)
        if kind == b"IDAT":
            if seen_end or idat_closed:
                raise ServiceError("browser_artifact_invalid", 422)
            seen_data = True
            compressed.extend(chunk)
            if len(compressed) > 1_048_576:
                raise ServiceError("browser_artifact_invalid", 422)
        elif kind == b"IEND":
            idat_closed = True
            if length != 0 or not seen_data:
                raise ServiceError("browser_artifact_invalid", 422)
            seen_end = True
            if end != len(data):
                raise ServiceError("browser_artifact_invalid", 422)
            break
        elif kind not in {b"IHDR", b"IDAT", b"IEND"} and kind[0] & 0x20 == 0:
            raise ServiceError("browser_artifact_invalid", 422)
        offset = end
    if not seen_header or not seen_data or not seen_end:
        raise ServiceError("browser_artifact_invalid", 422)
    # PNG has a strict upper bound on decoded scanline bytes; enforce it while
    # inflating to prevent compressed bombs from exhausting host memory.
    channels = {0: 1, 2: 3, 3: 1, 4: 2, 6: 4}[color]
    row_bytes = (width * channels * depth + 7) // 8
    expected_size = (row_bytes + 1) * height
    decoder = zlib.decompressobj()
    try:
        decoded = decoder.decompress(bytes(compressed), expected_size + 1)
    except zlib.error:
        raise ServiceError("browser_artifact_invalid", 422) from None
    if len(decoded) != expected_size or not decoder.eof or decoder.unused_data:
        raise ServiceError("browser_artifact_invalid", 422)
    stride = row_bytes + 1
    if any(decoded[row * stride] > 4 for row in range(height)):
        raise ServiceError("browser_artifact_invalid", 422)


class BrowserWorker(Protocol):
    """Transport owned by a browser manager; implementations must use the proxy."""

    async def command(self, message: Mapping[str, str]) -> Mapping[str, object]: ...

    async def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class BrowserWorkerPolicy:
    """Trusted worker/proxy identity included in a browser binding fingerprint."""

    worker_image_digest: str
    proxy_policy_digest: str
    version: Literal["browser/1"] = "browser/1"

    def __post_init__(self) -> None:
        for value in (self.worker_image_digest, self.proxy_policy_digest):
            if len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
                raise ValueError("browser worker policy digest invalid")

    @property
    def digest(self) -> str:
        return fingerprint(
            {
                "worker_image_digest": self.worker_image_digest,
                "proxy_policy_digest": self.proxy_policy_digest,
                "version": self.version,
                "operations": ["navigate", "snapshot", "screenshot", "close"],
            }
        )


def decode_png(value: object) -> bytes:
    if not isinstance(value, str):
        raise ServiceError("browser_protocol", 502)
    try:
        data = base64.b64decode(value, validate=True)
    except (ValueError, TypeError):
        raise ServiceError("browser_protocol", 502) from None
    validate_png(data)
    return data


def result_from_wire(expected: str, value: Mapping[str, object]) -> BrowserResult:
    """Strictly parse the reply shape; reject unexpected fields and operation drift."""
    allowed = {
        "navigate": {"operation", "final_url", "title"},
        "snapshot": {"operation", "final_url", "title", "text", "truncated"},
        "screenshot": {"operation", "screenshot_png_base64", "final_url", "title"},
        "close": {"operation"},
    }.get(expected)
    if allowed is None or set(value) - allowed or value.get("operation") != expected:
        raise ServiceError("browser_protocol", 502)
    if expected == "screenshot":
        png = decode_png(value.get("screenshot_png_base64"))
        final_url = value.get("final_url")
        title = value.get("title")
        for child in (final_url, title):
            if child is not None and not isinstance(child, str):
                raise ServiceError("browser_protocol", 502)
        return BrowserResult(
            "screenshot",
            final_url=final_url if isinstance(final_url, str) else None,
            title=title if isinstance(title, str) else None,
            screenshot_png=png,
        )
    final_url = value.get("final_url")
    title = value.get("title")
    text = value.get("text")
    for child in (final_url, title, text):
        if child is not None and not isinstance(child, str):
            raise ServiceError("browser_protocol", 502)
    truncated = value.get("truncated", False)
    if type(truncated) is not bool:
        raise ServiceError("browser_protocol", 502)
    return BrowserResult(
        expected,  # type: ignore[arg-type]
        final_url=final_url if isinstance(final_url, str) else None,
        title=title if isinstance(title, str) else None,
        text=text if isinstance(text, str) else None,
        truncated=truncated,
    )


def screenshot_key(call_id: str, data: bytes) -> tuple[str, str]:
    """Return idempotency key and content digest without exposing capture bytes."""
    digest = hashlib.sha256(data).hexdigest()
    return f"browser-{call_id}", digest
