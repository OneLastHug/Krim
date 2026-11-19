"""Local browser input/output limits; no preview authorization DTOs live here."""

from __future__ import annotations

import hashlib
import re
import secrets
from dataclasses import dataclass, field
from urllib.parse import unquote, urlsplit


class BrowserError(Exception):
    """Only fixed codes cross the tool boundary, never Playwright exceptions."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class BrowserLimits:
    session_seconds: float = 120
    action_seconds: float = 10
    check_seconds: float = 3
    poll_seconds: float = 0.5
    max_actions: int = 40
    max_events: int = 64
    max_requests: int = 256
    max_upload_bytes: int = 1024 * 1024
    max_download_bytes: int = 1024 * 1024
    max_screenshot_bytes: int = 1024 * 1024

    def __post_init__(self) -> None:
        bounds = {
            "session_seconds": (0.1, 300),
            "action_seconds": (0.01, 30),
            "check_seconds": (0.01, 5),
            "poll_seconds": (0.01, 2),
            "max_actions": (1, 100),
            "max_events": (1, 128),
            "max_requests": (1, 1000),
            "max_upload_bytes": (1, 8 * 1024 * 1024),
            "max_download_bytes": (1, 8 * 1024 * 1024),
            "max_screenshot_bytes": (1, 1024 * 1024),
        }
        for name, (low, high) in bounds.items():
            value = getattr(self, name)
            if isinstance(value, bool) or not low <= value <= high:
                raise ValueError("invalid browser limits")
            if name.startswith("max_") and not isinstance(value, int):
                raise ValueError("invalid browser limits")


@dataclass(frozen=True)
class BrowserArtifact:
    """Ephemeral bytes; I/A2 persists these with the originating project scope."""

    kind: str
    media_type: str
    data: bytes = field(repr=False)
    artifact_id: str = field(default_factory=lambda: secrets.token_hex(16))

    @property
    def metadata(self) -> dict[str, str | int]:
        return {
            "artifact_id": self.artifact_id,
            "kind": self.kind,
            "media_type": self.media_type,
            "size": len(self.data),
            "sha256": hashlib.sha256(self.data).hexdigest(),
        }


def canonical_origin(origin: str) -> str:
    parsed = urlsplit(origin)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.path
        or parsed.query
        or parsed.fragment
        or parsed.netloc != parsed.hostname
        or not re.fullmatch(r"[a-z0-9.-]+", parsed.hostname)
    ):
        raise BrowserError("browser_origin_denied")
    return origin


def trusted_control_origin(value: str) -> str:
    """Host-configured control origin; optional port is a header, never egress."""
    try:
        parsed = urlsplit(value)
        canonical_origin("https://" + (parsed.hostname or ""))
        if parsed.hostname is None:
            raise ValueError
        expected = parsed.hostname + (f":{parsed.port}" if parsed.port is not None else "")
        if (
            parsed.scheme != "https"
            or parsed.netloc != expected
            or parsed.path
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError
    except (ValueError, TypeError):
        raise BrowserError("browser_config_invalid") from None
    return value


def relative_target(path: str) -> str:
    if not isinstance(path, str) or not 1 <= len(path) <= 2048:
        raise BrowserError("browser_target_invalid")
    try:
        parsed = urlsplit(path)
    except ValueError:
        raise BrowserError("browser_target_invalid") from None
    decoded = path
    for _ in range(3):
        decoded = unquote(decoded)
    if re.search(r"%[0-9a-fA-F]{2}", decoded):
        raise BrowserError("browser_target_invalid")
    if (
        not path.startswith("/")
        or parsed.scheme
        or parsed.netloc
        or decoded.startswith("//")
        or "\\" in decoded
        or any(ord(c) < 32 or ord(c) == 127 for c in decoded)
        or parsed.fragment
        or decoded.split("?", 1)[0].startswith("/.krim-preview")
        or any(part in {".", ".."} for part in decoded.split("?", 1)[0].split("/"))
    ):
        raise BrowserError("browser_target_invalid")
    return path


def same_origin(url: str, origin: str, *, websocket: bool = False) -> bool:
    try:
        parsed = urlsplit(url)
        expected = urlsplit(origin)
        if (
            parsed.scheme != ("wss" if websocket else "https")
            or parsed.netloc != expected.netloc
            or parsed.username
            or parsed.password
        ):
            return False
        relative_target((parsed.path or "/") + ("?" + parsed.query if parsed.query else ""))
        return not parsed.fragment
    except (ValueError, BrowserError):
        return False


def selector(value: str) -> str:
    # Only CSS in the main frame. Explicitly select the CSS engine so input
    # cannot switch to xpath/text/frame/eval selector engines.
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 512
        or ">>" in value
        or any(ord(c) < 32 for c in value)
    ):
        raise BrowserError("browser_selector_invalid")
    return "css=" + value


def viewport_preset(value: str) -> tuple[int, int]:
    """Only fixed viewport sizes; no arbitrary geometry or device emulation."""
    if value == "desktop":
        return 1024, 768
    if value == "mobile":
        return 375, 812
    raise BrowserError("browser_viewport_invalid")


def bounded_text(value: str) -> str:
    if not isinstance(value, str) or len(value) > 4096:
        raise BrowserError("browser_text_invalid")
    return value


def upload_payload(name: str, media_type: str, data: bytes, maximum: int) -> dict[str, object]:
    if (
        not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,95}", name)
        or ".." in name
        or not re.fullmatch(r"[A-Za-z0-9.+-]+/[A-Za-z0-9.+-]+", media_type)
        or len(media_type) > 100
        or not isinstance(data, bytes)
        or not 0 < len(data) <= maximum
    ):
        raise BrowserError("browser_upload_invalid")
    return {"name": name, "mimeType": media_type, "buffer": data}
