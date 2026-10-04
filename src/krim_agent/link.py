"""安全 Web Link 的 Python 裁决与 App→Host 通知；不自动读取或导航。"""

from __future__ import annotations

import ipaddress
import re
from urllib.parse import quote, unquote, urljoin, urlsplit, urlunsplit

from pydantic import Field, field_validator

from .protocol.models import StrictModel


def _text(value: str) -> None:
    if not value or len(value) > 2048:
        raise ValueError("Link 长度必须在 1–2048 字符之间")
    if any(ord(char) < 33 or ord(char) == 127 or char.isspace() for char in value):
        raise ValueError("Link 不能包含空白或控制字符")
    if "\\" in value:
        raise ValueError("Link 不能包含反斜线")
    decoded = unquote(value)
    if any(ord(char) < 32 or ord(char) == 127 or char == "\\" for char in decoded):
        raise ValueError("Link 不能编码控制字符或反斜线")
    raw_scheme = re.match(r"^([^/?#]*):", value)
    decoded_scheme = re.match(r"^([^/?#]*):", decoded)
    if decoded_scheme is not None and decoded_scheme.group(1).lower() not in {"http", "https"}:
        raise ValueError("Link scheme 不在 Web 白名单中")
    if raw_scheme is None and decoded_scheme is not None:
        raise ValueError("Link 不能编码 scheme")


def normalize_link(value: str, *, base_url: str | None = None) -> str:
    _text(value)
    try:
        parsed = urlsplit(value)
    except ValueError:
        raise ValueError("Link URL 无效") from None
    if not parsed.scheme:
        if base_url is None:
            raise ValueError("相对 Link 需要明确页面 base")
        base = normalize_link(base_url)
        value = urljoin(base, value)
        parsed = urlsplit(value)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        raise ValueError("Link 必须是 HTTP/HTTPS URL")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("Link 不允许携带凭证")
    if "\\" in parsed.netloc or "%" in parsed.netloc or parsed.netloc.endswith(":"):
        raise ValueError("Link authority 无效")
    try:
        port = parsed.port
        host = parsed.hostname.encode("idna").decode("ascii").lower()
    except (ValueError, UnicodeError):
        raise ValueError("Link 主机或端口无效") from None
    if ":" in host:
        try:
            host = "[" + ipaddress.IPv6Address(host).compressed + "]"
        except ValueError:
            raise ValueError("Link IPv6 主机无效") from None
    else:
        labels = host.rstrip(".").split(".")
        if len(host) > 253 or any(
            not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label) for label in labels
        ):
            raise ValueError("Link 主机标签无效")
    scheme = parsed.scheme.lower()
    if port is not None and port != (80 if scheme == "http" else 443):
        host += f":{port}"
    path = quote(parsed.path or "/", safe="/%:@!$&()*+,;=-._~")
    query = quote(parsed.query, safe="/%?:@!$&()*+,;=-._~")
    fragment = quote(parsed.fragment, safe="/%?:@!$&()*+,;=-._~")
    normalized = urlunsplit((scheme, host, path, query, fragment))
    if len(normalized) > 2048:
        raise ValueError("规范化 Link 超过长度限制")
    return normalized


def link_origin(value: str) -> tuple[str, str, int]:
    parsed = urlsplit(normalize_link(value))
    return (
        parsed.scheme,
        parsed.hostname or "",
        parsed.port if parsed.port is not None else (80 if parsed.scheme == "http" else 443),
    )


class LinkResolve(StrictModel):
    url: str = Field(min_length=1, max_length=2048)
    base_url: str | None = Field(default=None, min_length=1, max_length=2048)


class LinkTarget(StrictModel):
    url: str
    external: bool


class LinkNavigate(StrictModel):
    url: str = Field(min_length=1, max_length=2048)
    topic_id: str | None = Field(default=None, min_length=1, max_length=128)
    message_id: str | None = Field(default=None, min_length=1, max_length=128)

    @field_validator("url")
    @classmethod
    def safe_url(cls, value: str) -> str:
        return normalize_link(value)
