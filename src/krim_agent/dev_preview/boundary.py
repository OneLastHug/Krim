"""Fixed preview origins and narrow HTTP header/cookie translation."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from http.cookies import CookieError, SimpleCookie
from urllib.parse import unquote, urlsplit

from .service import PreviewError

SESSION_COOKIE = "__Host-Krim-Preview"
BOOTSTRAP_PATH = "/.krim-preview/session"
_HOP = {
    b"connection",
    b"keep-alive",
    b"proxy-authenticate",
    b"proxy-authorization",
    b"te",
    b"trailer",
    b"transfer-encoding",
    b"upgrade",
}
_REQUEST = {
    b"accept",
    b"accept-encoding",
    b"accept-language",
    b"content-type",
    b"content-length",
    b"range",
    b"if-range",
    b"if-match",
    b"if-none-match",
    b"if-modified-since",
    b"if-unmodified-since",
    b"cache-control",
    b"pragma",
    b"user-agent",
}
_DOMAIN = re.compile(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?\Z")


@dataclass(frozen=True)
class PreviewOrigins:
    control_origin: str
    # Explicit deployment inputs: independent registrable sites, not siblings.
    control_cookie_domain: str
    preview_domain: str

    def __post_init__(self) -> None:
        control = urlsplit(self.control_origin)
        if (
            control.scheme != "https"
            or not control.hostname
            or control.username
            or control.password
            or control.path
            or control.query
            or control.fragment
            or control.netloc != control.netloc.lower()
        ):
            raise ValueError("control_origin must be a canonical HTTPS origin")
        for domain in (self.control_cookie_domain, self.preview_domain):
            if (
                not _DOMAIN.fullmatch(domain)
                or "." not in domain
                or ".." in domain
                or any(not part or len(part) > 63 for part in domain.split("."))
            ):
                raise ValueError("provide explicit independent DNS cookie domains")
        if not (
            control.hostname == self.control_cookie_domain
            or control.hostname.endswith("." + self.control_cookie_domain)
        ):
            raise ValueError("control cookie domain does not contain control host")
        a, b = self.control_cookie_domain, self.preview_domain
        if a == b or a.endswith("." + b) or b.endswith("." + a):
            raise ValueError("preview must use an independent cookie domain")

    def host(self, preview_id: str) -> str:
        # Stable DNS-safe key; no owner/project strings enter browser hostnames.
        label = hashlib.sha256(preview_id.encode()).hexdigest()[:40]
        return f"p-{label}.{self.preview_domain}"

    def origin(self, preview_id: str) -> str:
        return "https://" + self.host(preview_id)


def request_target(raw_path: bytes, query: bytes) -> bytes:
    try:
        path = raw_path.decode("ascii")
        search = query.decode("ascii")
    except UnicodeError:
        raise PreviewError("preview_target_invalid", 400) from None
    decoded = unquote(path)
    if (
        not path.startswith("/")
        or decoded.startswith("//")
        or "\\" in decoded
        or any(ord(c) < 32 or ord(c) == 127 for c in path + search + decoded)
        or "#" in path + search
        or "?" in path
    ):
        raise PreviewError("preview_target_invalid", 400)
    return raw_path + (b"?" + query if query else b"")


def connection_tokens(headers: list[tuple[bytes, bytes]]) -> set[bytes]:
    return {
        token.strip().lower()
        for k, v in headers
        if k.lower() == b"connection"
        for token in v.split(b",")
    }


def parse_cookies(headers: list[tuple[bytes, bytes]]) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, value in headers:
        if key.lower() != b"cookie":
            continue
        for part in value.decode("latin1").split(";"):
            cookie = SimpleCookie()
            try:
                cookie.load(part.strip())
            except CookieError:
                raise PreviewError("preview_cookie_invalid", 400) from None
            if len(cookie) != 1:
                raise PreviewError("preview_cookie_invalid", 400)
            for name, morsel in cookie.items():
                if name in result:
                    raise PreviewError("preview_cookie_invalid", 400)
                result[name] = morsel.coded_value
    return result


def request_headers(
    headers: list[tuple[bytes, bytes]],
    host: str,
    origin: str,
) -> list[tuple[bytes, bytes]]:
    excluded = _HOP | connection_tokens(headers)
    result = [
        (k.lower(), v) for k, v in headers if k.lower() in _REQUEST and k.lower() not in excluded
    ]
    cookies = parse_cookies(headers)
    app_cookies = "; ".join(f"{k}={v}" for k, v in cookies.items() if k != SESSION_COOKIE)
    if app_cookies:
        result.append((b"cookie", app_cookies.encode("latin1")))
    result.extend(
        [(b"host", host.encode()), (b"origin", origin.encode()), (b"x-forwarded-proto", b"https")]
    )
    return result


def response_headers(
    headers: list[tuple[bytes, bytes]],
    *,
    origin: str,
    upstream: str,
    control_origin: str,
) -> list[tuple[bytes, bytes]]:
    excluded = (
        _HOP
        | connection_tokens(headers)
        | {
            b"server",
            b"x-powered-by",
            b"alt-svc",
            b"report-to",
            b"reporting-endpoints",
            b"nel",
            b"service-worker-allowed",
            b"link",
            b"x-content-type-options",
            b"refresh",
            b"clear-site-data",
            b"content-location",
            b"access-control-allow-origin",
            b"access-control-allow-credentials",
            b"set-cookie",
            b"location",
            b"referrer-policy",
            b"cache-control",
            b"content-security-policy",
            b"content-security-policy-report-only",
        }
    )
    result = [(k.lower(), v) for k, v in headers if k.lower() not in excluded]
    nominated = connection_tokens(headers)
    for k, v in headers:
        if k.lower() in nominated:
            continue
        if k.lower() == b"set-cookie":
            cookie = SimpleCookie()
            try:
                cookie.load(v.decode("latin1"))
            except CookieError:
                continue
            for name, morsel in cookie.items():
                if name == SESSION_COOKIE:
                    continue
                morsel["domain"] = ""
                morsel["secure"] = True
                # Host-only applications can use their own login cookies.
                result.append((b"set-cookie", morsel.OutputString().encode("latin1")))
        if k.lower() == b"location":
            location = v.decode("latin1")
            parsed = urlsplit(location)
            decoded = unquote(location)
            if "\\" in decoded or any(ord(c) < 32 for c in decoded):
                raise PreviewError("preview_redirect_denied", 502)
            if parsed.scheme or parsed.netloc:
                prefix = parsed.scheme + "://" + parsed.netloc
                if prefix not in {origin, upstream}:
                    raise PreviewError("preview_redirect_denied", 502)
                location = origin + (parsed.path or "/")
                if parsed.query:
                    location += "?" + parsed.query
                if parsed.fragment:
                    location += "#" + parsed.fragment
            elif decoded.startswith("//"):
                raise PreviewError("preview_redirect_denied", 502)
            result.append((b"location", location.encode("latin1")))
    result.extend(
        [
            (b"cache-control", b"no-store"),
            (b"referrer-policy", b"no-referrer"),
            (b"x-content-type-options", b"nosniff"),
            (
                b"content-security-policy",
                (b"frame-ancestors 'none'; base-uri 'self'; form-action 'self'; worker-src 'none'"),
            ),
        ]
    )
    return result
