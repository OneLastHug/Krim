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
# Application CSRF/request headers must survive the proxy. Keep platform and
# proxy identity/routing namespaces private rather than naming individual apps.
_PRIVATE_X_HEADERS = {
    b"x-real-ip", b"x-client-ip", b"x-cluster-client-ip",
    b"x-original-url", b"x-original-host", b"x-rewrite-url",
    b"x-http-method-override", b"x-http-method", b"x-method-override",
}
_PRIVATE_X_PREFIXES = (
    b"x-krim-", b"x-forwarded-", b"x-proxy-", b"x-envoy-",
    b"x-amzn-", b"x-amz-", b"x-ms-", b"x-azure-", b"x-cloudflare-",
)
_DOMAIN = re.compile(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?\Z")
_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")
_HOST_KEY = "[0-9a-f]{40}"


@dataclass(frozen=True)
class PreviewOrigins:
    control_origin: str
    # Explicit deployment inputs. The default mode serves previews from an
    # independent registrable site. allow_same_site_subdomains instead opts
    # into the sibling layout: preview hosts share preview_domain with the
    # control host as first-level labels, so the control cookie domain must
    # stay host-only. hostname_prefix names the preview label family.
    control_cookie_domain: str
    preview_domain: str
    allow_same_site_subdomains: bool = False
    hostname_prefix: str = "p"

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
                or any(not _LABEL.fullmatch(part) for part in domain.split("."))
            ):
                raise ValueError("provide explicit independent DNS cookie domains")
        if not _LABEL.fullmatch(self.hostname_prefix) or len(self.hostname_prefix) > 22:
            # The preview label is prefix + "-" + 40 hex and must stay one
            # valid DNS label, so the prefix can add at most 22 characters.
            raise ValueError("hostname_prefix must keep the preview label within 63 characters")
        if len(self.hostname_prefix) + 42 + len(self.preview_domain) > 253:
            raise ValueError("preview hostnames exceed the DNS length limit")
        if not (
            control.hostname == self.control_cookie_domain
            or control.hostname.endswith("." + self.control_cookie_domain)
        ):
            raise ValueError("control cookie domain does not contain control host")
        if self.allow_same_site_subdomains:
            self._check_same_site(control.hostname)
        else:
            a, b = self.control_cookie_domain, self.preview_domain
            if a == b or a.endswith("." + b) or b.endswith("." + a):
                raise ValueError("preview must use an independent cookie domain")

    def _check_same_site(self, control_host: str) -> None:
        # Sibling layout only: preview hosts and the control host live side by
        # side under preview_domain. Control cookies are host-only, so the
        # browser never carries them to a sibling, and the parent cookie
        # domain that would leak them into previews stays rejected.
        if self.control_cookie_domain != control_host:
            raise ValueError("same-site previews require a host-only control cookie domain")
        label, separator, _ = control_host.rpartition("." + self.preview_domain)
        # Exactly one label under preview_domain: deeper control hosts would
        # blur the sibling layout this flag exists to express.
        if not separator or not _LABEL.fullmatch(label):
            raise ValueError(
                "same-site previews require the control host one label under preview_domain"
            )
        if re.fullmatch(re.escape(self.hostname_prefix) + "-" + _HOST_KEY, label):
            raise ValueError("control host must stay outside the preview hostname family")

    def host(self, preview_id: str) -> str:
        # Stable DNS-safe key; no owner/project strings enter browser hostnames.
        label = hashlib.sha256(preview_id.encode()).hexdigest()[:40]
        return f"{self.hostname_prefix}-{label}.{self.preview_domain}"

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
    result = []
    for key, value in headers:
        name = key.lower()
        application_header = (
            name.startswith(b"x-")
            and name not in _PRIVATE_X_HEADERS
            and not name.startswith(_PRIVATE_X_PREFIXES)
        )
        if name not in excluded and (name in _REQUEST or application_header):
            result.append((name, value))
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
    allow_embedding: bool = False,
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
    if allow_embedding:
        # The host policy below replaces upstream frame restrictions, allowing
        # only the configured control origin, never another preview or any site.
        excluded |= {b"x-frame-options"}
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
                (
                    "frame-ancestors " + (control_origin if allow_embedding else "'none'")
                    + "; base-uri 'self'; form-action 'self'; worker-src 'none'"
                ).encode("ascii"),
            ),
        ]
    )
    return result
