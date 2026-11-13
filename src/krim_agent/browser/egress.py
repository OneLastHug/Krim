"""Exact-origin browser destination policy to be enforced by the dedicated proxy.

This is a policy primitive, not a network sandbox: a worker must have no direct
network route and the proxy must invoke ``authorize_url`` for every request.
"""

from __future__ import annotations

import ipaddress
import re
import unicodedata
from collections.abc import Iterable, Sequence
from urllib.parse import urlsplit

from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.network.egress_policy import EgressPolicy, EgressViolation


class BrowserEgressPolicy:
    """HTTPS-only exact origins; validates destination URLs and all DNS answers."""

    def __init__(self, allowed_origins: Iterable[str]) -> None:
        origins: set[tuple[str, int]] = set()
        for value in allowed_origins:
            host, port = self._parse_origin(value)
            origins.add((host, port))
        if not origins or len(origins) > 64:
            raise ValueError("browser origin allowlist must contain 1..64 origins")
        self._origins = frozenset(origins)
        self._hosts = frozenset(host for host, _ in origins)
        self._ports = frozenset(port for _, port in origins)
        try:
            self._address_policy = EgressPolicy(self._hosts, self._ports)
        except EgressViolation:
            raise ValueError("browser origin allowlist invalid") from None
        self._digest = fingerprint({"version": "browser-egress/1", "origins": list(
            self.allowed_origins
        )})

    @staticmethod
    def _host(value: str) -> str:
        if not value or any(unicodedata.category(ch) == "Cc" for ch in value):
            raise ValueError("browser origin host invalid")
        try:
            address = ipaddress.ip_address(value)
        except ValueError:
            normalized = value.lower().rstrip(".")
            try:
                host = normalized.encode("idna").decode("ascii")
                decoded = host.encode("ascii").decode("idna")
                if not normalized.isascii() and decoded != normalized:
                    raise ValueError("browser origin host invalid")
                if normalized.isascii() and not normalized.startswith("xn--") and decoded != normalized:
                    raise ValueError("browser origin host invalid")
            except UnicodeError:
                raise ValueError("browser origin host invalid") from None
            labels = host.split(".")
            if (
                not host
                or len(host) > 253
                or len(labels) < 2
                or any(not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", x)
                       for x in labels)
                or host == "localhost"
                or host.endswith((".localhost", ".local", ".internal", ".test"))
            ):
                raise ValueError("browser origin host invalid")
            return host
        if not address.is_global:
            raise ValueError("browser origin address must be globally routable")
        return address.compressed.lower()

    @classmethod
    def _parse_origin(cls, value: str) -> tuple[str, int]:
        if (
            not isinstance(value, str)
            or len(value) > 512
            or any(ch.isspace() or unicodedata.category(ch) == "Cc" for ch in value)
            or "*" in value
            or "?" in value
            or "#" in value
            or "\\" in value
        ):
            raise ValueError("browser origin invalid")
        try:
            parsed = urlsplit(value)
            port = parsed.port
        except ValueError:
            raise ValueError("browser origin invalid") from None
        if (
            parsed.scheme.lower() != "https"
            or not parsed.netloc
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in ("", "/")
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("browser origin must be a bare HTTPS origin")
        host = cls._host(parsed.hostname)
        if port is not None and not 1 <= port <= 65535:
            raise ValueError("browser origin port invalid")
        return host, 443 if port is None else port

    @property
    def digest(self) -> str:
        return self._digest

    @property
    def allowed_origins(self) -> tuple[str, ...]:
        def authority(host: str, port: int) -> str:
            literal = f"[{host}]" if ":" in host else host
            return f"https://{literal}:{port}"

        return tuple(sorted(authority(host, port) for host, port in self._origins))

    def authorize_url(self, url: str) -> tuple[str, int]:
        """Authorize a navigation, redirect, subresource, or websocket destination.

        The worker/proxy must additionally block non-GET/HEAD/OPTIONS requests
        and all direct worker sockets. URL policy alone cannot enforce either.
        """
        if (
            not isinstance(url, str)
            or not url
            or len(url) > 4096
            or any(ch.isspace() or unicodedata.category(ch) == "Cc" for ch in url)
            or "\\" in url
        ):
            raise ServiceError("browser_invalid_input", 422)
        try:
            parsed = urlsplit(url)
            explicit_port = parsed.port
            port = 443 if explicit_port is None else explicit_port
        except ValueError:
            raise ServiceError("browser_invalid_input", 422) from None
        if (
            parsed.scheme.lower() not in {"https", "wss"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or "@" in parsed.netloc
        ):
            raise ServiceError("browser_egress_denied", 403)
        try:
            host = self._host(parsed.hostname)
        except ValueError:
            raise ServiceError("browser_egress_denied", 403) from None
        if (host, port) not in self._origins:
            raise ServiceError("browser_egress_denied", 403)
        # EgressPolicy rejects every non-global answer, including mixed DNS sets.
        # Validate the origin-only form so browser query strings remain allowed.
        authority = f"[{host}]" if ":" in host else host
        try:
            self._address_policy.validate_target(f"https://{authority}:{port}/")
        except EgressViolation:
            raise ServiceError("browser_egress_denied", 403) from None
        return host, port

    def authorize_addresses(self, addresses: Sequence[str]) -> tuple[str, ...]:
        try:
            return self._address_policy.approve_addresses(addresses)
        except EgressViolation:
            raise ServiceError("browser_egress_denied", 403) from None
