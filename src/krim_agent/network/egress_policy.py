"""出站目标与地址的纯校验策略。

策略对象由可信宿主构造并交给传输层；请求 URL 只能被策略校验，
任何 URL 内容都无法放宽策略。loopback 等内网目标只能通过
:meth:`EgressPolicy.authorize_loopback` 显式授权（用于宿主已确认的本机服务），
普通构造的 policy 永远拒绝内网地址，地址层校验是最后一道防线。

URL 规则与 ``krim_agent.mcp.types.MCPServerConfig`` 保持一致：
仅 https（loopback 授权另可 cleartext http）、无 userinfo、
URL 中任何位置不得出现 ``?``（query）或 ``#``（fragment）、不得含空白字符、
host 去尾部点并小写后与白名单精确匹配（不允许通配符）。
host 统一 IDNA 规范化为报文使用的 punycode 形式再匹配：unicode 与 punycode
写法等价命中同一白名单条目，IDNA 有损折叠歧义（软连字符、零宽、全角等）在
构造期与校验期都拒绝。
"""

from __future__ import annotations

import ipaddress
import unicodedata
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from urllib.parse import urlsplit

__all__ = ["EgressPolicy", "EgressTarget", "EgressViolation", "normalize_host"]


class EgressViolation(Exception):
    """出站请求被策略拒绝；消息只含 host/地址与原因，不含请求头或凭据。"""


def normalize_host(host: str) -> str:
    """与 MCP 配置一致：小写、去尾部点，作为精确匹配形式。"""
    return host.rstrip(".").lower()


def _canonical_host(normalized: str) -> str:
    """把（已小写、去尾点的）host 规范化为报文使用的 ASCII（IDNA）形式。

    非 ASCII host 经 IDNA 编码为 punycode——与 httpx 原始 host、TLS SNI、
    DNS 查询同形，保证白名单匹配、钉扎与证书校验身份一致。并要求编码能
    原样解回：nameprep/NFKC 有损折叠（软连字符、零宽字符、全角、``ß``→ss
    等）一律拒绝，阻断"不同写法同形"的歧义；合法 IDN 与 punycode 字面量
    保留。punycode 编码对未折叠输入是双射，同形异字 host 必须以规范形
    显式列入白名单才能命中。
    """
    if normalized.isascii():
        return normalized
    try:
        encoded = normalized.encode("idna").decode("ascii")
        if encoded.encode("ascii").decode("idna") != normalized:
            raise EgressViolation("host idna folding is ambiguous")
        return encoded
    except UnicodeError:
        raise EgressViolation("host is not a valid idna host") from None


def _contains_control_or_space(text: str) -> bool:
    """空白或任意 Unicode 控制字符（Cc，含 ``\\x7f`` 与 C1 区 U+0080-U+009F）。"""
    return any(char.isspace() or unicodedata.category(char) == "Cc" for char in text)


_DEFAULT_PORTS = {"https": 443, "http": 80}

# 显式拒绝网段：不依赖不同 Python 版本对分类标志的差异。
# 其中 ::ffff:0:0/96、64:ff9b::/96、2002::/16 在部分版本仍标为 is_global。
_REJECT_NETWORKS = tuple(
    ipaddress.ip_network(cidr)
    for cidr in (
        "0.0.0.0/8",
        "10.0.0.0/8",
        "100.64.0.0/10",
        "127.0.0.0/8",
        "169.254.0.0/16",
        "172.16.0.0/12",
        "192.0.0.0/24",
        "192.0.2.0/24",
        "192.168.0.0/16",
        "198.18.0.0/15",
        "198.51.100.0/24",
        "203.0.113.0/24",
        "240.0.0.0/4",
        "255.255.255.255/32",
        "::/128",
        "::1/128",
        "::ffff:0:0/96",
        "64:ff9b::/96",
        "fe80::/10",
        "fc00::/7",
        "ff00::/8",
        "2002::/16",
    )
)


@dataclass(frozen=True, slots=True)
class EgressTarget:
    """一次出站请求校验得到的目标要素。"""

    scheme: str
    host: str  # IDNA 规范形，与 allowed_hosts 的匹配形式一致
    port: int
    connect_host: str  # IDNA 规范形（ASCII），用于 DNS、TLS 与连接复用键


def _is_ip_literal(candidate: str) -> bool:
    try:
        ipaddress.ip_address(candidate)
    except ValueError:
        return False
    return True


def _normalize_allowed_hosts(allowed_hosts: Iterable[str]) -> frozenset[str]:
    hosts: set[str] = set()
    for entry in allowed_hosts:
        normalized = normalize_host(entry)
        if (
            not normalized
            or _contains_control_or_space(normalized)
            or any(char in normalized for char in ("*", "/", "@"))
            or (":" in normalized and not _is_ip_literal(normalized))
        ):
            raise EgressViolation("allowed host entry is invalid")
        hosts.add(_canonical_host(normalized))
    return frozenset(hosts)


def _normalize_allowed_ports(allowed_ports: Iterable[int]) -> frozenset[int]:
    ports: set[int] = set()
    for port in allowed_ports:
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise EgressViolation("allowed port entry is invalid")
        ports.add(int(port))
    return frozenset(ports)


def _unsafe_address_reason(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> str | None:
    if address.is_multicast:
        return f"multicast address is not allowed: {address}"
    if address.is_unspecified:
        return f"unspecified address is not allowed: {address}"
    if address.is_reserved:
        return f"reserved address is not allowed: {address}"
    if address.is_link_local:
        return f"link-local address is not allowed: {address}"
    if address.is_private:
        return f"private address is not allowed: {address}"
    for network in _REJECT_NETWORKS:
        if address in network:
            return f"address is in a blocked range: {address}"
    if not address.is_global:
        return f"address is not globally routable: {address}"
    return None


class EgressPolicy:
    """显式出站白名单：精确 host、精确端口、默认仅 HTTPS、拒绝内网地址。"""

    def __init__(self, allowed_hosts: Iterable[str], allowed_ports: Iterable[int]) -> None:
        self._allowed_hosts = _normalize_allowed_hosts(allowed_hosts)
        self._allowed_ports = _normalize_allowed_ports(allowed_ports)
        self._loopback_clearance = False

    @classmethod
    def authorize_loopback(
        cls, allowed_hosts: Iterable[str], allowed_ports: Iterable[int]
    ) -> EgressPolicy:
        """可信宿主专用构造：显式授权 loopback 目标（本机联调服务）。

        普通构造与任何用户提供的 URL 都拿不到该授权；授权同时允许
        cleartext http，但仅限 localhost/loopback 字面量，与 MCP 规则一致。
        """
        policy = cls(allowed_hosts, allowed_ports)
        policy._loopback_clearance = True
        return policy

    @property
    def allows_loopback(self) -> bool:
        return self._loopback_clearance

    def validate_target(self, url: str) -> EgressTarget:
        """校验请求 URL 是否满足出站白名单；通过则返回目标要素。"""
        if _contains_control_or_space(url):
            raise EgressViolation("egress URL contains whitespace or control characters")
        if "?" in url or "#" in url:
            raise EgressViolation("egress URL must not contain query or fragment")
        try:
            parsed = urlsplit(url)
            hostname = parsed.hostname
            port = parsed.port
        except ValueError:
            raise EgressViolation("egress URL is invalid") from None
        scheme = parsed.scheme.lower()
        if scheme not in _DEFAULT_PORTS or (scheme == "http" and not self._loopback_clearance):
            raise EgressViolation("only https egress is allowed")
        if parsed.username is not None or parsed.password is not None:
            raise EgressViolation("egress URL must not contain userinfo")
        if not parsed.netloc or not hostname:
            raise EgressViolation("egress URL has no host")
        normalized = normalize_host(hostname)
        canonical = _canonical_host(normalized)
        if canonical not in self._allowed_hosts:
            raise EgressViolation(f"egress host is not allowed: {normalized}")
        if scheme == "http" and not _is_ip_literal(canonical) and canonical != "localhost":
            raise EgressViolation("cleartext http is allowed only for loopback hosts")
        if port is None:
            port = _DEFAULT_PORTS[scheme]
        if port not in self._allowed_ports:
            raise EgressViolation("egress port is not allowed")
        return EgressTarget(
            scheme=scheme,
            host=canonical,
            port=port,
            connect_host=canonical,
        )

    def approve_addresses(self, addresses: Sequence[str]) -> tuple[str, ...]:
        """校验 DNS 解析结果：全部地址都必须安全，返回去重后的连接地址。

        任一地址落在内网/保留段即整体拒绝（防 mixed public/private DNS）；
        IPv4-mapped IPv6 先解包成 IPv4 再统一判定；loopback 仅在
        authorize_loopback 授权下允许，且必须全部地址都是 loopback。
        """
        resolved: list[ipaddress.IPv4Address | ipaddress.IPv6Address] = []
        seen: set[str] = set()
        for item in addresses:
            if not isinstance(item, str):
                raise EgressViolation("dns resolution returned a non-string address")
            try:
                address = ipaddress.ip_address(item)
            except ValueError:
                raise EgressViolation("dns resolution returned an invalid address") from None
            if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
                address = address.ipv4_mapped
            if str(address) not in seen:
                seen.add(str(address))
                resolved.append(address)
        if not resolved:
            raise EgressViolation("dns resolution returned no addresses")
        if any(address.is_loopback for address in resolved):
            if not self._loopback_clearance:
                raise EgressViolation("loopback address is not allowed")
            if not all(address.is_loopback for address in resolved):
                raise EgressViolation("mixed loopback and public dns resolution is not allowed")
            return tuple(str(address) for address in resolved)
        for address in resolved:
            reason = _unsafe_address_reason(address)
            if reason is not None:
                raise EgressViolation(reason)
        return tuple(str(address) for address in resolved)
