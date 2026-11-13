"""钉扎传输测试。

除标注的合成 TLS 端点用例外，模块级替身均为内存实现，不发起任何真实
网络连接；真实 TLS 用例只连接本机环回上的临时端点（测试专用可信注入，
自签 CA 仅在该用例内生效），不触任何第三方网络。容器强制出口
（防火墙/DNS 代理）不在覆盖范围。
"""

import asyncio
import contextlib
import ssl
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpcore
import httpx
import pytest

from krim_agent.network import http_transport as http_transport_module
from krim_agent.network.egress_policy import EgressPolicy, EgressViolation
from krim_agent.network.http_transport import (
    PinnedAsyncTransport,
    SystemResolver,
    make_http_client,
)

try:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    _CERT_TOOLING = True
except ImportError:  # pragma: no cover -- 合成 TLS 用例仅在此环境缺失时跳过
    _CERT_TOOLING = False


def _response(payload_lines: str, headers: list[tuple[str, str]], body: bytes = b"") -> bytes:
    head = payload_lines + "\r\n"
    head += "".join(f"{name}: {value}\r\n" for name, value in headers)
    return (head + "\r\n").encode("ascii") + body


def _ok_response(body: bytes = b"") -> bytes:
    return _response("HTTP/1.1 200 OK", [("Content-Length", str(len(body)))], body)


class WireStream(httpcore.AsyncNetworkStream):
    """按预置字节应答的假连接流；记录 TLS hostname 与写出字节。"""

    def __init__(self, owner: "RecordingBackend", payload: bytes) -> None:
        self._owner = owner
        self._buffer = payload
        self.written = bytearray()
        self.closed = False

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:  # noqa: ASYNC109 -- httpcore 流接口
        chunk = self._buffer[:max_bytes]
        self._buffer = self._buffer[max_bytes:]
        return chunk

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:  # noqa: ASYNC109 -- httpcore 流接口
        self.written.extend(buffer)

    async def aclose(self) -> None:
        self.closed = True

    async def start_tls(
        self,
        ssl_context: object,
        server_hostname: str,
        timeout: float | None = None,  # noqa: ASYNC109 -- httpcore 流接口
    ) -> httpcore.AsyncNetworkStream:
        self._owner.tls_hostnames.append(server_hostname)
        return self

    def get_extra_info(self, info: str) -> object:
        return None


class RecordingBackend(httpcore.AsyncNetworkBackend):
    """记录连接层实际目标的假 backend；可注入指定 IP 的连接失败或超时。"""

    def __init__(
        self,
        failing_addresses: frozenset[str] = frozenset(),
        timeout_addresses: frozenset[str] = frozenset(),
    ) -> None:
        self.attempts: list[tuple[str, int]] = []
        self.connections: list[tuple[str, int]] = []
        self.tls_hostnames: list[str] = []
        self.streams: list[WireStream] = []
        self._responses: list[bytes] = []
        self._failing = failing_addresses
        self._timing_out = timeout_addresses

    def queue_responses(self, payloads: Sequence[bytes]) -> None:
        self._responses = list(payloads)

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,  # noqa: ASYNC109 -- httpcore 接口签名
        local_address: str | None = None,
        socket_options: Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        self.attempts.append((host, port))
        if host in self._timing_out:
            raise TimeoutError(f"connect timed out: {host}")
        if host in self._failing:
            raise httpcore.ConnectError(f"connection refused: {host}")
        self.connections.append((host, port))
        payload = self._responses.pop(0) if self._responses else b""
        stream = WireStream(self, payload)
        self.streams.append(stream)
        return stream

    async def connect_unix_socket(
        self,
        path: str | bytes,
        timeout: float | None = None,  # noqa: ASYNC109 -- httpcore 接口签名
        socket_options: Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        raise AssertionError("unix socket egress must not be attempted")


class FakeResolver:
    """固定解析表的假解析器；记录调用，绝不触网。"""

    def __init__(self, table: dict[str, Sequence[str]]) -> None:
        self._table = table
        self.calls: list[str] = []

    async def resolve(self, host: str) -> Sequence[str]:
        self.calls.append(host)
        return self._table[host]


def _build(
    policy: EgressPolicy,
    resolver: FakeResolver,
    backend: RecordingBackend,
) -> httpx.AsyncClient:
    transport = PinnedAsyncTransport(policy, resolver=resolver, network_backend=backend)
    return httpx.AsyncClient(
        transport=transport,
        timeout=5.0,
        trust_env=False,
        follow_redirects=False,
    )


async def test_connection_uses_approved_ip_with_original_tls_hostname() -> None:
    policy = EgressPolicy({"api.test"}, {443})
    resolver = FakeResolver({"api.test": ("1.2.3.4",)})
    backend = RecordingBackend()
    backend.queue_responses([_ok_response()])
    async with _build(policy, resolver, backend) as client:
        response = await client.get("https://api.test/hello")
    assert response.status_code == 200
    # 连接层收到的就是解析出并获批准的实际 IP，而不是再做一次 DNS。
    assert backend.connections == [("1.2.3.4", 443)]
    # TLS 的 server_hostname（SNI/证书校验）保持原 host。
    assert backend.tls_hostnames == ["api.test"]
    request_bytes = bytes(backend.streams[0].written)
    assert b"Host: api.test\r\n" in request_bytes
    assert b"1.2.3.4" not in request_bytes  # URL 未被改写成 IP


async def test_disallowed_host_rejected_before_dns() -> None:
    policy = EgressPolicy({"api.test"}, {443})
    resolver = FakeResolver({"api.test": ("1.2.3.4",)})
    backend = RecordingBackend()
    async with _build(policy, resolver, backend) as client:
        with pytest.raises(EgressViolation):
            await client.get("https://evil.test/x")
    assert resolver.calls == []
    assert backend.attempts == []


async def test_cleartext_http_rejected_without_loopback_clearance() -> None:
    policy = EgressPolicy({"api.test"}, {80})
    resolver = FakeResolver({"api.test": ("1.2.3.4",)})
    backend = RecordingBackend()
    async with _build(policy, resolver, backend) as client:
        with pytest.raises(EgressViolation):
            await client.get("http://api.test/")
    assert backend.attempts == []


async def test_mixed_public_private_dns_rejected_whole() -> None:
    policy = EgressPolicy({"api.test"}, {443})
    resolver = FakeResolver({"api.test": ("1.2.3.4", "10.0.0.1")})
    backend = RecordingBackend()
    async with _build(policy, resolver, backend) as client:
        with pytest.raises(EgressViolation):
            await client.get("https://api.test/")
    assert backend.attempts == []


async def test_connection_falls_back_to_next_approved_ip() -> None:
    policy = EgressPolicy({"api.test"}, {443})
    resolver = FakeResolver({"api.test": ("1.2.3.4", "4.5.6.7")})
    backend = RecordingBackend(failing_addresses=frozenset({"1.2.3.4"}))
    backend.queue_responses([_ok_response()])
    async with _build(policy, resolver, backend) as client:
        response = await client.get("https://api.test/")
    assert response.status_code == 200
    assert backend.attempts == [("1.2.3.4", 443), ("4.5.6.7", 443)]
    assert backend.connections == [("4.5.6.7", 443)]


async def test_ipv6_address_is_pinned() -> None:
    policy = EgressPolicy({"api.test"}, {443})
    resolver = FakeResolver({"api.test": ("2606:4700::1111",)})
    backend = RecordingBackend()
    backend.queue_responses([_ok_response()])
    async with _build(policy, resolver, backend) as client:
        response = await client.get("https://api.test/")
    assert response.status_code == 200
    assert backend.connections == [("2606:4700::1111", 443)]
    assert backend.tls_hostnames == ["api.test"]


async def test_ipv4_mapped_public_address_unwrapped_for_connection() -> None:
    policy = EgressPolicy({"api.test"}, {443})
    resolver = FakeResolver({"api.test": ("::ffff:1.2.3.4",)})
    backend = RecordingBackend()
    backend.queue_responses([_ok_response()])
    async with _build(policy, resolver, backend) as client:
        response = await client.get("https://api.test/")
    assert response.status_code == 200
    assert backend.connections == [("1.2.3.4", 443)]


async def test_ipv4_mapped_loopback_rejected() -> None:
    policy = EgressPolicy({"api.test"}, {443})
    resolver = FakeResolver({"api.test": ("::ffff:127.0.0.1",)})
    backend = RecordingBackend()
    async with _build(policy, resolver, backend) as client:
        with pytest.raises(EgressViolation):
            await client.get("https://api.test/")
    assert backend.attempts == []


async def test_sni_hostname_extension_is_rejected() -> None:
    # per-request sni_hostname 扩展会同时改写 SNI 与证书校验身份，必须拒绝。
    policy = EgressPolicy({"api.test"}, {443})
    resolver = FakeResolver({"api.test": ("1.2.3.4",)})
    backend = RecordingBackend()
    async with _build(policy, resolver, backend) as client:
        with pytest.raises(EgressViolation):
            await client.get(
                "https://api.test/", extensions={"sni_hostname": "attacker.tld"}
            )
    assert backend.attempts == []  # 未建立任何连接


async def test_unicode_host_resolves_via_punycode_identity() -> None:
    # unicode 白名单条目与 httpx 的 punycode 规范形一致：可解析、可钉扎。
    policy = EgressPolicy({"täst.de"}, {443})
    resolver = FakeResolver({"xn--tst-qla.de": ("1.2.3.4",)})
    backend = RecordingBackend()
    backend.queue_responses([_ok_response()])
    async with _build(policy, resolver, backend) as client:
        response = await client.get("https://täst.de/")
    assert response.status_code == 200
    assert backend.connections == [("1.2.3.4", 443)]
    assert backend.tls_hostnames == ["xn--tst-qla.de"]  # 报文身份为 IDNA 规范形


async def test_redirect_is_not_followed() -> None:
    policy = EgressPolicy({"api.test"}, {443})
    resolver = FakeResolver({"api.test": ("1.2.3.4",)})
    backend = RecordingBackend()
    backend.queue_responses(
        [
            _response(
                "HTTP/1.1 307 Temporary Redirect",
                [("Location", "https://other.test/next"), ("Content-Length", "0")],
            )
        ]
    )
    async with _build(policy, resolver, backend) as client:
        assert client.follow_redirects is False
        response = await client.get("https://api.test/a")
    assert response.status_code == 307
    assert response.headers["location"] == "https://other.test/next"
    assert len(backend.attempts) == 1  # 未对 other.test 发起任何连接
    assert resolver.calls == ["api.test"]


async def test_followed_redirect_to_explicitly_allowed_host_is_revalidated() -> None:
    policy = EgressPolicy({"api.test", "other.test"}, {443})
    resolver = FakeResolver({"api.test": ("1.2.3.4",), "other.test": ("4.5.6.7",)})
    backend = RecordingBackend()
    backend.queue_responses(
        [
            _response(
                "HTTP/1.1 307 Temporary Redirect",
                [("Location", "https://other.test/next"), ("Content-Length", "0")],
            ),
            _ok_response(b"redirected"),
        ]
    )
    async with _build(policy, resolver, backend) as client:
        response = await client.get("https://api.test/a", follow_redirects=True)
    assert response.status_code == 200
    assert response.content == b"redirected"
    assert resolver.calls == ["api.test", "other.test"]
    assert backend.connections == [("1.2.3.4", 443), ("4.5.6.7", 443)]


async def test_followed_redirect_target_is_revalidated() -> None:
    # 即使调用方按请求打开 follow_redirects，重定向目标也重新过策略。
    policy = EgressPolicy({"api.test"}, {443})
    resolver = FakeResolver({"api.test": ("1.2.3.4",)})
    backend = RecordingBackend()
    backend.queue_responses(
        [
            _response(
                "HTTP/1.1 307 Temporary Redirect",
                [("Location", "https://other.test/next"), ("Content-Length", "0")],
            )
        ]
    )
    async with _build(policy, resolver, backend) as client:
        with pytest.raises(EgressViolation):
            await client.get("https://api.test/a", follow_redirects=True)
    assert resolver.calls == ["api.test"]  # other.test 未解析
    assert len(backend.attempts) == 1  # other.test 未连接
    assert backend.connections == [("1.2.3.4", 443)]


async def test_connect_timeout_falls_back_to_next_approved_ip() -> None:
    # 连接超时与连接拒绝同样回退到下一个批准地址。
    policy = EgressPolicy({"api.test"}, {443})
    resolver = FakeResolver({"api.test": ("1.2.3.4", "4.5.6.7")})
    backend = RecordingBackend(timeout_addresses=frozenset({"1.2.3.4"}))
    backend.queue_responses([_ok_response()])
    async with _build(policy, resolver, backend) as client:
        response = await client.get("https://api.test/")
    assert response.status_code == 200
    assert backend.attempts == [("1.2.3.4", 443), ("4.5.6.7", 443)]
    assert backend.connections == [("4.5.6.7", 443)]


async def test_environment_proxy_is_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    proxy_ip = "198.51.100.9"  # TEST-NET：若被使用会被策略拒绝，便于暴露
    proxy_names = (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    )
    for name in proxy_names:
        monkeypatch.setenv(name, f"http://{proxy_ip}:9")
    monkeypatch.setenv("NO_PROXY", "")
    policy = EgressPolicy({"api.test"}, {443})
    resolver = FakeResolver({"api.test": ("1.2.3.4",)})
    backend = RecordingBackend()
    backend.queue_responses([_ok_response()])
    client = make_http_client(policy, resolver=resolver, network_backend=backend)
    async with client:
        response = await client.get("https://api.test/")
    assert response.status_code == 200
    assert backend.connections == [("1.2.3.4", 443)]
    connected_ips = [host for host, _port in backend.attempts]
    assert proxy_ip not in connected_ips


async def test_policy_error_does_not_leak_credentials() -> None:
    policy = EgressPolicy({"api.test"}, {443})
    resolver = FakeResolver({"api.test": ("1.2.3.4",)})
    backend = RecordingBackend()
    async with _build(policy, resolver, backend) as client:
        with pytest.raises(EgressViolation) as exc_info:
            await client.get(
                "https://evil.test/x",
                headers={"Authorization": "Bearer {{PASSWORD_hzuk3xhb}}-123"},
            )
    text = f"{exc_info.value} {repr(exc_info.value)}"
    assert "{{PASSWORD_hzuk3xhb}}-123" not in text
    assert "Bearer" not in text
    assert backend.attempts == []


async def test_pins_refresh_per_request() -> None:
    policy = EgressPolicy({"api.test"}, {443})
    table: dict[str, Sequence[str]] = {"api.test": ("1.2.3.4",)}
    resolver = FakeResolver(table)
    backend = RecordingBackend()

    def closed_ok() -> bytes:
        return _response("HTTP/1.1 200 OK", [("Content-Length", "0"), ("Connection", "close")])

    backend.queue_responses([closed_ok(), closed_ok()])
    async with _build(policy, resolver, backend) as client:
        assert (await client.get("https://api.test/")).status_code == 200
        table["api.test"] = ("4.5.6.7",)
        assert (await client.get("https://api.test/")).status_code == 200
    # 服务端 close 后连接不复用，第二个请求按当次解析结果连接新的批准 IP。
    assert backend.connections == [("1.2.3.4", 443), ("4.5.6.7", 443)]


async def test_dns_revocation_closes_idle_connection_before_reuse() -> None:
    policy = EgressPolicy({"api.test"}, {443})
    table: dict[str, Sequence[str]] = {"api.test": ("1.2.3.4",)}
    resolver = FakeResolver(table)
    backend = RecordingBackend()
    backend.queue_responses([_ok_response() + _ok_response(), _ok_response()])
    async with _build(policy, resolver, backend) as client:
        assert (await client.get("https://api.test/")).status_code == 200
        table["api.test"] = ("4.5.6.7",)
        assert (await client.get("https://api.test/")).status_code == 200
    assert backend.connections == [("1.2.3.4", 443), ("4.5.6.7", 443)]
    assert backend.streams[0].closed is True


async def test_same_host_reconnects_for_each_dns_snapshot() -> None:
    policy = EgressPolicy({"api.test"}, {443})
    resolver = FakeResolver({"api.test": ("1.2.3.4",)})
    backend = RecordingBackend()
    backend.queue_responses([_ok_response(), _ok_response()])
    async with _build(policy, resolver, backend) as client:
        assert (await client.get("https://api.test/")).status_code == 200
        assert (await client.get("https://api.test/")).status_code == 200
    assert backend.connections == [("1.2.3.4", 443), ("1.2.3.4", 443)]
    assert len(backend.streams) == 2


async def test_same_ip_different_hosts_do_not_share_connections() -> None:
    policy = EgressPolicy({"a.test", "b.test"}, {443})
    resolver = FakeResolver({"a.test": ("1.2.3.4",), "b.test": ("1.2.3.4",)})
    backend = RecordingBackend()
    backend.queue_responses([_ok_response(), _ok_response()])
    async with _build(policy, resolver, backend) as client:
        first = await client.get("https://a.test/")
        second = await client.get("https://b.test/")
    assert first.status_code == 200
    assert second.status_code == 200
    # 不同 host 即使同一 IP 也保持独立连接与独立 TLS 身份。
    assert backend.connections == [("1.2.3.4", 443), ("1.2.3.4", 443)]
    assert backend.tls_hostnames == ["a.test", "b.test"]
    request_one = bytes(backend.streams[0].written)
    request_two = bytes(backend.streams[1].written)
    assert b"Host: a.test\r\n" in request_one
    assert b"Host: b.test\r\n" in request_two


async def test_response_streams_body_and_close_is_preserved() -> None:
    policy = EgressPolicy({"api.test"}, {443})
    resolver = FakeResolver({"api.test": ("1.2.3.4",)})
    backend = RecordingBackend()
    body = b"x" * 8192
    payload = _response("HTTP/1.1 200 OK", [("Content-Length", str(len(body)))], body)
    backend.queue_responses([payload])
    async with _build(policy, resolver, backend) as client:
        async with client.stream("GET", "https://api.test/") as response:
            assert response.status_code == 200
            chunks = [chunk async for chunk in response.aiter_bytes()]
        assert b"".join(chunks) == body
        stream = backend.streams[0]
        # Connection reuse is disabled so the next request must use its fresh DNS pin.
        assert stream.closed is True
    assert backend.streams[0].closed is True


async def test_cleartext_http_dns_must_resolve_only_to_loopback() -> None:
    policy = EgressPolicy.authorize_loopback({"localhost"}, {8399})
    resolver = FakeResolver({"localhost": ("1.2.3.4",)})
    backend = RecordingBackend()
    async with make_http_client(policy, resolver=resolver, network_backend=backend) as client:
        with pytest.raises(EgressViolation):
            await client.get("http://localhost:8399/")
    assert backend.attempts == []


async def test_loopback_clearance_end_to_end() -> None:
    resolver = FakeResolver({"localhost": ("127.0.0.1", "::1")})
    backend = RecordingBackend()
    backend.queue_responses([_ok_response()])

    # 普通构造：https 到 loopback 也被地址层拒绝。
    normal = EgressPolicy({"localhost"}, {8399})
    async with make_http_client(normal, resolver=resolver, network_backend=backend) as client:
        with pytest.raises(EgressViolation):
            await client.get("https://localhost:8399/")

    # 受信构造：cleartext http 到本机服务可用。
    trusted = EgressPolicy.authorize_loopback({"localhost"}, {8399})
    async with make_http_client(trusted, resolver=resolver, network_backend=backend) as client:
        response = await client.get("http://localhost:8399/")
    assert response.status_code == 200
    assert backend.connections == [("127.0.0.1", 8399)]
    assert backend.tls_hostnames == []  # cleartext，无 TLS


async def test_ip_literal_host_skips_dns() -> None:
    policy = EgressPolicy({"1.2.3.4"}, {443})
    resolver = FakeResolver({})
    backend = RecordingBackend()
    backend.queue_responses([_ok_response()])
    async with _build(policy, resolver, backend) as client:
        response = await client.get("https://1.2.3.4/")
    assert response.status_code == 200
    assert backend.connections == [("1.2.3.4", 443)]
    assert resolver.calls == []  # IP 字面量直接审批，不经 DNS
    assert backend.tls_hostnames == ["1.2.3.4"]


async def test_system_resolver_resolves_localhost_locally() -> None:
    resolver = SystemResolver(timeout_seconds=2.0)
    addresses = await resolver.resolve("localhost")
    assert addresses


# --- 合成 TLS 端点：真实握手、真实证书校验，仅绑定本机环回 ---


def _generate_ca(common_name: str) -> tuple[ec.EllipticCurvePrivateKey, x509.Certificate]:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    now = datetime.now(UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    return key, cert


def _generate_leaf(
    common_name: str, dns_name: str, ca_key: ec.EllipticCurvePrivateKey, ca_cert: x509.Certificate
) -> tuple[bytes, bytes]:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    now = datetime.now(UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(dns_name)]), critical=False)
        .sign(ca_key, hashes.SHA256())
    )
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    return key_pem, cert.public_bytes(serialization.Encoding.PEM)


class _TLSServer:
    """临时合成 TLS 端点：真实 ssl 握手，记录收到的 SNI，仅应答一个 GET。"""

    def __init__(self, base: Path, key_pem: bytes, cert_pem: bytes) -> None:
        self.sni_names: list[str | None] = []
        cert_file = base / "server-cert.pem"
        key_file = base / "server-key.pem"
        cert_file.write_bytes(cert_pem)
        key_file.write_bytes(key_pem)
        self._context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self._context.load_cert_chain(str(cert_file), str(key_file))
        self._context.sni_callback = self._record_sni

    def _record_sni(self, _sslobj: object, server_name: str | None, _ctx: object) -> None:
        # asyncio 下 sni_callback 收到 (SSLObject, server_name, SSLContext)。
        self.sni_names.append(server_name)
        return None

    async def start(self, bind_host: str) -> tuple[asyncio.Server, int]:
        server = await asyncio.start_server(self._serve, bind_host, 0, ssl=self._context)
        port = server.sockets[0].getsockname()[1]
        return server, port

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                line = await reader.readline()
                if not line or line in (b"\r\n", b"\n"):
                    break
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            await writer.drain()
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()


def _cert_verification_failed(exc: BaseException) -> bool:
    # httpcore 可能把 ssl 错误包装为 ConnectError 且不带 __cause__，
    # 因此按类型或消息双重识别（两条路径都源于 OpenSSL 证书校验失败）。
    current: BaseException | None = exc
    while current is not None:
        if isinstance(current, ssl.SSLCertVerificationError):
            return True
        if "certificate verify failed" in str(current).lower():
            return True
        current = current.__cause__
    return False


@pytest.mark.skipif(not _CERT_TOOLING, reason="cryptography unavailable for test certificates")
@pytest.mark.parametrize("bind_host", ["127.0.0.1", "::1"])
async def test_real_tls_handshake_keeps_original_host_sni_and_verifies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bind_host: str
) -> None:
    # 真实 TCP + 真实 TLS：自签 CA 只在本用例注入客户端信任；
    # 连接地址仍是策略批准的环回 IP，SNI 与证书校验都应保持原 host。
    ca_key, ca_cert = _generate_ca("krim-b3-test-ca")
    key_pem, cert_pem = _generate_leaf("api.test", "api.test", ca_key, ca_cert)
    ca_pem = ca_cert.public_bytes(serialization.Encoding.PEM).decode("ascii")
    endpoint = _TLSServer(tmp_path, key_pem, cert_pem)
    server, port = await endpoint.start(bind_host)
    try:
        policy = EgressPolicy.authorize_loopback({"api.test"}, {port})
        resolver = FakeResolver({"api.test": (bind_host,)})

        def _trusted_context(
            verify: object = True, cert: object = None, trust_env: bool = True
        ) -> ssl.SSLContext:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.load_verify_locations(cadata=ca_pem)
            return context

        monkeypatch.setattr(http_transport_module, "create_ssl_context", _trusted_context)
        transport = PinnedAsyncTransport(policy, resolver=resolver)  # 真实 AutoBackend
        async with httpx.AsyncClient(
            transport=transport, timeout=5.0, trust_env=False, follow_redirects=False
        ) as client:
            response = await client.get(f"https://api.test:{port}/")
    finally:
        server.close()
        await server.wait_closed()

    assert response.status_code == 200
    assert response.text == "ok"
    assert endpoint.sni_names == ["api.test"]  # SNI 为原 host，而非钉扎 IP


@pytest.mark.skipif(not _CERT_TOOLING, reason="cryptography unavailable for test certificates")
async def test_real_tls_rejects_certificate_from_untrusted_ca(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    trusted_key, trusted_cert = _generate_ca("krim-b3-trusted-ca")
    rogue_key, _rogue_cert = _generate_ca("krim-b3-rogue-ca")
    key_pem, cert_pem = _generate_leaf("api.test", "api.test", rogue_key, _rogue_cert)
    ca_pem = trusted_cert.public_bytes(serialization.Encoding.PEM).decode("ascii")
    endpoint = _TLSServer(tmp_path, key_pem, cert_pem)
    server, port = await endpoint.start("127.0.0.1")
    try:
        policy = EgressPolicy.authorize_loopback({"api.test"}, {port})
        resolver = FakeResolver({"api.test": ("127.0.0.1",)})

        def _trusted_context(
            verify: object = True, cert: object = None, trust_env: bool = True
        ) -> ssl.SSLContext:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.load_verify_locations(cadata=ca_pem)
            return context

        monkeypatch.setattr(http_transport_module, "create_ssl_context", _trusted_context)
        transport = PinnedAsyncTransport(policy, resolver=resolver)
        async with httpx.AsyncClient(
            transport=transport, timeout=5.0, trust_env=False, follow_redirects=False
        ) as client:
            with pytest.raises(Exception) as exc_info:  # noqa: B017 -- 断言链上有证书校验错误
                await client.get(f"https://api.test:{port}/")
    finally:
        server.close()
        await server.wait_closed()

    assert _cert_verification_failed(exc_info.value)
    assert endpoint.sni_names == ["api.test"]  # 握手已发生，但证书校验拒绝


@pytest.mark.skipif(not _CERT_TOOLING, reason="cryptography unavailable for test certificates")
async def test_real_tls_rejects_hostname_mismatch_even_with_trusted_ca(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # CA 受信任，但证书 SAN 是 other.test；校验必须针对原 host 而非钉扎 IP。
    ca_key, ca_cert = _generate_ca("krim-b3-test-ca")
    key_pem, cert_pem = _generate_leaf("other.test", "other.test", ca_key, ca_cert)
    ca_pem = ca_cert.public_bytes(serialization.Encoding.PEM).decode("ascii")
    endpoint = _TLSServer(tmp_path, key_pem, cert_pem)
    server, port = await endpoint.start("127.0.0.1")
    try:
        policy = EgressPolicy.authorize_loopback({"api.test"}, {port})
        resolver = FakeResolver({"api.test": ("127.0.0.1",)})

        def _trusted_context(
            verify: object = True, cert: object = None, trust_env: bool = True
        ) -> ssl.SSLContext:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.load_verify_locations(cadata=ca_pem)
            return context

        monkeypatch.setattr(http_transport_module, "create_ssl_context", _trusted_context)
        transport = PinnedAsyncTransport(policy, resolver=resolver)
        async with httpx.AsyncClient(
            transport=transport, timeout=5.0, trust_env=False, follow_redirects=False
        ) as client:
            with pytest.raises(Exception) as exc_info:  # noqa: B017 -- 断言链上有证书校验错误
                await client.get(f"https://api.test:{port}/")
    finally:
        server.close()
        await server.wait_closed()

    assert _cert_verification_failed(exc_info.value)
    assert endpoint.sni_names == ["api.test"]
