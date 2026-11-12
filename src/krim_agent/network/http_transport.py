"""受 :class:`EgressPolicy` 约束的 httpx 异步传输：实际 IP 钉扎。

关键安全性质：
- 连接层实际使用策略批准的 IP：DNS 结果先经策略审批，再经自定义
  httpcore network backend 以该 IP 发起 ``connect_tcp``；解析结果按请求刷新钉扎。
- TLS 的 SNI 与证书校验、HTTP ``Host`` 头保持原 host（由 httpcore 连接层按
  origin host 处理，本模块不改写 URL、不关闭验证），并把 URL host 统一为
  IDNA 规范形；per-request ``sni_hostname`` 扩展会改写 TLS 身份，被显式拒绝，
  绝不把 URL 改写成 IP 后降级证书校验。
- 默认仅 HTTPS、不读取环境代理（``trust_env=False``，连接池也未配置代理）、
  不跟随重定向（``follow_redirects=False``，即使调用方打开，重定向目标也会
  重新过策略）。
- 连接复用键是 httpcore 连接池的 origin（scheme/host/port）；每个 policy
  对应独立的 client 与连接池，不同 host 即使解析到同一 IP 也不会共享连接。
- 错误消息只含 host/地址与原因，不含请求头或凭据。

复用的既有安装 API（httpx 0.28.1 / httpcore 1.0.9）：TLS 仍由 httpcore
连接层完成（``stream.start_tls(server_hostname=原host)``），本模块只提供
socket 层钉扎；httpx 私有稳定件 ``map_httpcore_exceptions`` /
``AsyncResponseStream`` 与默认传输用法一致，保证超时/错误映射和响应流
backpressure/close 行为不变。
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from collections.abc import AsyncIterable, Iterable, Sequence
from typing import Protocol

import httpcore
import httpx
from httpcore._backends.auto import AutoBackend
from httpx._config import create_ssl_context
from httpx._transports.default import AsyncResponseStream, map_httpcore_exceptions

from krim_agent.network.egress_policy import EgressPolicy, EgressTarget, EgressViolation

__all__ = [
    "AddressResolver",
    "PinnedAsyncTransport",
    "PinnedIPBackend",
    "SystemResolver",
    "make_http_client",
]


class AddressResolver(Protocol):
    """把 host 解析为全部 A/AAAA 地址；实现需自行限时并保证不触网即可注入。"""

    async def resolve(self, host: str) -> Sequence[str]: ...


class SystemResolver:
    """默认解析器：``getaddrinfo(AF_UNSPEC)`` 取全部 A/AAAA，带 DNS 超时。"""

    def __init__(self, timeout_seconds: float = 5.0) -> None:
        self._timeout_seconds = timeout_seconds

    async def resolve(self, host: str) -> Sequence[str]:
        loop = asyncio.get_running_loop()
        try:
            infos = await asyncio.wait_for(
                loop.getaddrinfo(host, None, family=socket.AF_UNSPEC, type=socket.SOCK_STREAM),
                self._timeout_seconds,
            )
        except TimeoutError:
            raise EgressViolation("dns resolution timed out") from None
        except OSError:
            raise EgressViolation("dns resolution failed") from None
        addresses: list[str] = []
        for family, _type, _proto, _canonname, sockaddr in infos:
            if family not in (socket.AF_INET, socket.AF_INET6):
                continue
            address = str(sockaddr[0])
            if address not in addresses:
                addresses.append(address)
        return tuple(addresses)


class PinnedIPBackend(httpcore.AsyncNetworkBackend):
    """connect_tcp 钉扎层：只允许连接到策略批准的实际 IP。

    真实 socket 由 httpcore 现成 backend 完成（默认 ``AutoBackend``），
    TLS 握手与证书校验仍由 httpcore 连接层用原 host 进行，本层不重写 TLS。
    """

    def __init__(self, inner: httpcore.AsyncNetworkBackend | None = None) -> None:
        self._inner = inner if inner is not None else AutoBackend()
        self._pins: dict[str, tuple[str, ...]] = {}

    def pin(self, host: str, addresses: Sequence[str]) -> None:
        self._pins[host] = tuple(addresses)

    async def connect_tcp(  # noqa: ASYNC109 -- httpcore 接口签名，参数名不可改
        self,
        host: str,
        port: int,
        timeout: float | None = None,  # noqa: ASYNC109 -- httpcore 接口签名，参数名不可改
        local_address: str | None = None,
        socket_options: Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        addresses = self._pins.get(host)
        if not addresses:
            raise EgressViolation("connection target is not pinned by policy")
        last_error: Exception | None = None
        for address in addresses:
            try:
                return await self._inner.connect_tcp(
                    address,
                    port,
                    timeout=timeout,
                    local_address=local_address,
                    socket_options=socket_options,
                )
            except (httpcore.ConnectError, OSError, TimeoutError) as error:
                # 连接失败（含超时）都回退到下一个批准地址。
                last_error = error
        assert last_error is not None
        raise last_error

    async def connect_unix_socket(  # noqa: ASYNC109 -- httpcore 接口签名，参数名不可改
        self,
        path: str | bytes,
        timeout: float | None = None,  # noqa: ASYNC109 -- httpcore 接口签名，参数名不可改
        socket_options: Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        raise EgressViolation("unix socket egress is not allowed")


class PinnedAsyncTransport(httpx.AsyncBaseTransport):
    """把 httpx 请求约束在 :class:`EgressPolicy` 白名单内的传输实现。"""

    def __init__(
        self,
        policy: EgressPolicy,
        *,
        resolver: AddressResolver | None = None,
        network_backend: httpcore.AsyncNetworkBackend | None = None,
        limits: httpx.Limits | None = None,
        http1: bool = True,
        http2: bool = False,
    ) -> None:
        self._policy = policy
        self._resolver = resolver if resolver is not None else SystemResolver()
        self._backend = PinnedIPBackend(network_backend)
        effective_limits = limits if limits is not None else httpx.Limits()
        self._pool = httpcore.AsyncConnectionPool(
            ssl_context=create_ssl_context(verify=True, trust_env=False),
            max_connections=effective_limits.max_connections,
            max_keepalive_connections=effective_limits.max_keepalive_connections,
            keepalive_expiry=effective_limits.keepalive_expiry,
            http1=http1,
            http2=http2,
            network_backend=self._backend,
        )

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        assert isinstance(request.stream, httpx.AsyncByteStream)
        target = self._policy.validate_target(str(request.url))
        # TLS 身份（SNI 与证书校验）钉扎在已校验的 origin host 上：
        # httpcore 会读取 per-request sni_hostname 扩展同时改写两者，必须拒绝。
        if "sni_hostname" in request.extensions:
            raise EgressViolation("request extension sni_hostname is not allowed")
        addresses = await self._resolve_addresses(target)
        approved = self._policy.approve_addresses(addresses)
        if target.scheme == "http" and not all(
            ipaddress.ip_address(address).is_loopback for address in approved
        ):
            raise EgressViolation("cleartext http requires loopback addresses")
        for key in self._pin_keys(request.url, target):
            self._backend.pin(key, approved)
        core_request = httpcore.Request(
            method=request.method,
            url=httpcore.URL(
                scheme=request.url.raw_scheme,
                host=request.url.raw_host,
                port=request.url.port,
                target=request.url.raw_path,
            ),
            headers=request.headers.raw,
            content=request.stream,
            extensions=request.extensions,
        )
        with map_httpcore_exceptions():
            response = await self._pool.handle_async_request(core_request)
        assert isinstance(response.stream, AsyncIterable)
        return httpx.Response(
            status_code=response.status,
            headers=response.headers,
            stream=AsyncResponseStream(response.stream),
            extensions=response.extensions,
        )

    async def aclose(self) -> None:
        await self._pool.aclose()

    async def _resolve_addresses(self, target: EgressTarget) -> Sequence[str]:
        try:
            literal = ipaddress.ip_address(target.connect_host)
        except ValueError:
            return await self._resolver.resolve(target.connect_host)
        # IP 字面量 host：跳过 DNS，直接交给策略审批。
        return (str(literal),)

    def _pin_keys(self, url: httpx.URL, target: EgressTarget) -> tuple[str, ...]:
        # httpcore 连接层使用 raw_host（IDNA 形式）发起 connect_tcp；
        # 同时登记 unicode host，保证两种形式都能命中钉扎表。
        keys = {target.connect_host, url.host}
        raw_host = url.raw_host
        if raw_host is not None:
            keys.add(raw_host.decode("ascii"))
        return tuple(sorted(keys))


def make_http_client(
    policy: EgressPolicy,
    *,
    resolver: AddressResolver | None = None,
    timeout: float | httpx.Timeout = 10.0,
    headers: dict[str, str] | None = None,
    limits: httpx.Limits | None = None,
    dns_timeout_seconds: float = 5.0,
    network_backend: httpcore.AsyncNetworkBackend | None = None,
) -> httpx.AsyncClient:
    """构造受 policy 约束的 ``httpx.AsyncClient``。

    固定为 ``trust_env=False``（环境代理不接管）与 ``follow_redirects=False``，
    可直接替换 ``mcp.client`` HTTP 分支手工构造的 AsyncClient。
    """
    transport = PinnedAsyncTransport(
        policy,
        resolver=resolver if resolver is not None else SystemResolver(dns_timeout_seconds),
        network_backend=network_backend,
        limits=limits,
    )
    return httpx.AsyncClient(
        transport=transport,
        timeout=timeout,
        headers=headers,
        trust_env=False,
        follow_redirects=False,
    )
