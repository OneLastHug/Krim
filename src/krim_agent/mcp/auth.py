"""MCP 固定资源的 HTTPX 异步 OAuth Auth；使用显式宿主回调，见 ADR-0017。"""

from __future__ import annotations

import asyncio
import json
import math
import re
import secrets
from collections.abc import AsyncGenerator, Awaitable, Callable
from urllib.parse import urlsplit

import httpx
from pydantic import ConfigDict, Field

from krim_agent.protocol.models import StrictModel

from .client_store import OAuthClientBinding, OAuthClientRegistrationStore
from .discovery import OAuthDiscoveryClient, RegisteredClient
from .oauth import (
    OAuthAuthorizationStateStore,
    OAuthClient,
    OAuthConfig,
    OAuthError,
    OAuthHTTPRequester,
    OAuthStateStore,
    OAuthToken,
    OAuthTokenStore,
    TokenSnapshot,
    validate_oauth_endpoint,
)


class MCPAuthError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class ProtectedResourceMetadata(StrictModel):
    model_config = ConfigDict(extra="ignore", hide_input_in_errors=True)
    resource: str = Field(min_length=1, max_length=2048)
    authorization_servers: list[str] = Field(min_length=1, max_length=16)
    scopes_supported: list[str] | None = Field(default=None, max_length=64)
    bearer_methods_supported: list[str] = Field(default_factory=lambda: ["header"], max_length=16)


RedirectHandler = Callable[[str], Awaitable[None]]
CallbackHandler = Callable[[], Awaitable[tuple[str, str | None]]]


class MCPOAuthAuth(httpx.Auth):
    """对一个固定 MCP endpoint 认证；正常 SSE 响应始终由调用方流式消费。"""

    requires_response_body = False

    def __init__(
        self,
        resource_url: str,
        *,
        requester: OAuthHTTPRequester,
        tokens: OAuthTokenStore,
        subject: str,
        server: str,
        redirect_uri: str | None = None,
        redirect_handler: RedirectHandler | None = None,
        callback_handler: CallbackHandler | None = None,
        config: OAuthConfig | None = None,
        scopes: list[str] | None = None,
        issuer: str | None = None,
        registered_client: RegisteredClient | None = None,
        states: OAuthStateStore | OAuthAuthorizationStateStore | None = None,
        clients: OAuthClientRegistrationStore | None = None,
        allow_dynamic_registration: bool = False,
        client_name: str = "Krim Agent",
        timeout_seconds: float = 60,
        request_timeout_seconds: float = 10,
        max_request_bytes: int = 262144,
    ) -> None:
        try:
            host = urlsplit(resource_url).hostname
        except ValueError as error:
            raise ValueError("MCP OAuth resource URL 无效") from error
        validate_oauth_endpoint(resource_url, {host} if host else set(), allow_query=True)
        if (
            not subject
            or len(subject) > 256
            or re.fullmatch(r"[a-z][a-z0-9_-]{0,31}", server) is None
        ):
            raise ValueError("MCP OAuth server/subject 无效")
        if (
            not math.isfinite(timeout_seconds)
            or not 0 < timeout_seconds <= 300
            or not math.isfinite(request_timeout_seconds)
            or not 0 < request_timeout_seconds <= 120
            or not 1024 <= max_request_bytes <= 1_048_576
        ):
            raise ValueError("MCP OAuth 时间/请求预算无效")
        self.resource_url = str(httpx.URL(resource_url))
        self.requester, self.tokens = requester, tokens
        self.subject, self.server = subject, server
        self.redirect_handler, self.callback_handler = redirect_handler, callback_handler
        self.redirect_uri = redirect_uri or (config.redirect_uri if config else None)
        self.issuer = issuer or (config.issuer if config else None)
        self.scopes = list(scopes if scopes is not None else (config.scopes if config else []))
        self.registered_client = (
            registered_client.model_copy(deep=True) if registered_client else None
        )
        self.allow_dynamic_registration = allow_dynamic_registration
        self.client_name = client_name
        self.timeout_seconds = timeout_seconds
        self.request_timeout_seconds = request_timeout_seconds
        self.max_request_bytes = max_request_bytes
        # 默认保留旧的单进程状态；宿主需要跨进程回调时显式注入持久状态后端。
        self.states = states or OAuthStateStore()
        self.client = OAuthClient(self.states, tokens)
        self._lock = asyncio.Lock()
        self.last_error_code: str | None = None
        self._config = config.model_copy(deep=True) if config else None
        self._declared_config = config.model_copy(deep=True) if config else None
        self.clients = clients
        self._client_binding = (
            OAuthClientBinding(
                server=server,
                subject=subject,
                resource=self.resource_url,
                issuer=self.issuer,
                redirect_uri=self.redirect_uri,
                scopes=self.scopes,
                allowed_hosts=set(requester.allowed_hosts),
                client_name=client_name,
            )
            if config is None
            and registered_client is None
            and allow_dynamic_registration
            and self.redirect_uri is not None
            else None
        )
        if self._config is not None:
            if self._config.server != server:
                raise ValueError("MCP OAuth 配置 server 不一致")
            self._config.resource = self._config.resource or self.resource_url
            if not self._resource_matches(self._config.resource):
                raise ValueError("MCP OAuth 配置 resource 不一致")
            self._config.validate_endpoints()
            self._declared_config = self._config.model_copy(deep=True)

    @property
    def config(self) -> OAuthConfig | None:
        return self._config.model_copy(deep=True) if self._config else None

    @property
    def declared_config(self) -> OAuthConfig | None:
        return self._declared_config.model_copy(deep=True) if self._declared_config else None

    async def authorization_config(self) -> OAuthConfig:
        """可信宿主/浏览器发起授权前，复用声明或已验证DCR配置。"""
        if self._config is None and self.clients is not None and self._client_binding is not None:
            cached = await self.clients.read(self._client_binding)
            if cached.config is not None:
                self._config = cached.config.model_copy(deep=True)
        if self._config is None:
            await self._discover("")
        if self._config is None:
            raise MCPAuthError("oauth_client_required", "OAuth未形成授权配置")
        return self._config.model_copy(deep=True)

    def matches_endpoint(self, url: str) -> bool:
        expected, actual = httpx.URL(self.resource_url), httpx.URL(url)
        return (
            (expected.scheme, expected.host, expected.port, expected.query)
            == (actual.scheme, actual.host, actual.port, actual.query)
            and expected.path.rstrip("/") == actual.path.rstrip("/")
            and not actual.userinfo
            and not actual.fragment
        )

    def _resource_matches(self, resource: str) -> bool:
        try:
            host = urlsplit(resource).hostname
            validate_oauth_endpoint(resource, {host} if host else set(), allow_query=True)
            expected, actual = httpx.URL(self.resource_url), httpx.URL(resource)
        except (ValueError, OAuthError):
            return False
        return (expected.scheme, expected.host, expected.port) == (
            actual.scheme,
            actual.host,
            actual.port,
        ) and (expected.path.rstrip("/") + "/").startswith(actual.path.rstrip("/") + "/")

    @staticmethod
    def _challenge(response: httpx.Response) -> str | None:
        for header in response.headers.get_list("www-authenticate"):
            if len(header) > 8192:
                raise MCPAuthError("invalid_oauth_challenge", "OAuth challenge 超过大小限制")
            if re.match(r"^\s*Bearer(?:\s|$)", header, re.IGNORECASE):
                return header
        return None

    @staticmethod
    def _parameter(header: str, field: str) -> str | None:
        matched = re.search(
            rf'(?:^|[,\s]){field}\s*=\s*(?:"([^"]+)"|([^\s,]+))', header, re.IGNORECASE
        )
        return (matched.group(1) or matched.group(2)) if matched else None

    async def _prepare_body(self, request: httpx.Request) -> bytes:
        try:
            content = request.content
        except httpx.RequestNotRead:
            original = request.stream
            if not isinstance(original, httpx.AsyncByteStream):
                raise MCPAuthError("oauth_request_body", "OAuth 请求正文不能异步重放") from None
            buffer = bytearray()
            try:
                async for chunk in original:
                    buffer.extend(chunk)
                    if len(buffer) > self.max_request_bytes:
                        raise MCPAuthError("oauth_request_limit", "OAuth 请求正文超过限制")
            finally:
                await original.aclose()
            request.stream = httpx.ByteStream(bytes(buffer))
            content = await request.aread()
        if len(content) > self.max_request_bytes:
            raise MCPAuthError("oauth_request_limit", "OAuth 请求正文超过限制")
        return content

    @staticmethod
    def _interactive(request: httpx.Request, content: bytes) -> bool:
        if request.method == "GET" and "mcp-session-id" not in request.headers:
            return True
        try:
            message = json.loads(content)
        except (ValueError, RecursionError):
            return False
        return isinstance(message, dict) and message.get("method") == "initialize"

    @staticmethod
    async def _discard_unauthorized_body(response: httpx.Response) -> None:
        # 关闭原网络流，再让 HTTPX 的 auth 重试读取一个空正文；保留 challenge。
        original = response.stream
        if isinstance(original, httpx.AsyncByteStream):
            await original.aclose()
        response.stream = httpx.ByteStream(b"")
        response.headers.pop("content-encoding", None)
        response.headers["content-length"] = "0"
        await response.aread()

    @staticmethod
    def _attach(request: httpx.Request, token: OAuthToken) -> None:
        value = token.access_token.get_secret_value()
        if not value or re.fullmatch(r"[A-Za-z0-9._~+/=-]+", value) is None:
            raise MCPAuthError("invalid_access_token", "OAuth access token 不能作为 Bearer header")
        request.headers["Authorization"] = f"Bearer {value}"

    async def _usable(self) -> TokenSnapshot:
        if self._config is None and self.clients is not None and self._client_binding is not None:
            cached = await self.clients.read(self._client_binding)
            if cached.config is not None:
                self._config = cached.config.model_copy(deep=True)
        if self._config is None:
            return TokenSnapshot(None, 0)
        current = await self.tokens.read(self._config, self.subject)
        if current.token is not None and current.token.expired():
            if current.token.refresh_token is None:
                return TokenSnapshot(None, current.revision)
            await self.client.access_token(self._config, self.subject, self.requester)
            current = await self.tokens.read(self._config, self.subject)
        return current

    async def _discover(self, header: str) -> None:
        if self.clients is not None and self._client_binding is not None:
            async with self.clients.lock(self._client_binding):
                cached = await self.clients.read(self._client_binding)
                if cached.config is not None:
                    self._config = cached.config.model_copy(deep=True)
                    return
                try:
                    await self._discover_remote(header)
                    if self._config is None:
                        raise MCPAuthError("oauth_client_required", "DCR未形成有效配置")
                    await self.clients.write(
                        self._client_binding, self._config, expected_revision=cached.revision
                    )
                except BaseException:
                    self._config = None
                    raise
            return
        await self._discover_remote(header)

    async def _discover_remote(self, header: str) -> None:
        discovery = OAuthDiscoveryClient(self.requester)
        advertised = self._parameter(header, "resource_metadata")
        parsed = urlsplit(self.resource_url)
        if advertised:
            endpoints = [advertised]
        else:
            origin = f"{parsed.scheme}://{parsed.netloc}"
            endpoints = [
                origin + "/.well-known/oauth-protected-resource" + parsed.path.rstrip("/"),
                origin + "/.well-known/oauth-protected-resource",
            ]
        resource: ProtectedResourceMetadata | None = None
        for endpoint in dict.fromkeys(endpoints):
            response = await self.requester.request_json("GET", endpoint)
            if response.status_code == 404:
                continue
            try:
                resource = ProtectedResourceMetadata.model_validate(discovery._json(response))
            except ValueError:
                raise MCPAuthError(
                    "invalid_resource_metadata", "Protected Resource Metadata 字段无效"
                ) from None
            break
        if resource is None:
            raise MCPAuthError(
                "resource_metadata_unavailable", "MCP 没有 Protected Resource Metadata"
            )
        if (
            not self._resource_matches(resource.resource)
            or "header" not in resource.bearer_methods_supported
        ):
            raise MCPAuthError("resource_mismatch", "OAuth resource 与固定 MCP endpoint 不匹配")
        issuer = self.issuer or resource.authorization_servers[0]
        if self.issuer is not None and self.issuer not in resource.authorization_servers:
            raise MCPAuthError("oauth_issuer_mismatch", "MCP 未声明选定 OAuth issuer")
        metadata = await discovery.discover(issuer)
        required_scopes = (self._parameter(header, "scope") or "").split()
        if not set(required_scopes).issubset(self.scopes):
            raise MCPAuthError("oauth_scope_required", "MCP 需要宿主明确配置所需 scope")
        if self.redirect_uri is None:
            raise MCPAuthError("oauth_interaction_required", "OAuth 登录需要固定 redirect_uri")
        registered = self.registered_client
        if registered is None:
            if not self.allow_dynamic_registration:
                raise MCPAuthError("oauth_client_required", "OAuth 需要显式客户端或允许动态注册")
            registered = await discovery.register(
                metadata,
                redirect_uri=self.redirect_uri,
                scopes=self.scopes,
                client_name=self.client_name,
            )
        configured = discovery.configuration(
            metadata, registered, server=self.server, scopes=self.scopes
        )
        if configured.redirect_uri != self.redirect_uri:
            raise MCPAuthError("invalid_redirect_uri", "OAuth 客户端回调与固定 redirect_uri 不一致")
        configured.resource = resource.resource
        configured.validate_endpoints()
        self._config = configured

    async def _authorize(self, header: str) -> OAuthToken:
        if self.redirect_handler is None or self.callback_handler is None:
            raise MCPAuthError("oauth_interaction_required", "MCP OAuth 需要宿主登录回调")
        if self._config is None:
            await self._discover(header)
        if self._config is None:
            raise MCPAuthError("oauth_client_required", "OAuth 未形成客户端配置")
        attempt = await self.client.begin_authorization(self._config, self.subject)
        try:
            try:
                await self.redirect_handler(attempt.authorization_url)
                reply = await self.callback_handler()
            except asyncio.CancelledError:
                raise
            except Exception:
                raise MCPAuthError("oauth_callback_failed", "宿主 OAuth 回调失败") from None
            if not isinstance(reply, tuple) or len(reply) != 2:
                raise MCPAuthError("invalid_authorization_response", "OAuth callback 响应无效")
            code, state = reply
            if not isinstance(state, str) or not secrets.compare_digest(state, attempt.state):
                raise MCPAuthError("oauth_state_mismatch", "OAuth state 不匹配")
            if not isinstance(code, str) or not code or len(code) > 8192:
                raise MCPAuthError("invalid_authorization_response", "OAuth callback 缺少有效 code")
            return await self.client.exchange(
                attempt.state,
                code,
                self.subject,
                self.requester,
                expected_config=self._config,
            )
        finally:
            await self.client.discard_authorization(attempt.state, self.subject)

    async def async_auth_flow(
        self, request: httpx.Request
    ) -> AsyncGenerator[httpx.Request, httpx.Response]:
        self.last_error_code = None
        try:
            if not self.matches_endpoint(str(request.url)):
                raise MCPAuthError("oauth_resource_mismatch", "Auth 只能发送到固定 MCP endpoint")
            content = await self._prepare_body(request)
            interactive = self._interactive(request, content)
            budget = self.timeout_seconds if interactive else self.request_timeout_seconds
            async with asyncio.timeout(budget):
                current = await self._usable()
                sent_revision = current.revision
                if current.token is not None:
                    self._attach(request, current.token)
                response = yield request
                if response.status_code != 401:
                    return
                header = self._challenge(response)
                await self._discard_unauthorized_body(response)
                if header is None:
                    raise MCPAuthError(
                        "oauth_challenge_required", "MCP 401 未声明 Bearer challenge"
                    )
                async with self._lock:
                    current = await self._usable()
                    token = current.token if current.revision != sent_revision else None
                    if (
                        token is None
                        and current.token is not None
                        and current.token.refresh_token is not None
                    ):
                        if self._config is None:
                            raise MCPAuthError("oauth_client_required", "OAuth 缺少刷新配置")
                        try:
                            token = await self.client.refresh(
                                self._config, self.subject, self.requester
                            )
                        except OAuthError as error:
                            if error.code not in {"refresh_unavailable", "token_exchange_failed"}:
                                raise
                    if token is None:
                        if not interactive:
                            raise MCPAuthError(
                                "oauth_interaction_required", "MCP 工具请求需要重新登录后重试"
                            )
                        token = await self._authorize(header)
                self._attach(request, token)
                # aread 已把请求流转换为可重放的有限正文，重试保持原 RPC 内容。
                retried = yield request
                if retried.status_code == 401:
                    await self._discard_unauthorized_body(retried)
                    raise MCPAuthError("oauth_unauthorized", "OAuth 登录后 MCP 仍拒绝凭据")
        except TimeoutError:
            self.last_error_code = "oauth_timeout"
            raise MCPAuthError("oauth_timeout", "MCP OAuth 请求或登录超过时间预算") from None
        except OAuthError as error:
            self.last_error_code = error.code
            raise MCPAuthError(error.code, str(error)) from None
        except MCPAuthError as error:
            self.last_error_code = error.code
            raise
