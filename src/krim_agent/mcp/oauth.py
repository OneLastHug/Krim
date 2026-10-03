"""OAuth PKCE/令牌基础；不自动连接真实身份平台。"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import math
import secrets
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field
from types import TracebackType
from typing import Protocol
from urllib.parse import urlencode, urlsplit

import httpx
from cryptography.fernet import Fernet, InvalidToken
from pydantic import Field, SecretStr

from krim_agent.protocol.models import StrictModel


class OAuthError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def validate_oauth_endpoint(
    url: str, allowed_hosts: set[str], *, allow_query: bool = False
) -> None:
    """校验授权、metadata 和 token 请求共用的 endpoint 边界。"""

    if len(url) > 2048 or any(character.isspace() or ord(character) < 32 for character in url):
        raise OAuthError("invalid_oauth_endpoint", "OAuth endpoint 不能包含空白或控制字符")
    try:
        parsed = urlsplit(url)
        _ = parsed.port
    except ValueError as error:
        raise OAuthError("invalid_oauth_endpoint", "OAuth endpoint URL 格式无效") from error
    host = parsed.hostname.lower().rstrip(".") if parsed.hostname else ""
    allowed = {value.lower().rstrip(".") for value in allowed_hosts}
    if parsed.scheme not in {"http", "https"} or not host:
        raise OAuthError("invalid_oauth_endpoint", "OAuth endpoint 必须包含 http/https 主机")
    if (
        parsed.username is not None
        or parsed.password is not None
        or "#" in url
        or ("?" in url and not allow_query)
    ):
        raise OAuthError(
            "invalid_oauth_endpoint", "OAuth endpoint 不能包含凭证、查询参数或 fragment"
        )
    if host not in allowed:
        raise OAuthError("oauth_host_denied", "OAuth endpoint 不在主机白名单")
    if parsed.scheme == "http" and host not in {"127.0.0.1", "localhost", "::1"}:
        raise OAuthError("insecure_oauth_endpoint", "非本机 OAuth endpoint 必须使用 HTTPS")


class OAuthConfig(StrictModel):
    server: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,31}$")
    client_id: str = Field(min_length=1, max_length=256)
    issuer: str | None = Field(default=None, min_length=1, max_length=2048)
    resource: str | None = Field(default=None, min_length=1, max_length=2048)
    client_secret: SecretStr | None = None
    authorization_endpoint: str
    token_endpoint: str
    redirect_uri: str
    scopes: list[str] = Field(default_factory=list, max_length=64)
    allowed_hosts: set[str] = Field(default_factory=set)
    state_ttl_seconds: int = Field(default=600, ge=30, le=3600)

    def validate_endpoints(self) -> None:
        try:
            redirect = urlsplit(self.redirect_uri)
            _ = redirect.port
        except ValueError as error:
            raise OAuthError("invalid_redirect_uri", "redirect_uri URL 格式无效") from error
        if redirect.scheme not in {"http", "https"} or not redirect.hostname:
            raise OAuthError("invalid_redirect_uri", "redirect_uri 必须是 http/https URL")
        if (
            redirect.username is not None
            or redirect.password is not None
            or "#" in self.redirect_uri
            or len(self.redirect_uri) > 2048
            or any(character.isspace() for character in self.redirect_uri)
        ):
            raise OAuthError("invalid_redirect_uri", "redirect_uri 不能包含凭证或 fragment")
        if redirect.scheme == "http" and redirect.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise OAuthError("invalid_redirect_uri", "非本机 redirect_uri 必须使用 HTTPS")
        endpoints = [self.authorization_endpoint, self.token_endpoint]
        if self.issuer is not None:
            endpoints.append(self.issuer)
        for value in endpoints:
            try:
                parsed = urlsplit(value)
            except ValueError as error:
                raise OAuthError("invalid_oauth_endpoint", "OAuth endpoint URL 格式无效") from error
            allowed = self.allowed_hosts or ({parsed.hostname} if parsed.hostname else set())
            validate_oauth_endpoint(value, allowed)
        if self.resource is not None:
            try:
                host = urlsplit(self.resource).hostname
            except ValueError as error:
                raise OAuthError("invalid_oauth_resource", "OAuth resource URL 无效") from error
            validate_oauth_endpoint(self.resource, {host} if host else set(), allow_query=True)


class OAuthToken(StrictModel):
    access_token: SecretStr
    token_type: str = "Bearer"
    refresh_token: SecretStr | None = None
    expires_at: float | None = None
    scope: str | None = None

    def expired(self, now: float | None = None) -> bool:
        return self.expires_at is not None and self.expires_at <= (
            time.time() if now is None else now
        )


@dataclass(frozen=True)
class PKCERequest:
    state: str
    code_verifier: str = field(repr=False)
    authorization_url: str
    created_at: float


class OAuthStateStore:
    def __init__(self) -> None:
        self._states: dict[str, tuple[OAuthConfig, PKCERequest]] = {}

    def begin(self, config: OAuthConfig) -> PKCERequest:
        config.validate_endpoints()
        verifier = secrets.token_urlsafe(64)
        digest = hashlib.sha256(verifier.encode()).digest()
        challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
        state = secrets.token_urlsafe(32)
        parameters = {
            "response_type": "code",
            "client_id": config.client_id,
            "redirect_uri": config.redirect_uri,
            "scope": " ".join(config.scopes),
            "state": state,
            "code_challenge": challenge,
            "code_challenge_method": "S256",
        }
        if config.resource is not None:
            parameters["resource"] = config.resource
        request = PKCERequest(
            state=state,
            code_verifier=verifier,
            authorization_url=config.authorization_endpoint + "?" + urlencode(parameters),
            created_at=time.time(),
        )
        self._states[state] = (config.model_copy(deep=True), request)
        return request

    def consume(self, state: str) -> tuple[OAuthConfig, PKCERequest]:
        value = self._states.pop(state, None)
        if value is None:
            raise OAuthError("invalid_state", "OAuth state 不存在或已经使用")
        config, request = value
        if time.time() - request.created_at > config.state_ttl_seconds:
            raise OAuthError("expired_state", "OAuth state 已过期")
        return config, request

    def discard(self, state: str) -> None:
        self._states.pop(state, None)


@dataclass(frozen=True)
class TokenSnapshot:
    token: OAuthToken | None
    revision: int


def token_scope(config: OAuthConfig, subject: str) -> str:
    config.validate_endpoints()
    if not subject or len(subject) > 256 or any(ord(character) < 32 for character in subject):
        raise OAuthError("invalid_token_subject", "OAuth subject 必须是非空的受控标识")
    payload = {
        "server": config.server,
        "client_id": config.client_id,
        "authorization_endpoint": config.authorization_endpoint,
        "token_endpoint": config.token_endpoint,
        "redirect_uri": config.redirect_uri,
        "scopes": sorted(set(config.scopes)),
    }
    if config.issuer is not None:
        payload["issuer"] = config.issuer
    if config.resource is not None:
        payload["resource"] = config.resource
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def encode_token(token: OAuthToken) -> bytes:
    payload = token.model_dump(mode="json")
    payload["access_token"] = token.access_token.get_secret_value()
    payload["refresh_token"] = (
        token.refresh_token.get_secret_value() if token.refresh_token is not None else None
    )
    encoded = json.dumps(payload, allow_nan=False).encode()
    if len(encoded) > 65536:
        raise OAuthError("invalid_token_response", "OAuth token 超过大小限制")
    return encoded


class OAuthTokenStore(Protocol):
    async def read(self, config: OAuthConfig, subject: str) -> TokenSnapshot: ...

    async def write(
        self,
        config: OAuthConfig,
        subject: str,
        token: OAuthToken,
        *,
        expected_revision: int | None = None,
    ) -> None: ...

    def lock(self, config: OAuthConfig, subject: str) -> AbstractAsyncContextManager[None]: ...


class EncryptedTokenStore:
    def __init__(self, key: bytes | str) -> None:
        try:
            self._fernet = Fernet(key.encode() if isinstance(key, str) else key)
        except (TypeError, ValueError) as error:
            raise OAuthError("invalid_encryption_key", "Fernet 密钥无效") from error
        self._values: dict[tuple[str, str], bytes] = {}
        self._revisions: dict[tuple[str, str], int] = {}
        self._bindings: dict[tuple[str, str], str] = {}
        self._locks: dict[tuple[str, str], asyncio.Lock] = {}

    def put(self, server: str, subject: str, token: OAuthToken) -> None:
        key = (server, subject)
        self._values[key] = self._fernet.encrypt(encode_token(token))
        self._revisions[key] = self._revisions.get(key, 0) + 1

    def get(self, server: str, subject: str) -> OAuthToken | None:
        blob = self._values.get((server, subject))
        if blob is None:
            return None
        try:
            return OAuthToken.model_validate_json(self._fernet.decrypt(blob))
        except (InvalidToken, ValueError) as error:
            raise OAuthError("token_corrupt", "OAuth token 密文无法验证") from error

    def delete(self, server: str, subject: str) -> None:
        key = (server, subject)
        self._values.pop(key, None)
        self._bindings.pop(key, None)
        self._revisions[key] = self._revisions.get(key, 0) + 1

    async def read(self, config: OAuthConfig, subject: str) -> TokenSnapshot:
        scope = token_scope(config, subject)
        key = (config.server, subject)
        if self._bindings.get(key, scope) != scope:
            raise OAuthError("token_scope_mismatch", "内存令牌已绑定其他客户端配置")
        return TokenSnapshot(self.get(*key), self._revisions.get(key, 0))

    async def write(
        self,
        config: OAuthConfig,
        subject: str,
        token: OAuthToken,
        *,
        expected_revision: int | None = None,
    ) -> None:
        scope = token_scope(config, subject)
        current = await self.read(config, subject)
        if expected_revision is not None and current.revision != expected_revision:
            raise OAuthError("token_conflict", "OAuth 令牌在请求期间已变化")
        self.put(config.server, subject, token)
        self._bindings[(config.server, subject)] = scope

    @asynccontextmanager
    async def lock(self, config: OAuthConfig, subject: str) -> AsyncIterator[None]:
        token_scope(config, subject)
        async with self._locks.setdefault((config.server, subject), asyncio.Lock()):
            yield


Requester = Callable[[str, dict[str, str]], Awaitable[httpx.Response]]


class OAuthHTTPRequester:
    """显式宿主 allowlist、禁止代理/重定向的令牌请求客户端。"""

    def __init__(
        self,
        allowed_hosts: set[str],
        *,
        timeout_seconds: float = 10,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if not allowed_hosts or not 0 < timeout_seconds <= 120:
            raise ValueError("OAuth requester 需要主机白名单和有效超时")
        self.allowed_hosts = {host.lower().rstrip(".") for host in allowed_hosts}
        self._client = httpx.AsyncClient(
            trust_env=False, follow_redirects=False, timeout=timeout_seconds, transport=transport
        )

    async def __aenter__(self) -> OAuthHTTPRequester:
        return self

    async def __aexit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    async def __call__(self, url: str, form: dict[str, str]) -> httpx.Response:
        validate_oauth_endpoint(url, self.allowed_hosts)
        try:
            return await self._client.post(url, data=form)
        except httpx.HTTPError as error:
            raise OAuthError("token_request_failed", "令牌请求网络失败或超时") from error

    async def close(self) -> None:
        await self._client.aclose()

    async def request_json(
        self,
        method: str,
        url: str,
        payload: dict[str, object] | None = None,
        *,
        max_response_bytes: int = 65536,
    ) -> httpx.Response:
        """为 metadata/DCR 复用同一 allowlist、代理和重定向边界。"""

        validate_oauth_endpoint(url, self.allowed_hosts)
        try:
            async with self._client.stream(
                method, url, json=payload, headers={"Accept": "application/json"}
            ) as response:
                if 300 <= response.status_code < 400:
                    raise OAuthError("oauth_redirect", "OAuth metadata 不允许重定向")
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    body.extend(chunk)
                    if len(body) > max_response_bytes:
                        raise OAuthError(
                            "metadata_response_limit", "OAuth metadata 响应超过大小限制"
                        )
                return httpx.Response(
                    response.status_code,
                    headers={
                        key: value
                        for key, value in response.headers.items()
                        if key.lower() not in {"content-encoding", "content-length"}
                    },
                    content=bytes(body),
                    request=response.request,
                )
        except httpx.HTTPError as error:
            raise OAuthError(
                "metadata_request_failed", "OAuth metadata 请求网络失败或超时"
            ) from error


class OAuthClient:
    def __init__(self, states: OAuthStateStore, tokens: OAuthTokenStore) -> None:
        self.states, self.tokens = states, tokens

    async def exchange(self, state: str, code: str, subject: str, request: Requester) -> OAuthToken:
        if not code or not subject:
            raise OAuthError("invalid_authorization_response", "code 与 subject 必须非空")
        config, pkce = self.states.consume(state)
        form = {
            "grant_type": "authorization_code",
            "code": code,
            "client_id": config.client_id,
            "redirect_uri": config.redirect_uri,
            "code_verifier": pkce.code_verifier,
        }
        if config.client_secret is not None:
            form["client_secret"] = config.client_secret.get_secret_value()
        if config.resource is not None:
            form["resource"] = config.resource
        async with self.tokens.lock(config, subject):
            current = await self.tokens.read(config, subject)
            response = await request(config.token_endpoint, form)
            token = self._parse(response)
            await self.tokens.write(config, subject, token, expected_revision=current.revision)
            return token

    async def refresh(self, config: OAuthConfig, subject: str, request: Requester) -> OAuthToken:
        config = config.model_copy(deep=True)
        before = await self.tokens.read(config, subject)
        async with self.tokens.lock(config, subject):
            current = await self.tokens.read(config, subject)
            if (
                current.token is not None
                and current.revision != before.revision
                and not current.token.expired()
            ):
                return current.token
            return await self._refresh_locked(config, subject, current, request)

    async def _refresh_locked(
        self, config: OAuthConfig, subject: str, current: TokenSnapshot, request: Requester
    ) -> OAuthToken:
        token = current.token
        if token is None or token.refresh_token is None:
            raise OAuthError("refresh_unavailable", "没有可用 refresh token")
        form = {
            "grant_type": "refresh_token",
            "refresh_token": token.refresh_token.get_secret_value(),
            "client_id": config.client_id,
        }
        if config.client_secret is not None:
            form["client_secret"] = config.client_secret.get_secret_value()
        if config.resource is not None:
            form["resource"] = config.resource
        response = await request(config.token_endpoint, form)
        refreshed = self._parse(response, previous_refresh=token.refresh_token.get_secret_value())
        await self.tokens.write(config, subject, refreshed, expected_revision=current.revision)
        return refreshed

    async def access_token(
        self,
        config: OAuthConfig,
        subject: str,
        request: Requester,
        *,
        refresh_skew_seconds: float = 30,
    ) -> OAuthToken:
        if not math.isfinite(refresh_skew_seconds) or not 0 <= refresh_skew_seconds <= 3600:
            raise ValueError("refresh_skew_seconds 超出范围")
        config = config.model_copy(deep=True)
        current = await self.tokens.read(config, subject)
        if current.token is not None and not current.token.expired(
            time.time() + refresh_skew_seconds
        ):
            return current.token
        async with self.tokens.lock(config, subject):
            current = await self.tokens.read(config, subject)
            if current.token is not None and not current.token.expired(
                time.time() + refresh_skew_seconds
            ):
                return current.token
            return await self._refresh_locked(config, subject, current, request)

    @staticmethod
    def _parse(response: httpx.Response, previous_refresh: str | None = None) -> OAuthToken:
        if not 200 <= response.status_code < 300:
            raise OAuthError("token_exchange_failed", "OAuth endpoint 返回失败")
        try:
            data = response.json()
            access = data["access_token"]
            token_type = data.get("token_type", "Bearer")
            if (
                not isinstance(access, str)
                or not access
                or not isinstance(token_type, str)
                or token_type.lower() != "bearer"
            ):
                raise ValueError
            expires_in = data.get("expires_in")
            if expires_in is not None and (
                isinstance(expires_in, bool)
                or not isinstance(expires_in, (int, float))
                or not math.isfinite(expires_in)
                or expires_in <= 0
            ):
                raise ValueError
            refresh = data.get("refresh_token", previous_refresh)
            if refresh is not None and not isinstance(refresh, str):
                raise ValueError
            return OAuthToken(
                access_token=SecretStr(access),
                token_type=token_type,
                refresh_token=SecretStr(refresh) if refresh is not None else None,
                expires_at=time.time() + float(expires_in) if expires_in is not None else None,
                scope=data.get("scope") if isinstance(data.get("scope"), str) else None,
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise OAuthError("invalid_token_response", "OAuth token 响应结构无效") from error
