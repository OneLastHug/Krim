"""OAuth PKCE/令牌基础；不自动连接真实身份平台。"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import secrets
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from types import TracebackType
from urllib.parse import urlencode, urlsplit

import httpx
from cryptography.fernet import Fernet, InvalidToken
from pydantic import Field, SecretStr

from krim_agent.protocol.models import StrictModel


class OAuthError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class OAuthConfig(StrictModel):
    server: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,31}$")
    client_id: str = Field(min_length=1, max_length=256)
    client_secret: SecretStr | None = None
    authorization_endpoint: str
    token_endpoint: str
    redirect_uri: str
    scopes: list[str] = Field(default_factory=list, max_length=64)
    allowed_hosts: set[str] = Field(default_factory=set)
    state_ttl_seconds: int = Field(default=600, ge=30, le=3600)

    def validate_endpoints(self) -> None:
        redirect = urlsplit(self.redirect_uri)
        if redirect.scheme not in {"http", "https"} or not redirect.hostname:
            raise OAuthError("invalid_redirect_uri", "redirect_uri 必须是 http/https URL")
        if redirect.username or redirect.password or redirect.fragment:
            raise OAuthError("invalid_redirect_uri", "redirect_uri 不能包含凭证或 fragment")
        for value in (self.authorization_endpoint, self.token_endpoint):
            parsed = urlsplit(value)
            if parsed.scheme not in {"http", "https"} or not parsed.hostname:
                raise OAuthError(
                    "invalid_oauth_endpoint", "OAuth endpoint 必须包含 http/https 主机"
                )
            if parsed.username or parsed.password or parsed.fragment:
                raise OAuthError("invalid_oauth_endpoint", "OAuth endpoint 不能包含凭证或 fragment")
            if parsed.scheme == "http" and parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
                raise OAuthError("insecure_oauth_endpoint", "非本机 OAuth endpoint 必须使用 HTTPS")
            if self.allowed_hosts and parsed.hostname.lower().rstrip(".") not in {
                host.lower().rstrip(".") for host in self.allowed_hosts
            }:
                raise OAuthError("oauth_host_denied", "OAuth endpoint 不在主机白名单")


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
        request = PKCERequest(
            state=state,
            code_verifier=verifier,
            authorization_url=config.authorization_endpoint
            + "?"
            + urlencode(
                {
                    "response_type": "code",
                    "client_id": config.client_id,
                    "redirect_uri": config.redirect_uri,
                    "scope": " ".join(config.scopes),
                    "state": state,
                    "code_challenge": challenge,
                    "code_challenge_method": "S256",
                }
            ),
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


class EncryptedTokenStore:
    def __init__(self, key: bytes | str) -> None:
        try:
            self._fernet = Fernet(key.encode() if isinstance(key, str) else key)
        except Exception as error:
            raise OAuthError("invalid_encryption_key", "Fernet 密钥无效") from error
        self._values: dict[tuple[str, str], bytes] = {}

    def put(self, server: str, subject: str, token: OAuthToken) -> None:
        payload = token.model_dump(mode="json")
        payload["access_token"] = token.access_token.get_secret_value()
        payload["refresh_token"] = (
            token.refresh_token.get_secret_value() if token.refresh_token is not None else None
        )
        self._values[(server, subject)] = self._fernet.encrypt(json.dumps(payload).encode())

    def get(self, server: str, subject: str) -> OAuthToken | None:
        blob = self._values.get((server, subject))
        if blob is None:
            return None
        try:
            return OAuthToken.model_validate_json(self._fernet.decrypt(blob))
        except (InvalidToken, ValueError) as error:
            raise OAuthError("token_corrupt", "OAuth token 密文无法验证") from error

    def delete(self, server: str, subject: str) -> None:
        self._values.pop((server, subject), None)


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
        parsed = urlsplit(url)
        if parsed.hostname is None or parsed.hostname.lower().rstrip(".") not in self.allowed_hosts:
            raise OAuthError("oauth_host_denied", "令牌 endpoint 不在 requester 白名单")
        if (
            parsed.username
            or parsed.password
            or parsed.fragment
            or parsed.scheme not in {"http", "https"}
        ):
            raise OAuthError("invalid_oauth_endpoint", "令牌 endpoint 格式无效")
        if parsed.scheme == "http" and parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise OAuthError("insecure_oauth_endpoint", "非本机令牌 endpoint 必须 HTTPS")
        try:
            return await self._client.post(url, data=form)
        except httpx.HTTPError as error:
            raise OAuthError("token_request_failed", "令牌请求网络失败或超时") from error

    async def close(self) -> None:
        await self._client.aclose()


class OAuthClient:
    def __init__(self, states: OAuthStateStore, tokens: EncryptedTokenStore) -> None:
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
        response = await request(config.token_endpoint, form)
        token = self._parse(response)
        self.tokens.put(config.server, subject, token)
        return token

    async def refresh(self, config: OAuthConfig, subject: str, request: Requester) -> OAuthToken:
        config.validate_endpoints()
        current = self.tokens.get(config.server, subject)
        if current is None or current.refresh_token is None:
            raise OAuthError("refresh_unavailable", "没有可用 refresh token")
        form = {
            "grant_type": "refresh_token",
            "refresh_token": current.refresh_token.get_secret_value(),
            "client_id": config.client_id,
        }
        if config.client_secret is not None:
            form["client_secret"] = config.client_secret.get_secret_value()
        response = await request(config.token_endpoint, form)
        token = self._parse(response, previous_refresh=current.refresh_token.get_secret_value())
        self.tokens.put(config.server, subject, token)
        return token

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
