"""显式授权服务的 metadata/DCR；参考 mcp 1.30.0，见 ADR-0016。"""

from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit

import httpx
from pydantic import ConfigDict, Field, SecretStr

from krim_agent.protocol.models import StrictModel, check_json

from .oauth import OAuthConfig, OAuthError, OAuthHTTPRequester, validate_oauth_endpoint


class OAuthMetadata(StrictModel):
    model_config = ConfigDict(extra="ignore", hide_input_in_errors=True)
    issuer: str = Field(min_length=1, max_length=2048)
    authorization_endpoint: str = Field(min_length=1, max_length=2048)
    token_endpoint: str = Field(min_length=1, max_length=2048)
    registration_endpoint: str | None = Field(default=None, min_length=1, max_length=2048)
    response_types_supported: list[str] = Field(min_length=1, max_length=64)
    grant_types_supported: list[str] | None = Field(default=None, max_length=64)
    token_endpoint_auth_methods_supported: list[str] | None = Field(default=None, max_length=64)
    code_challenge_methods_supported: list[str] | None = Field(default=None, max_length=64)
    scopes_supported: list[str] | None = Field(default=None, max_length=128)


class RegisteredClient(StrictModel):
    model_config = ConfigDict(extra="ignore", hide_input_in_errors=True)
    client_id: str = Field(min_length=1, max_length=256)
    client_secret: SecretStr | None = None
    issuer: str
    redirect_uris: list[str] = Field(min_length=1, max_length=16)
    token_endpoint_auth_method: str = "none"
    grant_types: list[str] = Field(default_factory=lambda: ["authorization_code", "refresh_token"])
    response_types: list[str] = Field(default_factory=lambda: ["code"])
    scope: str | None = None


def _issuer_equal(left: str, right: str) -> bool:
    if left == right:
        return True
    short, long = sorted((left, right), key=len)
    try:
        parsed = urlsplit(short)
    except ValueError:
        return False
    return long == short + "/" and parsed.path == ""


class OAuthDiscoveryClient:
    def __init__(self, request: OAuthHTTPRequester, *, max_response_bytes: int = 65536) -> None:
        if not 1024 <= max_response_bytes <= 1_048_576:
            raise ValueError("OAuth metadata 响应限制无效")
        self.request = request
        self.max_response_bytes = max_response_bytes

    def _json(self, response: httpx.Response) -> dict[str, object]:
        if len(response.content) > self.max_response_bytes:
            raise OAuthError("metadata_response_limit", "OAuth metadata 响应超过大小限制")
        if not 200 <= response.status_code < 300:
            raise OAuthError("metadata_http_error", "OAuth metadata endpoint 返回失败")
        try:
            value = response.json()
            if not isinstance(value, dict):
                raise ValueError
            check_json(value, max_bytes=self.max_response_bytes)
            return value
        except (ValueError, RecursionError) as error:
            raise OAuthError("invalid_oauth_metadata", "OAuth metadata JSON 无效") from error

    async def discover(self, issuer: str, *, metadata_url: str | None = None) -> OAuthMetadata:
        validate_oauth_endpoint(issuer, self.request.allowed_hosts)
        parsed = urlsplit(issuer)
        prefix = parsed.path.rstrip("/")
        paths = ["/.well-known/oauth-authorization-server" + prefix]
        paths.append("/.well-known/openid-configuration" + prefix)
        if prefix:
            paths.append(prefix + "/.well-known/openid-configuration")
        if metadata_url is not None:
            validate_oauth_endpoint(metadata_url, self.request.allowed_hosts)
            location = urlsplit(metadata_url)
            if (location.scheme, location.hostname, location.port) != (
                parsed.scheme,
                parsed.hostname,
                parsed.port,
            ):
                raise OAuthError("oauth_issuer_mismatch", "metadata URL 必须属于选定 issuer 的来源")
        endpoints = (
            [metadata_url]
            if metadata_url is not None
            else [urlunsplit((parsed.scheme, parsed.netloc, path, "", "")) for path in paths]
        )
        for endpoint in endpoints:
            response = await self.request.request_json(
                "GET", endpoint, max_response_bytes=self.max_response_bytes
            )
            if response.status_code == 404:
                continue
            try:
                metadata = OAuthMetadata.model_validate(self._json(response))
            except ValueError as error:
                raise OAuthError("invalid_oauth_metadata", "OAuth metadata 字段无效") from error
            self._validate(metadata, expected_issuer=issuer)
            return metadata
        raise OAuthError("metadata_unavailable", "OAuth 服务没有可用 metadata")

    def _validate(self, metadata: OAuthMetadata, *, expected_issuer: str | None = None) -> None:
        if expected_issuer is not None and not _issuer_equal(metadata.issuer, expected_issuer):
            raise OAuthError("oauth_issuer_mismatch", "OAuth metadata issuer 不一致")
        for url in (metadata.issuer, metadata.authorization_endpoint, metadata.token_endpoint):
            validate_oauth_endpoint(url, self.request.allowed_hosts)
        if metadata.registration_endpoint is not None:
            validate_oauth_endpoint(metadata.registration_endpoint, self.request.allowed_hosts)
        if "code" not in metadata.response_types_supported:
            raise OAuthError("unsupported_oauth_metadata", "OAuth 服务不支持 code response")
        if "S256" not in (metadata.code_challenge_methods_supported or []):
            raise OAuthError("unsupported_oauth_metadata", "OAuth 服务不支持 S256 PKCE")
        if metadata.grant_types_supported is not None and not {
            "authorization_code",
            "refresh_token",
        }.issubset(metadata.grant_types_supported):
            raise OAuthError("unsupported_oauth_metadata", "OAuth 服务不支持授权码与 refresh grant")

    @staticmethod
    def _scopes(metadata: OAuthMetadata, scopes: list[str]) -> None:
        if len(scopes) > 64 or any(
            not scope
            or len(scope) > 256
            or any(not 33 <= ord(c) <= 126 or c in '"\\' for c in scope)
            for scope in scopes
        ):
            raise OAuthError("invalid_scope", "OAuth scope 必须是合法 ASCII token")
        if metadata.scopes_supported is not None and not set(scopes).issubset(
            metadata.scopes_supported
        ):
            raise OAuthError("invalid_scope", "OAuth 服务不支持请求的 scope")

    async def register(
        self,
        metadata: OAuthMetadata,
        *,
        redirect_uri: str,
        scopes: list[str] | None = None,
        token_endpoint_auth_method: str = "none",
        client_name: str = "Krim Agent",
    ) -> RegisteredClient:
        self._validate(metadata)
        self._scopes(metadata, scopes or [])
        if metadata.registration_endpoint is None:
            raise OAuthError("registration_unavailable", "OAuth 服务没有 registration endpoint")
        if token_endpoint_auth_method not in {"none", "client_secret_post"}:
            raise OAuthError("unsupported_client_auth", "Krim 当前只支持 none/client_secret_post")
        if (
            metadata.token_endpoint_auth_methods_supported is not None
            and token_endpoint_auth_method not in metadata.token_endpoint_auth_methods_supported
        ):
            raise OAuthError("unsupported_client_auth", "OAuth 服务不支持选定 client auth method")
        validator = OAuthConfig(
            server="registration",
            client_id="registration",
            authorization_endpoint=metadata.authorization_endpoint,
            token_endpoint=metadata.token_endpoint,
            redirect_uri=redirect_uri,
            allowed_hosts=self.request.allowed_hosts,
        )
        validator.validate_endpoints()
        payload: dict[str, object] = {
            "redirect_uris": [redirect_uri],
            "grant_types": ["authorization_code", "refresh_token"],
            "response_types": ["code"],
            "token_endpoint_auth_method": token_endpoint_auth_method,
            "client_name": client_name,
        }
        if scopes:
            payload["scope"] = " ".join(scopes)
        response = await self.request.request_json(
            "POST",
            metadata.registration_endpoint,
            payload,
            max_response_bytes=self.max_response_bytes,
        )
        values = self._json(response)
        values["issuer"] = metadata.issuer
        try:
            registered = RegisteredClient.model_validate(values)
        except ValueError as error:
            raise OAuthError("invalid_registration_response", "OAuth 注册响应字段无效") from error
        if (
            registered.redirect_uris != [redirect_uri]
            or registered.token_endpoint_auth_method != token_endpoint_auth_method
        ):
            raise OAuthError("invalid_registration_response", "OAuth 注册响应改变了回调或认证方式")
        if set(registered.grant_types) != {
            "authorization_code",
            "refresh_token",
        } or registered.response_types != ["code"]:
            raise OAuthError("invalid_registration_response", "OAuth 注册响应没有授权码能力")
        if token_endpoint_auth_method == "client_secret_post" and registered.client_secret is None:
            raise OAuthError("invalid_registration_response", "OAuth 注册响应缺少 client secret")
        if token_endpoint_auth_method == "none" and registered.client_secret is not None:
            raise OAuthError("invalid_registration_response", "公开客户端不应返回 secret")
        if registered.client_secret is not None and not registered.client_secret.get_secret_value():
            raise OAuthError("invalid_registration_response", "OAuth client secret 不能为空")
        if registered.scope is not None and set(registered.scope.split()) != set(scopes or []):
            raise OAuthError("invalid_registration_response", "OAuth 注册响应改变了 scope")
        registered.scope = " ".join(scopes or []) or None
        return registered

    def configuration(
        self,
        metadata: OAuthMetadata,
        client: RegisteredClient,
        *,
        server: str,
        scopes: list[str] | None = None,
    ) -> OAuthConfig:
        self._validate(metadata)
        if not _issuer_equal(client.issuer, metadata.issuer):
            raise OAuthError("oauth_issuer_mismatch", "OAuth 客户端属于其他 issuer")
        selected_scopes = scopes if scopes is not None else (client.scope or "").split()
        self._scopes(metadata, selected_scopes)
        if client.scope is not None and not set(selected_scopes).issubset(client.scope.split()):
            raise OAuthError("invalid_scope", "OAuth scope 超出已注册客户端范围")
        if client.token_endpoint_auth_method not in {"none", "client_secret_post"}:
            raise OAuthError("unsupported_client_auth", "OAuth 客户端认证方式不受支持")
        configured = OAuthConfig(
            server=server,
            client_id=client.client_id,
            issuer=metadata.issuer,
            client_secret=client.client_secret,
            authorization_endpoint=metadata.authorization_endpoint,
            token_endpoint=metadata.token_endpoint,
            redirect_uri=client.redirect_uris[0],
            scopes=selected_scopes,
            allowed_hosts=set(self.request.allowed_hosts),
        )
        configured.validate_endpoints()
        return configured
