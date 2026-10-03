"""离线 OAuth metadata/DCR/PKCE 示例；使用 HTTPX MockTransport，不连接身份平台。"""

import asyncio
import json

import httpx
from cryptography.fernet import Fernet

from krim_agent.mcp import (
    EncryptedTokenStore,
    OAuthClient,
    OAuthDiscoveryClient,
    OAuthHTTPRequester,
    OAuthStateStore,
)


async def main() -> None:
    issuer = "https://identity.example/tenant"
    redirect_uri = "http://127.0.0.1:9000/callback"

    def endpoint(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "issuer": issuer,
                    "authorization_endpoint": "https://identity.example/authorize",
                    "token_endpoint": "https://identity.example/token",
                    "registration_endpoint": "https://identity.example/register",
                    "response_types_supported": ["code"],
                    "grant_types_supported": ["authorization_code", "refresh_token"],
                    "code_challenge_methods_supported": ["S256"],
                    "token_endpoint_auth_methods_supported": ["none"],
                    "scopes_supported": ["tools"],
                },
            )
        if request.url.path == "/register":
            return httpx.Response(
                201,
                json={
                    "client_id": "offline-client",
                    "redirect_uris": [redirect_uri],
                    "grant_types": ["authorization_code", "refresh_token"],
                    "response_types": ["code"],
                    "token_endpoint_auth_method": "none",
                },
            )
        return httpx.Response(
            200,
            json={
                "access_token": "offline-access",
                "refresh_token": "offline-refresh",
                "expires_in": 600,
            },
        )

    async with OAuthHTTPRequester(
        {"identity.example"}, transport=httpx.MockTransport(endpoint)
    ) as requester:
        discovery = OAuthDiscoveryClient(requester)
        metadata = await discovery.discover(issuer)
        registered = await discovery.register(metadata, redirect_uri=redirect_uri, scopes=["tools"])
        config = discovery.configuration(metadata, registered, server="demo")
        states = OAuthStateStore()
        attempt = states.begin(config)
        # 示例不打开浏览器，只证明 state/S256 和 code exchange 的输入链。
        token = await OAuthClient(states, EncryptedTokenStore(Fernet.generate_key())).exchange(
            attempt.state,
            "offline-code",
            "subject",
            requester,
        )
        print(
            json.dumps(
                {
                    "issuer": config.issuer,
                    "client_id": registered.client_id,
                    "access_token_received": token.access_token.get_secret_value()
                    == "offline-access",
                    "pkce": "code_challenge_method=S256" in attempt.authorization_url,
                },
                ensure_ascii=False,
            )
        )


if __name__ == "__main__":
    asyncio.run(main())
