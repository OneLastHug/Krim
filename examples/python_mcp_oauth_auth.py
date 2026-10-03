"""离线 MCP HTTPX OAuth Auth 示例；虚构 token 与 MockTransport，不打开浏览器。"""

import asyncio
from urllib.parse import parse_qs, urlsplit

import httpx
from cryptography.fernet import Fernet

from krim_agent.mcp import EncryptedTokenStore, MCPOAuthAuth, OAuthConfig, OAuthHTTPRequester


async def main() -> None:
    resource = "http://127.0.0.1:9000/mcp"
    config = OAuthConfig(
        server="demo",
        client_id="offline-client",
        issuer="http://127.0.0.1:9001",
        resource=resource,
        authorization_endpoint="http://127.0.0.1:9001/authorize",
        token_endpoint="http://127.0.0.1:9001/token",
        redirect_uri="http://127.0.0.1:9002/callback",
        allowed_hosts={"127.0.0.1"},
    )
    callback = {}
    calls = []

    def endpoint(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/token":
            assert parse_qs(request.content.decode())["resource"] == [resource]
            return httpx.Response(200, json={"access_token": "offline-access", "expires_in": 600})
        calls.append(request.headers.get("authorization"))
        if request.headers.get("authorization") != "Bearer offline-access":
            return httpx.Response(401, headers={"WWW-Authenticate": "Bearer"})
        return httpx.Response(200, json={"authenticated": True})

    async def redirect(url: str) -> None:
        # 宿主在此展示授权 URL；离线示例只记录 state，不打开浏览器。
        callback["state"] = parse_qs(urlsplit(url).query)["state"][0]

    async def receive() -> tuple[str, str | None]:
        return "offline-code", callback["state"]

    async with OAuthHTTPRequester(
        {"127.0.0.1"}, transport=httpx.MockTransport(endpoint)
    ) as requester:
        auth = MCPOAuthAuth(
            resource,
            requester=requester,
            tokens=EncryptedTokenStore(Fernet.generate_key()),
            subject="offline-subject",
            server="demo",
            config=config,
            redirect_handler=redirect,
            callback_handler=receive,
        )
        async with httpx.AsyncClient(auth=auth, transport=httpx.MockTransport(endpoint)) as client:
            response = await client.post(
                resource, json={"jsonrpc": "2.0", "method": "initialize", "id": 1}
            )
            print(f"认证成功：{response.json()['authenticated']}；资源请求次数：{len(calls)}")
            assert calls == [None, "Bearer offline-access"]


if __name__ == "__main__":
    asyncio.run(main())
