"""离线浏览器授权intent演练：Cookie绑定、原state、完成后只读查询。"""

import asyncio
import json
from urllib.parse import parse_qs, urlsplit

import httpx
from cryptography.fernet import Fernet

from krim_agent.api import create_app
from krim_agent.mcp import (
    MCPOAuthAuth,
    MCPServerConfig,
    OAuthConfig,
    OAuthHTTPRequester,
    PersistentEncryptedStateStore,
    PersistentEncryptedTokenStore,
    TrustedMCPServer,
)
from krim_agent.mcp.browser import CALLBACK_PATH
from krim_agent.sdk import PythonAI


async def main() -> None:
    async with await PythonAI.create() as ai:
        database, key = ai.runtime.repository.database, Fernet.generate_key()

        async def endpoint(_request):
            return httpx.Response(200, json={"access_token": "fake-access", "expires_in": 600})

        async with OAuthHTTPRequester(
            {"identity.example"}, transport=httpx.MockTransport(endpoint)
        ) as requester:
            config = OAuthConfig(
                server="demo",
                client_id="client",
                issuer="https://identity.example",
                resource="https://mcp.example/mcp",
                authorization_endpoint="https://identity.example/authorize",
                token_endpoint="https://identity.example/token",
                redirect_uri="http://127.0.0.1" + CALLBACK_PATH,
                allowed_hosts={"identity.example"},
            )
            auth = MCPOAuthAuth(
                config.resource,
                requester=requester,
                tokens=PersistentEncryptedTokenStore(database, key),
                states=PersistentEncryptedStateStore(database, key),
                subject="subject",
                server="demo",
                config=config,
            )
            app = create_app(
                runtime=ai.runtime,
                oauth_browser_key=key,
                mcp_servers=[
                    TrustedMCPServer(
                        MCPServerConfig(
                            name="demo",
                            transport="streamable_http",
                            url=config.resource,
                            allowed_hosts={"mcp.example"},
                        ),
                        "v1",
                        auth=auth,
                    )
                ],
            )
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
                ) as api,
            ):
                resource = (await api.get("/v1/mcp/servers")).json()[0]
                await api.put(
                    "/v1/mcp/servers/demo",
                    json={
                        "expected_revision": 0,
                        "resource_hash": resource["resource_hash"],
                        "enabled": True,
                    },
                )
                started = (
                    await api.post(
                        "/v1/mcp/servers/demo/oauth/begin",
                        json={"expected_revision": 1, "resource_hash": resource["resource_hash"]},
                    )
                ).json()
                state = parse_qs(urlsplit(started["authorization_url"]).query)["state"][0]
                returned = await api.get(
                    CALLBACK_PATH, params={"state": state, "code": "fake-code"}
                )
                assert returned.status_code == 303
                saved = (
                    await api.get(f"/v1/mcp/servers/demo/oauth/attempts/{started['attempt_id']}")
                ).json()
                assert saved["phase"] == "complete" and saved["authorization_url"] is None
                print(
                    json.dumps(
                        {
                            "cookie_bound": True,
                            "completed": True,
                            "redirect_clean": state not in returned.headers["location"],
                        }
                    )
                )


if __name__ == "__main__":
    asyncio.run(main())
