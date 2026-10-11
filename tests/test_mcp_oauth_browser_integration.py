import asyncio
import os
import socket
import sys
from pathlib import Path

import httpx
import uvicorn
from cryptography.fernet import Fernet

from krim_agent.api import create_app
from krim_agent.mcp import (
    MCPOAuthAuth,
    MCPServerConfig,
    OAuthHTTPRequester,
    PersistentEncryptedStateStore,
    PersistentEncryptedTokenStore,
    PersistentOAuthClientRegistrationStore,
    TrustedMCPServer,
)
from krim_agent.mcp.browser import CALLBACK_PATH
from krim_agent.tools import ToolContext


async def test_real_prm_dcr_pkce_browser_callback_token_and_mcp_tool_chain(runtime):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    base = f"http://127.0.0.1:{listener.getsockname()[1]}"
    remote_socket = socket.socket()
    remote_socket.bind(("127.0.0.1", 0))
    remote_port = remote_socket.getsockname()[1]
    remote_socket.close()
    remote = f"http://127.0.0.1:{remote_port}"
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(Path(__file__).parent / "fixtures/mcp_oauth_http_server.py"),
        env={**os.environ, "KRIM_TEST_OAUTH_MCP_PORT": str(remote_port)},
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    key = Fernet.generate_key()
    database = runtime.repository.database
    requester = OAuthHTTPRequester({"127.0.0.1"})
    auth = MCPOAuthAuth(
        remote + "/mcp",
        requester=requester,
        tokens=PersistentEncryptedTokenStore(database, key),
        states=PersistentEncryptedStateStore(database, key),
        clients=PersistentOAuthClientRegistrationStore(database, key),
        subject="browser-subject",
        server="oauth",
        redirect_uri=base + CALLBACK_PATH,
        scopes=["tools"],
        allow_dynamic_registration=True,
    )
    app = create_app(
        runtime=runtime,
        oauth_browser_key=key,
        mcp_servers=[
            TrustedMCPServer(
                MCPServerConfig(
                    name="oauth",
                    transport="streamable_http",
                    url=remote + "/mcp",
                    allowed_hosts={"127.0.0.1"},
                    readonly_tools={"echo"},
                ),
                "v1",
                auth=auth,
            )
        ],
    )
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        async with httpx.AsyncClient(trust_env=False) as wait_remote:
            async with asyncio.timeout(10):
                while True:
                    if process.returncode is not None:
                        raise AssertionError((await process.stderr.read()).decode())
                    try:
                        if (await wait_remote.get(remote + "/mcp")).status_code == 401:
                            break
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(0.05)
        async with asyncio.timeout(5):
            while not server.started:
                if serving.done():
                    await serving
                await asyncio.sleep(0.01)
        async with httpx.AsyncClient(
            base_url=base, trust_env=False, follow_redirects=False
        ) as browser:
            resource = (await browser.get("/v1/mcp/servers")).json()[0]
            await browser.put(
                "/v1/mcp/servers/oauth",
                json={
                    "expected_revision": 0,
                    "resource_hash": resource["resource_hash"],
                    "enabled": True,
                },
            )
            control = {"expected_revision": 1, "resource_hash": resource["resource_hash"]}
            begun = await browser.post("/v1/mcp/servers/oauth/oauth/begin", json=control)
            assert begun.status_code == 200, begun.text
            attempt = begun.json()
            authorized = await browser.get(attempt["authorization_url"])
            assert authorized.status_code == 307
            callback = await browser.get(authorized.headers["location"])
            assert callback.status_code == 303, callback.text
            saved = (
                await browser.get(f"/v1/mcp/servers/oauth/oauth/attempts/{attempt['attempt_id']}")
            ).json()
            assert saved["phase"] == "complete"
            assert (
                await browser.post("/v1/mcp/servers/oauth/connect", json=control)
            ).status_code == 202
            ready = (
                await browser.post("/v1/mcp/servers/oauth/wait", params={"timeout_seconds": 5})
            ).json()
            assert ready["connection"]["status"] == "ready", ready
            result = await runtime.tools.invoke(
                "oauth__echo", {"text": "browser authorized"}, ToolContext(runtime.tools)
            )
            assert not result.is_error and result.data == {"result": "browser authorized"}
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(serving, 5)
        finally:
            listener.close()
            await requester.close()
            if process.returncode is None:
                process.terminate()
            await asyncio.wait_for(process.wait(), 5)
