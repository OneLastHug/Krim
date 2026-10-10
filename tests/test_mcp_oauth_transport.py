import asyncio
import os
import socket
import sys
from pathlib import Path

import httpx
import pytest
from cryptography.fernet import Fernet
from sqlalchemy import text

from krim_agent.mcp import (
    EncryptedTokenStore,
    MCPBridge,
    MCPOAuthAuth,
    MCPServerConfig,
    OAuthHTTPRequester,
    PersistentEncryptedStateStore,
    PersistentEncryptedTokenStore,
)
from krim_agent.persistence import Database
from krim_agent.tools import ToolContext, ToolRegistry

SERVER = Path(__file__).parent / "fixtures/mcp_oauth_http_server.py"


@pytest.mark.parametrize("persistent", [False, True])
async def test_real_mcp_streamable_http_oauth_transport_and_tool_call(tmp_path, persistent):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    listener.close()
    env = {**os.environ, "KRIM_TEST_OAUTH_MCP_PORT": str(port)}
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(SERVER),
        env=env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    registry = ToolRegistry()
    bridge = MCPBridge(registry)
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'transport.sqlite'}")
    key = Fernet.generate_key()
    try:
        if persistent:
            await database.initialize()
        async with httpx.AsyncClient(trust_env=False) as client:
            async with asyncio.timeout(10):
                while True:
                    if process.returncode is not None:
                        error = (await process.stderr.read()).decode()
                        raise AssertionError(f"MCP OAuth server exited: {error}")
                    try:
                        response = await client.get(f"http://127.0.0.1:{port}/mcp")
                        if response.status_code == 401:
                            break
                    except httpx.HTTPError:
                        await asyncio.sleep(0.05)
        base = f"http://127.0.0.1:{port}"
        redirect = {}

        async def redirect_handler(url):
            async with httpx.AsyncClient(trust_env=False, follow_redirects=False) as browser:
                reply = await browser.get(url)
            assert reply.status_code == 307
            redirect["url"] = reply.headers["location"]

        async def callback_handler():
            from urllib.parse import parse_qs, urlsplit

            query = parse_qs(urlsplit(redirect["url"]).query)
            return query["code"][0], query["state"][0]

        async with OAuthHTTPRequester({"127.0.0.1"}) as requester:
            auth = MCPOAuthAuth(
                base + "/mcp",
                requester=requester,
                tokens=(
                    PersistentEncryptedTokenStore(database, key)
                    if persistent
                    else EncryptedTokenStore(key)
                ),
                states=PersistentEncryptedStateStore(database, key) if persistent else None,
                subject="subject",
                server="oauth",
                redirect_uri="http://127.0.0.1:9000/callback",
                redirect_handler=redirect_handler,
                callback_handler=callback_handler,
                allow_dynamic_registration=True,
                scopes=["tools"],
            )
            status = await bridge.add_server(
                MCPServerConfig(
                    name="oauth",
                    transport="streamable_http",
                    url=base + "/mcp",
                    allowed_hosts={"127.0.0.1"},
                    readonly_tools={"echo"},
                    initialization_timeout_seconds=30,
                ),
                auth=auth,
            )
            assert status.status == "ready"
            result = await registry.invoke(
                "oauth__echo", {"text": "authenticated"}, ToolContext(registry)
            )
            assert not result.is_error and result.data == {"result": "authenticated"}
            assert auth.last_error_code is None
            if persistent:
                async with database.engine.connect() as connection:
                    assert (
                        await connection.execute(
                            text("SELECT status, ciphertext FROM oauth_states")
                        )
                    ).one() == ("consumed", None)
    finally:
        await bridge.close()
        await registry.close()
        if process.returncode is None:
            process.terminate()
        await asyncio.wait_for(process.wait(), 10)
        await database.close()
