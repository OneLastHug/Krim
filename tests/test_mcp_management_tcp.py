import asyncio
import json
import os
import socket
import sys

import uvicorn
from test_mcp_management import trusted

from krim_agent.api import create_app
from krim_agent.client import KrimClient
from krim_agent.mcp import MCPControlRequest, MCPSelectionRequest


async def test_real_client_cli_use_same_public_mcp_profile_and_background_connect(runtime):
    app = create_app(runtime=runtime, mcp_servers=[trusted()])
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    base = f"http://127.0.0.1:{listener.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        async with asyncio.timeout(5):
            while not server.started:
                if serving.done():
                    await serving
                await asyncio.sleep(0.01)

        def connect():
            with KrimClient(base) as client:
                view = client.mcp_servers()[0]
                selected = client.mcp_configure(
                    "demo",
                    MCPSelectionRequest(
                        expected_revision=0, resource_hash=view.resource_hash, enabled=True
                    ),
                )
                client.mcp_connect(
                    "demo",
                    MCPControlRequest(
                        expected_revision=selected.profile.revision,
                        resource_hash=view.resource_hash,
                    ),
                )
                result = client.mcp_wait("demo", 3)
                assert result.connection.status == "ready"
                return result

        ready = await asyncio.to_thread(connect)
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "krim_agent",
            "mcp",
            "get",
            "demo",
            "--url",
            base,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ},
        )
        out, err = await asyncio.wait_for(process.communicate(), 8)
        assert process.returncode == 0, err.decode()
        decoded = json.loads(out)
        assert decoded["profile"]["revision"] == ready.profile.revision
        assert decoded["connection"]["status"] == "ready"
        assert "trusted" not in out.decode()
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "krim_agent",
            "mcp",
            "disable",
            "demo",
            "--url",
            base,
            "--revision",
            "1",
            "--resource-hash",
            ready.resource_hash,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ},
        )
        out, err = await asyncio.wait_for(process.communicate(), 8)
        assert process.returncode == 0, err.decode()
        disabled = json.loads(out)
        assert disabled["profile"]["revision"] == 2 and not disabled["profile"]["enabled"]
        assert disabled["connection"]["status"] == "closed"
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(serving, 5)
        finally:
            listener.close()
