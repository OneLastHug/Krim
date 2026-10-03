import asyncio
import os
import socket
import sys
from pathlib import Path

import httpx

from krim_agent.mcp import MCPBridge, MCPServerConfig
from krim_agent.tools import ToolContext, ToolRegistry

SERVER = Path(__file__).parent / "fixtures/mcp_demo_server.py"


async def test_real_stdio_initialize_schema_call_and_cross_task_close():
    registry = ToolRegistry()
    bridge = MCPBridge(registry)
    status = await asyncio.create_task(
        bridge.add_server(
            MCPServerConfig(
                name="stdio",
                transport="stdio",
                command=sys.executable,
                args=[str(SERVER)],
                readonly_tools={"add", "echo"},
            )
        )
    )
    try:
        assert status.status == "ready" and "stdio__add" in status.tools
        result = await registry.invoke("stdio__add", {"left": 2}, ToolContext(registry))
        assert not result.is_error and result.data == {"sum": 5}
        schema = registry.get("stdio__add").schema().input_schema
        assert schema["properties"]["right"]["default"] == 3
        invalid = await registry.invoke("stdio__add", {"left": "2"}, ToolContext(registry))
        assert invalid.is_error and invalid.code == "invalid_input"
        assert len(registry.audit) == 2
    finally:
        await asyncio.create_task(bridge.close())
        await registry.close()
    assert bridge.statuses()[0].status == "closed"


async def test_real_streamable_http_initialize_call_and_origin_boundary():
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    listener.close()
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(SERVER),
        env={**os.environ, "KRIM_TEST_MCP_HTTP": "1", "KRIM_TEST_MCP_PORT": str(port)},
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    registry = ToolRegistry()
    bridge = MCPBridge(registry)
    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            async with asyncio.timeout(10):
                while True:
                    if process.returncode is not None:
                        raise AssertionError("测试 MCP HTTP 进程提前退出")
                    try:
                        await client.get(f"http://127.0.0.1:{port}/mcp")
                        break
                    except httpx.HTTPError:
                        await asyncio.sleep(0.05)
        status = await bridge.add_server(
            MCPServerConfig(
                name="http",
                transport="streamable_http",
                url=f"http://127.0.0.1:{port}/mcp",
                allowed_hosts={"127.0.0.1"},
                readonly_tools={"echo"},
            )
        )
        assert status.status == "ready"
        result = await registry.invoke("http__echo", {"text": "中文 MCP"}, ToolContext(registry))
        assert not result.is_error and "中文 MCP" in str(result.data)
    finally:
        await bridge.close()
        await registry.close()
        process.terminate()
        await asyncio.wait_for(process.wait(), 10)
