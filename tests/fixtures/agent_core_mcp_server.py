from __future__ import annotations

import asyncio
import os

from mcp.server.fastmcp import FastMCP

HOST = "127.0.0.1"
PORT = int(os.environ.get("KRIM_AGENT_TEST_MCP_PORT", "0"))
server = FastMCP(
    "Krim Agent MCP client fixture",
    host=HOST,
    port=PORT,
    streamable_http_path="/mcp",
)


@server.tool()
def echo(text: str) -> dict[str, str]:
    """Return supplied fixture text."""
    return {"echo": text}


@server.tool()
def greet(name: str) -> str:
    """Return a text-only result."""
    return f"hello {name}"


@server.tool()
async def wait_for_cancel(seconds: float = 30.0) -> str:
    """Wait long enough for lifecycle cancellation tests."""
    await asyncio.sleep(seconds)
    return "finished"


if os.environ.get("KRIM_AGENT_TEST_MCP_HTTP") == "1":
    server.run(transport="streamable-http")
else:
    server.run(transport="stdio")
