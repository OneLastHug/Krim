"""项目外安装包：MCP remove/reconnect 与撤销后的旧工具边界。"""

import asyncio
import json

from mcp.types import CallToolResult, ListToolsResult, Tool

import krim_agent
from krim_agent.mcp import MCPBridge, MCPServerConfig
from krim_agent.tools import ToolContext, ToolRegistry


class Session:
    async def initialize(self):
        return None

    async def list_tools(self, *, params=None):
        return ListToolsResult(
            tools=[Tool(name="echo", inputSchema={"type": "object", "properties": {}})]
        )

    async def call_tool(self, name, arguments):
        return CallToolResult(content=[], structuredContent={"ok": True})


async def make_session():
    return Session()


async def main() -> None:
    assert "/site-packages/" in krim_agent.__file__
    registry = ToolRegistry()
    bridge = MCPBridge(registry)
    try:
        status = await bridge.add_server(
            MCPServerConfig(
                name="demo", transport="stdio", command="fake", readonly_tools={"echo"}
            ),
            session_factory=make_session,
        )
        assert status.status == "ready"
        frozen = registry.select(["demo__echo"])
        assert (await bridge.remove_server("demo")).status == "closed"
        assert not bridge.tools()
        await bridge.add_server(
            MCPServerConfig(
                name="demo", transport="stdio", command="fake", readonly_tools={"echo"}
            ),
            session_factory=make_session,
        )
        stale = await frozen.invoke("demo__echo", {}, ToolContext(frozen))
        assert stale.is_error and stale.code == "mcp_request_failed"
        assert (await registry.invoke("demo__echo", {}, ToolContext(registry))).data == {"ok": True}
        print(
            json.dumps(
                {
                    "package": krim_agent.__file__,
                    "removed": True,
                    "reconnected": True,
                    "old_binding_rejected": True,
                }
            )
        )
    finally:
        await bridge.close()
        await registry.close()


if __name__ == "__main__":
    asyncio.run(main())
