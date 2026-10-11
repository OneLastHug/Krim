"""项目外安装包：0033引用/CAS/工具版本与配置秘密边界。"""

import asyncio
import json

from mcp.types import CallToolResult, ListToolsResult, Tool

import krim_agent
from krim_agent.mcp import (
    MCPControlRequest,
    MCPManager,
    MCPSelectionRequest,
    MCPServerConfig,
    TrustedMCPServer,
)
from krim_agent.persistence import Database
from krim_agent.tools import ToolRegistry


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
    database = Database("sqlite+aiosqlite:///:memory:")
    await database.initialize()
    manager = MCPManager(database, ToolRegistry())
    manager.register(
        TrustedMCPServer(
            MCPServerConfig(name="wheel", transport="stdio", command="trusted"),
            "v1",
            session_factory=make_session,
        )
    )
    try:
        view = (await manager.list())[0]
        enabled = await manager.configure(
            "wheel",
            MCPSelectionRequest(
                expected_revision=0, resource_hash=view.resource_hash, enabled=True
            ),
        )
        connected = await manager.connect(
            "wheel",
            MCPControlRequest(
                expected_revision=enabled.profile.revision, resource_hash=view.resource_hash
            ),
        )
        assert connected.connection.status in {"connecting", "ready"}
        assert (await manager.wait("wheel")).connection.status == "ready"
        print(
            json.dumps(
                {
                    "package": krim_agent.__file__,
                    "migration": "0035_oauth_browser_callbacks",
                    "revision": enabled.profile.revision,
                    "resource_hash": view.resource_hash,
                    "secret_free": True,
                }
            )
        )
    finally:
        await manager.close()
        await database.close()


if __name__ == "__main__":
    asyncio.run(main())
