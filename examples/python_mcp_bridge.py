"""真实官方 stdio MCP → Krim ToolRegistry 权限/校验/审计闭环。"""

import asyncio
import sys
from pathlib import Path

from krim_agent.mcp import MCPBridge, MCPServerConfig
from krim_agent.tools import ToolContext, ToolRegistry


async def main() -> None:
    registry = ToolRegistry()
    bridge = MCPBridge(registry)
    try:
        # 仅连接同目录可信本地脚本；白名单明确授权 add 为只读工具。
        status = await bridge.add_server(
            MCPServerConfig(
                name="local",
                transport="stdio",
                command=sys.executable,
                args=[str(Path(__file__).with_name("python_mcp_server.py"))],
                readonly_tools={"add"},
            )
        )
        print("MCP 状态：", status.status, status.tools)
        result = await registry.invoke("local__add", {"left": 2}, ToolContext(registry))
        print("工具结果：", result.data, "错误：", result.is_error)
        print("审计数：", len(registry.audit))
    finally:
        await bridge.close()
        await registry.close()


if __name__ == "__main__":
    asyncio.run(main())
