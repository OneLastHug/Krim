"""测试专用官方 MCP server；不连接外部服务或操作用户文件。"""

import os

from mcp.server.fastmcp import FastMCP

server = FastMCP("Krim tests", host="127.0.0.1", port=int(os.getenv("KRIM_TEST_MCP_PORT", "8766")))


@server.tool()
def add(left: int, right: int = 3) -> dict[str, int]:
    """只读整数加法。"""
    return {"sum": left + right}


@server.tool()
def echo(text: str) -> str:
    """只读回声。"""
    return text


if __name__ == "__main__":
    server.run(transport="streamable-http" if os.getenv("KRIM_TEST_MCP_HTTP") else "stdio")
