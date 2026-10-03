"""离线例子服务器：官方 MCP stdio，仅提供只读 add 工具。"""

from mcp.server.fastmcp import FastMCP

server = FastMCP("Krim local example")


@server.tool()
def add(left: int, right: int = 3) -> dict[str, int]:
    """返回两个整数之和，不连接外部服务。"""
    return {"sum": left + right}


if __name__ == "__main__":
    server.run(transport="stdio")
