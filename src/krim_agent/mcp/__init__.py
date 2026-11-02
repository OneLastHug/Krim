"""Official MCP SDK transport and lifecycle primitives."""

from krim_agent.mcp.client import MCPConnection
from krim_agent.mcp.types import MCPServerConfig, MCPTool

__all__ = ["MCPConnection", "MCPServerConfig", "MCPTool"]
