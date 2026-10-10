"""MCP资源引用管理路由；普通HTTP客户端不能登记宿主配置。"""

from fastapi import FastAPI, Query

from krim_agent.mcp.management import (
    MCPControlRequest,
    MCPManager,
    MCPResourceView,
    MCPSelectionRequest,
)


def mount_mcp_management(app: FastAPI, manager: MCPManager) -> None:
    @app.get("/v1/mcp/servers")
    async def servers() -> list[MCPResourceView]:
        return await manager.list()

    @app.get("/v1/mcp/servers/{name}")
    async def server(name: str) -> MCPResourceView:
        return await manager.view(name)

    @app.put("/v1/mcp/servers/{name}")
    async def configure(name: str, payload: MCPSelectionRequest) -> MCPResourceView:
        return await manager.configure(name, payload)

    @app.post("/v1/mcp/servers/{name}/connect", status_code=202)
    async def connect(name: str, payload: MCPControlRequest) -> MCPResourceView:
        return await manager.connect(name, payload)

    @app.post("/v1/mcp/servers/{name}/disconnect")
    async def disconnect(name: str, payload: MCPControlRequest) -> MCPResourceView:
        return await manager.disconnect(name, payload)

    @app.post("/v1/mcp/servers/{name}/wait")
    async def wait(
        name: str, timeout_seconds: float = Query(default=30, ge=0, le=300)
    ) -> MCPResourceView:
        return await manager.wait(name, timeout_seconds)
