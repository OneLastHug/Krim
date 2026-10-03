import asyncio
from types import SimpleNamespace

import pytest
from mcp.types import CallToolResult, ListToolsResult, TextContent, Tool

from krim_agent.mcp import MCPBridge, MCPConnectionError, MCPServerConfig, validate_server_url
from krim_agent.tools import ToolContext, ToolError, ToolRegistry


class FakeSession:
    def __init__(self, pages, *, fail_call=False):
        self.pages = pages
        self.fail_call = fail_call
        self.calls = []
        self.initialized = False

    async def initialize(self):
        self.initialized = True

    async def list_tools(self, *, params=None):
        cursor = params.cursor if params is not None else None
        page = self.pages[0] if cursor is None else self.pages[1]
        return ListToolsResult(tools=page.tools, nextCursor=page.nextCursor)

    async def call_tool(self, name, arguments):
        self.calls.append((name, arguments))
        if self.fail_call:
            raise RuntimeError("remote failure")
        return CallToolResult(
            content=[TextContent(type="text", text=f"{name}:{arguments}")],
            structuredContent={"ok": True, "arguments": arguments},
        )


def tool(name, *, description="tool", schema=None):
    return Tool(
        name=name,
        description=description,
        inputSchema=schema or {"type": "object", "properties": {}},
    )


async def factory(session):
    return session


async def test_discovery_pagination_namespace_schema_defaults_and_readonly_policy():
    pages = [
        SimpleNamespace(
            tools=[
                tool(
                    "lookup",
                    schema={
                        "type": "object",
                        "properties": {"q": {"type": "string", "default": "all"}},
                    },
                )
            ],
            nextCursor="next",
        ),
        SimpleNamespace(tools=[tool("other")], nextCursor=None),
    ]
    session = FakeSession(pages)
    registry = ToolRegistry()
    bridge = MCPBridge(registry)
    status = await bridge.add_server(
        MCPServerConfig(
            name="demo",
            transport="stdio",
            command="fake",
            namespace="remote",
            readonly_tools={"lookup"},
        ),
        session_factory=lambda: factory(session),
    )
    assert status.status == "ready" and status.tools == ["remote__lookup", "remote__other"]
    assert session.initialized
    lookup = await registry.invoke("remote__lookup", {}, ToolContext(registry))
    assert lookup.data == {"ok": True, "arguments": {"q": "all"}}
    assert session.calls == [("lookup", {"q": "all"})]
    with pytest.raises(ToolError) as error:
        await registry.call_tool("remote__other", {})
    assert error.value.code == "permission_denied"
    await bridge.close()
    assert bridge.statuses()[0].status == "closed"


async def test_bad_server_isolated_and_tool_errors_are_not_success():
    good = FakeSession([SimpleNamespace(tools=[tool("ok")], nextCursor=None)])
    bad = FakeSession([SimpleNamespace(tools=[tool("ok")], nextCursor=None)], fail_call=True)
    registry = ToolRegistry()
    bridge = MCPBridge(registry)
    good_status, bad_status = await asyncio.gather(
        bridge.add_server(
            MCPServerConfig(name="good", transport="stdio", command="good"),
            session_factory=lambda: factory(good),
        ),
        bridge.add_server(
            MCPServerConfig(name="bad", transport="stdio", command="bad", readonly_tools={"ok"}),
            session_factory=lambda: factory(bad),
        ),
    )
    assert good_status.status == "ready" and bad_status.status == "ready"
    bad_result = await registry.invoke("bad__ok", {}, ToolContext(registry))
    assert bad_result.is_error and bad_result.code == "mcp_request_failed"
    assert any(status.name == "good" and status.status == "ready" for status in bridge.statuses())
    await bridge.close()


async def test_conflict_schema_url_boundaries_and_cancel_cleanup():
    session = FakeSession([SimpleNamespace(tools=[tool("same")], nextCursor=None)])
    bridge = MCPBridge(ToolRegistry())
    await bridge.add_server(
        MCPServerConfig(name="one", transport="stdio", command="one"),
        session_factory=lambda: factory(session),
    )
    conflict = await bridge.add_server(
        MCPServerConfig(name="two", transport="stdio", command="two", namespace="one"),
        session_factory=lambda: factory(session),
    )
    assert conflict.status == "failed" and len(bridge.tools()) == 1
    for config in (
        MCPServerConfig(
            name="http",
            transport="streamable_http",
            url="http://example.com/mcp",
            allowed_hosts={"example.com"},
        ),
        MCPServerConfig(
            name="cred",
            transport="streamable_http",
            url="https://u:p@example.com/mcp",
            allowed_hosts={"example.com"},
        ),
    ):
        with pytest.raises(MCPConnectionError):
            validate_server_url(config)
    await bridge.close()
    assert not session.calls


async def test_connection_cancelled_call_does_not_leave_pending_owner_task():
    class SlowSession(FakeSession):
        async def call_tool(self, name, arguments):
            await asyncio.Event().wait()

    session = SlowSession([SimpleNamespace(tools=[tool("slow")], nextCursor=None)])
    registry = ToolRegistry()
    bridge = MCPBridge(registry)
    await bridge.add_server(
        MCPServerConfig(
            name="slow",
            transport="stdio",
            command="slow",
            request_timeout_seconds=5,
            readonly_tools={"slow"},
        ),
        session_factory=lambda: factory(session),
    )
    task = asyncio.create_task(registry.call_tool("slow__slow", {}, ToolContext(registry)))
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await bridge.close()
