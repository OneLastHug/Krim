import asyncio

import pytest
from mcp.types import CallToolResult, ListToolsResult, Tool

from krim_agent.mcp import MCPBridge, MCPConnectionError, MCPServerConfig
from krim_agent.tools import ToolContext, ToolError, ToolRegistry, tool


class Session:
    def __init__(self, label="old", names=("echo",)):
        self.label, self.names = label, names
        self.calls = []
        self.entered, self.cleanup, self.release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def initialize(self):
        return None

    async def list_tools(self, *, params=None):
        return ListToolsResult(
            tools=[
                Tool(name=name, inputSchema={"type": "object", "properties": {}})
                for name in self.names
            ]
        )

    async def call_tool(self, name, arguments):
        self.calls.append((name, arguments))
        return CallToolResult(content=[], structuredContent={"server": self.label})


async def available(session):
    return session


def config(**changes):
    values = {"name": "demo", "transport": "stdio", "command": "fake", "readonly_tools": {"echo"}}
    values.update(changes)
    return MCPServerConfig(**values)


async def test_remove_reconnect_uses_new_definition_and_old_frozen_call_never_reroutes():
    registry, old, new = ToolRegistry(), Session(), Session("new")
    bridge = MCPBridge(registry)
    try:
        await bridge.add_server(config(), session_factory=lambda: available(old))
        frozen = registry.select(["demo__echo"])
        original = registry.get("demo__echo")
        assert (await frozen.invoke("demo__echo", {}, ToolContext(frozen))).data == {
            "server": "old"
        }
        assert (await bridge.remove_server("demo")).status == "closed"
        assert not registry.definitions and not bridge.tools()
        assert (await bridge.remove_server("demo")).status == "closed"
        await bridge.add_server(config(), session_factory=lambda: available(new))
        assert registry.get("demo__echo") is not original
        stale = await frozen.invoke("demo__echo", {}, ToolContext(frozen))
        assert stale.is_error and stale.code == "mcp_request_failed"
        assert new.calls == []
        assert (await registry.invoke("demo__echo", {}, ToolContext(registry))).data == {
            "server": "new"
        }
        assert len(old.calls) == len(new.calls) == 1
    finally:
        await bridge.close()
        await registry.close()


@pytest.mark.parametrize("kind", ["disabled", "failed"])
async def test_failed_and_disabled_slots_can_be_explicitly_removed_and_recreated(kind):
    class FailingSession(Session):
        async def initialize(self):
            raise ValueError("remote fake secret")

    registry, bridge = ToolRegistry(), None
    bridge = MCPBridge(registry)
    try:
        failed = await bridge.add_server(
            config(enabled=kind != "disabled"), session_factory=lambda: available(FailingSession())
        )
        assert failed.status == kind and "remote fake secret" not in str(failed)
        await bridge.remove_server("demo")
        ready = await bridge.add_server(config(), session_factory=lambda: available(Session()))
        assert ready.status == "ready"
    finally:
        await bridge.close()


@pytest.mark.parametrize("operation", ["remove", "close", "cancel"])
async def test_stop_during_discovery_cannot_publish_late_tools(operation):
    class SlowDiscovery(Session):
        async def list_tools(self, *, params=None):
            self.entered.set()
            await asyncio.Event().wait()

    registry, remote = ToolRegistry(), SlowDiscovery()
    bridge = MCPBridge(registry)
    started = asyncio.create_task(
        bridge.add_server(config(), session_factory=lambda: available(remote))
    )
    try:
        await asyncio.wait_for(remote.entered.wait(), 3)
        assert bridge.statuses()[0].status == "connecting"
        if operation == "remove":
            await bridge.remove_server("demo")
        elif operation == "close":
            await bridge.close()
        else:
            started.cancel()
        result = (await asyncio.gather(started, return_exceptions=True))[0]
        if operation == "cancel":
            assert isinstance(result, asyncio.CancelledError)
        else:
            assert result.status == "closed"
        assert not registry.definitions and not bridge.tools()
        assert bridge.statuses()[0].status == "closed"
        if operation != "close":
            await bridge.remove_server("demo")
            assert (
                await bridge.add_server(config(), session_factory=lambda: available(Session()))
            ).status == "ready"
        else:
            with pytest.raises(MCPConnectionError, match="已关闭"):
                await bridge.add_server(config(), session_factory=lambda: available(Session()))
    finally:
        await bridge.close()
        await asyncio.gather(started, return_exceptions=True)


async def test_mutating_caller_configuration_during_discovery_cannot_grant_readonly():
    class GateDiscovery(Session):
        async def list_tools(self, *, params=None):
            self.entered.set()
            await self.release.wait()
            return await super().list_tools(params=params)

    supplied, remote, registry = config(readonly_tools=set()), GateDiscovery(), ToolRegistry()
    bridge = MCPBridge(registry)
    started = asyncio.create_task(
        bridge.add_server(supplied, session_factory=lambda: available(remote))
    )
    try:
        await asyncio.wait_for(remote.entered.wait(), 3)
        supplied.readonly_tools.add("echo")
        supplied.name, supplied.namespace, supplied.command = "changed", "changed", "changed"
        supplied.request_timeout_seconds = 120
        remote.release.set()
        result = await started
        assert result.name == "demo" and result.tools == ["demo__echo"]
        denied = await registry.invoke("demo__echo", {}, ToolContext(registry))
        assert denied.code == "permission_denied" and not remote.calls
    finally:
        remote.release.set()
        await asyncio.gather(started, return_exceptions=True)
        await bridge.close()


async def test_old_cleanup_preserves_host_replacement_definition():
    registry, bridge = ToolRegistry(), None
    bridge = MCPBridge(registry)

    @tool(name="demo__echo", risk="read")
    async def replacement() -> str:
        return "trusted host"

    try:
        await bridge.add_server(config(), session_factory=lambda: available(Session()))
        original = registry.get("demo__echo")
        assert registry.unregister("demo__echo", expected=original)
        registry.register(replacement)
        assert not registry.unregister("demo__echo", expected=original)
        await bridge.remove_server("demo")
        assert registry.get("demo__echo") is replacement
        assert (
            await registry.invoke("demo__echo", {}, ToolContext(registry))
        ).data == "trusted host"
    finally:
        await bridge.close()


async def test_registration_failure_after_partial_publication_rolls_back_owned_definitions():
    class FailingRegistry(ToolRegistry):
        def register(self, definition):
            result = super().register(definition)
            if definition.name.endswith("__second"):
                raise ValueError("injected publication failure")
            return result

    registry = FailingRegistry()
    bridge = MCPBridge(registry)
    try:
        status = await bridge.add_server(
            config(), session_factory=lambda: available(Session(names=("first", "second")))
        )
        assert status.status == "failed"
        assert not registry.definitions and not bridge.tools()
        good = await bridge.add_server(
            config(name="good"), session_factory=lambda: available(Session())
        )
        assert good.status == "ready"
    finally:
        await bridge.close()


@pytest.mark.parametrize("operation", ["connection", "remove", "bridge"])
async def test_owner_callback_cannot_close_itself_even_through_child_task(operation):
    registry = ToolRegistry()
    bridge = MCPBridge(registry)

    class SelfClosing(Session):
        async def call_tool(self, name, arguments):
            if operation == "connection":
                action = bridge._connections["demo"].close()
            elif operation == "remove":
                action = bridge.remove_server("demo")
            else:
                action = bridge.close()
            with pytest.raises(MCPConnectionError) as error:
                await asyncio.create_task(action)
            assert error.value.code == "mcp_self_close"
            return await super().call_tool(name, arguments)

    try:
        await bridge.add_server(config(), session_factory=lambda: available(SelfClosing()))
        result = await registry.invoke("demo__echo", {}, ToolContext(registry))
        assert result.data == {"server": "old"} and not result.is_error
        assert bridge.statuses()[0].status == "ready"
        assert not bridge._closed and not bridge._retiring
    finally:
        await bridge.close()


@pytest.mark.parametrize("operation", ["remove", "close"])
async def test_repeated_cancellation_waits_for_same_owner_and_blocks_same_name_until_reaped(
    operation,
):
    class SlowStop(Session):
        async def call_tool(self, name, arguments):
            self.entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                self.cleanup.set()
                await self.release.wait()

    registry, remote = ToolRegistry(), SlowStop()
    bridge = MCPBridge(registry)
    closing = None
    invocation = None
    try:
        await bridge.add_server(config(), session_factory=lambda: available(remote))
        connection = bridge._connections["demo"]
        invocation = asyncio.create_task(registry.invoke("demo__echo", {}, ToolContext(registry)))
        await asyncio.wait_for(remote.entered.wait(), 3)
        closing = asyncio.create_task(
            bridge.remove_server("demo") if operation == "remove" else bridge.close()
        )
        await asyncio.wait_for(remote.cleanup.wait(), 3)
        assert bridge.statuses()[0].status == "closing" and not registry.definitions
        with pytest.raises(MCPConnectionError):
            await bridge.add_server(config(), session_factory=lambda: available(Session("new")))
        closing.cancel()
        await asyncio.sleep(0)
        closing.cancel()
        remote.release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(closing, 3)
        await asyncio.gather(invocation, return_exceptions=True)
        assert connection._owner.done() and not connection._requests and not connection.connected
        assert not bridge._retiring and not bridge._connections
        assert bridge.statuses()[0].status == "closed"
        if operation == "remove":
            assert (
                await bridge.add_server(config(), session_factory=lambda: available(Session("new")))
            ).status == "ready"
    finally:
        remote.release.set()
        if closing is not None:
            await asyncio.gather(closing, return_exceptions=True)
        if invocation is not None:
            await asyncio.gather(invocation, return_exceptions=True)
        await bridge.close()


async def test_unknown_remove_is_clear_error_and_registry_missing_remove_is_idempotent():
    bridge = MCPBridge(ToolRegistry())
    with pytest.raises(MCPConnectionError) as error:
        await bridge.remove_server("missing")
    assert error.value.code == "mcp_server_not_found"
    assert not bridge.registry.unregister("missing")
    with pytest.raises(ToolError):
        bridge.registry.get("missing")
    await bridge.close()
