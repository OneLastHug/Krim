import asyncio
from dataclasses import replace

import httpx
import pytest
from alembic import command
from mcp.types import CallToolResult, ListToolsResult, Tool
from pydantic import SecretStr
from sqlalchemy import text

from krim_agent.api import create_app
from krim_agent.mcp import (
    MCPControlRequest,
    MCPManager,
    MCPSelectionRequest,
    MCPServerConfig,
    TrustedMCPServer,
)
from krim_agent.persistence import Database
from krim_agent.persistence.migration import migration_config
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.sdk import PythonAI
from krim_agent.tools import ToolContext, ToolRegistry


class Session:
    def __init__(self, label="one"):
        self.label, self.calls = label, []

    async def initialize(self):
        return None

    async def list_tools(self, *, params=None):
        return ListToolsResult(
            tools=[Tool(name="echo", inputSchema={"type": "object", "properties": {}})]
        )

    async def call_tool(self, name, arguments):
        self.calls.append((name, arguments))
        return CallToolResult(content=[], structuredContent={"label": self.label})


async def session_factory(label="one"):
    return Session(label)


def trusted(name="demo", label="one", *, auto_connect=False):
    return TrustedMCPServer(
        MCPServerConfig(name=name, transport="stdio", command="trusted", readonly_tools={"echo"}),
        version="v1",
        display_name="Demo",
        auto_connect=auto_connect,
        session_factory=lambda: session_factory(label),
    )


async def make_manager(tmp_path, *, auto_connect=False):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'mcp.sqlite'}")
    await database.initialize()
    manager = MCPManager(database, ToolRegistry())
    manager.register(trusted(auto_connect=auto_connect))
    return database, manager


async def test_host_registration_public_hash_cas_restart_and_secret_free_metadata(tmp_path):
    database, manager = await make_manager(tmp_path)
    try:
        listed = await manager.list()
        assert len(listed) == 1 and listed[0].available and listed[0].profile is None
        digest = listed[0].resource_hash
        selected = await manager.configure(
            "demo", MCPSelectionRequest(expected_revision=0, resource_hash=digest, enabled=True)
        )
        assert selected.profile.revision == 1 and selected.profile.enabled
        with pytest.raises(ServiceError) as conflict:
            await manager.configure(
                "demo",
                MCPSelectionRequest(expected_revision=0, resource_hash=digest, enabled=False),
            )
        assert conflict.value.code == "mcp_profile_conflict"
        await manager.connect("demo", MCPControlRequest(expected_revision=1, resource_hash=digest))
        ready = await manager.wait("demo")
        assert ready.connection.status == "ready"
        await manager.close()
    finally:
        await database.close()
    reopened = Database(f"sqlite+aiosqlite:///{tmp_path / 'mcp.sqlite'}")
    manager2 = MCPManager(reopened, ToolRegistry())
    manager2.register(trusted())
    try:
        await reopened.initialize()
        saved = await manager2.list()
        assert saved[0].profile.revision == 1 and saved[0].profile.enabled
        async with reopened.engine.connect() as connection:
            raw = str(
                (
                    await connection.execute(
                        text("SELECT public_metadata FROM mcp_resource_profiles")
                    )
                ).scalar()
            )
            assert "trusted" not in raw and "command" not in raw
    finally:
        await manager2.close()
        await reopened.close()


async def test_enable_disabled_host_or_changed_resource_is_rejected_without_connect(tmp_path):
    database, manager = await make_manager(tmp_path)
    try:
        view = (await manager.list())[0]
        with pytest.raises(ServiceError) as error:
            await manager.connect(
                "demo", MCPControlRequest(expected_revision=1, resource_hash=view.resource_hash)
            )
        assert error.value.code == "mcp_resource_disabled"
        await manager.configure(
            "demo",
            MCPSelectionRequest(
                expected_revision=0, resource_hash=view.resource_hash, enabled=True
            ),
        )
        with pytest.raises(ServiceError) as changed:
            await manager.connect(
                "demo", MCPControlRequest(expected_revision=1, resource_hash="0" * 64)
            )
        assert changed.value.code == "mcp_resource_changed"
    finally:
        await manager.close()
        await database.close()


async def test_profile_disable_stops_connection_and_future_call_is_denied(tmp_path):
    database, manager = await make_manager(tmp_path)
    try:
        view = (await manager.list())[0]
        digest = view.resource_hash
        await manager.configure(
            "demo", MCPSelectionRequest(expected_revision=0, resource_hash=digest, enabled=True)
        )
        await manager.connect("demo", MCPControlRequest(expected_revision=1, resource_hash=digest))
        await manager.wait("demo")
        assert manager.registry.definitions
        disabled = await manager.configure(
            "demo", MCPSelectionRequest(expected_revision=1, resource_hash=digest, enabled=False)
        )
        assert disabled.profile.revision == 2 and disabled.connection.status == "closed"
        assert not manager.registry.definitions
    finally:
        await manager.close()
        await database.close()


async def test_auto_connect_only_uses_persisted_enabled_same_fingerprint(tmp_path):
    database, manager = await make_manager(tmp_path, auto_connect=True)
    try:
        view = (await manager.list())[0]
        await manager.configure(
            "demo",
            MCPSelectionRequest(
                expected_revision=0, resource_hash=view.resource_hash, enabled=True
            ),
        )
        await manager.initialize()
        assert (await manager.view("demo")).connection.status == "ready"
    finally:
        await manager.close()
        await database.close()


async def test_persistent_profile_empty_table_can_downgrade_but_nonempty_refuses(tmp_path):
    database, manager = await make_manager(tmp_path)
    try:
        async with database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(migration_config(sync), "0032_oauth_states")
            )
            await connection.run_sync(lambda sync: command.upgrade(migration_config(sync), "head"))
        view = (await manager.list())[0]
        await manager.configure(
            "demo",
            MCPSelectionRequest(
                expected_revision=0, resource_hash=view.resource_hash, enabled=True
            ),
        )
        with pytest.raises(RuntimeError, match="MCP引用选择"):
            async with database.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(migration_config(sync), "0032_oauth_states")
                )
    finally:
        await manager.close()
        await database.close()


async def test_api_rejects_unknown_configuration_and_keeps_same_authorization(runtime):
    runtime.settings.api_key = SecretStr("management-test-key")
    app = create_app(runtime=runtime, mcp_servers=[trusted()])
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api,
    ):
        assert (await api.get("/v1/mcp/servers")).status_code == 401
        api.headers["Authorization"] = "Bearer management-test-key"
        view = (await api.get("/v1/mcp/servers")).json()[0]
        assert "command" not in str(view) and "trusted" not in str(view)
        refused = await api.put(
            "/v1/mcp/servers/demo",
            json={
                "expected_revision": 0,
                "resource_hash": view["resource_hash"],
                "enabled": True,
                "command": "injected",
                "url": "https://outside.invalid",
            },
        )
        assert refused.status_code == 422
        payload = {"expected_revision": 0, "resource_hash": view["resource_hash"], "enabled": True}
        assert (await api.put("/v1/mcp/servers/demo", json=payload)).status_code == 200
        control = {"expected_revision": 1, "resource_hash": view["resource_hash"]}
        assert (await api.post("/v1/mcp/servers/demo/connect", json=control)).status_code == 202
        ready = (await api.post("/v1/mcp/servers/demo/wait", params={"timeout_seconds": 3})).json()
        assert ready["connection"]["status"] == "ready"
        assert (
            app.state.agents.resources.tool_versions["demo__echo"] == "mcp-" + view["resource_hash"]
        )
        assert (await api.post("/v1/mcp/servers/demo/disconnect", json=control)).status_code == 200
        assert not app.state.mcp.registry.definitions or "demo__echo" not in {
            d.name for d in app.state.mcp.registry.definitions
        }
        foreign = await api.put(
            "/v1/mcp/servers/demo", json=payload, headers={"Origin": "https://foreign.invalid"}
        )
        assert foreign.status_code == 403


async def test_profile_store_cas_and_scope_are_real_across_independent_connections(tmp_path):
    first, one = await make_manager(tmp_path)
    second, two = await make_manager(tmp_path)
    other = MCPManager(second, ToolRegistry(), scope="other")
    other.register(trusted())
    try:
        view = (await one.list())[0]
        request = MCPSelectionRequest(
            expected_revision=0, resource_hash=view.resource_hash, enabled=True
        )
        result = await asyncio.gather(
            one.configure("demo", request), two.configure("demo", request), return_exceptions=True
        )
        assert (
            sum(
                isinstance(value, ServiceError) and value.code == "mcp_profile_conflict"
                for value in result
            )
            == 1
        )
        assert (await one.view("demo")).profile.revision == 1
        assert (await other.view("demo")).profile is None
    finally:
        await one.close()
        await two.close()
        await other.close()
        await first.close()
        await second.close()


async def test_secret_rotation_and_set_order_keep_hash_and_private_values_never_persist(tmp_path):
    database, manager = await make_manager(tmp_path)
    try:
        original = MCPServerConfig(
            name="private",
            transport="stdio",
            command="private-command",
            cwd="/private/workspace",
            args=["private-argument"],
            env={"Z": "private-environment", "A": "secret-first"},
            headers={"X-Key": SecretStr("secret-header")},
            readonly_tools={"echo", "other"},
            allowed_hosts={"z.example", "a.example"},
        )
        manager.register(TrustedMCPServer(original, "1"))
        first = await manager.view("private")
        second_manager = MCPManager(database, ToolRegistry(), scope="hash-test")
        changed = original.model_copy(deep=True)
        changed.headers = {"X-Key": SecretStr("rotated-header")}
        changed.env = {"A": "changed-secret", "Z": "new-environment"}
        changed.readonly_tools = {"other", "echo"}
        changed.allowed_hosts = {"a.example", "z.example"}
        second_manager.register(TrustedMCPServer(changed, "1"))
        assert (await second_manager.view("private")).resource_hash == first.resource_hash
        await manager.configure(
            "private",
            MCPSelectionRequest(
                expected_revision=0, resource_hash=first.resource_hash, enabled=False
            ),
        )
        raw = (tmp_path / "mcp.sqlite").read_bytes()
        for secret in (
            "private-command",
            "private-argument",
            "private-environment",
            "secret-first",
            "secret-header",
            "/private/workspace",
        ):
            assert secret.encode() not in raw and secret not in first.model_dump_json()
        await second_manager.close()
    finally:
        await manager.close()
        await database.close()


async def test_restart_with_changed_or_missing_host_keeps_old_reference_without_connecting(
    tmp_path,
):
    database, manager = await make_manager(tmp_path)
    try:
        view = await manager.view("demo")
        await manager.configure(
            "demo",
            MCPSelectionRequest(
                expected_revision=0, resource_hash=view.resource_hash, enabled=True
            ),
        )
        changed = MCPManager(database, ToolRegistry())
        changed.register(replace(trusted(auto_connect=True), version="v2"))
        await changed.initialize()
        current = await changed.view("demo")
        assert current.profile.resource_hash == view.resource_hash
        assert current.resource_hash != view.resource_hash and not changed.registry.definitions
        with pytest.raises(ServiceError) as error:
            await changed.connect(
                "demo", MCPControlRequest(expected_revision=1, resource_hash=view.resource_hash)
            )
        assert error.value.code == "mcp_resource_changed"
        missing = MCPManager(database, ToolRegistry())
        saved = await missing.view("demo")
        assert not saved.available and saved.resource_hash is None and saved.profile.enabled
        disabled = await missing.configure(
            "demo",
            MCPSelectionRequest(
                expected_revision=1, resource_hash=view.resource_hash, enabled=False
            ),
        )
        assert not disabled.profile.enabled
        await changed.close()
        await missing.close()
    finally:
        await manager.close()
        await database.close()


async def test_dispatch_guard_denies_external_profile_revocation_before_remote_call(tmp_path):
    database, manager = await make_manager(tmp_path)
    remote = Session()

    async def fixed():
        return remote

    manager.register(replace(trusted("fixed"), session_factory=fixed))
    try:
        view = await manager.view("fixed")
        await manager.configure(
            "fixed",
            MCPSelectionRequest(
                expected_revision=0, resource_hash=view.resource_hash, enabled=True
            ),
        )
        await manager.connect(
            "fixed", MCPControlRequest(expected_revision=1, resource_hash=view.resource_hash)
        )
        await manager.wait("fixed")
        saved = (await manager.view("fixed")).profile
        await manager.store.put(
            "fixed",
            MCPSelectionRequest(
                expected_revision=1, resource_hash=saved.resource_hash, enabled=False
            ),
            saved.resource_version,
            saved.description,
        )
        denied = await manager.registry.invoke("fixed__echo", {}, ToolContext(manager.registry))
        assert denied.is_error and denied.code == "mcp_resource_disabled" and remote.calls == []
    finally:
        await manager.close()
        await database.close()


async def test_cancelled_wait_is_readonly_and_repeated_connect_joins_original_discovery(tmp_path):
    database, manager = await make_manager(tmp_path)
    entered, release = asyncio.Event(), asyncio.Event()
    calls = 0

    class Slow(Session):
        async def initialize(self):
            nonlocal calls
            calls += 1
            entered.set()
            await release.wait()

    async def slow():
        return Slow()

    manager.register(replace(trusted("slow"), session_factory=slow))
    try:
        view = await manager.view("slow")
        await manager.configure(
            "slow",
            MCPSelectionRequest(
                expected_revision=0, resource_hash=view.resource_hash, enabled=True
            ),
        )
        control = MCPControlRequest(expected_revision=1, resource_hash=view.resource_hash)
        await manager.connect("slow", control)
        await asyncio.wait_for(entered.wait(), 3)
        await manager.connect("slow", control)
        waiting = asyncio.create_task(manager.wait("slow"))
        await asyncio.sleep(0)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        assert calls == 1 and not manager._jobs["slow"][2].done()
        release.set()
        assert (await manager.wait("slow")).connection.status == "ready"
    finally:
        release.set()
        await manager.close()
        await database.close()


async def test_sdk_uses_same_tools_versions_and_old_agent_does_not_rebind_after_reconnect():
    class Provider:
        name = "mcp-model-loop"

        async def stream(self, messages, tools):
            if messages[-1].role == "tool":
                yield ModelText(text="done")
            else:
                yield ModelToolCall(name="demo__echo", arguments={})

    async with await PythonAI.create(provider=Provider()) as ai:
        manager = ai.mcp()
        manager.register(trusted())
        view = await manager.view("demo")
        await manager.configure(
            "demo",
            MCPSelectionRequest(
                expected_revision=0, resource_hash=view.resource_hash, enabled=True
            ),
        )
        control = MCPControlRequest(expected_revision=1, resource_hash=view.resource_hash)
        await manager.connect("demo", control)
        await manager.wait("demo")
        original = await ai.agent()
        first = await original.generate_text("call")
        assert not first.steps[0].tool_results[0].is_error
        await manager.disconnect("demo", control)
        await manager.connect("demo", control)
        await manager.wait("demo")
        stale = await original.generate_text("call again")
        assert stale.steps[0].tool_results[0].code == "mcp_request_failed"
        fresh = await ai.agent()
        assert not (await fresh.generate_text("call")).steps[0].tool_results[0].is_error
