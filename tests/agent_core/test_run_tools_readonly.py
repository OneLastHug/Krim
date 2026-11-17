"""Planning trusts explicit connection configuration, never remote risk hints alone."""

from dataclasses import replace

import pytest

from krim_agent.contracts import RunEvent, ServiceError
from krim_agent.interaction.service import plan_allows
from krim_agent.mcp import MCPServerConfig, MCPTool
from krim_agent.runtime.run_tools import open_run_tools, trusted_readonly_names
from krim_agent.security import LOCAL
from krim_agent.tools.core import Invocation, ToolSource
from krim_agent.tools.executor import ToolContext, ToolExecutor
from krim_agent.tools.mcp import mcp_tools
from krim_agent.tools.registry import ToolRegistry
from krim_agent.tools.runtime import ToolRuntime
from krim_agent.tools.session import SessionToolScope, ToolBundle


class Connection:
    def __init__(self, *, readonly=("read",)):
        self.config = MCPServerConfig(
            name="remote",
            transport="stdio",
            command="fixture",
            selected_tools=("read", "write"),
            readonly_tools=frozenset(readonly),
        )
        self.closed = False
        self.supports_resources = False
        self.calls = []
        self.tools = tuple(
            MCPTool(
                name,
                "Remote annotations claim read only",
                {"type": "object"},
                {"type": "object"},
                True,
            )
            for name in ("read", "write")
        )

    async def call_tool(self, name, args):
        self.calls.append(name)
        return {"result": name}

    async def close(self):
        self.closed = True


def test_trust_requires_selected_configuration_source_and_full_alias_chain():
    connection = Connection()
    read, remote_hint_only = mcp_tools(connection)
    alias = replace(read, name="web_search", binding_target=read)
    # Even a read risk is insufficient when trusted config did not mark it read-only.
    assert remote_hint_only.risk == "read"
    names = trusted_readonly_names((alias, remote_hint_only), (connection,))
    assert names == frozenset({"web_search", "remote__read"})
    assert plan_allows(alias, names) and plan_allows(read, names)
    assert not plan_allows(remote_hint_only, names)
    assert not trusted_readonly_names((alias,), ())
    assert not trusted_readonly_names((replace(read, risk="write"),), (connection,))
    drift = replace(read, source=ToolSource("mcp", "remote", "read", "0" * 64))
    assert not trusted_readonly_names((drift,), (connection,))
    unselected = replace(
        read,
        source=ToolSource(
            "mcp",
            "remote",
            "not_selected",
            connection.config.configuration_digest,
        ),
    )
    assert not trusted_readonly_names((unselected,), (connection,))
    connection.closed = True
    assert not trusted_readonly_names((alias, read), (connection,))


async def test_run_trust_includes_account_owner_configuration_and_guard_still_applies():
    scope = SessionToolScope(LOCAL, "local", "session", "turn", "run")
    host_connection = Connection()
    account_connection = Connection()
    account_connection.config = account_connection.config.model_copy(
        update={"timeout_seconds": 20.0},
    )
    host_specs = mcp_tools(host_connection)
    account_specs = mcp_tools(account_connection, namespace="account")
    approval_names = []

    async def resolve(call):
        return scope

    async def host_policy(call):
        return "allow"

    async def account_policy(call):
        return "ask"

    class Account:
        bundle = ToolBundle(account_specs, account_policy)
        connections = (account_connection,)

        async def close(self):
            await account_connection.close()

    class AccountService:
        async def configurations_for_turn(self, turn_id):
            assert turn_id == "turn"
            return (account_connection.config,)

        async def open_bundle(self, configs, resolve, scope=None):
            return Account()

    tools = None

    async def guard(call):
        spec = tools.executor.registry.get(call.name)
        return "allow" if plan_allows(spec, tools.trusted_readonly_names) else "deny"

    async def emit(kind, data):
        return RunEvent(run_id="run", seq=1, event_id="1", kind=kind, data=data)

    async def approve(call, seconds):
        approval_names.append(call.name)
        return True

    host = ToolRuntime(ToolRegistry(host_specs), host_policy, (host_connection,))
    tools = await open_run_tools(
        ToolExecutor(ToolRegistry(()), None),
        scope=scope,
        resolve=resolve,
        host=host,
        account_mcp=AccountService(),
        guard=guard,
    )
    try:
        assert tools.trusted_readonly_names == frozenset(
            {
                "remote__read",
                "account__remote__read",
            }
        )
        for name in (
            "remote__read",
            "account__remote__read",
            "remote__write",
            "account__remote__write",
            "read_mcp_resource",
        ):
            spec = tools.executor.registry.get(name)
            arguments = (
                '{"server":"host:remote","uri":"doc://a"}' if name == "read_mcp_resource" else "{}"
            )
            call = Invocation("run", "call", "span", 0, name, arguments, spec.fingerprint)
            result = await tools.executor.execute(call, ToolContext(emit, approve, None))
            assert result.status == ("ok" if name.endswith("__read") else "denied")
        assert host_connection.calls == ["read"]
        assert account_connection.calls == ["read"]
        # Planning trust never bypasses the personal connection's ask policy.
        assert approval_names == ["account__remote__read"]
        account_connection.closed = True
        assert tools.trusted_readonly_names == frozenset({"remote__read"})
    finally:
        await tools.close()
    assert not host_connection.closed
    with pytest.raises(ServiceError, match="closed"):
        _ = tools.trusted_readonly_names
