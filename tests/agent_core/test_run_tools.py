"""Single-run composition, visibility, ownership and checkpoint contracts."""

import json
from dataclasses import replace

import pytest

from krim_agent.contracts import RunEvent, ServiceError, StrictModel
from krim_agent.mcp import MCPServerConfig
from krim_agent.runtime.run_tools import open_run_tools
from krim_agent.security import Membership, PrincipalContext
from krim_agent.tools.catalog_search import ToolSearchInput
from krim_agent.tools.core import Invocation, ToolSource, ToolSpec
from krim_agent.tools.executor import ToolContext, ToolExecutor
from krim_agent.tools.mcp_resources import ReadResourceInput
from krim_agent.tools.registry import ToolRegistry
from krim_agent.tools.runtime import ToolRuntime, ToolRuntimeConfig
from krim_agent.tools.session import SessionToolScope, ToolBundle


class Args(StrictModel):
    value: str = ""


async def native(arguments):
    return {"value": arguments.value}


def native_spec():
    return ToolSpec("local_read", Args, native, {"type": "object"}, "1", "a" * 64)


def external_spec(name, calls, *, risk="write"):
    async def execute(call, arguments):
        calls.append((call.name, arguments.value))
        return {"value": arguments.value}

    return ToolSpec(
        name,
        Args,
        native,
        {"type": "object"},
        "1",
        "a" * 64,
        risk=risk,
        source=ToolSource("mcp", "remote", name, "b" * 64),
        external_executor=execute,
        description="Search remote documents",
    )


def owner(principal="alice", run="run"):
    return SessionToolScope(
        PrincipalContext(principal, 1, (Membership(workspace_id="local"),)),
        "local",
        "session",
        "turn",
        run,
    )


def invoke(spec, arguments=None):
    return Invocation(
        "run", "call", "span", 0, spec.name, json.dumps(arguments or {}), spec.fingerprint
    )


async def emit(kind, data):
    return RunEvent(run_id="run", seq=1, event_id="1", kind=kind, data=data)


async def approve(call, seconds):
    return True


class Connection:
    def __init__(self):
        self.config = MCPServerConfig(name="remote", transport="stdio", command="fixture")
        self.closed = False
        self.supports_resources = True
        self.reads = 0
        self.closes = 0

    async def read_resource(self, uri):
        self.reads += 1
        return {"contents": [{"uri": uri, "mimeType": "text/plain", "text": "x" * 100000}]}

    async def close(self):
        self.closes += 1
        self.closed = True


def test_runtime_config_accepts_trusted_absolute_web_result_root(tmp_path):
    config_file = tmp_path / "tools.json"
    root = tmp_path / "saved-web"
    config_file.write_text(json.dumps({"web": {"result_root": str(root)}}), encoding="utf-8")
    config_file.chmod(0o600)
    config = ToolRuntimeConfig.load(config_file, environment={})
    assert config.web_result_root == root
    config_file.write_text('{"web":{"result_root":"relative"}}', encoding="utf-8")
    with pytest.raises(ServiceError, match="invalid_input"):
        ToolRuntimeConfig.load(config_file, environment={})


async def test_run_checkpoint_contains_synchronous_web_reference_state_and_rolls_back():
    scope = owner()

    async def resolve(call):
        return scope

    class WebState:
        def __init__(self):
            self.data = {"version": "web/1", "refs": ["opaque"]}

        def snapshot(self, actual_scope):
            assert actual_scope == "run"
            return json.loads(json.dumps(self.data))

        def restore(self, actual_scope, state):
            assert actual_scope == "run"
            if state.get("corrupt"):
                raise ServiceError("not_found", 404)
            self.data = json.loads(json.dumps(state))

    web = WebState()
    host = ToolRuntime(ToolRegistry(()), None, web=web)
    tools = await open_run_tools(ToolExecutor(ToolRegistry(()), None), scope=scope,
                                 resolve=resolve, host=host)
    try:
        state = json.loads(json.dumps(tools.checkpoint()))
        assert state["web"] == web.data
        web.data = {"version": "web/1", "refs": []}
        tools.restore(state)
        assert web.data["refs"] == ["opaque"]
        before = tools.checkpoint()
        bad = json.loads(json.dumps(before))
        bad["web"]["corrupt"] = True
        with pytest.raises(ServiceError, match="not_found"):
            tools.restore(bad)
        assert tools.checkpoint() == before
    finally:
        await tools.close()


async def test_base_already_contains_exact_host_specs_uses_scoped_replacement():
    scope = owner()
    calls, checks, policies, hooks = [], [], [], []
    spec = external_spec("remote__search", calls)

    async def resolve(call):
        checks.append(call.name)
        return scope

    async def base_policy(call):
        policies.append(("base", call.name))
        return "deny" if call.name == spec.name else "allow"

    async def host_policy(call):
        policies.append(("host", call.name))
        return "allow"

    async def pre(call):
        hooks.append("pre")

    async def post(call, result):
        hooks.append("post")

    base = ToolExecutor(ToolRegistry((native_spec(), spec)), base_policy, pre, post)
    host = ToolRuntime(ToolRegistry((spec,)), host_policy)
    tools = await open_run_tools(base, scope=scope, resolve=resolve, host=host)
    try:
        effective = tools.executor.registry.get(spec.name)
        assert effective.fingerprint == spec.fingerprint
        assert effective.external_executor is not spec.external_executor
        result = await tools.executor.execute(
            invoke(effective, {"value": "ok"}), ToolContext(emit, approve, None)
        )
        assert result.status == "ok" and calls == [(spec.name, "ok")]
        assert policies == [("host", spec.name)]
        assert checks == [spec.name] * 3 and hooks == ["pre", "post"]
        assert (
            len([value for value, _ in tools.executor.registry._frozen if value.name == spec.name])
            == 1
        )
    finally:
        await tools.close()
    changed = ToolRuntime(ToolRegistry((replace(spec, description="changed"),)), host_policy)
    with pytest.raises(ServiceError, match="tool_binding_changed"):
        await open_run_tools(base, scope=scope, resolve=resolve, host=changed)
    assert base.registry.get(spec.name) is spec


async def test_large_authorized_catalog_checkpoint_does_not_send_full_schema_set():
    scope = owner()

    async def resolve(call):
        return scope

    specs = tuple(
        replace(external_spec(f"remote__tool_{index}", []), description="x" * 7000)
        for index in range(155)
    )
    host = ToolRuntime(ToolRegistry(specs), None)
    assert len(json.dumps(host.registry.schemas())) > 1024 * 1024
    tools = await open_run_tools(
        ToolExecutor(ToolRegistry(()), None), scope=scope, resolve=resolve, host=host
    )
    try:
        assert len(tools.executor.registry.schemas()) == 159  # 155 external + resources + search
        assert len(tools.schemas()) == 4
        state = json.loads(json.dumps(tools.checkpoint()))
        tools.restore(state)
    finally:
        await tools.close()


async def test_visibility_checkpoint_restore_and_borrowed_host_lifetime():
    scope = owner()
    calls, checks, policies = [], [], []

    async def resolve(call):
        checks.append(call.name)
        return scope

    async def policy(call):
        policies.append(call.name)
        return "ask"

    hooks = []

    async def pre(call):
        hooks.append("pre")

    async def post(call, result):
        hooks.append("post")

    base = ToolExecutor(ToolRegistry((native_spec(),)), policy, pre, post)
    connection = Connection()
    host = ToolRuntime(
        ToolRegistry((external_spec("remote__search", calls),)), policy, (connection,)
    )
    tools = await open_run_tools(base, scope=scope, resolve=resolve, host=host)
    try:
        names = {item["name"] for item in tools.schemas()}
        assert names == {
            "local_read",
            "tool_search",
            "list_mcp_resources",
            "list_mcp_resource_templates",
            "read_mcp_resource",
        }
        assert tools.executor.registry.get("remote__search") is not None
        search = tools.executor.registry.get("tool_search")
        result = await tools.executor.execute(
            invoke(search, {"names": ["remote__search"]}),
            ToolContext(emit, approve, None),
        )
        assert result.status == "ok"
        assert hooks == ["pre", "post"]
        assert "remote__search" in {item["name"] for item in tools.schemas()}
        assert calls == []  # Selection is not execution or approval bypass.
        page = await tools.resources.read(
            scope, ReadResourceInput(server="host:remote", uri="doc://a")
        )
        state = json.loads(json.dumps(tools.checkpoint()))
        assert connection.reads == 1
    finally:
        await tools.close()
    assert not connection.closed and connection.closes == 0
    restored = await open_run_tools(base, scope=scope, resolve=resolve, host=host)
    try:
        restored.restore(state)
        assert "remote__search" in {item["name"] for item in restored.schemas()}
        more = await restored.resources.read(
            scope,
            ReadResourceInput(
                server="host:remote",
                result_ref=page["result_ref"],
                offset=12000,
            ),
        )
        assert len(more["contents"][0]["text"]) == 12000 and connection.reads == 1
        remote = restored.executor.registry.get("remote__search")
        result = await restored.executor.execute(
            invoke(remote, {"value": "ok"}), ToolContext(emit, approve, None)
        )
        assert result.status == "ok" and calls == [("remote__search", "ok")]
        assert policies[-1] == "remote__search"
    finally:
        await restored.close()
    with pytest.raises(ServiceError, match="closed"):
        restored.schemas()


async def test_checkpoint_scope_schema_and_resource_corruption_are_rejected_atomically():
    scope = owner()

    async def resolve(call):
        return scope

    base = ToolExecutor(ToolRegistry((native_spec(),)), None)
    host = ToolRuntime(ToolRegistry((external_spec("remote__search", []),)), None, (Connection(),))
    tools = await open_run_tools(base, scope=scope, resolve=resolve, host=host)
    try:
        state = json.loads(json.dumps(tools.checkpoint()))
        tools.catalog.search(scope, ToolSearchInput(names=("remote__search",)))
        active = tools.schemas()
        bad = json.loads(json.dumps(state))
        bad["resources"]["bindings"]["host:remote"] = "changed"
        with pytest.raises(ServiceError, match="tool_binding_changed"):
            tools.restore(bad)
        assert tools.schemas() == active  # No partial catalog rollback on failure.
        other = await open_run_tools(base, scope=owner("bob"), resolve=resolve, host=host)
        try:
            with pytest.raises(ServiceError, match="tool_binding_changed"):
                other.restore(state)
        finally:
            await other.close()
        changed = ToolRuntime(ToolRegistry((external_spec("remote__different", []),)), None)
        drift = await open_run_tools(base, scope=scope, resolve=resolve, host=changed)
        try:
            with pytest.raises(ServiceError, match="tool_binding_changed"):
                drift.restore(state)
        finally:
            await drift.close()
    finally:
        await tools.close()


async def test_account_owned_connections_close_and_plan_guard_covers_external_write():
    scope = owner()
    calls, lookups, account_closes, guards = [], [], [], []
    connection = Connection()

    async def resolve(call):
        return scope

    async def policy(call):
        return "ask"

    class AccountBundle:
        bundle = ToolBundle((external_spec("account__remote__write", calls),), policy)
        connections = (connection,)

        async def close(self):
            account_closes.append(True)
            await connection.close()

    class AccountService:
        async def configurations_for_turn(self, turn_id):
            lookups.append(turn_id)
            return (connection.config,)

        async def open_bundle(self, configs, actual_resolve, scope=None):
            assert scope is not None and scope.run_id == "run"
            assert configs == (connection.config,)
            return AccountBundle()

    async def guard(call):
        guards.append(call.name)
        return "deny" if call.name == "account__remote__write" else "allow"

    base = ToolExecutor(ToolRegistry((native_spec(),)), policy)
    tools = await open_run_tools(
        base, scope=scope, resolve=resolve, account_mcp=AccountService(), guard=guard
    )
    try:
        spec = tools.executor.registry.get("account__remote__write")
        result = await tools.executor.execute(invoke(spec), ToolContext(emit, approve, None))
        assert result.status == "denied" and result.code == "permission_denied"
        assert calls == [] and guards == [spec.name]
        assert lookups == ["turn"]
    finally:
        await tools.close()
        await tools.close()
    assert account_closes == [True] and connection.closes == 1


async def test_close_releases_cached_resources_even_after_scope_cancellation():
    cancelled = False

    def check():
        if cancelled:
            raise ServiceError("cancelled")

    scope = replace(owner(), check=check)

    async def resolve(call):
        return scope

    host = ToolRuntime(ToolRegistry(()), None, (Connection(),))
    tools = await open_run_tools(
        ToolExecutor(ToolRegistry(()), None), scope=scope, resolve=resolve, host=host
    )
    await tools.resources.read(scope, ReadResourceInput(server="host:remote", uri="doc://a"))
    assert tools.resources._results
    cancelled = True
    with pytest.raises(ServiceError, match="cancelled"):
        tools.checkpoint()
    await tools.close()
    assert not tools.resources._results
    assert not host.connections[0].closed


async def test_scope_cancellation_and_duplicate_names_fail_without_leaking_account():
    scope = owner()
    cancelled = replace(scope, check=lambda: (_ for _ in ()).throw(ServiceError("cancelled")))

    async def resolve(call):
        return cancelled

    base = ToolExecutor(ToolRegistry(()), None)
    with pytest.raises(ServiceError, match="cancelled"):
        await open_run_tools(base, scope=cancelled, resolve=resolve)
    tools = await open_run_tools(base, scope=scope, resolve=resolve)
    try:
        search = tools.executor.registry.get("tool_search")
        result = await tools.executor.execute(invoke(search), ToolContext(emit, approve, None))
        assert result.code == "cancelled"
    finally:
        await tools.close()
