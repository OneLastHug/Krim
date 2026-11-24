"""Discovery and resources exercise public bundles without provider tool search."""

from dataclasses import replace

import pytest

from krim_agent.contracts import ServiceError, StrictModel, fingerprint
from krim_agent.mcp import MCPServerConfig
from krim_agent.security import Membership, PrincipalContext
from krim_agent.tools.catalog_search import ToolCatalogService, ToolSearchInput
from krim_agent.tools.core import Invocation, ToolSource, ToolSpec
from krim_agent.tools.mcp_resources import (
    ListResourcesInput,
    MCPResourceBinding,
    MCPResourceService,
    ReadResourceInput,
)
from krim_agent.tools.registry import ToolRegistry
from krim_agent.tools.runtime import ToolRuntime, ToolRuntimeConfig
from krim_agent.tools.session import SessionToolScope
from krim_agent.web_access import WebAccessConfig


class Args(StrictModel):
    value: str = ""


async def execute(arguments):
    return {"value": arguments.value}


async def external(invocation, arguments):
    return await execute(arguments)


def scope(principal="alice", session="session", run="run"):
    actor = PrincipalContext(principal, 1, (Membership(workspace_id="local"),))
    return SessionToolScope(actor, "local", session, "turn", run)


def tool(index):
    return ToolSpec(
        f"remote__search_{index}",
        Args,
        execute,
        {"type": "object"},
        "1",
        "a" * 64,
        description=f"Search document {index}",
        source=ToolSource("mcp", "remote", f"search_{index}", "b" * 64),
        external_executor=external,
    )


def invocation(spec):
    return Invocation("run", "call", "span", 0, spec.name, "{}", spec.fingerprint)


def test_catalog_beyond_old_limit_pagination_activation_and_snapshot():
    specs = tuple(tool(index) for index in range(70))
    service = ToolCatalogService(specs)
    owner = scope()
    assert service.active_specs(owner) == ()
    first = service.search(owner, ToolSearchInput(query="document", limit=11, activate=False))
    assert len(first["tools"]) == 11 and first["total"] == 70
    assert "input_schema" not in first["tools"][0]
    assert service.active_specs(owner) == ()
    names = [item["name"] for item in first["tools"]]
    cursor = first["next_cursor"]
    while cursor:
        page = service.search(owner, ToolSearchInput(query="document", limit=11, cursor=cursor))
        names.extend(item["name"] for item in page["tools"])
        cursor = page["next_cursor"]
    assert len(set(names)) == 70
    assert len(service.active_specs(owner)) == 59
    assert service.active_specs(scope("bob")) == ()
    chosen = service.search(owner, ToolSearchInput(names=(names[0],)))
    handle = chosen["tools"][0]["handle"]
    assert chosen["tools"][0]["input_schema"]["type"] == "object"
    restored = ToolCatalogService(specs)
    restored.restore(owner, service.snapshot(owner))
    assert restored.active_specs(owner) == service.active_specs(owner)
    with pytest.raises(ServiceError, match="tool_binding_changed"):
        restored.restore(scope("bob"), service.snapshot(owner))
    with pytest.raises(ServiceError, match="tool_binding_changed"):
        service.activate(owner, (handle[:-1] + "x",))
    with pytest.raises(ServiceError, match="invalid_cursor"):
        service.search(
            scope("bob"), ToolSearchInput(query="document", limit=11, cursor=first["next_cursor"])
        )


async def test_tool_search_executes_through_shared_executor_json_arguments():
    from krim_agent.contracts import RunEvent
    from krim_agent.tools.executor import ToolContext, ToolExecutor

    owner = scope()

    async def resolve(call):
        return owner

    service = ToolCatalogService((tool(0),))
    bundle = service.bundle(resolve)
    spec = bundle.specs[0]
    registry = ToolRegistry(bundle.specs)
    events = []

    async def emit(kind, data):
        events.append(kind)
        return RunEvent(
            run_id="run",
            seq=len(events),
            event_id=str(len(events)),
            kind=kind,
            data=data,
        )

    async def approve(call, seconds):
        pytest.fail("discovery does not request side-effect approval")

    call = replace(invocation(spec), arguments_json='{"names":["remote__search_0"]}')
    result = await ToolExecutor(registry, bundle.policy).execute(
        call,
        ToolContext(emit, approve, None),
    )
    assert result.status == "ok"
    assert result.value["tools"][0]["name"] == "remote__search_0"
    assert service.active_specs(owner)[0].name == "remote__search_0"


async def test_catalog_bundle_scopes_and_schema_drift():
    owner = scope()
    checked = []
    owner = replace(owner, check=lambda: checked.append(True))

    async def resolve(call):
        return owner

    service = ToolCatalogService((tool(0),), owner_scope=owner)
    bundle = service.bundle(resolve)
    spec = bundle.specs[0]
    assert spec.capability == "session_read"
    assert await bundle.policy(invocation(spec)) == "allow"
    parsed = ToolSearchInput.model_validate({"names": ["remote__search_0"]}, strict=True)
    result = await spec.scoped_executor(invocation(spec), parsed, None)
    assert result["tools"][0]["name"] == "remote__search_0" and checked
    with pytest.raises(ServiceError, match="permission_denied"):
        service.search(scope("bob"), ToolSearchInput())
    service._specs["remote__search_0"].output_schema["required"] = ["changed"]
    with pytest.raises(ServiceError, match="tool_binding_changed"):
        service.active_specs(owner)


class ResourceConnection:
    def __init__(self, name="remote"):
        self.config = MCPServerConfig(
            name=name,
            transport="stdio",
            command="fixture",
            selected_tools=("read",),
        )
        self.closed = False
        self.supports_resources = True
        self.calls = []
        self.payload = "x" * 100000

    async def list_resources(self, cursor=None):
        self.calls.append(("list", cursor))
        return {
            "resources": [{"name": "notes", "uri": "file:///notes"}],
            "nextCursor": "second" if cursor is None else None,
        }

    async def list_resource_templates(self, cursor=None):
        self.calls.append(("templates", cursor))
        return {"resourceTemplates": [{"name": "notes", "uriTemplate": "notes://{id}"}]}

    async def read_resource(self, uri):
        self.calls.append(("read", uri))
        return {"contents": [{"uri": uri, "mimeType": "text/plain", "text": self.payload}]}


async def test_resources_route_uri_without_host_access_and_paginate_full_result():
    owner = scope()
    connection = ResourceConnection()
    service = MCPResourceService((MCPResourceBinding("host:remote", connection),))
    assert (await service.list(owner, ListResourcesInput()))["sources"][0][
        "server"
    ] == "host:remote"
    first = await service.list(owner, ListResourcesInput(server="host:remote"))
    assert first["next_cursor"] != "second"
    await service.list(owner, ListResourcesInput(server="host:remote", cursor=first["next_cursor"]))
    assert connection.calls[-1] == ("list", "second")
    templates = await service.list(owner, ListResourcesInput(server="host:remote"), templates=True)
    assert templates["resourceTemplates"][0]["uriTemplate"] == "notes://{id}"
    page = await service.read(
        owner, ReadResourceInput(server="host:remote", uri="file:///etc/passwd")
    )
    assert connection.calls[-1] == ("read", "file:///etc/passwd")
    assert len(page["contents"][0]["text"]) == 12000 and page["total_chars"] == 100000
    full = page["contents"][0]["text"]
    ref = page["result_ref"]
    while page["next_offset"] is not None:
        page = await service.read(
            owner,
            ReadResourceInput(
                server="host:remote",
                result_ref=ref,
                offset=page["next_offset"],
            ),
        )
        full += page["contents"][0]["text"]
    assert full == connection.payload
    assert sum(call[0] == "read" for call in connection.calls) == 1
    assert service.result(owner, ref)["contents"][0]["text"] == connection.payload
    with pytest.raises(ServiceError, match="not_found"):
        await service.read(scope("bob"), ReadResourceInput(server="host:remote", result_ref=ref))
    with pytest.raises(ServiceError, match="invalid_cursor"):
        await service.list(
            scope("bob"), ListResourcesInput(server="host:remote", cursor=first["next_cursor"])
        )
    with pytest.raises(ServiceError, match="invalid_cursor"):
        await service.list(
            owner,
            ListResourcesInput(server="host:remote", cursor=first["next_cursor"]),
            templates=True,
        )


async def test_large_resource_snapshot_has_no_one_mib_content_ceiling():
    owner = scope()
    connection = ResourceConnection()
    connection.payload = "x" * (2 * 1024 * 1024)
    service = MCPResourceService((MCPResourceBinding("host:remote", connection),))
    page = await service.read(owner, ReadResourceInput(server="host:remote", uri="notes://large"))
    assert page["total_chars"] == len(connection.payload)
    restored = MCPResourceService((MCPResourceBinding("host:remote", connection),))
    restored.restore(owner, service.snapshot(owner))
    assert restored.result(owner, page["result_ref"])["contents"][0]["text"] == connection.payload


async def test_resource_resume_restores_full_content_and_rejects_cursor_cycles():
    owner = scope()
    connection = ResourceConnection()
    service = MCPResourceService((MCPResourceBinding("host:remote", connection),))
    page = await service.read(owner, ReadResourceInput(server="host:remote", uri="file:///a"))
    first = await service.list(owner, ListResourcesInput(server="host:remote"))
    snapshot = service.snapshot(owner)
    restored_connection = ResourceConnection()
    restored = MCPResourceService((MCPResourceBinding("host:remote", restored_connection),))
    restored.restore(owner, snapshot)
    resumed = await restored.read(
        owner,
        ReadResourceInput(
            server="host:remote",
            result_ref=page["result_ref"],
            offset=12000,
        ),
    )
    assert resumed["contents"][0]["text"] == "x" * 12000
    assert restored_connection.calls == []
    with pytest.raises(ServiceError, match="tool_binding_changed"):
        restored.restore(scope("bob"), snapshot)

    async def cyclic(cursor=None):
        return {"resources": [], "nextCursor": "third" if cursor == "second" else "second"}

    restored_connection.list_resources = cyclic
    second = await restored.list(
        owner,
        ListResourcesInput(
            server="host:remote",
            cursor=first["next_cursor"],
        ),
    )
    with pytest.raises(ServiceError, match="mcp_cursor_invalid"):
        await restored.list(
            owner,
            ListResourcesInput(
                server="host:remote",
                cursor=second["next_cursor"],
            ),
        )


async def test_account_resource_binding_never_leaks_sources_or_uses_other_owner():
    owner = scope()
    connection = ResourceConnection()
    service = MCPResourceService((MCPResourceBinding("account:remote", connection, owner),))
    assert service.sources(scope("bob")) == []
    with pytest.raises(ServiceError, match="not_found"):
        await service.read(
            scope("bob"), ReadResourceInput(server="account:remote", uri="private://file")
        )
    assert not connection.calls
    connection.closed = True
    with pytest.raises(ServiceError, match="mcp_connection_closed"):
        await service.read(owner, ReadResourceInput(server="account:remote", uri="private://file"))


@pytest.mark.parametrize("name,body", [
    ("list_mcp_resources", ListResourcesInput(server="missing")),
    ("list_mcp_resource_templates", ListResourcesInput(server="missing")),
    ("read_mcp_resource", ReadResourceInput(server="missing", uri="catalog")),
])
async def test_resource_missing_source_matches_invisible_source_without_remote_call(name, body):
    owner = scope()
    other = scope("bob")
    connection = ResourceConnection()
    service = MCPResourceService((MCPResourceBinding("hidden", connection, other),))

    async def resolve(call):
        return owner

    spec = next(spec for spec in service.bundle(resolve).specs if spec.name == name)
    missing = await spec.scoped_executor(invocation(spec), body, None)
    hidden = await spec.scoped_executor(invocation(spec), body.model_copy(
        update={"server": "hidden"}), None)

    assert missing == hidden
    assert missing["ok"] is False and missing["code"] == "not_found"
    assert "list_mcp_resources" in missing["next_action"]
    assert "hidden" not in str(missing) and "bob" not in str(missing)
    assert connection.calls == [] and service.sources(owner) == []


@pytest.mark.parametrize("missing", ["unknown_ref", "other_scope", "wrong_server"])
async def test_resource_cached_lookup_miss_recovers_without_refetch_or_media(missing):
    owner = scope()
    connection = ResourceConnection()
    media = []

    async def sink(*args):
        media.append(args)
        return None

    service = MCPResourceService((MCPResourceBinding("host:remote", connection),
                                  MCPResourceBinding("host:other", ResourceConnection("other"))),
                                 media_sink=sink)
    result = await service.read(owner, ReadResourceInput(server="host:remote", uri="notes://one"))
    actual_scope = scope("bob") if missing == "other_scope" else owner

    async def resolve(call):
        return actual_scope

    spec = next(spec for spec in service.bundle(resolve).specs if spec.name == "read_mcp_resource")
    recovered = await spec.scoped_executor(invocation(spec), ReadResourceInput(
        server="host:other" if missing == "wrong_server" else "host:remote",
        result_ref="unknown" if missing == "unknown_ref" else result["result_ref"],
    ), None)

    assert recovered["ok"] is False and recovered["code"] == "not_found"
    assert connection.calls == [("read", "notes://one")] and media == []


@pytest.mark.parametrize("name,method,body", [
    ("list_mcp_resources", "list_resources", ListResourcesInput(server="host:remote")),
    ("list_mcp_resource_templates", "list_resource_templates",
     ListResourcesInput(server="host:remote")),
    ("read_mcp_resource", "read_resource",
     ReadResourceInput(server="host:remote", uri="notes://one")),
])
@pytest.mark.parametrize("code", ["permission_denied", "tool_binding_changed", "cancelled",
                                      "facts_corrupt", "not_found"])
async def test_resource_remote_service_errors_are_not_local_misses(name, method, body, code):
    owner = scope()
    connection = ResourceConnection()

    async def read_resource(uri):
        raise ServiceError(code)

    setattr(connection, method, read_resource)
    service = MCPResourceService((MCPResourceBinding("host:remote", connection),))

    async def resolve(call):
        return owner

    spec = next(spec for spec in service.bundle(resolve).specs if spec.name == name)
    with pytest.raises(ServiceError, match=code):
        await spec.scoped_executor(invocation(spec), body, None)


@pytest.mark.parametrize("name,body", [
    ("list_mcp_resources", ListResourcesInput(server="missing")),
    ("list_mcp_resource_templates", ListResourcesInput(server="missing")),
    ("read_mcp_resource", ReadResourceInput(server="missing", uri="catalog")),
])
@pytest.mark.parametrize("code", ["permission_denied", "not_found", "cancelled"])
async def test_resource_scope_resolution_errors_are_not_recovered(name, body, code):
    async def resolve(call):
        raise ServiceError(code)

    service = MCPResourceService(())
    spec = next(spec for spec in service.bundle(resolve).specs if spec.name == name)
    with pytest.raises(ServiceError, match=code):
        await spec.scoped_executor(invocation(spec), body, None)


async def test_resource_binding_change_still_rejects_at_tool_boundary():
    owner = scope()
    connection = ResourceConnection()
    service = MCPResourceService((MCPResourceBinding("host:remote", connection),))
    connection.config = connection.config.model_copy(update={"command": "changed"})

    async def resolve(call):
        return owner

    spec = next(spec for spec in service.bundle(resolve).specs if spec.name == "read_mcp_resource")
    with pytest.raises(ServiceError, match="tool_binding_changed"):
        await spec.scoped_executor(invocation(spec), ReadResourceInput(
            server="host:remote", uri="catalog"), None)
    assert connection.calls == []


async def test_resource_recovery_rechecks_scope_and_preserves_cancellation():
    checks = []

    def check():
        checks.append(True)
        if len(checks) == 2:
            raise ServiceError("cancelled")

    owner = replace(scope(), check=check)

    async def resolve(call):
        return owner

    spec = next(spec for spec in MCPResourceService(()).bundle(resolve).specs
                if spec.name == "read_mcp_resource")
    with pytest.raises(ServiceError, match="cancelled"):
        await spec.scoped_executor(invocation(spec), ReadResourceInput(
            server="missing", uri="catalog"), None)
    assert len(checks) == 2


async def test_resource_media_not_found_is_not_a_lookup_recovery():
    owner = scope()

    async def resolve(call):
        return owner

    async def sink(*args):
        raise ServiceError("not_found", 404)

    service = MCPResourceService((MCPResourceBinding("host:remote", ResourceConnection()),),
                                 media_sink=sink)
    spec = next(spec for spec in service.bundle(resolve).specs if spec.name == "read_mcp_resource")
    with pytest.raises(ServiceError, match="not_found"):
        await spec.scoped_executor(invocation(spec), ReadResourceInput(
            server="host:remote", uri="notes://one"), None)


async def test_resource_bundle_passes_media_to_authorized_sink():
    owner = scope()
    connection = ResourceConnection()

    async def read_resource(uri):
        return {"contents": [{"uri": uri, "mimeType": "image/png", "blob": "aGVsbG8="}]}

    connection.read_resource = read_resource
    seen = []

    async def sink(actual_scope, server, record):
        seen.append((actual_scope, server, record))
        return {"ref": "authorized-image"}

    async def resolve(call):
        return owner

    service = MCPResourceService((MCPResourceBinding("host:remote", connection),), media_sink=sink)
    bundle = service.bundle(resolve)
    spec = next(spec for spec in bundle.specs if spec.name == "read_mcp_resource")
    result = await spec.scoped_executor(
        invocation(spec), ReadResourceInput(server="host:remote", uri="picture://one"), None
    )
    assert result["media"] == {"ref": "authorized-image"}
    assert result["contents"][0]["encoding"] == "base64"
    assert seen[0][0] is owner
    assert seen[0][2]["contents"][0]["blob"] == "aGVsbG8="


async def test_host_bundle_contains_only_external_and_checks_scope():
    owner = scope()
    checked = []

    async def resolve(call):
        checked.append(call.name)
        return owner

    async def policy(call):
        return "ask"

    local = ToolSpec("local_read", Args, execute, {"type": "object"}, "1", "a" * 64)
    runtime = ToolRuntime(ToolRegistry((local, tool(0))), policy)
    bundle = runtime.bundle(resolve)
    assert [item.name for item in bundle.specs] == ["remote__search_0"]
    spec = bundle.specs[0]
    assert await bundle.policy(invocation(spec)) == "ask"
    assert await spec.external_executor(invocation(spec), Args(value="ok")) == {"value": "ok"}
    assert checked == [spec.name] * 3
    assert spec.fingerprint == runtime.registry.get(spec.name).fingerprint


async def test_composed_host_bundle_preserves_override_target_policy():
    from krim_agent.contracts import RunEvent
    from krim_agent.tools.bundles import compose_executor
    from krim_agent.tools.executor import ToolContext, ToolExecutor

    owner = scope()
    decisions = []

    async def resolve(call):
        return owner

    async def policy(call):
        decisions.append(call.name)
        return "allow"

    target = tool(0)
    alias = replace(target, name="web_search", binding_target=target)
    runtime = ToolRuntime(ToolRegistry((alias,)), policy)
    executor = compose_executor(ToolExecutor(ToolRegistry(()), None), (runtime.bundle(resolve),))
    spec = executor.registry.get("web_search")

    async def emit(kind, data):
        return RunEvent(run_id="run", seq=1, event_id="1", kind=kind, data=data)

    async def approve(call, seconds):
        return True

    result = await executor.execute(
        replace(invocation(spec), arguments_json='{"value":"ok"}'),
        ToolContext(emit, approve, None),
    )
    assert result.status == "ok"
    assert result.value == {"value": "ok"}
    assert decisions == ["web_search", target.name]


def test_mcp_namespace_and_long_remote_names_are_stable_and_do_not_collide():
    from krim_agent.mcp import MCPTool
    from krim_agent.tools.mcp import mcp_tools

    connection = ResourceConnection()
    names = ("read-value", "read.value", "a" * 100)
    connection.config = connection.config.model_copy(update={"selected_tools": names})
    connection.tools = tuple(
        MCPTool(
            name,
            "Read",
            {"type": "object"},
            {"type": "object"},
            True,
        )
        for name in names
    )
    host = mcp_tools(connection)
    account = mcp_tools(connection, namespace="account")
    assert all(len(spec.name) <= 64 for spec in host + account)
    assert len({spec.name for spec in host + account}) == 6
    assert [spec.name for spec in host] == [spec.name for spec in mcp_tools(connection)]
    assert all(spec.source.server == "remote" for spec in host + account)


def test_session_tool_fingerprints_bind_catalog_connections_and_owner():
    owner = scope()

    async def resolve(call):
        return owner

    original = ToolCatalogService((tool(0),), owner_scope=owner).bundle(resolve).specs[0]
    changed_catalog = ToolCatalogService((tool(1),), owner_scope=owner).bundle(resolve).specs[0]
    changed_owner = (
        ToolCatalogService((tool(0),), owner_scope=scope("bob")).bundle(resolve).specs[0]
    )
    assert len({original.fingerprint, changed_catalog.fingerprint, changed_owner.fingerprint}) == 3

    def resources(connection, actual_owner):
        service = MCPResourceService((MCPResourceBinding("host:remote", connection, actual_owner),))
        return service.bundle(resolve)

    connection = ResourceConnection()
    changed = ResourceConnection()
    changed.config = changed.config.model_copy(update={"command": "different"})
    first = resources(connection, owner)
    other_config = resources(changed, owner)
    other_owner = resources(connection, scope("bob"))
    for left, new_config, new_owner in zip(
        first.specs, other_config.specs, other_owner.specs, strict=True
    ):
        assert len({left.fingerprint, new_config.fingerprint, new_owner.fingerprint}) == 3


async def test_old_resource_invocation_cannot_read_reconfigured_server():
    from krim_agent.contracts import RunEvent
    from krim_agent.tools.executor import ToolContext, ToolExecutor

    owner = scope()

    async def resolve(call):
        return owner

    before = MCPResourceService((MCPResourceBinding("host:remote", ResourceConnection(), owner),))
    connection = ResourceConnection()
    connection.config = connection.config.model_copy(update={"command": "other-endpoint"})
    after = MCPResourceService((MCPResourceBinding("host:remote", connection, owner),))
    old_spec = next(
        spec for spec in before.bundle(resolve).specs if spec.name == "read_mcp_resource"
    )
    bundle = after.bundle(resolve)

    async def emit(kind, data):
        return RunEvent(run_id="run", seq=1, event_id="1", kind=kind, data=data)

    async def approve(call, seconds):
        return True

    call = replace(
        invocation(old_spec), arguments_json='{"server":"host:remote","uri":"file:///a"}'
    )
    result = await ToolExecutor(ToolRegistry(bundle.specs), bundle.policy).execute(
        call,
        ToolContext(emit, approve, None),
    )
    assert result.code == "tool_binding_changed"
    assert connection.calls == []


def test_mcp_hashed_alias_cannot_collide_with_literal_tool_name():
    from krim_agent.mcp import MCPTool
    from krim_agent.tools.mcp import mcp_tools

    connection = ResourceConnection()
    suffix = fingerprint({"namespace": None, "server": "remote", "tool": "a-b"})[:16]
    literal = "a_b_" + suffix
    names = ("a-b", "a.b", literal)
    connection.config = connection.config.model_copy(update={"selected_tools": names})
    connection.tools = tuple(
        MCPTool(
            name,
            "Read",
            {"type": "object"},
            {"type": "object"},
            True,
        )
        for name in names
    )
    specs = mcp_tools(connection)
    assert len({spec.name for spec in specs}) == 3
    assert specs[2].name == "remote__" + literal
    assert len(ToolRegistry(specs).schemas()) == 3
    connection.config = connection.config.model_copy(
        update={"selected_tools": tuple(reversed(names))}
    )
    assert {spec.source.remote_name: spec.name for spec in specs} == {
        spec.source.remote_name: spec.name for spec in mcp_tools(connection)
    }
    connection.config = connection.config.model_copy(update={"selected_tools": ("a" * 129,)})
    with pytest.raises(ServiceError, match="invalid_input"):
        mcp_tools(connection)


def test_trusted_runtime_config_has_no_transport_size_quota(tmp_path):
    import json

    path = tmp_path / "large-tools.json"
    tools = [f"tool_{index:06d}" for index in range(90000)]
    path.write_text(json.dumps({"public_tools": tools}), encoding="utf-8")
    path.chmod(0o600)
    assert path.stat().st_size > 1024 * 1024
    config = ToolRuntimeConfig.load(path)
    assert config.public_tools == tuple(tools)
    for bad in ('{"public_tools": [], "public_tools": []}', '{"web":1e999}', '{"web":NaN}'):
        path.write_text(bad, encoding="utf-8")
        with pytest.raises(ServiceError, match="invalid_input"):
            ToolRuntimeConfig.load(path)


async def test_unconfigured_web_is_not_advertised_and_config_has_no_count_limit():
    config = ToolRuntimeConfig(web=WebAccessConfig(), public_tools=())
    runtime = await ToolRuntime.open(ToolRegistry(()), config, None)
    assert runtime.registry.schemas() == []
    await runtime.close()
    ToolRuntimeConfig(public_tools=tuple(f"tool_{i}" for i in range(100)))
    ToolRuntimeConfig(
        mcp_servers=tuple(
            MCPServerConfig(
                name=f"server{i}", transport="stdio", command="fixture", selected_tools=("read",)
            )
            for i in range(12)
        )
    )
