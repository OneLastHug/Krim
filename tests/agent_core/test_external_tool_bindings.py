"""无需旧冻结材料的外部工具责任链与装配回归。"""

import os
from dataclasses import replace

import httpx
import pytest
from pydantic import SecretStr

from krim_agent.composition import open_application
from krim_agent.contracts import RunEvent, ServiceError, encoded
from krim_agent.mcp import MCPServerConfig, MCPTool
from krim_agent.tools.bindings import resolve_tool_overrides
from krim_agent.tools.builtins import builtin_registry, demo_policy
from krim_agent.tools.core import Invocation
from krim_agent.tools.executor import ToolContext, ToolExecutor
from krim_agent.tools.mcp import arguments_model, mcp_tools
from krim_agent.tools.registry import ToolRegistry
from krim_agent.tools.runtime import ToolRuntime, ToolRuntimeConfig
from krim_agent.tools.web import web_tools
from krim_agent.web_access import TavilyWebAccess, WebAccessConfig


class Connection:
    def __init__(self, config=None):
        self.config = config or MCPServerConfig(
            name="fixture",
            transport="stdio",
            command="fixture",
            selected_tools=("search",),
            readonly_tools=frozenset({"search"}),
        )
        self.closed, self.calls = False, []
        self.tools = (
            MCPTool(
                "search",
                "来源的实际参数",
                {
                    "type": "object",
                    "required": ["term"],
                    "properties": {"term": {"type": "string"}},
                    "additionalProperties": False,
                },
                {
                    "type": "object",
                    "properties": {"text": {"type": "string"}},
                    "required": ["text"],
                    "additionalProperties": False,
                },
                True,
            ),
        )

    async def connect(self):
        return self

    async def call_tool(self, name, arguments):
        if self.closed:
            raise ServiceError("mcp_connection_closed", 503)
        self.calls.append((name, arguments))
        return {"text": arguments["term"]}

    async def close(self):
        self.closed = True


def backend():
    def handler(request):
        return httpx.Response(
            200,
            json={
                "results": [
                    {"title": "HTTP", "url": "https://example.com/", "content": "HTTP body"}
                ]
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return TavilyWebAccess(WebAccessConfig(api_key=SecretStr("test-key")), client=client), client


async def execute(registry, arguments, policy, pre=None, post=None, allow=True):
    events, approvals = [], []

    async def emit(kind, data):
        events.append((kind, data))
        return RunEvent(
            run_id="run", seq=len(events), event_id=str(len(events)), kind=kind, data=data
        )

    async def approve(invocation, seconds):
        approvals.append(invocation)
        return allow

    invocation = Invocation(
        "run",
        "call",
        "span",
        0,
        "web_search",
        encoded(arguments),
        registry.get("web_search").fingerprint,
    )
    result = await ToolExecutor(registry, policy, pre, post).execute(
        invocation, ToolContext(emit, approve, 2)
    )
    return result, events, approvals


def test_legacy_descriptor_metadata_and_legacy_write_guard():
    for spec, _ in builtin_registry()._frozen:
        expected = {
            "name", "version", "implementation_digest", "input_schema", "output_schema",
        }
        if spec.description:
            expected.add("description")
        descriptor = spec.descriptor()
        assert set(descriptor) == expected
        if spec.description:
            assert descriptor["description"] == spec.description
        assert not {"risk", "capability", "source", "policy_version"} & set(descriptor)
        with pytest.raises(ValueError):
            replace(spec, risk="write")


def test_original_json_schema_local_definitions_and_remote_ref_rejection():
    schema = {
        "type": "object",
        "properties": {"value": {"$ref": "#/$defs/word"}},
        "$defs": {"word": {"type": "string", "minLength": 2}},
        "required": ["value"],
    }
    model = arguments_model("Fixture", schema)
    assert model.model_json_schema() == schema
    assert model.model_validate({"value": "ok"}).model_dump() == {"value": "ok"}
    with pytest.raises(ValueError):
        model.model_validate({"value": 5})
    with pytest.raises(ValueError):
        arguments_model("Bad", {"type": "object", "$ref": "https://example.com/schema"})


@pytest.mark.parametrize("denied", ["web_search", "fixture__search"])
async def test_alias_and_real_target_both_require_permission(denied):
    web, client = backend()
    connection = Connection()
    specs = web_tools(web) + mcp_tools(connection)
    registry = resolve_tool_overrides(specs, {"web_search": "fixture__search"}, ("web_search",))
    names = []

    async def policy(invocation):
        names.append(invocation.name)
        return "deny" if invocation.name == denied else "allow"

    try:
        result, events, approvals = await execute(registry, {"term": "ok"}, policy)
        assert result.code == "permission_denied" and not connection.calls and not approvals
        assert names == ["web_search", "fixture__search"]
        assert not any(kind == "tool_intent" for kind, _ in events)
    finally:
        await web.close()
        await client.aclose()


async def test_target_schema_one_approval_one_chain_and_no_fallback():
    web, client = backend()
    connection = Connection()
    specs = web_tools(web) + mcp_tools(connection)
    registry = resolve_tool_overrides(specs, {"web_search": "fixture__search"}, ("web_search",))
    hooks = []

    async def policy(invocation):
        return "ask" if invocation.name == "fixture__search" else "allow"

    async def pre(invocation):
        hooks.append(("pre", invocation.run_id, invocation.call_id))

    async def post(invocation, result):
        hooks.append(("post", invocation.run_id, invocation.call_id))

    try:
        spec = registry.get("web_search")
        assert spec.description == "来源的实际参数" and spec.parameters.model_json_schema()[
            "required"
        ] == ["term"]
        result, events, approvals = await execute(registry, {"term": "MCP body"}, policy, pre, post)
        assert result.status == "ok" and result.value == {"text": "MCP body"}
        assert len(approvals) == len(connection.calls) == 1
        assert approvals[0].tool_hash == spec.fingerprint
        assert hooks == [("pre", "run", "call"), ("post", "run", "call")]
        assert len([kind for kind, _ in events if kind == "tool_intent"]) == 1
        await connection.close()
        result, _, _ = await execute(registry, {"term": "closed"}, policy)
        assert result.code == "mcp_unavailable" and len(connection.calls) == 1
    finally:
        await web.close()
        await client.aclose()


async def test_configured_runtime_mcp_only_default_asks_and_closes(monkeypatch, tmp_path):
    import krim_agent.tools.runtime as runtime_module

    created = []

    def factory(config):
        connection = Connection(config)
        created.append(connection)
        return connection

    monkeypatch.setattr(runtime_module, "MCPConnection", factory)
    source = Connection().config
    config = ToolRuntimeConfig(
        mcp_servers=(source,),
        public_tools=("web_search",),
        tool_overrides={"web_search": "fixture__search"},
    )
    async with await open_application(tmp_path / "runtime.sqlite", tool_config=config) as app:
        assert app.tool_runtime is not None
        registry = app.tool_runtime.registry
        result, _, approvals = await execute(registry, {"term": "no key"}, app.tool_runtime.policy)
        assert result.status == "ok" and len(approvals) == 1
        assert result.value == {"text": "no key"}
    assert created[0].closed


def _secure_runtime_config_file(path):
    path.chmod(0o600)
    if os.name == "nt":
        import subprocess

        user = subprocess.run(["whoami"], check=True, capture_output=True, text=True).stdout.strip()
        subprocess.run(
            [
                "icacls",
                str(path),
                "/inheritance:r",
                "/grant:r",
                f"{user}:(F)",
                "*S-1-5-18:(F)",
                "*S-1-5-32-544:(F)",
            ],
            check=True,
            capture_output=True,
        )


def test_host_config_refs_credentials_and_does_not_reuse_model_key(tmp_path):
    path = tmp_path / "tools.json"
    path.write_text(encoded({"web": {}, "tool_overrides": {}}))
    _secure_runtime_config_file(path)
    loaded = ToolRuntimeConfig.load(path, environment={"KRIM_AGENT_WEB_API_KEY": "fake-key"})
    assert loaded.web.api_key.get_secret_value() == "fake-key"
    path.write_text(encoded({"web": {"api_key_env": "KRIM_AGENT_HTTP_API_KEY"}}))
    _secure_runtime_config_file(path)
    with pytest.raises(ServiceError):
        ToolRuntimeConfig.load(path, environment={"KRIM_AGENT_HTTP_API_KEY": "do-not-use"})


def test_runtime_config_rejects_untrusted_file_links_and_permissions(tmp_path):
    payload = encoded({"web": {}, "tool_overrides": {}}).encode()
    trusted = tmp_path / "trusted.json"
    trusted.write_bytes(payload)
    _secure_runtime_config_file(trusted)
    assert ToolRuntimeConfig.load(trusted, environment={"KRIM_AGENT_WEB_API_KEY": ""})

    loose = tmp_path / "loose.json"
    loose.write_bytes(payload)
    loose.chmod(0o644)
    if os.name == "posix":
        with pytest.raises(ServiceError):
            ToolRuntimeConfig.load(loose, environment={"KRIM_AGENT_WEB_API_KEY": ""})

    linked = tmp_path / "linked.json"
    linked.symlink_to(trusted)
    with pytest.raises(ServiceError):
        ToolRuntimeConfig.load(linked, environment={"KRIM_AGENT_WEB_API_KEY": ""})

    hard = tmp_path / "hard.json"
    hard.hardlink_to(trusted)
    with pytest.raises(ServiceError):
        ToolRuntimeConfig.load(hard, environment={"KRIM_AGENT_WEB_API_KEY": ""})


def test_registry_capacity_is_a_stable_service_error():
    from dataclasses import replace

    base = builtin_registry()
    spec = next(item for item, _ in base._frozen)
    specs = tuple(replace(spec, name=f"tool_{index}") for index in range(17))
    with pytest.raises(ServiceError) as raised:
        ToolRegistry(specs)
    assert (raised.value.code, raised.value.status) == ("tool_limit", 422)


async def test_runtime_config_injects_pinned_policy_for_http_mcp(monkeypatch):
    import krim_agent.tools.runtime as runtime_module

    observed = []

    def factory(config, *, egress_policy=None):
        observed.append(egress_policy)
        return Connection(config)

    monkeypatch.setattr(runtime_module, "MCPConnection", factory)
    source = MCPServerConfig(
        name="remote",
        transport="streamable_http",
        url="https://mcp.example.test:8443/v1",
        allowed_hosts=frozenset({"mcp.example.test"}),
        selected_tools=("search",),
    )
    runtime = await ToolRuntime.open(
        builtin_registry(), ToolRuntimeConfig(mcp_servers=(source,)), demo_policy
    )
    try:
        assert runtime.connections
    finally:
        await runtime.close()
    assert len(observed) == 1
    assert observed[0].validate_target("https://mcp.example.test:8443/v1").port == 8443
    from krim_agent.network.egress_policy import EgressViolation

    with pytest.raises(EgressViolation):
        observed[0].validate_target("https://other.example.test:8443/v1")


async def test_discovery_failure_closes_resources_and_old_registry_is_not_modified(monkeypatch):
    import krim_agent.tools.runtime as runtime_module

    connection = Connection()

    async def failed():
        raise ServiceError("mcp_discovery_failed")

    connection.connect = failed
    monkeypatch.setattr(runtime_module, "MCPConnection", lambda config: connection)
    base = builtin_registry()
    before = base.schemas()
    with pytest.raises(ServiceError):
        await ToolRuntime.open(
            base, ToolRuntimeConfig(mcp_servers=(connection.config,)), demo_policy
        )
    assert connection.closed and base.schemas() == before
