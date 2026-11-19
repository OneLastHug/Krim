"""Offline checks: no paid model calls, remote MCP, Web, Docker, or real credentials."""

from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import SecretStr, ValidationError

from krim_agent.contracts import ServiceError
from krim_agent.mcp import MCPServerConfig
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig
from krim_agent.providers.types import ModelText
from krim_agent.tools.runtime import ToolRuntimeConfig
from krim_agent.web_access import WebAccessConfig
from tests.coding_agent import external_acceptance as module
from tests.coding_agent import verify_external_services_real as runner
from tests.coding_agent.external_acceptance import AcceptanceError, Scenario
from tests.coding_agent.external_challenge import INPUT_CSV, make_challenge


@pytest.fixture
def services():
    scenario = Scenario(
        tool_config_file=Path("tools.json"),
        mcp_server="docs",
        mcp_tool="doc_read",
        mcp_task="Consult the Python csv reference.",
        mcp_resource_uri="doc://brief",
    )
    provider = OpenAICompatibleConfig(
        base_url="https://model.example.test/v1",
        model="test",
        api_key=SecretStr("provider-fixture"),
        allow_images=True,
    )
    mcp = MCPServerConfig(
        name="docs",
        transport="streamable_http",
        url="https://docs.example.test/mcp",
        allowed_hosts=frozenset({"docs.example.test"}),
        selected_tools=("doc_read",),
        readonly_tools=frozenset({"doc_read"}),
        headers={"Authorization": SecretStr("Bearer mcp-fixture")},
    )
    tools = ToolRuntimeConfig(
        web=WebAccessConfig(api_key=SecretStr("web-fixture")), mcp_servers=(mcp,)
    )
    return module.validate_services(scenario, provider, tools)


def test_configuration_has_no_credentials_or_argument_values_in_summary(services):
    text = json.dumps(services.summary())
    assert "fixture" not in text and "example.test" not in text
    assert services.summary()["external_calls_started"] is False
    assert "provider-fixture" not in repr(services)


@pytest.mark.parametrize(
    "change",
    [
        "no-provider",
        "no-key",
        "no-images",
        "no-web",
        "http-web",
        "missing-mcp",
        "stdio",
        "writable",
        "unselected",
        "override",
    ],
)
def test_incomplete_or_unsafe_service_configuration_fails_closed(services, change):
    provider, tools, mcp = services.provider, services.tools, services.mcp
    if change == "no-provider":
        provider = None
    elif change == "no-key":
        provider = provider.model_copy(update={"api_key": SecretStr("")})
    elif change == "no-images":
        provider = provider.model_copy(update={"allow_images": False})
    elif change == "no-web":
        tools = replace(tools, web=None)
    elif change == "http-web":
        tools = replace(
            tools,
            web=WebAccessConfig(endpoint="http://127.0.0.1:9999", api_key=SecretStr("web-fixture")),
        )
    elif change == "missing-mcp":
        tools = replace(tools, mcp_servers=())
    elif change == "stdio":
        mcp = MCPServerConfig(name="docs", transport="stdio", command="python")
    elif change == "writable":
        mcp = mcp.model_copy(update={"readonly_tools": frozenset()})
    elif change == "unselected":
        mcp = mcp.model_copy(update={"selected_tools": ()})
    elif change == "override":
        tools = replace(tools, tool_overrides={"web_search": "other"})
    if change in {"stdio", "writable", "unselected"}:
        tools = replace(tools, mcp_servers=(mcp,))
    with pytest.raises(AcceptanceError):
        module.validate_services(services.scenario, provider, tools)


def test_manifest_load_uses_existing_environment_reference_loader(tmp_path, monkeypatch, services):
    tools_path = tmp_path / "tools.json"
    tools_path.write_text(
        json.dumps(
            {
                "web": {"api_key_env": "ACCEPT_WEB"},
                "mcp_servers": [
                    {
                        "name": "docs",
                        "transport": "streamable_http",
                        "url": "https://docs.example.test/mcp",
                        "allowed_hosts": ["docs.example.test"],
                        "selected_tools": ["doc_read"],
                        "readonly_tools": ["doc_read"],
                        "bearer_env": "ACCEPT_MCP",
                    }
                ],
            }
        )
    )
    tools_path.chmod(0o600)
    scenario_path = tmp_path / "scenario.json"
    scenario_path.write_text(services.scenario.model_dump_json())
    monkeypatch.setenv("ACCEPT_WEB", "web-fixture")
    monkeypatch.setenv("ACCEPT_MCP", "mcp-fixture")
    monkeypatch.setattr(OpenAICompatibleConfig, "from_env", lambda: services.provider)
    loaded = module.load_services(scenario_path)
    assert loaded.mcp.headers["Authorization"].get_secret_value() == "Bearer mcp-fixture"
    assert loaded.tools.web.api_key.get_secret_value() == "web-fixture"
    assert loaded.scenario.mcp_tool == "doc_read"


@pytest.mark.parametrize(
    "value", ["http://example.test", "https://user@example.test/x", "https://example.test/?token=x"]
)
def test_reference_urls_do_not_accept_credentials_or_plaintext(value, services):
    with pytest.raises(ValidationError):
        Scenario.model_validate(services.scenario.model_dump() | {"web_url": value})


def test_configuration_failure_does_not_echo_bad_input(tmp_path):
    path = tmp_path / "scenario.json"
    path.write_text('{"api_key":"DO_NOT_ECHO"}')
    with pytest.raises(AcceptanceError) as caught:
        module.load_services(path)
    assert "DO_NOT_ECHO" not in str(caught.value)
    assert "DO_NOT_ECHO" not in json.dumps(module.failure(caught.value))


def fake_adapters(monkeypatch, services, *, fail_at=None):
    calls, closed = [], []
    challenge = make_challenge(order=("red", "green", "blue"))
    monkeypatch.setattr(module, "make_challenge", lambda: challenge)

    class Web:
        def __init__(self, config):
            assert config is services.tools.web

        async def search(self, *args, **kwargs):
            calls.append("web.search")
            if fail_at == "web":
                raise RuntimeError("DO_NOT_ECHO_CREDENTIAL")
            return {"results": [{"content": "csv reference"}]}

        async def fetch(self, *args, **kwargs):
            calls.append("web.fetch")
            return {"results": [{"content": "csv reference", "result_ref": "ref"}]}

        async def close(self):
            closed.append("web")

    class MCP:
        def __init__(self, config):
            self.config = config
            self.supports_resources = True
            self.tools = (
                SimpleNamespace(
                    name="doc_read",
                    description="Read docs",
                    input_schema={"type": "object", "properties": {}},
                    output_schema=None,
                    read_only=True,
                ),
            )

        async def connect(self):
            calls.append("mcp.connect")
            if fail_at == "mcp":
                raise ServiceError("mcp_connect_failed", 502)

        async def call_tool(self, name, args):
            calls.append("mcp.call")
            return {"doc": "csv reference"}

        async def list_resources(self):
            return {"resources": []}

        async def list_resource_templates(self):
            return {"resourceTemplates": []}

        async def read_resource(self, uri):
            return {"contents": [{"uri": uri, "text": "reference"}]}

        async def close(self):
            closed.append("mcp")

    class Provider:
        def __init__(self, config):
            assert config is services.provider

        async def stream(self, messages, tools):
            calls.append("model.image")
            assert not tools and messages[0].images[0].data == challenge.image
            if fail_at == "vision":
                yield ModelText(text='{"order":["blue","green","red"]}')
            else:
                yield ModelText(text='{"order":["red","green","blue"]}')

        async def close(self):
            closed.append("model")

    monkeypatch.setattr(module, "TavilyWebAccess", Web)
    monkeypatch.setattr(module, "MCPConnection", MCP)
    monkeypatch.setattr(module, "OpenAICompatibleProvider", Provider)
    return calls, closed


async def test_preflight_uses_three_adapters_and_closes_all(monkeypatch, services):
    calls, closed = fake_adapters(monkeypatch, services)
    result = await module.preflight(services)
    assert all(result[key]["passed"] for key in ("web", "mcp", "vision"))
    assert result["mcp"]["public_name"] == "docs__doc_read"
    assert calls == [
        "web.search",
        "web.fetch",
        "web.fetch",
        "mcp.connect",
        "mcp.call",
        "model.image",
    ]
    assert closed == ["web", "mcp", "model"]
    assert "fixture" not in json.dumps(result)


@pytest.mark.parametrize(
    "phase,expected",
    [("web", ["web"]), ("mcp", ["web", "mcp"]), ("vision", ["web", "mcp", "model"])],
)
async def test_preflight_failure_stops_and_releases_owned_clients(
    monkeypatch, services, phase, expected
):
    _, closed = fake_adapters(monkeypatch, services, fail_at=phase)
    with pytest.raises((AcceptanceError, RuntimeError, ServiceError)):
        await module.preflight(services)
    assert closed == expected


async def test_account_preflight_uses_production_namespacing(monkeypatch, services):
    configured = replace(
        services, scenario=services.scenario.model_copy(update={"mcp_scope": "account"})
    )
    fake_adapters(monkeypatch, configured)
    result = await module.preflight(configured)
    assert result["mcp"]["public_name"] == "account__docs__doc_read"


def completed_events(services):
    names = [
        "web_search",
        "web_fetch",
        "docs__doc_read",
        "tool_search",
        "view_image",
        "ask_user",
        "enter_plan_mode",
        "exit_plan_mode",
        "memory_write",
        "shell",
        "read_output",
        "read_mcp_resource",
    ]
    return [
        SimpleNamespace(
            kind="tool_end",
            data={
                "name": name,
                "status": "ok",
                "value": {"status": "ready"},
                "arguments": {"server": "host:docs", "uri": services.scenario.mcp_resource_uri},
            },
        )
        for name in names
    ]


def grade(events, services):
    return module.grade_evidence(
        events,
        services,
        restarted=True,
        compactions=1,
        goal={"status": "completed"},
        plan={"counts": {"completed": 1, "pending": 0, "in_progress": 0}},
        mcp_tool_name="docs__doc_read",
    )


def test_grader_requires_recorded_success_not_tool_names_or_model_text(services):
    events = completed_events(services)
    assert all(grade(events, services).values())
    for event in events:
        event.data["status"] = "denied"
    assert not any(
        grade(events, services)[name]
        for name in ("web_search", "web_fetch", "view_image", "mcp_tool", "mcp_resource")
    )


def test_resource_evidence_is_bound_to_source_and_uri(services):
    events = completed_events(services)
    events[-1].data["arguments"]["server"] = "account:other"
    assert grade(events, services)["mcp_resource"] is False


def test_prompt_does_not_expose_credentials_or_oracle(services):
    prompt = runner.task_prompt(services, "docs__doc_read")
    assert "fixture" not in prompt and "example.test" not in prompt
    assert "validator.py" in prompt and "view_image" in prompt and "mode=text" in prompt


def test_dynamic_question_ids_and_plan_confirmation():
    pending = {
        "id": "request",
        "revision": 1,
        "kind": "user_input",
        "payload": {"questions": [{"id": "chosen-by-model", "mode": "text"}]},
    }
    body = runner.answer_body(pending)
    assert body["answers"][0]["question_id"] == "chosen-by-model"
    pending["kind"] = "plan_confirmation"
    assert runner.answer_body(pending)["confirmed"] is True


def test_question_choices_are_not_blindly_selected():
    pending = {
        "id": "request",
        "revision": 1,
        "kind": "user_input",
        "payload": {"questions": [{"id": "q", "mode": "single"}]},
    }
    with pytest.raises(AcceptanceError):
        runner.answer_body(pending)


async def test_full_mode_checks_platform_before_paid_requests(tmp_path, monkeypatch, services):
    calls = AsyncMock()
    monkeypatch.setattr(runner, "preflight", calls)

    def denied(_path):
        raise AcceptanceError("trusted root platform required")

    monkeypatch.setattr(runner, "require_platform", denied)
    output = tmp_path / "not-created"
    with pytest.raises(AcceptanceError):
        await runner.execute(services, output, full=True)
    assert not output.exists() and calls.await_count == 0


async def test_preflight_errors_are_sanitized_and_reported(tmp_path, monkeypatch, services):
    monkeypatch.setattr(runner, "preflight", AsyncMock(side_effect=RuntimeError("SECRET_UPSTREAM")))
    output = tmp_path / "evidence"
    result = await runner.execute(services, output, full=False)
    assert result["passed"] is False and result["error"]["type"] == "RuntimeError"
    text = (output / "result.json").read_text()
    assert "SECRET_UPSTREAM" not in text and "provider-fixture" not in text


async def test_existing_evidence_directory_refused_before_service_calls(
    tmp_path, monkeypatch, services
):
    calls = AsyncMock()
    monkeypatch.setattr(runner, "preflight", calls)
    with pytest.raises(FileExistsError):
        await runner.execute(services, tmp_path, full=False)
    assert calls.await_count == 0


def test_artifact_reader_refuses_links_and_non_regular_files(tmp_path):
    (tmp_path / "validator.py").write_bytes(b"print('ok')")
    assert runner.read_artifact(tmp_path, "validator.py") == b"print('ok')"
    (tmp_path / "link.py").symlink_to(tmp_path / "validator.py")
    with pytest.raises(OSError):
        runner.read_artifact(tmp_path, "link.py")
    os.mkfifo(tmp_path / "fifo.py")
    with pytest.raises(AcceptanceError):
        runner.read_artifact(tmp_path, "fifo.py")


async def test_isolated_host_always_closes_broker_and_partial_host(tmp_path, monkeypatch, services):
    calls = []
    monkeypatch.setattr(runner, "APPROVED_STORAGE_ROOT", tmp_path)

    @asynccontextmanager
    async def broker(_paths):
        calls.append("broker-open")
        try:
            yield None
        finally:
            calls.append("broker-close")

    class Host:
        def __init__(self, *args):
            pass

        async def setup(self):
            raise RuntimeError("fail-after-host-start")

        async def close(self):
            calls.append("host-close")

    monkeypatch.setattr(runner, "broker_lifecycle", broker)
    monkeypatch.setattr(runner, "initialize", AsyncMock(return_value="disposable"))
    monkeypatch.setattr(runner, "Host", Host)
    report = {}
    with pytest.raises(RuntimeError):
        async with runner.isolated_host(services, tmp_path, report):
            pass
    assert calls == ["broker-open", "host-close", "broker-close"]
    assert report["temporary_root_removed"] is True
    assert not list(tmp_path.glob("external-real-*"))


def holdout_result():
    return {
        "rows": [
            {"line": 2, "valid": True, "code": None},
            {"line": 3, "valid": False, "code": "green"},
            {"line": 4, "valid": True, "code": None},
            {"line": 5, "valid": False, "code": "red"},
            {"line": 6, "valid": False, "code": "blue"},
            {"line": 7, "valid": True, "code": None},
        ]
    }


def test_holdout_rejects_hardcoded_public_sample_and_missing_lines():
    value = holdout_result()
    raw = json.dumps(value).encode()
    assert runner.grade_holdout(raw, ("red", "green", "blue"))["passed"] is True
    sample = {"rows": [dict(row, line=9 - row["line"]) for row in reversed(value["rows"])]}
    assert (
        runner.grade_holdout(json.dumps(sample).encode(), ("red", "green", "blue"))["passed"]
        is False
    )
    value["rows"].pop()
    with pytest.raises(AcceptanceError):
        runner.grade_holdout(json.dumps(value).encode(), ("red", "green", "blue"))


async def test_independent_program_uses_verified_container_not_host_python(tmp_path, monkeypatch):
    from krim_agent.development.contracts import EnvironmentHandle
    from tests.agent_core.test_dev_environment_execution import make_record

    record = make_record(tmp_path)
    calls = []

    class Docker:
        async def run(self, args):
            calls.append(args)
            return SimpleNamespace(code=0, stdout=json.dumps(holdout_result()), stderr="")

    async def checked(_runner, args):
        assert args[0] == "create" and "--runtime=runsc" in args and "--network=none" in args
        assert any("python3 -I validator.py" in arg for arg in args)
        return "a" * 64

    monkeypatch.setattr(runner.docker, "checked", checked)
    monkeypatch.setattr(runner.docker, "inspect", AsyncMock(return_value={"test": True}))
    monkeypatch.setattr(runner.docker, "verify", lambda *args: "stopped")
    host = SimpleNamespace(
        content=tmp_path,
        handle=EnvironmentHandle.model_validate(record.environment.model_dump()),
        owner=record.environment.owner_id,
        session_id="session",
        project={"project_id": record.environment.project_id},
        docker=Docker(),
        app=SimpleNamespace(
            development=SimpleNamespace(
                repositories=SimpleNamespace(
                    environments=SimpleNamespace(resolve=AsyncMock(return_value=record))
                )
            )
        ),
    )
    result = await runner.grade_program(host, ("red", "green", "blue"))
    assert result["passed"] is True and calls == [("start", "--attach", "a" * 64)]
    grader_input = next(tmp_path.glob(".grader-*.csv")).read_bytes()
    assert grader_input != INPUT_CSV
    assert grader_input.startswith(b"name,age\nFox,10\n")


async def test_account_isolation_and_deletion_via_production_api(tmp_path, services):
    from krim_agent.account_mcp.secrets import MasterKeyring
    from krim_agent.composition import open_application
    from krim_agent.config import Settings
    from tests.agent_core.test_account_api import initialize
    from tests.coding_agent.verify_general_tools_real import client_for, request

    key_file = tmp_path / "mcp-key.json"
    MasterKeyring.create(b"x" * 32).save(key_file)
    path = tmp_path / "account.sqlite"
    token = await initialize(path)
    async with await open_application(
        path, settings=Settings(personal_mcp_key_file=key_file)
    ) as app:
        client = await client_for(app, token)
        try:
            me = await request(client, "GET", "/v2/me")
            workspace = me["workspaces"][0]["workspace_id"]
            base = f"/v2/workspaces/{workspace}/sessions"
            session = await request(client, "POST", base, {"request_key": "s"})
            mcp = services.mcp
            connection = await request(
                client,
                "POST",
                "/v2/me/mcp-connections",
                {
                    "request_key": "c",
                    "name": mcp.name,
                    "url": mcp.url,
                    "allowed_hosts": sorted(mcp.allowed_hosts),
                    "selected_tools": list(mcp.selected_tools),
                    "readonly_tools": sorted(mcp.readonly_tools),
                    "credentials": {"Authorization": "Bearer fixture"},
                },
            )
            update_body = {
                "request_key": "update-connection",
                "expected_revision": 1,
                "name": mcp.name,
                "url": mcp.url,
                "allowed_hosts": sorted(mcp.allowed_hosts),
                "selected_tools": list(mcp.selected_tools),
                "readonly_tools": sorted(mcp.readonly_tools),
            }
            for invalid in ({"allowed_hosts": [123]}, {"selected_tools": ["doc_read", "doc_read"]}):
                response = await client.put(
                    "/v2/me/mcp-connections/" + connection["id"], json=update_body | invalid
                )
                assert response.status_code == 422
            connection = await request(
                client, "PUT", "/v2/me/mcp-connections/" + connection["id"], update_body
            )
            route = base + "/" + session["id"]
            await request(
                client,
                "PUT",
                route + "/mcp",
                {
                    "request_key": "select",
                    "expected_revision": 0,
                    "connections": [
                        {"connection_id": connection["id"], "revision": connection["revision"]}
                    ],
                },
            )
            host = SimpleNamespace(
                client=client,
                route=route,
                connection=connection,
                services=replace(
                    services, scenario=services.scenario.model_copy(update={"mcp_scope": "account"})
                ),
            )
            result = await runner.account_checks(host)
            assert result["status"] == "passed" and result["deleted_connection_reselection_denied"]
        finally:
            await client.aclose()


async def test_host_shutdown_joins_producers_before_stopping_environment(
    tmp_path, monkeypatch, services
):
    from contextlib import nullcontext

    calls = []

    async def step(name):
        calls.append(name)

    host = runner.Host(services, None, tmp_path, "test-token", {})
    host.client = SimpleNamespace(aclose=lambda: step("client"))
    host.handle = object()
    host.app = SimpleNamespace(
        sessions=SimpleNamespace(close=lambda: step("sessions")),
        service=SimpleNamespace(close=lambda: step("runs")),
        development=SimpleNamespace(
            host_cleanup=nullcontext,
            environments=SimpleNamespace(stop=lambda _: step("environment")),
        ),
        close=lambda: step("application"),
    )
    monkeypatch.setattr(runner, "cleanup_containers", AsyncMock(return_value=([], [])))
    await host.close()
    assert calls == ["client", "sessions", "runs", "environment", "application"]
    assert host.report["cleanup_errors"] == []


async def test_cleanup_failure_retains_root_and_sanitizes_error(tmp_path, monkeypatch, services):
    monkeypatch.setattr(runner, "APPROVED_STORAGE_ROOT", tmp_path)

    @asynccontextmanager
    async def broker(_paths):
        yield None

    class Host:
        def __init__(self, *args):
            pass

        async def setup(self):
            pass

        async def close(self):
            raise RuntimeError("SENSITIVE_UPSTREAM")

    monkeypatch.setattr(runner, "broker_lifecycle", broker)
    monkeypatch.setattr(runner, "initialize", AsyncMock(return_value="test-token"))
    monkeypatch.setattr(runner, "Host", Host)
    report = {}
    async with runner.isolated_host(services, tmp_path, report):
        pass
    assert report["cleanup_errors"] == [{"type": "RuntimeError", "code": "acceptance_failed"}]
    assert "SENSITIVE_UPSTREAM" not in json.dumps(report)
    assert await asyncio.to_thread(Path(report["retained_root"]).is_dir)
    assert "temporary_root_removed" not in report
