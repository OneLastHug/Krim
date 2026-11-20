"""Production startup, explicit fixture injection, and retired-target boundaries."""

import httpx
import pytest

from krim_agent.api.server import create_app
from krim_agent.composition import open_application
from krim_agent.contracts import RunRequest, ServiceError
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage.sqlite import SQLiteStore


async def test_unconfigured_management_does_not_register_or_answer_with_fixtures(tmp_path):
    async with await open_application(tmp_path / "management.sqlite") as app:
        assert not app.service.bindings
        assert app.service.loop.executor.registry.schemas() == []
        with pytest.raises(ServiceError, match="provider_not_configured"):
            await app.service.start(RunRequest(text="test", request_key="missing-model"))
        with pytest.raises(ServiceError, match="provider_not_configured"):
            await app.sessions.create(LOCAL, "local", SessionCreate(request_key="missing-session"))
        assert not app.service.tasks
        web = create_app(application=app)
        async with web.router.lifespan_context(web):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=web), base_url="http://127.0.0.1"
            ) as client:
                response = await client.get("/healthz")
        assert response.status_code == 200
        assert response.json()["provider"] == "unconfigured"
        assert response.json()["run_ready"] is False


@pytest.mark.parametrize(
    "target", ["demo", "target-v1", "target-v2", "target-bad", "target-workspace"],
)
async def test_retired_targets_cannot_create_new_work(tmp_path, target):
    async with await open_application(tmp_path / "retired.sqlite") as app:
        with pytest.raises(ServiceError, match="target_retired"):
            await app.service.start(RunRequest(text="test", request_key="new-run", target=target))
        with pytest.raises(ServiceError, match="target_retired"):
            await app.sessions.create(
                LOCAL, "local", SessionCreate(request_key="new-session", target_key=target)
            )
        assert not app.service.tasks


async def test_real_model_boundary_uses_default_session_target_without_demo_alias(tmp_path, wire):
    async with wire.application(tmp_path / "session.sqlite") as app:
        assert set(app.service.bindings) == {"target-http"}
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="ordinary"))
        assert session.target_key == "target-http"
        turn = await app.sessions.submit(
            LOCAL, "local", session.id,
            TurnRequest(text="hello", request_key="first", expected_revision=session.revision),
        )
        finished = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
        assert finished.status == "completed"
        assert wire.requests
        assert all(spec["function"]["name"] not in {"demo"} for spec in wire.requests[0]["tools"])


async def test_management_only_startup_does_not_connect_configured_remote_tools(
    tmp_path, monkeypatch,
):
    import sys

    from krim_agent.mcp.types import MCPServerConfig
    from krim_agent.tools.runtime import ToolRuntime, ToolRuntimeConfig

    async def unexpected_open(*_args, **_kwargs):
        raise AssertionError("management-only startup must not connect MCP")

    monkeypatch.setattr(ToolRuntime, "open", unexpected_open)
    tools = ToolRuntimeConfig(mcp_servers=(
        MCPServerConfig(name="configured", transport="stdio", command=sys.executable),
    ))
    async with await open_application(tmp_path / "no-mcp-connect.sqlite", tool_config=tools) as app:
        assert app.tool_runtime is None
        assert not app.service.bindings


async def test_sdk_settings_are_revalidated_before_startup(tmp_path):
    from krim_agent.config import Settings

    invalid = Settings.model_construct(read_parallelism=0)
    with pytest.raises(ServiceError, match="invalid_input"):
        await open_application(tmp_path / "invalid-settings.sqlite", settings=invalid)


@pytest.mark.parametrize("field,value", [
    ("max_steps", 3), ("max_tool_calls", 3), ("history_compaction", False),
])
async def test_sdk_development_overrides_are_rejected_not_silently_ignored(tmp_path, field, value):
    from krim_agent.development.configuration import DevelopmentConfiguration

    legacy = DevelopmentConfiguration.model_construct(**{field: value})
    with pytest.raises(ServiceError, match="invalid_input"):
        await open_application(tmp_path / "legacy-limit.sqlite", development_configuration=legacy)


@pytest.mark.parametrize("boundary", ["shared_kernel", "diagnostic_stream"])
async def test_new_execution_boundaries_remain_covered_by_binding_seal(
    tmp_path, wire, monkeypatch, boundary,
):
    from krim_agent.runtime import engine

    async def unexpected_replacement(*_args, **_kwargs):
        raise AssertionError("changed code must not execute under the old binding")

    async with wire.application(tmp_path / "sealed.sqlite") as app:
        binding = app.service.bindings["target-http"]
        if boundary == "shared_kernel":
            monkeypatch.setattr(engine, "execute_sdk", unexpected_replacement)
        else:
            monkeypatch.setattr(
                type(app.provider), "stream_with_diagnostics", unexpected_replacement,
            )
        with pytest.raises(ServiceError, match="target_binding_changed"):
            binding.assert_current()


async def test_retired_run_idempotency_and_cold_history_never_retarget(tmp_path):
    path = tmp_path / "history.sqlite"
    request = RunRequest(text="historical", request_key="old-demo", target="demo")
    store = await SQLiteStore.open(path)
    try:
        run, created = await store.begin(request, {"key": "demo", "version": "historical"})
        assert created
        await store.finish(run.id, "interrupted", "cancelled")
    finally:
        await store.close()
    async with await open_application(path) as app:
        existing = await app.service.start(request)
        assert existing.id == run.id
        snapshot = await app.store.snapshot(run.id)
        assert snapshot.request.target == "demo"
        assert snapshot.target["key"] == "demo"
        assert not app.service.tasks
