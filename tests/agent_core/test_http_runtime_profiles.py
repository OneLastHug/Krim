"""通用宿主配置复用真实本机 HTTP，不调用商业模型。"""

import pytest

from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import RunRequest, ServiceError
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig
from krim_agent.sdk import SessionSDK
from krim_agent.sessions.types import SessionCreate, TurnRequest


def profile(path, **changes):
    return Settings(
        database=path,
        provider="http",
        http_mode="agent",
        max_active_runs=3,
        max_steps=12,
        max_tool_calls=10,
        max_tools_per_step=2,
        run_timeout=90,
        **changes,
    )


def http_config(wire, **changes):
    return OpenAICompatibleConfig(
        base_url=f"http://127.0.0.1:{wire.port}/v1",
        model="trusted-host-model",
        max_tool_calls=2,
        read_timeout_seconds=20,
        **changes,
    )


async def test_agent_http_uses_host_model_and_real_tool_wire(tmp_path, wire):
    path = tmp_path / "generic.sqlite"
    async with await open_application(
        path,
        settings=profile(path),
        http_config=http_config(wire),
        registry=wire.registry(),
    ) as app:
        binding = app.service.bindings["target-http"]
        assert binding.settings.max_steps == 12
        assert binding.settings.max_tool_calls == 10
        assert binding.settings.max_tools_per_step == 2
        assert binding.settings.target_version == "agent-http/1"
        started = await app.service.start(
            RunRequest(text="/add 2 3", request_key="generic-add", target="target-http")
        )
        assert (await app.service.wait(started.id)).status == "completed"
        assert (await app.service.result(started.id)).text == "5"
        assert len(wire.requests) == 2
        assert all(body["model"] == "trusted-host-model" for body in wire.requests)
        assert all("reasoning_effort" not in body for body in wire.requests)
        assert wire.executions


async def test_agent_http_session_keeps_previous_turn_context(tmp_path, wire):
    path = tmp_path / "generic-session.sqlite"
    async with await open_application(
        path,
        settings=profile(path),
        http_config=http_config(wire),
        registry=wire.registry(),
    ) as app:
        sdk = SessionSDK(app.sessions)
        session = await sdk.create(SessionCreate(request_key="session", target_key="target-http"))
        first = await sdk.submit(
            session.id,
            TurnRequest(request_key="base", text="基数为7", expected_revision=session.revision),
        )
        assert (await sdk.wait(session.id, first.id)).status == "completed"
        current = await sdk.get(session.id)
        second = await sdk.submit(
            session.id,
            TurnRequest(request_key="follow", text="与3相加", expected_revision=current.revision),
        )
        assert (await sdk.wait(session.id, second.id)).status == "completed"
        result = await sdk.result(session.id, second.id)
        assert result.text == "10"
        follow = [body for body in wire.requests if any(
            message.get("content") == "与3相加" for message in body["messages"]
        )]
        assert follow
        assert any(message.get("content") == "基数为7" for message in follow[0]["messages"])
        assert any(message["role"] == "tool" for message in follow[0]["messages"])


@pytest.mark.parametrize("mode", ["unknown", None, []])
async def test_invalid_http_mode_rejected_before_database(tmp_path, wire, mode):
    path = tmp_path / "not-created.sqlite"
    settings = profile(path).model_copy(update={"http_mode": mode})
    with pytest.raises(ServiceError, match="provider_config"):
        await open_application(path, settings=settings, http_config=http_config(wire))
    assert not path.exists()
    assert not wire.requests


async def test_agent_mode_does_not_silently_select_script(tmp_path):
    path = tmp_path / "script-fallback.sqlite"
    with pytest.raises(ServiceError, match="provider_config"):
        await open_application(path, settings=Settings(http_mode="agent"))
    assert not path.exists()


async def test_agent_http_rejects_tool_budget_widening(tmp_path, wire):
    path = tmp_path / "too-wide.sqlite"
    settings = profile(path).model_copy(update={"max_tools_per_step": 1})
    with pytest.raises(ServiceError, match="provider_config"):
        await open_application(path, settings=settings, http_config=http_config(wire))
    assert not path.exists()
    assert not wire.requests
