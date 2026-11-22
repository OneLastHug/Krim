from __future__ import annotations

from pathlib import Path

import pytest

from krim_agent.composition import open_application
from krim_agent.contracts import ServiceError
from krim_agent.subagents.configuration import SubagentProfileConfiguration, SubagentsConfiguration
from tests.support.fake_provider import FakeProvider


async def test_disabled_configuration_keeps_delegation_absent(tmp_path: Path) -> None:
    provider = FakeProvider()
    async with await open_application(tmp_path / "disabled.sqlite", provider=provider) as app:
        assert app.subagents is None
        assert app.service.delegates == {}
        schemas = app.service.bindings["target-http"].loop.executor.registry.schemas()
        assert "spawn_agent" not in {item["name"] for item in schemas}
    assert not provider.closed


async def test_enabled_composition_freezes_profile_routes_before_admission(tmp_path: Path) -> None:
    provider, fast = FakeProvider(), FakeProvider()
    configuration = SubagentsConfiguration(
        enabled=True,
        profiles={"reader": SubagentProfileConfiguration(model="fast", max_depth=1)},
    )
    async with await open_application(
        tmp_path / "enabled.sqlite", provider=provider,
        subagent_configuration=configuration, model_routes={"fast": fast},
    ) as app:
        assert app.subagents is not None
        assert app.service.lifecycle is app.subagents
        root = app.service.bindings["target-http"]
        root.assert_current()
        assert {"spawn_agent", "send_message", "wait_agent", "list_agents"} <= {
            item["name"] for item in root.loop.executor.registry.schemas()
        }
        default = app.service.delegates["subagent:target-http:default"]
        reader = app.service.delegates["subagent:target-http:reader"]
        assert default.loop.provider is provider
        assert reader.loop.provider is fast
        assert default.loop.subagents is app.subagents
        assert reader.loop.subagents is app.subagents
        default.assert_current()
        reader.assert_current()
        with pytest.raises(TypeError):
            app.service.delegates["forged"] = reader  # type: ignore[index]
    assert not provider.closed and not fast.closed


async def test_missing_named_route_fails_before_any_work_is_accepted(tmp_path: Path) -> None:
    configuration = SubagentsConfiguration(
        enabled=True, profiles={"reader": SubagentProfileConfiguration(model="missing")},
    )
    with pytest.raises(ServiceError) as error:
        await open_application(tmp_path / "missing.sqlite", provider=FakeProvider(),
                               subagent_configuration=configuration)
    assert error.value.code == "provider_not_configured"
