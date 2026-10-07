"""Provider/工具版本登记、旧实例冻结、停用和生命周期所有权。"""

import pytest

from krim_agent.agents import AgentCatalog, AgentDefinition, AgentProfile, AgentProfileUpdate
from krim_agent.agents.resources import provider_fingerprint
from krim_agent.application.agents import AgentPromptRequest, AgentService
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import FakeProvider


class VersionedProvider(FakeProvider):
    def __init__(self, version: str):
        super().__init__(delay=0)
        self.name = f"fake-{version}"
        self.version = version


async def test_resources_expose_public_fingerprints_without_secret_or_callable(runtime):
    service = AgentService(runtime, runtime.settings, providers={"blue": VersionedProvider("v1")})
    try:
        service.register_provider("blue", service.providers["blue"], version="v1")
        service.register_tool_version("add", "builtin-2026-10")
        resources = service.resource_inventory()
        provider = next(item for item in resources if item.key == "blue")
        tool = next(item for item in resources if item.key == "add")
        assert provider.version == "v1" and provider.owned is False and provider.available
        assert tool.version == "builtin-2026-10"
        dumped = str([item.model_dump() for item in resources])
        assert "callable" not in dumped and "api_key" not in dumped
        with pytest.raises(ValueError) as bad:
            service.register_provider("invalid key", VersionedProvider("v2"), version="v2")
        assert "Provider key" in str(bad.value)
    finally:
        await service.close()


async def test_profile_freezes_provider_version_and_rejects_changed_resource_on_restore(runtime):
    first = VersionedProvider("v1")
    service = AgentService(runtime, runtime.settings, providers={"blue": first})
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name="blue-worker", description="版本资源", provider_key="blue", tool_names=["echo"]
            ),
        )
    )
    view = await service.spawn("blue-worker", persistent=True)
    await service.execute(
        view.agent_id, AgentPromptRequest(prompt="/echo old", idempotency_key="old")
    )
    old_hash = service.manager._handles[view.agent_id].definition.resource_version
    await service.close()
    second = VersionedProvider("v2")
    cold = AgentService(runtime, runtime.settings, providers={"blue": second})
    cold.register_provider("blue", second, version="v2")
    try:
        assert await cold.restore_persistent() == 0
        inventory = await cold.inventory()
        assert inventory[0].error_code == "agent_definition_changed"
        assert cold.resource_inventory()[0].fingerprint != provider_fingerprint(first)
        with pytest.raises(ServiceError) as denied:
            await cold.restore(view.agent_id)
        assert denied.value.code == "agent_definition_changed"
        assert old_hash != (await cold.definition("blue-worker")).resource_version
    finally:
        await cold.close()


async def test_owned_provider_closes_once_and_runtime_provider_cannot_be_owned(runtime):
    class Owned(VersionedProvider):
        def __init__(self):
            super().__init__("owned")
            self.closed = 0

        async def close(self):
            self.closed += 1

    owned = Owned()
    service = AgentService(runtime, runtime.settings)
    try:
        service.register_provider("owned", owned, version="v1", owned=True)
        with pytest.raises(ValueError):
            service.register_provider("runtime", Owned(), version="v2", owned=True)
    finally:
        await service.close()
        assert owned.closed == 1
        await service.close()
        assert owned.closed == 1


async def test_profile_resource_update_does_not_change_existing_agent(runtime):
    first = VersionedProvider("v1")
    service = AgentService(runtime, runtime.settings, providers={"blue": first})
    try:
        service.register_provider("blue", first, version="v1")
        await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name="worker", description="固定", provider_key="blue", tool_names=["echo"]
                ),
            )
        )
        view = await service.spawn("worker", persistent=True)
        old = service.manager._handles[view.agent_id].definition.resource_version
        second = VersionedProvider("v2")
        service.register_provider("blue", second, version="v2")
        assert service.manager._handles[view.agent_id].definition.resource_version == old
        fresh = await service.spawn("worker")
        assert service.manager._handles[fresh.agent_id].definition.resource_version != old
    finally:
        await service.close()


async def test_same_provider_class_and_name_version_change_requires_original_resource(runtime):
    service = AgentService(runtime, runtime.settings)
    service.register_provider("blue", runtime.provider, version="v1")
    service.register_tool_version("add", "add-v1")
    config = AgentProfile(
        name="versioned", description="版本边界", provider_key="blue", tool_names=["add"]
    )
    await service.profile_put(AgentProfileUpdate(expected_revision=0, config=config))
    view = await service.spawn(config.name, persistent=True)
    manifest = (await service.session(view.agent_id)).permissions["resources"]
    assert manifest["provider_version"] == "v1" and manifest["tool_versions"]["add"] == "add-v1"
    await service.close()
    cold = AgentService(runtime, runtime.settings)
    try:
        cold.register_provider("blue", runtime.provider, version="v2")
        cold.register_tool_version("add", "add-v1")
        assert await cold.restore_persistent() == 0
        assert (await cold.inventory())[0].error_code == "agent_definition_changed"
        cold.register_provider("blue", runtime.provider, version="v1")
        restored = await cold.restore(view.agent_id)
        assert restored.agent_id == view.agent_id
        cold.register_tool_version("echo", "unrelated-change")
        assert cold.manager.definition_hash(
            await cold.definition(config.name)
        ) == cold.manager.definition_hash(cold.manager._handles[view.agent_id].definition)
    finally:
        await cold.close()


async def test_missing_resource_and_disabled_provider_do_not_break_other_directory_entries(runtime):
    service = AgentService(runtime, runtime.settings, providers={"blue": runtime.provider})
    try:
        config = AgentProfile(name="missing", description="保留目录", provider_key="blue")
        await service.profile_put(AgentProfileUpdate(expected_revision=0, config=config))
        service.disable_provider("blue")
        directory = await service.catalog_all()
        assert next(item for item in directory if item.name == "default").available
        unavailable = next(item for item in directory if item.name == config.name)
        assert not unavailable.available and unavailable.error_code == "agent_profile_provider"
        with pytest.raises(ServiceError) as rejected:
            await service.spawn(config.name)
        assert rejected.value.code == "agent_profile_provider"
        service.register_provider("blue", runtime.provider, version="v1")
        assert (await service.spawn(config.name)).status == "created"
    finally:
        await service.close()


async def test_owned_replaced_and_borrowed_resources_close_after_all_agent_production(runtime):
    import asyncio

    from krim_agent.providers import ModelText

    class Owned:
        name = "owned-slow"

        def __init__(self):
            self.entered = asyncio.Event()
            self.production_closed = asyncio.Event()
            self.close_calls = 0

        async def stream(self, _messages, _tools):
            self.entered.set()
            try:
                yield ModelText(text="partial")
                await asyncio.Event().wait()
            finally:
                self.production_closed.set()

        async def close(self):
            assert self.production_closed.is_set()
            self.close_calls += 1

    original, replacement = Owned(), VersionedProvider("replacement")
    service = AgentService(runtime, runtime.settings)
    service.register_provider("owned", original, version="v1", owned=True)
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(name="slow", description="拥有资源", provider_key="owned"),
        )
    )
    agent = await service.spawn("slow")
    await service.start(agent.agent_id, AgentPromptRequest(prompt="wait", idempotency_key="wait"))
    await asyncio.wait_for(original.entered.wait(), 5)
    service.register_provider("owned", replacement, version="v2")
    assert original.close_calls == 0
    await service.close()
    assert original.close_calls == 1 and original.production_closed.is_set()


async def test_provider_config_hash_changes_endpoint_but_never_includes_api_key(runtime):
    from pydantic import SecretStr

    from krim_agent.agents import AgentManager
    from krim_agent.providers.openai_compatible import (
        OpenAICompatibleConfig,
        OpenAICompatibleProvider,
    )
    from krim_agent.tools import ToolRegistry

    first = OpenAICompatibleProvider(
        OpenAICompatibleConfig(
            base_url="http://127.0.0.1:1/v1", model="test", api_key=SecretStr("secret-one")
        )
    )
    rotation = OpenAICompatibleProvider(
        OpenAICompatibleConfig(
            base_url="http://127.0.0.1:1/v1", model="test", api_key=SecretStr("secret-two")
        )
    )
    moved = OpenAICompatibleProvider(
        OpenAICompatibleConfig(base_url="http://127.0.0.1:2/v1", model="test")
    )
    try:
        assert provider_fingerprint(first) == provider_fingerprint(rotation)
        assert provider_fingerprint(first) != provider_fingerprint(moved)
        definitions = [
            AgentDefinition(name="compatible", provider=p, tools=ToolRegistry())
            for p in [first, rotation, moved]
        ]
        assert AgentManager.definition_hash(definitions[0]) == AgentManager.definition_hash(
            definitions[1]
        )
        assert AgentManager.definition_hash(definitions[0]) != AgentManager.definition_hash(
            definitions[2]
        )
    finally:
        await first.close()
        await rotation.close()
        await moved.close()


async def test_persistent_children_restore_after_parent_regardless_of_inventory_sort(runtime):
    from dataclasses import replace

    service = AgentService(runtime, runtime.settings)
    parent = await service.manager.spawn(
        replace(await service.definition("default"), agent_id="z-parent", persistent=True)
    )
    child_definition = AgentDefinition(name="child", provider=runtime.provider, tools=runtime.tools)
    service.catalog.register("child", child_definition, description="持久子", expose_as_tool=False)
    child = await service.manager.spawn(
        replace(
            child_definition, agent_id="a-child", persistent=True, parent_agent_id=parent.agent_id
        )
    )
    await child.execute("/add 2 3")
    await service.close()
    cold = AgentService(runtime, runtime.settings)
    cold.catalog.register("child", child_definition, description="持久子", expose_as_tool=False)
    try:
        assert await cold.restore_persistent() == 2
        restored = await cold.get(child.agent_id)
        assert restored.session.permissions == (await cold.session(child.agent_id)).permissions
        assert restored.session.parent_session_id == parent.session_id and restored.turn == 1
    finally:
        await cold.close()


async def test_resource_client_cli_use_same_public_inventory(server_url):
    import asyncio
    import json
    import sys

    from krim_agent.client import KrimClient

    def read():
        with KrimClient(server_url) as client:
            return client.agent_resources()

    resources = await asyncio.to_thread(read)
    assert next(item for item in resources if item.key == "runtime").kind == "provider"
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "krim_agent",
        "agent",
        "resources",
        "--url",
        server_url,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    output, errors = await asyncio.wait_for(process.communicate(), 20)
    assert process.returncode == 0, errors.decode()
    assert json.loads(output) == [item.model_dump(mode="json") for item in resources]


async def test_explicit_profile_does_not_add_unselected_runtime_tools(runtime):
    service = AgentService(runtime, runtime.settings)
    try:
        await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name="bounded",
                    description="严格工具子集",
                    tool_names=["add"],
                ),
            )
        )
        agent = await service.spawn("bounded")
        handle = service.manager._handles[agent.agent_id]
        assert {tool.name for tool in handle.runtime.tools.definitions} == {"add"}
        before = service.manager.definition_hash(handle.definition)
        runtime.tools._definitions.pop("echo")
        assert service.manager.definition_hash(handle.definition) == before
    finally:
        await service.close()


async def test_long_profile_name_has_bounded_stable_model_tool_name(runtime):
    service = AgentService(runtime, runtime.settings)
    try:
        name = "a" * 64
        await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0, config=AgentProfile(name=name, description="长名称")
            )
        )
        tool = await service.as_tool(name, 1)
        assert len(tool.name) == 64 and tool.name == AgentCatalog.tool_name(name)
        assert (await service.catalog_all())[-1].tool_name == tool.name
    finally:
        await service.close()


async def test_owned_close_failure_still_closes_other_resources_and_sdk_owner(monkeypatch):
    from krim_agent.sdk import PythonAI

    class Tracked(FakeProvider):
        def __init__(self):
            super().__init__(delay=0)
            self.closes = 0

        async def close(self):
            self.closes += 1

    class Faulty(Tracked):
        async def close(self):
            self.closes += 1
            raise RuntimeError("raw-secret-must-not-escape")

    root, bad, good = Tracked(), Faulty(), Tracked()
    monkeypatch.setattr("krim_agent.sdk.text.create_provider", lambda _settings: root)
    ai = await PythonAI.create()
    service = ai.agents()
    service.register_provider("bad", bad, version="v1", owned=True)
    service.register_provider("good", good, version="v1", owned=True)
    service.register_provider("same-good", good, version="v1", owned=True)
    with pytest.raises(ServiceError) as error:
        await ai.close()
    assert error.value.code == "agent_resource_close_failed" and "raw-secret" not in str(
        error.value
    )
    assert root.closes == bad.closes == good.closes == 1
    assert ai.runtime._closed
    await ai.close()
    assert root.closes == good.closes == 1


async def test_transient_recovery_keeps_history_and_never_resets_persistent_session(runtime):
    from sqlalchemy import update

    from krim_agent.persistence.database import AgentSessionRow

    service = AgentService(runtime, runtime.settings)
    ordinary = await service.spawn("default")
    retained = await service.spawn("default", persistent=True)
    await service.close()
    async with runtime.repository.database.sessions.begin() as db:
        await db.execute(
            update(AgentSessionRow)
            .where(AgentSessionRow.agent_id == ordinary.agent_id)
            .values(status="open", owner_id="dead-process")
        )
    assert await service.sessions.recover_transient() == 1
    assert (await service.session(ordinary.agent_id)).status == "destroyed"
    assert (await service.session(retained.agent_id)).status == "open"
