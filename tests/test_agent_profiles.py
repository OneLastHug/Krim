"""Agent版本配置、真实系统前缀、权限/工具子集与旧版本恢复。"""

import asyncio

import pytest

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentPromptRequest, AgentService
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import ModelText


def profile(**values):
    return AgentProfile(name="worker", description="可审阅的Agent", tool_names=["add"], **values)


async def test_configuration_cas_old_version_and_invalid_resources(runtime):
    service = AgentService(runtime, runtime.settings)
    try:
        first = await service.profile_put(AgentProfileUpdate(expected_revision=0, config=profile()))
        assert first.revision == 1
        with pytest.raises(ServiceError, match="版本"):
            await service.profile_put(AgentProfileUpdate(expected_revision=0, config=profile()))
        changed = first.config.model_copy(update={"instructions": "新的system指令"})
        second = await service.profile_put(AgentProfileUpdate(expected_revision=1, config=changed))
        assert second.revision == 2
        assert (await service.profile_get("worker", 1)).config.instructions == ""
        with pytest.raises(ServiceError) as missing:
            await service.profile_put(
                AgentProfileUpdate(
                    expected_revision=2,
                    config=changed.model_copy(update={"provider_key": "unknown"}),
                )
            )
        assert missing.value.code == "agent_profile_provider"
        assert (await service.profile_get("worker")).revision == 2
        with pytest.raises(ServiceError):
            await service.profile_put(
                AgentProfileUpdate(
                    expected_revision=0, config=profile().model_copy(update={"name": "default"})
                )
            )
    finally:
        await service.close()


async def test_real_system_prefix_persists_across_turns_without_rewriting_user_messages(runtime):
    seen = []

    class SystemAware:
        name = "system-aware"

        async def stream(self, messages, tools):
            seen.append([m.model_copy(deep=True) for m in messages])
            assert messages[0].role == "system" and messages[0].content == "遵守确定的任务规则"
            assert {t.name for t in tools} == {"add"}
            yield ModelText(text="遵守规则")

    runtime.provider = SystemAware()
    service = AgentService(runtime, runtime.settings)
    try:
        await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0, config=profile(instructions="遵守确定的任务规则")
            )
        )
        view = await service.spawn("worker")
        for i in range(2):
            result = await service.execute(
                view.agent_id, AgentPromptRequest(prompt=f"任务{i}", idempotency_key=f"turn-{i}")
            )
            assert result.status == "completed"
        messages = await runtime.repository.list_messages(view.topic_id)
        assert [m.content for m in messages if m.role == "user"] == ["任务0", "任务1"]
        assert len(seen) == 2 and len([m for m in seen[1] if m.role == "system"]) == 1
    finally:
        await service.close()


async def test_profile_update_does_not_mutate_live_instance_and_restore_uses_old_revision(runtime):
    service = AgentService(runtime, runtime.settings)
    await service.profile_put(
        AgentProfileUpdate(expected_revision=0, config=profile(instructions="旧指令"))
    )
    view = await service.spawn("worker", persistent=True)
    await service.execute(
        view.agent_id, AgentPromptRequest(prompt="第一轮", idempotency_key="first")
    )
    old_hash = service.manager.definition_hash(service.manager._handles[view.agent_id].definition)
    await service.profile_put(
        AgentProfileUpdate(expected_revision=1, config=profile(instructions="新指令"))
    )
    assert service.manager._handles[view.agent_id].definition.instructions == "旧指令"
    await service.close()
    reopened = AgentService(runtime, runtime.settings)
    try:
        assert await reopened.restore_persistent() == 1
        handle = reopened.manager._handles[view.agent_id]
        assert (
            handle.definition.profile_revision == 1 and handle.definition.instructions == "旧指令"
        )
        assert reopened.manager.definition_hash(handle.definition) == old_hash
        fresh = await reopened.spawn("worker")
        assert fresh.profile_revision == 2
        assert reopened.manager._handles[fresh.agent_id].definition.instructions == "新指令"
    finally:
        await reopened.close()


@pytest.mark.parametrize(
    "mode,allowed", [("agent", True), ("ask", False), ("plan", True), ("auto", True)]
)
async def test_modes_enforce_server_tool_policy(mode, allowed, runtime):
    service = AgentService(runtime, runtime.settings)
    try:
        await service.profile_put(
            AgentProfileUpdate(expected_revision=0, config=profile(mode=mode))
        )
        view = await service.spawn("worker")
        result = await service.execute(view.agent_id, AgentPromptRequest(prompt="/add 2 3"))
        audits = await runtime.repository.audits(result.current_run_id)
        assert len(audits) == 1 and audits[0].ok is allowed
        assert audits[0].name == "add"
    finally:
        await service.close()


async def test_public_http_profiles_and_frozen_version(api):
    payload = AgentProfileUpdate(expected_revision=0, config=profile()).model_dump(mode="json")
    created = await api.put("/v1/agents/profiles/worker", json=payload)
    assert created.status_code == 200 and created.json()["revision"] == 1
    assert (await api.put("/v1/agents/profiles/worker", json=payload)).status_code == 409
    spawned = await api.post("/v1/agents", json={"name": "worker", "profile_revision": 1})
    assert spawned.status_code == 201 and spawned.json()["profile_revision"] == 1
    result = await api.post(
        f"/v1/agents/{spawned.json()['agent_id']}/execute", json={"prompt": "/add 2 3"}
    )
    assert result.status_code == 200 and result.json()["status"] == "completed"
    assert (await api.get("/v1/agents/profiles")).json()[0]["name"] == "worker"


async def test_unavailable_inventory_explains_resource_and_can_restore_after_registration(runtime):
    service = AgentService(runtime, runtime.settings, providers={"custom": runtime.provider})
    await service.profile_put(
        AgentProfileUpdate(expected_revision=0, config=profile(provider_key="custom"))
    )
    view = await service.spawn("worker", persistent=True)
    await service.close()
    cold = AgentService(runtime, runtime.settings)
    try:
        assert await cold.restore_persistent() == 0
        item = (await cold.inventory())[0]
        assert not item.available and item.error_code == "agent_profile_provider"
        cold.providers["custom"] = runtime.provider
        restored = await cold.restore(view.agent_id)
        assert restored.agent_id == view.agent_id and restored.topic_id == view.topic_id
        assert (await cold.inventory())[0].available
    finally:
        await cold.close()


async def test_sdk_agent_service_owns_only_its_handles_and_persistent_cold_restore(tmp_path):
    from krim_agent.config import Settings
    from krim_agent.providers import FakeProvider
    from krim_agent.sdk import PythonAI

    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path / 'sdk.sqlite'}")
    async with await PythonAI.create(settings=settings, provider=FakeProvider(delay=0)) as ai:
        agents = ai.agents()
        await agents.profile_put(AgentProfileUpdate(expected_revision=0, config=profile()))
        view = await agents.spawn("worker", persistent=True)
        result = await agents.execute(view.agent_id, AgentPromptRequest(prompt="/add 2 3"))
        assert result.status == "completed"
        with pytest.raises(ServiceError) as busy:
            await PythonAI.create(settings=settings, provider=FakeProvider(delay=0))
        assert busy.value.code == "agent_inventory_busy"
    async with await PythonAI.create(settings=settings, provider=FakeProvider(delay=0)) as cold:
        assert await cold.agents().restore_persistent() == 1
        result = await cold.agents().get(view.agent_id)
        assert (
            result.profile_revision == 1 and result.result.text == '[fake] 工具结果：{"sum": 5.0}'
        )


async def test_real_tcp_cli_configure_create_and_execute(server_url, tmp_path):
    import json
    import os
    import sys

    path = tmp_path / "agent.json"
    path.write_text(profile().model_dump_json(), encoding="utf-8")

    async def cli(*arguments):
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "krim_agent",
            "agent",
            *arguments,
            "--url",
            server_url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={k: v for k, v in os.environ.items() if k != "KRIM_AGENT_API_KEY"},
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), 20)
        assert process.returncode == 0, stderr.decode()
        return json.loads(stdout)

    configured = await cli("configure", str(path))
    assert configured["revision"] == 1
    current = await cli("profile", "worker")
    assert current["config"]["tool_names"] == ["add"]
    view = await cli("spawn", "worker", "--revision", "1", "--persistent")
    result = await cli("execute", view["agent_id"], "/add 2 3")
    assert result["status"] == "completed" and '"sum": 5.0' in result["result"]["text"]
    inventory = await cli("inventory")
    assert inventory[0]["agent_id"] == view["agent_id"] and inventory[0]["available"]
    restored = await cli("restore", view["agent_id"])
    assert restored["current_run_id"] == result["current_run_id"]
    disabled = await cli("disable", "worker", "--revision", "1")
    assert disabled["revision"] == 2 and not disabled["enabled"]
    assert (await cli("profile", "worker", "--revision", "1"))["enabled"]
    assert not (await cli("profile", "worker"))["enabled"]
    enabled = await cli("enable", "worker", "--revision", "2")
    assert enabled["revision"] == 3 and enabled["enabled"]
    assert (await cli("get", view["agent_id"]))["profile_revision"] == 1


async def test_system_prefix_capacity_fails_before_provider_dispatch(runtime):
    from krim_agent.context_engine import ContextRunConfig

    class Never:
        name = "must-not-dispatch"
        calls = 0

        async def stream(self, _messages, _tools):
            self.calls += 1
            yield ModelText(text="bad")

    provider = Never()
    runtime.provider = provider
    service = AgentService(runtime, runtime.settings)
    try:
        config = profile(instructions="x " * 3000).model_copy(
            update={
                "tool_names": [],
                "context_config": ContextRunConfig(
                    tokenizer="cl100k_base",
                    reserved_output_tokens=16,
                    model_context_window_tokens=512,
                    model_framing_reserve_tokens=0,
                ),
            }
        )
        await service.profile_put(AgentProfileUpdate(expected_revision=0, config=config))
        view = await service.spawn("worker")
        result = await service.execute(view.agent_id, AgentPromptRequest(prompt="任务"))
        assert result.status == "failed" and provider.calls == 0
        assert result.result.code == "agent_instruction_budget"
    finally:
        await service.close()


async def test_real_process_restores_frozen_old_profile_after_head_update(tmp_path):
    import json
    import sys
    from pathlib import Path

    script = Path(__file__).parent / "fixtures/agent_profile_worker.py"

    async def run(*args):
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(script),
            str(tmp_path / "process.sqlite"),
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), 25)
        assert process.returncode == 0, stderr.decode()
        return json.loads(stdout)

    first = await run("create")
    cold = await run("restore", first["agent_id"])
    assert first["agent_id"] == cold["agent_id"] and first["topic_id"] == cold["topic_id"]
    assert first["run_id"] == cold["run_id"] and cold["calls"] == 0


async def test_versioned_profile_can_be_used_as_agent_tool_and_keeps_parent_permission(runtime):
    from krim_agent.tools import ToolContext
    from krim_agent.tools.core import Decision

    async def allow(_definition, _context) -> Decision:
        return "allow"

    runtime.tool_policy = allow
    service = AgentService(runtime, runtime.settings)
    try:
        await service.profile_put(AgentProfileUpdate(expected_revision=0, config=profile()))
        tool = await service.as_tool("worker", 1)
        runtime.tools.register(tool)
        result = await runtime.tools.call_tool(
            tool.name, {"prompt": "/add 2 3"}, ToolContext(runtime.tools, policy=allow)
        )
        assert result["status"] == "completed" and '"sum": 5.0' in result["text"]
        assert not service.manager._handles
    finally:
        await service.close()


async def test_disabled_head_blocks_new_historical_spawn_but_keeps_existing_instance(runtime):
    service = AgentService(runtime, runtime.settings)
    try:
        config = profile()
        await service.profile_put(AgentProfileUpdate(expected_revision=0, config=config))
        old = await service.spawn("worker", persistent=True)
        await service.profile_put(
            AgentProfileUpdate(expected_revision=1, config=config, enabled=False)
        )
        with pytest.raises(ServiceError) as disabled:
            await service.spawn("worker", profile_revision=1)
        assert disabled.value.code == "agent_profile_disabled"
        assert (await service.get(old.agent_id)).profile_revision == 1
    finally:
        await service.close()
