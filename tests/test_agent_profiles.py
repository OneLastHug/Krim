"""Agent版本配置、真实系统前缀、权限/工具子集与旧版本恢复。"""


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
