"""Completed SDK workers may adopt code-only updates for a fresh task."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from krim_agent.contracts import fingerprint
from krim_agent.subagents.configuration import SubagentProfileConfiguration
from krim_agent.subagents.errors import SubagentError
from krim_agent.subagents.providers.base import ProviderRegistry
from krim_agent.subagents.providers.sdk import SdkSessionProvider
from krim_agent.subagents.service import SubagentService


def fixture(change=None):
    profile = SubagentProfileConfiguration(workspace="isolated_write")
    current = {
        "key": "subagent:target-development:worker", "schema_version": 2,
        "provider": {"digest": "a" * 64, "implementation_digest": "a" * 64,
                     "config": {"model": "worker"}},
        "tools": [{"name": "shell", "implementation_digest": "a" * 64,
                   "input_schema": {"type": "object"}}],
        "system_prompt": "worker task", "limits": {},
    }
    prior = deepcopy(current)
    prior["provider"]["digest"] = "b" * 64
    prior["provider"]["implementation_digest"] = "b" * 64
    status = "completed"
    if change == "model":
        prior["provider"]["config"]["model"] = "different"
    elif change == "schema":
        prior["tools"][0]["input_schema"] = {"type": "string"}
    elif change == "prompt":
        prior["system_prompt"] = "different task"
    elif change == "active":
        status = "running"
    member = {
        "provider": "sdk-worker", "provider_version": fingerprint(prior),
        "profile": "worker", "profile_version": fingerprint(profile.model_dump(mode="json")),
        "task": {"run_id": "old-child"}, "activation": None,
    }
    if change == "profile":
        member["profile_version"] = "c" * 64
    elif change == "unbound":
        member["provider_version"] = "c" * 64

    async def snapshot(run_id):
        assert run_id == "old-child"
        return SimpleNamespace(target={**prior, "context": {"schema_version": 5}},
                               run=SimpleNamespace(status=status))

    service = object.__new__(SubagentService)
    service.store = SimpleNamespace(snapshot=snapshot)
    service.providers = ProviderRegistry()
    service._profiles = {"worker": profile}
    service._activations = {}
    provider = SdkSessionProvider(None, current)
    registered = service.providers.register(
        "sdk-worker", provider, version=provider.version, capabilities=provider.capabilities,
    )
    return service, member, registered


async def test_fresh_followup_accepts_implementation_only_route_change():
    service, member, registered = fixture()
    with pytest.raises(SubagentError, match="subagent_not_recoverable"):
        service._registration(member, cold=True)
    assert await service._followup_registration(member) is registered
    # Route selection must not rewrite old member/task/run facts.
    assert member["provider_version"] != registered.version


@pytest.mark.parametrize("change", ["model", "schema", "prompt", "profile", "active", "unbound"])
async def test_followup_keeps_frozen_contract_and_terminal_fences(change):
    service, member, _ = fixture(change)
    with pytest.raises(SubagentError, match="subagent_not_recoverable"):
        await service._followup_registration(member)
