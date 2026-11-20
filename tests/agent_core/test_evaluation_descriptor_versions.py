"""Report partition validation after the shared-kernel descriptor migration."""

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.evaluation import continuous_reports


def descriptor(monkeypatch, version="ai-python/0.9.0-shared-host/1"):
    # Test this validator after the execution-conditions boundary; the actual
    # domain parser and complete product chain are covered by session-chain tests.
    monkeypatch.setattr(continuous_reports, "execution_conditions", lambda value: value)
    binding = {
        "schema_version": 2, "key": "target-http", "version": "agent/1",
        "provider": {}, "system_prompt": "", "prompt_fingerprint": "a" * 64,
        "tools": [], "policy": {}, "pre": {}, "post": {}, "limits": {},
        "limits_fingerprint": "b" * 64,
    }
    if version is not None:
        binding["agent_loop"] = version
    if version == "ai-python/0.9.0-shared-host/1":
        binding["execution_kernel"] = {"version": "sdk-execution/1", "digest": "c" * 64}
    conditions = {
        "schema_version": "krim-run/1", "binding_descriptor": binding,
        "budget": {}, "public_runtime_conditions": {},
    }
    return binding, {"execution_conditions": conditions}, {"execution_conditions": conditions}


def test_shared_kernel_identity_survives_report_partition(monkeypatch):
    binding, target, environment = descriptor(monkeypatch)
    _, common = continuous_reports.partition(target, environment)
    assert common["execution_conditions"]["constraints"]["execution_kernel"] == (
        binding["execution_kernel"]
    )


@pytest.mark.parametrize("version", [None, "ai-python/0.9.0-host-v1", "ai-python/0.9.0-host-v2"])
def test_historical_descriptors_remain_readable_without_relabeling(monkeypatch, version):
    binding, target, environment = descriptor(monkeypatch, version)
    continuous_reports.partition(target, environment)
    assert binding.get("agent_loop") == version
    assert "execution_kernel" not in binding


@pytest.mark.parametrize("kernel", [
    None,
    {"version": "sdk-execution/1"},
    {"version": "sdk-execution/1", "digest": "not-a-digest"},
    {"version": "unknown", "digest": "c" * 64},
    {"version": "sdk-execution/1", "digest": "c" * 64, "unexpected": "value"},
])
def test_new_shared_kernel_descriptor_must_be_complete_and_known(monkeypatch, kernel):
    binding, target, environment = descriptor(monkeypatch)
    binding["execution_kernel"] = kernel
    with pytest.raises(ServiceError, match="facts_corrupt"):
        continuous_reports.partition(target, environment)
