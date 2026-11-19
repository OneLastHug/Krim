"""Explicit unlimited policy migration keeps legacy intent separate from limits."""

import pytest
from pydantic import ValidationError

from krim_agent.development.configuration import DevelopmentConfiguration


def configuration(tmp_path, **changes):
    return DevelopmentConfiguration.model_validate({
        "image_id": "sha256:" + "a" * 64,
        "broker_environments_root": tmp_path / "environments",
        "private_state_root": tmp_path / "state",
        **changes,
    })


def test_default_configuration_is_unlimited(tmp_path):
    value = configuration(tmp_path)
    assert value.schema_version == 2
    assert value.resource_policy == "unlimited"
    assert value.history_compaction
    assert all(getattr(value, name) is None for name in (
        "cpu_cores", "memory_bytes", "pids_limit", "max_processes", "max_steps", "max_tool_calls"
    ))


def test_legacy_configuration_migrates_finite_values_explicitly(tmp_path):
    value = configuration(tmp_path, schema_version=1, cpu_cores=1.0,
                          memory_bytes=1073741824, pids_limit=256, max_processes=4,
                          max_steps=8, max_tool_calls=32, history_compaction=False)
    assert value.schema_version == 2 and value.resource_policy == "unlimited"
    assert value.cpu_cores is None and value.max_processes is None
    assert value.max_steps is None and value.max_tool_calls is None
    # An explicit user preference is not a resource ceiling.
    assert not value.history_compaction
    assert DevelopmentConfiguration.model_validate_json(value.model_dump_json()) == value


@pytest.mark.parametrize("legacy", [None, 0, 4])
def test_unlimited_null_zero_and_old_positive_limits(tmp_path, legacy):
    assert configuration(tmp_path, max_processes=legacy).max_processes is None


@pytest.mark.parametrize("changes", [
    {"max_processes": True}, {"max_processes": -1}, {"max_steps": "8"},
    {"cpu_cores": float("inf")}, {"cpu_cores": float("nan")},
    {"schema_version": True}, {"schema_version": 1.0},
])
def test_unlimited_migration_rejects_invalid_configuration(tmp_path, changes):
    with pytest.raises(ValidationError):
        configuration(tmp_path, **changes)


def test_explicit_limited_policy_preserves_ceilings_without_old_maximums(tmp_path):
    value = configuration(tmp_path, resource_policy="limited", max_processes=100,
                          cpu_cores=128, memory_bytes=2**40, pids_limit=100000,
                          max_steps=1000, max_tool_calls=10000)
    assert value.max_processes == 100 and value.cpu_cores == 128
    assert value.max_steps == 1000
    with pytest.raises(ValidationError):
        configuration(tmp_path, resource_policy="limited", max_processes=0)
    with pytest.raises(ValidationError):
        configuration(tmp_path, schema_version=3)
