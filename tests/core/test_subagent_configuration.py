from __future__ import annotations

import json
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
from pydantic import ValidationError

from krim_agent.configuration import ConfigurationError, load_configuration
from krim_agent.subagents.configuration import (
    DEFAULT_WORKER_PROFILE,
    SubagentsConfiguration,
    resolve_worker_profile,
)


def _write_config(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    path.chmod(0o600)
    return path


def test_configuration_without_new_sections_stays_compatible(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path / "krim.yaml", "schema_version: 1\n")

    configuration = load_configuration(config_path, environment={})

    assert configuration.models == {}
    assert configuration.subagents == SubagentsConfiguration()
    assert configuration.subagents.enabled is False
    assert configuration.snapshot["models"] == {}
    assert configuration.snapshot["subagents"]["enabled"] is False


def test_named_models_and_profiles_resolve_with_secret_references(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path / "krim.yaml",
        """schema_version: 1
model:
  base_url: https://primary.example/v1
  model: primary-model
  api_key:
    env: KRIM_PRIMARY_KEY
models:
  fast:
    base_url: https://fast.example/v1
    model: fast-model
    api_key:
      env: KRIM_FAST_KEY
    max_output_tokens: 1024
subagents:
  enabled: true
  profiles:
    research:
      model: fast
      tools: [read_file, grep]
      workspace: isolated_write
      max_depth: 1
      max_output_tokens: 512
""",
    )

    configuration = load_configuration(
        config_path,
        environment={"KRIM_PRIMARY_KEY": "primary-secret", "KRIM_FAST_KEY": "fast-secret"},
    )

    assert set(configuration.models) == {"fast"}
    fast = configuration.models["fast"]
    assert fast.model == "fast-model"
    assert fast.api_key.get_secret_value() == "fast-secret"
    assert fast.max_output_tokens == 1024
    assert configuration.model is not None
    assert configuration.model.api_key.get_secret_value() == "primary-secret"
    assert configuration.subagents.enabled is True
    profile = configuration.subagents.profiles["research"]
    assert profile.model == "fast"
    assert profile.provider == "sdk_session"
    assert profile.tools == ("read_file", "grep")
    assert profile.workspace == "isolated_write"
    assert profile.max_depth == 1
    assert profile.max_output_tokens == 512
    serialized = json.dumps(configuration.snapshot, sort_keys=True)
    assert "KRIM_FAST_KEY" in serialized
    assert "KRIM_PRIMARY_KEY" in serialized
    assert "fast-secret" not in serialized
    assert "primary-secret" not in serialized


def test_worker_profile_resolution_prefers_default_profile(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path / "krim.yaml",
        """schema_version: 1
model:
  base_url: https://primary.example/v1
  model: primary-model
subagents:
  enabled: true
  profiles:
    worker:
      workspace: isolated_write
  default_profile: worker
""",
    )

    configuration = load_configuration(config_path, environment={})
    subagents = configuration.subagents

    assert resolve_worker_profile(subagents, None) == subagents.profiles["worker"]
    assert resolve_worker_profile(subagents, "worker") == subagents.profiles["worker"]
    with pytest.raises(KeyError):
        resolve_worker_profile(subagents, "missing")
    assert resolve_worker_profile(SubagentsConfiguration(), None) == DEFAULT_WORKER_PROFILE
    assert DEFAULT_WORKER_PROFILE.model == "default"
    assert DEFAULT_WORKER_PROFILE.provider == "sdk_session"
    assert DEFAULT_WORKER_PROFILE.workspace == "shared_read_only"


def test_process_provider_routes_reference_configured_argv(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path / "krim.yaml",
        """schema_version: 1
model:
  base_url: https://primary.example/v1
  model: primary-model
subagents:
  enabled: true
  profiles:
    batch:
      provider: process
      process: one_shot
  processes:
    one_shot:
      argv: [krim-worker, --once]
""",
    )

    configuration = load_configuration(config_path, environment={})

    profile = configuration.subagents.profiles["batch"]
    assert profile.provider == "process"
    assert profile.process == "one_shot"
    assert configuration.subagents.processes["one_shot"].argv == ("krim-worker", "--once")

    conflicting = _write_config(
        tmp_path / "conflict.yaml",
        """schema_version: 1
model:
  base_url: https://primary.example/v1
  model: primary-model
subagents:
  profiles:
    research:
      provider: sdk_session
      process: one_shot
  processes:
    one_shot:
      argv: [krim-worker, --once]
""",
    )
    with pytest.raises(ConfigurationError) as exc:
        load_configuration(conflicting, environment={})
    assert exc.value.diagnostic == "schema:subagents.profiles.research:value_error"


@pytest.mark.parametrize(
    ("content", "expected_diagnostic"),
    [
        (
            """schema_version: 1
model:
  base_url: https://primary.example/v1
  model: primary-model
subagents:
  profiles:
    research:
      model: fast
""",
            "subagents.profiles.research.model:unknown_route",
        ),
        (
            """schema_version: 1
subagents:
  profiles:
    research: {}
""",
            "subagents.profiles.research.model:default_route_unconfigured",
        ),
        (
            """schema_version: 1
subagents:
  default_profile: missing
""",
            "schema:subagents:value_error",
        ),
        (
            """schema_version: 1
models:
  default:
    base_url: https://primary.example/v1
    model: primary-model
""",
            "models.default:reserved",
        ),
        (
            """schema_version: 1
models:
  Bad_Name:
    base_url: https://primary.example/v1
    model: primary-model
""",
            "models.Bad_Name:invalid_name",
        ),
        (
            """schema_version: 1
model:
  base_url: https://primary.example/v1
  model: primary-model
subagents:
  profiles:
    research:
      provider: process
""",
            "schema:subagents.profiles.research:value_error",
        ),
        (
            """schema_version: 1
model:
  base_url: https://primary.example/v1
  model: primary-model
subagents:
  profiles:
    research:
      provider: process
      process: absent_entry
  processes:
    one_shot:
      argv: [krim-worker, --once]
""",
            "subagents.profiles.research.process:unknown_process",
        ),
        (
            """schema_version: 1
subagents:
  processes:
    batch:
      argv: []
""",
            "schema:subagents.processes.batch.argv",
        ),
    ],
)
def test_route_references_and_names_are_validated_at_startup(
    tmp_path: Path, content: str, expected_diagnostic: str
) -> None:
    config_path = _write_config(tmp_path / "krim.yaml", content)
    with pytest.raises(ConfigurationError) as exc:
        load_configuration(config_path, environment={})
    assert exc.value.code == "invalid_input"
    assert exc.value.diagnostic.startswith(expected_diagnostic)


def test_named_model_secrets_follow_primary_reference_semantics(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path / "krim.yaml",
        """schema_version: 1
models:
  fast:
    base_url: https://fast.example/v1
    model: fast-model
    api_key:
      env: KRIM_FAST_KEY
""",
    )

    with pytest.raises(ConfigurationError) as exc:
        load_configuration(config_path, environment={})
    assert exc.value.diagnostic == "secret:KRIM_FAST_KEY:missing"

    plaintext = _write_config(
        tmp_path / "plaintext.yaml",
        """schema_version: 1
models:
  fast:
    base_url: https://fast.example/v1
    model: fast-model
    api_key: plaintext-is-rejected
""",
    )
    with pytest.raises(ConfigurationError) as exc:
        load_configuration(plaintext, environment={})
    assert exc.value.code == "invalid_input"
    assert exc.value.diagnostic.startswith("schema:models")


def test_named_model_routes_stay_on_the_existing_http_provider_shape(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path / "krim.yaml",
        """schema_version: 1
models:
  fast:
    base_url: https://fast.example/v1
    model: fast-model
    wire_api: anthropic_messages
""",
    )

    with pytest.raises(ConfigurationError) as exc:
        load_configuration(config_path, environment={})
    assert exc.value.diagnostic == "schema:wire_api:literal_error"


def test_named_model_references_are_not_reported_as_legacy(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path / "krim.yaml",
        """schema_version: 1
models:
  fast:
    base_url: https://fast.example/v1
    model: fast-model
    api_key:
      env: KRIM_FAST_KEY
""",
    )

    configuration = load_configuration(
        config_path,
        environment={"KRIM_FAST_KEY": "fast-secret", "KRIM_AGENT_PROVIDER": "script"},
    )

    assert "KRIM_FAST_KEY" not in configuration.legacy_environment_variables
    assert "KRIM_AGENT_PROVIDER" in configuration.legacy_environment_variables


def test_limit_defaults_cover_each_resource_domain() -> None:
    limits = SubagentsConfiguration()

    assert limits.enabled is False
    assert limits.max_depth == 2
    assert limits.max_resident_agents == 8
    assert limits.max_model_requests == 4
    assert limits.max_tool_processes == 16
    assert limits.max_message_bytes == 262_144
    assert limits.max_queued_messages == 128
    assert limits.max_event_bytes == 65_536
    assert limits.max_cached_events == 4_096
    assert limits.max_requests_per_tree is None


def test_loaded_configuration_is_frozen_after_startup(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path / "krim.yaml", "schema_version: 1\n")
    configuration = load_configuration(config_path, environment={})

    with pytest.raises(FrozenInstanceError):
        configuration.models = {}  # type: ignore[misc]
    with pytest.raises(ValidationError):
        configuration.subagents.enabled = True
