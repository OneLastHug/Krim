from __future__ import annotations

import json
from pathlib import Path

from krim_agent.config_migration import _write_validated, migrate


def test_migration_references_env_secrets_without_copying_values(tmp_path: Path) -> None:
    secret = "migration-provider-secret-value"
    env_file = tmp_path / ".env"
    env_file.write_text(
        """KRIM_AGENT_PROVIDER=script
KRIM_AGENT_SYSTEM_PROMPT=legacy math prompt text
KRIM_AGENT_TARGET_VERSION=legacy-target-v1
KRIM_AGENT_DATABASE=data/krim.sqlite
KRIM_AGENT_READ_PARALLELISM=8
KRIM_AGENT_MAX_STEPS=5
KRIM_AGENT_HTTP_BASE_URL=https://provider.example/v1
KRIM_AGENT_HTTP_MODEL=sample-model
KRIM_AGENT_HTTP_API_KEY=migration-provider-secret-value
""",
        encoding="utf-8",
    )
    env_file.chmod(0o600)

    config, diagnostics, conflicts = migrate(
        env_file=env_file,
        inputs={name: None for name in ("sandbox", "development", "workspace", "tools", "access")},
    )

    assert not conflicts
    assert config["execution"] == {"parallelism": 8, "max_steps": 5}
    assert config["model"]["api_key"] == {"env": "KRIM_AGENT_HTTP_API_KEY"}
    assert "KRIM_AGENT_PROVIDER: retired; not migrated" in diagnostics
    assert "KRIM_AGENT_TARGET_VERSION: retired; not migrated" in diagnostics
    assert "KRIM_AGENT_SYSTEM_PROMPT: retired; review prompt manually" in diagnostics
    output = tmp_path / "krim.yaml"
    _write_validated(output, config)
    emitted = output.read_text(encoding="utf-8")
    assert "KRIM_AGENT_HTTP_API_KEY" in emitted
    assert "legacy math prompt text" not in emitted
    assert secret not in emitted


def test_access_json_tokens_are_replaced_by_secret_references(tmp_path: Path) -> None:
    secret = "legacy-access-token-must-not-appear"
    access_file = tmp_path / "access.json"
    access_file.write_text(
        json.dumps(
            {
                "credentials": [
                    {
                        "principal_id": "operator",
                        "token": secret,
                        "memberships": [
                            {"workspace_id": "default", "role": "admin", "approve": True}
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    access_file.chmod(0o600)

    config, diagnostics, conflicts = migrate(
        env_file=None,
        inputs={
            "sandbox": None,
            "development": None,
            "workspace": None,
            "tools": None,
            "access": access_file,
        },
    )

    assert not conflicts
    assert config["access"]["policy"]["credentials"][0]["token"] == {
        "env": "KRIM_ACCESS_OPERATOR_TOKEN"
    }
    assert all(secret not in item for item in diagnostics)
    output = tmp_path / "krim.yaml"
    _write_validated(output, config)
    assert secret not in output.read_text(encoding="utf-8")
    assert "KRIM_ACCESS_OPERATOR_TOKEN" in output.read_text(encoding="utf-8")


def test_development_limits_conflicting_with_execution_require_resolution(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("KRIM_AGENT_MAX_STEPS=8\n", encoding="utf-8")
    development_file = tmp_path / "development.json"
    development_file.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "resource_policy": "unlimited",
                "image_id": "sha256:" + "0" * 64,
                "broker_environments_root": "/srv/krim/envs",
                "private_state_root": "/srv/krim/state",
                "max_steps": 2,
                "max_tool_calls": None,
            }
        ),
        encoding="utf-8",
    )

    config, diagnostics, conflicts = migrate(
        env_file=env_file,
        inputs={
            "sandbox": None,
            "development": development_file,
            "workspace": None,
            "tools": None,
            "access": None,
        },
    )

    assert "execution.max_steps" in conflicts
    assert "development.max_steps: moved to execution.max_steps" in diagnostics
    assert config["environment"]["development"].get("max_steps") is None
