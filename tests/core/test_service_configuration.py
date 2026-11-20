from __future__ import annotations

import json
from pathlib import Path

import pytest

from krim_agent.config import DEFAULT_TOOL_PARALLELISM, Settings
from krim_agent.configuration import ConfigurationError, load_configuration
from krim_agent.contracts import ServiceError
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig


def _write_config(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    path.chmod(0o600)
    return path


def test_default_configuration_is_real_provider_and_relative_to_main_file(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path / "krim.yaml", "schema_version: 1\n")

    configuration = load_configuration(config_path, environment={})

    assert configuration.model is None
    assert not configuration.ready_for_execution
    assert configuration.settings.provider == "http"
    assert configuration.settings.read_parallelism == DEFAULT_TOOL_PARALLELISM
    assert configuration.settings.database == tmp_path / ".data/krim-agent.db"
    assert configuration.snapshot["model"] is None
    assert not hasattr(Settings, "from_env")
    assert not hasattr(OpenAICompatibleConfig, "from_env")


def test_deployment_sample_uses_the_single_schema(tmp_path: Path) -> None:
    source = Path(__file__).resolve().parents[2] / "deployment" / "krim.yaml.example"
    config_path = _write_config(tmp_path / "krim.yaml", source.read_text(encoding="utf-8"))

    configuration = load_configuration(config_path, environment={})

    assert configuration.model is None
    assert configuration.settings.read_parallelism == DEFAULT_TOOL_PARALLELISM
    assert configuration.continuation_policy.identical_no_progress_limit == 2
    assert configuration.settings.database == tmp_path / ".data/krim-agent.db"


def test_minimal_management_configuration_accepts_unconfigured_model(tmp_path: Path) -> None:
    database = tmp_path / "agent.sqlite"
    config_path = _write_config(
        tmp_path / "krim.yaml",
        f"""schema_version: 1
storage:
  database: '{database}'
server:
  host: 127.0.0.1
  port: 8788
""",
    )

    configuration = load_configuration(config_path, environment={})

    assert configuration.model is None
    assert not configuration.ready_for_execution
    assert configuration.settings.database == database
    assert configuration.server.host == "127.0.0.1"
    assert configuration.server.port == 8788


def test_explicit_secret_references_resolve_but_snapshot_keeps_names(tmp_path: Path) -> None:
    secret = "never-write-this-secret"
    access_secret = "access-secret-value"
    config_path = _write_config(
        tmp_path / "krim.yaml",
        """schema_version: 1
model:
  base_url: https://provider.example/v1
  model: example-model
  api_key:
    env: KRIM_MODEL_KEY
access:
  api_key:
    env: KRIM_AGENT_API_KEY
storage:
  database: data/service.sqlite
execution:
  parallelism: 8
""",
    )

    configuration = load_configuration(
        config_path,
        environment={
            "KRIM_MODEL_KEY": secret,
            "KRIM_AGENT_API_KEY": access_secret,
            "KRIM_AGENT_PROVIDER": "script",
            "KRIM_AGENT_SYSTEM_PROMPT": "legacy math prompt text",
            "KRIM_AGENT_TARGET_VERSION": "legacy-target-v1",
        },
    )

    assert configuration.model is not None
    assert configuration.model.api_key.get_secret_value() == secret
    assert configuration.settings.api_key.get_secret_value() == access_secret
    assert configuration.settings.database == tmp_path / "data/service.sqlite"
    assert "KRIM_AGENT_PROVIDER" in configuration.legacy_environment_variables
    assert "KRIM_AGENT_SYSTEM_PROMPT" in configuration.legacy_environment_variables
    assert "KRIM_AGENT_TARGET_VERSION" in configuration.legacy_environment_variables
    assert "KRIM_MODEL_KEY" not in configuration.legacy_environment_variables
    assert "KRIM_AGENT_API_KEY" not in configuration.legacy_environment_variables
    serialized = json.dumps(configuration.snapshot, sort_keys=True)
    assert "KRIM_MODEL_KEY" in serialized
    assert "KRIM_AGENT_API_KEY" in serialized
    assert secret not in serialized
    assert access_secret not in serialized
    assert "legacy math prompt text" not in serialized
    assert "legacy-target-v1" not in serialized


@pytest.mark.parametrize(
    ("content", "expected_diagnostic"),
    [
        ("schema_version: 1\nschema_version: 1\n", "yaml:duplicate_key:"),
        (
            "schema_version: 1\nexecution:\n  parallelism: 4\n  typo: true\n",
            "schema:execution.typo:extra_forbidden",
        ),
        (
            "schema_version: 1\nexecution:\n  parallelism: true\n",
            "schema:execution.parallelism:int_type",
        ),
        (
            """schema_version: 1
model: &model {base_url: https://example.com, model: x}
tools: *model
""",
            "yaml:alias:",
        ),
        (
            "schema_version: 1\nmodel: !!python/object/apply:os.system ['echo unsafe']\n",
            "yaml:invalid:",
        ),
    ],
)
def test_rejects_duplicate_keys_unknown_fields_aliases_and_unsafe_tags(
    tmp_path: Path, content: str, expected_diagnostic: str
) -> None:
    config_path = _write_config(tmp_path / "krim.yaml", content)
    with pytest.raises(ConfigurationError) as exc:
        load_configuration(config_path, environment={})
    assert exc.value.code == "invalid_input"
    assert exc.value.diagnostic.startswith(expected_diagnostic)


def test_rejects_plaintext_secret_and_development_execution_overrides(tmp_path: Path) -> None:
    plaintext = _write_config(
        tmp_path / "krim.yaml",
        """schema_version: 1
model:
  base_url: https://provider.example/v1
  model: example-model
  api_key: this-is-not-a-reference
""",
    )
    with pytest.raises(ServiceError) as exc:
        load_configuration(plaintext, environment={})
    assert exc.value.code == "invalid_input"

    development = _write_config(
        tmp_path / "krim.yaml",
        """schema_version: 1
environment:
  development:
    schema_version: 2
    resource_policy: unlimited
    image_id: sha256:0000000000000000000000000000000000000000000000000000000000000000
    broker_environments_root: /tmp/krim
    private_state_root: /tmp/krim-state
    max_steps: 2
""",
    )
    with pytest.raises(ServiceError) as exc:
        load_configuration(development, environment={})
    assert exc.value.code == "invalid_input"


def test_secret_reference_must_exist_without_leaking_name_or_value(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path / "krim.yaml",
        """schema_version: 1
model:
  base_url: https://provider.example/v1
  model: example-model
  api_key:
    env: KRIM_MODEL_KEY
""",
    )

    with pytest.raises(ConfigurationError) as exc:
        load_configuration(config_path, environment={})
    assert exc.value.code == "invalid_input"
    assert exc.value.diagnostic == "secret:KRIM_MODEL_KEY:missing"
    assert "KRIM_MODEL_KEY" not in str(exc.value)


def test_inline_environment_tools_access_and_vault_paths_are_typed(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path / "krim.yaml",
        """schema_version: 1
environment: {}
tools:
  personal_mcp_key_file: secrets/mcp-master.key
  mcp_servers:
    - name: private_tools
      transport: stdio
      command: python
      args: [-m, private_tools_server]
      selected_tools: [read_file]
      readonly_tools: [read_file]
      env:
        ACCESS_TOKEN:
          env: PRIVATE_TOOLS_TOKEN
access:
  policy:
    credentials:
      - principal_id: service_admin
        token:
          env: KRIM_SERVICE_TOKEN
        memberships:
          - workspace_id: default
            role: admin
            approve: true
""",
    )

    configuration = load_configuration(
        config_path,
        environment={
            "PRIVATE_TOOLS_TOKEN": "tool-token-123",
            "KRIM_SERVICE_TOKEN": "access-token-456",
        },
    )

    assert configuration.personal_mcp_key_file == tmp_path / "secrets/mcp-master.key"
    assert configuration.tools is not None
    assert configuration.tools.mcp_servers[0].args == ("-m", "private_tools_server")
    assert configuration.tools.mcp_servers[0].selected_tools == ("read_file",)
    assert configuration.tools.mcp_servers[0].readonly_tools == frozenset({"read_file"})
    assert configuration.tools.mcp_servers[0].env == {"ACCESS_TOKEN": "tool-token-123"}
    assert configuration.access is not None
    assert configuration.access.credentials[0].token.get_secret_value() == "access-token-456"
    serialized = json.dumps(configuration.snapshot, sort_keys=True)
    assert "PRIVATE_TOOLS_TOKEN" in serialized
    assert "KRIM_SERVICE_TOKEN" in serialized
    assert "tool-token-123" not in serialized
    assert "access-token-456" not in serialized


def test_empty_and_http_mcp_yaml_sequences_are_accepted(tmp_path: Path) -> None:
    empty_path = _write_config(
        tmp_path / "empty.yaml",
        """schema_version: 1
tools:
  mcp_servers: []
  public_tools: []
""",
    )
    empty = load_configuration(empty_path, environment={})
    assert empty.tools is not None
    assert empty.tools.mcp_servers == ()
    assert empty.tools.public_tools == ()

    http_path = _write_config(
        tmp_path / "http.yaml",
        """schema_version: 1
tools:
  mcp_servers:
    - name: remote_api
      transport: streamable_http
      url: https://mcp.example.com/mcp
      allowed_hosts: [mcp.example.com]
      selected_tools: [lookup]
      readonly_tools: [lookup]
""",
    )
    http_config = load_configuration(http_path, environment={})
    assert http_config.tools is not None
    server = http_config.tools.mcp_servers[0]
    assert server.url == "https://mcp.example.com/mcp"
    assert server.allowed_hosts == frozenset({"mcp.example.com"})
    assert server.selected_tools == ("lookup",)
    assert server.readonly_tools == frozenset({"lookup"})


def test_unknown_access_policy_fields_are_rejected(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path / "krim.yaml",
        """schema_version: 1
access:
  policy:
    credentials: []
    ignored: true
""",
    )

    with pytest.raises(ConfigurationError) as exc:
        load_configuration(config_path, environment={})
    assert exc.value.diagnostic == "access.policy.ignored:unknown_field"


def test_all_inline_host_paths_resolve_relative_to_main_config(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path / "krim.yaml",
        """schema_version: 1
storage:
  database: data/main.sqlite
  legacy_artifact_root: artifacts/legacy
server:
  tls_certificate: cert/server.crt
  tls_private_key: key/server.key
evaluation:
  database: data/eval.sqlite
environment:
  sandbox:
    broker_socket: run/sandbox.sock
  development:
    schema_version: 2
    image_id: sha256:0000000000000000000000000000000000000000000000000000000000000000
    broker_environments_root: data/environments
    private_state_root: data/private
tools:
  personal_mcp_key_file: secrets/mcp-master.key
  web:
    result_root: data/web-results
""",
    )

    configuration = load_configuration(config_path, environment={})

    assert configuration.settings.database == tmp_path / "data/main.sqlite"
    assert configuration.legacy_artifact_root == tmp_path / "artifacts/legacy"
    assert configuration.server.tls_certificate == tmp_path / "cert/server.crt"
    assert configuration.server.tls_private_key == tmp_path / "key/server.key"
    assert configuration.settings.product_eval_database == tmp_path / "data/eval.sqlite"
    assert configuration.sandbox is not None
    assert configuration.sandbox.broker_socket == tmp_path / "run/sandbox.sock"
    assert configuration.development is not None
    assert configuration.development.broker_environments_root == tmp_path / "data/environments"
    assert configuration.development.private_state_root == tmp_path / "data/private"
    assert configuration.personal_mcp_key_file == tmp_path / "secrets/mcp-master.key"
    assert configuration.tools is not None
    assert configuration.tools.web_result_root == tmp_path / "data/web-results"
