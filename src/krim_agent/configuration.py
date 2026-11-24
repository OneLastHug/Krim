"""Single, explicit service configuration entrypoint.

The YAML document is trusted host input. Secrets are resolved only when their
field contains an explicit ``{env: NAME}`` reference; configuration snapshots
retain reference names, never resolved values.
"""

from __future__ import annotations

import copy
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

import yaml
from pydantic import Field, SecretStr, ValidationError, field_validator, model_validator
from yaml.constructor import ConstructorError
from yaml.events import AliasEvent
from yaml.nodes import MappingNode

from krim_agent.config import DEFAULT_TOOL_PARALLELISM, Settings
from krim_agent.contracts import ServiceError, StrictModel
from krim_agent.development.configuration import DevelopmentConfiguration
from krim_agent.goals.continuation import ContinuationPolicy
from krim_agent.mcp.types import MCPServerConfig
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig
from krim_agent.sandbox.configuration import SandboxConfiguration
from krim_agent.security import AccessPolicy, Credential, Membership
from krim_agent.subagents.configuration import ROUTE_NAME_PATTERN, SubagentsConfiguration
from krim_agent.tools.runtime import ToolRuntimeConfig
from krim_agent.web_access.tavily import WebAccessConfig

_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MAX_CONFIG_BYTES = 262_144


class SecretReference(StrictModel):
    """A name-only reference to a secret supplied by the host environment."""

    env: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")


class ModelConfiguration(StrictModel):
    base_url: str = Field(min_length=1, max_length=2048)
    model: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
    wire_api: str = "chat_completions"
    api_key: SecretReference | None = None
    max_output_tokens: int | None = Field(default=None, ge=1)
    reasoning_effort: str | None = None
    thinking: str | None = None
    connect_timeout_seconds: float | None = Field(default=10.0, gt=0)
    read_timeout_seconds: float | None = Field(default=None, gt=0)
    write_timeout_seconds: float | None = Field(default=30.0, gt=0)
    pool_timeout_seconds: float | None = Field(default=30.0, gt=0)
    max_request_bytes: int | None = Field(default=None, ge=256)
    allow_images: bool = False
    max_response_bytes: int | None = Field(default=None, ge=1)
    max_event_bytes: int | None = Field(default=None, ge=1)
    max_events: int | None = Field(default=None, ge=1)
    max_arguments_bytes: int | None = Field(default=None, ge=2)
    max_tool_calls: int | None = Field(default=None, ge=1)


class GoalContinuationConfiguration(StrictModel):
    version: Literal["goal-continuation-policy/1"] = "goal-continuation-policy/1"
    identical_no_progress_limit: int = Field(default=2, ge=2)
    max_targeted_recovery_attempts: int = Field(default=1, ge=0)
    repeated_failure_limit: int = Field(default=2, ge=1)


class ExecutionConfiguration(StrictModel):
    parallelism: int = Field(default=DEFAULT_TOOL_PARALLELISM, ge=1)
    max_steps: int | None = Field(default=None, ge=1)
    max_tools_per_step: int | None = Field(default=None, ge=1)
    max_tool_calls: int | None = Field(default=None, ge=1)
    max_active_runs: int | None = Field(default=None, ge=1)
    max_events: int | None = Field(default=None, ge=4)
    max_output_bytes: int | None = Field(default=None, ge=1)
    max_input_bytes: int | None = Field(default=None, ge=1)
    max_request_bytes: int | None = Field(default=None, ge=256)
    run_timeout: float | None = Field(default=None, gt=0)
    model_timeout: float | None = Field(default=None, gt=0)
    tool_timeout: float | None = Field(default=None, gt=0)
    approval_timeout: float | None = Field(default=None, gt=0)
    session_max_queued: int | None = Field(default=None, ge=1)
    session_max_open: int | None = Field(default=None, ge=1)
    session_max_turns: int | None = Field(default=None, ge=1)
    session_max_history_bytes: int | None = Field(default=None, ge=1)
    session_queue_timeout: float | None = Field(default=None, gt=0)
    context_max_bytes: int | None = Field(default=None, ge=256)
    context_max_messages: int | None = Field(default=None, ge=2)
    context_reserved_output_tokens: int | None = Field(default=None, ge=1)
    context_counter: str = "approx-utf8/1"
    history_compaction: bool = True
    goal_continuation: GoalContinuationConfiguration = Field(
        default_factory=GoalContinuationConfiguration
    )


class EnvironmentConfiguration(StrictModel):
    sandbox: dict[str, Any] | None = None
    development: dict[str, Any] | None = None


class StorageConfiguration(StrictModel):
    database: Path = Path(".data/krim-agent.db")
    legacy_artifact_root: Path | None = None


class ServerConfiguration(StrictModel):
    host: str = "127.0.0.1"
    port: int = Field(default=8765, ge=1, le=65535)
    tls_certificate: Path | None = None
    tls_private_key: Path | None = None

    @field_validator("host")
    @classmethod
    def valid_host(cls, value: str) -> str:
        if not value or len(value) > 253 or any(c.isspace() or ord(c) < 33 for c in value):
            raise ValueError("invalid server host")
        return value

    @model_validator(mode="after")
    def tls_pair(self) -> ServerConfiguration:
        if (self.tls_certificate is None) != (self.tls_private_key is None):
            raise ValueError("TLS certificate and key must be configured together")
        return self


class EvaluationConfiguration(StrictModel):
    enabled: bool = False
    database: Path | None = None
    automation_enabled: bool = False


class _AccessSection(StrictModel):
    api_key: SecretReference | None = None
    policy: dict[str, Any] | None = None
    identity_mode: str = "auto"
    gateway_display_name: str | None = Field(default=None, min_length=1, max_length=64)


class _WebSection(StrictModel):
    endpoint: str = "https://api.tavily.com"
    api_key: SecretReference | None = None
    result_root: Path | None = None


class _MCPSection(StrictModel):
    name: str
    transport: str
    version: str = "1"
    command: str | None = None
    args: list[str] = Field(default_factory=list)
    env: dict[str, SecretReference] | None = None
    url: str | None = None
    allowed_hosts: list[str] = Field(default_factory=list)
    headers: dict[str, SecretReference] = Field(default_factory=dict)
    selected_tools: list[str] = Field(default_factory=list)
    readonly_tools: list[str] = Field(default_factory=list)
    timeout_seconds: float = Field(default=10.0, gt=0)


class _ToolsSection(StrictModel):
    web: _WebSection | None = None
    personal_mcp_key_file: Path | None = None
    mcp_servers: list[_MCPSection] = Field(default_factory=list)
    tool_overrides: dict[str, str] = Field(default_factory=dict)
    public_tools: list[str] | None = None


@dataclass(frozen=True)
class ServiceConfiguration:
    settings: Settings
    model: OpenAICompatibleConfig | None
    sandbox: SandboxConfiguration | None
    development: DevelopmentConfiguration | None
    tools: ToolRuntimeConfig | None
    access: AccessPolicy | None
    continuation_policy: ContinuationPolicy
    server: ServerConfiguration
    legacy_artifact_root: Path | None
    personal_mcp_key_file: Path | None
    snapshot: dict[str, Any]
    legacy_environment_variables: tuple[str, ...] = ()
    # Named model routes for subagent profiles, resolved once at startup.
    # Snapshots keep the env reference names, never the resolved values.
    models: Mapping[str, OpenAICompatibleConfig] = field(default_factory=dict)
    subagents: SubagentsConfiguration = field(default_factory=SubagentsConfiguration)

    @property
    def ready_for_execution(self) -> bool:
        """Whether a real model is configured; optional environments are separate."""
        return self.model is not None


class ConfigurationError(ServiceError):
    """Sanitized startup diagnosis containing no input values or exception text."""

    def __init__(self, diagnostic: str) -> None:
        self.diagnostic = diagnostic
        super().__init__("invalid_input", 422)


class _RootConfiguration(StrictModel):
    schema_version: int = Field(default=1, ge=1, le=1)
    model: ModelConfiguration | None = None
    execution: ExecutionConfiguration = Field(default_factory=ExecutionConfiguration)
    environment: EnvironmentConfiguration = Field(default_factory=EnvironmentConfiguration)
    tools: _ToolsSection | None = None
    access: _AccessSection = Field(default_factory=_AccessSection)
    storage: StorageConfiguration = Field(default_factory=StorageConfiguration)
    server: ServerConfiguration = Field(default_factory=ServerConfiguration)
    evaluation: EvaluationConfiguration = Field(default_factory=EvaluationConfiguration)
    models: dict[str, ModelConfiguration] = Field(default_factory=dict)
    subagents: SubagentsConfiguration = Field(default_factory=SubagentsConfiguration)

    @model_validator(mode="after")
    def subagent_routes(self) -> _RootConfiguration:
        """Validate declared route references; diagnostics keep names only."""
        def key(value: str) -> str:
            return re.sub(r"[^A-Za-z0-9_.-]", "_", value)[:64] or "key"

        if "default" in self.models:
            raise ConfigurationError("models.default:reserved")
        for name in self.models:
            if not ROUTE_NAME_PATTERN.fullmatch(name):
                raise ConfigurationError(f"models.{key(name)}:invalid_name")
        for profile_name, profile in self.subagents.profiles.items():
            safe = key(profile_name)
            if profile.model != "default" and profile.model not in self.models:
                raise ConfigurationError(f"subagents.profiles.{safe}.model:unknown_route")
            if profile.model == "default" and self.model is None:
                raise ConfigurationError(
                    f"subagents.profiles.{safe}.model:default_route_unconfigured"
                )
            if profile.provider == "process" and profile.process not in self.subagents.processes:
                raise ConfigurationError(f"subagents.profiles.{safe}.process:unknown_process")
        return self


def _construct_mapping(
    loader: _StrictSafeLoader, node: MappingNode, deep: bool = False
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key_node, value_node in node.value:
        if key_node.tag == "tag:yaml.org,2002:merge" or key_node.value == "<<":
            raise ConstructorError(
                None, None, "YAML merge keys are not supported", key_node.start_mark
            )
        key = loader.construct_object(key_node, deep=deep)
        if not isinstance(key, str):
            raise ConstructorError(
                None, None, "configuration keys must be strings", key_node.start_mark
            )
        if key in result:
            raise ConstructorError(None, None, "duplicate configuration key", key_node.start_mark)
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


class _StrictSafeLoader(yaml.SafeLoader):
    def compose_node(self, parent: Any, index: Any) -> Any:
        if self.check_event(AliasEvent):
            raise ConstructorError(
                None, None, "YAML aliases are not supported", self.peek_event().start_mark
            )
        return super().compose_node(parent, index)

    def construct_mapping(self, node: MappingNode, deep: bool = False) -> dict[str, Any]:
        return _construct_mapping(self, node, deep)


_StrictSafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_mapping
)

_PATH_KEYS = frozenset(
    {
        "database",
        "legacy_artifact_root",
        "broker_socket",
        "runtime_manifest",
        "broker_environments_root",
        "private_state_root",
        "repository_root",
        "stage_root",
        "cache_root",
        "control_certificate",
        "control_private_key",
        "preview_certificate",
        "preview_private_key",
        "ca_path",
        "seccomp_path",
        "result_root",
        "personal_mcp_key_file",
        "tls_certificate",
        "tls_private_key",
    }
)


def _resolve_config_paths(value: Any, base: Path, key: str | None = None) -> Any:
    if isinstance(value, dict):
        return {name: _resolve_config_paths(item, base, name) for name, item in value.items()}
    if isinstance(value, list):
        return [_resolve_config_paths(item, base, key) for item in value]
    if key in _PATH_KEYS and value is not None:
        if not isinstance(value, str) or not value:
            raise ConfigurationError(f"path:{key or 'unknown'}:invalid")
        path = Path(value)
        if ".." in path.parts:
            raise ConfigurationError(f"path:{key or 'unknown'}:parent_segment")
        return path if path.is_absolute() else base / path
    return value


def _read_main_file(path: Path) -> bytes:
    absolute = path.absolute()
    if any(part.is_symlink() for part in (absolute, *absolute.parents)):
        raise ConfigurationError("file:symlink")
    known = absolute.lstat()
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NONBLOCK", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(absolute, flags)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise ConfigurationError("file:not_regular")
        if info.st_nlink != 1:
            raise ConfigurationError("file:hard_link")
        identity = (info.st_dev, info.st_ino, info.st_mode, info.st_nlink, info.st_size)
        known_identity = (known.st_dev, known.st_ino, known.st_mode, known.st_nlink, known.st_size)
        if identity != known_identity:
            raise ConfigurationError("file:changed")
        if info.st_size > _MAX_CONFIG_BYTES:
            raise ConfigurationError("file:too_large")
        if os.name == "posix":
            if info.st_mode & 0o077:
                raise ConfigurationError("file:permissions")
            if not hasattr(os, "geteuid") or info.st_uid != os.geteuid():
                raise ConfigurationError("file:owner")
        data = bytearray()
        while len(data) <= _MAX_CONFIG_BYTES:
            block = os.read(fd, min(65536, _MAX_CONFIG_BYTES + 1 - len(data)))
            if not block:
                break
            data.extend(block)
        if len(data) != info.st_size:
            raise ConfigurationError("file:changed_during_read")
        return bytes(data)
    finally:
        os.close(fd)


def _resolve_secret(
    reference: SecretReference | None, environment: Mapping[str, str]
) -> SecretStr | None:
    if reference is None:
        return None
    value = environment.get(reference.env)
    if not isinstance(value, str) or not value:
        raise ConfigurationError(f"secret:{reference.env}:missing")
    return SecretStr(value)


def _resolve_policy(
    raw: dict[str, Any] | None, environment: Mapping[str, str]
) -> AccessPolicy | None:
    if raw is None:
        return None
    if set(raw) != {"credentials"}:
        names = sorted(
            name for name in set(raw) - {"credentials"}
            if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name)
        )
        suffix = ".".join(names) if names else "unknown_field"
        raise ConfigurationError(f"access.policy.{suffix}:unknown_field")
    credentials = raw.get("credentials")
    if not isinstance(credentials, list):
        raise ValueError("invalid access policy")
    parsed: list[Credential] = []
    for raw_credential in credentials:
        if not isinstance(raw_credential, dict):
            raise ValueError("invalid access credential")
        credential = copy.deepcopy(raw_credential)
        references = credential.get("token")
        token = _resolve_secret(SecretReference.model_validate(references), environment)
        memberships = credential.get("memberships")
        if not isinstance(memberships, list):
            raise ValueError("invalid access memberships")
        credential["token"] = token
        credential["memberships"] = tuple(Membership.model_validate(item) for item in memberships)
        parsed.append(Credential.model_validate(credential))
    return AccessPolicy(credentials=tuple(parsed))


def _parse_tools(
    section: _ToolsSection | None, environment: Mapping[str, str]
) -> ToolRuntimeConfig | None:
    if section is None:
        return None
    web: WebAccessConfig | None = None
    web_result_root: Path | None = None
    if section.web is not None:
        web = WebAccessConfig(
            endpoint=section.web.endpoint,
            api_key=_resolve_secret(section.web.api_key, environment),
        )
        web_result_root = section.web.result_root
    servers: list[MCPServerConfig] = []
    for item in section.mcp_servers:
        values = item.model_dump(exclude={"env", "headers"})
        values["args"] = tuple(item.args)
        values["allowed_hosts"] = frozenset(item.allowed_hosts)
        values["selected_tools"] = tuple(item.selected_tools)
        values["readonly_tools"] = frozenset(item.readonly_tools)
        if item.env is not None:
            resolved_env = {
                key: cast(SecretStr, _resolve_secret(reference, environment)).get_secret_value()
                for key, reference in item.env.items()
            }
            values["env"] = resolved_env
            values["secret_env_names"] = frozenset(resolved_env)
        values["headers"] = {
            key: cast(SecretStr, _resolve_secret(reference, environment))
            for key, reference in item.headers.items()
        }
        servers.append(MCPServerConfig.model_validate(values))
    return ToolRuntimeConfig(
        web=web,
        mcp_servers=tuple(servers),
        tool_overrides=section.tool_overrides,
        public_tools=tuple(section.public_tools) if section.public_tools is not None else None,
        web_result_root=web_result_root,
    )


def _continuation_policy(values: GoalContinuationConfiguration) -> ContinuationPolicy:
    return ContinuationPolicy(**values.model_dump(mode="python"))


def _secret_reference_names(value: Any) -> set[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        if set(value) == {"env"} and isinstance(value.get("env"), str):
            found.add(value["env"])
        for item in value.values():
            found.update(_secret_reference_names(item))
    elif isinstance(value, (tuple, list)):
        for item in value:
            found.update(_secret_reference_names(item))
    return found


_LEGACY_SETTING_NAMES = frozenset(
    "KRIM_AGENT_" + field.upper()
    for field in (
        "provider", "http_mode", "database", "api_key", "identity_mode",
        "gateway_display_name", "max_steps", "max_tools_per_step", "read_parallelism",
        "max_tool_calls", "max_active_runs", "max_events", "max_output_bytes",
        "max_input_bytes", "max_request_bytes", "run_timeout", "model_timeout",
        "tool_timeout", "approval_timeout", "system_prompt", "target_version",
        "tool_parallelism", "access_policy_file", "sandbox_config_file",
        "development_config_file", "workspace_profile_file", "tool_config_file",
        "personal_mcp_key_file", "product_eval_enabled", "product_eval_database",
        "evaluation_automation_enabled", "session_max_queued", "session_max_open",
        "session_max_turns", "session_max_history_bytes", "session_queue_timeout",
        "context_max_bytes", "context_max_messages", "context_reserved_output_tokens",
        "context_counter", "history_compaction", "development_history_compaction",
    )
) | frozenset(
    "KRIM_AGENT_HTTP_" + field
    for field in (
        "BASE_URL", "MODEL", "WIRE_API", "API_KEY", "MAX_OUTPUT_TOKENS",
        "REASONING_EFFORT", "THINKING", "CONNECT_TIMEOUT_SECONDS",
        "READ_TIMEOUT_SECONDS", "WRITE_TIMEOUT_SECONDS", "POOL_TIMEOUT_SECONDS",
        "MAX_REQUEST_BYTES", "MAX_RESPONSE_BYTES", "MAX_EVENT_BYTES", "MAX_EVENTS",
        "MAX_ARGUMENTS_BYTES", "MAX_TOOL_CALLS", "ALLOW_IMAGES",
    )
)


def _validation_diagnostic(error: ValidationError) -> str:
    errors = error.errors(include_input=False)
    if not errors:
        return "schema:invalid"
    first = errors[0]
    parts: list[str] = []
    for item in first.get("loc", ()):
        if isinstance(item, int):
            parts.append(f"[{item}]")
        elif isinstance(item, str):
            safe = re.sub(r"[^A-Za-z0-9_.-]", "_", item)[:64]
            parts.append(safe or "field")
    path = ".".join(parts) or "root"
    kind = re.sub(r"[^A-Za-z0-9_]", "_", str(first.get("type", "invalid")))
    return f"schema:{path}:{kind}"


def _yaml_diagnostic(error: yaml.YAMLError) -> str:
    problem = getattr(error, "problem", None)
    kinds = {
        "duplicate configuration key": "duplicate_key",
        "YAML merge keys are not supported": "merge_key",
        "YAML aliases are not supported": "alias",
        "configuration keys must be strings": "invalid_key",
    }
    kind = kinds.get(problem, "invalid")
    mark = getattr(error, "problem_mark", None)
    if mark is None:
        return f"yaml:{kind}"
    return f"yaml:{kind}:{mark.line + 1}:{mark.column + 1}"


def load_configuration(
    path: str | Path,
    *,
    environment: Mapping[str, str] | None = None,
) -> ServiceConfiguration:
    """Load, validate and resolve one explicitly named ``krim.yaml`` file."""
    environment = os.environ if environment is None else environment
    source = Path(path)
    try:
        raw = yaml.load(_read_main_file(source), Loader=_StrictSafeLoader)
        if not isinstance(raw, dict):
            raise ConfigurationError("schema:root:object_required")
        base = source.absolute().parent
        raw = _resolve_config_paths(raw, base)
        storage = raw.setdefault("storage", {})
        if isinstance(storage, dict) and "database" not in storage:
            storage["database"] = base / ".data/krim-agent.db"
        root = _RootConfiguration.model_validate(raw)

        # The removed per-development run limits must not be silently accepted
        # through DevelopmentConfiguration's historical compatibility parser.
        development_raw = root.environment.development
        deprecated_development_fields = (
            {"max_steps", "max_tool_calls", "history_compaction"}
            & set(development_raw or {})
        )
        if deprecated_development_fields:
            field = sorted(deprecated_development_fields)[0]
            raise ConfigurationError(f"development.{field}:execution_override")
        sandbox = (
            SandboxConfiguration.model_validate(root.environment.sandbox)
            if root.environment.sandbox is not None else None
        )
        development = (
            DevelopmentConfiguration.model_validate(root.environment.development)
            if root.environment.development is not None else None
        )

        model = None
        if root.model is not None:
            model_values = root.model.model_dump(exclude={"api_key"})
            model_values["api_key"] = (
                _resolve_secret(root.model.api_key, environment) or SecretStr("")
            )
            model = OpenAICompatibleConfig.model_validate(model_values)

        models: dict[str, OpenAICompatibleConfig] = {}
        for name, route in root.models.items():
            route_values = route.model_dump(exclude={"api_key"})
            route_values["api_key"] = _resolve_secret(route.api_key, environment) or SecretStr("")
            models[name] = OpenAICompatibleConfig.model_validate(route_values)

        access_policy = _resolve_policy(root.access.policy, environment)
        access_key = _resolve_secret(root.access.api_key, environment) or SecretStr("")
        tools = _parse_tools(root.tools, environment)
        execution = root.execution
        settings = Settings(
            database=root.storage.database,
            provider="http",
            api_key=access_key,
            max_steps=execution.max_steps,
            max_tools_per_step=execution.max_tools_per_step,
            read_parallelism=execution.parallelism,
            max_tool_calls=execution.max_tool_calls,
            max_active_runs=execution.max_active_runs,
            max_events=execution.max_events,
            max_output_bytes=execution.max_output_bytes,
            max_input_bytes=execution.max_input_bytes,
            max_request_bytes=execution.max_request_bytes,
            run_timeout=execution.run_timeout,
            model_timeout=execution.model_timeout,
            tool_timeout=execution.tool_timeout,
            approval_timeout=execution.approval_timeout,
            identity_mode=root.access.identity_mode,
            gateway_display_name=root.access.gateway_display_name,
            product_eval_enabled=root.evaluation.enabled,
            product_eval_database=root.evaluation.database,
            evaluation_automation_enabled=root.evaluation.automation_enabled,
            session_max_queued=execution.session_max_queued,
            session_max_open=execution.session_max_open,
            session_max_turns=execution.session_max_turns,
            session_max_history_bytes=execution.session_max_history_bytes,
            session_queue_timeout=execution.session_queue_timeout,
            context_max_bytes=execution.context_max_bytes,
            context_max_messages=execution.context_max_messages,
            context_reserved_output_tokens=execution.context_reserved_output_tokens,
            context_counter=execution.context_counter,
            history_compaction=execution.history_compaction,
            development_history_compaction=execution.history_compaction,
        )

        # These refs are identifiers, not secret values. Re-parsing the raw YAML
        # also preserves exactly the explicit references used by the snapshot.
        snapshot = {
            "schema_version": root.schema_version,
            "model": root.model.model_dump(mode="json") if root.model is not None else None,
            "models": {name: route.model_dump(mode="json") for name, route in root.models.items()},
            "subagents": root.subagents.model_dump(mode="json"),
            "execution": execution.model_dump(mode="json"),
            "environment": {
                "sandbox": sandbox.model_dump(mode="json") if sandbox is not None else None,
                "development": development.model_dump(
                    mode="json", exclude={"history_compaction", "max_steps", "max_tool_calls"}
                ) if development is not None else None,
            },
            "tools": root.tools.model_dump(mode="json") if root.tools is not None else None,
            "access": {
                "api_key": (root.access.api_key.model_dump(mode="json")
                            if root.access.api_key is not None else None),
                "policy": copy.deepcopy(root.access.policy),
                "identity_mode": root.access.identity_mode,
                "gateway_display_name": root.access.gateway_display_name,
            },
            "storage": root.storage.model_dump(mode="json"),
            "server": root.server.model_dump(mode="json"),
            "evaluation": root.evaluation.model_dump(mode="json"),
        }
        referenced = _secret_reference_names(snapshot)
        legacy = tuple(sorted((set(environment) & _LEGACY_SETTING_NAMES) - referenced))
        return ServiceConfiguration(
            settings=settings,
            model=model,
            sandbox=sandbox,
            development=development,
            tools=tools,
            access=access_policy,
            continuation_policy=_continuation_policy(execution.goal_continuation),
            server=root.server,
            legacy_artifact_root=root.storage.legacy_artifact_root,
            personal_mcp_key_file=(
                root.tools.personal_mcp_key_file if root.tools is not None else None
            ),
            snapshot=snapshot,
            legacy_environment_variables=legacy,
            models=models,
            subagents=root.subagents,
        )
    except ConfigurationError:
        raise
    except ValidationError as error:
        raise ConfigurationError(_validation_diagnostic(error)) from None
    except yaml.YAMLError as error:
        raise ConfigurationError(_yaml_diagnostic(error)) from None
    except ServiceError as error:
        raise ConfigurationError(f"component:{error.code}") from None
    except OSError:
        raise ConfigurationError("file:unavailable") from None
    except (ValueError, TypeError, KeyError, RecursionError):
        raise ConfigurationError("configuration:invalid") from None


def report_legacy_environment(configuration: ServiceConfiguration) -> tuple[str, ...]:
    """Return variable names that were ignored, without reading their values."""
    return configuration.legacy_environment_variables
