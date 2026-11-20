"""One-time, non-executing migration of legacy service configuration.

This tool reads simple KEY=VALUE .env lines and explicit legacy JSON files. It
never executes shell syntax and never copies or prints secret values.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import sys
import tempfile
from pathlib import Path
from typing import Any

import yaml

from krim_agent.configuration import ConfigurationError, load_configuration
from krim_agent.contracts import ServiceError

_EXECUTION_ENV = {
    "KRIM_AGENT_MAX_STEPS": ("max_steps", "int"),
    "KRIM_AGENT_MAX_TOOLS_PER_STEP": ("max_tools_per_step", "int"),
    "KRIM_AGENT_READ_PARALLELISM": ("parallelism", "int"),
    "KRIM_AGENT_MAX_TOOL_CALLS": ("max_tool_calls", "int"),
    "KRIM_AGENT_MAX_ACTIVE_RUNS": ("max_active_runs", "int"),
    "KRIM_AGENT_MAX_EVENTS": ("max_events", "int"),
    "KRIM_AGENT_MAX_OUTPUT_BYTES": ("max_output_bytes", "int"),
    "KRIM_AGENT_MAX_INPUT_BYTES": ("max_input_bytes", "int"),
    "KRIM_AGENT_MAX_REQUEST_BYTES": ("max_request_bytes", "int"),
    "KRIM_AGENT_RUN_TIMEOUT": ("run_timeout", "float"),
    "KRIM_AGENT_MODEL_TIMEOUT": ("model_timeout", "float"),
    "KRIM_AGENT_TOOL_TIMEOUT": ("tool_timeout", "float"),
    "KRIM_AGENT_APPROVAL_TIMEOUT": ("approval_timeout", "float"),
    "KRIM_AGENT_SESSION_MAX_QUEUED": ("session_max_queued", "int"),
    "KRIM_AGENT_SESSION_MAX_OPEN": ("session_max_open", "int"),
    "KRIM_AGENT_SESSION_MAX_TURNS": ("session_max_turns", "int"),
    "KRIM_AGENT_SESSION_MAX_HISTORY_BYTES": ("session_max_history_bytes", "int"),
    "KRIM_AGENT_SESSION_QUEUE_TIMEOUT": ("session_queue_timeout", "float"),
    "KRIM_AGENT_CONTEXT_MAX_BYTES": ("context_max_bytes", "int"),
    "KRIM_AGENT_CONTEXT_MAX_MESSAGES": ("context_max_messages", "int"),
    "KRIM_AGENT_CONTEXT_RESERVED_OUTPUT_TOKENS": ("context_reserved_output_tokens", "int"),
    "KRIM_AGENT_HISTORY_COMPACTION": ("history_compaction", "bool"),
}

_PROVIDER_ENV = {
    "BASE_URL": "base_url",
    "MODEL": "model",
    "WIRE_API": "wire_api",
    "MAX_OUTPUT_TOKENS": "max_output_tokens",
    "REASONING_EFFORT": "reasoning_effort",
    "THINKING": "thinking",
    "CONNECT_TIMEOUT_SECONDS": "connect_timeout_seconds",
    "READ_TIMEOUT_SECONDS": "read_timeout_seconds",
    "WRITE_TIMEOUT_SECONDS": "write_timeout_seconds",
    "POOL_TIMEOUT_SECONDS": "pool_timeout_seconds",
    "MAX_REQUEST_BYTES": "max_request_bytes",
    "MAX_RESPONSE_BYTES": "max_response_bytes",
    "MAX_EVENT_BYTES": "max_event_bytes",
    "MAX_EVENTS": "max_events",
    "MAX_ARGUMENTS_BYTES": "max_arguments_bytes",
    "MAX_TOOL_CALLS": "max_tool_calls",
    "ALLOW_IMAGES": "allow_images",
}

_FILE_ENV = {
    "sandbox": "KRIM_AGENT_SANDBOX_CONFIG_FILE",
    "development": "KRIM_AGENT_DEVELOPMENT_CONFIG_FILE",
    "workspace": "KRIM_AGENT_WORKSPACE_PROFILE_FILE",
    "tools": "KRIM_AGENT_TOOL_CONFIG_FILE",
    "access": "KRIM_AGENT_ACCESS_POLICY_FILE",
}
_PATH_FIELDS = {
    "broker_socket", "runtime_manifest", "broker_environments_root", "private_state_root",
    "repository_root", "stage_root", "cache_root", "control_certificate",
    "control_private_key", "preview_certificate", "preview_private_key", "ca_path",
    "seccomp_path", "result_root", "artifact_root", "legacy_artifact_root",
    "personal_mcp_key_file",
}


def _pairs_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate legacy JSON key")
        value[key] = item
    return value


def _read_json(path: Path) -> dict[str, Any]:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_size > 262_144 or path.is_symlink():
        raise ValueError("legacy input is not a bounded regular file")
    raw = path.read_bytes()
    value = json.loads(raw, object_pairs_hook=_pairs_without_duplicates)
    if not isinstance(value, dict):
        raise ValueError("legacy JSON root must be an object")
    return value


def _parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)=(.*)", line)
        if match is None:
            raise ValueError(f"invalid .env syntax at line {number}")
        name, value = match.groups()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        if name in values:
            raise ValueError(f"duplicate .env variable {name}")
        values[name] = value
    return values


def _number(name: str, value: str, kind: str) -> int | float | bool | None:
    lowered = value.strip().lower()
    if lowered in {"", "none", "unlimited", "null"}:
        return None
    if kind == "bool":
        if lowered not in {"true", "false"}:
            raise ValueError(f"invalid legacy variable {name}")
        return lowered == "true"
    if kind == "int":
        if re.fullmatch(r"[0-9]+", lowered) is None:
            raise ValueError(f"invalid legacy variable {name}")
        return int(lowered)
    if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", lowered) is None:
        raise ValueError(f"invalid legacy variable {name}")
    return float(lowered)


def _old_path(value: Any, parent: Path) -> Any:
    if not isinstance(value, str) or not value:
        return value
    path = Path(value)
    return path if path.is_absolute() else Path(os.path.abspath(parent / path))


def _normalize_old_paths(value: Any, parent: Path, key: str | None = None) -> Any:
    if isinstance(value, dict):
        return {name: _normalize_old_paths(item, parent, name) for name, item in value.items()}
    if isinstance(value, list):
        return [_normalize_old_paths(item, parent, key) for item in value]
    if key in _PATH_FIELDS and value is not None:
        return _old_path(value, parent)
    return value


def _merge_execution(
    execution: dict[str, Any], name: str, value: Any, conflicts: list[str]
) -> None:
    if name in execution and execution[name] != value:
        conflicts.append(f"execution.{name}")
    else:
        execution[name] = value


def _migrate_tools(raw: dict[str, Any], diagnostics: list[str], parent: Path) -> dict[str, Any]:
    allowed = {"web", "mcp_servers", "tool_overrides", "public_tools"}
    if set(raw) - allowed:
        diagnostics.extend(f"tools.{name}: ignored" for name in sorted(set(raw) - allowed))
    result: dict[str, Any] = {}
    web = raw.get("web")
    if isinstance(web, dict):
        output_web: dict[str, Any] = {}
        if isinstance(web.get("endpoint"), str):
            output_web["endpoint"] = web["endpoint"]
        env_name = web.get("api_key_env")
        if isinstance(env_name, str) and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", env_name):
            output_web["api_key"] = {"env": env_name}
        if web.get("result_root") is not None:
            output_web["result_root"] = _old_path(web["result_root"], parent)
        result["web"] = output_web
        for name in set(web) - {"endpoint", "api_key_env", "result_root"}:
            diagnostics.append(f"tools.web.{name}: ignored")
    elif web is not None:
        diagnostics.append("tools.web: invalid shape")

    servers = raw.get("mcp_servers", [])
    if not isinstance(servers, list):
        diagnostics.append("tools.mcp_servers: invalid shape")
        servers = []
    migrated_servers: list[dict[str, Any]] = []
    for index, original in enumerate(servers):
        prefix = f"tools.mcp_servers[{index}]"
        if not isinstance(original, dict):
            diagnostics.append(f"{prefix}: invalid shape")
            continue
        server = dict(original)
        for secret_field in ("headers", "env"):
            if secret_field in server:
                diagnostics.append(f"{prefix}.{secret_field}: plaintext values not copied")
                server.pop(secret_field)
        headers: dict[str, Any] = {}
        old_headers = server.pop("headers_env", {}) or {}
        if not isinstance(old_headers, dict):
            diagnostics.append(f"{prefix}.headers_env: invalid reference map")
            old_headers = {}
        for header, env_name in old_headers.items():
            if isinstance(env_name, str) and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", env_name):
                headers[header] = {"env": env_name}
            else:
                diagnostics.append(f"{prefix}.headers_env: invalid reference")
        env_values: dict[str, Any] = {}
        old_env = server.pop("env_env", {}) or {}
        if not isinstance(old_env, dict):
            diagnostics.append(f"{prefix}.env_env: invalid reference map")
            old_env = {}
        for key, env_name in old_env.items():
            if isinstance(env_name, str) and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", env_name):
                env_values[key] = {"env": env_name}
            else:
                diagnostics.append(f"{prefix}.env_env: invalid reference")
        bearer = server.pop("bearer_env", None)
        if bearer is not None:
            if "authorization" in {name.lower() for name in headers}:
                diagnostics.append(f"{prefix}.bearer_env: conflicts with authorization header")
            elif isinstance(bearer, str) and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", bearer):
                headers["Authorization"] = {"env": bearer}
            else:
                diagnostics.append(f"{prefix}.bearer_env: invalid reference")
        if headers:
            server["headers"] = headers
        if env_values:
            server["env"] = env_values
        migrated_servers.append(_normalize_old_paths(server, parent))
    if migrated_servers:
        result["mcp_servers"] = migrated_servers
    for name in ("tool_overrides", "public_tools"):
        if name in raw:
            result[name] = raw[name]
    return result


def _migrate_access(raw: dict[str, Any], diagnostics: list[str]) -> dict[str, Any]:
    credentials = raw.get("credentials")
    if not isinstance(credentials, list):
        diagnostics.append("access.credentials: invalid shape")
        return {}
    migrated: list[dict[str, Any]] = []
    for index, original in enumerate(credentials):
        if not isinstance(original, dict):
            diagnostics.append(f"access.credentials[{index}]: invalid shape")
            continue
        item = dict(original)
        token = item.get("token")
        if isinstance(token, dict) and set(token) == {"env"}:
            pass
        else:
            principal = str(item.get("principal_id", f"credential_{index + 1}"))
            safe_id = re.sub(r"[^A-Za-z0-9_]", "_", principal).upper()[:48]
            env_name = f"KRIM_ACCESS_{safe_id}_TOKEN"
            item["token"] = {"env": env_name}
            diagnostics.append(f"access credential token: provide secret via {env_name}")
        migrated.append(item)
    return {"credentials": migrated}


def _legacy_input_path(
    explicit: Path | None,
    env_name: str,
    values: dict[str, str],
    env_parent: Path | None,
) -> Path | None:
    if explicit is not None:
        return explicit
    raw = values.get(env_name)
    if not raw:
        return None
    candidate = Path(raw)
    if candidate.is_absolute() or env_parent is None:
        return candidate
    return env_parent / candidate


def migrate(
    *,
    env_file: Path | None,
    inputs: dict[str, Path | None],
) -> tuple[dict[str, Any], list[str], list[str]]:
    values: dict[str, str] = {}
    diagnostics: list[str] = []
    conflicts: list[str] = []
    if env_file is not None:
        values = _parse_env_file(env_file)
    base = env_file.absolute().parent if env_file is not None else Path.cwd()
    config: dict[str, Any] = {
        "schema_version": 1,
        "execution": {},
        "environment": {},
        "tools": {},
        "access": {"identity_mode": "auto"},
        "storage": {},
        "server": {},
        "evaluation": {},
    }

    if values.get("KRIM_AGENT_DATABASE"):
        config["storage"]["database"] = _old_path(values["KRIM_AGENT_DATABASE"], base)
    if values.get("KRIM_AGENT_API_KEY"):
        config["access"]["api_key"] = {"env": "KRIM_AGENT_API_KEY"}
    if values.get("KRIM_AGENT_IDENTITY_MODE"):
        config["access"]["identity_mode"] = values["KRIM_AGENT_IDENTITY_MODE"]
    if values.get("KRIM_AGENT_GATEWAY_DISPLAY_NAME"):
        config["access"]["gateway_display_name"] = values["KRIM_AGENT_GATEWAY_DISPLAY_NAME"]
    if "KRIM_AGENT_PRODUCT_EVAL_ENABLED" in values:
        enabled = _number(
            "KRIM_AGENT_PRODUCT_EVAL_ENABLED", values["KRIM_AGENT_PRODUCT_EVAL_ENABLED"], "bool"
        )
        if enabled is not None:
            config["evaluation"]["enabled"] = enabled
    if values.get("KRIM_AGENT_PRODUCT_EVAL_DATABASE"):
        config["evaluation"]["database"] = _old_path(
            values["KRIM_AGENT_PRODUCT_EVAL_DATABASE"], base
        )
    if "KRIM_AGENT_EVALUATION_AUTOMATION_ENABLED" in values:
        enabled = _number(
            "KRIM_AGENT_EVALUATION_AUTOMATION_ENABLED",
            values["KRIM_AGENT_EVALUATION_AUTOMATION_ENABLED"],
            "bool",
        )
        if enabled is not None:
            config["evaluation"]["automation_enabled"] = enabled
    for env_name, (field, kind) in _EXECUTION_ENV.items():
        if env_name in values:
            parsed = _number(env_name, values[env_name], kind)
            if parsed is not None:
                config["execution"][field] = parsed
    if "KRIM_AGENT_CONTEXT_COUNTER" in values:
        config["execution"]["context_counter"] = values["KRIM_AGENT_CONTEXT_COUNTER"]

    model: dict[str, Any] = {}
    for suffix, field in _PROVIDER_ENV.items():
        env_name = "KRIM_AGENT_HTTP_" + suffix
        if env_name not in values:
            continue
        raw_value = values[env_name]
        if field.endswith("_bytes") or field in {
            "max_output_tokens", "max_events", "max_arguments_bytes", "max_tool_calls"
        }:
            parsed_value = _number(env_name, raw_value, "int")
        elif field.endswith("_seconds"):
            parsed_value = _number(env_name, raw_value, "float")
        elif field == "allow_images":
            parsed_value = _number(env_name, raw_value, "bool")
        else:
            parsed_value = raw_value
        if parsed_value is not None:
            model[field] = parsed_value
    provider_key_name = "KRIM_AGENT_HTTP_API_KEY"
    if provider_key_name in values:
        model["api_key"] = {"env": provider_key_name}
    if "base_url" in model and "model" in model:
        config["model"] = model
    elif model:
        diagnostics.append("KRIM_AGENT_HTTP_*: model configuration incomplete")

    for legacy in (
        "KRIM_AGENT_PROVIDER",
        "KRIM_AGENT_HTTP_MODE",
        "KRIM_AGENT_TARGET_VERSION",
        "KRIM_AGENT_TOOL_PARALLELISM",
        "KRIM_AGENT_DEVELOPMENT_HISTORY_COMPACTION",
    ):
        if legacy in values:
            diagnostics.append(f"{legacy}: retired; not migrated")
    if "KRIM_AGENT_SYSTEM_PROMPT" in values:
        diagnostics.append("KRIM_AGENT_SYSTEM_PROMPT: retired; review prompt manually")
    for name, env_name in _FILE_ENV.items():
        explicit_path = inputs.get(name)
        declared_path = _legacy_input_path(None, env_name, values, base)
        path = explicit_path or declared_path
        if explicit_path is not None and declared_path is not None:
            if explicit_path.absolute() != declared_path.absolute():
                conflicts.append(f"{name} input path")
        if path is None:
            continue
        raw = _read_json(path)
        parent = path.absolute().parent
        if name == "sandbox":
            if "max_projects_per_account" in raw:
                diagnostics.append("sandbox.max_projects_per_account: retired")
                raw.pop("max_projects_per_account", None)
            config["environment"]["sandbox"] = _normalize_old_paths(raw, parent)
        elif name == "development":
            old = dict(raw)
            for old_field, exec_field in (
                ("max_steps", "max_steps"),
                ("max_tool_calls", "max_tool_calls"),
                ("history_compaction", "history_compaction"),
            ):
                if old_field in old:
                    value = old.pop(old_field)
                    if value is not None:
                        _merge_execution(config["execution"], exec_field, value, conflicts)
                        diagnostics.append(
                            f"development.{old_field}: moved to execution.{exec_field}"
                        )
            old["schema_version"] = 2
            config["environment"]["development"] = _normalize_old_paths(old, parent)
        elif name == "workspace":
            artifact_root = raw.get("artifact_root")
            if artifact_root is not None:
                config["storage"]["legacy_artifact_root"] = _old_path(artifact_root, parent)
            diagnostics.append("workspace profile and execution: legacy reads only")
        elif name == "tools":
            migrated_tools = _migrate_tools(raw, diagnostics, parent)
            if config["tools"] and migrated_tools:
                diagnostics.append("tools: duplicate environment and file sources")
                conflicts.append("tools")
            else:
                config["tools"].update(migrated_tools)
        elif name == "access":
            access_policy = _migrate_access(raw, diagnostics)
            if "policy" in config["access"]:
                conflicts.append("access.policy")
            else:
                config["access"]["policy"] = access_policy

    personal_key_path = values.get("KRIM_AGENT_PERSONAL_MCP_KEY_FILE")
    if personal_key_path:
        config["tools"]["personal_mcp_key_file"] = _old_path(personal_key_path, base)

    if not config["execution"]:
        config.pop("execution")
    if not config["environment"]:
        config.pop("environment")
    if not config["tools"]:
        config.pop("tools")
    access = config["access"]
    if (
        not access.get("api_key")
        and not access.get("policy")
        and access.get("identity_mode") == "auto"
    ):
        config.pop("access")
    if not config["storage"]:
        config.pop("storage")
    if not config["server"]:
        config.pop("server")
    if not config["evaluation"]:
        config.pop("evaluation")
    return config, diagnostics, conflicts


def _collect_secret_references(value: Any) -> set[str]:
    result: set[str] = set()
    if isinstance(value, dict):
        if set(value) == {"env"} and isinstance(value["env"], str):
            result.add(value["env"])
        for item in value.values():
            result.update(_collect_secret_references(item))
    elif isinstance(value, list):
        for item in value:
            result.update(_collect_secret_references(item))
    return result


def _yaml_ready(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {name: _yaml_ready(item) for name, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_yaml_ready(item) for item in value]
    return value


def _write_validated(output: Path, config: dict[str, Any]) -> None:
    output = output.absolute()
    if output.exists() or output.is_symlink():
        raise FileExistsError("output exists")
    output.parent.mkdir(parents=True, exist_ok=True)
    body = yaml.safe_dump(_yaml_ready(config), sort_keys=False, allow_unicode=True).encode("utf-8")
    fd, temporary_name = tempfile.mkstemp(prefix=".krim-config-", suffix=".yaml", dir=output.parent)
    temporary = Path(temporary_name)
    try:
        if os.name == "posix":
            os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        references = _collect_secret_references(config)
        environment = {name: "migration-validation-only" for name in references}
        load_configuration(temporary, environment=environment)
        os.link(temporary, output)
        if os.name == "posix":
            os.chmod(output, 0o600)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate legacy Krim service configuration once.")
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--sandbox", type=Path)
    parser.add_argument("--development", type=Path)
    parser.add_argument("--workspace", type=Path)
    parser.add_argument("--tools", type=Path)
    parser.add_argument("--access", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    try:
        config, diagnostics, conflicts = migrate(
            env_file=args.env_file,
            inputs={
                "sandbox": args.sandbox,
                "development": args.development,
                "workspace": args.workspace,
                "tools": args.tools,
                "access": args.access,
            },
        )
        for diagnostic in diagnostics:
            print(diagnostic)
        for conflict in conflicts:
            print(f"conflict: {conflict}")
        if conflicts:
            raise SystemExit(2)
        if args.check:
            print("migration check: valid")
            return
        if args.output is None:
            raise ServiceError("invalid_input", 422)
        _write_validated(args.output, config)
        print(f"configuration written: {args.output}")
    except SystemExit:
        raise
    except ConfigurationError as error:
        print(f"configuration rejected: {error.diagnostic}", file=sys.stderr)
        raise SystemExit(1) from None
    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
        json.JSONDecodeError,
        ServiceError,
        yaml.YAMLError,
    ):
        print("migration failed; secret values were not printed", file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
