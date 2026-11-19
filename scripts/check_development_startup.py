#!/usr/bin/env python3
"""Check local development configuration, TLS files, and explicit model bounds."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import ssl
import stat
import sys
from pathlib import Path
from typing import Any

MAX_BOUNDS_BYTES = 4_096
MAX_PROMPT_BYTES = 16_384
MAX_CA_BYTES = 1_048_576
MAX_IMAGE_BATCH_BYTES = 512 * 1024
MAX_IMAGE_BASE64_BYTES = 4 * ((MAX_IMAGE_BATCH_BYTES + 2) // 3)
DEFAULT_HOST_MAX_REQUEST_BYTES = 262_144
MAX_HOST_REQUEST_BYTES = 1_048_576
DEVELOPMENT_RUN_TIMEOUT_SECONDS = 300
DEVELOPMENT_TOOL_TIMEOUT_SECONDS = 30
PROVIDER_MAX_ARGUMENTS_BYTES = 4_096


class PreflightError(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _read_regular(path: Path, *, limit: int, private: bool = False) -> bytes:
    if os.name != "posix" or not hasattr(os, "O_NOFOLLOW"):
        raise ValueError("unsupported host")
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("invalid path")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
            raise ValueError("invalid file")
        if private and (info.st_uid != os.geteuid() or info.st_mode & 0o077):
            raise ValueError("untrusted file")
        data = os.read(fd, limit + 1)
        if len(data) != info.st_size or len(data) > limit:
            raise ValueError("file changed")
        return data
    finally:
        os.close(fd)


def load_provider_bounds(path: Path) -> dict[str, int | bool]:
    """Read only explicit, non-secret provider bounds; never consult the environment."""
    try:
        raw = _read_regular(path, limit=MAX_BOUNDS_BYTES, private=True)
        value = json.loads(raw, object_pairs_hook=_reject_duplicate_keys)
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        raise PreflightError("PROVIDER_BOUNDS_INVALID") from None
    if type(value) is not dict or set(value) != {
        "allow_images", "max_output_tokens", "max_request_bytes", "max_tool_calls"
    }:
        raise PreflightError("PROVIDER_BOUNDS_INVALID")

    allow_images = value["allow_images"]
    output_tokens = value["max_output_tokens"]
    request_bytes = value["max_request_bytes"]
    tool_calls = value["max_tool_calls"]
    if (
        type(allow_images) is not bool
        or type(output_tokens) is not int or not 1 <= output_tokens <= 4096
        or type(request_bytes) is not int or not 256 <= request_bytes <= 1_048_576
        or type(tool_calls) is not int or not 1 <= tool_calls <= 4
    ):
        raise PreflightError("PROVIDER_BOUNDS_INVALID")
    return {
        "allow_images": allow_images,
        "max_output_tokens": output_tokens,
        "max_request_bytes": request_bytes,
        "max_tool_calls": tool_calls,
    }


def prompt_sha256(path: Path) -> str:
    try:
        return hashlib.sha256(_read_regular(path, limit=MAX_PROMPT_BYTES)).hexdigest()
    except (OSError, ValueError, TypeError):
        raise PreflightError("PROMPT_INVALID") from None


def _check_tls_file(path: Path, *, private: bool) -> None:
    try:
        info = path.lstat()
        if (
            path.resolve(strict=True) != path
            or not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_mode & 0o022
            or private and info.st_mode & 0o077
        ):
            raise ValueError("untrusted TLS file")
    except (OSError, ValueError):
        raise PreflightError("TLS_FILE_INVALID") from None


def verify_tls_listener(listener: Any) -> None:
    pairs = (
        (listener.control_certificate, listener.control_private_key),
        (listener.preview_certificate, listener.preview_private_key),
    )
    try:
        for certificate, key in pairs:
            _check_tls_file(certificate, private=False)
            _check_tls_file(key, private=True)
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.load_cert_chain(certfile=str(certificate), keyfile=str(key))
    except PreflightError:
        raise
    except (OSError, ssl.SSLError, ValueError, TypeError):
        raise PreflightError("TLS_CERTIFICATE_INVALID") from None


def verify_browser(browser: Any) -> None:
    try:
        ca_bytes = _read_regular(browser.ca_path, limit=MAX_CA_BYTES)
    except (OSError, ValueError, TypeError):
        raise PreflightError("BROWSER_CA_INVALID") from None
    if hashlib.sha256(ca_bytes).hexdigest() != browser.ca_sha256:
        raise PreflightError("BROWSER_CA_HASH_MISMATCH")
    try:
        info = browser.seccomp_path.lstat()
        if (
            browser.seccomp_path.resolve(strict=True) != browser.seccomp_path
            or not stat.S_ISREG(info.st_mode)
        ):
            raise ValueError("invalid seccomp file")
    except (OSError, ValueError, TypeError):
        raise PreflightError("SECCOMP_FILE_MISSING") from None


def verify_consistency(
    development: Any, bounds: dict[str, int | bool], host_max_request_bytes: int,
) -> int:
    if development.model_images and not bounds["allow_images"]:
        raise PreflightError("IMAGE_CAPABILITY_MISMATCH")
    # Actual image size, tool schemas, and history determine the wire budget.
    # A request smaller than the maximum possible image batch can still be valid.
    return min(host_max_request_bytes, int(bounds["max_request_bytes"]))


def _load_project_configs(
    project_root: Path, development_path: Path, sandbox_path: Path,
) -> tuple[Any, Any]:
    source_root = project_root / "src"
    if not project_root.is_absolute() or not source_root.is_dir():
        raise PreflightError("PROJECT_ROOT_INVALID")
    if str(source_root) not in sys.path:
        sys.path.insert(0, str(source_root))
    try:
        from krim_agent.development.configuration import DevelopmentConfiguration
        from krim_agent.sandbox.configuration import SandboxConfiguration

        development = DevelopmentConfiguration.load(development_path)
    except Exception:
        raise PreflightError("DEVELOPMENT_CONFIG_INVALID") from None
    try:
        sandbox = SandboxConfiguration.load(sandbox_path)
    except Exception:
        raise PreflightError("SANDBOX_CONFIG_INVALID") from None
    return development, sandbox


def _build_report(
    development: Any, sandbox: Any, bounds: dict[str, int | bool], prompt_hash: str,
    host_max_request_bytes: int,
) -> dict[str, Any]:
    del sandbox  # Loading validates the native configuration; no Broker is contacted.
    checks: list[dict[str, str]] = []
    effective_request_bytes = min(
        host_max_request_bytes, int(bounds["max_request_bytes"]),
    )

    def record(code: str, operation: Any) -> None:
        try:
            operation()
        except PreflightError as exc:
            checks.append({"code": exc.code, "status": "fail"})
        except Exception:
            checks.append({"code": code, "status": "fail"})
        else:
            checks.append({"code": code, "status": "pass"})

    preview = development.preview
    listener = preview.listener if preview is not None else None
    if listener is None:
        checks.append({"code": "TLS_LISTENER_UNCONFIGURED", "status": "fail"})
    else:
        record("TLS_CERTIFICATES_LOAD", lambda: verify_tls_listener(listener))

    if development.browser is not None:
        record("BROWSER_CA_AND_SECCOMP", lambda: verify_browser(development.browser))
    else:
        checks.append({"code": "BROWSER_COMPONENTS_UNCONFIGURED", "status": "fail"})

    record(
        "IMAGE_CAPABILITY_AND_BUDGET",
        lambda: verify_consistency(development, bounds, host_max_request_bytes),
    )
    state = "static_ready" if all(item["status"] == "pass" for item in checks) else "blocked"
    return {
        "state": state,
        "checks": checks,
        "capabilities": {
            "model_images_enabled": bool(development.model_images),
            "provider_image_submission_enabled": bool(bounds["allow_images"]),
            "browser_components_configured": development.browser is not None,
        },
        "budgets": {
            "development_max_steps": development.max_steps,
            "development_max_tool_calls": development.max_tool_calls,
            "provider_max_tool_calls": bounds["max_tool_calls"],
            "provider_max_output_tokens": bounds["max_output_tokens"],
            "provider_max_request_bytes": bounds["max_request_bytes"],
            "host_max_request_bytes": host_max_request_bytes,
            "effective_max_request_bytes": effective_request_bytes,
            "provider_max_arguments_bytes": PROVIDER_MAX_ARGUMENTS_BYTES,
            "development_run_timeout_seconds": DEVELOPMENT_RUN_TIMEOUT_SECONDS,
            "development_tool_timeout_seconds": DEVELOPMENT_TOOL_TIMEOUT_SECONDS,
            "maximum_image_batch_base64_bytes": MAX_IMAGE_BASE64_BYTES,
            "request_budget_after_maximum_image_batch": (
                effective_request_bytes - MAX_IMAGE_BASE64_BYTES
            ),
            "actual_wire_request_checked_at_runtime": True,
        },
        "prompt_sha256": prompt_hash,
        "unverified": [
            "broker_runtime", "docker_runtime", "application_startup",
            "provider_model", "provider_network_or_credentials", "image_host_product",
        ],
    }


def run_preflight(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    try:
        development, sandbox = _load_project_configs(
            args.project_root, args.development_config, args.sandbox_config,
        )
        bounds = load_provider_bounds(args.provider_bounds)
        if (
            type(args.host_max_request_bytes) is not int
            or not 256 <= args.host_max_request_bytes <= MAX_HOST_REQUEST_BYTES
        ):
            raise PreflightError("HOST_REQUEST_BUDGET_INVALID")
        digest = prompt_sha256(args.prompt)
        report = _build_report(
            development, sandbox, bounds, digest, args.host_max_request_bytes,
        )
        return report, 0 if report["state"] == "static_ready" else 2
    except PreflightError as exc:
        return {
            "state": "blocked",
            "checks": [{"code": exc.code, "status": "fail"}],
            "unverified": [
                "broker_runtime", "docker_runtime", "application_startup",
                "provider_model", "provider_network_or_credentials", "image_host_product",
            ],
        }, 2
    except Exception:
        return {
            "state": "blocked",
            "checks": [{"code": "PREFLIGHT_INTERNAL_ERROR", "status": "fail"}],
            "unverified": [
                "broker_runtime", "docker_runtime", "application_startup",
                "provider_model", "provider_network_or_credentials", "image_host_product",
            ],
        }, 2


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--development-config", type=Path, required=True)
    parser.add_argument("--sandbox-config", type=Path, required=True)
    parser.add_argument("--provider-bounds", type=Path, required=True)
    parser.add_argument(
        "--host-max-request-bytes", type=int,
        default=DEFAULT_HOST_MAX_REQUEST_BYTES,
    )
    parser.add_argument("--prompt", type=Path, required=True)
    args = parser.parse_args()
    report, status = run_preflight(args)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
