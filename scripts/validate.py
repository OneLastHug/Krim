#!/usr/bin/env python3
"""Krim Agent 的分组验证入口；失败证据不随进程退出删除。"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FRONTEND = ROOT / "frontend"
CI_TESTS = (
    "tests/core/test_contracts.py",
    "tests/core/test_storage.py",
    "tests/coding_agent/test_external_acceptance.py",
    "tests/coding_agent/test_external_challenge.py",
    "tests/coding_agent/test_development_contracts.py",
    "tests/coding_agent/test_preview_route_contracts.py",
    "tests/coding_agent/test_sdk_process_recovery.py",
    "tests/coding_agent/test_sdk_host_bridge.py",
    "tests/coding_agent/test_sdk_run_service.py",
    "tests/coding_agent/test_stream_binding_runtime.py",
    "tests/coding_agent/test_sdk_loop_migration.py",
    "tests/coding_agent/test_sdk_model_legacy_context.py",
    "tests/coding_agent/test_sdk_migration_faults.py",
    "tests/coding_agent/test_sdk_target_phases.py",
    "tests/coding_agent/test_sdk_lifetime.py",
    "tests/coding_agent/test_development_entrypoint.py",
    "tests/agent_core/test_dev_network_broker.py",
    "tests/agent_core/test_dev_network_lifecycle.py",
    "tests/agent_core/test_dev_network_shim.py",
    "tests/agent_core/test_dev_network_command_policy.py",
    "tests/coding_agent/test_download_authorization.py",
    "tests/agent_core/test_project_access_delivery.py",
    "tests/coding_agent/test_project_credentials_delivery.py",
    "tests/coding_agent/test_project_secret_authorization.py",
    "tests/coding_agent/test_project_secret_bootstrap.py",
    "tests/coding_agent/test_project_secret_lifecycle.py",
    "tests/coding_agent/test_project_secret_composition.py",
    "tests/coding_agent/test_project_secret_storage.py",
    "tests/coding_agent/test_project_secret_wiring.py",
    "tests/coding_agent/test_project_credentials_git.py",
    "tests/coding_agent/test_project_credentials_composition.py",
    "tests/coding_agent/test_project_credentials_cache.py",
    "tests/coding_agent/test_project_credentials_runner_safety.py",
    "tests/coding_agent/test_project_credentials_transport.py",
    "tests/coding_agent/test_project_credentials_transport_safety.py",
    "tests/coding_agent/test_project_credentials_workspace.py",
    "tests/coding_agent/test_project_credentials_increment_edges.py",
    "tests/coding_agent/test_xfs_mount_resolution.py",
    "tests/agent_core/test_dev_environment_backend.py",
    "tests/agent_core/test_dev_environment_review.py",
    "tests/agent_core/test_dev_environment_execution.py",
    "tests/agent_core/test_dev_environment_fencing.py",
    "tests/agent_core/test_dev_environment_preview.py",
    "tests/agent_core/test_dev_environment_preview_control.py",
    "tests/agent_core/test_dev_environment_preview_relay.py",
    "tests/agent_core/test_dev_environment_preview_transport.py",
    "tests/agent_core/test_dev_preview_protocol.py",
    "tests/agent_core/test_dev_preview_transport.py",
    "tests/agent_core/test_dev_preview_websockets.py",
    "tests/coding_agent/test_development_storage.py",
    "tests/coding_agent/test_project_access_storage.py",
    "tests/coding_agent/test_preview_storage.py",
    "tests/coding_agent/test_preview_sqlite_api.py",
    "tests/coding_agent/test_preview_entrypoint.py",
    "tests/coding_agent/test_development_serving.py",
    "tests/coding_agent/test_browser_worker_lifecycle.py",
    "tests/coding_agent/test_browser_artifacts.py",
    "tests/coding_agent/test_browser_downloads.py",
    "tests/agent_core/test_dev_browser_bootstrap.py",
    "tests/agent_core/test_dev_browser_boundary.py",
    "tests/agent_core/test_dev_browser_chroot_acceptance.py",
    "tests/agent_core/test_dev_browser_context.py",
    "tests/agent_core/test_dev_browser_download_events.py",
    "tests/agent_core/test_dev_browser_downloads.py",
    "tests/agent_core/test_dev_browser_lifecycle.py",
    "tests/agent_core/test_dev_browser_preview.py",
    "tests/agent_core/test_dev_browser_probe_contract.py",
    "tests/agent_core/test_dev_browser_runtime_import.py",
    "tests/agent_core/test_dev_browser_seccomp_proposal.py",
    "tests/agent_core/test_dev_browser_session.py",
    "tests/agent_core/test_dev_browser_resize.py",
    "tests/agent_core/test_dev_browser_download_context.py",
    "tests/agent_core/test_dev_browser_worker_boundary.py",
    "tests/agent_core/test_dev_browser_worker_bridge.py",
    "tests/agent_core/test_dev_browser_worker_build.py",
    "tests/agent_core/test_dev_browser_worker_download_setup.py",
    "tests/agent_core/test_dev_browser_worker_ipc.py",
    "tests/agent_core/test_project_access_service.py",
    "tests/agent_core/test_project_access_egress_git.py",
    "tests/agent_core/test_sdk_adapter_probe.py",
    "tests/agent_core/test_sdk_adapter_media.py",
    "tests/coding_agent/test_sdk_context_compaction.py",
    "tests/coding_agent/test_sdk_context_source_port.py",
    "tests/coding_agent/test_development_context_v3.py",
    "tests/coding_agent/test_development_model_media.py",
    "tests/coding_agent/test_development_provider_env.py",
    "tests/coding_agent/test_development_preflight.py",
    "tests/coding_agent/test_git_grant_storage.py",
    "tests/coding_agent/test_development_git_operations.py",
    "tests/coding_agent/test_development_git_lifecycle.py",
    "tests/coding_agent/test_development_git_credentials.py",
    "tests/coding_agent/test_development_git_network.py",
    "tests/coding_agent/test_development_git_wiring.py",
    "tests/coding_agent/test_development_git_approval_recovery.py",
    "tests/agent_core/test_dev_network_observation.py",
    "tests/core/test_compaction_manifest.py",
    "tests/agent_core/test_dev_terminal_backend.py",
    "tests/agent_core/test_dev_terminal_client.py",
    "tests/agent_core/test_dev_terminal_edges.py",
    "tests/agent_core/test_dev_terminal_host.py",
    "tests/agent_core/test_dev_terminal_log.py",
    "tests/agent_core/test_dev_terminal_recovery.py",
    "tests/agent_core/test_dev_terminal_worker.py",
    "tests/core/test_runtime.py",
    "tests/core/test_tool_boundaries.py",
    "tests/agent_core/test_sessions.py",
    "tests/agent_core/test_approval_park.py",
    "tests/agent_core/test_session_controls.py",
    "tests/agent_core/test_session_lifecycle.py",
    "tests/agent_core/test_session_sidebar.py",
    "tests/agent_core/test_font_assets.py",
    "tests/agent_core/test_distribution_manifest.py",
    "tests/agent_core/test_extension_migration.py",
    "tests/agent_core/test_schema_capabilities.py",
    "tests/agent_core/test_accounts_service.py",
    "tests/agent_core/test_account_api.py",
    "tests/agent_core/test_account_cli.py",
    "tests/agent_core/test_account_runtime.py",
    "tests/agent_core/test_account_config.py",
    "tests/agent_core/test_sandbox_quota.py",
    "tests/agent_core/test_sandbox_storage.py",
    "tests/agent_core/test_sandbox_control.py",
    "tests/agent_core/test_sandbox_filesystem.py",
    "tests/agent_core/test_sandbox_leases.py",
    "tests/agent_core/test_sandbox_broker_control.py",
    "tests/agent_core/test_sandbox_broker.py",
    "tests/agent_core/test_sandbox_checkpoints.py",
    "tests/agent_core/test_sandbox_command.py",
    "tests/agent_core/test_sandbox_command_regressions.py",
    "tests/agent_core/test_sandbox_jobs.py",
    "tests/agent_core/test_sandbox_bindings.py",
    "tests/agent_core/test_sandbox_quota_control.py",
    "tests/agent_core/test_sandbox_quota_management.py",
    "tests/agent_core/test_sandbox_maintenance.py",
    "tests/agent_core/test_sandbox_maintenance_api.py",
    "tests/agent_core/test_maintenance_unlimited.py",
    "tests/agent_core/test_sandbox_recovery.py",
    "tests/agent_core/test_sandbox_transfer.py",
    "tests/agent_core/test_sandbox_file_editing.py",
    "tests/agent_core/test_personal_mcp_secrets.py",
    "tests/agent_core/test_account_mcp_service.py",
    "tests/agent_core/test_account_mcp_runtime_integration.py",
    "tests/agent_core/test_sandbox_tasks.py",
    "tests/agent_core/test_sandbox_completion.py",
    "tests/agent_core/test_browser_foundation.py",
    "tests/agent_core/test_egress_policy.py",
    "tests/agent_core/test_egress_transport.py",
    "tests/agent_core/test_runtime_profiles.py",
    "tests/agent_core/test_runtime_profile_runner.py",
    "tests/agent_core/test_quota_retirement.py",
    "tests/agent_core/test_quota_retirement_adapter.py",
    "tests/agent_core/test_sandbox_admin.py",
    "tests/agent_core/test_parallel_wave1.py",
    "tests/agent_core/test_sandbox_wave1_integration.py",
    "tests/agent_core/test_mcp_egress_integration.py",
    "tests/agent_core/test_quota_retirement_control.py",
    "tests/agent_core/test_wave1_distribution.py",
    # General session tools, private memory, live context and unlimited execution.
    "tests/agent_core/test_planning_api.py",
    "tests/agent_core/test_planning_service.py",
    "tests/agent_core/test_planning_tools.py",
    "tests/agent_core/test_interaction_api.py",
    "tests/agent_core/test_interaction_service.py",
    "tests/agent_core/test_memory_api.py",
    "tests/agent_core/test_memory_tools.py",
    "tests/agent_core/test_extensions_api.py",
    "tests/agent_core/test_extensions_service.py",
    "tests/agent_core/test_goal_api.py",
    "tests/agent_core/test_goal_continuation.py",
    "tests/agent_core/test_goal_contracts.py",
    "tests/agent_core/test_goal_service.py",
    "tests/agent_core/test_goal_tools.py",
    "tests/agent_core/test_session_goal_retry_policy.py",
    "tests/agent_core/test_session_goal_scheduler.py",
    "tests/agent_core/test_account_mcp_bundle.py",
    "tests/agent_core/test_mcp_client.py",
    "tests/agent_core/test_mcp_resources_client.py",
    "tests/agent_core/test_external_tool_bindings.py",
    "tests/agent_core/test_derived_tool_freeze.py",
    "tests/agent_core/test_stream_binding_fences.py",
    "tests/agent_core/test_tool_bundles.py",
    "tests/agent_core/test_tool_catalog_resources.py",
    "tests/agent_core/test_web_access.py",
    "tests/agent_core/test_web_result_integrity.py",
    "tests/agent_core/test_web_result_references.py",
    "tests/agent_core/test_application_web_persistence.py",
    "tests/agent_core/test_run_tool_scope_bindings.py",
    "tests/agent_core/test_run_tools.py",
    "tests/agent_core/test_run_tools_readonly.py",
    "tests/agent_core/test_run_tools_web_state.py",
    "tests/agent_core/test_runtime_wait_barrier.py",
    "tests/agent_core/test_model_request_budget.py",
    "tests/agent_core/test_session_inputs_integration.py",
    "tests/agent_core/test_session_snapshots.py",
    "tests/agent_core/test_session_tool_contract.py",
    "tests/agent_core/test_session_tool_host.py",
    "tests/agent_core/test_sdk_active_tools.py",
    "tests/agent_core/test_sdk_session_media_state.py",
    "tests/agent_core/test_sandbox_running_download.py",
    "tests/agent_core/test_sandbox_schema4.py",
    "tests/agent_core/test_session_unlimited_service.py",
    "tests/agent_core/test_session_sdk_pagination.py",
    "tests/agent_core/test_unlimited_checkpoints.py",
    "tests/agent_core/test_unlimited_environment.py",
    "tests/agent_core/test_unlimited_files.py",
    "tests/agent_core/test_unlimited_http_boundary.py",
    "tests/agent_core/test_unlimited_patch.py",
    "tests/agent_core/test_unlimited_session_settings.py",
    "tests/agent_core/test_unlimited_storage.py",
    "tests/coding_agent/test_context_api.py",
    "tests/coding_agent/test_context_service.py",
    "tests/coding_agent/test_context_unlimited.py",
    "tests/coding_agent/test_general_session_tools_flow.py",
    "tests/coding_agent/test_ordinary_context_admission.py",
    "tests/coding_agent/test_ordinary_context_application.py",
    "tests/coding_agent/test_ordinary_context_source.py",
    "tests/coding_agent/test_media_selection.py",
    "tests/coding_agent/test_session_media.py",
    "tests/coding_agent/test_session_media_application.py",
    "tests/coding_agent/test_view_image.py",
    "tests/coding_agent/test_file_tools.py",
    "tests/coding_agent/test_provider_unlimited.py",
    "tests/coding_agent/test_sdk_response_unlimited.py",
    # QA single-session recovery and isolated-worker regressions.
    "tests/agent_core/test_subagent_isolated_composition.py",
    "tests/agent_core/test_subagent_isolated_sdk.py",
    "tests/agent_core/test_subagent_isolated_tools.py",
    "tests/agent_core/test_subagent_workspace_tools.py",
    "tests/agent_core/test_subagent_followup_workspace.py",
    "tests/agent_core/test_subagent_followup_upgrade.py",
    "tests/agent_core/test_subagent_legacy_preparation.py",
    "tests/agent_core/test_session_target_upgrade.py",
    "tests/agent_core/test_subagent_profile_catalog.py",
    "tests/agent_core/test_sdk_input_recovery.py",
    "tests/agent_core/test_shell_input_admission.py",
    "tests/agent_core/test_preview_control_headers.py",
    "tests/coding_agent/test_goal_conflict_recovery.py",
    "tests/coding_agent/test_mcp_resource_recovery.py",
    "tests/coding_agent/test_preview_status_recovery.py",
    "tests/coding_agent/test_sdk_current_run_compaction.py",
    "tests/coding_agent/test_sdk_user_input.py",
    "tests/coding_agent/test_unlimited_configuration.py",
    "tests/core/test_context_manifest.py",
    "tests/core/test_http_responses.py",
    "tests/core/test_read_sides.py",
    "tests/safe_eval/test_neutral_eval.py",
    "tests/safe_eval/test_continuous_eval.py",
)
FRONTEND_CHECKS = ("check", "test:contracts", "test:components", "build")
MATERIAL_GROUPS = ("tests/core", "tests/agent_core", "tests/safe_eval")


def runtime_errors(only: str) -> list[str]:
    errors = []
    if only != "frontend":
        if not sys.platform.startswith("linux"):
            errors.append("Python 运行与隔离测试需要 Linux")
        if sys.version_info[:2] != (3, 12):
            errors.append(f"要求 Python 3.12，当前为 {sys.version.split()[0]}")
        if hasattr(os, "getuid") and os.getuid() == 0:
            errors.append("进程/资源测试要求普通非 root 用户")
    if only != "python":
        try:
            result = subprocess.run(["node", "--version"], check=True,
                                    capture_output=True, text=True)
        except (OSError, subprocess.CalledProcessError):
            errors.append("Node.js 不可用")
        else:
            match = re.search(r"v?(\d+)\.(\d+)\.(\d+)", result.stdout)
            if not match or tuple(map(int, match.groups())) < (24, 15, 0):
                errors.append("前端要求 Node.js >=24.15.0")
    return errors


def report_material_diagnostics() -> None:
    print("材料变量诊断；缺材料的实际测试不能计为通过：", flush=True)
    for group in MATERIAL_GROUPS:
        files = [*(ROOT / group).glob("test_*.py"), ROOT / group / "conftest.py"]
        references = {name for path in files if path.is_file()
                      for name in re.findall(r"\bKRIM_[A-Z0-9_]+\b", path.read_text())}
        missing = sorted(name for name in references if not os.environ.get(name))
        print(f"- {group}: 未设置 {', '.join(missing) or '无'}")


def run(command: list[str], cwd: Path, evidence: Path, label: str,
        env: dict[str, str]) -> int:
    print(f"\n== {label} ==\n$ {' '.join(command)}", flush=True)
    try:
        result = subprocess.run(command, cwd=cwd, env=env, capture_output=True)
    except OSError as error:
        (evidence / f"{label}.log").write_text(str(error))
        print(str(error), file=sys.stderr)
        return 127
    output = result.stdout + result.stderr
    (evidence / f"{label}.log").write_bytes(output)
    print(output.decode("utf-8", errors="replace"), flush=True)
    return result.returncode


def evidence_directory() -> Path:
    configured = os.environ.get("KRIM_VALIDATION_ROOT")
    if configured:
        base = Path(configured)
    elif os.name == "nt":
        base = ROOT / ".cache" / "validation"
    else:
        cache = Path(os.environ.get("XDG_CACHE_HOME", str(Path.home() / ".cache")))
        base = cache / "krim-agent" / "validation"
    base.mkdir(mode=0o700, parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix="run-", dir=base))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", choices=("ci", "full"), default="ci")
    parser.add_argument("--only", choices=("all", "python", "frontend"), default="all")
    args = parser.parse_args()
    errors = runtime_errors(args.only)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 2
    # 不使用 TemporaryDirectory：失败时保留数据库、连接/进程收据和原始日志。
    evidence = evidence_directory()
    print(f"验证记录：{evidence}", flush=True)
    env = os.environ.copy()
    env.setdefault("LANG", "C.UTF-8")
    env.setdefault("LD_LIBRARY_PATH", "")
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    if args.only != "frontend":
        # Pytest imports this checkout; spawned workers must use the same source.
        env["PYTHONPATH"] = str(ROOT / "src")
        # evidence 本身已独占创建；省去重复嵌套，给 AF_UNIX socket 留出路径长度。
        safe_root = evidence
        if args.suite == "ci" or not env.get("KRIM_SAFE_EVAL_ROOT"):
            env["KRIM_SAFE_EVAL_ROOT"] = str(safe_root)
        else:
            safe_root = Path(env["KRIM_SAFE_EVAL_ROOT"])
        if not safe_root.is_dir():
            print("KRIM_SAFE_EVAL_ROOT 必须是现有测试证据目录", file=sys.stderr)
            return 2
        pytest_run = (
            evidence if safe_root == evidence
            else Path(tempfile.mkdtemp(prefix="run-", dir=safe_root))
        )
        command = [sys.executable, "-m", "pytest", "-ra",
                   f"--basetemp={pytest_run / 'p'}", f"--junitxml={evidence / 'pytest.xml'}"]
        if args.suite == "ci":
            command += list(CI_TESTS)
        else:
            report_material_diagnostics()
        if code := run(command, ROOT, evidence, "python", env):
            return code
        if args.suite == "ci":
            print("CI 所列自供夹具完整执行；其它材料组未运行，不计为通过。", flush=True)
    if args.only != "python":
        npm = "npm.cmd" if os.name == "nt" else "npm"
        for script in FRONTEND_CHECKS:
            if code := run([npm, "run", script], FRONTEND, evidence,
                           "frontend-" + script.replace(":", "-"), env):
                return code
    print("所选验证组通过；未运行组不计为通过。", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
