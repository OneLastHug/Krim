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
    "tests/core/test_runtime.py",
    "tests/agent_core/test_sessions.py",
    "tests/agent_core/test_session_controls.py",
    "tests/agent_core/test_session_lifecycle.py",
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
    "tests/agent_core/test_sandbox_jobs.py",
    "tests/agent_core/test_sandbox_bindings.py",
    "tests/agent_core/test_sandbox_quota_control.py",
    "tests/agent_core/test_sandbox_quota_management.py",
    "tests/agent_core/test_sandbox_maintenance.py",
    "tests/agent_core/test_sandbox_maintenance_api.py",
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
        safe_root = evidence / "safe-eval"
        safe_root.mkdir()
        if args.suite == "ci" or not env.get("KRIM_SAFE_EVAL_ROOT"):
            env["KRIM_SAFE_EVAL_ROOT"] = str(safe_root)
        else:
            safe_root = Path(env["KRIM_SAFE_EVAL_ROOT"])
        if not safe_root.is_dir():
            print("KRIM_SAFE_EVAL_ROOT 必须是现有测试证据目录", file=sys.stderr)
            return 2
        pytest_run = Path(tempfile.mkdtemp(prefix="run-", dir=safe_root))
        command = [sys.executable, "-m", "pytest", "-ra",
                   f"--basetemp={pytest_run / 'pytest'}", f"--junitxml={evidence / 'pytest.xml'}"]
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
