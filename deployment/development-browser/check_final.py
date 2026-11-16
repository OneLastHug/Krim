"""Check current runtime delivery, without reading or regenerating old logs."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

DEPLOY_FILES = (
    "Dockerfile",
    "worker.py",
    "stdio_channel.py",
    "entrypoint.py",
    "build_context.py",
    "prepare_wheels.py",
    "worker-downloads.json",
    "worker-requirements.txt",
    "seccomp-chroot.json",
    "host_bridge.py",
    "launcher.py",
    "fixed_proxy.py",
    "sandbox_probe.py",
    "namespace_probe.py",
    "network_probe.py",
    "ttl_probe.py",
    "verify_container.py",
    "verify_root_bridge.py",
    "seccomp_proposal.py",
    "RUNTIME-INTERFACE.md",
    "check_final.py",
    "freeze_final.py",
)


def main() -> int:
    here = Path(__file__).resolve().parent
    root = here.parents[1]
    tests = sorted(
        str(p.relative_to(root)) for p in (root / "tests/agent_core").glob("test_dev_browser_*.py")
    )
    files = [str((here / name).relative_to(root)) for name in DEPLOY_FILES if name.endswith(".py")]
    env = {**os.environ, "PYTHONPATH": str(root / "src"), "PYTHONDONTWRITEBYTECODE": "1"}
    checks = [
        [sys.executable, "-m", "pytest", *tests, "-q"],
        [sys.executable, "-m", "ruff", "check", "src/krim_agent/dev_browser", *tests, *files],
        [sys.executable, "-m", "mypy", "--follow-imports=silent", "src/krim_agent/dev_browser"],
    ]
    failed = False
    for command in checks:
        completed = subprocess.run(command, cwd=root, env=env, text=True, timeout=100, check=False)
        failed |= completed.returncode != 0
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
