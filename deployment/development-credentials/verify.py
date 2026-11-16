"""Run D2 checks with the invoking Python and existing tools, never install.

Example from repository root using the pre-provisioned verification interpreter:
    python deployment/development-credentials/verify.py
Outputs are captured under an explicit caller-selected directory if requested.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCES = [
    "src/krim_agent/project_access/__init__.py",
    "src/krim_agent/project_access/delivery.py",
    "src/krim_agent/project_access/git.py",
    "src/krim_agent/project_access/git_credentials.py",
    "src/krim_agent/project_access/git_execution.py",
    "src/krim_agent/project_access/secrets.py",
    "src/krim_agent/project_access/service.py",
]
TESTS = [
    "tests/agent_core/test_project_access_service.py",
    "tests/agent_core/test_project_access_egress_git.py",
    "tests/agent_core/test_project_access_delivery.py",
    "tests/coding_agent/test_project_access_storage.py",
    "tests/coding_agent/test_project_credentials_delivery.py",
    "tests/coding_agent/test_project_credentials_git.py",
    "tests/coding_agent/test_project_credentials_composition.py",
    "tests/coding_agent/test_project_credentials_cache.py",
    "tests/coding_agent/test_project_credentials_runner_safety.py",
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.output is not None:
        args.output.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src"), "PYTHONDONTWRITEBYTECODE": "1"}
    commands = [
        ("pytest", ["-m", "pytest", "-q", *TESTS]),
        (
            "ruff",
            [
                "-m",
                "ruff",
                "check",
                *SOURCES,
                *TESTS[2:3],
                *TESTS[4:],
                "deployment/development-credentials",
            ],
        ),
        ("mypy", ["-m", "mypy", *SOURCES]),
    ]
    status = 0
    results = {}
    for name, command in commands:
        completed = subprocess.run(
            [sys.executable, *command],
            cwd=ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        print(completed.stdout.decode("utf-8", errors="replace"), end="", flush=True)
        results[name] = completed.returncode
        if args.output is not None:
            (args.output / f"{name}.txt").write_bytes(completed.stdout)
        status = status or completed.returncode
    if args.output is not None:
        files = [
            *SOURCES,
            TESTS[2],
            *TESTS[4:],
            "deployment/development-credentials/README.md",
            "deployment/development-credentials/verify.py",
        ]
        manifest = {p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest() for p in files}
        (args.output / "manifest.json").write_text(
            json.dumps({"checks": results, "files_sha256": manifest}, indent=2) + "\n",
            encoding="utf-8",
        )
    return status


if __name__ == "__main__":
    raise SystemExit(main())
