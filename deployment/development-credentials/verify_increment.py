"""Verify the Git transport/workspace increment without modifying accepted D2 files."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import runpy
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCES = [
    f"src/krim_agent/project_access/{name}.py"
    for name in (
        "git_transport",
        "git_transport_shim",
        "git_transport_worker",
        "git_workspace",
    )
]
TESTS = [
    f"tests/coding_agent/test_project_credentials_{name}.py"
    for name in (
        "transport",
        "transport_safety",
        "workspace",
        "increment_edges",
    )
]
DEPLOYMENT = [
    "deployment/development-credentials/verify_increment.py",
    "deployment/development-credentials/verify_transport_live.py",
    "deployment/development-credentials/probe_transport_image.py",
    "deployment/development-credentials/INCREMENT.md",
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    baseline_path = ROOT / "docs/coding-agent/evidence/D2/manifest.json"
    baseline = json.loads(baseline_path.read_text()) if baseline_path.exists() else None

    def verify_baseline():
        if baseline is not None:
            assert all(
                hashlib.sha256((ROOT / p).read_bytes()).hexdigest() == value
                for p, value in baseline["files_sha256"].items()
            )

    verify_baseline()
    previous = runpy.run_path(str(ROOT / "deployment/development-credentials/verify.py"))
    checks = {}
    environment = {**os.environ, "PYTHONPATH": str(ROOT / "src"), "PYTHONDONTWRITEBYTECODE": "1"}
    commands = [
        ("pytest", ["-m", "pytest", "-q", *previous["TESTS"], *TESTS]),
        ("ruff", ["-m", "ruff", "check", *SOURCES, *TESTS, *DEPLOYMENT[:-1]]),
        ("mypy", ["-m", "mypy", *SOURCES]),
    ]
    for label, command in commands:
        result = subprocess.run(
            [sys.executable, *command],
            cwd=ROOT,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        (args.output / f"{label}.txt").write_bytes(result.stdout)
        print(result.stdout.decode(errors="replace"), end="", flush=True)
        checks[label] = result.returncode
    verify_baseline()
    manifest = {
        "checks": checks,
        "accepted15_unchanged": baseline is not None,
        "files_sha256": {
            p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest()
            for p in (*SOURCES, *TESTS, *DEPLOYMENT)
        },
    }
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return int(any(checks.values()))


if __name__ == "__main__":
    raise SystemExit(main())
