"""Run bounded E checks and persist their complete output for the integrator."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contracts-root", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    package = "src/krim_agent/dev_preview"
    tests = [
        str(p.relative_to(root))
        for p in sorted((root / "tests/agent_core").glob("test_dev_preview_*.py"))
    ]
    commands = [
        [
            sys.executable,
            "-I",
            "deployment/development-preview/check.py",
            "--contracts-root",
            str(args.contracts_root),
        ],
        [
            sys.executable,
            "-m",
            "ruff",
            "check",
            "--no-cache",
            package,
            *tests,
            "deployment/development-preview/check.py",
            "deployment/development-preview/verify.py",
        ],
        [
            sys.executable,
            "-m",
            "mypy",
            "--follow-imports=silent",
            "--no-incremental",
            "--cache-dir=/dev/null",
            package,
        ],
    ]
    env = dict(
        os.environ,
        MYPYPATH=os.pathsep.join((str(root / "src"), str(args.contracts_root))),
        PYTHONDONTWRITEBYTECODE="1",
    )
    outputs = []
    failed = False
    for i, command in enumerate(commands, 1):
        result = subprocess.run(
            command, cwd=root, env=env, capture_output=True, text=True, timeout=120, check=False
        )
        text = f"CHECK {i}; exit={result.returncode}\n{result.stdout}{result.stderr}"
        print(text, flush=True)
        outputs.append(text)
        failed |= result.returncode != 0
    (root / "deployment/development-preview/verification.txt").write_text(
        "\n".join(outputs), encoding="utf-8"
    )
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
