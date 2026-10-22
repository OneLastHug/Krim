"""工作区外的固定验证器；真实生成代码必须使用M0已认证的普通隔离入口。"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import stat
import sys
from pathlib import Path
from typing import Any


def _read_regular(root: int, name: str) -> bytes:
    parts = name.split("/")
    parent = os.dup(root)
    try:
        for component in parts[:-1]:
            st = os.stat(component, dir_fd=parent, follow_symlinks=False)
            if not stat.S_ISDIR(st.st_mode):
                raise ValueError("nonregular_source")
            child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
            live = os.fstat(child)
            if (live.st_dev, live.st_ino) != (st.st_dev, st.st_ino):
                os.close(child)
                raise ValueError("source_changed")
            os.close(parent)
            parent = child
        st = os.stat(parts[-1], dir_fd=parent, follow_symlinks=False)
        if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1 or st.st_size > 65536:
            raise ValueError("nonregular_source")
        leaf = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        try:
            live = os.fstat(leaf)
            if (
                live.st_dev,
                live.st_ino,
                live.st_size,
                live.st_nlink,
            ) != (st.st_dev, st.st_ino, st.st_size, 1):
                raise ValueError("source_changed")
            data = os.read(leaf, st.st_size + 1)
            after = os.fstat(leaf)
            if len(
                data,
            ) != st.st_size or (
                after.st_mtime_ns,
                after.st_ctime_ns,
            ) != (st.st_mtime_ns, st.st_ctime_ns):
                raise ValueError("source_changed")
            return data
        finally:
            os.close(leaf)
    finally:
        os.close(parent)


def verify_repository(directory: Path, initial: dict[str, Any], *, final: bool) -> dict[str, Any]:
    checks: dict[str, bool] = {}
    previous_path = list(sys.path)
    try:
        root = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            source = _read_regular(root, "ledger/reconcile.py")
            tests = _read_regular(root, "tests/test_reconcile.py")
            checks["inputs_unchanged"] = all(
                hashlib.sha256(_read_regular(root, name)).hexdigest() == initial[name]["sha256"]
                for name in ["transactions.csv", "catalog.csv"]
            )
        finally:
            os.close(root)
        namespace: dict[str, Any] = {
            "__name__": "verified_repository",
            "__file__": str(directory / "ledger/reconcile.py"),
        }
        sys.path.insert(0, str(directory))
        exec(compile(source, namespace["__file__"], "exec"), namespace)
        function = namespace["reconcile"]
        vectors = [
            (
                [
                    {"id": "1", "account": "a", "amount": "0.10"},
                    {"id": "2", "account": "a", "amount": "0.20"},
                ],
                {"a": 30},
            ),
            (
                [
                    {"id": "1", "account": "a", "amount": "1.00"},
                    {"id": "1", "account": "a", "amount": "1.00"},
                ],
                {"a": 100},
            ),
            (
                [
                    {"id": "1", "account": "a", "amount": "2.00"},
                    {"id": "2", "account": "a", "amount": "-0.50"},
                ],
                {"a": 150},
            ),
        ]
        for index, (rows, expected) in enumerate(vectors):
            checks[f"gold_{index}"] = function(rows) == expected
        for index, bad in enumerate(
            [
                [{"id": "1", "account": "a", "amount": "NaN"}],
                [{"id": "1", "account": "a", "amount": "0.001"}],
                [
                    {"id": "1", "account": "a", "amount": "1"},
                    {"id": "1", "account": "b", "amount": "2"},
                ],
            ],
        ):
            try:
                function(bad)
            except (ValueError, ArithmeticError):
                checks[f"reject_{index}"] = True
            else:
                checks[f"reject_{index}"] = False
        if final:
            checks["source_changed"] = (
                hashlib.sha256(source).hexdigest() != initial["ledger/reconcile.py"]["sha256"]
            )
            checks["tests_changed"] = (
                hashlib.sha256(tests).hexdigest() != initial["tests/test_reconcile.py"]["sha256"]
            )
            checks["effective_tests"] = (
                sum(
                    isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and n.name.startswith("test_")
                    for n in ast.walk(ast.parse(tests))
                )
                >= 2
            )
            root = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                checks["ledger"] = json.loads(_read_regular(root, "outputs/ledger.json")) == {
                    "a": 180,
                    "b": 250,
                }
                checks["summary"] = (
                    _read_regular(root, "outputs/summary.md").decode()
                    == "accounts=2; total_cents=430\n"
                )
            finally:
                os.close(root)
        return {
            "passed": all(checks.values()),
            "checks": checks,
            "environment_error": False,
            "baseline_checks_complete": all(
                name in checks
                for name in [
                    "gold_0",
                    "gold_1",
                    "gold_2",
                    "reject_0",
                    "reject_1",
                    "reject_2",
                ]
            ),
        }
    except (
        KeyError,
        ValueError,
        ArithmeticError,
        OSError,
        TypeError,
        SyntaxError,
        ImportError,
        SystemExit,
    ) as error:
        return {
            "passed": False,
            "checks": checks,
            "environment_error": False,
            "reason": type(error).__name__,
        }
    finally:
        sys.path[:] = previous_path


class NativeVerifier:
    def __init__(self) -> None:
        self.driver = Path(__file__).resolve()
        self.digest = hashlib.sha256(self.driver.read_bytes()).hexdigest()

    def assert_current(self) -> None:
        if hashlib.sha256(self.driver.read_bytes()).hexdigest() != self.digest:
            raise ValueError("target_binding_changed")

    def argv(self, profile: Any, directory: Path, initial_file: Path, *, final: bool) -> list[str]:
        self.assert_current()
        args = [
            sys.executable,
            "-I",
            "-B",
            str(self.driver),
            "--workspace",
            str(directory),
            "--initial",
            str(initial_file),
            "--phase",
            "final" if final else "initial",
        ]
        if not profile.fixture_only:
            from .adapters.profiles import require_m0

            require_m0(profile.m0)
            prefix = profile.m0["isolation_entry"].get("argv_prefix")
            if not isinstance(prefix, list) or not prefix:
                raise ValueError("native_not_ready")
            args = prefix + args
        return args

    def result(self, facts: dict[str, Any]) -> dict[str, Any]:
        if not facts["output_complete"]:
            return {"passed": False, "environment_error": True, "reason": "verifier_output_limit"}
        try:
            result = json.loads(facts["stdout"])
            if not isinstance(result, dict) or type(result.get("passed")) is not bool:
                raise ValueError("verifier_schema")
            return result
        except (ValueError, TypeError):
            return {"passed": False, "environment_error": True, "reason": "verifier_protocol"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--initial", type=Path, required=True)
    parser.add_argument("--phase", choices=["initial", "final"], required=True)
    args = parser.parse_args()
    outcome = verify_repository(
        args.workspace, json.loads(args.initial.read_text()), final=args.phase == "final"
    )
    print(json.dumps(outcome))
    raise SystemExit(0 if outcome["passed"] else 1)
