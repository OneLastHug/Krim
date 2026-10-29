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
    import io
    import types
    import unittest

    checks: dict[str, bool] = {}
    previous_path = list(sys.path)
    test_run = None
    phase = "final" if final else "initial"
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
        bads = [
            [{"id": "1", "account": "a", "amount": "NaN"}],
            [{"id": "1", "account": "a", "amount": "0.001"}],
            [
                {"id": "1", "account": "a", "amount": "1"},
                {"id": "1", "account": "b", "amount": "2"},
            ],
        ]
        for index, bad in enumerate(bads):
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
            # 有限unittest判据：固定文件内可发现的TestCase、至少两个有断言的方法，且真实执行成功。
            tree = ast.parse(tests)

            def calls_reconcile(node: ast.AST) -> bool:
                return any(
                    isinstance(x, ast.Call)
                    and isinstance(x.func, ast.Name)
                    and x.func.id == "reconcile"
                    for x in ast.walk(node)
                )

            asserting = set()
            for node in tree.body:
                if not isinstance(node, ast.ClassDef):
                    continue
                for method in node.body:
                    if not isinstance(method, ast.FunctionDef) or not method.name.startswith(
                        "test_"
                    ):
                        continue
                    values = {
                        target.id
                        for x in ast.walk(method)
                        if isinstance(x, ast.Assign) and calls_reconcile(x.value)
                        for target in x.targets
                        if isinstance(target, ast.Name)
                    }
                    for check in ast.walk(method):
                        is_assert = isinstance(check, ast.Assert) or (
                            isinstance(check, ast.Call)
                            and isinstance(check.func, ast.Attribute)
                            and check.func.attr.startswith("assert")
                            and isinstance(check.func.value, ast.Name)
                            and check.func.value.id == "self"
                        )
                        # 拒绝assertTrue(True)等空断言；只支持直接调用或简单局部赋值关联。
                        if is_assert and (
                            calls_reconcile(check)
                            or any(
                                isinstance(x, ast.Name) and x.id in values for x in ast.walk(check)
                            )
                        ):
                            asserting.add((node.name, method.name))
            module = types.ModuleType("_pn2_regression")
            module.__file__ = str(directory / "tests/test_reconcile.py")
            exec(compile(tests, module.__file__, "exec"), module.__dict__)
            suite = unittest.TestLoader().loadTestsFromModule(module)

            def cases(group: Any) -> list[Any]:
                return [
                    test
                    for item in group
                    for test in (cases(item) if isinstance(item, unittest.TestSuite) else [item])
                ]

            discovered = cases(suite)
            assertion_count = sum(
                (type(t).__name__, t._testMethodName) in asserting for t in discovered
            )
            if not 2 <= len(discovered) <= 32:
                checks["effective_tests"] = False
                test_run = {
                    "discovered": len(discovered),
                    "executed": 0,
                    "failures": 0,
                    "errors": 0,
                    "skipped": 0,
                    "asserting_tests": assertion_count,
                }
            else:
                result = unittest.TextTestRunner(stream=io.StringIO(), verbosity=0).run(suite)
                test_run = {
                    "discovered": len(discovered),
                    "executed": result.testsRun,
                    "failures": len(result.failures),
                    "errors": len(result.errors),
                    "skipped": len(result.skipped),
                    "asserting_tests": assertion_count,
                }
                checks["effective_tests"] = (
                    result.wasSuccessful()
                    and result.testsRun == len(discovered)
                    and not result.skipped
                    and assertion_count >= 2
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
            "schema_version": "native-verifier/1",
            "phase": phase,
            "passed": all(checks.values()),
            "checks": checks,
            "environment_error": False,
            "baseline_checks_complete": all(
                k in checks
                for k in ["gold_0", "gold_1", "gold_2", "reject_0", "reject_1", "reject_2"]
            ),
            "test_run": test_run,
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
            "schema_version": "native-verifier/1",
            "phase": phase,
            "passed": False,
            "checks": checks,
            "environment_error": False,
            "baseline_checks_complete": False,
            "test_run": test_run,
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
        from typing import cast

        from krim_agent.contracts import parse_json

        try:
            if (
                facts.get("output_complete") is not True
                or facts.get("stdout_eof") is not True
                or facts.get("stderr_eof") is not True
            ):
                raise ValueError("verifier_output_limit")
            argv = facts["argv"]
            phase = argv[argv.index("--phase") + 1]
            result = parse_json(facts["stdout"])
            if not isinstance(result, dict):
                raise ValueError("verifier_schema")
            checks = result.get("checks")
            expected = {
                "inputs_unchanged",
                "gold_0",
                "gold_1",
                "gold_2",
                "reject_0",
                "reject_1",
                "reject_2",
            }
            if phase == "final":
                expected |= {
                    "source_changed",
                    "tests_changed",
                    "effective_tests",
                    "ledger",
                    "summary",
                }
            if (
                phase not in {"initial", "final"}
                or result.get("schema_version") != "native-verifier/1"
                or result.get("phase") != phase
                or not isinstance(checks, dict)
                or set(checks) != expected
                or not all(type(v) is bool for v in checks.values())
                or type(result.get("passed")) is not bool
                or result["passed"] != all(checks.values())
                or result.get("environment_error") is not False
                or result.get("baseline_checks_complete") is not True
                or type(facts.get("wait_returncode")) is not int
                or facts["wait_returncode"] != (0 if result["passed"] else 1)
            ):
                raise ValueError("verifier_schema")
            if phase == "final":
                run = result.get("test_run")
                keys = {
                    "discovered",
                    "executed",
                    "failures",
                    "errors",
                    "skipped",
                    "asserting_tests",
                }
                if (
                    not isinstance(run, dict)
                    or set(run) != keys
                    or any(type(v) is not int or v < 0 for v in run.values())
                ):
                    raise ValueError("verifier_tests")
                counts = cast(dict[str, int], run)
                effective = (
                    2 <= counts["executed"] == counts["discovered"] <= 32
                    and counts["asserting_tests"] >= 2
                    and counts["failures"] == counts["errors"] == counts["skipped"] == 0
                )
                if checks["effective_tests"] != effective:
                    raise ValueError("verifier_tests")
            elif result.get("test_run") is not None:
                raise ValueError("verifier_tests")
            return result
        except (ValueError, TypeError, KeyError, IndexError):
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
