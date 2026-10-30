"""agent-bench/1 的固定工作区外校验驱动。

本模块只接受固定 case_id。它会在当前驱动进程中执行候选源码，因此真实目标只能由
已资格的隔离入口启动本文件的 CLI；测试中的源码由本项目测试自己创建，是可信 fixture，
不代表模型结果。此模块不接收或导入调用方提供的 verifier。
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import stat
import sys
import types
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal, cast

from pydantic import Field, model_validator

from krim_agent.contracts import StrictModel

from .agent_benchmark import REFERENCE_SCHEMA, benchmark_case
from .domain import CaseSpec, Digest, Identity, neutral_fingerprint, relative_file

SCHEMA_VERSION: Literal["agent-bench-verifier/1"] = "agent-bench-verifier/1"
MAX_FILE_BYTES = 65536
MAX_TOTAL_BYTES = 262144
MAX_ENTRIES = 256
MAX_DEPTH = 8


class BenchmarkFailure(ValueError):
    """验证驱动遇到不可安全继续的工作区或绑定错误。"""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class TestCounts(StrictModel):
    discovered: int = Field(ge=1)
    executed: int = Field(ge=1)
    passed: int = Field(ge=0)
    failures: int = Field(ge=0)
    errors: int = Field(ge=0)
    skipped: int = Field(ge=0)

    @model_validator(mode="after")
    def consistent(self) -> TestCounts:
        if self.executed != self.discovered:
            raise ValueError("benchmark_test_counts")
        if self.passed + self.failures + self.errors + self.skipped != self.executed:
            raise ValueError("benchmark_test_counts")
        return self


class BenchmarkResult(StrictModel):
    schema_version: Literal["agent-bench-verifier/1"] = SCHEMA_VERSION
    case_id: Identity
    case_fingerprint: Digest
    phase: Literal["initial", "final"]
    checks: dict[str, bool]
    passed: bool
    environment_error: bool
    baseline_checks_complete: bool | None
    test_run: TestCounts | None
    reason: str | None = None

    @model_validator(mode="after")
    def strict_checks(self) -> BenchmarkResult:
        if not all(type(value) is bool for value in self.checks.values()):
            raise ValueError("benchmark_checks")
        if self.passed is not all(self.checks.values()):
            raise ValueError("benchmark_passed")
        if self.environment_error and self.passed:
            raise ValueError("benchmark_environment_error")
        return self


_MUTABLE_FILES: dict[str, frozenset[str]] = {
    "amount-parse-v1": frozenset({"money.py"}),
    "inventory-snapshot-v1": frozenset({"inventory/catalog.py", "inventory/cli.py"}),
    "report-refactor-v1": frozenset({"report.py"}),
    "csv-groups-v1": frozenset({"csv_report.py"}),
    "json-records-v1": frozenset({"records.py"}),
    "inventory-artifacts-v1": frozenset({"pipeline.py"}),
}
_OUTPUT_FILES: dict[str, frozenset[str]] = {
    "amount-parse-v1": frozenset(),
    "inventory-snapshot-v1": frozenset({"outputs/snapshot.json"}),
    "report-refactor-v1": frozenset(),
    "csv-groups-v1": frozenset({"outputs/groups.json"}),
    "json-records-v1": frozenset(),
    "inventory-artifacts-v1": frozenset(
        {"outputs/summary.json", "outputs/summary.md"}
    ),
}
_FINAL_CHECKS: dict[str, tuple[str, ...]] = {
    "amount-parse-v1": (
        "behavior.valid_decimal",
        "behavior.negative_decimal",
        "behavior.reject_excess_scale",
        "behavior.reject_invalid_text",
        "behavior.reject_non_finite",
        "source.changed",
        "protected.inputs_unchanged",
    ),
    "inventory-snapshot-v1": (
        "behavior.aggregate_and_sort",
        "behavior.reject_invalid_row",
        "artifact.snapshot_json",
        "structure.cli_entrypoint",
        "source.changed",
        "protected.inputs_unchanged",
    ),
    "report-refactor-v1": (
        "behavior.empty_rows",
        "behavior.unicode_and_sorting",
        "behavior.numeric_count",
        "structure.normalize_rows",
        "structure.render_report",
        "structure.pipeline_uses_helpers",
        "source.changed",
        "protected.inputs_unchanged",
    ),
    "csv-groups-v1": (
        "behavior.deduplication",
        "behavior.missing_values",
        "behavior.unicode_normalization",
        "behavior.grouping_and_totals",
        "artifact.groups_json",
        "structure.cli_entrypoint",
        "source.changed",
        "protected.inputs_unchanged",
    ),
    "json-records-v1": (
        "behavior.normalize_valid_record",
        "behavior.collect_all_errors",
        "behavior.preserve_unrelated_fields",
        "source.changed",
        "protected.inputs_unchanged",
    ),
    "inventory-artifacts-v1": (
        "behavior.report_json_data",
        "behavior.report_markdown",
        "artifact.summary_json",
        "artifact.summary_markdown",
        "structure.main_uses_builder",
        "source.changed",
        "protected.inputs_unchanged",
    ),
}
_INITIAL_BASE = (
    "baseline.target_not_yet_met",
    "baseline.mutable_source_pristine",
    "baseline.protected_inputs_unchanged",
    "baseline.probes_executed",
)
_INITIAL_CHECKS: dict[str, tuple[str, ...]] = {
    case_id: _INITIAL_BASE
    + (
        ("baseline.original_semantics_held", "baseline.refactor_target_missing")
        if case_id == "report-refactor-v1"
        else ()
    )
    for case_id in _FINAL_CHECKS
}


@contextmanager
def _loaded_module(
    root_fd: int, relative: str, display_path: Path
) -> Iterator[types.ModuleType]:
    source = _read_regular(root_fd, relative).decode("utf-8", errors="strict")
    module_name = f"_agent_bench_candidate_{uuid.uuid4().hex}"
    module = types.ModuleType(module_name)
    module.__file__ = str(display_path / relative)
    sys.modules[module_name] = module
    try:
        exec(compile(source, module.__file__, "exec"), module.__dict__)
        yield module
    finally:
        sys.modules.pop(module_name, None)


@contextmanager
def _loaded_inventory_package(
    root_fd: int, workspace: Path
) -> Iterator[tuple[types.ModuleType, types.ModuleType]]:
    package_name = f"_agent_bench_inventory_{uuid.uuid4().hex}"
    package = types.ModuleType(package_name)
    package.__package__ = package_name
    package.__path__ = [str(workspace / "inventory")]
    package.__file__ = str(workspace / "inventory/__init__.py")
    names = [package_name]
    sys.modules[package_name] = package
    try:
        source = _read_regular(root_fd, "inventory/__init__.py").decode("utf-8", "strict")
        exec(compile(source, package.__file__, "exec"), package.__dict__)
        modules: dict[str, types.ModuleType] = {}
        for leaf in ("catalog", "cli"):
            name = f"{package_name}.{leaf}"
            module = types.ModuleType(name)
            module.__package__ = package_name
            module.__file__ = str(workspace / f"inventory/{leaf}.py")
            sys.modules[name] = module
            names.append(name)
            source = _read_regular(root_fd, f"inventory/{leaf}.py").decode("utf-8", "strict")
            exec(compile(source, module.__file__, "exec"), module.__dict__)
            modules[leaf] = module
        yield modules["catalog"], modules["cli"]
    finally:
        for name in reversed(names):
            sys.modules.pop(name, None)


def _callable(module: types.ModuleType, name: str) -> Callable[..., Any]:
    value = getattr(module, name, None)
    if not callable(value):
        raise BenchmarkFailure("candidate_interface_missing")
    return cast(Callable[..., Any], value)


def _raises(call: Callable[[], Any]) -> bool:
    try:
        call()
    except Exception:
        return True
    return False


def _probes(
    values: Sequence[tuple[str, Callable[[], bool]]]
) -> tuple[dict[str, bool], TestCounts]:
    checks: dict[str, bool] = {}
    for name, probe in values:
        try:
            checks[name] = probe() is True
        except Exception:
            checks[name] = False
    count = len(values)
    passed = sum(value is True for value in checks.values())
    return checks, TestCounts(
        discovered=count,
        executed=count,
        passed=passed,
        failures=count - passed,
        errors=0,
        skipped=0,
    )


def _check_amount(root_fd: int, workspace: Path) -> tuple[dict[str, bool], TestCounts]:
    from decimal import Decimal

    with _loaded_module(root_fd, "money.py", workspace) as module:
        parse_amount = _callable(module, "parse_amount")

        def amount_is(text: str, expected: str) -> bool:
            value = parse_amount(text)
            return type(value) is Decimal and value == Decimal(expected)

        return _probes(
            [
                ("behavior.valid_decimal", lambda: amount_is("12.30", "12.30")),
                ("behavior.negative_decimal", lambda: amount_is("-0.45", "-0.45")),
                (
                    "behavior.reject_excess_scale",
                    lambda: _raises(lambda: parse_amount("1.234")),
                ),
                (
                    "behavior.reject_invalid_text",
                    lambda: _raises(lambda: parse_amount("12..3")),
                ),
                (
                    "behavior.reject_non_finite",
                    lambda: _raises(lambda: parse_amount("NaN")),
                ),
            ]
        )


def _check_inventory(
    root_fd: int, workspace: Path, files: Mapping[str, bytes]
) -> tuple[dict[str, bool], TestCounts]:
    with _loaded_inventory_package(root_fd, workspace) as (catalog, cli):
        build_snapshot = _callable(catalog, "build_snapshot")
        main = _callable(cli, "main")
        valid_rows = [
            {"sku": "B", "quantity": 2},
            {"sku": " A ", "quantity": 1},
            {"sku": "B", "quantity": 3},
        ]
        expected = {
            "items": [{"sku": "A", "quantity": 1}, {"sku": "B", "quantity": 5}],
            "total_quantity": 6,
        }
        entry_source = _read_regular(root_fd, "inventory/__main__.py").decode("utf-8", "strict")
        cli_source = _read_regular(root_fd, "inventory/cli.py").decode("utf-8", "strict")
        cli_tree = ast.parse(cli_source)
        main_node = _function_node(cli_tree, "main")
        entry_ok = (
            main_node is not None
            and _calls_name(main_node, "build_snapshot")
            and _calls_method(main_node, {"write_text", "write_bytes", "dump"})
            and "from .cli import main" in entry_source
            and "SystemExit(main())" in entry_source
        )
        return _probes(
            [
                (
                    "behavior.aggregate_and_sort",
                    lambda: build_snapshot(valid_rows) == expected,
                ),
                (
                    "behavior.reject_invalid_row",
                    lambda: _raises(
                        lambda: build_snapshot([{"sku": "x", "quantity": True}])
                    ),
                ),
                (
                    "artifact.snapshot_json",
                    lambda: _json_output_matches(
                        files.get("outputs/snapshot.json"), expected
                    ),
                ),
                (
                    "structure.cli_entrypoint",
                    lambda: entry_ok and callable(main),
                ),
            ]
        )


def _check_refactor(
    root_fd: int, workspace: Path
) -> tuple[dict[str, bool], TestCounts]:
    with _loaded_module(root_fd, "report.py", workspace) as module:
        build_report = _callable(module, "build_report")
        vectors: list[tuple[list[dict[str, Any]], str]] = [
            ([], "name,count\n"),
            (
                [{"name": " Café ", "count": "2"}, {"name": " TEA ", "count": "1"}],
                "name,count\ncafé,2\ntea,1\n",
            ),
            (
                [{"name": "x", "count": 3}, {"name": "x", "count": "04"}],
                "name,count\nx,3\nx,4\n",
            ),
        ]
        def preserves(rows: list[dict[str, Any]], expected: str) -> bool:
            return _safe_equal(lambda: build_report(rows), expected)

        semantic = [preserves(rows, expected) for rows, expected in vectors]
        tree = ast.parse(_read_regular(root_fd, "report.py").decode("utf-8", "strict"))
        build_node = _function_node(tree, "build_report")
        normalize_node = _function_node(tree, "normalize_rows")
        render_node = _function_node(tree, "render_report")
        probes = {
            "behavior.empty_rows": semantic[0],
            "behavior.unicode_and_sorting": semantic[1],
            "behavior.numeric_count": semantic[2],
            "structure.normalize_rows": normalize_node is not None,
            "structure.render_report": render_node is not None,
            "structure.pipeline_uses_helpers": (
                build_node is not None
                and _calls_name(build_node, "normalize_rows")
                and _calls_name(build_node, "render_report")
            ),
        }
        passed = sum(value is True for value in probes.values())
        count = len(probes)
        return probes, TestCounts(
            discovered=count,
            executed=count,
            passed=passed,
            failures=count - passed,
            errors=0,
            skipped=0,
        )


def _csv_group_map(value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(value, dict) or not isinstance(value.get("groups"), list):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for item in value["groups"]:
        if isinstance(item, dict) and isinstance(item.get("group"), str):
            result[item["group"]] = item
    return result


def _check_csv(
    root_fd: int, workspace: Path, files: Mapping[str, bytes]
) -> tuple[dict[str, bool], TestCounts]:
    with _loaded_module(root_fd, "csv_report.py", workspace) as module:
        summarize_csv = _callable(module, "summarize_csv")
        main = _callable(module, "main")
        text = files["transactions.csv"].decode("utf-8", "strict")
        summary = _safe_value(lambda: summarize_csv(text))
        groups = summary.get("groups") if isinstance(summary, dict) else None
        group_map = _csv_group_map(summary)
        tree = ast.parse(_read_regular(root_fd, "csv_report.py").decode("utf-8", "strict"))
        main_node = _function_node(tree, "main")
        cli_ok = (
            main_node is not None
            and _calls_name(main_node, "summarize_csv")
            and _calls_method(main_node, {"write_text", "write_bytes", "dump"})
        )
        expected_groups = ["(missing)", "咖啡", "茶"]
        missing = group_map.get("(missing)", {})
        coffee = group_map.get("咖啡", {})
        tea = group_map.get("茶", {})
        expected_artifact = {
            "groups": [
                {"group": "(missing)", "count": 1, "total": "2.00", "labels": ["Café"]},
                {"group": "咖啡", "count": 1, "total": "0.00", "labels": ["浅焙"]},
                {"group": "茶", "count": 2, "total": "2.00", "labels": ["乌龙", "绿茶"]},
            ]
        }
        return _probes(
            [
                (
                    "behavior.deduplication",
                    lambda: tea.get("count") == 2
                    and "重复" not in tea.get("labels", []),
                ),
                (
                    "behavior.missing_values",
                    lambda: missing.get("count") == 1
                    and missing.get("total") == "2.00",
                ),
                (
                    "behavior.unicode_normalization",
                    lambda: coffee.get("labels") == ["浅焙"]
                    and missing.get("labels") == ["Café"],
                ),
                (
                    "behavior.grouping_and_totals",
                    lambda: isinstance(groups, list)
                    and [row.get("group") for row in groups if isinstance(row, dict)]
                    == expected_groups
                    and tea.get("total") == "2.00"
                    and tea.get("labels") == ["乌龙", "绿茶"],
                ),
                (
                    "artifact.groups_json",
                    lambda: _json_output_matches(
                        files.get("outputs/groups.json"), expected_artifact
                    ),
                ),
                ("structure.cli_entrypoint", lambda: cli_ok and callable(main)),
            ]
        )


def _check_records(
    root_fd: int, workspace: Path, files: Mapping[str, bytes]
) -> tuple[dict[str, bool], TestCounts]:
    with _loaded_module(root_fd, "records.py", workspace) as module:
        normalize_record = _callable(module, "normalize_record")
        inputs = _strict_json(files["records.json"].decode("utf-8", "strict"))
        valid_input, invalid_input = inputs
        valid = _safe_value(lambda: normalize_record(valid_input))
        invalid = _safe_value(lambda: normalize_record(invalid_input))
        expected_valid = {
            "ok": True,
            "record": {
                "id": "r-7",
                "email": "a@example.com",
                "display_name": "李雷",
                "age": 0,
                "custom": {"label": " keep "},
            },
            "errors": [],
        }
        expected_errors = [
            {"path": "/id", "code": "required_string", "message": "must be a non-empty string"},
            {"path": "/email", "code": "invalid_email", "message": "must be a valid email address"},
            {
                "path": "/age",
                "code": "invalid_age",
                "message": "must be a non-negative integer or null",
            },
        ]
        errors = invalid.get("errors") if isinstance(invalid, dict) else None
        complete_errors = (
            invalid == {"ok": False, "record": None, "errors": expected_errors}
            and isinstance(errors, list)
            and all(
                isinstance(error, dict)
                and set(error) == {"path", "code", "message"}
                and all(
                    isinstance(error[field], str)
                    for field in ("path", "code", "message")
                )
                for error in errors
            )
        )
        unrelated = (
            isinstance(valid, dict)
            and isinstance(valid.get("record"), dict)
            and valid["record"].get("custom") == {"label": " keep "}
        )
        return _probes(
            [
                ("behavior.normalize_valid_record", lambda: valid == expected_valid),
                ("behavior.collect_all_errors", lambda: complete_errors),
                ("behavior.preserve_unrelated_fields", lambda: unrelated),
            ]
        )


def _check_artifacts(
    root_fd: int, workspace: Path, files: Mapping[str, bytes]
) -> tuple[dict[str, bool], TestCounts]:
    with _loaded_module(root_fd, "pipeline.py", workspace) as module:
        build_report = _callable(module, "build_report")
        main = _callable(module, "main")
        records = _strict_json(files["source/stock.json"].decode("utf-8", "strict"))
        expected_summary = {
            "records": 2,
            "total_quantity": 3,
            "skus": ["K-1", "K-2"],
        }
        expected_markdown = (
            "# Inventory report\n\n- Records: 2\n- Total quantity: 3\n- SKUs: K-1, K-2\n"
        )
        built = _safe_value(lambda: build_report(records))
        valid_tuple = isinstance(built, tuple) and len(built) == 2
        summary_value = built[0] if valid_tuple else None
        markdown_value = built[1] if valid_tuple else None
        source = _read_regular(root_fd, "pipeline.py").decode("utf-8", "strict")
        main_node = _function_node(ast.parse(source), "main")
        uses_builder = (
            main_node is not None
            and _calls_name(main_node, "build_report")
            and _calls_method(main_node, {"write_text", "write_bytes", "dump"})
        )
        return _probes(
            [
                ("behavior.report_json_data", lambda: summary_value == expected_summary),
                ("behavior.report_markdown", lambda: markdown_value == expected_markdown),
                (
                    "artifact.summary_json",
                    lambda: _json_output_matches(
                        files.get("outputs/summary.json"), expected_summary
                    ),
                ),
                (
                    "artifact.summary_markdown",
                    lambda: files.get("outputs/summary.md")
                    == expected_markdown.encode("utf-8"),
                ),
                ("structure.main_uses_builder", lambda: uses_builder and callable(main)),
            ]
        )


_CHECKERS: dict[
    str,
    Callable[[int, Path, Mapping[str, bytes]], tuple[dict[str, bool], TestCounts]],
] = {
    "amount-parse-v1": lambda fd, workspace, _: _check_amount(fd, workspace),
    "inventory-snapshot-v1": _check_inventory,
    "report-refactor-v1": lambda fd, workspace, _: _check_refactor(fd, workspace),
    "csv-groups-v1": _check_csv,
    "json-records-v1": _check_records,
    "inventory-artifacts-v1": _check_artifacts,
}
_REFERENCE_IDS = {
    "amount-parse-v1": "amount-decimal-contract-v1",
    "inventory-snapshot-v1": "inventory-snapshot-contract-v1",
    "report-refactor-v1": "report-output-semantics-v1",
    "csv-groups-v1": "csv-unicode-groups-contract-v1",
    "json-records-v1": "json-record-validation-contract-v1",
    "inventory-artifacts-v1": "inventory-json-markdown-artifacts-v1",
}


def _expected_initial_files(case: CaseSpec) -> dict[str, bytes]:
    values = {name: text.encode("utf-8") for name, text in case.files.items()}
    values["INSTRUCTIONS.md"] = case.instruction.encode("utf-8")
    return values


def _case_identity(case: CaseSpec) -> str:
    reference = case.reference
    expected = {
        "schema_version": REFERENCE_SCHEMA,
        "case_id": case.case_id,
        "checker_id": f"{case.case_id}/fixed-checker-1",
        "reference_id": _REFERENCE_IDS[case.case_id],
    }
    if case.verifier != "agent-bench/1" or reference != expected:
        raise BenchmarkFailure("benchmark_reference_binding")
    return neutral_fingerprint(case.model_dump(mode="json"))


def _validate_initial_manifest(
    case: CaseSpec, initial: Mapping[str, Any], frozen_files: Mapping[str, bytes]
) -> None:
    if set(initial) != set(frozen_files):
        raise BenchmarkFailure("initial_manifest_shape")
    for relative, expected in frozen_files.items():
        item = initial.get(relative)
        if not isinstance(item, dict) or set(item) != {"sha256", "bytes"}:
            raise BenchmarkFailure("initial_manifest_shape")
        if (
            type(item.get("bytes")) is not int
            or item["bytes"] != len(expected)
            or item.get("sha256") != hashlib.sha256(expected).hexdigest()
        ):
            raise BenchmarkFailure("initial_manifest_binding")
    if case.case_id not in _CHECKERS:
        raise BenchmarkFailure("unknown_benchmark_case")


def _open_absolute_directory(path: Path) -> int:
    if not path.is_absolute():
        raise BenchmarkFailure("workspace_path_invalid")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    fd: int | None = None
    try:
        fd = os.open(os.sep, flags)
        for component in path.parts:
            if component in {path.anchor, "", "."}:
                continue
            if component == "..":
                raise BenchmarkFailure("workspace_path_invalid")
            info = os.stat(component, dir_fd=fd, follow_symlinks=False)
            if not stat.S_ISDIR(info.st_mode):
                raise BenchmarkFailure("workspace_path_invalid")
            child = os.open(component, flags, dir_fd=fd)
            live = os.fstat(child)
            if (live.st_dev, live.st_ino) != (info.st_dev, info.st_ino):
                os.close(child)
                raise BenchmarkFailure("workspace_changed")
            os.close(fd)
            fd = child
        result = fd
        fd = None
        return result
    except BenchmarkFailure:
        raise
    except OSError:
        raise BenchmarkFailure("workspace_path_invalid") from None
    finally:
        if fd is not None:
            os.close(fd)


def _read_regular(root_fd: int, relative: str) -> bytes:
    try:
        relative_file(relative)
    except ValueError:
        raise BenchmarkFailure("workspace_path_invalid") from None
    parts = relative.split("/")
    parent = os.dup(root_fd)
    try:
        for component in parts[:-1]:
            info = os.stat(component, dir_fd=parent, follow_symlinks=False)
            if not stat.S_ISDIR(info.st_mode):
                raise BenchmarkFailure("workspace_nonregular")
            child = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=parent,
            )
            live = os.fstat(child)
            if (live.st_dev, live.st_ino) != (info.st_dev, info.st_ino):
                os.close(child)
                raise BenchmarkFailure("workspace_changed")
            os.close(parent)
            parent = child
        return _read_leaf(parent, parts[-1])
    except BenchmarkFailure:
        raise
    except OSError:
        raise BenchmarkFailure("workspace_missing_or_unsafe") from None
    finally:
        os.close(parent)


def _read_leaf(parent_fd: int, name: str) -> bytes:
    info = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > MAX_FILE_BYTES:
        raise BenchmarkFailure("workspace_nonregular_or_oversize")
    leaf = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent_fd)
    try:
        live = os.fstat(leaf)
        if (live.st_dev, live.st_ino, live.st_size, live.st_nlink) != (
            info.st_dev,
            info.st_ino,
            info.st_size,
            1,
        ):
            raise BenchmarkFailure("workspace_changed")
        chunks: list[bytes] = []
        remaining = info.st_size + 1
        while remaining:
            chunk = os.read(leaf, min(remaining, 16384))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        after = os.fstat(leaf)
        if len(data) != info.st_size or (after.st_mtime_ns, after.st_ctime_ns) != (
            info.st_mtime_ns,
            info.st_ctime_ns,
        ):
            raise BenchmarkFailure("workspace_changed")
        return data
    finally:
        os.close(leaf)


def _scan_workspace(
    root_fd: int, input_paths: set[str], output_paths: frozenset[str]
) -> dict[str, bytes]:
    allowed_files = input_paths | set(output_paths)
    allowed_dirs = {"outputs"}
    for relative in allowed_files:
        path = Path(relative)
        while path.parent != Path("."):
            allowed_dirs.add(path.parent.as_posix())
            path = path.parent
    result: dict[str, bytes] = {}
    total = 0
    entries = 0

    def visit(fd: int, prefix: str, depth: int) -> None:
        nonlocal entries, total
        if depth > MAX_DEPTH:
            raise BenchmarkFailure("workspace_depth_limit")
        for name in sorted(os.listdir(fd)):
            entries += 1
            if entries > MAX_ENTRIES:
                raise BenchmarkFailure("workspace_entry_limit")
            relative = prefix + name
            info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode):
                if relative not in allowed_dirs:
                    raise BenchmarkFailure("workspace_extra_directory")
                child = os.open(
                    name,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                    dir_fd=fd,
                )
                try:
                    live = os.fstat(child)
                    if (live.st_dev, live.st_ino) != (info.st_dev, info.st_ino):
                        raise BenchmarkFailure("workspace_changed")
                    visit(child, relative + "/", depth + 1)
                finally:
                    os.close(child)
                continue
            if relative not in allowed_files:
                raise BenchmarkFailure("workspace_extra_file")
            data = _read_leaf(fd, name)
            total += len(data)
            if total > MAX_TOTAL_BYTES:
                raise BenchmarkFailure("workspace_total_limit")
            result[relative] = data

    try:
        visit(root_fd, "", 0)
    except BenchmarkFailure:
        raise
    except OSError:
        raise BenchmarkFailure("workspace_scan_failed") from None
    if not set(input_paths) <= set(result):
        raise BenchmarkFailure("workspace_input_missing")

    actual_dirs: set[str] = set()

    def collect_dirs(fd: int, prefix: str) -> None:
        for name in os.listdir(fd):
            info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode):
                relative = prefix + name
                actual_dirs.add(relative)
                child = os.open(
                    name,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                    dir_fd=fd,
                )
                try:
                    collect_dirs(child, relative + "/")
                finally:
                    os.close(child)
            elif not stat.S_ISREG(info.st_mode):
                raise BenchmarkFailure("workspace_nonregular")

    try:
        collect_dirs(root_fd, "")
    except BenchmarkFailure:
        raise
    except OSError:
        raise BenchmarkFailure("workspace_scan_failed") from None
    if actual_dirs != allowed_dirs:
        raise BenchmarkFailure("workspace_directory_shape")
    return result


def _safe_equal(call: Callable[[], Any], expected: Any) -> bool:
    try:
        return (call() == expected) is True
    except Exception:
        return False


def _safe_value(call: Callable[[], Any]) -> Any:
    try:
        return call()
    except Exception:
        return None


def _strict_json(data: str) -> Any:
    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise ValueError("duplicate_json_key")
            result[key] = value
        return result

    def constant(_: str) -> None:
        raise ValueError("nonfinite_json")

    return json.loads(data, object_pairs_hook=pairs, parse_constant=constant)


def _json_output_matches(data: bytes | None, expected: Any) -> bool:
    if data is None:
        return False
    try:
        return (_strict_json(data.decode("utf-8", "strict")) == expected) is True
    except (UnicodeError, ValueError, TypeError):
        return False


def _function_node(tree: ast.AST, name: str) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    for node in getattr(tree, "body", []):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    return None


def _name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _calls_name(node: ast.AST, name: str) -> bool:
    return any(
        isinstance(item, ast.Call) and _name(item.func) == name for item in ast.walk(node)
    )


def _calls_method(node: ast.AST, names: set[str]) -> bool:
    return any(
        isinstance(item, ast.Call)
        and isinstance(item.func, ast.Attribute)
        and item.func.attr in names
        for item in ast.walk(node)
    )


def _finalize_checks(
    case_id: str,
    phase: Literal["initial", "final"],
    probes: dict[str, bool],
    test_run: TestCounts,
    *,
    source_changed: bool,
    protected_unchanged: bool,
) -> tuple[dict[str, bool], TestCounts, bool]:
    final = dict(probes)
    final["source.changed"] = source_changed
    final["protected.inputs_unchanged"] = protected_unchanged
    if phase == "final":
        checks = {name: final.get(name, False) for name in _FINAL_CHECKS[case_id]}
        return checks, test_run, False

    target_values = [
        value
        for name, value in final.items()
        if name.startswith(("behavior.", "artifact.", "structure."))
    ]
    baseline = {
        "baseline.target_not_yet_met": bool(target_values) and not all(target_values),
        "baseline.mutable_source_pristine": not source_changed,
        "baseline.protected_inputs_unchanged": protected_unchanged,
        "baseline.probes_executed": (
            test_run.discovered > 0
            and test_run.executed == test_run.discovered
            and test_run.errors == 0
            and test_run.skipped == 0
        ),
    }
    if case_id == "report-refactor-v1":
        behavior_values = [value for key, value in final.items() if key.startswith("behavior.")]
        baseline["baseline.original_semantics_held"] = bool(behavior_values) and all(
            behavior_values
        )
        baseline["baseline.refactor_target_missing"] = not all(
            final.get(key, False)
            for key in (
                "structure.normalize_rows",
                "structure.render_report",
                "structure.pipeline_uses_helpers",
            )
        )
    checks = {name: baseline.get(name, False) for name in _INITIAL_CHECKS[case_id]}
    complete = (
        set(checks) == set(_INITIAL_CHECKS[case_id])
        and test_run.discovered > 0
        and test_run.executed == test_run.discovered
        and test_run.errors == 0
        and test_run.skipped == 0
    )
    return checks, test_run, complete


def _error_result(
    case_id: str,
    fingerprint: str,
    phase: Literal["initial", "final"],
    code: str,
) -> dict[str, Any]:
    expected = _INITIAL_CHECKS[case_id] if phase == "initial" else _FINAL_CHECKS[case_id]
    checks = {name: False for name in expected}
    return BenchmarkResult(
        case_id=case_id,
        case_fingerprint=fingerprint,
        phase=phase,
        checks=checks,
        passed=False,
        environment_error=True,
        baseline_checks_complete=False if phase == "initial" else None,
        test_run=None,
        reason=code,
    ).model_dump(mode="json")


def verify_benchmark(
    case_id: str,
    workspace: Path,
    initial: Mapping[str, Any],
    phase: Literal["initial", "final"],
) -> dict[str, Any]:
    """运行固定 case_id 的 checker；真实候选仅应在已资格隔离入口中执行此函数。"""
    case = benchmark_case(case_id)
    fingerprint = _case_identity(case)
    try:
        frozen = _expected_initial_files(case)
        _validate_initial_manifest(case, initial, frozen)
        root_fd = _open_absolute_directory(workspace)
        try:
            input_paths = set(frozen)
            before = _scan_workspace(root_fd, input_paths, _OUTPUT_FILES[case_id])
            mutable = _MUTABLE_FILES[case_id]
            if phase == "initial":
                for relative, expected in frozen.items():
                    if before.get(relative) != expected:
                        raise BenchmarkFailure("initial_workspace_binding")
            checker_files = dict(before)
            for relative in input_paths - mutable:
                checker_files[relative] = frozen[relative]
            probes, test_run = _CHECKERS[case_id](root_fd, workspace, checker_files)
            after = _scan_workspace(root_fd, input_paths, _OUTPUT_FILES[case_id])
            source_changed = any(
                hashlib.sha256(after[name]).hexdigest() != initial[name]["sha256"]
                for name in mutable
            )
            protected = (input_paths - mutable) | {"INSTRUCTIONS.md"}
            protected_unchanged = all(
                hashlib.sha256(after[name]).hexdigest() == initial[name]["sha256"]
                for name in protected
            )
            checks, counts, baseline_complete = _finalize_checks(
                case_id,
                phase,
                probes,
                test_run,
                source_changed=source_changed,
                protected_unchanged=protected_unchanged,
            )
        finally:
            os.close(root_fd)
        expected_names = set(
            _INITIAL_CHECKS[case_id] if phase == "initial" else _FINAL_CHECKS[case_id]
        )
        if set(checks) != expected_names:
            raise BenchmarkFailure("benchmark_check_set")
        result = BenchmarkResult(
            case_id=case_id,
            case_fingerprint=fingerprint,
            phase=phase,
            checks=checks,
            passed=all(checks.values()),
            environment_error=False,
            baseline_checks_complete=baseline_complete if phase == "initial" else None,
            test_run=counts,
        )
        return result.model_dump(mode="json")
    except BenchmarkFailure as error:
        return _error_result(case_id, fingerprint, phase, error.code)
    except (
        OSError,
        UnicodeError,
        ValueError,
        TypeError,
        KeyError,
        SyntaxError,
        ImportError,
    ) as error:
        code = (
            "candidate_or_workspace_error"
            if isinstance(error, (SyntaxError, ImportError))
            else "benchmark_error"
        )
        return _error_result(case_id, fingerprint, phase, code)


def parse_benchmark_result(
    value: object,
    *,
    case_id: str,
    phase: Literal["initial", "final"],
) -> dict[str, Any]:
    """严格解析 verifier 进程输出并核对固定 case/fingerprint/check 集合。"""
    case = benchmark_case(case_id)
    fingerprint = _case_identity(case)
    try:
        result = BenchmarkResult.model_validate(value)
        expected = set(
            _INITIAL_CHECKS[case_id] if phase == "initial" else _FINAL_CHECKS[case_id]
        )
        if (
            result.schema_version != SCHEMA_VERSION
            or result.case_id != case_id
            or result.case_fingerprint != fingerprint
            or result.phase != phase
            or set(result.checks) != expected
        ):
            raise ValueError("benchmark_result_binding")
        if result.environment_error:
            if (
                result.passed
                or result.test_run is not None
                or result.reason is None
                or (phase == "initial" and result.baseline_checks_complete is not False)
                or (phase == "final" and result.baseline_checks_complete is not None)
            ):
                raise ValueError("benchmark_environment_error")
        else:
            if result.reason is not None or result.test_run is None:
                raise ValueError("benchmark_result_counts")
            counts = result.test_run
            if counts.discovered < 1 or counts.executed != counts.discovered:
                raise ValueError("benchmark_result_counts")
            if phase == "initial":
                if result.baseline_checks_complete is not True:
                    raise ValueError("benchmark_baseline_incomplete")
            elif result.baseline_checks_complete is not None:
                raise ValueError("benchmark_result_phase")
        return result.model_dump(mode="json")
    except (ValueError, TypeError):
        raise ValueError("benchmark_verifier_protocol") from None


def _load_initial(path: Path) -> dict[str, Any]:
    try:
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > MAX_TOTAL_BYTES:
            raise BenchmarkFailure("initial_manifest_unsafe")
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            live = os.fstat(fd)
            if (live.st_dev, live.st_ino, live.st_size, live.st_nlink) != (
                info.st_dev,
                info.st_ino,
                info.st_size,
                1,
            ):
                raise BenchmarkFailure("initial_manifest_changed")
            data = os.read(fd, info.st_size + 1)
            after = os.fstat(fd)
            if (
                len(data) != info.st_size
                or (after.st_mtime_ns, after.st_ctime_ns)
                != (info.st_mtime_ns, info.st_ctime_ns)
            ):
                raise BenchmarkFailure("initial_manifest_changed")
        finally:
            os.close(fd)
        parsed = _strict_json(data.decode("utf-8", "strict"))
        if not isinstance(parsed, dict):
            raise BenchmarkFailure("initial_manifest_shape")
        return parsed
    except BenchmarkFailure:
        raise
    except (OSError, UnicodeError, ValueError):
        raise BenchmarkFailure("initial_manifest_unsafe") from None


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fixed agent-bench/1 verifier")
    parser.add_argument("--case", required=True, choices=tuple(_CHECKERS))
    parser.add_argument("--workspace", required=True, type=Path)
    parser.add_argument("--initial", required=True, type=Path)
    parser.add_argument("--phase", required=True, choices=("initial", "final"))
    args = parser.parse_args(argv)
    try:
        initial = _load_initial(args.initial)
    except BenchmarkFailure as error:
        case = benchmark_case(args.case)
        result = _error_result(args.case, _case_identity(case), args.phase, error.code)
    else:
        result = verify_benchmark(args.case, args.workspace, initial, args.phase)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
