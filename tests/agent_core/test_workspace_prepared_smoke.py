"""Retirement boundary for the target-workspace prepared smoke path."""

import ast
from pathlib import Path

from scripts import prepare_dev_sandbox


def test_rootfs_qualification_remains_without_legacy_workspace_profile() -> None:
    source = Path(prepare_dev_sandbox.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions = {
        node.name for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    imports = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    } | {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    string_constants = {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }

    assert {"build_rootfs", "verify_manifest", "qualify_and_hold", "hold_service"} <= functions
    assert "krim_agent.workspace.targets" not in imports
    assert "workspace-profile.json" not in string_constants
    assert "workspace_profile" not in string_constants
    assert "BASE-QUALIFIED" in string_constants
