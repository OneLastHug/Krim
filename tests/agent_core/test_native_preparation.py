"""原生工具准备辅助的纯文件/命令构造回归，不安装或启动工具。"""

from pathlib import Path

import pytest

from scripts.prepare_native_agents import (
    NODE_VERSION,
    PreparationError,
    _distribution_root,
    _hermes_sync_command,
    _repair_node_layout,
    subprocess_env,
)


def test_distribution_root_accepts_flat_and_single_nested_layout(tmp_path):
    nested = tmp_path / "download" / "node-distribution"
    (nested / "bin").mkdir(parents=True)
    (nested / "bin/node").write_bytes(b"fixture-node")
    assert _distribution_root(tmp_path / "download", "bin/node") == nested
    assert _distribution_root(nested, "bin/node") == nested


def test_distribution_root_rejects_ambiguous_or_missing_entry(tmp_path):
    with pytest.raises(PreparationError):
        _distribution_root(tmp_path, "bin/node")
    for name in ("first", "second"):
        entry = tmp_path / name / "bin"
        entry.mkdir(parents=True)
        (entry / "node").write_bytes(b"fixture")
    with pytest.raises(PreparationError):
        _distribution_root(tmp_path, "bin/node")


def test_nested_node_layout_is_repaired_without_overwriting(tmp_path):
    root = tmp_path / "installed"
    nested = root / f"node-v{NODE_VERSION}-linux-x64"
    (nested / "bin").mkdir(parents=True)
    (nested / "bin/node").write_bytes(b"verified fixture")
    _repair_node_layout(root)
    assert (root / "bin/node").read_bytes() == b"verified fixture"
    assert not nested.exists()
    _repair_node_layout(root)
    assert (root / "bin/node").read_bytes() == b"verified fixture"


def test_node_layout_repair_does_not_touch_unrelated_material(tmp_path):
    root = tmp_path / "installed"
    (root / f"node-v{NODE_VERSION}-linux-x64/bin").mkdir(parents=True)
    sentinel = root / "unrelated.txt"
    sentinel.write_text("keep", encoding="utf-8")
    with pytest.raises(PreparationError):
        _repair_node_layout(root)
    assert sentinel.read_text(encoding="utf-8") == "keep"


def test_hermes_uses_upstream_frozen_lock_not_dependency_bypass():
    source = Path("/verified/hermes")
    command = _hermes_sync_command(Path("/tools/uv"), Path("/tools/python3.14"), source)
    assert command[:3] == ["/tools/uv", "sync", "--frozen"]
    assert command[command.index("--project") + 1] == str(source)
    assert "--no-default-groups" in command and "--no-dev" in command
    assert "--no-deps" not in command and "--extra" not in command
    checked = _hermes_sync_command(Path("/tools/uv"), Path("/tools/python3.14"), source, check=True)
    assert checked[-2:] == ["--check", "--offline"]


def test_install_environment_has_distinct_empty_npm_configs(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "synthetic-not-a-real-key")
    env = subprocess_env(tmp_path)
    assert "OPENAI_API_KEY" not in env
    assert env["NPM_CONFIG_USERCONFIG"] != env["NPM_CONFIG_GLOBALCONFIG"]
    for name in ("NPM_CONFIG_USERCONFIG", "NPM_CONFIG_GLOBALCONFIG"):
        assert Path(env[name]).read_bytes() == b""
    assert env["NPM_CONFIG_IGNORE_SCRIPTS"] == "true"


def test_install_environment_rejects_nonempty_npm_config(tmp_path):
    (tmp_path / "empty-npm-user.rc").write_text("unexpected=true", encoding="utf-8")
    with pytest.raises(PreparationError):
        subprocess_env(tmp_path)
