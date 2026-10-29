"""P6发行清单和安装验证器的纯本地回归。"""

from __future__ import annotations

import hashlib
import io
import json
import subprocess
import tarfile
import zipfile
from pathlib import Path

import pytest

from scripts.verify_distribution import (
    ROOT,
    DistributionError,
    _check_member,
    _metadata_version,
    _source_snapshot,
    _validate_member_path,
    _verify_entry_points,
    _verify_sdist,
    _verify_wheel,
    distribution_contract,
)


def test_sdist_whitelist_matches_wheel_sources_and_preserves_notices() -> None:
    contract = distribution_contract(ROOT)
    project = (ROOT / "pyproject.toml").read_text(encoding="utf-8")

    assert contract["name"] == "krim-agent"
    assert contract["version"] == "0.4.0a1"
    assert contract["wheel_payload_files"] == [
        path.removeprefix("src/") for path in contract["wheel_files"]
    ]
    assert contract["sdist_include"] == [
        "pyproject.toml",
        "README.md",
        ".gitignore",
        "src/krim_agent",
        "frontend/src",
        "frontend/scripts",
        "frontend/package.json",
        "frontend/package-lock.json",
        "frontend/index.html",
        "frontend/tsconfig.json",
        "frontend/vite.config.ts",
        "frontend/vitest.config.ts",
        "frontend/THIRD-PARTY-NOTICES.txt",
    ]
    assert contract["sdist_exclude"] == [
        "**/.cache/**",
        "**/.vite/**",
        "**/node_modules/**",
        "**/__pycache__/**",
        "**/*.pyc",
        "**/*.pyo",
        "**/*.pyd",
    ]
    sdist_files = set(contract["sdist_source_files"])
    assert {"pyproject.toml", "README.md", ".gitignore", *contract["wheel_files"]} <= sdist_files
    assert contract["sdist_source_hashes"][".gitignore"] == hashlib.sha256(
        (ROOT / ".gitignore").read_bytes()
    ).hexdigest()
    assert "frontend/scripts/build.mjs" in sdist_files
    assert "frontend/index.html" in sdist_files
    assert "frontend/tsconfig.json" in sdist_files
    assert "frontend/vite.config.ts" in sdist_files
    assert "frontend/vitest.config.ts" in sdist_files
    assert "frontend/THIRD-PARTY-NOTICES.txt" in sdist_files
    assert any(path.startswith("frontend/src/") for path in sdist_files)
    assert not any("node_modules" in path or ".cache" in path for path in sdist_files)
    assert not any(
        path.startswith(("frontend/contracts/", "frontend/e2e/")) for path in sdist_files
    )
    assert "src/krim_agent/web/THIRD-PARTY-NOTICES.txt" in contract["wheel_files"]
    assert "license =" not in project
    assert all((ROOT / name).is_file() for name in contract["wheel_files"])


def test_frontend_version_is_synced_without_lockfile_dependency_drift() -> None:
    package = json.loads((ROOT / "frontend/package.json").read_text(encoding="utf-8"))
    lock = json.loads((ROOT / "frontend/package-lock.json").read_text(encoding="utf-8"))
    lock_root = lock["packages"][""]

    assert package["version"] == lock["version"] == lock_root["version"] == "0.4.0-alpha.1"
    assert package["private"] is True
    assert lock_root["dependencies"] == package["dependencies"]
    assert lock_root["devDependencies"] == package["devDependencies"]


@pytest.mark.parametrize(
    "name",
    ["../outside", "/absolute/path", "src\\krim_agent\\__init__.py", "a/node_modules/pkg.js"],
)
def test_archive_paths_reject_traversal_and_development_material(name: str) -> None:
    with pytest.raises(DistributionError):
        _validate_member_path(name)


def test_release_metadata_does_not_invent_a_project_license() -> None:
    metadata = b"Name: krim-agent\nVersion: 0.4.0a1\nLicense-Expression: MIT\n\n"

    with pytest.raises(DistributionError, match="自有许可"):
        _metadata_version(metadata, "krim-agent", "0.4.0a1", "wheel")


def test_release_metadata_normalizes_project_dependencies() -> None:
    metadata = (
        b"Name: krim-agent\nVersion: 0.4.0a1\n"
        b"Requires-Dist: FASTAPI < 1, >= 0.115\n\n"
    )

    _metadata_version(
        metadata,
        "krim-agent",
        "0.4.0a1",
        "wheel",
        ["fastapi>=0.115, <1"],
    )


def test_release_metadata_rejects_unapproved_dependency() -> None:
    metadata = (
        b"Name: krim-agent\nVersion: 0.4.0a1\n"
        b"Requires-Dist: fastapi>=0.115,<1\n"
        b"Requires-Dist: injected-package>=1\n\n"
    )

    with pytest.raises(DistributionError, match="Requires-Dist与pyproject.toml依赖不一致"):
        _metadata_version(
            metadata,
            "krim-agent",
            "0.4.0a1",
            "wheel",
            ["fastapi>=0.115,<1"],
        )


@pytest.mark.parametrize(
    "extra",
    [
        "backup = krim_agent.cli:main",
        "[gui_scripts]\nkrim-gui = krim_agent.cli:main",
    ],
)
def test_entry_points_reject_extra_commands_and_groups(extra: str) -> None:
    entry_points = (
        "[console_scripts]\n"
        "krim = krim_agent.cli:main\n"
        "krim-eval = krim_agent.evaluation.cli:main\n"
        f"{extra}\n"
    ).encode()

    with pytest.raises(DistributionError, match="entry_points.txt与pyproject.toml入口不一致"):
        _verify_entry_points(entry_points)


def test_source_snapshot_marks_dirty_git_worktree(monkeypatch, tmp_path: Path) -> None:
    def fake_run(command, **_kwargs):
        if command[1:3] == ["rev-parse", "HEAD"]:
            return subprocess.CompletedProcess(command, 0, b"a" * 40 + b"\n", b"")
        if command[1:2] == ["status"]:
            return subprocess.CompletedProcess(command, 0, b" M src/krim_agent/module.py\n", b"")
        raise AssertionError(command)

    monkeypatch.setattr(subprocess, "run", fake_run)
    snapshot = _source_snapshot(
        tmp_path,
        {"sdist_source_hashes": {"src/krim_agent/module.py": "abc"}},
    )

    assert snapshot["git_head"] == "a" * 40
    assert snapshot["git_worktree_dirty"] is True


@pytest.mark.parametrize(
    "secret",
    [
        b"-----BEGIN PRIVATE KEY-----\nfake\n",
        b"sk-proj-abcdefghijklmnopqrstuvwxyz123456-AB_cd",
    ],
)
def test_secret_scanner_rejects_private_keys_and_openai_project_keys(secret: bytes) -> None:
    with pytest.raises(DistributionError, match="秘密材料"):
        _check_member("src/krim_agent/module.py", secret)


def test_archive_path_scanner_accepts_current_package_notice() -> None:
    notice = ROOT / "src/krim_agent/web/THIRD-PARTY-NOTICES.txt"
    _check_member("src/krim_agent/web/THIRD-PARTY-NOTICES.txt", notice.read_bytes())


def test_wheel_verification_records_file_and_archive_hashes(tmp_path: Path) -> None:
    wheel = tmp_path / "krim_agent-0.4.0a1-py3-none-any.whl"
    info = "krim_agent-0.4.0a1.dist-info"
    members = {
        "krim_agent/__init__.py": b"__version__ = '0.4.0a1'\n",
        f"{info}/METADATA": (
            b"Name: krim-agent\nVersion: 0.4.0a1\n"
            b"Requires-Dist: FASTAPI <1, >=0.115\n\n"
        ),
        f"{info}/WHEEL": b"Wheel-Version: 1.0\nTag: py3-none-any\n",
        f"{info}/RECORD": b"krim_agent/__init__.py,,\n",
        f"{info}/entry_points.txt": (
            b"[console_scripts]\nkrim = krim_agent.cli:main\n"
            b"krim-eval = krim_agent.evaluation.cli:main\n"
        ),
    }
    with zipfile.ZipFile(wheel, "w") as archive:
        for name, data in members.items():
            archive.writestr(name, data)

    record = _verify_wheel(
        wheel,
        {
            "wheel_files": ["src/krim_agent/__init__.py"],
            "wheel_payload_files": ["krim_agent/__init__.py"],
            "name": "krim-agent",
            "version": "0.4.0a1",
            "project_dependencies": ["fastapi>=0.115,<1"],
            "sdist_source_hashes": {
                "src/krim_agent/__init__.py": hashlib.sha256(
                    b"__version__ = '0.4.0a1'\n"
                ).hexdigest()
            },
        },
    )

    assert record["product_file_count"] == 1
    assert record["sha256"]
    assert {item["path"] for item in record["files"]} == set(members)


def test_sdist_verification_rejects_extra_payload_and_records_hashes(tmp_path: Path) -> None:
    sdist = tmp_path / "krim_agent-0.4.0a1.tar.gz"
    archive_root = "krim_agent-0.4.0a1"
    gitignore = b"__pycache__/\nnode_modules/\n"
    contract = {
        "sdist_source_files": [
            ".gitignore",
            "README.md",
            "pyproject.toml",
            "src/krim_agent/__init__.py",
        ],
        "sdist_source_hashes": {".gitignore": hashlib.sha256(gitignore).hexdigest()},
        "name": "krim-agent",
        "version": "0.4.0a1",
        "project_dependencies": [],
    }
    members = {
        "PKG-INFO": b"Name: krim-agent\nVersion: 0.4.0a1\n\n",
        ".gitignore": gitignore,
        "README.md": b"Package summary\n",
        "pyproject.toml": b"[project]\nname = 'krim-agent'\n",
        "src/krim_agent/__init__.py": b"__version__ = '0.4.0a1'\n",
    }
    with tarfile.open(sdist, "w:gz") as archive:
        for name, data in members.items():
            info = tarfile.TarInfo(f"{archive_root}/{name}")
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))

    record = _verify_sdist(sdist, contract)
    assert record["source_file_count"] == 4
    assert record["sha256"]
    assert len(record["files"]) == len(members)

    tampered_sdist = tmp_path / "tampered" / "krim_agent-0.4.0a1.tar.gz"
    tampered_sdist.parent.mkdir()
    tampered_members = members | {".gitignore": gitignore + b"tampered\\n"}
    with tarfile.open(tampered_sdist, "w:gz") as archive:
        for name, data in tampered_members.items():
            info = tarfile.TarInfo(f"{archive_root}/{name}")
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    with pytest.raises(DistributionError, match="源码字节与来源快照不匹配"):
        _verify_sdist(tampered_sdist, contract)

    bad_sdist = tmp_path / "bad" / "krim_agent-0.4.0a1.tar.gz"
    bad_sdist.parent.mkdir()
    with tarfile.open(bad_sdist, "w:gz") as archive:
        for name, data in members.items():
            info = tarfile.TarInfo(f"{archive_root}/{name}")
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
        extra = b"old docs must not ship"
        info = tarfile.TarInfo(f"{archive_root}/docs/history.md")
        info.size = len(extra)
        archive.addfile(info, io.BytesIO(extra))
    with pytest.raises(DistributionError, match="禁止目录"):
        _verify_sdist(bad_sdist, contract)
