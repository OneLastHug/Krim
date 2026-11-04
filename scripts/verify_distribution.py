#!/usr/bin/env python3
"""核对Krim Agent wheel/sdist清单，并在项目外离线验证安装包。"""

from __future__ import annotations

import argparse
import configparser
import email.parser
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import traceback
import uuid
import zipfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from krim_agent.font_assets import (  # noqa: E402
    FONT_MANIFEST_FILENAME,
    FontAsset,
    FontManifestError,
    load_font_assets,
)

EXPECTED_PYTHON_VERSION = "0.4.0a1"
FONT_MANIFEST_SOURCE = f"src/krim_agent/web/{FONT_MANIFEST_FILENAME}"
FONT_DIRECTORY_SOURCE = "src/krim_agent/web/assets/fonts"
EXPECTED_SDIST_INCLUDE = [
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
EXPECTED_SDIST_EXCLUDE = [
    "**/.cache/**",
    "**/.vite/**",
    "**/node_modules/**",
    "**/__pycache__/**",
    "**/*.pyc",
    "**/*.pyo",
    "**/*.pyd",
]
FORBIDDEN_SEGMENTS = {
    ".cache",
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    ".vite",
    "__pycache__",
    "build",
    "dist",
    "docs",
    "node_modules",
    "rootfs",
    "site-packages",
    "tests",
    "venv",
}
FORBIDDEN_NAMES = {
    ".env",
    ".env.local",
    ".env.production",
    "credentials.json",
    "id_rsa",
    "id_ed25519",
}
SECRET_PATTERNS = (
    re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----"),
    re.compile(rb"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(rb"\b(?:gh[pousr]|github_pat)_[A-Za-z0-9_]{20,}\b"),
    re.compile(rb"\bsk-ant-[A-Za-z0-9_-]{24,}\b"),
    re.compile(rb"\bsk-proj-[A-Za-z0-9_-]{20,}\b"),
    re.compile(rb"https?://[^/\s:@]+:[^/\s@]+@"),
)


class DistributionError(RuntimeError):
    """候选发行包违反静态清单或安装验证条件。"""


def _normalize_requirement(
    value: str,
) -> tuple[str, tuple[str, ...], str, str, str]:
    """按PEP 508/440语义标准化依赖名、extras、URL、specifier和marker。"""
    try:
        requirement = Requirement(value)
    except InvalidRequirement as error:
        raise DistributionError(f"无法解析依赖声明：{value!r}：{error}") from error
    return (
        canonicalize_name(requirement.name),
        tuple(sorted(canonicalize_name(extra) for extra in requirement.extras)),
        requirement.url or "",
        str(requirement.specifier),
        str(requirement.marker) if requirement.marker is not None else "",
    )


def _requirement_for_extra(value: str, extra: str) -> str:
    """为可选依赖附加其PEP 508 extra标记。"""
    try:
        requirement = Requirement(value)
    except InvalidRequirement as error:
        raise DistributionError(f"无法解析可选依赖声明：{value!r}：{error}") from error
    extras = f"[{','.join(sorted(requirement.extras))}]" if requirement.extras else ""
    base = f"{requirement.name}{extras}"
    if requirement.url:
        base += f" @ {requirement.url}"
    else:
        base += str(requirement.specifier)
    marker = f"extra == {json.dumps(extra)}"
    if requirement.marker is not None:
        marker = f"({requirement.marker}) and ({marker})"
    return f"{base}; {marker}"


def _normalize_requirements(
    values: list[str], label: str
) -> tuple[tuple[str, tuple[str, ...], str, str, str], ...]:
    try:
        return tuple(sorted(_normalize_requirement(value) for value in values))
    except DistributionError as error:
        raise DistributionError(f"{label}依赖声明无效：{error}") from error


def _json_write(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _npm_alpha_version(python_version: str) -> str:
    match = re.fullmatch(r"(\d+(?:\.\d+)*)a(\d+)", python_version)
    if match is None:
        raise DistributionError(f"当前版本不是受支持的Python alpha版本：{python_version}")
    return f"{match.group(1)}-alpha.{int(match.group(2))}"


def _expand_wheel_files(
    root: Path,
    wheel_paths: list[str],
    font_assets: tuple[FontAsset, ...],
) -> list[str]:
    """只展开已校验字体清单明确列出的受管理目录内容。"""
    files = [
        item for item in wheel_paths
        if item not in {FONT_DIRECTORY_SOURCE, FONT_MANIFEST_SOURCE}
        and (root / item).is_file()
    ]
    if (root / FONT_MANIFEST_SOURCE).is_file():
        files.append(FONT_MANIFEST_SOURCE)
    files.extend(f"src/krim_agent/web/{asset.path}" for asset in font_assets)
    return sorted(files)


def distribution_contract(root: Path = ROOT) -> dict[str, Any]:
    """加载并校验静态发行清单；sdist源码与wheel保持一致。"""
    root = root.resolve()
    try:
        project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
        package = json.loads((root / "frontend/package.json").read_text(encoding="utf-8"))
        lock = json.loads((root / "frontend/package-lock.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, tomllib.TOMLDecodeError) as error:
        raise DistributionError(f"读取发行元数据失败：{error}") from error

    project_meta = project.get("project", {})
    name = project_meta.get("name")
    version = project_meta.get("version")
    project_dependencies = project_meta.get("dependencies", [])
    optional_dependencies = project_meta.get("optional-dependencies", {})
    build_config = project.get("tool", {}).get("hatch", {}).get("build", {})
    targets = build_config.get("targets", {})
    wheel_config = targets.get("wheel", {})
    sdist_config = targets.get("sdist", {})
    wheel_paths = wheel_config.get("only-include")
    wheel_sources = wheel_config.get("sources")
    sdist_include = sdist_config.get("only-include")
    sdist_exclude = sdist_config.get("exclude")
    errors: list[str] = []
    metadata_dependencies: list[str] = []

    if not isinstance(project_dependencies, list) or not all(
        isinstance(item, str) for item in project_dependencies
    ):
        errors.append("project.dependencies必须是PEP 508依赖字符串列表")
    else:
        metadata_dependencies.extend(project_dependencies)
        try:
            _normalize_requirements(project_dependencies, "pyproject.toml")
        except DistributionError as error:
            errors.append(str(error))

    if not isinstance(optional_dependencies, dict):
        errors.append("project.optional-dependencies必须是extra到PEP 508依赖列表的映射")
    else:
        for extra, requirements in optional_dependencies.items():
            if not isinstance(extra, str) or re.fullmatch(
                r"[A-Za-z0-9]+(?:[-_.][A-Za-z0-9]+)*", extra
            ) is None:
                errors.append(f"可选依赖extra名称无效：{extra!r}")
                continue
            if not isinstance(requirements, list) or not all(
                isinstance(item, str) for item in requirements
            ):
                errors.append(f"project.optional-dependencies[{extra!r}]必须是PEP 508依赖列表")
                continue
            for requirement in requirements:
                try:
                    marked = _requirement_for_extra(requirement, extra)
                    _normalize_requirements([marked], f"pyproject.optional-dependencies[{extra}]")
                except DistributionError as error:
                    errors.append(str(error))
                else:
                    metadata_dependencies.append(marked)

    if name != "krim-agent":
        errors.append(f"Python发行名应为krim-agent，实际为{name!r}")
    if version != EXPECTED_PYTHON_VERSION:
        errors.append(f"Python版本应保持{EXPECTED_PYTHON_VERSION}，实际为{version!r}")
    if "license" in project_meta:
        errors.append("项目尚未声明自有许可；不得在发行配置中推定或新增license")
    font_manifest_path = root / FONT_MANIFEST_SOURCE
    font_manifest_configured = (
        FONT_MANIFEST_SOURCE in wheel_paths if isinstance(wheel_paths, list) else False
    )
    font_directory_configured = (
        FONT_DIRECTORY_SOURCE in wheel_paths if isinstance(wheel_paths, list) else False
    )
    try:
        font_assets = load_font_assets(root / "src/krim_agent/web")
    except FontManifestError as error:
        errors.append(f"正式字体资产清单无效：{error}")
        font_assets = ()

    if not isinstance(wheel_paths, list) or not all(isinstance(item, str) for item in wheel_paths):
        errors.append("wheel only-include必须是显式路径白名单")
        wheel_paths = []
    if len(wheel_paths) != len(set(wheel_paths)):
        errors.append("wheel only-include存在重复路径")
    if not font_manifest_configured or not font_directory_configured:
        errors.append("wheel必须显式列出字体清单文件及唯一受管理字体目录")
    if sdist_include != EXPECTED_SDIST_INCLUDE:
        errors.append(f"sdist白名单应为{EXPECTED_SDIST_INCLUDE!r}，实际为{sdist_include!r}")
    if sdist_exclude != EXPECTED_SDIST_EXCLUDE:
        errors.append(f"sdist缓存排除项应为{EXPECTED_SDIST_EXCLUDE!r}，实际为{sdist_exclude!r}")

    sdist_files: set[str] = set()
    for item in EXPECTED_SDIST_INCLUDE:
        target = root / item
        if not target.exists():
            errors.append(f"sdist白名单路径不存在：{item}")
            continue
        if target.is_symlink():
            errors.append(f"sdist白名单不能指向符号链接：{item}")
            continue
        candidates = target.rglob("*") if target.is_dir() else [target]
        for candidate in candidates:
            if candidate.is_symlink():
                errors.append(f"sdist源码目录不能包含符号链接：{candidate.relative_to(root)}")
                continue
            if not candidate.is_file():
                continue
            relative = candidate.relative_to(root).as_posix()
            path_parts = PurePosixPath(relative).parts
            if "__pycache__" in path_parts or candidate.suffix in {".pyc", ".pyo", ".pyd"}:
                continue
            try:
                _validate_member_path(relative)
            except DistributionError as error:
                errors.append(f"sdist路径包含禁止内容：{relative}：{error}")
            else:
                sdist_files.add(relative)

    for item in wheel_paths:
        try:
            _validate_member_path(item)
        except DistributionError as error:
            errors.append(f"wheel清单路径无效：{item!r}：{error}")
            continue
        target = root / item
        if item == FONT_DIRECTORY_SOURCE:
            if target.exists() or target.is_symlink():
                if target.is_symlink() or not target.is_dir():
                    errors.append(f"wheel受管理字体路径必须是实际目录：{item}")
            elif font_assets:
                errors.append(f"字体清单存在时受管理字体目录不得缺失：{item}")
        elif item == FONT_MANIFEST_SOURCE:
            if target.exists() or target.is_symlink():
                if target.is_symlink() or not target.is_file():
                    errors.append(f"wheel字体清单必须是普通文件：{item}")
            elif font_manifest_path.exists() or font_manifest_path.is_symlink() or font_assets:
                errors.append(f"wheel字体清单文件不存在：{item}")
        elif not target.is_file():
            errors.append(f"wheel清单文件不存在：{item}")

    if wheel_sources != ["src"]:
        errors.append(f"wheel sources应保持现有src映射，实际为{wheel_sources!r}")
    if len(wheel_paths) != len([item for item in wheel_paths if item.startswith("src/")]):
        errors.append("wheel白名单中存在src/之外的路径，无法按现有sources映射重建")

    wheel_expanded_paths = _expand_wheel_files(root, wheel_paths, font_assets)
    if len(wheel_expanded_paths) != len(set(wheel_expanded_paths)):
        errors.append("展开wheel字体清单后出现重复成员")
    wheel_payload_paths = [item.removeprefix("src/") for item in wheel_expanded_paths]
    if len(wheel_payload_paths) != len(wheel_expanded_paths):
        errors.append("wheel白名单展开后存在src/之外的路径，无法按现有sources映射重建")

    package_root = root / "src/krim_agent"
    package_files = {
        path.relative_to(root).as_posix()
        for path in package_root.rglob("*")
        if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc"
    }
    listed_files = set(wheel_expanded_paths)
    if package_files != listed_files:
        missing = sorted(package_files - listed_files)
        absent = sorted(listed_files - package_files)
        if missing:
            errors.append(f"运行时目录含未列入wheel/sdist的文件：{missing}")
        if absent:
            errors.append(f"wheel路径不在src/krim_agent内：{absent}")
    if not listed_files <= sdist_files:
        errors.append(f"sdist未包含wheel重建所需源码：{sorted(listed_files - sdist_files)}")
    required_frontend = {
        "frontend/scripts/build.mjs",
        "frontend/package.json",
        "frontend/package-lock.json",
        "frontend/index.html",
        "frontend/tsconfig.json",
        "frontend/vite.config.ts",
        "frontend/vitest.config.ts",
        "frontend/THIRD-PARTY-NOTICES.txt",
    }
    missing_frontend = sorted(required_frontend - sdist_files)
    if missing_frontend or not any(path.startswith("frontend/src/") for path in sdist_files):
        errors.append(f"sdist缺少前端重建材料：{missing_frontend or ['frontend/src']}")

    expected_frontend = _npm_alpha_version(version) if isinstance(version, str) else ""
    lock_root = lock.get("packages", {}).get("", {})
    if package.get("version") != expected_frontend:
        errors.append(
            f"前端私有包版本应与Python alpha版本对应为{expected_frontend}，"
            f"实际为{package.get('version')!r}"
        )
    if package.get("private") is not True:
        errors.append("前端包必须继续标记private，避免被误发布到npm")
    if lock.get("version") != expected_frontend or lock_root.get("version") != expected_frontend:
        errors.append("package-lock顶层与packages[\"\"]版本未和package.json同步")
    if lock_root.get("dependencies") != package.get("dependencies"):
        errors.append("package-lock根dependencies与package.json不一致")
    if lock_root.get("devDependencies") != package.get("devDependencies"):
        errors.append("package-lock根devDependencies与package.json不一致")

    notices = "src/krim_agent/web/THIRD-PARTY-NOTICES.txt"
    frontend_notices = "frontend/THIRD-PARTY-NOTICES.txt"
    if notices not in listed_files:
        errors.append("wheel必须包含Python UI使用的第三方许可告知文件")
    if notices not in sdist_files or frontend_notices not in sdist_files:
        errors.append("sdist必须包含Python UI与前端源代码的第三方许可告知文件")
    if errors:
        raise DistributionError("发行清单预检失败：\n- " + "\n- ".join(errors))

    return {
        "name": name,
        "version": version,
        "project_dependencies": metadata_dependencies,
        "frontend_version": expected_frontend,
        "wheel_files": sorted(wheel_expanded_paths),
        "wheel_include_paths": sorted(wheel_paths),
        "wheel_payload_files": sorted(wheel_payload_paths),
        "font_assets": [
            {"path": asset.path, "size_bytes": asset.size_bytes, "sha256": asset.sha256}
            for asset in font_assets
        ],
        "sdist_source_files": sorted(sdist_files),
        "sdist_source_hashes": {
            name: _sha256((root / name).read_bytes()) for name in sorted(sdist_files)
        },
        "sdist_include": sdist_include,
        "sdist_exclude": sdist_exclude,
        "python_file_count": sum(path.endswith(".py") for path in wheel_expanded_paths),
        "third_party_notice": notices,
    }


def _source_snapshot(root: Path, contract: dict[str, Any]) -> dict[str, Any]:
    source_hashes = contract["sdist_source_hashes"]
    encoded = json.dumps(source_hashes, sort_keys=True, separators=(",", ":")).encode("utf-8")
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            env=_minimal_env(),
            capture_output=True,
            check=False,
        )
    except OSError:
        result = None
    head = (
        result.stdout.decode("ascii", errors="replace").strip()
        if result is not None and result.returncode == 0
        else None
    )
    try:
        status = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=all"],
            cwd=root,
            env=_minimal_env(),
            capture_output=True,
            check=False,
        )
    except OSError:
        status = None
    worktree_dirty = (
        bool(status.stdout.strip()) if status is not None and status.returncode == 0 else None
    )
    return {
        "git_head": head,
        "git_worktree_dirty": worktree_dirty,
        "file_count": len(source_hashes),
        "files_sha256": _sha256(encoded),
        "files": source_hashes,
    }


def _validate_member_path(name: str) -> str:
    if not name or "\\" in name:
        raise DistributionError("归档路径必须是非空POSIX相对路径")
    normalized = name[:-1] if name.endswith("/") else name
    parts = normalized.split("/")
    if not normalized or any(part in {"", ".", ".."} for part in parts):
        raise DistributionError(f"归档路径包含绝对或越界部分：{name!r}")
    path = PurePosixPath(normalized)
    if path.is_absolute():
        raise DistributionError(f"归档路径包含绝对或越界部分：{name!r}")
    parts_lower = {part.casefold() for part in path.parts}
    if parts_lower & FORBIDDEN_SEGMENTS:
        raise DistributionError(f"归档路径包含禁止目录：{name!r}")
    if path.name.casefold() in FORBIDDEN_NAMES:
        raise DistributionError(f"归档路径包含禁止秘密文件名：{name!r}")
    return name


def _check_member(name: str, data: bytes) -> None:
    _validate_member_path(name)
    for pattern in SECRET_PATTERNS:
        if pattern.search(data):
            raise DistributionError(f"归档文件疑似含有秘密材料：{name}")


def _read_wheel(path: Path) -> dict[str, bytes]:
    try:
        with zipfile.ZipFile(path) as archive:
            contents: dict[str, bytes] = {}
            for info in archive.infolist():
                name = _validate_member_path(info.filename)
                if info.is_dir():
                    continue
                if name in contents:
                    raise DistributionError(f"wheel包含重复归档路径：{name}")
                data = archive.read(info)
                _check_member(name, data)
                contents[name] = data
            return contents
    except (OSError, zipfile.BadZipFile) as error:
        raise DistributionError(f"wheel无法读取：{path}：{error}") from error


def _read_sdist(path: Path) -> dict[str, bytes]:
    try:
        with tarfile.open(path, mode="r:gz") as archive:
            contents: dict[str, bytes] = {}
            for member in archive.getmembers():
                name = _validate_member_path(member.name)
                if member.isdir():
                    continue
                if not member.isfile():
                    raise DistributionError(f"sdist不允许链接或特殊文件：{member.name}")
                stream = archive.extractfile(member)
                if stream is None:
                    raise DistributionError(f"无法读取sdist成员：{name}")
                data = stream.read()
                _check_member(name, data)
                if name in contents:
                    raise DistributionError(f"sdist包含重复归档路径：{name}")
                contents[name] = data
            return contents
    except (OSError, tarfile.TarError) as error:
        raise DistributionError(f"sdist无法读取：{path}：{error}") from error


def _record_files(contents: dict[str, bytes]) -> list[dict[str, Any]]:
    return [
        {"path": name, "size_bytes": len(data), "sha256": _sha256(data)}
        for name, data in sorted(contents.items())
    ]


def _metadata_version(
    data: bytes,
    expected_name: str,
    expected_version: str,
    label: str,
    expected_dependencies: list[str] | None = None,
) -> None:
    metadata = email.parser.BytesParser().parsebytes(data)
    name = str(metadata.get("Name", "")).lower().replace("_", "-")
    version = metadata.get("Version")
    if name != expected_name or version != expected_version:
        raise DistributionError(
            f"{label}版本身份不符：Name={name!r} Version={version!r}"
        )
    license_fields = (metadata.get("License"), metadata.get("License-Expression"))
    if any(value and value.strip() for value in license_fields):
        raise DistributionError(f"{label}元数据不应擅自声明项目自有许可")
    if expected_dependencies is not None:
        actual_dependencies = metadata.get_all("Requires-Dist", [])
        expected_normalized = _normalize_requirements(expected_dependencies, "pyproject.toml")
        actual_normalized = _normalize_requirements(actual_dependencies, f"{label} METADATA")
        if actual_normalized != expected_normalized:
            raise DistributionError(
                f"{label} Requires-Dist与pyproject.toml依赖不一致："
                f"expected={expected_normalized!r} actual={actual_normalized!r}"
            )


class _CaseSensitiveConfigParser(configparser.ConfigParser):
    def optionxform(self, optionstr: str) -> str:
        return optionstr


def _verify_entry_points(data: bytes) -> None:
    parser = _CaseSensitiveConfigParser(
        interpolation=None,
        strict=True,
        delimiters=("=",),
    )
    try:
        parser.read_string(data.decode("utf-8"))
    except (configparser.Error, UnicodeDecodeError) as error:
        raise DistributionError(f"wheel entry_points.txt格式无效：{error}") from error
    if parser.defaults():
        raise DistributionError("wheel entry_points.txt不允许DEFAULT入口配置")
    actual = {
        section: dict(parser.items(section, raw=True))
        for section in parser.sections()
    }
    expected = {
        "console_scripts": {
            "krim": "krim_agent.cli:main",
            "krim-eval": "krim_agent.evaluation.cli:main",
        }
    }
    if actual != expected:
        raise DistributionError(
            f"wheel entry_points.txt与pyproject.toml入口不一致："
            f"expected={expected!r} actual={actual!r}"
        )


def _artifact_stem(contract: dict[str, Any]) -> str:
    normalized_name = re.sub(r"[-_.]+", "_", contract["name"])
    return f"{normalized_name}-{contract['version']}"


def _verify_wheel(path: Path, contract: dict[str, Any]) -> dict[str, Any]:
    if not path.name.startswith(_artifact_stem(contract) + "-") or not path.name.endswith(".whl"):
        raise DistributionError(f"wheel文件名与发行身份不符：{path.name}")
    contents = _read_wheel(path)
    payload = {name for name in contents if not name.split("/", 1)[0].endswith(".dist-info")}
    expected = set(contract["wheel_payload_files"])
    if payload != expected:
        raise DistributionError(
            f"wheel产品文件与白名单不一致：多出{sorted(payload - expected)}，"
            f"缺少{sorted(expected - payload)}"
        )
    source_hashes = contract.get("sdist_source_hashes", {})
    for source_path in contract["wheel_files"]:
        payload_path = source_path.removeprefix("src/")
        if _sha256(contents[payload_path]) != source_hashes.get(source_path):
            raise DistributionError(f"wheel源码字节与来源快照不一致：{source_path}")

    dist_info_dirs = {
        name.split("/", 1)[0]
        for name in contents
        if name.split("/", 1)[0].endswith(".dist-info")
    }
    if len(dist_info_dirs) != 1:
        raise DistributionError(f"wheel应且仅应包含一个dist-info目录：{sorted(dist_info_dirs)}")
    dist_info = next(iter(dist_info_dirs))
    metadata_name = f"{dist_info}/METADATA"
    wheel_name = f"{dist_info}/WHEEL"
    entry_points_name = f"{dist_info}/entry_points.txt"
    for required in (metadata_name, wheel_name, f"{dist_info}/RECORD", entry_points_name):
        if required not in contents:
            raise DistributionError(f"wheel缺少必要元数据：{required}")
    _metadata_version(
        contents[metadata_name],
        contract["name"],
        contract["version"],
        "wheel",
        contract["project_dependencies"],
    )
    _verify_entry_points(contents[entry_points_name])
    return {
        "filename": path.name,
        "sha256": _sha256(path.read_bytes()),
        "files": _record_files(contents),
        "product_file_count": len(payload),
        "dist_info": dist_info,
    }


def _verify_sdist(path: Path, contract: dict[str, Any]) -> dict[str, Any]:
    expected_stem = _artifact_stem(contract)
    if path.name not in {f"{expected_stem}.tar.gz", f"{expected_stem}.tgz"}:
        raise DistributionError(f"sdist文件名与发行身份不符：{path.name}")
    contents = _read_sdist(path)
    roots = {name.split("/", 1)[0] for name in contents}
    if len(roots) != 1 or any("/" not in name for name in contents):
        raise DistributionError(f"sdist必须将文件放在唯一顶层目录：{sorted(roots)}")
    root_name = next(iter(roots))
    if root_name != expected_stem:
        raise DistributionError(f"sdist归档顶层目录与发行身份不符：{root_name}")
    relative = {name[len(root_name) + 1 :]: data for name, data in contents.items()}
    expected = set(contract["sdist_source_files"]) | {"PKG-INFO"}
    if set(relative) != expected:
        raise DistributionError(
            f"sdist内容与可重建白名单不一致：多出{sorted(set(relative) - expected)}，"
            f"缺少{sorted(expected - set(relative))}"
        )
    for name, expected_hash in contract.get("sdist_source_hashes", {}).items():
        if name not in relative or _sha256(relative[name]) != expected_hash:
            raise DistributionError(f"sdist源码字节与来源快照不匹配：{name}")
    _metadata_version(
        relative["PKG-INFO"],
        contract["name"],
        contract["version"],
        "sdist",
        contract["project_dependencies"],
    )
    return {
        "filename": path.name,
        "sha256": _sha256(path.read_bytes()),
        "files": _record_files(contents),
        "source_file_count": len(contents) - 1,
        "archive_root": root_name,
    }


def _minimal_env() -> dict[str, str]:
    allowed = {
        "HOME",
        "LANG",
        "LC_ALL",
        "LD_LIBRARY_PATH",
        "PATH",
        "SYSTEMROOT",
        "TEMP",
        "TMP",
        "TMPDIR",
        "USERPROFILE",
        "UV_CACHE_DIR",
        "XDG_CACHE_HOME",
        "WINDIR",
    }
    env = {key: value for key, value in os.environ.items() if key in allowed}
    env.update(
        {
            "PIP_NO_INDEX": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "UV_OFFLINE": "1",
        }
    )
    return env


def _run_step(
    name: str,
    command: list[str],
    cwd: Path,
    evidence: Path,
    env: dict[str, str],
) -> None:
    started = datetime.now(UTC).isoformat()
    try:
        result = subprocess.run(command, cwd=cwd, env=env, capture_output=True, check=False)
        stdout, stderr, returncode = result.stdout, result.stderr, result.returncode
    except OSError as error:
        stdout, stderr, returncode = b"", str(error).encode("utf-8", errors="replace"), 127
    (evidence / f"{name}.stdout.log").write_bytes(stdout)
    (evidence / f"{name}.stderr.log").write_bytes(stderr)
    _json_write(
        evidence / f"{name}.json",
        {
            "argv": command,
            "cwd": str(cwd),
            "started_at": started,
            "finished_at": datetime.now(UTC).isoformat(),
            "exit_code": returncode,
        },
    )
    if returncode:
        detail = stderr.decode("utf-8", errors="replace")[-4000:]
        raise DistributionError(
            f"{name}失败，exit={returncode}；原始日志保存在{evidence}。\n{detail}"
        )


def _find_uv(configured: str | None) -> str:
    if configured:
        candidate = Path(configured)
        if candidate.is_file():
            return str(candidate.resolve())
        resolved = shutil.which(configured)
        if resolved:
            return resolved
        raise DistributionError(f"找不到指定uv可执行文件：{configured}")
    found = shutil.which("uv")
    if found:
        return found
    fallback = Path("/home/krim/.venvs/krim-tools/bin/uv")
    if fallback.is_file():
        return str(fallback)
    raise DistributionError("找不到uv；请通过--uv指定项目已有的uv，不会安装新工具")


def _artifact_pair(directory: Path) -> tuple[Path, Path]:
    wheels = sorted(directory.glob("*.whl"))
    sdists = sorted([*directory.glob("*.tar.gz"), *directory.glob("*.tgz")])
    if len(wheels) != 1 or len(sdists) != 1:
        raise DistributionError(
            "候选目录必须恰含一个wheel和一个sdist；"
            f"当前wheel={len(wheels)} sdist={len(sdists)}：{directory}"
        )
    return wheels[0].resolve(), sdists[0].resolve()


def _new_external_venv(root: Path) -> tuple[Path, Path]:
    base = Path(tempfile.mkdtemp(prefix="krim-agent-distribution-"))
    resolved_root = root.resolve()
    if base.resolve().is_relative_to(resolved_root):
        shutil.rmtree(base, ignore_errors=True)
        raise DistributionError("系统临时目录落在项目内，不能满足项目外安装验证")
    venv = base / "venv"
    result = subprocess.run(
        [sys.executable, "-I", "-B", "-m", "venv", str(venv)],
        cwd=base,
        env=_minimal_env(),
        capture_output=True,
        check=False,
    )
    (base / "venv-create.stdout.log").write_bytes(result.stdout)
    (base / "venv-create.stderr.log").write_bytes(result.stderr)
    if result.returncode:
        raise DistributionError(
            f"项目外venv创建失败，exit={result.returncode}；证据在{base}"
        )
    return base, venv


def _smoke_installed(
    wheel: Path,
    root: Path,
    evidence: Path,
    uv: str,
    on_venv_created: Callable[[Path], None] | None = None,
) -> tuple[Path, bool]:
    external, venv = _new_external_venv(root)
    if on_venv_created is not None:
        on_venv_created(external)
    python = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    env = _minimal_env()
    env["UV_PROJECT_ENVIRONMENT"] = str(venv)
    _run_step(
        "locked-runtime-dependencies",
        [
            uv,
            "sync",
            "--frozen",
            "--offline",
            "--project",
            str(root),
            "--no-install-project",
            "--no-dev",
        ],
        root,
        evidence,
        env,
    )
    _run_step(
        "offline-wheel-install",
        [
            uv,
            "pip",
            "install",
            "--offline",
            "--no-deps",
            "--python",
            str(python),
            str(wheel),
        ],
        external,
        evidence,
        env,
    )

    api_smoke = """
from pathlib import Path
import sys
import tempfile
from fastapi.testclient import TestClient
import krim_agent
from krim_agent.api.server import create_app
from krim_agent.config import Settings
from krim_agent.sdk import (
    EvaluationAutomationSDK,
    EvaluationSDK,
    RunSDK,
    SessionSDK,
    WorkspaceEvaluationSDK,
    WorkspaceSDK,
)
assert krim_agent.__version__ == EXPECTED_VERSION
installed_file = Path(krim_agent.__file__).resolve()
assert installed_file.is_relative_to(Path(sys.prefix).resolve()), installed_file
assert not installed_file.is_relative_to(Path(SOURCE_ROOT).resolve()), installed_file
assert all((RunSDK, EvaluationSDK, SessionSDK, WorkspaceSDK,
            WorkspaceEvaluationSDK, EvaluationAutomationSDK))
with tempfile.TemporaryDirectory(prefix="krim-agent-api-smoke-") as directory:
    app = create_app(settings=Settings(
        database=Path(directory) / "smoke.sqlite",
        product_eval_enabled=False,
        evaluation_automation_enabled=False,
    ))
    with TestClient(
        app,
        base_url="http://127.0.0.1",
        client=("127.0.0.1", 50000),
    ) as client:
        response = client.get("/openapi.json")
        assert response.status_code == 200, response.text
        assert response.json()["info"]["version"] == EXPECTED_VERSION
        print("ASGI /openapi.json", response.status_code, "version", EXPECTED_VERSION)
""".replace("EXPECTED_VERSION", repr(distribution_contract(root)["version"])).replace(
        "SOURCE_ROOT", repr(str(root.resolve()))
    )
    _run_step(
        "installed-import-api-sdk",
        [str(python), "-I", "-B", "-c", api_smoke],
        external,
        evidence,
        env,
    )
    for command_name in ("krim", "krim-eval"):
        executable = venv / ("Scripts" if os.name == "nt" else "bin") / (
            command_name + (".exe" if os.name == "nt" else "")
        )
        _run_step(
            f"installed-{command_name}-help",
            [str(executable), "--help"],
            external,
            evidence,
            env,
        )
    return external, True


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preflight", action="store_true", help="只核对源码白名单和版本元数据")
    parser.add_argument("--build-source", type=Path, help="离线构建并验证此项目源码")
    parser.add_argument("--artifacts-dir", type=Path, help="验证含一个wheel与一个sdist的构建目录")
    parser.add_argument("--wheel", type=Path, help="候选wheel路径；须与--sdist配对")
    parser.add_argument("--sdist", type=Path, help="候选sdist路径；须与--wheel配对")
    parser.add_argument(
        "--report-root", type=Path, help="验证记录根目录，默认项目.cache独立UUID目录"
    )
    parser.add_argument("--uv", help="项目已有uv可执行路径；不安装新工具")
    parser.add_argument(
        "--keep-venv", action="store_true", help="成功后保留项目外安装环境供同包浏览器演练"
    )
    args = parser.parse_args(argv)
    modes = [
        args.build_source is not None,
        args.artifacts_dir is not None,
        args.wheel is not None or args.sdist is not None,
    ]
    if args.preflight:
        if any(modes):
            parser.error("--preflight不能与构建或候选包参数同时使用")
    elif sum(modes) != 1:
        parser.error("请选择--build-source、--artifacts-dir，或同时提供--wheel与--sdist")
    elif (args.wheel is None) != (args.sdist is None):
        parser.error("--wheel与--sdist必须成对提供")
    return args


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        contract_root = args.build_source.resolve() if args.build_source else ROOT
        contract = distribution_contract(contract_root)
    except (OSError, DistributionError) as error:
        print(str(error), file=sys.stderr)
        return 2

    if args.preflight:
        summary = {
            "status": "preflight-passed",
            "name": contract["name"],
            "version": contract["version"],
            "frontend_version": contract["frontend_version"],
            "wheel_file_count": len(contract["wheel_files"]),
            "python_file_count": contract["python_file_count"],
            "sdist_file_count_before_generated_metadata": len(contract["sdist_source_files"]),
            "sdist_include": contract["sdist_include"],
            "sdist_exclude": contract["sdist_exclude"],
            "package_license_declared": False,
            "third_party_notice_included": True,
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    report_root = args.report_root or (ROOT / ".cache/distribution-verification")
    report_root.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex
    run_dir = report_root / run_id
    run_dir.mkdir(exist_ok=False)
    started = datetime.now(UTC).isoformat()
    state: dict[str, Any] = {
        "status": "running",
        "started_at": started,
        "project_root": str(contract_root),
        "version": contract["version"],
        "report_directory": str(run_dir.resolve()),
        "offline": True,
        "package_license_declared": False,
    }
    source_snapshot = _source_snapshot(contract_root, contract)
    source_snapshot_path = run_dir / "source-snapshot.json"
    _json_write(source_snapshot_path, source_snapshot)
    state["source_snapshot"] = {
        "git_head": source_snapshot["git_head"],
        "git_worktree_dirty": source_snapshot["git_worktree_dirty"],
        "file_count": source_snapshot["file_count"],
        "files_sha256": source_snapshot["files_sha256"],
        "manifest": str(source_snapshot_path),
    }
    _json_write(run_dir / "run.json", state)
    print(f"验证记录：{run_dir.resolve()}", flush=True)
    external_venv: Path | None = None
    artifact_manifest: dict[str, Any] | None = None
    manifest_path = run_dir / "distribution-manifest.json"

    def record_external_venv(path: Path) -> None:
        nonlocal external_venv
        external_venv = path
        outside_project = not path.resolve().is_relative_to(contract_root.resolve())
        state.update(
            {
                "external_venv": str(path),
                "external_venv_preserved": True,
                "venv_created_outside_project": outside_project,
            }
        )
        if artifact_manifest is not None:
            artifact_manifest["installation"]["venv_created_outside_project"] = outside_project
            _json_write(manifest_path, artifact_manifest)
        _json_write(run_dir / "run.json", state)

    try:
        if args.build_source:
            uv = _find_uv(args.uv)
            artifact_dir = run_dir / "artifacts"
            artifact_dir.mkdir(exist_ok=False)
            _run_step(
                "offline-build",
                [uv, "build", "--offline", "--python", sys.executable,
                 "--out-dir", str(artifact_dir)],
                contract_root,
                run_dir,
                _minimal_env(),
            )
            wheel, sdist = _artifact_pair(artifact_dir)
        elif args.artifacts_dir:
            wheel, sdist = _artifact_pair(args.artifacts_dir.resolve())
        else:
            assert args.wheel is not None and args.sdist is not None
            wheel, sdist = args.wheel.resolve(), args.sdist.resolve()

        wheel_record = _verify_wheel(wheel, contract)
        sdist_record = _verify_sdist(sdist, contract)
        artifact_manifest = {
            "name": contract["name"],
            "version": contract["version"],
            "frontend_private_version": contract["frontend_version"],
            "python_file_count": contract["python_file_count"],
            "source_snapshot": {
                key: source_snapshot[key]
                for key in ("git_head", "git_worktree_dirty", "file_count", "files_sha256")
            },
            "wheel": wheel_record,
            "sdist": sdist_record,
            "installation": {
                "status": "pending",
                "venv_created_outside_project": None,
                "uv_offline": True,
                "dependencies_resolved_from_local_cache": None,
                "planned_smokes": [
                    "locked runtime dependency sync (offline, no project install or dev)",
                    "isolated wheel install without dependency resolution",
                    "installed Python import and SDK exports",
                    "ASGI TestClient GET /openapi.json (no server process)",
                    "installed krim --help",
                    "installed krim-eval --help",
                ],
                "external_model_or_provider_calls": 0,
                "external_cli_execution_claimed": False,
            },
        }
        _json_write(manifest_path, artifact_manifest)
        state.update(
            {
                "wheel": str(wheel),
                "sdist": str(sdist),
                "manifest": str(manifest_path),
                "verification_stage": "project-external-install-and-smoke",
            }
        )
        _json_write(run_dir / "run.json", state)

        uv = _find_uv(args.uv)
        external_venv, venv_created = _smoke_installed(
            wheel, contract_root, run_dir, uv, on_venv_created=record_external_venv
        )
        artifact_manifest["installation"].update(
            {
                "status": "passed",
                "venv_created_outside_project": venv_created,
                "dependencies_resolved_from_local_cache": True,
            }
        )
        _json_write(manifest_path, artifact_manifest)
        if not args.keep_venv:
            shutil.rmtree(external_venv)
        state.update(
            {
                "status": "passed",
                "finished_at": datetime.now(UTC).isoformat(),
                "external_venv_removed": not args.keep_venv,
                "external_venv_preserved": str(external_venv) if args.keep_venv else None,
            }
        )
        _json_write(run_dir / "run.json", state)
        print(json.dumps(artifact_manifest, ensure_ascii=False, indent=2))
        return 0
    except Exception as error:
        if artifact_manifest is not None:
            artifact_manifest["installation"].update(
                {"status": "failed", "error": str(error)}
            )
            _json_write(manifest_path, artifact_manifest)
        state.update(
            {
                "status": "failed",
                "finished_at": datetime.now(UTC).isoformat(),
                "error": str(error),
                "traceback": traceback.format_exc(),
                "external_venv_preserved": str(external_venv) if external_venv else None,
            }
        )
        _json_write(run_dir / "run.json", state)
        print(f"发行验证失败，记录保留在{run_dir.resolve()}：{error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
