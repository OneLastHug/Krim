#!/usr/bin/env python3
"""在专用 Ubuntu 24.04 WSL 内验证并安装隔离的原生 Agent 工具。"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import platform
import posixpath
import secrets
import shutil
import subprocess
import tarfile
import tomllib
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

INSTALL_ROOT = Path("/home/krim/.local/share/krim-agent/native-tools")
CACHE_ROOT = Path("/home/krim/.cache/native-preparation")
OMP_REPO = "can1357/oh-my-pi"
OMP_TAG = "v18.13.3"
OMP_COMMIT = "413d278a87a69b453efabf8d1fedb9940385e8c6"
OMP_SHA256 = "42d39fa8996fed9f999e26306e60e2fdbea0c0de0d0b2d68feba275f44c41d04"
HERMES_REPO = "NousResearch/hermes-agent"
HERMES_TAG = "v0.22.5"
HERMES_COMMIT = "bf0203a794a07aac3ff764972ab9087baa4e4f4e"
NODE_VERSION = "24.22.2"
SRT_NAME = "@anthropic-ai/sandbox-runtime"
SRT_VERSION = "0.0.81"


class PreparationError(RuntimeError):
    """准备过程无法安全继续。"""


def log(message: str) -> None:
    print(message, flush=True)
    if RUN_LOG is not None:
        with RUN_LOG.open("a", encoding="utf-8") as stream:
            stream.write(message + "\n")


RUN_LOG: Path | None = None


def check_environment() -> None:
    if platform.system() != "Linux" or platform.machine() not in {"x86_64", "amd64"}:
        raise PreparationError("仅支持 x86_64 Linux WSL 环境。")
    try:
        os_release = Path("/etc/os-release").read_text(encoding="utf-8")
    except OSError as error:
        raise PreparationError("无法读取 Linux 发行版信息。") from error
    if "ID=ubuntu" not in os_release or 'VERSION_ID="24.04"' not in os_release:
        raise PreparationError("目标必须是 Ubuntu 24.04。")
    if os.getuid() == 0 or os.environ.get("USER") == "root":
        raise PreparationError("安装工具必须以 krim 普通用户运行；root 仅用于已授权的 apt 安装。")
    if Path.home() != Path("/home/krim"):
        raise PreparationError("当前用户主目录不是专用 WSL 的 /home/krim。")


def canonical_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def request_bytes(url: str, *, api: bool = False, timeout: int = 90) -> tuple[bytes, str]:
    allowed_hosts = {
        "api.github.com": {"api.github.com"},
        "github.com": {
            "github.com",
            "release-assets.githubusercontent.com",
            "objects.githubusercontent.com",
        },
        "codeload.github.com": {"codeload.github.com"},
        "raw.githubusercontent.com": {"raw.githubusercontent.com"},
        "nodejs.org": {"nodejs.org"},
        "registry.npmjs.org": {"registry.npmjs.org"},
    }
    initial_host = urllib.parse.urlparse(url).hostname
    if initial_host not in allowed_hosts:
        raise PreparationError(f"拒绝非官方下载主机：{initial_host}")
    headers = {"User-Agent": "Krim-Agent-native-preparation/1.0"}
    if api:
        headers["Accept"] = "application/vnd.github+json"
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        final_url = response.geturl()
        parsed = urllib.parse.urlparse(final_url)
        if parsed.scheme != "https" or parsed.hostname not in allowed_hosts[initial_host]:
            raise PreparationError(f"下载跳转到未授权地址：{parsed.scheme}://{parsed.hostname}")
        return response.read(), final_url


def request_json(url: str, save_to: Path, *, api: bool = True) -> Any:
    payload, _ = request_bytes(url, api=api)
    save_to.write_bytes(payload)
    try:
        return json.loads(payload)
    except json.JSONDecodeError as error:
        raise PreparationError(f"官方元信息不是有效 JSON：{url}") from error


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def git_blob_sha(data: bytes) -> str:
    return hashlib.sha1(b"blob " + str(len(data)).encode("ascii") + bytes([0]) + data).hexdigest()


def git_blob_file(path: Path) -> str:
    digest = hashlib.sha1(b"blob " + str(path.stat().st_size).encode("ascii") + bytes([0]))
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def require_fresh_dir(path: Path, *, resume: bool = False) -> Path:
    if resume:
        path.mkdir(parents=True, exist_ok=True)
        if not path.is_dir():
            raise PreparationError(f"预期目录不存在或不是目录：{path.name}")
        return path
    path.mkdir(parents=True, exist_ok=False)
    return path


def download(
    url: str,
    directory: Path,
    filename: str,
    expected_sha256: str | None = None,
    *,
    resume: bool = False,
) -> Path:
    if directory.exists() and not resume:
        raise PreparationError(f"下载目录不是全新空目录：{directory.name}")
    require_fresh_dir(directory, resume=resume)
    output = directory / filename
    if output.exists():
        if resume and expected_sha256 and sha256_file(output).lower() == expected_sha256.lower():
            log(f"复用已校验文件：{filename}；SHA-256={expected_sha256}")
            return output
        if resume and expected_sha256 is None:
            log(f"复用待后续元信息/内容核验文件：{filename}；SHA-256={sha256_file(output)}")
            return output
        raise PreparationError(f"下载目录内已有无法安全复用的文件：{filename}")
    if any(directory.iterdir()):
        raise PreparationError(f"下载目录并非空目录：{directory.name}")
    allowed_hosts = {
        "github.com": {
            "github.com",
            "release-assets.githubusercontent.com",
            "objects.githubusercontent.com",
        },
        "codeload.github.com": {"codeload.github.com"},
        "raw.githubusercontent.com": {"raw.githubusercontent.com"},
        "nodejs.org": {"nodejs.org"},
        "registry.npmjs.org": {"registry.npmjs.org"},
    }
    initial_host = urllib.parse.urlparse(url).hostname
    if initial_host not in allowed_hosts:
        raise PreparationError(f"拒绝非官方下载主机：{initial_host}")
    request = urllib.request.Request(
        url, headers={"User-Agent": "Krim-Agent-native-preparation/1.0"}
    )
    output = directory / filename
    digest = hashlib.sha256()
    with urllib.request.urlopen(request, timeout=240) as response:
        final_url = response.geturl()
        parsed = urllib.parse.urlparse(final_url)
        if parsed.scheme != "https" or parsed.hostname not in allowed_hosts[initial_host]:
            raise PreparationError(f"下载跳转到未授权地址：{parsed.scheme}://{parsed.hostname}")
        with output.open("xb") as stream:
            while chunk := response.read(1024 * 1024):
                stream.write(chunk)
                digest.update(chunk)
    actual = digest.hexdigest()
    if expected_sha256 is not None and actual.lower() != expected_sha256.lower():
        raise PreparationError(f"SHA-256 不匹配：{filename}，实际 {actual}")
    log(
        f"下载完成：{filename}；SHA-256={actual}；最终主机={urllib.parse.urlparse(final_url).hostname}"
    )
    return output


def github_json(run_dir: Path, repo: str, endpoint: str, label: str) -> Any:
    url = f"https://api.github.com/repos/{repo}/{endpoint.lstrip('/')}"
    return request_json(url, run_dir / "metadata" / f"{label}.json")


def resolve_tag_commit(run_dir: Path, repo: str, tag: str, label: str) -> str:
    ref = github_json(
        run_dir, repo, f"git/ref/tags/{urllib.parse.quote(tag, safe='')}", f"{label}-tag-ref"
    )
    obj = ref["object"]
    depth = 0
    while obj.get("type") == "tag":
        depth += 1
        if depth > 5:
            raise PreparationError(f"{repo} 标签注解层数异常。")
        annotated = github_json(
            run_dir, repo, f"git/tags/{obj['sha']}", f"{label}-tag-object-{depth}"
        )
        obj = annotated["object"]
    if obj.get("type") != "commit":
        raise PreparationError(f"{repo} 标签没有解析到 commit。")
    return obj["sha"]


def github_tree(run_dir: Path, repo: str, commit: str, label: str) -> dict[str, dict[str, str]]:
    data = github_json(run_dir, repo, f"git/trees/{commit}?recursive=1", f"{label}-tree")
    if data.get("truncated"):
        raise PreparationError(f"{repo} 源码树被 GitHub 截断，无法完整核对。")
    return {
        entry["path"]: {"sha": entry["sha"], "type": entry["type"], "mode": entry["mode"]}
        for entry in data.get("tree", [])
        if entry.get("type") == "blob"
    }


def asset_by_name(release: dict[str, Any], name: str) -> dict[str, Any]:
    matches = [asset for asset in release.get("assets", []) if asset.get("name") == name]
    if len(matches) != 1:
        raise PreparationError(f"GitHub 发布资产 {name} 缺失或不唯一。")
    return matches[0]


def extract_tar_safely(
    archive: Path, destination: Path, *, expected_tree: dict[str, dict[str, str]] | None = None
) -> Path:
    """只解包普通文件、目录和目标仍在根目录内的链接。"""
    require_fresh_dir(destination)
    links: list[tuple[tarfile.TarInfo, Path]] = []
    found_blobs: set[str] = set()
    with tarfile.open(archive, mode="r:*") as tar:
        members = tar.getmembers()
        top_levels: set[str] = set()
        for member in members:
            pure = PurePosixPath(member.name)
            if pure.is_absolute() or ".." in pure.parts:
                raise PreparationError(f"压缩包含越界路径：{member.name}")
            parts = tuple(part for part in pure.parts if part not in {"", "."})
            if not parts:
                continue
            top_levels.add(parts[0])
            target = destination.joinpath(*parts)
            if not target.resolve(strict=False).is_relative_to(destination.resolve()):
                raise PreparationError(f"压缩包路径越出解包目录：{member.name}")
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            elif member.isfile():
                target.parent.mkdir(parents=True, exist_ok=True)
                source = tar.extractfile(member)
                if source is None:
                    raise PreparationError(f"无法读取压缩包成员：{member.name}")
                size = member.size
                blob_digest = (
                    hashlib.sha1(b"blob " + str(size).encode("ascii") + bytes([0]))
                    if expected_tree is not None
                    else None
                )
                with target.open("xb") as output:
                    while chunk := source.read(1024 * 1024):
                        output.write(chunk)
                        if blob_digest is not None:
                            blob_digest.update(chunk)
                relative = (
                    "/".join(parts[1:])
                    if expected_tree is not None and len(parts) > 1
                    else "/".join(parts)
                )
                if expected_tree is not None:
                    expected = expected_tree.get(relative)
                    matches = (
                        expected is not None
                        and blob_digest is not None
                        and blob_digest.hexdigest() == expected["sha"]
                    )
                    if (
                        not matches
                        and PurePosixPath(relative).suffix.lower() == ".ps1"
                        and expected is not None
                    ):
                        normalized = target.read_bytes().replace(bytes([13, 10]), bytes([10]))
                        matches = git_blob_sha(normalized) == expected["sha"]
                        if matches:
                            log(f"源码核验通过：{relative}（按上游规则核对 CRLF）")
                    if not matches:
                        raise PreparationError(f"收尾组 源码与已核对 commit 不一致：{relative}")
                    found_blobs.add(relative)
                target.chmod(member.mode & 0o777)
            elif member.issym() or member.islnk():
                links.append((member, target))
            else:
                raise PreparationError(f"拒绝解包特殊文件：{member.name}")
        for member, target in links:
            base = target.parent if member.issym() else destination
            link_path = Path(posixpath.normpath(posixpath.join(str(base), member.linkname)))
            if Path(member.linkname).is_absolute() or not link_path.resolve(
                strict=False
            ).is_relative_to(destination.resolve()):
                raise PreparationError(f"拒绝越界链接：{member.name}")
            relative = (
                "/".join(PurePosixPath(member.name).parts[1:])
                if expected_tree is not None
                else None
            )
            if expected_tree is not None:
                expected = expected_tree.get(relative or "")
                target_bytes = member.linkname.encode("utf-8")
                if expected is None or git_blob_sha(target_bytes) != expected["sha"]:
                    raise PreparationError(f"收尾组 源码链接与已核对 commit 不一致：{relative}")
                found_blobs.add(relative or "")
            target.parent.mkdir(parents=True, exist_ok=True)
            if member.issym():
                target.symlink_to(member.linkname)
            else:
                target.hardlink_to(link_path)
    if expected_tree is not None:
        expected_paths = set(expected_tree)
        if found_blobs != expected_paths:
            missing = sorted(expected_paths - found_blobs)[:5]
            extra = sorted(found_blobs - expected_paths)[:5]
            raise PreparationError(f"收尾组 源码树文件集合不一致；missing={missing} extra={extra}")
    roots = [destination / name for name in sorted(top_levels)]
    if len(roots) != 1 or not roots[0].is_dir():
        raise PreparationError("压缩包顶层目录异常。")
    return roots[0]


def verify_git_tree_extraction(root: Path, expected_tree: dict[str, dict[str, str]]) -> None:
    expected_paths = set(expected_tree)
    found_paths: set[str] = set()
    for relative, entry in expected_tree.items():
        candidate = root / relative
        if entry["mode"] == "120000":
            if (
                not candidate.is_symlink()
                or git_blob_sha(os.readlink(candidate).encode("utf-8")) != entry["sha"]
            ):
                raise PreparationError(f"收尾组 源文件与已核对 commit 不一致：{relative}")
        else:
            if not candidate.is_file():
                raise PreparationError(f"收尾组 源文件与已核对 commit 不一致：{relative}")
            actual = git_blob_file(candidate)
            if actual != entry["sha"]:
                normalized_ok = (
                    PurePosixPath(relative).suffix.lower() == ".ps1"
                    and git_blob_sha(candidate.read_bytes().replace(bytes([13, 10]), bytes([10])))
                    == entry["sha"]
                )
                if not normalized_ok:
                    raise PreparationError(f"收尾组 源文件与已核对 commit 不一致：{relative}")
        found_paths.add(relative)
    actual_paths = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() or path.is_symlink()
    }
    if actual_paths != expected_paths:
        raise PreparationError("收尾组 解包目录的文件清单与固定 commit 不一致。")


def validate_hermes_package(source: Path, report_path: Path) -> dict[str, Any]:
    pyproject = source / "pyproject.toml"
    if not pyproject.is_file():
        raise PreparationError("收尾组 tag 源码中缺少 pyproject.toml。")
    config = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    project = config.get("project", {})
    build_system = config.get("build-system", {})
    requirements = project.get("requires-python", "")
    if requirements.replace(" ", "") != ">=3.11,<3.15":
        raise PreparationError(f"收尾组 Python 要求与预期不符：{requirements!r}")
    packaging_files = ["setup.py", "setup.cfg", "hatch_build.py", "build.py"]
    present_hooks = [name for name in packaging_files if (source / name).exists()]
    setup_text = (
        (source / "setup.py").read_text(encoding="utf-8") if (source / "setup.py").is_file() else ""
    )
    editable_guard = all(
        marker in setup_text
        for marker in ("build_editable", "_GuardedBdistWheel", "_GuardedSdist", "HERMES_NIX_BUILD")
    )
    scripts = project.get("scripts", {})
    tools = config.get("tool", {})
    hatch_build = tools.get("hatch", {}).get("build", {})
    setuptools_config = tools.get("setuptools", {})
    build_hooks = {
        "hatch": hatch_build.get("hooks", {}),
        "setuptools_cmdclass": setuptools_config.get("cmdclass", {}),
        "backend_path": build_system.get("backend-path", []),
    }
    dependencies = [*project.get("dependencies", []), *build_system.get("requires", [])]
    direct_dependencies = [
        dependency for dependency in dependencies if " @ " in dependency or "git+" in dependency
    ]
    inactive_android_dependencies = [
        dependency
        for dependency in direct_dependencies
        if "sys_platform == 'android'" in dependency
    ]
    active_direct_dependencies = [
        dependency
        for dependency in direct_dependencies
        if dependency not in inactive_android_dependencies
    ]
    report = {
        "commit": HERMES_COMMIT,
        "package_version": project.get("version"),
        "requires_python": requirements,
        "build_backend": build_system.get("build-backend"),
        "build_requirements": build_system.get("requires", []),
        "base_dependencies": project.get("dependencies", []),
        "optional_dependency_groups": sorted(project.get("optional-dependencies", {}).keys()),
        "console_scripts": scripts,
        "packaging_hook_files": present_hooks,
        "editable_setup_guard_verified": editable_guard,
        "install_method": "PEP 660 editable install from verified source checkout",
        "declared_build_hooks": build_hooks,
        "inactive_platform_direct_dependencies": inactive_android_dependencies,
        "package_version_is_commit_identified": project.get("version") == "0.0.0",
    }
    canonical_json(report_path, report)
    log("收尾组 安装前静态检查：" + json.dumps(report, ensure_ascii=False, sort_keys=True))
    unexpected_hooks = [name for name in present_hooks if name != "setup.py"]
    if (
        unexpected_hooks
        or ("setup.py" in present_hooks and not editable_guard)
        or any(build_hooks.values())
    ):
        raise PreparationError(
            f"收尾组 源码含未审核打包钩子：files={present_hooks}; hooks={build_hooks}"
        )
    if build_system.get("build-backend") not in {
        "setuptools.build_meta",
        "hatchling.build",
        "flit_core.buildapi",
    }:
        raise PreparationError(
            f"收尾组 使用未审核的 build backend：{build_system.get('build-backend')!r}"
        )
    if active_direct_dependencies:
        raise PreparationError(
            f"收尾组 Linux 依赖存在直接 URL/Git 源：{active_direct_dependencies}"
        )
    if not scripts:
        raise PreparationError("收尾组 未声明 console script，无法确认本地 CLI 入口。")
    return report


def verify_sri(path: Path, integrity: str) -> str:
    algorithm, encoded = integrity.split("-", 1)
    if algorithm not in {"sha256", "sha384", "sha512"}:
        raise PreparationError(f"不支持 npm integrity 算法：{algorithm}")
    digest = hashlib.new(algorithm, path.read_bytes()).digest()
    actual = base64.b64encode(digest).decode("ascii")
    if actual != encoded.split("?", 1)[0]:
        raise PreparationError("npm tarball integrity 校验失败。")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def prepare(run_id: str, *, resume: bool = False) -> Path:
    global RUN_LOG
    check_environment()
    run_dir = CACHE_ROOT / run_id
    if run_dir.exists() and not resume:
        raise PreparationError("准备记录目录已存在；复用时必须明确指定 --resume。")
    run_dir.mkdir(parents=True, exist_ok=resume)
    for name in ("metadata", "downloads", "unpacked", "reports", "clean-cwd", "isolated-home"):
        (run_dir / name).mkdir(exist_ok=resume)
    RUN_LOG = run_dir / "preparation.log"
    RUN_LOG.touch(exist_ok=resume)
    log(f"准备记录 ID：{run_id}")
    log(f"专用发行版：Ubuntu 24.04；普通用户：{os.getuid()}；架构：{platform.machine()}")

    INSTALL_ROOT.mkdir(parents=True, exist_ok=True)
    for child in (
        "omp-v18.13.3",
        "hermes-agent-v0.22.5",
        "python-3.14",
        f"node-v{NODE_VERSION}-linux-x64",
        f"sandbox-runtime-v{SRT_VERSION}",
    ):
        if (INSTALL_ROOT / child).exists():
            raise PreparationError(f"目标已存在，拒绝覆盖：{child}")

    omp_release = github_json(run_dir, OMP_REPO, f"releases/tags/{OMP_TAG}", "oh-my-pi-release")
    omp_commit = resolve_tag_commit(run_dir, OMP_REPO, OMP_TAG, "oh-my-pi")
    if omp_commit != OMP_COMMIT:
        raise PreparationError(f"oh-my-pi tag commit 不匹配：{omp_commit}")
    omp_asset = asset_by_name(omp_release, "omp-linux-x64")
    omp_digest = omp_asset.get("digest")
    if omp_digest and omp_digest != f"sha256:{OMP_SHA256}":
        raise PreparationError(f"GitHub 官方 asset digest 不匹配：{omp_digest}")
    sums_asset = asset_by_name(omp_release, "SHA256SUMS.txt")
    license_asset = asset_by_name(omp_release, "LICENSE")
    notice_asset = asset_by_name(omp_release, "THIRD-PARTY-NOTICES.txt")
    download(
        omp_asset["browser_download_url"],
        run_dir / "downloads" / "oh-my-pi-binary",
        "omp-linux-x64",
        OMP_SHA256,
        resume=resume,
    )
    sums_file = download(
        sums_asset["browser_download_url"],
        run_dir / "downloads" / "oh-my-pi-checksums",
        "SHA256SUMS.txt",
        sums_asset["digest"].split(":", 1)[1],
        resume=resume,
    )
    checksum_lines = sums_file.read_text(encoding="utf-8", errors="replace").splitlines()
    if not any(
        line.split()[0].lower() == OMP_SHA256 and line.split()[-1].lstrip("* ") == "omp-linux-x64"
        for line in checksum_lines
        if len(line.split()) >= 2
    ):
        raise PreparationError("上游 SHA256SUMS.txt 未包含指定 omp-linux-x64 摘要。")
    download(
        license_asset["browser_download_url"],
        run_dir / "downloads" / "oh-my-pi-license",
        "LICENSE",
        license_asset["digest"].split(":", 1)[1],
        resume=resume,
    )
    download(
        notice_asset["browser_download_url"],
        run_dir / "downloads" / "oh-my-pi-notice",
        "THIRD-PARTY-NOTICES.txt",
        notice_asset["digest"].split(":", 1)[1],
        resume=resume,
    )

    hermes_commit = resolve_tag_commit(run_dir, HERMES_REPO, HERMES_TAG, "hermes")
    if hermes_commit != HERMES_COMMIT:
        raise PreparationError(f"收尾组 tag commit 不匹配：{hermes_commit}")
    hermes_tree = github_tree(run_dir, HERMES_REPO, hermes_commit, "hermes")
    hermes_archive = download(
        f"https://codeload.github.com/{HERMES_REPO}/tar.gz/{HERMES_COMMIT}",
        run_dir / "downloads" / "hermes-source",
        "hermes-agent.tar.gz",
        resume=resume,
    )
    hermes_unpack_dir = run_dir / "unpacked" / "hermes-source"
    if resume and hermes_unpack_dir.exists():
        roots = list(hermes_unpack_dir.iterdir())
        if len(roots) != 1 or not roots[0].is_dir():
            raise PreparationError("缓存的 收尾组 解包目录结构异常。")
        hermes_source_root = roots[0]
        verify_git_tree_extraction(hermes_source_root, hermes_tree)
        log("复用已按 commit 逐文件核验的 收尾组 源码解包目录。")
    else:
        hermes_source_root = extract_tar_safely(
            hermes_archive, hermes_unpack_dir, expected_tree=hermes_tree
        )
    hermes_report = validate_hermes_package(
        hermes_source_root, run_dir / "reports" / "hermes-package-inspection.json"
    )

    releases = request_json(
        "https://api.github.com/repos/astral-sh/python-build-standalone/releases?per_page=10",
        run_dir / "metadata" / "python-build-standalone-releases.json",
    )
    py_release = None
    py_asset = None
    for release in releases:
        candidates = [
            item
            for item in release.get("assets", [])
            if item.get("name", "").startswith("cpython-3.14.")
            and "x86_64-unknown-linux-gnu-install_only.tar.gz" in item.get("name", "")
            and "freethreaded" not in item.get("name", "")
        ]
        if candidates:
            py_release, py_asset = release, candidates[0]
            break
    if py_asset is None:
        raise PreparationError(
            "未找到带官方 GitHub SHA-256 元数据的 CPython 3.14 standalone 安装包。"
        )
    py_digest = py_asset.get("digest")
    if not isinstance(py_digest, str) or not py_digest.startswith("sha256:"):
        raise PreparationError("Python standalone GitHub asset 没有可核对的 SHA-256 digest。")
    py_archive = download(
        py_asset["browser_download_url"],
        run_dir / "downloads" / "python-standalone",
        py_asset["name"],
        py_digest.split(":", 1)[1],
        resume=resume,
    )
    python_tree = extract_tar_safely(py_archive, run_dir / "unpacked" / "python-standalone")
    python_candidates = list(python_tree.rglob("python3.14"))
    if not python_candidates:
        raise PreparationError("Python standalone 包中未找到 python3.14。")

    node_sums = download(
        f"https://nodejs.org/dist/v{NODE_VERSION}/SHASUMS256.txt",
        run_dir / "downloads" / "node-checksums",
        "SHASUMS256.txt",
        resume=resume,
    )
    node_archive_name = f"node-v{NODE_VERSION}-linux-x64.tar.xz"
    node_sum = None
    for line in node_sums.read_text(encoding="ascii", errors="replace").splitlines():
        fields = line.split()
        if len(fields) >= 2 and fields[-1].lstrip("* ") == node_archive_name:
            node_sum = fields[0]
            break
    if not node_sum or len(node_sum) != 64:
        raise PreparationError(f"Node 官方 SHASUMS256.txt 未包含 {node_archive_name}。")
    node_archive = download(
        f"https://nodejs.org/dist/v{NODE_VERSION}/{node_archive_name}",
        run_dir / "downloads" / "node-distribution",
        node_archive_name,
        node_sum,
        resume=resume,
    )
    node_tree = extract_tar_safely(node_archive, run_dir / "unpacked" / "node-distribution")
    if not (node_tree / "bin" / "node").is_file() or not (node_tree / "bin" / "npm").exists():
        raise PreparationError("官方 Node 包缺少 node 或 npm 入口。")

    registry_url = "https://registry.npmjs.org/%40anthropic-ai%2fsandbox-runtime/" + SRT_VERSION
    srt_meta = request_json(
        registry_url, run_dir / "metadata" / "sandbox-runtime-registry.json", api=False
    )
    if srt_meta.get("name") != SRT_NAME or srt_meta.get("version") != SRT_VERSION:
        raise PreparationError("npm registry 包名或版本与目标不一致。")
    dist = srt_meta.get("dist", {})
    integrity = dist.get("integrity")
    tarball_url = dist.get("tarball", "")
    if not integrity or not tarball_url.startswith("https://registry.npmjs.org/"):
        raise PreparationError("npm 官方发布缺少 integrity 或 tarball 不来自 registry.npmjs.org。")
    srt_tarball = download(
        tarball_url, run_dir / "downloads" / "sandbox-runtime-package", "sandbox-runtime.tgz"
    )
    srt_sha256 = verify_sri(srt_tarball, integrity)
    srt_root = extract_tar_safely(srt_tarball, run_dir / "unpacked" / "sandbox-runtime-package")
    package_json = json.loads((srt_root / "package.json").read_text(encoding="utf-8"))
    if package_json.get("name") != SRT_NAME or package_json.get("version") != SRT_VERSION:
        raise PreparationError("npm tarball 中的包名或版本与 registry 元信息不一致。")
    srt_report = {
        "name": package_json["name"],
        "version": package_json["version"],
        "registry_integrity": integrity,
        "tarball_sha256": srt_sha256,
        "bin": package_json.get("bin", {}),
        "lifecycle_scripts": {
            key: value
            for key, value in package_json.get("scripts", {}).items()
            if key in {"preinstall", "install", "postinstall", "prepare"}
        },
        "dependencies": package_json.get("dependencies", {}),
    }
    canonical_json(run_dir / "reports" / "sandbox-runtime-package-inspection.json", srt_report)
    log(
        "sandbox-runtime 安装前静态检查："
        + json.dumps(srt_report, ensure_ascii=False, sort_keys=True)
    )

    canonical_json(
        run_dir / "reports" / "source-and-artifact-verification.json",
        {
            "oh_my_pi": {
                "tag": OMP_TAG,
                "commit": omp_commit,
                "asset": "omp-linux-x64",
                "expected_sha256": OMP_SHA256,
                "checksums_asset": "SHA256SUMS.txt",
                "license_files": ["LICENSE", "THIRD-PARTY-NOTICES.txt"],
            },
            "hermes": {"tag": HERMES_TAG, "commit": hermes_commit, "inspection": hermes_report},
            "python": {
                "release_tag": py_release.get("tag_name"),
                "asset": py_asset["name"],
                "sha256": py_digest.split(":", 1)[1],
            },
            "node": {"version": NODE_VERSION, "asset": node_archive_name, "sha256": node_sum},
            "sandbox_runtime": srt_report,
            "apt_dependencies": {
                "socat": "installed separately from Ubuntu 24.04 apt sources",
                "ripgrep": "installed separately from Ubuntu 24.04 apt sources",
            },
        },
    )
    log("第一阶段完成：发布、摘要及安装钩子已检查；尚未安装 Python/npm 包。")
    return run_dir


def subprocess_env(run_dir: Path, path: str = "/usr/bin:/bin") -> dict[str, str]:
    """仅向构建/包管理子进程传递无凭据的最小环境。"""
    user_config = run_dir / "empty-npm-user.rc"
    global_config = run_dir / "empty-npm-global.rc"
    for config in (user_config, global_config):
        if config.exists():
            if config.is_symlink() or config.read_bytes():
                raise PreparationError("npm 隔离配置必须是本轮空文件。")
        else:
            config.touch(mode=0o600, exist_ok=False)
    return {
        "PATH": path,
        "HOME": str(run_dir / "isolated-home"),
        "TMPDIR": str(run_dir / "tmp"),
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PIP_CONFIG_FILE": os.devnull,
        "PIP_KEYRING_PROVIDER": "disabled",
        "PIP_NO_INPUT": "1",
        "PIP_DISABLE_PIP_VERSION_CHECK": "1",
        "NPM_CONFIG_USERCONFIG": str(user_config),
        "NPM_CONFIG_GLOBALCONFIG": str(global_config),
        "NPM_CONFIG_IGNORE_SCRIPTS": "true",
        "NPM_CONFIG_AUDIT": "false",
        "NPM_CONFIG_FUND": "false",
        "NPM_CONFIG_UPDATE_NOTIFIER": "false",
        "NPM_CONFIG_REGISTRY": "https://registry.npmjs.org/",
        "NPM_CONFIG_CACHE": str(run_dir / "npm-cache"),
    }


def run_logged(
    command: list[str], *, cwd: Path, env: dict[str, str], label: str, timeout: int = 1800
) -> subprocess.CompletedProcess[str]:
    log(f"运行命令：{label}")
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        check=False,
    )
    output = completed.stdout or ""
    if output:
        log(output.rstrip())
    log(f"命令退出码：{completed.returncode}")
    if completed.returncode != 0:
        raise PreparationError(f"命令失败：{label}，退出码 {completed.returncode}")
    return completed


def _distribution_root(directory: Path, executable: str) -> Path:
    if (directory / executable).is_file() and not directory.is_symlink():
        return directory
    candidates = [
        item
        for item in directory.iterdir()
        if item.is_dir() and not item.is_symlink() and (item / executable).is_file()
    ]
    if len(candidates) != 1:
        raise PreparationError("发行包根目录不唯一或缺少可执行文件。")
    return candidates[0]


def _repair_node_layout(directory: Path) -> None:
    nested = directory / f"node-v{NODE_VERSION}-linux-x64"
    if (directory / "bin/node").exists():
        return
    if not nested.is_dir() or nested.is_symlink() or list(directory.iterdir()) != [nested]:
        raise PreparationError("已有 Node 布局不属于本次可恢复目录。")
    for item in list(nested.iterdir()):
        item.rename(directory / item.name)
    nested.rmdir()


def _hermes_sync_command(uv: Path, python: Path, source: Path, *, check: bool = False) -> list[str]:
    # 官方锁文件包含上游依赖覆盖；普通 pip 重解析无法表达这些条件。
    command = [
        str(uv),
        "sync",
        "--frozen",
        "--no-dev",
        "--no-default-groups",
        "--project",
        str(source),
        "--python",
        str(python),
    ]
    return command + ["--check", "--offline"] if check else command


def install(run_id: str, *, resume: bool = False) -> Path:
    global RUN_LOG
    check_environment()
    run_dir = CACHE_ROOT / run_id
    if (
        not run_dir.is_dir()
        or not (run_dir / "reports" / "source-and-artifact-verification.json").is_file()
    ):
        raise PreparationError("指定 run-id 未完成第一阶段验证。")
    RUN_LOG = run_dir / "preparation.log"
    report = json.loads(
        (run_dir / "reports" / "source-and-artifact-verification.json").read_text(encoding="utf-8")
    )
    omp_dir = INSTALL_ROOT / "omp-v18.13.3"
    hermes_dir = INSTALL_ROOT / "hermes-agent-v0.22.5"
    py_dir = INSTALL_ROOT / "python-3.14"
    node_dir = INSTALL_ROOT / f"node-v{NODE_VERSION}-linux-x64"
    srt_dir = INSTALL_ROOT / f"sandbox-runtime-v{SRT_VERSION}"
    for target in (omp_dir, hermes_dir, py_dir, node_dir, srt_dir):
        if target.is_symlink() or (target.exists() and not target.is_dir()):
            raise PreparationError(f"目标不是自有普通目录：{target.name}")
        if target.exists() and not resume:
            raise PreparationError(f"目标已存在，拒绝覆盖：{target.name}")
    tmp = run_dir / "tmp"
    tmp.mkdir(exist_ok=True)
    env = subprocess_env(run_dir)
    downloads = run_dir / "downloads"
    if (
        sha256_file(downloads / "oh-my-pi-binary" / "omp-linux-x64")
        != report["oh_my_pi"]["expected_sha256"]
    ):
        raise PreparationError("oh-my-pi 二进制在阶段间发生变化。")
    if (
        sha256_file(downloads / "python-standalone" / report["python"]["asset"])
        != report["python"]["sha256"]
    ):
        raise PreparationError("Python standalone 包在阶段间发生变化。")
    if (
        sha256_file(downloads / "node-distribution" / report["node"]["asset"])
        != report["node"]["sha256"]
    ):
        raise PreparationError("Node 官方包在阶段间发生变化。")
    if (
        verify_sri(
            downloads / "sandbox-runtime-package" / "sandbox-runtime.tgz",
            report["sandbox_runtime"]["registry_integrity"],
        )
        != report["sandbox_runtime"]["tarball_sha256"]
    ):
        raise PreparationError("sandbox-runtime npm 包在阶段间发生变化。")
    hermes_tree = json.loads(
        (run_dir / "metadata" / "hermes-tree.json").read_text(encoding="utf-8")
    )
    hermes_expected = {
        item["path"]: item for item in hermes_tree["tree"] if item.get("type") == "blob"
    }
    hermes_source_check = run_dir / "unpacked" / "hermes-source"
    hermes_root = next(hermes_source_check.iterdir())
    for relative, entry in hermes_expected.items():
        candidate = hermes_root / relative
        if entry["mode"] == "120000":
            if (
                not candidate.is_symlink()
                or git_blob_sha(os.readlink(candidate).encode("utf-8")) != entry["sha"]
            ):
                raise PreparationError(f"收尾组 源文件在阶段间发生变化：{relative}")
        else:
            if not candidate.is_file():
                raise PreparationError(f"收尾组 源文件在阶段间发生变化：{relative}")
            if git_blob_file(candidate) != entry["sha"]:
                normalized_ok = (
                    PurePosixPath(relative).suffix.lower() == ".ps1"
                    and git_blob_sha(candidate.read_bytes().replace(bytes([13, 10]), bytes([10])))
                    == entry["sha"]
                )
                if not normalized_ok:
                    raise PreparationError(f"收尾组 源文件在阶段间发生变化：{relative}")

    omp_dir.mkdir(exist_ok=resume)
    omp_materials = {
        "omp-linux-x64": "oh-my-pi-binary",
        "SHA256SUMS.txt": "oh-my-pi-checksums",
        "LICENSE": "oh-my-pi-license",
        "THIRD-PARTY-NOTICES.txt": "oh-my-pi-notice",
    }
    for name, folder in omp_materials.items():
        source, target = downloads / folder / name, omp_dir / name
        if target.exists():
            if target.is_symlink() or sha256_file(target) != sha256_file(source):
                raise PreparationError(f"已有 OMP 材料不匹配：{name}")
        else:
            shutil.copy2(source, target)
    (omp_dir / "omp-linux-x64").chmod(0o755)

    node_extract = _distribution_root(run_dir / "unpacked" / "node-distribution", "bin/node")
    if node_dir.exists():
        _repair_node_layout(node_dir)
        if sha256_file(node_dir / "bin/node") != sha256_file(node_extract / "bin/node"):
            raise PreparationError("已有 Node 二进制与已核验发行不一致。")
    else:
        shutil.copytree(node_extract, node_dir, symlinks=True)
    py_extracted = _distribution_root(run_dir / "unpacked" / "python-standalone", "bin/python3.14")
    if py_dir.exists():
        if sha256_file(py_dir / "bin/python3.14") != sha256_file(py_extracted / "bin/python3.14"):
            raise PreparationError("已有 Python 二进制与已核验发行不一致。")
    else:
        shutil.copytree(py_extracted, py_dir, symlinks=True)
    hermes_source = next((run_dir / "unpacked" / "hermes-source").iterdir())
    env = subprocess_env(run_dir, f"{node_dir / 'bin'}:/usr/bin:/bin")
    python_bin = py_dir / "bin/python3.14"
    if not python_bin.is_file():
        raise PreparationError("已安装私有 CPython 包内缺少 python3.14 可执行文件。")
    hermes_venv = hermes_dir / "venv"
    hermes_dir.mkdir(exist_ok=resume)
    if not hermes_venv.exists():
        run_logged(
            [str(python_bin), "-m", "venv", str(hermes_venv)],
            cwd=run_dir / "clean-cwd",
            env=env,
            label="创建 收尾组 独立 Python 3.14 venv",
        )
    elif not (hermes_venv / "bin/python").samefile(python_bin):
        raise PreparationError("已有 收尾组 venv 不属于本次私有 Python。")
    uv = Path("/home/krim/.venvs/krim-tools/bin/uv")
    if not uv.is_file() or not (hermes_source / "uv.lock").is_file():
        raise PreparationError("缺少已有 uv 工具或固定上游 uv.lock。")
    hermes_env = env | {
        "UV_PROJECT_ENVIRONMENT": str(hermes_venv),
        "UV_CACHE_DIR": str(run_dir / "uv-cache"),
        "UV_PYTHON_DOWNLOADS": "never",
    }
    lock_before = sha256_file(hermes_source / "uv.lock")
    already_synced = resume and (run_dir / "reports/hermes-lock-install.json").is_file()
    run_logged(
        _hermes_sync_command(uv, python_bin, hermes_source, check=already_synced),
        cwd=run_dir / "clean-cwd",
        env=hermes_env,
        label="检查已安装的官方 收尾组 锁定环境"
        if already_synced
        else "按官方 uv.lock 同步 收尾组 base 依赖及官方覆盖（不安装 extras）",
        timeout=3600,
    )
    if sha256_file(hermes_source / "uv.lock") != lock_before:
        raise PreparationError("收尾组 锁文件在冻结安装期间发生变化。")
    canonical_json(
        run_dir / "reports/hermes-lock-install.json",
        {
            "method": "official-uv-lock-frozen",
            "lock_sha256": lock_before,
            "extras": [],
            "upstream_overrides": tomllib.loads(
                (hermes_source / "pyproject.toml").read_text(encoding="utf-8")
            )
            .get("tool", {})
            .get("uv", {})
            .get("override-dependencies", []),
            "known_metadata_conflict": "official lock overrides browser-harness Pillow pin",
            "upstream_issue": "https://github.com/NousResearch/hermes-agent/issues/143313",
        },
    )

    srt_tarball = run_dir / "downloads" / "sandbox-runtime-package" / "sandbox-runtime.tgz"
    node_bin_dir = node_dir / "bin"
    npm_env = subprocess_env(run_dir, f"{node_bin_dir}:/usr/bin:/bin")
    srt_dir.mkdir(exist_ok=resume)
    vendor_dir = srt_dir / "vendor"
    vendor_dir.mkdir(exist_ok=resume)
    vendor_archive = vendor_dir / "sandbox-runtime.tgz"
    if vendor_archive.exists():
        if vendor_archive.is_symlink() or sha256_file(vendor_archive) != sha256_file(srt_tarball):
            raise PreparationError("已有 SRT tarball 与本轮核验材料不一致。")
    else:
        shutil.copy2(srt_tarball, vendor_archive)
    package_config = {
        "name": "krim-private-sandbox-runtime",
        "version": "0.0.0-private",
        "private": True,
        "dependencies": {SRT_NAME: "file:vendor/sandbox-runtime.tgz"},
    }
    package_path = srt_dir / "package.json"
    if package_path.exists():
        if json.loads(package_path.read_text(encoding="utf-8")) != package_config:
            raise PreparationError("已有 SRT 配置不属于本轮安装。")
    else:
        canonical_json(package_path, package_config)
    run_logged(
        [
            str(node_bin_dir / "npm"),
            "install",
            "--prefix",
            str(srt_dir),
            "--ignore-scripts",
            "--no-audit",
            "--no-fund",
        ],
        cwd=srt_dir,
        env=npm_env,
        label="本地安装 sandbox-runtime 固定 npm tarball（忽略全部生命周期脚本）",
        timeout=3600,
    )

    smoke: dict[str, Any] = {}

    def smoke_cmd(key: str, command: list[str]) -> None:
        result = run_logged(command, cwd=run_dir / "clean-cwd", env=env, label=key, timeout=45)
        smoke[key] = {
            "exit_code": result.returncode,
            "output": (result.stdout or "").strip()[:4000],
        }

    smoke_cmd("oh-my-pi --version", [str(omp_dir / "omp-linux-x64"), "--version"])
    smoke_cmd("oh-my-pi --help", [str(omp_dir / "omp-linux-x64"), "--help"])
    smoke_cmd("private Python version", [str(python_bin), "--version"])
    hermes_script = next(iter(report["hermes"]["inspection"]["console_scripts"]))
    hermes_cli = hermes_venv / "bin" / hermes_script
    smoke_cmd(f"收尾组 CLI {hermes_script} --version", [str(hermes_cli), "--version"])
    smoke_cmd(f"收尾组 CLI {hermes_script} --help", [str(hermes_cli), "--help"])
    smoke_cmd("private Node version", [str(node_bin_dir / "node"), "--version"])
    smoke_cmd(
        "private npm version",
        [
            str(node_bin_dir / "node"),
            str(node_dir / "lib/node_modules/npm/bin/npm-cli.js"),
            "--version",
        ],
    )
    srt_pkg = srt_dir / "node_modules" / "@anthropic-ai" / "sandbox-runtime" / "package.json"
    srt_json = json.loads(srt_pkg.read_text(encoding="utf-8"))
    srt_bin = srt_json.get("bin", {})
    if isinstance(srt_bin, str):
        srt_bin = {"sandbox-runtime": srt_bin}
    if not srt_bin:
        raise PreparationError("sandbox-runtime npm 包未声明命令入口。")
    srt_name, srt_relative = next(iter(srt_bin.items()))
    srt_exe = srt_dir / "node_modules" / "@anthropic-ai" / "sandbox-runtime" / srt_relative
    smoke_cmd(
        f"sandbox-runtime {srt_name} --version",
        [str(node_bin_dir / "node"), str(srt_exe), "--version"],
    )
    smoke_cmd(
        f"sandbox-runtime {srt_name} --help", [str(node_bin_dir / "node"), str(srt_exe), "--help"]
    )
    smoke_cmd("socat --version", ["/usr/bin/socat", "-V"])
    smoke_cmd("ripgrep --version", ["/usr/bin/rg", "--version"])
    smoke_cmd("bubblewrap --version", ["/usr/bin/bwrap", "--version"])

    install_record = {
        "oh_my_pi": {
            "version": OMP_TAG,
            "commit": OMP_COMMIT,
            "sha256": OMP_SHA256,
            "executable": str(omp_dir / "omp-linux-x64"),
        },
        "hermes": {
            "tag": HERMES_TAG,
            "commit": HERMES_COMMIT,
            "python": str(python_bin),
            "venv": str(hermes_venv),
            "executable": str(hermes_cli),
            "base_dependencies_only": True,
        },
        "python": {
            "asset": report["python"]["asset"],
            "sha256": report["python"]["sha256"],
            "executable": str(python_bin),
        },
        "node": {
            "version": NODE_VERSION,
            "sha256": report["node"]["sha256"],
            "executable": str(node_bin_dir / "node"),
            "npm": str(node_bin_dir / "npm"),
        },
        "sandbox_runtime": {
            "version": SRT_VERSION,
            "integrity": report["sandbox_runtime"]["registry_integrity"],
            "sha256": report["sandbox_runtime"]["tarball_sha256"],
            "executable": str(srt_exe),
        },
        "system_tools": {
            "socat": "/usr/bin/socat",
            "ripgrep": "/usr/bin/rg",
            "bubblewrap": "/usr/bin/bwrap",
        },
        "smoke_checks": smoke,
        "model_api_requests": 0,
        "credential_access": "not performed",
    }
    canonical_json(run_dir / "reports" / "installation-record.json", install_record)
    log("安装及命令级冒烟核验完成；记录仅列工具版本、摘要、路径和命令输出，不包含凭据。")
    return run_dir


def main() -> int:
    parser = argparse.ArgumentParser(
        description="准备专用 Ubuntu 24.04 WSL 原生 Agent 工具；默认只核对与下载，不安装。"
    )
    parser.add_argument("--run-id", help="复用第一阶段输出的记录 ID。")
    parser.add_argument(
        "--install", action="store_true", help="在完成静态检查后安装已核验的固定版本。"
    )
    parser.add_argument(
        "--resume", action="store_true", help="仅复用指定记录中摘要一致的已完成下载。"
    )
    args = parser.parse_args()
    try:
        if args.install:
            if not args.run_id:
                raise PreparationError("安装阶段必须提供第一阶段输出的 --run-id。")
            result = install(args.run_id, resume=args.resume)
        else:
            if args.resume and not args.run_id:
                raise PreparationError("--resume 必须搭配现有的 --run-id。")
            run_id = args.run_id or datetime.now(UTC).strftime(
                "%Y%m%dT%H%M%SZ"
            ) + "-" + secrets.token_hex(4)
            result = prepare(run_id, resume=args.resume)
        log(f"本轮记录：{result}")
        return 0
    except (
        PreparationError,
        OSError,
        tarfile.TarError,
        urllib.error.URLError,
        subprocess.SubprocessError,
    ) as error:
        log(f"准备失败：{type(error).__name__}: {error}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
