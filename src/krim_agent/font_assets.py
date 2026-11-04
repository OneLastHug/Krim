"""使用标准库校验有限的同源字体资产清单。"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path

FONT_MANIFEST_FILENAME = "font-manifest.json"
FONT_ASSET_DIRECTORY = "assets/fonts"
FONT_MANIFEST_VERSION = 1
MAX_FONT_MANIFEST_BYTES = 1_048_576
MAX_FONT_ASSETS = 1024
_FONT_PATH = re.compile(
    r"assets/fonts/[A-Za-z0-9][A-Za-z0-9._-]*-[A-Za-z0-9_-]{6,64}\.woff2\Z"
)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class FontManifestError(ValueError):
    """打包字体清单或受管理目录无效。"""


@dataclass(frozen=True)
class FontAsset:
    """一条经校验的字体路径、声明大小与摘要。"""

    path: str
    size_bytes: int
    sha256: str


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise FontManifestError(f"字体清单包含重复JSON键：{key!r}")
        result[key] = value
    return result


def parse_font_manifest(data: bytes) -> tuple[FontAsset, ...]:
    """解析严格的schema-v1 JSON，并拒绝含糊或不安全的资产路径。"""
    if len(data) > MAX_FONT_MANIFEST_BYTES:
        raise FontManifestError("字体清单超过允许大小")
    try:
        value = json.loads(data.decode("utf-8"), object_pairs_hook=_unique_object)
    except (UnicodeError, json.JSONDecodeError) as error:
        raise FontManifestError(f"字体清单不是有效UTF-8 JSON：{error}") from error
    if not isinstance(value, dict) or set(value) != {"version", "fonts"}:
        raise FontManifestError("字体清单必须仅包含version和fonts字段")
    if type(value["version"]) is not int or value["version"] != FONT_MANIFEST_VERSION:
        raise FontManifestError("字体清单版本不受支持")
    entries = value["fonts"]
    if not isinstance(entries, list) or len(entries) > MAX_FONT_ASSETS:
        raise FontManifestError("字体清单fonts必须是有限列表")

    assets: list[FontAsset] = []
    seen_paths: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {"path", "size_bytes", "sha256"}:
            raise FontManifestError("字体条目字段不符合schema")
        path = entry["path"]
        size = entry["size_bytes"]
        digest = entry["sha256"]
        if not isinstance(path, str) or _FONT_PATH.fullmatch(path) is None:
            raise FontManifestError(f"字体路径必须是专用目录内带hash的WOFF2：{path!r}")
        if type(size) is not int or size <= 0:
            raise FontManifestError(f"字体大小必须是正整数：{path}")
        if not isinstance(digest, str) or _SHA256.fullmatch(digest) is None:
            raise FontManifestError(f"字体SHA256格式无效：{path}")
        normalized = path.casefold()
        if normalized in seen_paths:
            raise FontManifestError(f"字体清单包含重复路径：{path}")
        seen_paths.add(normalized)
        assets.append(FontAsset(path=path, size_bytes=size, sha256=digest))
    return tuple(assets)


def _open_regular(path: Path, *, label: str) -> tuple[int, os.stat_result]:
    try:
        before = path.lstat()
    except OSError as error:
        raise FontManifestError(f"{label}不存在或无法检查：{path}") from error
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise FontManifestError(f"{label}必须是普通文件且不能是符号链接：{path}")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
        opened = os.fstat(descriptor)
    except OSError as error:
        raise FontManifestError(f"{label}不能安全打开：{path}") from error
    if not stat.S_ISREG(opened.st_mode):
        os.close(descriptor)
        raise FontManifestError(f"{label}必须是普通文件：{path}")
    return descriptor, opened


def _read_manifest(path: Path) -> bytes:
    descriptor, opened = _open_regular(path, label="字体清单")
    try:
        if opened.st_size > MAX_FONT_MANIFEST_BYTES:
            raise FontManifestError("字体清单超过允许大小")
        with os.fdopen(descriptor, "rb") as stream:
            descriptor = -1
            data = stream.read(MAX_FONT_MANIFEST_BYTES + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    return data


def _validate_web_root(web_root: Path) -> None:
    try:
        metadata = web_root.lstat()
    except OSError as error:
        raise FontManifestError(f"UI资源目录不存在或无法检查：{web_root}") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise FontManifestError(f"UI资源根目录必须是实际目录：{web_root}")


def _check_directory(path: Path, *, label: str) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    except OSError as error:
        raise FontManifestError(f"{label}无法检查：{path}") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise FontManifestError(f"{label}必须是普通目录且不能是符号链接：{path}")
    return True


def _verify_font_file(path: Path, asset: FontAsset) -> None:
    descriptor, opened = _open_regular(path, label="字体资产")
    try:
        if opened.st_size != asset.size_bytes:
            raise FontManifestError(f"字体资产大小与清单不符：{asset.path}")
        digest = hashlib.sha256()
        with os.fdopen(descriptor, "rb") as stream:
            descriptor = -1
            magic = stream.read(4)
            if magic != b"wOF2":
                raise FontManifestError(f"字体资产内容不是WOFF2：{asset.path}")
            digest.update(magic)
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if digest.hexdigest() != asset.sha256:
        raise FontManifestError(f"字体资产SHA256与清单不符：{asset.path}")


def load_font_assets(web_root: Path, *, allow_legacy_empty: bool = True) -> tuple[FontAsset, ...]:
    """服务字体资产前校验清单及专用目录的精确集合。

    兼容无清单的旧源码树，但仅限字体目录不存在或为空；绝不据此注册目录扫描结果。
    """
    web_root = Path(web_root)
    _validate_web_root(web_root)
    assets_directory = web_root / "assets"
    font_directory = web_root / FONT_ASSET_DIRECTORY
    manifest_path = web_root / FONT_MANIFEST_FILENAME
    assets_directory_exists = _check_directory(assets_directory, label="UI assets目录")
    font_directory_exists = _check_directory(font_directory, label="字体资产目录")

    try:
        manifest_metadata = manifest_path.lstat()
    except FileNotFoundError:
        if not allow_legacy_empty:
            raise FontManifestError("缺少正式字体清单") from None
        if font_directory_exists:
            try:
                with os.scandir(font_directory) as entries:
                    if next(entries, None) is not None:
                        raise FontManifestError("缺少字体清单时专用字体目录必须为空")
            except OSError as error:
                raise FontManifestError(f"不能检查无清单字体目录：{font_directory}") from error
        return ()
    except OSError as error:
        raise FontManifestError(f"字体清单无法检查：{manifest_path}") from error
    if stat.S_ISLNK(manifest_metadata.st_mode) or not stat.S_ISREG(manifest_metadata.st_mode):
        raise FontManifestError(f"字体清单必须是普通文件且不能是符号链接：{manifest_path}")

    assets = parse_font_manifest(_read_manifest(manifest_path))
    expected_names = {Path(asset.path).name for asset in assets}
    expected_casefold = {name.casefold() for name in expected_names}
    if len(expected_casefold) != len(expected_names):
        raise FontManifestError("字体清单含有大小写冲突的重复文件名")

    if assets:
        if not assets_directory_exists or not font_directory_exists:
            raise FontManifestError("字体清单引用的assets/fonts目录不存在")
    if font_directory_exists:
        actual_names: set[str] = set()
        try:
            with os.scandir(font_directory) as entries:
                for entry in entries:
                    if entry.is_symlink() or not entry.is_file(follow_symlinks=False):
                        raise FontManifestError(f"字体目录中含符号链接或非文件：{entry.path}")
                    actual_names.add(entry.name)
        except OSError as error:
            raise FontManifestError(f"不能检查字体资产目录：{font_directory}") from error
        if len({name.casefold() for name in actual_names}) != len(actual_names):
            raise FontManifestError("字体目录含有大小写冲突的重复文件名")
        if actual_names != expected_names:
            missing = sorted(expected_names - actual_names)
            extra = sorted(actual_names - expected_names)
            raise FontManifestError(f"字体目录文件集合与清单不符：缺少{missing}，额外{extra}")
    elif expected_names:
        raise FontManifestError("字体清单引用的字体文件缺失")

    for asset in assets:
        _verify_font_file(web_root / asset.path, asset)
    return assets
