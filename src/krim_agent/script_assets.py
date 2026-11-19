"""按构建清单逐名注册按需加载脚本，不公开资源目录。"""
from __future__ import annotations

import hashlib
import re
import stat
from dataclasses import dataclass
from pathlib import Path

from krim_agent.contracts import parse_json

_SCRIPT_PATH = re.compile(r"assets/[A-Za-z0-9][A-Za-z0-9._-]{0,127}\.js\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_MAX_MANIFEST = 65536
_MAX_SCRIPT = 32 * 1024 * 1024


@dataclass(frozen=True)
class ScriptAsset:
    path: str
    size_bytes: int
    sha256: str


def _regular(path: Path, *, directory: bool = False) -> None:
    info = path.lstat()
    kind = stat.S_ISDIR if directory else stat.S_ISREG
    if stat.S_ISLNK(info.st_mode) or not kind(info.st_mode):
        raise ValueError("脚本资源必须是实际目录中的普通文件")


def load_script_assets(root: Path) -> tuple[ScriptAsset, ...]:
    """旧单bundle发行版可无清单；有清单时验证路径、长度、摘要。"""
    root = Path(root)
    _regular(root, directory=True)
    manifest = root / "script-manifest.json"
    try:
        info = manifest.lstat()
    except FileNotFoundError:
        return ()
    _regular(manifest)
    if info.st_size > _MAX_MANIFEST:
        raise ValueError("脚本清单过大")
    value = parse_json(manifest.read_text(encoding="utf-8"))
    if (not isinstance(value, dict) or set(value) != {"version", "scripts"}
            or type(value["version"]) is not int or value["version"] != 1):
        raise ValueError("脚本清单版本或字段无效")
    rows = value["scripts"]
    if not isinstance(rows, list) or len(rows) > 64:
        raise ValueError("脚本清单必须为有限列表")
    if rows:
        _regular(root / "assets", directory=True)
    assets = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"path", "size_bytes", "sha256"}:
            raise ValueError("脚本清单条目字段无效")
        path, size, digest = row["path"], row["size_bytes"], row["sha256"]
        if (not isinstance(path, str) or not _SCRIPT_PATH.fullmatch(path)
                or ".." in path or path.casefold() in seen
                or type(size) is not int or not 0 < size <= _MAX_SCRIPT
                or not isinstance(digest, str) or not _DIGEST.fullmatch(digest)):
            raise ValueError("脚本清单条目无效")
        seen.add(path.casefold())
        file = root / path
        _regular(file)
        if file.stat().st_size != size or hashlib.sha256(file.read_bytes()).hexdigest() != digest:
            raise ValueError("脚本资产与构建清单不符")
        assets.append(ScriptAsset(path, size, digest))
    return tuple(assets)
