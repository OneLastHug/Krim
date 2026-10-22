"""普通regular文件初态与有界采证；不声称内核级观察。"""

from __future__ import annotations

import hashlib
import os
import stat
from pathlib import Path
from typing import Any

from .domain import ARTIFACT_BYTES, relative_file


def initial_files(directory: Path, files: dict[str, str]) -> dict[str, Any]:
    result = {}
    for name, text in files.items():
        relative_file(name)
        path = directory / name
        path.parent.mkdir(parents=True, exist_ok=True)
        data = text.encode()
        with path.open("xb") as file:
            file.write(data)
        result[name] = {"sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}
    return result


def bounded_files(directory: Path) -> dict[str, Any]:
    """lstat后用同一目录FD打开；不跟随中间目录或叶链接。"""
    result: dict[str, Any] = {}
    total = 0
    entries = 0
    root_stat = directory.lstat()
    root_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)

    def visit(fd: int, prefix: str, depth: int, expected: os.stat_result) -> None:
        nonlocal total, entries
        current = os.fstat(fd)
        if (
            not stat.S_ISDIR(expected.st_mode)
            or depth > 8
            or (current.st_dev, current.st_ino) != (expected.st_dev, expected.st_ino)
        ):
            raise ValueError("artifact_invalid")
        for name in sorted(os.listdir(fd)):
            entries += 1
            if entries > 256:
                raise ValueError("artifact_limit")
            st = os.stat(name, dir_fd=fd, follow_symlinks=False)
            relative = prefix + name
            if stat.S_ISDIR(st.st_mode):
                child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                try:
                    visit(child, relative + "/", depth + 1, st)
                finally:
                    os.close(child)
                continue
            if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1:
                raise ValueError("artifact_invalid")
            total += st.st_size
            if total > ARTIFACT_BYTES or len(result) >= 16:
                raise ValueError("artifact_limit")
            leaf = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
            try:
                live = os.fstat(leaf)
                if (
                    not stat.S_ISREG(live.st_mode)
                    or (live.st_dev, live.st_ino) != (st.st_dev, st.st_ino)
                    or live.st_nlink != 1
                ):
                    raise ValueError("artifact_invalid")
                with os.fdopen(leaf, "rb", closefd=False) as source:
                    data = source.read(st.st_size + 1)
                after = os.fstat(leaf)
                if (
                    len(data) != st.st_size
                    or after.st_mtime_ns != st.st_mtime_ns
                    or after.st_ctime_ns != st.st_ctime_ns
                ):
                    raise ValueError("artifact_invalid")
            finally:
                os.close(leaf)
            result[relative] = {
                "sha256": hashlib.sha256(data).hexdigest(),
                "bytes": len(data),
                "text": data.decode("utf-8", errors="strict"),
            }

    try:
        visit(root_fd, "", 0, root_stat)
    finally:
        os.close(root_fd)
    return result


def bounded_roots(
    directory: Path,
    roots: tuple[str, ...] = ("ledger", "tests", "outputs"),
) -> dict[str, Any]:
    """显式根的合并仍服从原16文件/1MiB总边界，绝不放宽原FD采证。"""
    result: dict[str, Any] = {}
    total = 0
    for root in roots:
        relative_file(root)
        for name, item in bounded_files(directory / root).items():
            total += item["bytes"]
            if total > ARTIFACT_BYTES or len(result) >= 16:
                raise ValueError("artifact_limit")
            result[root + "/" + name] = item
    return result
