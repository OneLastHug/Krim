"""在可信装配时冻结源码指纹，流事件只复用相同来源摘要。"""

import hashlib
import os
from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=128)
def _read_digest(path: str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def source_digest(path: str | os.PathLike[str]) -> str:
    """代码更新须重新装配；本函数不用于用户文件或执行权限。"""
    return _read_digest(os.path.abspath(os.fspath(path)))


def reset_source_fingerprints() -> None:
    """新 Application 核对当前发行来源，不继承旧装配的文件摘要。"""
    _read_digest.cache_clear()
