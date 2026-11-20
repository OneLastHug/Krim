"""Shared safe path validation and directory descriptor operations."""

from __future__ import annotations

import os
import stat
import unicodedata
from pathlib import Path

from krim_agent.contracts import ServiceError

_SECRET_PARTS = {"credentials", "id_rsa", "id_ed25519"}


def path_parts(name: str) -> tuple[str, ...]:
    """Validate a bounded relative artifact path, including secret-file exclusions."""
    if type(name) is not str:
        raise ServiceError("artifact_invalid")
    parts = tuple(name.split("/"))
    if (
        len(name.encode()) > 512
        or len(parts) > 16
        or any(
            part in {"", ".", "..", ".git", ".ssh", ".aws", ".codex", ".env"}
            for part in parts
        )
        or any(
            part.startswith(".env.") or part.lower() in _SECRET_PARTS for part in parts
        )
        or "\\" in name
        or unicodedata.normalize("NFC", name) != name
        or any(ord(char) < 32 or ord(char) == 127 for char in name)
    ):
        raise ServiceError("artifact_invalid")
    return parts


def directory_fd(path: Path, *, create: bool = False) -> int:
    """Open a directory from the filesystem root without following path links."""
    if not path.is_absolute():
        raise ServiceError("artifact_invalid")
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for part in path.parts[1:]:
            if create:
                try:
                    os.mkdir(part, 0o700, dir_fd=fd)
                except FileExistsError:
                    pass
            info = os.stat(part, dir_fd=fd, follow_symlinks=False)
            if not stat.S_ISDIR(info.st_mode):
                raise ServiceError("artifact_invalid")
            child = os.open(
                part,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=fd,
            )
            actual = os.fstat(child)
            if (actual.st_ino, actual.st_dev) != (info.st_ino, info.st_dev):
                os.close(child)
                raise ServiceError("artifact_invalid")
            os.close(fd)
            fd = child
        result, fd = fd, -1
        return result
    except OSError:
        raise ServiceError("artifact_invalid") from None
    finally:
        if fd != -1:
            os.close(fd)
