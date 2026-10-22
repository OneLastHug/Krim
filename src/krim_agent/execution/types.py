"""纯OS有界类型；不包含业务身份、Artifact或数据库。"""

import unicodedata
from dataclasses import dataclass, field
from pathlib import Path

from krim_agent.contracts import ServiceError


def path_parts(name: str) -> tuple[str, ...]:
    if type(name) is not str:
        raise ServiceError("artifact_invalid")
    parts = tuple(name.split("/"))
    if (
        not isinstance(name, str)
        or len(name.encode()) > 512
        or len(parts) > 16
        or any(p in {"", ".", "..", ".git", ".ssh", ".aws", ".codex", ".env"} for p in parts)
        or "\\" in name
        or unicodedata.normalize("NFC", name) != name
        or any(ord(c) < 32 or ord(c) == 127 for c in name)
    ):
        raise ServiceError("artifact_invalid")
    return parts


@dataclass(frozen=True)
class Profile:
    rootfs: Path
    base: Path
    cgroup_root: Path
    material: Path
    evidence: Path
    wall: float = 20.0
    cpu_seconds: float = 2.0
    memory: int = 134217728
    pids: int = 16
    output: int = 262144
    binary: Path = Path("/usr/local/bin/bwrap")

    def __post_init__(self):
        if any(
            not isinstance(p, Path) or not p.is_absolute()
            for p in [
                self.rootfs,
                self.base,
                self.cgroup_root,
                self.material,
                self.evidence,
                self.binary,
            ]
        ):
            raise ServiceError("sandbox_unavailable")
        if (
            type(self.wall) not in (int, float)
            or type(self.cpu_seconds) not in (int, float)
            or not 0 < self.wall <= 20
            or not 0 < self.cpu_seconds <= 2
            or type(self.memory) is not int
            or not 33554432 <= self.memory <= 134217728
            or type(self.pids) is not int
            or not 4 <= self.pids <= 16
            or type(self.output) is not int
            or not 1 <= self.output <= 262144
        ):
            raise ServiceError("sandbox_unavailable")


@dataclass(frozen=True)
class Request:
    argv: tuple[str, ...]
    files: dict[str, bytes] = field(default_factory=dict)
    cwd: str = "."

    def __post_init__(self):
        if (
            type(self.argv) is not tuple
            or not 1 <= len(self.argv) <= 32
            or any(type(x) is not str or "\0" in x for x in self.argv)
            or sum(len(x.encode()) for x in self.argv) > 8192
            or type(self.files) is not dict
            or len(self.files) > 32
        ):
            raise ServiceError("invalid_input")
        for name, data in self.files.items():
            path_parts(name)
            if type(data) is not bytes or len(data) > 1048576:
                raise ServiceError("artifact_limit")
        if sum(len(x) for x in self.files.values()) > 8388608:
            raise ServiceError("artifact_limit")
        if self.cwd != ".":
            path_parts(self.cwd)


@dataclass(frozen=True)
class Receipt:
    exit_code: int | None
    stdout: bytes
    stderr: bytes
    files: dict[str, bytes]
    output_complete: bool
    cleanup: str
    code: str | None
    evidence: str
