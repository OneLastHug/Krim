"""可信工作区配置与可比较OS投影；不从HTTP/模型登记宿主路径或能力。"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import asdict, dataclass
from pathlib import Path

from pydantic import JsonValue

from krim_agent.contracts import ServiceError, encoded
from krim_agent.execution import linux
from krim_agent.execution.types import Profile
from krim_agent.workspace.types import WorkspaceProfile


def environment(profile: Profile) -> dict[str, JsonValue]:
    def regular(path: Path) -> bytes:
        for parent in reversed(path.parents):
            if not stat.S_ISDIR(parent.lstat().st_mode):
                raise ServiceError("workspace_unavailable")
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size > 8388608:
                raise ServiceError("workspace_unavailable")
            with os.fdopen(os.dup(fd), "rb") as stream:
                data = stream.read(8388609)
            if len(data) != info.st_size:
                raise ServiceError("workspace_unavailable")
            return data
        finally:
            os.close(fd)

    def sha(path: Path) -> str:
        return hashlib.sha256(regular(path)).hexdigest()

    base = json.loads(regular(profile.base))
    if base["status"] != "BASE-QUALIFIED" or base["rootfs"]["path"] != str(profile.rootfs):
        raise ServiceError("workspace_unavailable")
    return {"base_sha256": sha(profile.base),
        "rootfs_manifest_sha256": base["rootfs"]["manifest_sha256"],
            "bwrap_sha256": sha(profile.binary), "policy": "python-unittest-x86_64/1",
            "execution": {name: sha(Path(linux.__file__).with_name(name+".py"))
                          for name in ["linux", "types", "cgroups", "seccomp", "_broker"]},
            "limits": {key: value for key, value in asdict(profile).items()
                       if key in {"wall", "cpu_seconds", "memory", "pids", "output"}}}


@dataclass(frozen=True)
class WorkspaceConfiguration:
    profile: WorkspaceProfile
    execution: Profile
    artifact_root: Path

    def __post_init__(self) -> None:
        if ("max_tools_per_step" not in self.profile.model_fields_set
                or not self.artifact_root.is_absolute()
                or self.profile.environment != environment(self.execution)):
            raise ServiceError("workspace_unavailable")

    @classmethod
    def load(cls, path: Path) -> WorkspaceConfiguration:
        try:
            for parent in reversed(path.parents):
                if not stat.S_ISDIR(parent.lstat().st_mode):
                    raise ValueError("配置父路径不是普通目录")
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_size > 65536:
                raise ValueError("配置文件类型/权限/容量")
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                actual = os.fstat(fd)
                if (actual.st_dev, actual.st_ino) != (info.st_dev, info.st_ino):
                    raise ValueError("配置已被替换")
                data = os.read(fd, 65537)
            finally:
                os.close(fd)
            value = json.loads(data)
            if set(value) != {"profile", "execution", "artifact_root"}:
                raise ValueError("配置字段不完整")
            profile = WorkspaceProfile.model_validate_json(encoded(value["profile"]))
            fields = dict(value["execution"])
            for name in ["rootfs", "base", "cgroup_root", "material", "evidence", "binary"]:
                fields[name] = Path(fields[name])
            return cls(profile, Profile(**fields), Path(value["artifact_root"]))
        except (OSError, ValueError, TypeError, KeyError):
            raise ServiceError("workspace_unavailable") from None
