"""目录成果私有清单：旧记录能力缺失、完整性、容量和摘要均显式保留。"""

from __future__ import annotations

import hashlib
import json
from typing import Literal

from pydantic import Field, JsonValue, ValidationError, model_validator

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.workspace import Workspace

from .models import ExecutionDirectory, SandboxLimits
from .records import CommandDirectory

MAX_DIRECTORY_METADATA_BYTES = 2_097_152


def directory_metadata_limit(limits: SandboxLimits) -> int:
    return min(limits.artifact_bytes, MAX_DIRECTORY_METADATA_BYTES)


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()


class DirectoryManifest(StrictModel):
    version: Literal[1] = 1
    complete: bool
    entries: list[CommandDirectory] = Field(max_length=16384)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def checksum(self) -> DirectoryManifest:
        if self.sha256 != _digest(self.model_dump(mode="json", exclude={"sha256"})):
            raise ValueError("目录来源摘要与集合不匹配")
        return self


class CommandDirectoryCatalog(StrictModel):
    command_id: str
    recorded: bool
    complete: bool
    entries: list[CommandDirectory] = Field(max_length=16384)
    source_hash: str | None = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def facts(self) -> CommandDirectoryCatalog:
        if not self.recorded and (self.complete or self.entries):
            raise ValueError("未采集目录不能声明完整或提供目录条目")
        if self.complete and self.source_hash is None:
            raise ValueError("完整目录必须绑定原命令来源")
        return self


def freeze_directory_manifest(
    directories: tuple[ExecutionDirectory, ...] | None,
    files: set[str],
    limits: SandboxLimits,
    *,
    complete: bool,
) -> dict[str, JsonValue] | None:
    if directories is None:
        return None
    if not isinstance(directories, tuple) or type(complete) is not bool:
        raise ServiceError("execution_artifact_invalid", "目录成果类型或完整性无效", 409)
    entries: list[CommandDirectory] = []
    paths: set[str] = set()
    for item in directories:
        if (
            not isinstance(item, ExecutionDirectory)
            or not isinstance(item.path, str)
            or type(item.mode) is not int
            or not 0 <= item.mode <= 0o777
        ):
            raise ServiceError("execution_artifact_invalid", "目录成果内容或普通模式无效", 409)
        try:
            parts = Workspace._parts(item.path)
        except ServiceError:
            raise ServiceError("execution_artifact_invalid", "目录成果路径无效", 409) from None
        if not parts or item.path in paths or item.path in files:
            raise ServiceError("execution_artifact_invalid", "目录重复、与文件冲突或路径无效", 409)
        paths.add(item.path)
        entries.append(CommandDirectory(path=item.path, mode=item.mode))
    if len(paths) + len(files) > limits.artifact_files * 4:
        raise ServiceError("execution_directory_limit", "目录和文件扫描数量超过原上限", 409)
    if complete:
        for path in paths | files:
            parents = path.split("/")[:-1]
            if any("/".join(parents[:end]) not in paths for end in range(1, len(parents) + 1)):
                raise ServiceError("execution_artifact_invalid", "完整成果缺少真实父目录", 409)
    encoded: list[JsonValue] = [
        entry.model_dump(mode="json") for entry in sorted(entries, key=lambda entry: entry.path)
    ]
    # 与SQLAlchemy默认JSON序列化相同，非ASCII路径转义也计入持久容量。
    if len(json.dumps(encoded).encode("utf-8")) > directory_metadata_limit(limits):
        raise ServiceError("execution_directory_limit", "目录元数据超过容量上限", 409)
    value: dict[str, JsonValue] = {"version": 1, "complete": complete, "entries": encoded}
    value["sha256"] = _digest(value)
    return value


def read_directory_manifest(
    result: dict[str, JsonValue] | None, files: set[str], limits: SandboxLimits
) -> DirectoryManifest | None:
    if result is None or "directory_manifest" not in result:
        return None
    try:
        manifest = DirectoryManifest.model_validate(result["directory_manifest"])
        if manifest.complete is not result.get("artifacts_complete"):
            raise ValueError("complete")
        expected = freeze_directory_manifest(
            tuple(ExecutionDirectory(item.path, item.mode) for item in manifest.entries),
            files,
            limits,
            complete=manifest.complete,
        )
        if expected != result["directory_manifest"]:
            raise ValueError("manifest")
        return manifest
    except (ValidationError, ValueError, TypeError, ServiceError):
        raise ServiceError("execution_record_corrupt", "命令目录来源校验失败", 409) from None
