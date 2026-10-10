"""有界SKILL.md文件包解析；只读数据，不跟随链接、不解包或执行脚本。"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import stat
import zipfile
from pathlib import Path
from typing import cast

import yaml
from pydantic import Field

from .protocol.domain import ServiceError
from .protocol.models import StrictModel
from .skills import SkillDefinition, SkillResource, resource_path

MAX_ARCHIVE_BYTES = 45000
MAX_PACKAGE_BYTES = 262144
MAX_FILE_BYTES = 32768
MAX_FILES = 17
MAX_ENTRIES = 64
FIELDS = {"name", "description", "license", "compatibility", "metadata", "allowed-tools"}
RESERVED = {"krim-package-sha256", "krim-package-format", "krim-package-source"}


def _stamp(info: os.stat_result) -> tuple[int, ...]:
    # 自身读取可能改变atime；一致性保护关注内容、身份和授权属性。
    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_uid,
        info.st_gid,
        info.st_nlink,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


class SkillPackageRequest(StrictModel):
    archive_base64: str = Field(min_length=1, max_length=60000)
    expected_digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


def invalid(code: str, message: str) -> ServiceError:
    return ServiceError("skill_package_" + code, message, 422)


def _path(value: str) -> str:
    if value == "SKILL.md":
        return value
    try:
        return resource_path(value)
    except ValueError:
        raise invalid("path", "文件包路径须为不重复的受限相对键") from None


def _frontmatter(raw: bytes) -> tuple[dict[str, object], str]:
    if len(raw) > MAX_FILE_BYTES or b"\0" in raw:
        raise invalid("text", "SKILL.md须为有界UTF-8文本")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise invalid("text", "SKILL.md须为UTF-8文本") from None
    lines = text.splitlines(keepends=True)
    if not lines or lines[0].strip() != "---":
        raise invalid("manifest", "SKILL.md缺少YAML frontmatter")
    end = next((i for i in range(1, len(lines)) if lines[i].strip() == "---"), None)
    if end is None:
        raise invalid("manifest", "SKILL.md frontmatter未闭合")
    header = "".join(lines[1:end])
    if len(header.encode()) > 8192:
        raise invalid("manifest", "技能frontmatter超过容量")
    try:
        for token in yaml.scan(header):
            if isinstance(
                token, (yaml.tokens.AnchorToken, yaml.tokens.AliasToken, yaml.tokens.TagToken)
            ):
                raise invalid("yaml", "技能YAML不接受tag、anchor或alias")
        depth = 0
        events = 0
        for event in yaml.parse(header):
            events += 1
            if isinstance(event, (yaml.events.MappingStartEvent, yaml.events.SequenceStartEvent)):
                depth += 1
            elif isinstance(event, (yaml.events.MappingEndEvent, yaml.events.SequenceEndEvent)):
                depth -= 1
            if depth > 8 or events > 512:
                raise invalid("yaml", "技能YAML结构超过边界")
        node = yaml.compose(header, Loader=yaml.BaseLoader)
        if not isinstance(node, yaml.nodes.MappingNode):
            raise invalid("manifest", "技能frontmatter须为映射")
        pending: list[tuple[yaml.nodes.Node, int]] = [(node, 0)]
        count = 0
        while pending:
            current, depth = pending.pop()
            count += 1
            if depth > 8 or count > 256:
                raise invalid("yaml", "技能YAML结构超过边界")
            if isinstance(current, yaml.nodes.MappingNode):
                keys = []
                for key, child_value in current.value:
                    if not isinstance(key, yaml.nodes.ScalarNode) or key.value == "<<":
                        raise invalid("yaml", "技能YAML映射键无效")
                    keys.append(key.value)
                    pending.append((child_value, depth + 1))
                if len(keys) != len(set(keys)):
                    raise invalid("yaml", "技能YAML字段重复")
            elif isinstance(current, yaml.nodes.SequenceNode):
                pending.extend((child, depth + 1) for child in current.value)
        value: object = yaml.load(header, Loader=yaml.BaseLoader)
    except yaml.YAMLError:
        raise invalid("yaml", "技能YAML无效") from None
    if not isinstance(value, dict) or set(value) - FIELDS:
        raise invalid("manifest", "技能frontmatter包含未知字段")
    return cast(dict[str, object], value), "".join(lines[end + 1 :])


def package_from_files(files: dict[str, bytes], *, source: str) -> SkillDefinition:
    """已读取的包数据进入既有SkillDefinition；正文、许可和资源不执行。"""
    if not 1 <= len(files) <= MAX_FILES or "SKILL.md" not in files:
        raise invalid("files", "文件包须含唯一SKILL.md和最多16个资源")
    if sum(len(raw) for raw in files.values()) > MAX_PACKAGE_BYTES:
        raise invalid("limit", "文件包展开字节超过容量")
    for file_name, raw in files.items():
        _path(file_name)
        if len(file_name.split("/")) > 8 or len(raw) > MAX_FILE_BYTES or b"\0" in raw:
            raise invalid("limit", "文件包路径/文件类型或容量超过边界")
    header, content = _frontmatter(files["SKILL.md"])
    name, description = header.get("name"), header.get("description")
    if not isinstance(name, str) or not isinstance(description, str):
        raise invalid("manifest", "技能须提供name与description")
    metadata = header.get("metadata", {})
    if (
        not isinstance(metadata, dict)
        or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in metadata.items()
        )
        or set(metadata) & RESERVED
    ):
        raise invalid("manifest", "技能metadata须为字符串映射且不能覆盖包来源字段")
    allowed = header.get("allowed-tools", "")
    if isinstance(allowed, str):
        tools = allowed.split()
    elif isinstance(allowed, list) and all(isinstance(item, str) for item in allowed):
        tools = cast(list[str], allowed)
    else:
        raise invalid("manifest", "allowed-tools须为有限字符串或字符串数组")
    entries = [
        {"path": path, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
        for path, raw in sorted(files.items())
    ]
    digest = hashlib.sha256(json.dumps(entries, sort_keys=True).encode()).hexdigest()
    values = cast(dict[str, str], metadata).copy()
    values.update(
        {
            "krim-package-sha256": digest,
            "krim-package-format": "agent-skills-v1",
            "krim-package-source": source,
        }
    )
    try:
        resources = [
            SkillResource(path=path, content=raw.decode("utf-8"))
            for path, raw in sorted(files.items())
            if path != "SKILL.md"
        ]
        return SkillDefinition.model_validate(
            {
                "name": name,
                "description": description,
                "content": content,
                "license": header.get("license"),
                "compatibility": header.get("compatibility"),
                "metadata": values,
                "allowed_tools": tools,
                "resources": [item.model_dump() for item in resources],
            }
        )
    except (ValueError, UnicodeDecodeError):
        raise invalid("definition", "技能正文、资源或元数据不符合现有容量/类型边界") from None


def package_from_archive(raw: bytes) -> SkillDefinition:
    if not 1 <= len(raw) <= MAX_ARCHIVE_BYTES:
        raise invalid("limit", "技能ZIP须为1至45000字节")
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            entries = archive.infolist()
            if not 1 <= len(entries) <= MAX_ENTRIES:
                raise invalid("files", "技能ZIP条目数量超过边界")
            files: dict[str, bytes] = {}
            seen: set[str] = set()
            total = 0
            for item in entries:
                name = item.filename[:-1] if item.is_dir() else item.filename
                _path(name)
                if name in seen:
                    raise invalid("files", "技能ZIP条目键重复")
                seen.add(name)
                mode = item.external_attr >> 16
                kind = stat.S_IFMT(mode)
                if item.flag_bits & 1 or kind not in {0, stat.S_IFREG, stat.S_IFDIR}:
                    raise invalid("file_type", "技能ZIP不能加密或含链接/特殊文件")
                if item.is_dir():
                    continue
                if name in files or item.file_size > MAX_FILE_BYTES:
                    raise invalid("files", "技能ZIP文件重复或超过单文件容量")
                if item.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}:
                    raise invalid("compression", "技能ZIP只接受store/deflate")
                with archive.open(item) as stream:
                    content = stream.read(MAX_FILE_BYTES + 1)
                total += len(content)
                if len(content) != item.file_size or total > MAX_PACKAGE_BYTES:
                    raise invalid("limit", "技能ZIP展开内容不完整或超过容量")
                files[name] = content
            return package_from_files(files, source="zip")
    except (zipfile.BadZipFile, RuntimeError, OSError, ValueError):
        raise invalid("archive", "技能ZIP无效或不能完整读取") from None


def package_from_directory(path: str | Path) -> SkillDefinition:
    """仅可信宿主读取指定包根；远端请求不得调用本地路径接口。"""
    root = Path(path)
    if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
        raise ServiceError("skill_package_platform", "当前平台尚未验证包目录读取，仍可导入ZIP", 503)
    files: dict[str, bytes] = {}
    scanned = 0
    total = 0

    def read(fd: int, prefix: str, depth: int) -> None:
        nonlocal scanned, total
        before = os.fstat(fd)
        if depth > 8:
            raise invalid("limit", "技能目录深度超过边界")
        with os.scandir(fd) as iterator:
            for entry in iterator:
                scanned += 1
                name = prefix + entry.name
                _path(name)
                if scanned > MAX_ENTRIES:
                    raise invalid("limit", "技能目录条目超过容量")
                info = entry.stat(follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    child = os.open(
                        entry.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd
                    )
                    try:
                        read(child, name + "/", depth + 1)
                    finally:
                        os.close(child)
                elif stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
                    if len(files) >= MAX_FILES or info.st_size > MAX_FILE_BYTES:
                        raise invalid("limit", "技能目录文件数量/大小超过容量")
                    child = os.open(
                        entry.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd
                    )
                    try:
                        initial = os.fstat(child)
                        if not stat.S_ISREG(initial.st_mode) or initial.st_nlink != 1:
                            raise invalid("file_type", "技能目录只接受普通单链接文件")
                        content = bytearray()
                        while len(content) <= MAX_FILE_BYTES:
                            part = os.read(child, min(65536, MAX_FILE_BYTES + 1 - len(content)))
                            if not part:
                                break
                            content.extend(part)
                        final = os.fstat(child)
                        if _stamp(initial) != _stamp(final) or len(content) != final.st_size:
                            raise invalid("changed", "技能文件读取期间变化")
                        total += len(content)
                        if total > MAX_PACKAGE_BYTES:
                            raise invalid("limit", "技能目录展开内容超过容量")
                        files[name] = bytes(content)
                    finally:
                        os.close(child)
                else:
                    raise invalid("file_type", "技能目录拒绝链接/硬链接/特殊文件")
        if _stamp(before) != _stamp(os.fstat(fd)):
            raise invalid("changed", "技能目录读取期间变化")

    try:
        fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            read(fd, "", 0)
        finally:
            os.close(fd)
    except OSError:
        raise invalid("directory", "技能包根目录不可读取或含链接") from None
    result = package_from_files(files, source="directory")
    if result.name != root.name:
        raise invalid("name", "包目录名须与SKILL.md中的name一致")
    return result


def decode_archive(request: SkillPackageRequest) -> bytes:
    try:
        raw = base64.b64decode(request.archive_base64, validate=True)
    except ValueError:
        raise invalid("archive", "技能ZIP须为有效Base64") from None
    if len(raw) > MAX_ARCHIVE_BYTES:
        raise invalid("limit", "技能ZIP超过45000字节")
    return raw
