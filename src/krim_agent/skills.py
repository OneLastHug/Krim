"""Skill 解析与目录值类型：SKILL.md 纯解析，不执行、不读宿主文件系统。

SKILL.md 采用通行 Agent Skills 格式：YAML frontmatter 必需 ``name`` 与
``description``，可选 ``license``、``compatibility`` 与字符串 ``metadata``；
未知扩展字段一律忽略，不影响 content_hash。frontmatter 由收紧的 PyYAML
SafeLoader 解析：禁止别名、显式标签、合并键、非字符串键与重复键，歧义
输入明确报错。资源路径必须是安全的 POSIX 相对文件名，所有内容字节受限。
本模块产出的是不可信文本：上层注入上下文时须按不可信内容标注，且不得
借此改变工具权限。依赖 ``pyyaml``（需加入 pyproject 依赖）。
"""

from __future__ import annotations

import unicodedata
from collections.abc import Hashable
from typing import Any, Self, cast

import yaml
from pydantic import Field, ValidationError, field_validator, model_validator
from yaml.error import YAMLError
from yaml.events import AliasEvent

from krim_agent.contracts import StrictModel, fingerprint

# 导入契约限额；调整任何常量都会改变校验边界与 content_hash 语义，须同步目录版本。
SKILL_NAME_MAX_CHARS = 64
SKILL_DESCRIPTION_MAX_CHARS = 1024
SKILL_BODY_MAX_BYTES = 32 * 1024
SKILL_TOTAL_MAX_BYTES = 64 * 1024
SKILL_RESOURCE_PATH_MAX_CHARS = 256
SKILL_RESOURCE_MAX_BYTES = 32 * 1024
SKILL_RESOURCES_TOTAL_MAX_BYTES = 32 * 1024
SKILL_RESOURCE_MAX_COUNT = 32
SKILL_METADATA_MAX_ENTRIES = 16
SKILL_METADATA_KEY_MAX_CHARS = 64
SKILL_METADATA_VALUE_MAX_CHARS = 1024
SKILL_LICENSE_MAX_CHARS = 256
SKILL_COMPATIBILITY_MAX_CHARS = 500

_SKILL_HASH_SCHEMA = "krim-skill-document/1"
_NAME_PATTERN = r"^[a-z0-9]+(-[a-z0-9]+)*$"
_HASH_PATTERN = r"^[0-9a-f]{64}$"


class SkillParseError(ValueError):
    """SKILL.md 文档或资源无法安全解析；消息面向操作者，可直接展示。"""


def _utf8_size(text: str) -> int:
    try:
        return len(text.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise SkillParseError("文本包含无法编码为 UTF-8 的字符") from exc


def _validate_resource_path(path: str) -> None:
    """只接受安全的 POSIX 相对文件名：拒绝遍历、绝对、反斜杠、编码与不可见字符。"""
    if not isinstance(path, str) or not path or len(path) > SKILL_RESOURCE_PATH_MAX_CHARS:
        raise SkillParseError("资源路径为空、非字符串或超长")
    if "\\" in path:
        raise SkillParseError("资源路径不允许反斜杠")
    if "%" in path:
        raise SkillParseError("资源路径不允许百分号编码")
    if ":" in path:
        raise SkillParseError("资源路径不允许冒号")
    _utf8_size(path)
    if path.startswith("/") or path.endswith("/"):
        raise SkillParseError("资源路径必须是相对文件路径")
    for char in path:
        if unicodedata.category(char) in ("Cc", "Cf"):
            raise SkillParseError("资源路径包含控制或不可见格式字符")
    for part in path.split("/"):
        if part in ("", ".", ".."):
            raise SkillParseError("资源路径包含空段或相对跳转段")


def _validate_resource_content(content: str) -> int:
    """资源内容必须为有界文本；返回 UTF-8 字节数。"""
    if not isinstance(content, str):
        raise SkillParseError("资源内容必须为文本")
    if "\x00" in content:
        raise SkillParseError("资源内容包含 NUL 字符")
    size = _utf8_size(content)
    if size > SKILL_RESOURCE_MAX_BYTES:
        raise SkillParseError(f"单个资源字节超过 {SKILL_RESOURCE_MAX_BYTES}")
    return size


def _validate_resources(resources: dict[str, str]) -> None:
    if not isinstance(resources, dict) or any(not isinstance(key, str) for key in resources):
        raise SkillParseError("resources 必须为字符串映射")
    if len(resources) > SKILL_RESOURCE_MAX_COUNT:
        raise SkillParseError(f"资源数量超过 {SKILL_RESOURCE_MAX_COUNT}")
    seen: set[str] = set()
    total = 0
    for path, content in resources.items():
        _validate_resource_path(path)
        total += _validate_resource_content(content)
        key = unicodedata.normalize("NFC", path).casefold()
        if key in seen:
            raise SkillParseError("资源路径存在大小写或 Unicode 规范化歧义")
        seen.add(key)
    if total > SKILL_RESOURCES_TOTAL_MAX_BYTES:
        raise SkillParseError(f"资源总字节超过 {SKILL_RESOURCES_TOTAL_MAX_BYTES}")


def _validate_metadata(metadata: dict[str, str]) -> None:
    if len(metadata) > SKILL_METADATA_MAX_ENTRIES:
        raise SkillParseError(f"metadata 条目数超过 {SKILL_METADATA_MAX_ENTRIES}")
    for key, value in metadata.items():
        if not isinstance(key, str) or not key or len(key) > SKILL_METADATA_KEY_MAX_CHARS:
            raise SkillParseError("metadata 键为空、非字符串或超长")
        if not isinstance(value, str) or len(value) > SKILL_METADATA_VALUE_MAX_CHARS:
            raise SkillParseError("metadata 值必须为不超过上限的字符串")
        _utf8_size(key)
        _utf8_size(value)
        if "\x00" in key or "\x00" in value:
            raise SkillParseError("metadata 包含 NUL 字符")


class _StrictYamlLoader(yaml.SafeLoader):
    """SafeLoader 收紧：拒绝别名、显式标签、合并键、非字符串键与重复键。"""

    def compose_node(self, parent: Any, index: Any) -> Any:
        event = cast(Any, self).peek_event()
        if isinstance(event, AliasEvent):
            raise SkillParseError("YAML 别名不受支持")
        if event is not None and getattr(event, "tag", None) is not None:
            raise SkillParseError("YAML 显式标签不受支持")
        return super().compose_node(parent, index)

    def construct_mapping(self, node: Any, deep: bool = False) -> dict[Hashable, Any]:
        mapping: dict[str, Any] = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if not isinstance(key, str):
                raise SkillParseError("YAML 键必须为字符串")
            if key == "<<":
                raise SkillParseError("YAML 合并键不受支持")
            if key in mapping:
                raise SkillParseError("YAML 重复键")
            mapping[key] = self.construct_object(value_node, deep=deep)
        return cast(dict[Hashable, Any], mapping)


def _canonical_hash(
    *,
    name: str,
    description: str,
    license: str | None,
    compatibility: str | None,
    body: str,
    resources: dict[str, str],
    metadata: dict[str, str],
) -> str:
    """对规范化已知内容做指纹；键排序由 contracts.encoded 统一处理。"""
    payload: dict[str, Any] = {
        "schema": _SKILL_HASH_SCHEMA,
        "name": name,
        "description": description,
        "license": license,
        "compatibility": compatibility,
        "body": body,
        "resources": resources,
        "metadata": metadata,
    }
    return fingerprint(payload)


class SkillDocument(StrictModel):
    """已验证的单个 Skill；content_hash 绑定全部已知内容字段。"""

    name: str = Field(min_length=1, max_length=SKILL_NAME_MAX_CHARS, pattern=_NAME_PATTERN)
    description: str = Field(min_length=1, max_length=SKILL_DESCRIPTION_MAX_CHARS)
    license: str | None = Field(default=None, min_length=1, max_length=SKILL_LICENSE_MAX_CHARS)
    compatibility: str | None = Field(
        default=None, min_length=1, max_length=SKILL_COMPATIBILITY_MAX_CHARS
    )
    body: str
    resources: dict[str, str]
    metadata: dict[str, str] = Field(default_factory=dict)
    content_hash: str = Field(pattern=_HASH_PATTERN)

    @field_validator("description")
    @classmethod
    def _clean_required_text(cls, value: str) -> str:
        _utf8_size(value)
        if "\x00" in value:
            raise ValueError("文本字段包含 NUL 字符")
        return value

    @field_validator("license", "compatibility")
    @classmethod
    def _clean_optional_text(cls, value: str | None) -> str | None:
        if value is not None:
            _utf8_size(value)
            if "\x00" in value:
                raise ValueError("文本字段包含 NUL 字符")
        return value

    @field_validator("description")
    @classmethod
    def _meaningful_description(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("description 不能为空白")
        return value

    @field_validator("body")
    @classmethod
    def _bounded_body(cls, value: str) -> str:
        if _utf8_size(value) > SKILL_BODY_MAX_BYTES:
            raise ValueError(f"Skill 正文字节超过 {SKILL_BODY_MAX_BYTES}")
        return value

    @field_validator("resources")
    @classmethod
    def _bounded_resources(cls, value: dict[str, str]) -> dict[str, str]:
        _validate_resources(value)
        return value

    @field_validator("metadata")
    @classmethod
    def _bounded_metadata(cls, value: dict[str, str]) -> dict[str, str]:
        _validate_metadata(value)
        return value

    @model_validator(mode="after")
    def _bound_content(self) -> Self:
        expected = _canonical_hash(
            name=self.name,
            description=self.description,
            license=self.license,
            compatibility=self.compatibility,
            body=self.body,
            resources=self.resources,
            metadata=self.metadata,
        )
        if self.content_hash != expected:
            raise ValueError("content_hash 与 Skill 内容不一致")
        return self


class SkillSummary(StrictModel):
    """目录索引值：未选择 Skill 时只披露名称、描述与内容指纹。"""

    name: str = Field(min_length=1, max_length=SKILL_NAME_MAX_CHARS, pattern=_NAME_PATTERN)
    description: str = Field(min_length=1, max_length=SKILL_DESCRIPTION_MAX_CHARS)
    content_hash: str = Field(pattern=_HASH_PATTERN)

    @field_validator("description")
    @classmethod
    def _meaningful_description(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("description 不能为空白")
        return value

    @classmethod
    def from_document(cls, document: SkillDocument) -> SkillSummary:
        return cls(
            name=document.name,
            description=document.description,
            content_hash=document.content_hash,
        )


def _split_frontmatter(document: str) -> tuple[str, str]:
    """按首个 ``---`` 行与下一个独立 ``---`` 行切分；返回 YAML 文本与正文。"""
    if not document.startswith("---") or (len(document) > 3 and document[3] not in ("\n", "\r")):
        raise SkillParseError("文档必须以 --- frontmatter 起始")
    start = 4
    cursor = start
    while True:
        newline = document.find("\n", cursor)
        line_end = newline if newline != -1 else len(document)
        if document[cursor:line_end].rstrip("\r") == "---":
            frontmatter = document[start:cursor]
            body = "" if newline == -1 else document[newline + 1 :]
            return frontmatter, body
        if newline == -1:
            raise SkillParseError("frontmatter 未闭合")
        cursor = newline + 1


def _parse_frontmatter(text: str) -> dict[str, Any]:
    try:
        data = yaml.load(text, Loader=_StrictYamlLoader)
    except YAMLError as exc:
        raise SkillParseError("frontmatter 不是有效或无歧义的 YAML") from exc
    except RecursionError as exc:
        raise SkillParseError("frontmatter 嵌套过深") from exc
    if not isinstance(data, dict):
        raise SkillParseError("frontmatter 为空或不是字符串映射")
    return data


def _required_str(frontmatter: dict[str, Any], key: str) -> str:
    value = frontmatter.get(key)
    if value is None:
        raise SkillParseError(f"frontmatter 缺少 {key}")
    if not isinstance(value, str):
        raise SkillParseError(f"{key} 必须为字符串")
    return value


def _optional_str(frontmatter: dict[str, Any], key: str) -> str | None:
    if key not in frontmatter:
        return None
    value = frontmatter[key]
    if not isinstance(value, str):
        raise SkillParseError(f"{key} 必须为字符串")
    return value


def _metadata_map(frontmatter: dict[str, Any]) -> dict[str, str]:
    if "metadata" not in frontmatter:
        return {}
    value = frontmatter["metadata"]
    if not isinstance(value, dict):
        raise SkillParseError("metadata 必须为字符串映射")
    metadata = dict(value)
    _validate_metadata(metadata)
    return metadata


def parse_skill(document: str, resources: dict[str, str] | None = None) -> SkillDocument:
    """解析 SKILL.md 文本与可选资源表；失败抛 SkillParseError，不做任何 IO。"""
    if not isinstance(document, str):
        raise SkillParseError("document 必须为字符串")
    if resources is None:
        resource_map: dict[str, str] = {}
    elif isinstance(resources, dict):
        resource_map = dict(resources)
    else:
        raise SkillParseError("resources 必须为字符串映射")
    if "\x00" in document:
        raise SkillParseError("文档包含 NUL 字符")
    _validate_resources(resource_map)
    total = _utf8_size(document) + sum(_utf8_size(item) for item in resource_map.values())
    if total > SKILL_TOTAL_MAX_BYTES:
        raise SkillParseError(f"文档与资源总字节超过 {SKILL_TOTAL_MAX_BYTES}")
    frontmatter_text, body = _split_frontmatter(document)
    frontmatter = _parse_frontmatter(frontmatter_text)
    name = _required_str(frontmatter, "name")
    description = _required_str(frontmatter, "description")
    skill_license = _optional_str(frontmatter, "license")
    compatibility = _optional_str(frontmatter, "compatibility")
    metadata = _metadata_map(frontmatter)
    content_hash = _canonical_hash(
        name=name,
        description=description,
        license=skill_license,
        compatibility=compatibility,
        body=body,
        resources=resource_map,
        metadata=metadata,
    )
    try:
        return SkillDocument(
            name=name,
            description=description,
            license=skill_license,
            compatibility=compatibility,
            body=body,
            resources=resource_map,
            metadata=metadata,
            content_hash=content_hash,
        )
    except ValidationError as exc:
        raise SkillParseError("Skill 字段校验失败") from exc
