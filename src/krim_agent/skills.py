"""按话题保存的技能合约；正文和文本资源均为明确提供的数据。"""

from __future__ import annotations

import hashlib
import json
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Annotated, Literal

from pydantic import Field, JsonValue, TypeAdapter, field_validator, model_validator

from .protocol.models import StrictModel, check_json

if TYPE_CHECKING:
    from .protocol.domain import MessageView

SkillName = Annotated[
    str, Field(min_length=1, max_length=64, pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
]


def resource_path(value: str) -> str:
    path = PurePosixPath(value)
    if (
        not value
        or len(value) > 256
        or path.is_absolute()
        or str(path) != value
        or any(part in {".", ".."} for part in path.parts)
        or any(ord(character) < 32 for character in value)
        or any(character in value for character in "\\:%?#")
        or value == "SKILL.md"
    ):
        raise ValueError("技能资源必须是有限的相对键，不能是 URL、上级或 SKILL.md")
    return value


class SkillResource(StrictModel):
    path: str = Field(min_length=1, max_length=256)
    content: str = Field(min_length=1, max_length=8192)

    _path = field_validator("path")(resource_path)

    @field_validator("content")
    @classmethod
    def bounded_text(cls, value: str) -> str:
        check_json(value, max_bytes=24000)
        return value


class SkillDefinition(StrictModel):
    name: SkillName
    description: str = Field(min_length=1, max_length=1024)
    content: str = Field(min_length=1, max_length=8192)
    loading: Literal["deferred", "always"] = "deferred"
    license: str | None = Field(default=None, min_length=1, max_length=200)
    compatibility: str | None = Field(default=None, min_length=1, max_length=500)
    metadata: dict[str, str] = Field(default_factory=dict)
    allowed_tools: list[str] = Field(default_factory=list, max_length=32)
    resources: list[SkillResource] = Field(default_factory=list, max_length=16)

    @model_validator(mode="after")
    def bounded(self) -> SkillDefinition:
        if not self.description.strip() or not self.content.strip():
            raise ValueError("技能描述和正文不能是空白")
        check_json(self.content, max_bytes=24000)
        if len({resource.path for resource in self.resources}) != len(self.resources):
            raise ValueError("技能资源键重复")
        check_json(TypeAdapter(JsonValue).validate_python(self.metadata), max_bytes=2048)
        check_json(TypeAdapter(JsonValue).validate_python(self.allowed_tools), max_bytes=2048)
        check_json(self.model_dump(mode="json"), max_bytes=49152)
        return self

    def summary(self) -> SkillSummary:
        return SkillSummary(
            **self.model_dump(exclude={"content", "resources"}),
            resource_paths=[resource.path for resource in self.resources],
            digest=hashlib.sha256(self.model_dump_json().encode()).hexdigest(),
        )


class SkillSummary(StrictModel):
    name: SkillName
    description: str
    loading: Literal["deferred", "always"]
    license: str | None = None
    compatibility: str | None = None
    metadata: dict[str, str] = Field(default_factory=dict)
    allowed_tools: list[str] = Field(default_factory=list)
    resource_paths: list[str] = Field(default_factory=list)
    digest: str


class SkillCatalogView(StrictModel):
    skills: list[SkillSummary] = Field(default_factory=list, max_length=32)


class SkillCatalog(StrictModel):
    skills: list[SkillDefinition] = Field(default_factory=list, max_length=32)

    @model_validator(mode="after")
    def bounded(self) -> SkillCatalog:
        if len({skill.name for skill in self.skills}) != len(self.skills):
            raise ValueError("技能名称重复")
        check_json(self.model_dump(mode="json"), max_bytes=262144)
        check_json(self.view().model_dump(mode="json"), max_bytes=32768)
        return self

    def view(self) -> SkillCatalogView:
        return SkillCatalogView(
            skills=[skill.summary() for skill in sorted(self.skills, key=lambda item: item.name)]
        )

    def read(self, name: str, resource: str | None = None) -> SkillReadResult:
        skill = next((value for value in self.skills if value.name == name), None)
        if skill is None:
            raise KeyError("skill_not_found")
        if resource is None:
            return SkillReadResult(name=name, resource=None, content=skill.content)
        item = next((value for value in skill.resources if value.path == resource), None)
        if item is None:
            raise KeyError("skill_resource_not_found")
        return SkillReadResult(name=name, resource=resource, content=item.content)

    def batches(self) -> list[SkillBatch]:
        result: list[SkillBatch] = []
        current: list[SkillDefinition] = []
        for skill in self.skills:
            try:
                SkillBatch(skills=[*current, skill])
            except ValueError:
                if current:
                    result.append(SkillBatch(skills=current))
                current = [skill]
                SkillBatch(skills=current)
            else:
                current.append(skill)
        if current:
            result.append(SkillBatch(skills=current))
        return result


class SkillBatch(StrictModel):
    skills: list[SkillDefinition] = Field(min_length=1, max_length=16)

    @model_validator(mode="after")
    def bounded(self) -> SkillBatch:
        if len({skill.name for skill in self.skills}) != len(self.skills):
            raise ValueError("单次注入的技能名称重复")
        check_json(self.model_dump(mode="json"), max_bytes=49152)
        return self


class SkillSnapshot(SkillCatalog):
    selected_names: list[SkillName] = Field(default_factory=list, max_length=8)

    @model_validator(mode="after")
    def selected(self) -> SkillSnapshot:
        names = {skill.name for skill in self.skills}
        if len(set(self.selected_names)) != len(self.selected_names):
            raise ValueError("本次选择的技能名称重复")
        if not set(self.selected_names).issubset(names):
            raise ValueError("本次选择的技能不在话题目录中")
        return self


class SkillTopicRequest(StrictModel):
    topic_id: str = Field(min_length=1, max_length=128)


class SkillInjectRequest(SkillBatch):
    topic_id: str = Field(min_length=1, max_length=128)


class SkillRemoveRequest(SkillTopicRequest):
    name: SkillName | None = None


class SkillReadInput(StrictModel):
    name: SkillName
    resource: str | None = Field(default=None, min_length=1, max_length=256)

    @field_validator("resource")
    @classmethod
    def valid_resource(cls, value: str | None) -> str | None:
        return resource_path(value) if value is not None else None


class SkillReadRequest(SkillReadInput):
    topic_id: str = Field(min_length=1, max_length=128)


class SkillReadResult(StrictModel):
    name: SkillName
    resource: str | None
    content: str = Field(min_length=1, max_length=8192)

    @field_validator("content")
    @classmethod
    def bounded_text(cls, value: str) -> str:
        check_json(value, max_bytes=24000)
        return value


def materialize_skills(message: MessageView) -> MessageView:
    raw = message.details.get("skills_snapshot")
    if message.role != "user" or raw is None:
        return message
    snapshot = SkillSnapshot.model_validate(raw)
    selected = set(snapshot.selected_names)
    payload = {
        "skills": snapshot.view().model_dump(mode="json")["skills"],
        "loaded": [
            {"name": skill.name, "content": skill.content}
            for skill in snapshot.skills
            if skill.loading == "always" or skill.name in selected
        ],
    }
    details = {key: value for key, value in message.details.items() if key != "skills_snapshot"}
    details.setdefault("_prompt_context_raw_text", message.content)
    content = message.content + "\n\n[Krim Agent 技能目录数据；按需使用 read_skill 读取正文/资源]\n"
    content += json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return message.model_copy(update={"content": content, "details": details}, deep=True)
