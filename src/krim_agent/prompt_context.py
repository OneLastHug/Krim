"""Mentions 与 Context Tags 的中立 Python 合约；不读取路径、URL 或执行内容。"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, JsonValue, model_validator

from .protocol.models import StrictModel, check_json


class MentionInput(StrictModel):
    kind: Literal["text", "file", "resource"] = "text"
    label: str = Field(min_length=1, max_length=120)
    content: str = Field(min_length=1, max_length=8192)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def bounded(self) -> MentionInput:
        if not self.label.strip() or not self.content.strip():
            raise ValueError("引用标签与内容不能是空白")
        if any(
            ord(character) < 32 or character in "\u007f\u0085\u2028\u2029"
            for character in self.label
        ):
            raise ValueError("引用标签不能包含控制符或换行")
        check_json(self.metadata, max_bytes=8192)
        return self


class MentionRef(MentionInput):
    id: str = Field(min_length=1, max_length=36)


class MentionOccurrence(StrictModel):
    mention_id: str = Field(min_length=1, max_length=36)
    start: int = Field(ge=0, le=8192)
    end: int = Field(gt=0, le=8192)

    @model_validator(mode="after")
    def valid_range(self) -> MentionOccurrence:
        if self.end <= self.start:
            raise ValueError("引用结束位置必须大于开始位置")
        return self


class MentionSnapshot(MentionRef):
    start: int = Field(ge=0, le=8192)
    end: int = Field(gt=0, le=8192)


class ContextTagInput(StrictModel):
    name: str = Field(min_length=1, max_length=120)
    content: str = Field(min_length=1, max_length=8192)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def bounded(self) -> ContextTagInput:
        if not self.name.strip() or not self.content.strip():
            raise ValueError("背景标签名称与内容不能是空白")
        check_json(self.metadata, max_bytes=8192)
        return self


class PromptContext(StrictModel):
    mentions: list[MentionRef] = Field(default_factory=list, max_length=32)
    context_tags: list[ContextTagInput] = Field(default_factory=list, max_length=32)

    @model_validator(mode="after")
    def bounded(self) -> PromptContext:
        if len({mention.id for mention in self.mentions}) != len(self.mentions):
            raise ValueError("引用 ID 重复")
        if len({tag.name for tag in self.context_tags}) != len(self.context_tags):
            raise ValueError("背景标签名称重复")
        check_json(self.model_dump(mode="json"))
        return self


class PromptSnapshot(StrictModel):
    mentions: list[MentionSnapshot] = Field(default_factory=list, max_length=32)
    context_tags: list[ContextTagInput] = Field(default_factory=list, max_length=32)

    @model_validator(mode="after")
    def bounded(self) -> PromptSnapshot:
        check_json(self.model_dump(mode="json"))
        return self


class PromptContextRequest(StrictModel):
    topic_id: str = Field(min_length=1, max_length=128)


class MentionAddRequest(MentionInput):
    topic_id: str = Field(min_length=1, max_length=128)


class MentionRemoveRequest(PromptContextRequest):
    mention_id: str = Field(min_length=1, max_length=36)


class ContextTagAddRequest(ContextTagInput):
    topic_id: str = Field(min_length=1, max_length=128)


class ContextTagRemoveRequest(PromptContextRequest):
    name: str = Field(min_length=1, max_length=120)
