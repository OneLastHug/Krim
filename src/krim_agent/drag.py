"""Drag & Drop 只预览明确描述，drop 复用既有 MentionRef 合约。"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, JsonValue, model_validator

from .prompt_context import MentionInput
from .protocol.models import StrictModel, check_json


class DragPreview(StrictModel):
    kind: Literal["text", "file", "resource"] = "text"
    label: str = Field(min_length=1, max_length=120)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def bounded(self) -> DragPreview:
        MentionInput(kind=self.kind, label=self.label, content="preview", metadata=self.metadata)
        check_json(self.model_dump(mode="json"), max_bytes=12288)
        return self


class DragState(StrictModel):
    active: bool
    preview: DragPreview | None = None


class DragEnterRequest(DragPreview):
    topic_id: str = Field(min_length=1, max_length=128)


class DragDropRequest(MentionInput):
    topic_id: str = Field(min_length=1, max_length=128)
