"""Prompt 结构化节点；只描述用户数据，不赋予工具或附件权限。"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field, field_validator

from .prompt_context import MentionOccurrence
from .protocol.models import StrictModel, check_json
from .skills import SkillName


class PromptTextNode(StrictModel):
    type: Literal["text"] = "text"
    text: str = Field(min_length=1, max_length=8192)

    @field_validator("text")
    @classmethod
    def text_is_valid(cls, value: str) -> str:
        if "\0" in value:
            raise ValueError("Prompt 文本节点不能包含 NUL")
        check_json(value, max_bytes=32768)
        return value


class PromptMentionNode(StrictModel):
    type: Literal["mention"] = "mention"
    mention_id: str = Field(min_length=1, max_length=36)
    label: str = Field(min_length=1, max_length=120)


class PromptSkillNode(StrictModel):
    type: Literal["skill"] = "skill"
    name: SkillName


PromptNode = Annotated[
    PromptTextNode | PromptMentionNode | PromptSkillNode, Field(discriminator="type")
]


def prompt_node_text(nodes: list[PromptNode]) -> str:
    return "".join(
        node.text
        if isinstance(node, PromptTextNode)
        else "@" + node.label
        if isinstance(node, PromptMentionNode)
        else ""
        for node in nodes
    )


def prompt_node_mentions(nodes: list[PromptNode]) -> list[MentionOccurrence]:
    position = 0
    mentions: list[MentionOccurrence] = []
    for node in nodes:
        if isinstance(node, PromptTextNode):
            position += len(node.text)
        elif isinstance(node, PromptMentionNode):
            token = "@" + node.label
            mentions.append(
                MentionOccurrence(
                    mention_id=node.mention_id, start=position, end=position + len(token)
                )
            )
            position += len(token)
        else:
            pass
    return mentions


def validate_prompt_nodes(text: str, nodes: list[PromptNode]) -> None:
    if len(nodes) > 128:
        raise ValueError("Prompt 节点数量超过 128")
    if prompt_node_text(nodes) != text:
        raise ValueError("Prompt 节点序列与用户正文不一致")
    check_json([node.model_dump(mode="json") for node in nodes], max_bytes=32768)
