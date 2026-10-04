"""每次 Run 的 Skill 快照和 read_skill 只读工具。"""

from __future__ import annotations

from pydantic import BaseModel

from krim_agent.skills import SkillCatalog, SkillReadInput, SkillReadResult
from krim_agent.tools.core import ToolContext, ToolDefinition, ToolError


def read_snapshot(catalog: SkillCatalog, name: str, resource: str | None = None) -> SkillReadResult:
    try:
        return catalog.read(name, resource)
    except KeyError as error:
        raise ToolError(str(error.args[0]), "技能或资源不在本次运行快照中") from error


async def read_skill(arguments: BaseModel, context: ToolContext) -> SkillReadResult:
    payload = SkillReadInput.model_validate(arguments)
    if context.skill_snapshot is None:
        raise ToolError("skill_not_available", "当前运行没有技能快照")
    return read_snapshot(context.skill_snapshot, payload.name, payload.resource)


_READ_SKILL = ToolDefinition(
    "read_skill",
    "按需读取本次运行技能正文或明确提供的文本资源；不会读取路径或网络",
    SkillReadInput,
    read_skill,
    "read",
    output_schema=SkillReadResult,
)


def definition() -> ToolDefinition:
    return _READ_SKILL
