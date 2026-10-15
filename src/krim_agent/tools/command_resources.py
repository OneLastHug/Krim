"""独立只读原资源事实工具；保留既有八个命令工具的已发布Schema。"""

from functools import cache
from typing import Any

from pydantic import BaseModel, JsonValue

from krim_agent.execution.agent_models import CommandLookup
from krim_agent.execution.management import CommandResourcesView

from .core import ToolContext, ToolDefinition, ToolError


@cache
def command_resources_tool() -> ToolDefinition:
    return ToolDefinition(
        "command_resources",
        "只读原命令冻结限额、整树内核统计与回收事实，不重新执行或访问内核组",
        CommandLookup,
        _execute,
        "read",
        output_schema=CommandResourcesView,
        output_guard=_guard,
    )


async def _execute(arguments: BaseModel, context: ToolContext) -> dict[str, JsonValue]:
    if context.commands is None:
        raise ToolError("execution_unbound", "当前上下文没有命令执行服务")
    return await context.commands.tool_operation("resources", arguments.model_dump(), context)


async def _guard(value: Any, context: ToolContext) -> None:
    if context.commands is None:
        raise ToolError("execution_unbound", "当前上下文没有命令执行服务")
    await context.commands.validate_result("resources", value, context)
