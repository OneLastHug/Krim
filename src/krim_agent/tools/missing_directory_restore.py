"""缺失目录恢复三个原私有叶；所有Hook后由严格output_guard完成持久资格。"""

import json
from functools import cache
from typing import Any

from pydantic import BaseModel

from krim_agent.workspace_missing_directory_restore_control import (
    MISSING_DIRECTORY_RESTORE_STEP_TOOLS,
    MissingDirectoryRestoreStepInput,
    MissingDirectoryRestoreStepView,
)

from .core import ToolContext, ToolDefinition, ToolError


@cache
def missing_directory_restore_tools() -> tuple[ToolDefinition, ...]:
    def definition(kind: str, name: str) -> ToolDefinition:
        async def execute(arguments: BaseModel, context: ToolContext) -> dict[str, object]:
            backend = context.workspace_directory_restores
            if backend is None:
                raise ToolError(
                    "workspace_missing_directory_restore_context", "缺少原缺失目录恢复后端"
                )
            value = await backend.tool_step(kind, arguments.model_dump(), context)
            context._directory_restore_response = json.dumps(value, sort_keys=True)
            return value

        async def guard(value: Any, context: ToolContext) -> None:
            if (
                context.workspace_directory_restores is None
                or context._directory_restore_response != json.dumps(value, sort_keys=True)
            ):
                raise ToolError(
                    "workspace_missing_directory_restore_unverified", "缺失目录恢复回程不可信"
                )
            await context.workspace_directory_restores.validate_step(kind, value, context)

        return ToolDefinition(
            name,
            "仅在原私有缺失目录恢复围栏中读取原缺失事实、发布或核实；不接受材料或重定向",
            MissingDirectoryRestoreStepInput,
            execute,
            "write" if kind == "execute" else "read",
            output_schema=MissingDirectoryRestoreStepView,
            output_guard=guard,
        )

    return tuple(
        definition(kind, name) for kind, name in MISSING_DIRECTORY_RESTORE_STEP_TOOLS.items()
    )
