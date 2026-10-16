"""三私有Host叶只调用原Backend；所有Hook后核对真实候选再持久回执。"""

import json
from functools import cache
from typing import Any

from pydantic import BaseModel

from krim_agent.workspace_container_retention import ContainerRetentionPreview
from krim_agent.workspace_container_retention_control import (
    CONTAINER_RETENTION_STEP_TOOLS,
    ContainerRetentionInspectInput,
    ContainerRetentionOperationInput,
    ContainerRetentionStepView,
)

from .core import ToolContext, ToolDefinition, ToolError


@cache
def container_retention_tools() -> tuple[ToolDefinition, ...]:
    def definition(kind: str, name: str) -> ToolDefinition:
        async def execute(arguments: BaseModel, context: ToolContext) -> dict[str, object]:
            backend = context.workspace_container_retentions
            if backend is None:
                raise ToolError("workspace_container_retention_context", "缺少原受控回收后端")
            value = await backend.tool_step(kind, arguments.model_dump(), context)
            context._container_retention_response = json.dumps(value, sort_keys=True)
            return value

        async def guard(value: Any, context: ToolContext) -> None:
            if (
                context.workspace_container_retentions is None
                or context._container_retention_response != json.dumps(value, sort_keys=True)
            ):
                raise ToolError("workspace_container_retention_unverified", "回收回程不可信")
            await context.workspace_container_retentions.validate_step(kind, value, context)

        return ToolDefinition(
            name,
            "仅沿原私有Host围栏审阅空事务容器、回收或只读核实；不接受路径或材料",
            ContainerRetentionInspectInput
            if kind == "inspect"
            else ContainerRetentionOperationInput,
            execute,
            "write" if kind == "remove" else "read",
            output_schema=ContainerRetentionPreview
            if kind == "inspect"
            else ContainerRetentionStepView,
            output_guard=guard,
        )

    return tuple(definition(kind, name) for kind, name in CONTAINER_RETENTION_STEP_TOOLS.items())
