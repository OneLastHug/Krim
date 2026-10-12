"""显式模型准备/启动工具；原调用绑定与最终回程校验。"""

from functools import cache
from typing import Any

from pydantic import BaseModel, JsonValue

from krim_agent.workspace_tree import TreePlanRequest, TreePlanStartRequest, TreePlanView

from .core import ToolContext, ToolDefinition, ToolError


@cache
def model_tree_tools() -> tuple[ToolDefinition, ...]:
    async def prepare(arguments: BaseModel, context: ToolContext) -> dict[str, JsonValue]:
        if context.workspace_tree_models is None:
            raise ToolError("workspace_tree_backend_missing", "模型树服务未装配")
        return await context.workspace_tree_models.tool_prepare(arguments.model_dump(), context)

    async def start(arguments: BaseModel, context: ToolContext) -> dict[str, JsonValue]:
        if context.workspace_tree_models is None:
            raise ToolError("workspace_tree_backend_missing", "模型树服务未装配")
        return await context.workspace_tree_models.tool_start(arguments.model_dump(), context)

    async def validate_prepare(value: Any, context: ToolContext) -> None:
        if context.workspace_tree_models is None:
            raise ToolError("workspace_tree_backend_missing", "模型树服务未装配")
        await context.workspace_tree_models.validate_result("prepare", value, context)

    async def validate_start(value: Any, context: ToolContext) -> None:
        if context.workspace_tree_models is None:
            raise ToolError("workspace_tree_backend_missing", "模型树服务未装配")
        await context.workspace_tree_models.validate_result("start", value, context)

    return (
        ToolDefinition(
            "workspace_tree_model_prepare",
            "从当前会话原命令准备目录树planned计划，不创建宿主目录或文件",
            TreePlanRequest,
            prepare,
            "read",
            output_schema=TreePlanView,
            output_guard=validate_prepare,
        ),
        ToolDefinition(
            "workspace_tree_start",
            "沿原模型Run/call和权限预算执行原planned目录树，逐项原回执，可能部分成功",
            TreePlanStartRequest,
            start,
            "write",
            output_schema=TreePlanView,
            output_guard=validate_start,
        ),
    )
