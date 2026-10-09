"""命令工具统一走原Registry，模型参数不包含宿主挂载或文件字节。"""

from collections.abc import Awaitable, Callable
from functools import cache
from typing import Any

from pydantic import BaseModel, JsonValue

from krim_agent.execution.agent_models import (
    AgentCommandRequest,
    CommandLookup,
    CommandRead,
    CommandSlice,
    CommandSummary,
)

from .core import ToolContext, ToolDefinition, ToolError


@cache
def command_tools() -> tuple[ToolDefinition, ...]:
    def execute(
        operation: str,
    ) -> Callable[[BaseModel, ToolContext], Awaitable[dict[str, JsonValue]]]:
        async def call(arguments: BaseModel, context: ToolContext) -> dict[str, JsonValue]:
            if context.commands is None:
                raise ToolError("execution_unbound", "当前上下文没有命令执行服务")
            return await context.commands.tool_operation(operation, arguments.model_dump(), context)

        return call

    def guard(operation: str) -> Callable[[Any, ToolContext], Awaitable[None]]:
        async def validate(value: Any, context: ToolContext) -> None:
            if context.commands is None:
                raise ToolError("execution_unbound", "当前上下文没有命令执行服务")
            await context.commands.validate_result(operation, value, context)

        return validate

    return (
        ToolDefinition(
            "command_execute",
            "在隔离副本执行命令，明确输入文件须经原读取授权；等待原结果，不修改原工作区",
            AgentCommandRequest,
            execute("execute"),
            "write",
            output_schema=CommandSummary,
            output_guard=guard("execute"),
        ),
        ToolDefinition(
            "command_get",
            "读取同一会话的原命令状态、有界输出与产物摘要，不重跑",
            CommandLookup,
            execute("get"),
            "read",
            output_schema=CommandSummary,
            output_guard=guard("get"),
        ),
        ToolDefinition(
            "command_read",
            "分页读取原命令输出或产物字节，base64保留原始值，文本仅为预览",
            CommandRead,
            execute("read"),
            "read",
            output_schema=CommandSlice,
            output_guard=guard("read"),
        ),
        ToolDefinition(
            "command_cancel",
            "取消同一会话的原命令并等待本机执行收束，不重新执行",
            CommandLookup,
            execute("cancel"),
            "write",
            output_schema=CommandSummary,
            output_guard=guard("cancel"),
        ),
    )
