"""先分类目录私有围栏，再核实原Host/Model叶资格；falsey不代表无围栏。"""

import re

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_directory_restore import directory_restore_model_source
from krim_agent.workspace_missing_directory_restore import MissingDirectoryRestoreFence
from krim_agent.workspace_missing_directory_restore_control import (
    MISSING_DIRECTORY_RESTORE_STEP_TOOLS,
)

from .core import ToolContext, ToolError


def validate_missing_directory_restore_parent(name: str, context: ToolContext) -> None:
    fence = context.directory_restore
    leaf = name in MISSING_DIRECTORY_RESTORE_STEP_TOOLS.values()
    if fence is None:
        if leaf or context.directory_restore_qualify is not None:
            raise ToolError(
                "workspace_missing_directory_restore_context", "缺失目录恢复叶需要原私有围栏"
            )
        return
    if (
        type(fence) is not MissingDirectoryRestoreFence
        or type(fence.kind) is not str
        or MISSING_DIRECTORY_RESTORE_STEP_TOOLS.get(fence.kind) != name
        or type(fence.plan_id) is not str
        or type(fence.operation_id) is not str
        or type(fence.token) is not str
        or re.fullmatch(r"[a-f0-9]{64}", fence.token) is None
        or context.workspace_directory_restores is None
        or context.agent_id is None
        or context.session_id is None
        or any(
            value is not None
            for value in (
                context.checkpoint_capture,
                context.checkpoint_restore,
                context.directory_checkpoint_capture,
                context.directory_checkpoint_qualify,
                context.command_capture,
                context.workspace_batch,
                context.mixed_batch,
                context.tree_fence,
            )
        )
    ):
        raise ToolError(
            "workspace_missing_directory_restore_context",
            "缺失目录恢复叶须保留原来源、阶段与独立围栏",
        )
    try:
        source = directory_restore_model_source(fence.model_source)
    except ServiceError:
        raise ToolError(
            "workspace_missing_directory_restore_context", "缺失目录恢复原父来源无效"
        ) from None
    valid = (
        context.origin == "host"
        and context.run_id == "local"
        and context.source_call_id is None
        and context.directory_restore_qualify is None
        if source is None
        else context.origin == "model"
        and context.run_id == source.run_id
        and context.source_call_id == source.call_id
        and callable(context.directory_restore_qualify)
        and context.workspace_checkpoint_controls is not None
    )
    if not valid:
        raise ToolError(
            "workspace_missing_directory_restore_context", "缺失目录恢复须保留原父Run/call与资格"
        )
