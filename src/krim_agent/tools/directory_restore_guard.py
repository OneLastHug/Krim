"""先分类目录私有围栏，再决定原Host叶资格；falsey不代表无围栏。"""

import re

from krim_agent.workspace_directory_restore import DirectoryRestoreFence
from krim_agent.workspace_directory_restore_control import DIRECTORY_RESTORE_STEP_TOOLS

from .core import ToolContext, ToolError


def validate_directory_restore_parent(name: str, context: ToolContext) -> None:
    fence = context.directory_restore
    leaf = name in DIRECTORY_RESTORE_STEP_TOOLS.values()
    if fence is None:
        if leaf or context.directory_restore_qualify is not None:
            raise ToolError("workspace_directory_restore_context", "目录恢复叶需要原私有围栏")
        return
    if (
        type(fence) is not DirectoryRestoreFence
        or type(fence.kind) is not str
        or DIRECTORY_RESTORE_STEP_TOOLS.get(fence.kind) != name
        or type(fence.plan_id) is not str
        or type(fence.operation_id) is not str
        or type(fence.token) is not str
        or re.fullmatch(r"[a-f0-9]{64}", fence.token) is None
        or context.workspace_directory_restores is None
        or context.agent_id is None
        or context.session_id is None
        or context.origin != "host"
        or context.run_id != "local"
        or context.source_call_id is not None
        or context.directory_restore_qualify is not None
        or fence.model_source is not None
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
            "workspace_directory_restore_context", "目录恢复叶须保留原Host来源、阶段与独立围栏"
        )
