"""Host/local、实际私有阶段与预算由可信层提供；falsey围栏不能降级无围栏。"""

import re

from krim_agent.workspace_container_retention import ContainerRetentionFence
from krim_agent.workspace_container_retention_control import (
    CONTAINER_RETENTION_STEP_TOOLS,
    ContainerRetentionInspection,
)
from krim_agent.workspace_directory_restore import IDENTIFIER

from .core import ToolContext, ToolError


def retention_binding(context: ToolContext) -> tuple[object, ...] | None:
    fence = context.container_retention
    if fence is None:
        return None
    if type(fence) is ContainerRetentionInspection:
        return (type(fence), fence.inspection_id, fence.source_plan_id, fence.kind, fence.token)
    if type(fence) is ContainerRetentionFence:
        return (type(fence), fence.operation_id, fence.epoch, fence.kind, fence.token)
    raise ToolError("workspace_container_retention_context", "回收私有围栏类型无效")


def validate_container_retention_parent(name: str, context: ToolContext) -> None:
    fence = context.container_retention
    if fence is None:
        if name in CONTAINER_RETENTION_STEP_TOOLS.values():
            raise ToolError("workspace_container_retention_context", "回收叶需要原私有Host围栏")
        return
    if type(fence) not in {ContainerRetentionInspection, ContainerRetentionFence}:
        raise ToolError("workspace_container_retention_context", "回收私有围栏类型无效")
    if (
        type(fence.kind) is not str
        or CONTAINER_RETENTION_STEP_TOOLS.get(fence.kind) != name
        or type(fence.token) is not str
        or re.fullmatch(r"[a-f0-9]{64}", fence.token) is None
        or context.workspace_container_retentions is None
        or context.origin != "host"
        or context.run_id != "local"
        or context.source_call_id is not None
        or type(context.agent_id) is not str
        or re.fullmatch(IDENTIFIER, context.agent_id) is None
        or context.agent_id != context.session_id
        or type(context.topic_id) is not str
        or re.fullmatch(IDENTIFIER, context.topic_id) is None
        or any(
            value is not None
            for value in (
                context.checkpoint_capture,
                context.checkpoint_restore,
                context.directory_checkpoint_capture,
                context.directory_checkpoint_qualify,
                context.directory_restore,
                context.directory_restore_qualify,
                context.command_capture,
                context.workspace_batch,
                context.mixed_batch,
                context.tree_fence,
            )
        )
    ):
        raise ToolError("workspace_container_retention_context", "回收须保留原Host/local与独立阶段")
    if type(fence) is ContainerRetentionInspection:
        valid = (
            type(fence.inspection_id) is str
            and re.fullmatch(IDENTIFIER, fence.inspection_id) is not None
            and type(fence.source_plan_id) is str
            and re.fullmatch(IDENTIFIER, fence.source_plan_id) is not None
            and fence.kind == "inspect"
        )
    else:
        valid = (
            type(fence.operation_id) is str
            and re.fullmatch(IDENTIFIER, fence.operation_id) is not None
            and type(fence.epoch) is int
            and 0 <= fence.epoch <= 128
            and fence.kind in {"remove", "verify"}
        )
    if not valid:
        raise ToolError("workspace_container_retention_context", "回收原选择器或epoch无效")
