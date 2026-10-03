"""Python SubAgent 生命周期接口。"""

from .catalog import AgentCatalog, AgentDescriptor, AgentToolInput
from .manager import AgentHandle, AgentManager
from .types import AgentDefinition, AgentError, ExecResult

__all__ = [
    "AgentCatalog",
    "AgentDefinition",
    "AgentDescriptor",
    "AgentError",
    "AgentHandle",
    "AgentManager",
    "AgentToolInput",
    "ExecResult",
]
