"""Python SubAgent 生命周期接口。"""

from .catalog import AgentCatalog, AgentDescriptor, AgentToolInput
from .manager import AgentHandle, AgentManager
from .profiles import AgentProfile, AgentProfileUpdate, AgentProfileVersion
from .sessions import AgentSessionControl, AgentSessionView, AgentTurnView
from .types import AgentDefinition, AgentError, ExecResult

__all__ = [
    "AgentCatalog",
    "AgentDefinition",
    "AgentDescriptor",
    "AgentError",
    "AgentHandle",
    "AgentManager",
    "AgentToolInput",
    "AgentProfile",
    "AgentProfileUpdate",
    "AgentProfileVersion",
    "ExecResult",
    "AgentSessionControl",
    "AgentSessionView",
    "AgentTurnView",
]
