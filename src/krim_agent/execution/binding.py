"""执行资源由可信宿主创建；撤销标记跨已有Agent定义共享。"""

from dataclasses import dataclass, field
from threading import Event

from krim_agent.protocol.domain import ServiceError

from .linux import LinuxSandboxRunner
from .records import CommandResource

COMMAND_TOOL_NAMES = frozenset(
    {
        "command_execute",
        "command_get",
        "command_read",
        "command_cancel",
        "command_changes",
        "command_propose",
        "command_history",
        "command_inspect",
    }
)


@dataclass(frozen=True)
class ExecutionBinding:
    resource: CommandResource
    runner: LinuxSandboxRunner
    revoked: Event = field(default_factory=Event, compare=False, repr=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.runner, LinuxSandboxRunner)
            or self.resource.fingerprint != self.runner.fingerprint
        ):
            raise ValueError("执行绑定必须对应同一可信runner指纹")

    @property
    def available(self) -> bool:
        return not self.revoked.is_set() and not self.runner.closed

    def require(self) -> LinuxSandboxRunner:
        if not self.available:
            raise ServiceError("execution_unavailable", "执行资源已停用或关闭", 403)
        return self.runner


@dataclass(frozen=True)
class CommandCapture:
    token: str
    path: str
    parent_call_id: str
