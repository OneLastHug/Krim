"""整棵命令进程树的独立限额与内核观测，不把rlimit当作合计。"""

from typing import Literal

from pydantic import ConfigDict, Field

from krim_agent.protocol.models import StrictModel

MAX_SAFE = 9007199254740991


class SandboxTreeLimits(StrictModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    cpu_seconds: float = Field(default=10.0, ge=0.1, le=300)
    memory_bytes: int = Field(default=268435456, ge=67108864, le=2147483648, multiple_of=4096)
    process_count: int = Field(default=32, ge=4, le=128)


class SandboxTreeUsage(StrictModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    scope: Literal["process_tree"] = "process_tree"
    backend: Literal["cgroup_v2"] = "cgroup_v2"
    limits: SandboxTreeLimits
    membership_verified: bool
    cpu_microseconds: int | None = Field(ge=0, le=MAX_SAFE)
    memory_peak_bytes: int | None = Field(ge=0, le=MAX_SAFE)
    tasks_peak: int | None = Field(ge=0, le=MAX_SAFE)
    oom_kills: int | None = Field(ge=0, le=MAX_SAFE)
    task_limit_hits: int | None = Field(ge=0, le=MAX_SAFE)
    populated: bool | None
    leader_reaped: bool | None = None
