"""共享调度与逐项算法只消费这些端口。"""

from collections.abc import Callable
from contextvars import ContextVar
from functools import cache
from typing import Any, Protocol, runtime_checkable

from .domain import BatchPlan, CleanupResult, ExecutionRef, PreparedTrial, TrialSpec
from .paging import ExecutionPage, PlanHeader


@cache
def _admission_fence() -> ContextVar[tuple[object, Callable[[], None]] | None]:
    """仅可信内核准入Task传递的事务fence；不进入JSON，也不持有资源。"""
    return ContextVar("krim_eval_admission_fence", default=None)


class AgentAdapter(Protocol):
    def descriptor(self) -> dict[str, Any]: ...
    def assert_current(self, frozen: dict[str, Any]) -> None: ...
    async def prepare(self, trial: TrialSpec) -> PreparedTrial: ...
    async def execute(self, prepared: PreparedTrial) -> ExecutionRef: ...
    async def collect(self, ref: ExecutionRef) -> dict[str, Any]: ...
    async def cancel(self, ref: ExecutionRef) -> CleanupResult: ...
    async def close(self) -> CleanupResult: ...


class MetricPort(Protocol):
    def identity(self, rule: dict[str, Any]) -> dict[str, str]: ...
    def result(
        self,
        rule: dict[str, Any],
        status: str,
        value: int | None,
        reason: str | None = None,
    ) -> dict[str, Any]: ...
    async def score(
        self,
        rule: dict[str, Any],
        case: dict[str, Any],
        output: dict[str, Any],
    ) -> dict[str, Any]: ...


class EvalRepository(Protocol):
    @property
    def healthy(self) -> bool: ...
    async def by_key(self, request: dict[str, Any]) -> dict[str, Any] | None: ...
    async def begin(self, request: dict[str, Any]) -> tuple[dict[str, Any], bool]: ...
    async def snapshot(self, task_id: str) -> dict[str, Any]: ...
    async def set_running(self, task_id: str) -> None: ...
    def plan(self, snapshot: dict[str, Any]) -> BatchPlan: ...
    def adapter(self, request: dict[str, Any]) -> AgentAdapter: ...
    def metrics(self) -> MetricPort: ...
    async def intent(self, trial: TrialSpec) -> None: ...
    async def attach(self, trial: TrialSpec, ref: ExecutionRef) -> None: ...
    async def record(
        self,
        trial: TrialSpec,
        ref: ExecutionRef,
        observation: dict[str, Any] | None,
    ) -> None: ...
    async def commit(self, trial: TrialSpec, body: dict[str, Any]) -> None: ...
    async def stop(self, task_id: str) -> None: ...
    async def finish(self, task_id: str, status: str, code: str) -> None: ...
    def mode(self, request: dict[str, Any]) -> str: ...


def method_identities(adapter: AgentAdapter) -> tuple[object, ...]:
    return tuple(
        getattr(getattr(adapter, name), "__func__", getattr(adapter, name))
        for name in [
            "descriptor",
            "assert_current",
            "prepare",
            "execute",
            "collect",
            "cancel",
            "close",
        ]
    )


class AdapterLease:
    """只标装配的关闭权，不是新调度器，也不以字符串作为控制权限。"""

    def __init__(self, adapter: AgentAdapter, *, owned: bool = False) -> None:
        self.adapter = adapter
        self.owned = owned


@runtime_checkable
class PagedEvalRepository(EvalRepository, Protocol):
    async def commit_with_receipt(
        self, trial: TrialSpec, body: dict[str, Any], receipt: dict[str, Any]
    ) -> None: ...
    async def execution_header(self, task_id: str) -> PlanHeader: ...
    async def execution_page(
        self, task_id: str, cursor: str | None, *, limit: int = 16
    ) -> ExecutionPage: ...
    async def read_trial(
        self, scope: str, task_id: str, case_id: str, repetition: int
    ) -> dict[str, Any]: ...
    async def record_resource_receipt(self, trial: TrialSpec, receipt: dict[str, Any]) -> None: ...
    async def complete(self, task_id: str) -> None: ...


@runtime_checkable
class RetiringAdapter(AgentAdapter, Protocol):
    def retirement_receipt(self, ref: ExecutionRef) -> dict[str, Any]: ...
    def retire(self, ref: ExecutionRef, receipt: dict[str, Any]) -> None: ...
