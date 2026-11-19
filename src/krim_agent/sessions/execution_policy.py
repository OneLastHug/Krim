"""单会话执行模式合约与热切换通知；持久数据权威，通知只是调度加速。"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal
from weakref import WeakKeyDictionary

from pydantic import Field

from krim_agent.contracts import StrictModel

if TYPE_CHECKING:
    from krim_agent.security import PrincipalContext
    from krim_agent.storage.sqlite import SQLiteStore

ExecutionPolicyMode = Literal["manual", "auto", "bypass"]
EXECUTION_POLICY_MODES = frozenset({"manual", "auto", "bypass"})


class SessionExecutionPolicyUpdate(StrictModel):
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    expected_revision: int = Field(ge=0)
    mode: ExecutionPolicyMode


class SessionExecutionPolicyView(StrictModel):
    session_id: str
    mode: ExecutionPolicyMode
    revision: int = Field(ge=0)
    updated_at: float
    updated_by: str
    # project_id 为 None 表示未绑定项目的旧会话；access 只反映能力上限，
    # 不约束模式本身，bypass 不能越过项目只读上限。
    project_id: str | None
    project_access: Literal["read", "write"] | None
    project_access_revision: int = Field(ge=0)


@dataclass(frozen=True)
class ExecutionPolicyChange:
    session_id: str
    mode: ExecutionPolicyMode
    revision: int


ChangeCallback = Callable[[ExecutionPolicyChange], None]


class ExecutionPolicyNotifier:
    """模式变化回调注册表；订阅方只把通知当重评触发器，状态以重读持久数据为准。

    回调为同步函数，在发布方事件循环线程内联执行：只允许登记任务或置位
    事件等轻量动作，回调异常会向发布方传播，由发布方决定是否吞掉。
    """

    def __init__(self) -> None:
        self._subscribers: list[ChangeCallback] = []

    def subscribe(self, callback: ChangeCallback) -> Callable[[], None]:
        self._subscribers.append(callback)

        def unsubscribe() -> None:
            try:
                self._subscribers.remove(callback)
            except ValueError:
                pass

        return unsubscribe

    def publish(self, change: ExecutionPolicyChange) -> None:
        for callback in list(self._subscribers):
            callback(change)


execution_policy_notifications = ExecutionPolicyNotifier()


_EXECUTION_GATES: WeakKeyDictionary[asyncio.AbstractEventLoop, dict[str, asyncio.Lock]] = (
    WeakKeyDictionary()
)


def execution_gate(session_id: str) -> asyncio.Lock:
    """单会话启动闸门（ADR-0015 5.6）：排序模式CAS提交与运行时started交接。

    模式API提交与运行时queued调用的最新判定重读、确认决议、started标记
    （tool_intent/operations.admit）落库共用同一把锁；executor在entered
    边界释放，不持有整段工具执行或模型等待。因此PUT要么发生在最新重读
    之前（重读即见新模式），要么发生在started标记之后（已启动调用按计划
    继续至完成），不存在旧判定漏过新提交的交错。按事件循环弱引用分桶，
    loop回收后锁不残留，避免loop id复用串闸门。
    """
    loop = asyncio.get_running_loop()
    gates = _EXECUTION_GATES.get(loop)
    if gates is None:
        gates = {}
        _EXECUTION_GATES[loop] = gates
    gate = gates.get(session_id)
    if gate is None:
        gate = asyncio.Lock()
        gates[session_id] = gate
    return gate


async def commit_mode(
    store: SQLiteStore,
    *,
    principal: PrincipalContext,
    workspace: str,
    session_id: str,
    request_key: str,
    expected_revision: int,
    mode: ExecutionPolicyMode,
) -> SessionExecutionPolicyView:
    """在会话启动闸门内提交模式CAS；发布仍由调用方在闸门外执行。"""
    from krim_agent.storage.execution_policy import set_mode

    async with execution_gate(session_id):
        return await set_mode(
            store,
            principal=principal,
            workspace=workspace,
            session_id=session_id,
            request_key=request_key,
            expected_revision=expected_revision,
            mode=mode,
        )


def notify_mode_change(view: SessionExecutionPolicyView) -> None:
    execution_policy_notifications.publish(
        ExecutionPolicyChange(view.session_id, view.mode, view.revision)
    )
