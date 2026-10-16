"""受控整树来源的原预算，权限/审批后冻结，物理资格前再次核实。"""

import asyncio

from krim_agent.protocol.domain import ServiceError
from krim_agent.tools.core import ToolContext, execution_budget_snapshot


class TreeMoveBudget:
    def __init__(self, prefix: str) -> None:
        self.prefix = prefix
        self.snapshot: tuple[object, ...] | None = None

    def start(self, context: ToolContext) -> None:
        self.snapshot = execution_budget_snapshot(context.execution_timeouts)
        self.check(context)

    def check(self, context: ToolContext) -> None:
        if not context.execution_timeouts:
            raise ServiceError(self.prefix + "_context", "整树工具须原Registry执行预算", 403)
        if self.snapshot != execution_budget_snapshot(context.execution_timeouts):
            raise ServiceError(self.prefix + "_budget", "原整树执行预算或暂停资格改变", 409)
        now = asyncio.get_running_loop().time()
        for execution in context.execution_timeouts:
            when = execution.deadline.when()
            if (
                not execution.active
                or execution.deadline.expired()
                or when is not None
                and when <= now
            ):
                raise ServiceError(self.prefix + "_deadline", "原整树执行预算已到期", 409)
