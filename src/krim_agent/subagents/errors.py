"""子代理域错误：code 在 contracts 注册，附加身份、消息接受状态与建议动作。"""

from __future__ import annotations

from typing import Literal

from krim_agent.contracts import ERROR_CODES, ServiceError

# 子代理域新增码；通用情形复用既有码：invalid_input、capacity、permission_denied、
# revision_conflict、key_conflict、not_found、closed、schema_upgrade_required、
# storage_unavailable。CAS 失败一律 revision_conflict（旧版本更新）。
SUBAGENT_ERROR_CODES = frozenset(
    {
        "subagent_name_conflict",
        "subagent_depth_exceeded",
        "subagent_capability",
        "subagent_tree_closed",
        "subagent_activation_closed",
        "subagent_task_closed",
        "subagent_not_continuable",
        "subagent_not_recoverable",
        "subagent_cleanup_failed",
    }
)
assert SUBAGENT_ERROR_CODES <= ERROR_CODES

MessageAcceptState = Literal["not_accepted", "accepted", "unknown"]


class SubagentError(ServiceError):
    """工具面错误：模型凭 code/accepted/action 决定下一步，身份进 ids 供寻址。"""

    def __init__(
        self,
        code: str,
        *,
        accepted: bool | None = None,
        ids: dict[str, str] | None = None,
        action: str | None = None,
        status: int = 409,
    ) -> None:
        # accepted=None 表示存储或连接故障导致接受状态未知，只能按原键核对；
        # False 表示确认未接受；True 表示已接受。
        self.accepted = accepted
        self.ids: dict[str, str] = dict(ids) if ids else {}
        self.action = action
        super().__init__(code, status)

    def payload(self) -> dict[str, object]:
        """JSON 兼容错误体；accepted 恒在（null=未知），不携带异常原文或内部栈。"""

        body: dict[str, object] = {
            "code": self.code,
            "status": self.status,
            # accepted: true/false 明确接受与否；null 表示存储或连接故障导致
            # 接受状态未知，只能用原 send_key 核对，不能重建任务。
            "accepted": self.accepted,
        }
        if self.ids:
            body["ids"] = self.ids
        if self.action:
            body["action"] = self.action
        return body
