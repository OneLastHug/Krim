"""消息父链与活动路径算法；不依赖数据库、网络或模型。"""

from collections import defaultdict

from krim_agent.protocol.domain import MessageView, ServiceError

ROOT = "__root__"


class MessageTree:
    def __init__(self, messages: list[MessageView]) -> None:
        self.nodes = {message.id: message for message in messages}
        if len(self.nodes) != len(messages):
            raise ServiceError("message_tree_corrupt", "消息 ID 重复", 409)
        self.children: dict[str, list[MessageView]] = defaultdict(list)
        verified: set[str] = set()
        for message in messages:
            self.children[message.parent_id or ROOT].append(message)
            current: MessageView | None = message
            seen: set[str] = set()
            while current is not None and current.id not in verified:
                if current.id in seen:
                    raise ServiceError("message_tree_corrupt", "消息父链存在循环", 409)
                seen.add(current.id)
                if current.parent_id is None:
                    break
                parent = self.nodes.get(current.parent_id)
                if parent is None or parent.topic_id != current.topic_id:
                    raise ServiceError("message_tree_corrupt", "消息父链缺失或跨话题", 409)
                current = parent
            verified.update(seen)
        for children in self.children.values():
            children.sort(key=lambda message: (message.created_at, message.id))

    def active_path(self, choices: dict[str, str | None]) -> list[MessageView]:
        for parent_id, child_id in choices.items():
            if parent_id != ROOT and parent_id not in self.nodes:
                raise ServiceError("message_tree_corrupt", "活动分支父节点缺失", 409)
            if child_id is not None and child_id not in {
                child.id for child in self.children.get(parent_id, [])
            }:
                raise ServiceError("message_tree_corrupt", "活动分支不是该父节点的子节点", 409)
        path: list[MessageView] = []
        parent_id = ROOT
        while children := self.children.get(parent_id):
            selected = choices.get(parent_id, children[0].id)
            if selected is None:
                break
            message = self.nodes[selected]
            path.append(message)
            parent_id = message.id
        return path

    def select(self, message_id: str, choices: dict[str, str | None]) -> dict[str, str | None]:
        current = self.nodes.get(message_id)
        if current is None:
            raise ServiceError("message_not_found", "消息不在该话题中", 404)
        selected = dict(choices)
        while current is not None:
            selected[current.parent_id or ROOT] = current.id
            current = self.nodes.get(current.parent_id) if current.parent_id else None
        return selected

    def user_ancestor(self, message_id: str) -> MessageView:
        current = self.nodes.get(message_id)
        if current is None:
            raise ServiceError("message_not_found", "消息不在该话题中", 404)
        while current.role != "user":
            parent = self.nodes.get(current.parent_id) if current.parent_id else None
            if parent is None:
                raise ServiceError("user_ancestor_missing", "消息没有用户祖先", 409)
            current = parent
        return current

    def require_complete_tools(self, path: list[MessageView]) -> None:
        """允许从已完成的取消结果继续；拒绝将悬空工具请求交给下一轮模型。"""
        pending: set[str] = set()
        for message in path:
            if message.role == "tool":
                call_id = message.details.get("call_id")
                if not isinstance(call_id, str) or call_id not in pending:
                    raise ServiceError("incomplete_tool_group", "工具结果缺少对应请求", 409)
                pending.remove(call_id)
                continue
            if pending:
                raise ServiceError("incomplete_tool_group", "历史工具请求尚未闭合，请重新生成", 409)
            calls = message.details.get("tool_calls", [])
            if not isinstance(calls, list):
                raise ServiceError("incomplete_tool_group", "工具请求结构不完整", 409)
            for call in calls:
                if not isinstance(call, dict) or not isinstance(call.get("id"), str):
                    raise ServiceError("incomplete_tool_group", "工具请求缺少关联 ID", 409)
                call_id = call["id"]
                if not isinstance(call_id, str) or call_id in pending:
                    raise ServiceError("incomplete_tool_group", "工具请求 ID 重复", 409)
                pending.add(call_id)
        if pending:
            raise ServiceError("incomplete_tool_group", "历史工具请求尚未闭合，请重新生成", 409)
