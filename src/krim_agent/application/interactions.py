"""持久用户交互服务：工具权限等待和真实答案，不自动重放批准。"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time

from pydantic import JsonValue, TypeAdapter

from krim_agent.interactions import InteractionKind, InteractionReply, InteractionView
from krim_agent.persistence import KrimRepository
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import check_json
from krim_agent.tools import ToolContext, ToolDefinition, ToolError


class InteractionService:
    def __init__(self, repository: KrimRepository, *, seconds: float = 120) -> None:
        if not 0 < seconds <= 300:
            raise ValueError("用户交互等待必须大于 0 且不超过 300 秒")
        self.repository, self.seconds = repository, seconds

    async def reply(
        self, run_id: str, interaction_id: str, request: InteractionReply
    ) -> InteractionView:
        value = await self.repository.get_interaction(run_id, interaction_id)
        if value.kind == "approval" and request.action not in {"approve", "deny"}:
            raise ServiceError("interaction_reply_invalid", "审批请求只能批准或拒绝", 422)
        if value.kind == "question" and request.action != "answer":
            raise ServiceError("interaction_reply_invalid", "问题请求需要真实答案", 422)
        status = {"approve": "approved", "deny": "denied", "answer": "answered"}[request.action]
        response: dict[str, JsonValue] = {"action": request.action}
        if request.answer is not None:
            response["answer"] = request.answer
        return await self.repository.reply_interaction(run_id, interaction_id, status, response)

    async def _request(
        self,
        kind: InteractionKind,
        tool_name: str,
        payload: dict[str, JsonValue],
        context: ToolContext,
    ) -> InteractionView:
        check_json(payload)
        key = hashlib.sha256(
            json.dumps(
                payload, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":")
            ).encode()
        ).hexdigest()
        value = await self.repository.create_interaction(
            context.topic_id,
            context.run_id,
            context.call_id,
            tool_name,
            kind,
            key,
            payload,
            timeout_seconds=self.seconds,
        )
        # 旧批准/答案属于已记录事实，不授予新执行；等待只能从 pending 开始。
        if value.status != "pending":
            raise ToolError("interaction_already_resolved", "旧交互已经处理，不能重新执行")
        try:
            if context.interaction_publisher is not None:
                data = TypeAdapter(dict[str, JsonValue]).validate_json(value.model_dump_json())
                await context.interaction_publisher(
                    {"type": "interaction_request", "interaction": data}
                )
            while True:
                value = await self.repository.get_interaction(context.run_id, value.id)
                if value.status != "pending":
                    return value
                remaining = value.expires_at - time.time()
                if remaining <= 0:
                    await self.repository.close_interaction(context.run_id, value.id, expired=True)
                    return await self.repository.get_interaction(context.run_id, value.id)
                await asyncio.sleep(min(0.1, remaining))
        except asyncio.CancelledError:
            await asyncio.shield(self.repository.close_interaction(context.run_id, value.id))
            raise
        except Exception:
            await self.repository.close_interaction(context.run_id, value.id)
            raise

    async def approve(self, definition: ToolDefinition, context: ToolContext) -> bool:
        parsed = definition.parameters.model_validate(context.arguments, strict=True)
        arguments = TypeAdapter(dict[str, JsonValue]).validate_python(
            parsed.model_dump(mode="json", by_alias=True)
        )
        payload: dict[str, JsonValue] = {
            "tool_name": definition.name,
            "risk": definition.risk,
            "arguments": arguments,
        }
        if definition.name == "workspace_read" and context.command_capture is not None:
            payload["input_capture"] = {
                "scope": "complete_file",
                "purpose": "command_input",
                "path": context.command_capture.path,
                "parent_call_id": context.command_capture.parent_call_id,
            }
        value = await self._request("approval", definition.name, payload, context)
        if value.status == "approved":
            return True
        if value.status == "denied":
            return False
        raise ToolError("interaction_" + value.status, "工具审批已取消或过期")

    async def question(self, question: str, options: list[str], context: ToolContext) -> str:
        payload: dict[str, JsonValue] = {"question": question, "options": list(options)}
        value = await self._request("question", "question", payload, context)
        answer = (value.response or {}).get("answer")
        if value.status == "answered" and isinstance(answer, str) and answer.strip():
            return answer
        raise ToolError("interaction_" + value.status, "问题没有收到真实用户答案")
