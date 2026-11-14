"""Trusted host worker envelopes; all business bindings use I's shared digest."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from krim_agent.development.contracts import CommandRequest, ProcessHandle
from krim_agent.development.ports import RuntimeExecution, command_digest

WorkerCall = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


class WorkerError(RuntimeError):
    """Bounded error without raw backend messages or host paths."""


class WorkerClient:
    def __init__(self, call: WorkerCall) -> None:
        self.call = call

    async def invoke(self, payload: dict[str, Any]) -> dict[str, Any]:
        reply = await self.call(payload)
        if reply.get("ok") is not True or not isinstance(reply.get("result"), dict):
            raise WorkerError("terminal_worker_failed")
        return dict(reply["result"])

    async def attach(
        self, request: CommandRequest, process: ProcessHandle, execution: RuntimeExecution,
    ) -> dict[str, Any]:
        process.require_binding(request)
        if (execution.process_id != process.process_id
                or execution.operation_id != request.operation_id
                or execution.environment.model_dump(exclude={"state"})
                != request.environment.model_dump(exclude={"state"})
                or execution.pty != request.pty or not execution.attach_argv):
            raise ValueError("execution binding changed")
        result = await self.invoke({
            "action": "submit", "process_id": process.process_id,
            "request": request.model_dump(mode="json"), "request_digest": command_digest(request),
            "execution_id": execution.execution_id, "attach_argv": list(execution.attach_argv),
        })
        self.validate_reply(result, request, process)
        return result

    async def inspect(self, request: CommandRequest, process: ProcessHandle) -> dict[str, Any]:
        result = await self.invoke(self.control(request, process, "inspect"))
        self.validate_reply(result, request, process)
        return result

    @staticmethod
    def validate_reply(
        reply: dict[str, Any], request: CommandRequest, process: ProcessHandle,
    ) -> None:
        if reply.get("process_id") != process.process_id:
            raise ValueError("worker process binding changed")
        if reply.get("request_digest") != command_digest(request):
            if not (reply.get("state") == "unknown" and reply.get("reason") == "receipt_missing"
                    and reply.get("request_digest") is None):
                raise ValueError("worker digest binding changed")

    @staticmethod
    def control(
        request: CommandRequest, process: ProcessHandle, action: str, **values: Any,
    ) -> dict[str, Any]:
        process.require_binding(request)
        return {"action": action, "process_id": process.process_id,
                "request_digest": command_digest(request), **values}
