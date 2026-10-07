"""Python AgentSession SDK；与 REST/CLI 使用同一会话与持久事实。"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING
from uuid import uuid4

from krim_agent.agents import AgentSessionView, AgentTurnView, ExecResult
from krim_agent.application.agents import AgentPromptRequest, AgentService, AgentView
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StreamFrame

if TYPE_CHECKING:
    from .text import StreamTextResult
    from .types import GenerateResult


class AgentSession:
    """一个实例对应一个稳定 session；重开保留上下文、轮次和预算。"""

    def __init__(self, service: AgentService, agent_id: str) -> None:
        self._service = service
        self.agent_id = agent_id
        self.session_id = agent_id

    @property
    def service(self) -> AgentService:
        if self._service._closed:
            raise ServiceError("sdk_closed", "Agent SDK 已关闭", 503)
        return self._service

    async def state(self) -> AgentSessionView:
        return await self.service.session(self.agent_id)

    async def view(self) -> AgentView:
        return await self.service.get(self.agent_id)

    async def execute(self, prompt: str, *, idempotency_key: str | None = None) -> ExecResult:
        result = await self.service.execute(
            self.agent_id, AgentPromptRequest(prompt=prompt, idempotency_key=idempotency_key)
        )
        assert result.result is not None
        return result.result

    async def start(self, prompt: str, *, idempotency_key: str | None = None) -> AgentView:
        return await self.service.start(
            self.agent_id, AgentPromptRequest(prompt=prompt, idempotency_key=idempotency_key)
        )

    async def wait(self) -> ExecResult:
        result = await self.service.wait(self.agent_id)
        assert result.result is not None
        return result.result

    async def stream_text(
        self, prompt: str, *, idempotency_key: str | None = None
    ) -> StreamTextResult:
        key = idempotency_key or str(uuid4())
        await self.start(prompt, idempotency_key=key)
        run_id = await self.service.wait_started(self.agent_id, key)
        return await self.stream(run_id=run_id)

    async def generate_text(
        self, prompt: str, *, idempotency_key: str | None = None
    ) -> GenerateResult:
        stream = await self.stream_text(prompt, idempotency_key=idempotency_key)
        result = await stream.collect()
        await self.service.wait_run(self.agent_id, result.run.id)
        return result

    async def resume(self, prompt: str, *, idempotency_key: str | None = None) -> ExecResult:
        result = await self.service.resume(
            self.agent_id, AgentPromptRequest(prompt=prompt, idempotency_key=idempotency_key)
        )
        assert result.result is not None
        return result.result

    async def turns(self) -> list[AgentTurnView]:
        return await self.service.session_turns(self.agent_id)

    async def stream(self, *, run_id: str | None = None) -> StreamTextResult:
        from .text import StreamTextResult

        runtime, state = await self.service.stream_state(self.agent_id, 0, run_id)
        return StreamTextResult(runtime, state.run)

    async def events(
        self, from_seq: int = 0, *, run_id: str | None = None
    ) -> AsyncIterator[StreamFrame]:
        runtime, state = await self.service.stream_state(self.agent_id, from_seq, run_id)
        subscription = runtime.subscribe(state, from_seq)
        try:
            async for frame in subscription:
                if frame is not None:
                    yield frame
        finally:
            await subscription.aclose()

    async def stop(self) -> AgentView:
        return await self.service.stop(self.agent_id)

    async def close(self, *, expected_revision: int | None = None) -> AgentSessionView:
        revision = (
            expected_revision if expected_revision is not None else (await self.state()).revision
        )
        return await self.service.close_session(self.agent_id, revision)

    async def reopen(self, *, expected_revision: int | None = None) -> AgentSessionView:
        revision = (
            expected_revision if expected_revision is not None else (await self.state()).revision
        )
        return await self.service.reopen_session(self.agent_id, revision)

    async def restore(self) -> AgentView:
        return await self.service.restore(self.agent_id)

    async def destroy(self) -> AgentView:
        return await self.service.destroy(self.agent_id)
