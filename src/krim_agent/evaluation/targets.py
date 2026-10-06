"""评估目标适配器；取消读侧时显式收束自己的实际生产。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import replace
from typing import TYPE_CHECKING, Literal, cast

from pydantic import JsonValue

from krim_agent.agents import AgentDefinition, AgentManager
from krim_agent.context_engine import ContextRunConfig

from .runner import EvalAttempt, EvalCleanupError
from .types import EvalCase, EvalOutput, EvalTargetInfo, EvalToolCall, fingerprint

if TYPE_CHECKING:
    from krim_agent.agents.manager import AgentHandle
    from krim_agent.persistence import KrimRepository
    from krim_agent.sdk import GenerateResult, PythonAI, StreamTextResult


def generation_output(result: GenerateResult, *, agent_id: str | None = None) -> EvalOutput:
    if result.run.status in {"queued", "running"}:
        raise ValueError("评估输出必须来自终态运行")
    status = cast(Literal["completed", "failed", "aborted", "interrupted"], result.run.status)
    audits = {audit.call_id: audit for audit in result.audits}
    tools = []
    for step in result.steps:
        for index, call in enumerate(step.tool_calls):
            audit = audits.get(call.id)
            observation = step.tool_results[index] if index < len(step.tool_results) else None
            tools.append(
                EvalToolCall(
                    call_id=call.id,
                    name=call.name,
                    arguments=call.arguments,
                    ok=audit.ok if audit else None,
                    code=audit.code if audit else None,
                    result=observation.data if observation else None,
                )
            )
    return EvalOutput(
        value=result.text,
        status=status,
        run_id=result.run.id,
        topic_id=result.run.topic_id,
        agent_id=agent_id,
        finish_reason=result.finish_reason,
        code=result.code,
        tools=tools,
        model_steps=len(result.model_steps) if result.model_steps else None,
        token_usage=result.token_usage,
        usage_complete=result.usage_complete,
        trace_context=result.trace_context,
    )


def _prompt(case: EvalCase, prefix: str) -> str:
    if not isinstance(case.input, str) or not case.input.strip():
        raise ValueError("生成与 Agent 评估需要非空文本输入")
    return prefix + case.input


async def _cleanup(operation: Awaitable[None]) -> None:
    task = asyncio.ensure_future(operation)
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise
    except Exception:
        raise EvalCleanupError("评估运行未能完成资源清理") from None


class GenerationTarget:
    def __init__(self, ai: PythonAI, *, version: str = "1", prompt_prefix: str = "") -> None:
        self.ai, self.version, self.prompt_prefix = ai, version, prompt_prefix

    def describe(self) -> EvalTargetInfo:
        settings = self.ai.runtime.settings
        return EvalTargetInfo(
            name="generation",
            kind="pythonai",
            version=self.version,
            config={
                "provider": self.ai.runtime.provider.name,
                "model": settings.provider_model,
                "max_steps": settings.max_steps,
                "context": ContextRunConfig.from_settings(settings).model_dump(mode="json"),
                "tools_hash": fingerprint(
                    [tool.model_dump(mode="json") for tool in self.ai.runtime.tools.list_tools()]
                ),
                "prompt_prefix_hash": fingerprint(self.prompt_prefix),
            },
        )

    async def run(self, case: EvalCase, attempt: EvalAttempt) -> EvalOutput:
        prompt = _prompt(case, self.prompt_prefix)
        topic_id = None
        if attempt.checkpoint is not None:
            previous = attempt.bindings.get("topic_id")
            topic_id = (
                previous
                if isinstance(previous, str)
                else (await self.ai.runtime.repository.create_topic("评估样本")).id
            )
            await attempt.save_binding(
                {"topic_id": topic_id, "request_key": attempt.idempotency_key}
            )
        # start 也受清理保护：取消发生在话题/Run 写入期间时，先取得真正 Run，再停止。
        start = asyncio.create_task(
            self.ai.stream_text(prompt, topic_id=topic_id, idempotency_key=attempt.idempotency_key)
        )
        stream: StreamTextResult | None = None
        try:
            stream = await asyncio.shield(start)
            if attempt.checkpoint is not None:
                await attempt.save_binding({**attempt.bindings, "run_id": stream.run.id})
            attempt.output = generation_output(await stream.collect())
            return attempt.output
        except BaseException:

            async def cleanup() -> None:
                nonlocal stream
                if stream is None:
                    try:
                        stream = await start
                    except Exception:
                        return
                await stream.abort()
                try:
                    attempt.output = generation_output(await stream.collect())
                except Exception:
                    pass

            await _cleanup(cleanup())
            raise

    async def recover(self, case: EvalCase, attempt: EvalAttempt) -> EvalOutput | None:
        return await _recover_run(self.ai.runtime.repository, attempt)


class AgentTarget:
    def __init__(
        self,
        manager: AgentManager,
        definition: AgentDefinition,
        *,
        version: str = "1",
        prompt_prefix: str = "",
    ) -> None:
        self.manager, self.definition = manager, definition
        self.version, self.prompt_prefix = version, prompt_prefix

    def describe(self) -> EvalTargetInfo:
        return EvalTargetInfo(
            name=self.definition.name,
            kind="agent",
            version=self.version,
            config={
                "provider": self.definition.provider.name,
                "definition_hash": self.manager.definition_hash(self.definition),
                "prompt_prefix_hash": fingerprint(self.prompt_prefix),
            },
        )

    async def run(self, case: EvalCase, attempt: EvalAttempt) -> EvalOutput:
        from krim_agent.sdk.text import StreamTextResult

        prompt = _prompt(case, self.prompt_prefix)
        spawn = asyncio.create_task(self.manager.spawn(replace(self.definition, agent_id="")))
        handle: AgentHandle | None = None
        try:
            handle = await asyncio.shield(spawn)
            if attempt.checkpoint is not None:
                await attempt.save_binding(
                    {
                        "topic_id": handle.topic_id,
                        "agent_id": handle.agent_id,
                        "request_key": attempt.idempotency_key,
                    }
                )
            executed = await handle.execute(prompt, attempt.idempotency_key)
            if executed.run_id is None:
                attempt.output = EvalOutput(
                    status="failed", agent_id=handle.agent_id, code=executed.code
                )
            else:
                run = await handle.runtime.repository.get_run(executed.run_id)
                attempt.output = generation_output(
                    await StreamTextResult(handle.runtime, run).collect(), agent_id=handle.agent_id
                )
            return attempt.output
        finally:

            async def cleanup() -> None:
                nonlocal handle
                if handle is None:
                    try:
                        handle = await spawn
                    except Exception:
                        return
                await handle.destroy()
                if attempt.output is None and handle.current_run is not None:
                    try:
                        attempt.output = generation_output(
                            await StreamTextResult(handle.runtime, handle.current_run).collect(),
                            agent_id=handle.agent_id,
                        )
                    except Exception:
                        pass

            await _cleanup(cleanup())

    async def recover(self, case: EvalCase, attempt: EvalAttempt) -> EvalOutput | None:
        value = await _recover_run(self.manager.repository, attempt)
        if value is not None:
            value.agent_id = str(attempt.bindings.get("agent_id", "")) or None
        return value


TargetFunction = Callable[[EvalCase, EvalAttempt], Awaitable[EvalOutput]]


class CallableTarget:
    """只运行调用方提供的可信异步函数；不从数据集导入代码。"""

    def __init__(
        self,
        name: str,
        function: TargetFunction,
        *,
        version: str = "1",
        config: dict[str, JsonValue] | None = None,
    ) -> None:
        self.info = EvalTargetInfo(name=name, kind="callable", version=version, config=config or {})
        self.function = function

    def describe(self) -> EvalTargetInfo:
        return self.info.model_copy(deep=True)

    async def run(self, case: EvalCase, attempt: EvalAttempt) -> EvalOutput:
        return await self.function(case, attempt)


async def _recover_run(repository: KrimRepository, attempt: EvalAttempt) -> EvalOutput | None:
    from sqlalchemy import select

    from krim_agent.persistence.database import RunRow
    from krim_agent.protocol.domain import ServiceError
    from krim_agent.sdk.snapshots import read_generation_result

    topic = attempt.bindings.get("topic_id")
    key = attempt.bindings.get("request_key")
    if topic is None:
        return None
    if not isinstance(topic, str) or key != attempt.idempotency_key:
        raise ServiceError("eval_binding_invalid", "评估运行绑定不正确", 409)
    async with repository.database.write_lock, repository.database.sessions() as session:
        run_id = await session.scalar(
            select(RunRow.id).where(RunRow.topic_id == topic, RunRow.idempotency_key == key)
        )
    if run_id is None:
        return None
    run = await repository.get_run(run_id)
    if run.status in {"queued", "running"}:
        raise ServiceError("eval_source_active", "原评估生产仍活跃，不能重放", 409)
    return generation_output(await read_generation_result(repository, run_id))
