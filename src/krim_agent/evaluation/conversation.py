"""同话题多轮目标；每轮复用真实 Runtime 与只读原 Run 恢复。"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from pydantic import JsonValue

from krim_agent.protocol.domain import ServiceError
from krim_agent.usage import USAGE_FIELDS, UsageTotals

from .runner import EvalAttempt
from .targets import GenerationTarget
from .types import (
    EvalCase,
    EvalObservation,
    EvalOutput,
    EvalTargetInfo,
    EvalTurnResult,
    EvalUsageTotals,
    fingerprint,
)

if TYPE_CHECKING:
    from krim_agent.sdk import PythonAI


def combine_usage(outputs: Sequence[EvalObservation]) -> UsageTotals | EvalUsageTotals | None:
    usage = [output.token_usage for output in outputs]
    known = [value for value in usage if value is not None]
    totals: UsageTotals | EvalUsageTotals | None = None
    if known:
        steps = sum(output.model_steps or 0 for output in outputs)
        model = UsageTotals if steps <= 32 else EvalUsageTotals
        fields: dict[str, JsonValue] = {
            field: sum(getattr(value, field) for value in known)
            if len(known) == len(outputs)
            and all(getattr(value, field) is not None for value in known)
            else None
            for field in USAGE_FIELDS
        }
        fields.update(
            reported={
                field: sum(value.reported.get(field, 0) for value in known)
                for field in USAGE_FIELDS
                if any(field in value.reported for value in known)
            },
            attempted_steps=steps,
            reported_steps=sum(value.reported_steps for value in known),
            confirmed_steps=sum(value.confirmed_steps for value in known),
            usage_complete=all(output.usage_complete for output in outputs),
        )
        totals = model.model_validate(fields)
    return totals


def conversation_output(turns: list[EvalTurnResult]) -> EvalOutput:
    if not turns:
        raise ValueError("会话必须有实际轮次输出")
    outputs = [turn.output for turn in turns]
    totals = combine_usage(outputs)
    last = outputs[-1]
    return EvalOutput(
        **{
            **last.model_dump(),
            "tools": [tool for output in outputs for tool in output.tools or []]
            if all(output.tools is not None for output in outputs)
            else None,
            "model_steps": sum(output.model_steps or 0 for output in outputs)
            if all(output.model_steps is not None for output in outputs)
            else None,
            "token_usage": totals,
            "usage_complete": bool(totals and totals.usage_complete),
            "turns": turns,
        }
    )


class ConversationTarget:
    """首条 input 加至多15个 turns；每个样本/重复独立会话。"""

    def __init__(self, ai: PythonAI, *, version: str = "1", prompt_prefix: str = "") -> None:
        self.ai = ai
        self.generation = GenerationTarget(ai, version=version, prompt_prefix=prompt_prefix)

    def describe(self) -> EvalTargetInfo:
        info = self.generation.describe()
        return info.model_copy(update={"name": "conversation", "kind": "conversation"})

    @staticmethod
    def prompts(case: EvalCase) -> list[str]:
        if not isinstance(case.input, str) or not case.input.strip():
            raise ValueError("会话评估需要非空首条文本")
        return [case.input, *(turn.input for turn in case.turns)]

    def _turn_attempt(self, attempt: EvalAttempt, index: int, prompt: str) -> EvalAttempt:
        saved = attempt.bindings.get("turns", {})
        if not isinstance(saved, dict):
            raise ServiceError("eval_binding_invalid", "多轮绑定无效", 409)
        raw = saved.get(str(index), {})
        if not isinstance(raw, dict):
            raise ServiceError("eval_binding_invalid", "轮次绑定无效", 409)
        raw = dict(raw)
        if raw.get("input_hash", fingerprint(prompt)) != fingerprint(prompt):
            raise ServiceError("eval_binding_invalid", "轮次输入指纹已变化", 409)
        topic_id = attempt.bindings.get("topic_id")
        if topic_id is not None:
            if raw.get("topic_id", topic_id) != topic_id:
                raise ServiceError("eval_binding_invalid", "轮次话题不一致", 409)
            raw["topic_id"] = topic_id

        async def checkpoint(values: dict[str, JsonValue]) -> None:
            current = attempt.bindings.get("turns", {})
            if not isinstance(current, dict):
                raise ServiceError("eval_binding_invalid", "多轮绑定无效", 409)
            await attempt.save_binding(
                {
                    **attempt.bindings,
                    "turns": {**current, str(index): {**values, "input_hash": fingerprint(prompt)}},
                }
            )

        return EvalAttempt(
            attempt.report_id,
            attempt.case_id,
            attempt.repetition,
            generation=attempt.generation,
            namespace=f"turn:{index}",
            bindings=raw,
            checkpoint=checkpoint,
        )

    async def _read_turn(self, case: EvalCase, attempt: EvalAttempt) -> EvalOutput | None:
        if "request_key" not in attempt.bindings:
            return None
        value = await self.generation.recover(case, attempt)
        if value is not None and value.status in {"aborted", "interrupted"}:
            raise ServiceError("eval_resolution_required", "原会话轮次已中断", 409)
        return value

    async def run(self, case: EvalCase, attempt: EvalAttempt) -> EvalOutput:
        prompts = self.prompts(case)
        if "topic_id" not in attempt.bindings:
            topic = await self.ai.runtime.repository.create_topic("多轮评估样本")
            await attempt.save_binding({**attempt.bindings, "topic_id": topic.id, "turns": {}})
        turns: list[EvalTurnResult] = []
        child: EvalAttempt | None = None
        try:
            for index, prompt in enumerate(prompts):
                turn_case = EvalCase(id=case.id, input=prompt)
                child = self._turn_attempt(attempt, index, prompt)
                value = await self._read_turn(turn_case, child)
                if value is None:
                    value = await self.generation.run(turn_case, child)
                turns.append(
                    EvalTurnResult(
                        index=index,
                        input=prompt,
                        output=EvalObservation.model_validate(value.model_dump(exclude={"turns"})),
                    )
                )
                attempt.output = conversation_output(turns)
                if len(turns) < len(prompts) and value.status == "completed":
                    attempt.output.status = "interrupted"
                    attempt.output.code = "eval_conversation_partial"
                if value.status != "completed":
                    break
            if attempt.output is None:
                raise ValueError("会话没有真实输出")
            return attempt.output
        except BaseException:
            if child is not None and child.output is not None and len(turns) < len(prompts):
                turns.append(
                    EvalTurnResult(
                        index=len(turns),
                        input=prompts[len(turns)],
                        output=EvalObservation.model_validate(
                            child.output.model_dump(exclude={"turns"})
                        ),
                    )
                )
                attempt.output = conversation_output(turns)
            if attempt.output is not None and len(turns) < len(prompts):
                attempt.output.status = "interrupted"
                attempt.output.code = "eval_conversation_partial"
            raise

    async def recover(self, case: EvalCase, attempt: EvalAttempt) -> EvalOutput | None:
        turns: list[EvalTurnResult] = []
        for index, prompt in enumerate(self.prompts(case)):
            child = self._turn_attempt(attempt, index, prompt)
            value = await self._read_turn(EvalCase(id=case.id, input=prompt), child)
            if value is None:
                return None
            turns.append(
                EvalTurnResult(
                    index=index,
                    input=prompt,
                    output=EvalObservation.model_validate(value.model_dump(exclude={"turns"})),
                )
            )
            if value.status != "completed":
                break
        return conversation_output(turns)
