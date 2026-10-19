"""唯一item执行算法；SRC-M2-02取消语义，丢回程不重复目标或评分。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import cast

from pydantic import JsonValue

from krim_agent.contracts import RunView, ServiceError
from krim_agent.runtime.contracts import settled
from krim_agent.runtime.service import RunService
from krim_agent.storage.sqlite import SQLiteStore

from . import scorers
from .targets import RunTarget
from .trace_cases import observe_snapshot
from .types import EvalOutput, FrozenCase, RuleSpec, Score


@dataclass
class ItemWork:
    runs: RunService
    admission: asyncio.Task[RunView] | None = None
    run_id: str | None = None
    scorer: asyncio.Task[Score] | None = None

    async def cleanup(self) -> None:
        if self.scorer is not None:
            if not self.scorer.done():
                self.scorer.cancel()
            await asyncio.gather(self.scorer, return_exceptions=True)
        if self.admission is not None:
            await asyncio.gather(self.admission, return_exceptions=True)
            if not self.admission.cancelled() and self.admission.exception() is None:
                self.run_id = self.admission.result().id
        if self.run_id is not None:
            await self.runs.cancel(self.run_id)


async def execute_item(
    eval_id: str,
    repetition: int,
    case: FrozenCase,
    rules: list[RuleSpec],
    store: SQLiteStore,
    work: ItemWork,
    *,
    item_timeout_seconds: float = 12,
) -> None:
    if type(item_timeout_seconds) not in {int, float} or not (
        item_timeout_seconds == 12 or 210 <= item_timeout_seconds <= 270
    ):
        raise ServiceError("invalid_input", 422)
    output: EvalOutput | None = None
    scores: list[Score] = []
    timed_out = False
    success = False
    try:
        try:
            async with asyncio.timeout(item_timeout_seconds):
                work.admission = asyncio.create_task(
                    RunTarget(work.runs).start(eval_id, case.case_id, repetition)
                )
                run = await asyncio.shield(work.admission)
                work.run_id = run.id
                await store.attach_child(eval_id, case.case_id, repetition, run.id)
                await work.runs.wait(run.id)
                try:
                    output = observe_snapshot(await store.snapshot(run.id))
                except ServiceError as error:
                    if error.code != "facts_corrupt":
                        raise
                    scores = [
                        scorers.result(rule, "error", None, "facts_corrupt") for rule in rules
                    ]
                for rule in rules if output is not None else []:
                    assert output is not None
                    expected_metadata = scorers.result(rule, "scored", 1).rule
                    work.scorer = asyncio.create_task(
                        scorers.score_rule(
                            rule.model_copy(deep=True),
                            case.reference.model_copy(deep=True),
                            output.model_copy(deep=True),
                        )
                    )
                    try:
                        async with asyncio.timeout(1):
                            value = await asyncio.shield(work.scorer)
                        value = Score.model_validate(value.model_dump(mode="json"))
                        if value.rule != expected_metadata:
                            raise ValueError("评分身份变化")
                    except TimeoutError:
                        work.scorer.cancel()
                        await settled(asyncio.gather(work.scorer, return_exceptions=True))
                        value = scorers.result(rule, "error", None, "scorer_timeout")
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        value = scorers.result(rule, "error", None, "scorer_error")
                    scores.append(value)
                    work.scorer = None
        except TimeoutError:
            timed_out = True
            await settled(work.cleanup())
            # 没有真正评分的单元不伪装成已评分；显式记录本item期限失败。
            scores = [scorers.result(r, "skipped", None, "item_timeout") for r in rules]
        body: dict[str, JsonValue] = {
            "output": output.model_dump(mode="json") if output is not None else None,
            "scores": cast(JsonValue, [s.model_dump(mode="json") for s in scores]),
            "quality": scorers.classify(scores, timed_out=timed_out),
            "code": "run_timeout" if timed_out else None,
        }
        await store.commit_item(eval_id, case.case_id, repetition, body)
        success = True
    finally:
        if not success:
            await settled(work.cleanup())
