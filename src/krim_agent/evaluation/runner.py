"""唯一Trial执行算法：两种后端共享准入、采证、评分、提交和取消join。"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from krim_agent.contracts import ServiceError

from .domain import ExecutionRef, TrialSpec, clone
from .lifecycle import settled
from .metrics import classify
from .ports import AgentAdapter, EvalRepository, MetricPort, PagedEvalRepository, RetiringAdapter
from .types import Score


@dataclass
class ItemWork:
    adapter: AgentAdapter | None
    admission: asyncio.Task[ExecutionRef] | None = None
    ref: ExecutionRef | None = None
    scorer: asyncio.Task[dict[str, Any]] | None = None
    cleanup_unknown: bool = False
    observation: dict[str, Any] | None = None

    async def cleanup(self) -> None:
        if self.scorer is not None:
            if not self.scorer.done():
                self.scorer.cancel()
            await asyncio.gather(self.scorer, return_exceptions=True)
        if self.admission is not None:
            # execute尚未交回ref时不能取消admission、遗失真实进程身份。
            await asyncio.gather(self.admission, return_exceptions=True)
            if not self.admission.cancelled() and self.admission.exception() is None:
                self.ref = self.admission.result()
        if self.ref is not None:
            if self.adapter is None:
                raise ServiceError("facts_corrupt")
            cleanup = await self.adapter.cancel(self.ref)
            self.observation = cleanup.observation
            self.cleanup_unknown = not cleanup.closed
            if self.cleanup_unknown:
                raise ServiceError("execution_unknown")


async def execute_item(
    trial: TrialSpec,
    rules: list[dict[str, Any]],
    repository: EvalRepository,
    work: ItemWork,
    metrics: MetricPort,
    *,
    item_timeout_seconds: float,
    scorer_timeout_seconds: float,
    guard: Callable[[], None],
    retire_completed: bool = False,
) -> None:
    output: dict[str, Any] | None = None
    scores: list[dict[str, Any]] = []
    timed_out = False
    success = False

    def fence() -> None:
        guard()
        if work.adapter is not None:
            work.adapter.assert_current(trial.target)

    try:
        try:
            async with asyncio.timeout(item_timeout_seconds):
                fence()
                if work.adapter is None:
                    output = clone(work.observation)
                else:
                    prepared = await work.adapter.prepare(trial)
                    fence()
                    await repository.intent(trial)
                    try:
                        fence()
                    except ServiceError as error:
                        if (
                            retire_completed
                            and error.code == "target_binding_changed"
                            and work.admission is None
                            and work.ref is None
                        ):
                            record_not_admitted = getattr(guard, "_record_not_admitted", None)
                            if record_not_admitted is not None:
                                record_not_admitted(trial, work)
                        raise
                    work.admission = asyncio.create_task(work.adapter.execute(prepared))
                    work.ref = await asyncio.shield(work.admission)
                    fence()
                    await repository.attach(trial, work.ref)
                    fence()
                    try:
                        output = await work.adapter.collect(work.ref)
                        await repository.record(trial, work.ref, output)
                        fence()
                    except ServiceError as error:
                        if error.code != "facts_corrupt":
                            raise
                        # 未通过原record的副本不能参与评分或提交；finally只记录
                        # 实际owner清理取得的同一ref事实，失败交原kernel收束。
                        output = None
                        raise
                if output is not None or work.adapter is None:
                    scores = await score_observation(
                        trial, rules, work, metrics, output, scorer_timeout_seconds, fence
                    )
                    fence()
        except TimeoutError:
            timed_out = True
            await settled(work.cleanup())
            output = output or work.observation
            if work.ref is not None:
                await settled(repository.record(trial, work.ref, output))
            scores = [metrics.result(r, "skipped", None, "item_timeout") for r in rules]
        body = {
            "output": output,
            "scores": scores,
            "quality": classify([Score.model_validate(s) for s in scores], timed_out=timed_out),
            "code": "run_timeout" if timed_out else None,
        }
        fence()
        if retire_completed and work.ref is not None:
            if not isinstance(work.adapter, RetiringAdapter) or not isinstance(
                repository, PagedEvalRepository
            ):
                raise ServiceError("facts_corrupt")
            receipt = work.adapter.retirement_receipt(work.ref)
            await repository.commit_with_receipt(trial, body, receipt)
            fence()
            work.adapter.retire(work.ref, receipt)
        else:
            await repository.commit(trial, body)
            fence()
        success = True
    except BaseException as error:
        notification = getattr(guard, "failed", None)
        if notification is not None:
            notification(error)
        raise
    finally:
        if not success:
            try:
                await settled(work.cleanup())
            finally:
                if work.ref is not None:
                    await settled(repository.record(trial, work.ref, output or work.observation))


async def score_observation(
    trial: TrialSpec,
    rules: list[dict[str, Any]],
    work: ItemWork,
    metrics: MetricPort,
    output: dict[str, Any] | None,
    scorer_timeout_seconds: float,
    fence: Callable[[], None],
) -> list[dict[str, Any]]:
    scores: list[dict[str, Any]] = []
    for rule in rules:
        fence()
        if output is None:
            scores.append(metrics.result(rule, "skipped", None, "output_missing"))
            continue
        availability = trial.case.get("_continuous", {}).get("availability")
        if availability is not None and (
            (rule["name"] == "krim_result" and not availability.get("text"))
            or (
                rule["name"] in {"native_proof", "process_constraints"}
                and not availability.get("arguments")
            )
        ):
            scores.append(metrics.result(rule, "skipped", None, "evidence_redacted"))
            continue
        if (
            rule["name"] == "artifact_match"
            and availability is not None
            and not set(trial.case["reference"].get("files", {})) <= set(availability["artifacts"])
        ):
            scores.append(metrics.result(rule, "skipped", None, "evidence_redacted"))
            continue
        identity = metrics.identity(rule)
        work.scorer = asyncio.create_task(
            metrics.score(clone(rule), clone(trial.case), clone(output))
        )
        try:
            async with asyncio.timeout(scorer_timeout_seconds):
                value = await asyncio.shield(work.scorer)
            fence()
            value = Score.model_validate(value).model_dump(mode="json")
            if value["rule"] != identity:
                raise ValueError("评分身份变化")
        except TimeoutError:
            work.scorer.cancel()
            await settled(asyncio.gather(work.scorer, return_exceptions=True))
            fence()
            value = metrics.result(rule, "error", None, "scorer_timeout")
        except asyncio.CancelledError:
            raise
        except Exception:
            value = metrics.result(rule, "error", None, "scorer_error")
        fence()
        scores.append(value)
        work.scorer = None
    return scores
