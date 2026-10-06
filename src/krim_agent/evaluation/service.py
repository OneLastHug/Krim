"""持久评估应用服务：保存输出和逐项评分，复用目标执行与评分内核。"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Sequence
from typing import Protocol
from uuid import uuid4

from pydantic import JsonValue

from krim_agent.persistence import Database
from krim_agent.persistence.eval_store import EvaluationStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import utc_now

from .jobs import (
    EvalEvent,
    EvalItem,
    EvalItemPage,
    EvalJobSpec,
    EvalJobView,
    EvalPartialReport,
    EvalResumeRequest,
    JobStatus,
)
from .runner import EvalAttempt, EvalCleanupError, EvalScoringUnknown, EvalTarget, _score, _scorers
from .scorers import EvalScorer
from .types import (
    EvalCase,
    EvalCaseResult,
    EvalConfig,
    EvalDataset,
    EvalOutput,
    EvalReport,
    EvalScore,
    classify,
    summarize,
)


async def _commit[T](operation: Awaitable[T]) -> T:
    task = asyncio.ensure_future(operation)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


class RecoverableTarget(Protocol):
    async def recover(self, case: EvalCase, attempt: EvalAttempt) -> EvalOutput | None: ...


class EvalRunHandle:
    """只控制自己的评估批次；结束读取不等于取消生产。"""

    def __init__(self, service: EvaluationService, job_id: str) -> None:
        self.service, self.id = service, job_id

    async def view(self) -> EvalJobView:
        return await self.service.store.view(self.id)

    async def items(self, *, offset: int = 0, limit: int = 100) -> EvalItemPage:
        return await self.service.store.items(self.id, offset=offset, limit=limit)

    async def partial_report(self) -> EvalPartialReport:
        return await self.service.partial_report(self.id)

    async def report(self) -> EvalReport:
        return await self.service.store.report(self.id)

    async def wait(self) -> EvalJobView:
        task = self.service.tasks.get(self.id)
        if task is not None:
            await asyncio.shield(task)
        else:
            while (await self.view()).status in {"queued", "running"}:  # noqa: ASYNC110 -- 跨进程只读轮询
                if (await self.view()).can_resume:
                    break
                await asyncio.sleep(0.05)
        return await self.view()

    async def pause(self) -> EvalJobView:
        return await self.service.stop(self.id, "pause")

    async def cancel(self) -> EvalJobView:
        return await self.service.stop(self.id, "cancel")

    async def resume(
        self, *, retry_uncertain: bool = False, skip_uncertain: bool = False
    ) -> EvalRunHandle:
        return await self.service.resume(
            self.id,
            request=EvalResumeRequest(
                retry_uncertain=retry_uncertain, skip_uncertain=skip_uncertain
            ),
        )

    async def events(self, from_seq: int = 0) -> AsyncIterator[EvalEvent | None]:
        cursor = from_seq
        heartbeat = time.monotonic()
        while True:
            records = await self.service.store.events(self.id, cursor)
            for record in records:
                cursor = record.seq + 1
                heartbeat = time.monotonic()
                yield record
            view = await self.view()
            if (
                view.status not in {"queued", "running"} or view.can_resume
            ) and cursor >= view.event_count:
                return
            if time.monotonic() - heartbeat >= 15:
                yield None
                heartbeat = time.monotonic()
            await asyncio.sleep(0.05)


class EvaluationService:
    def __init__(
        self,
        database: Database,
        *,
        scope: str = "python",
        lease_seconds: float = 30,
        max_concurrency: int = 16,
    ) -> None:
        self.store = EvaluationStore(database, scope=scope, lease_seconds=lease_seconds)
        self.owner = str(uuid4())
        self.tasks: dict[str, asyncio.Task[None]] = {}
        self.programs: dict[str, tuple[EvalTarget, list[EvalScorer]]] = {}
        self.pool = asyncio.Semaphore(max_concurrency)
        self.closed = False
        self.started: dict[str, asyncio.Event] = {}

    async def start(
        self,
        dataset: EvalDataset,
        target: EvalTarget,
        *,
        scorers: Sequence[EvalScorer] | None = None,
        config: EvalConfig | None = None,
        request_key: str | None = None,
    ) -> EvalRunHandle:
        prepared = asyncio.create_task(
            self._start(dataset, target, scorers=scorers, config=config, request_key=request_key)
        )
        try:
            return await asyncio.shield(prepared)
        except asyncio.CancelledError:
            handle = await prepared
            await self.stop(handle.id, "pause")
            raise

    async def _start(
        self,
        dataset: EvalDataset,
        target: EvalTarget,
        *,
        scorers: Sequence[EvalScorer] | None = None,
        config: EvalConfig | None = None,
        request_key: str | None = None,
    ) -> EvalRunHandle:
        if self.closed:
            raise ServiceError("eval_service_closed", "评估服务已关闭", 503)
        selected, infos = _scorers(scorers)
        spec = EvalJobSpec(
            dataset=dataset, target=target.describe(), config=config or EvalConfig(), scorers=infos
        )
        job_id, created = await self.store.create(spec, request_key)
        self.programs[job_id] = (target, selected)
        if created:
            await self._launch(job_id, target, selected, EvalResumeRequest())
        return EvalRunHandle(self, job_id)

    def handle(self, job_id: str) -> EvalRunHandle:
        return EvalRunHandle(self, job_id)

    async def _items(self, job_id: str) -> list[EvalItem]:
        values = []
        offset = 0
        while True:
            page = await self.store.items(job_id, offset=offset, limit=1000)
            values.extend(page.items)
            offset += len(page.items)
            if offset >= page.total:
                return values

    async def partial_report(self, job_id: str) -> EvalPartialReport:
        spec = await self.store.spec(job_id)
        results = [i.result for i in await self._items(job_id) if i.result is not None]
        return EvalPartialReport(
            job=await self.store.view(job_id),
            spec=spec,
            results=results,
            summary=summarize(results, spec.scorers) if results else None,
        )

    async def resume(
        self,
        job_id: str,
        *,
        target: EvalTarget | None = None,
        scorers: Sequence[EvalScorer] | None = None,
        request: EvalResumeRequest | None = None,
    ) -> EvalRunHandle:
        prepared = asyncio.create_task(
            self._resume(job_id, target=target, scorers=scorers, request=request)
        )
        try:
            return await asyncio.shield(prepared)
        except asyncio.CancelledError:
            handle = await prepared
            await self.stop(handle.id, "pause")
            raise

    async def _resume(
        self,
        job_id: str,
        *,
        target: EvalTarget | None,
        scorers: Sequence[EvalScorer] | None,
        request: EvalResumeRequest | None,
    ) -> EvalRunHandle:
        program = self.programs.get(job_id)
        chosen = target or (program[0] if program else None)
        if chosen is None:
            raise ServiceError("eval_program_missing", "恢复需要可信目标和评分器", 409)
        selected, infos = _scorers(
            scorers if scorers is not None else program[1] if program else None
        )
        spec = await self.store.spec(job_id)
        if spec.target != chosen.describe() or spec.scorers != infos:
            raise ServiceError("eval_program_changed", "目标或评分配置已变化，不能恢复", 409)
        self.programs[job_id] = (chosen, selected)
        await self._launch(job_id, chosen, selected, request or EvalResumeRequest())
        return self.handle(job_id)

    async def _launch(
        self, job_id: str, target: EvalTarget, scorers: list[EvalScorer], request: EvalResumeRequest
    ) -> None:
        if self.closed:
            raise ServiceError("eval_service_closed", "评估服务已关闭", 503)
        current = self.tasks.get(job_id)
        if current and not current.done():
            raise ServiceError("eval_busy", "评估已在当前进程执行", 409)
        epoch = await self.store.claim(job_id, self.owner)
        self.started[job_id] = asyncio.Event()
        self.tasks[job_id] = asyncio.create_task(
            self._drive(job_id, epoch, target, scorers, request), name=f"krim-eval-job-{job_id}"
        )
        await self.started[job_id].wait()

    async def stop(self, job_id: str, action: str) -> EvalJobView:
        await self.store.control(job_id, action)
        task = self.tasks.get(job_id)
        if task and not task.done():
            task.cancel()
            await asyncio.shield(task)
        return await self.store.view(job_id)

    async def _drive(
        self,
        job_id: str,
        epoch: int,
        target: EvalTarget,
        scorers: list[EvalScorer],
        request: EvalResumeRequest,
    ) -> None:
        heartbeat: asyncio.Task[None] | None = None
        workers: list[asyncio.Task[None]] = []
        parent = asyncio.current_task()
        lease_lost = False

        async def renew() -> None:
            nonlocal lease_lost
            try:
                while True:
                    await asyncio.sleep(self.store.lease_seconds / 3)
                    await self.store.renew(job_id, self.owner, epoch)
                    view = await self.store.view(job_id)
                    if view.control is not None and parent is not None:
                        parent.cancel()
                        return
            except Exception:
                lease_lost = True
                if parent is not None:
                    parent.cancel()

        try:
            self.started[job_id].set()
            spec = await self.store.spec(job_id)
            heartbeat = asyncio.create_task(renew())
            pending = iter([item for item in await self._items(job_id) if item.phase != "done"])

            async def worker() -> None:
                for item in pending:
                    async with self.pool:
                        await self._run_item(job_id, epoch, spec, item, target, scorers, request)

            workers = [asyncio.create_task(worker()) for _ in range(spec.config.concurrency)]
            await asyncio.gather(*workers)
            items = await self._items(job_id)
            if any(item.phase in {"uncertain", "grading_uncertain"} for item in items):
                await self.store.finish(
                    job_id,
                    self.owner,
                    epoch,
                    "interrupted",
                    error_code=(
                        "eval_grading_resolution_required"
                        if any(item.phase == "grading_uncertain" for item in items)
                        else "eval_resolution_required"
                    ),
                )
                return
            results = [item.result for item in items if item.result is not None]
            view = await self.store.view(job_id)
            report = EvalReport(
                id=job_id,
                created_at=view.created_at,
                finished_at=utc_now(),
                dataset=spec.dataset,
                dataset_fingerprint=spec.dataset.fingerprint,
                target=spec.target,
                config=spec.config,
                scorers=spec.scorers,
                results=results,
                summary=summarize(results, spec.scorers),
            )
            await self.store.finish(job_id, self.owner, epoch, "completed", report=report)
        except asyncio.CancelledError:
            for task_to_cancel in workers:
                task_to_cancel.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            if not lease_lost:
                view = await self.store.view(job_id)
                state: JobStatus = "cancelled" if view.control == "cancel" else "paused"
                try:
                    await _commit(self.store.finish(job_id, self.owner, epoch, state))
                except ServiceError as error:
                    if error.code != "eval_lease_lost":
                        raise
        except Exception:
            for task_to_cancel in workers:
                task_to_cancel.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            if not lease_lost:
                try:
                    await self.store.finish(
                        job_id, self.owner, epoch, "failed", error_code="eval_job_failed"
                    )
                except ServiceError as error:
                    if error.code != "eval_lease_lost":
                        raise
        finally:
            if heartbeat is not None:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)

    async def _run_item(
        self,
        job_id: str,
        epoch: int,
        spec: EvalJobSpec,
        item: EvalItem,
        target: EvalTarget,
        scorers: list[EvalScorer],
        request: EvalResumeRequest,
    ) -> None:
        case = spec.dataset.cases[item.ordinal // spec.config.repetitions]
        bindings = dict(item.bindings)
        target_bindings = bindings.get("target", {})
        score_bindings = bindings.get("scores", {})
        if not isinstance(target_bindings, dict) or not isinstance(score_bindings, dict):
            raise ServiceError("eval_binding_invalid", "评估绑定结构无效", 409)

        async def binding(values: dict[str, JsonValue]) -> None:
            nonlocal target_bindings
            target_bindings = values
            await self.store.mutate(
                job_id,
                self.owner,
                epoch,
                item.ordinal,
                bindings={**bindings, "target": target_bindings, "scores": score_bindings},
            )

        async def score_binding(values: dict[str, JsonValue]) -> None:
            nonlocal score_bindings
            score_bindings = values
            await self.store.mutate(
                job_id,
                self.owner,
                epoch,
                item.ordinal,
                bindings={**bindings, "target": target_bindings, "scores": score_bindings},
            )

        attempt = EvalAttempt(
            job_id,
            case.id,
            item.repetition,
            generation=item.generation,
            bindings=target_bindings,
            checkpoint=binding,
            score_bindings=score_bindings,
            score_checkpoint=score_binding,
            retry_scoring=request.retry_uncertain,
            skip_scoring=request.skip_uncertain,
        )
        output, error_code = item.output, item.error_code
        started = time.monotonic()
        duration = item.duration_ms
        execution_finished = item.phase in {"executed", "scoring", "grading_uncertain"}
        score_started: float | None = None
        try:
            if item.phase in {"executing", "uncertain"}:
                recover = getattr(target, "recover", None)
                try:
                    if recover is None:
                        raise ServiceError("eval_resolution_required", "目标不能自动恢复", 409)
                    recovered = await recover(case, attempt)
                    if recovered is not None:
                        output = recovered
                        if recovered.status in {"aborted", "interrupted"}:
                            raise ServiceError("eval_resolution_required", "原运行已中断", 409)
                        if error_code == "eval_resolution_required":
                            error_code = None
                        await _commit(
                            self.store.mutate(
                                job_id,
                                self.owner,
                                epoch,
                                item.ordinal,
                                phase="executed",
                                output=output,
                                error_code=error_code,
                            )
                        )
                        item.phase = "executed"
                        execution_finished = True
                    else:
                        item.phase = "pending"
                        error_code = None
                except Exception:
                    await _commit(
                        self.store.mutate(
                            job_id,
                            self.owner,
                            epoch,
                            item.ordinal,
                            phase="uncertain",
                            output=output,
                            error_code="eval_resolution_required",
                        )
                    )
                    if request.retry_uncertain:
                        # 活跃源仍拒绝；显式重试也不能并发重放同一活跃生产。
                        if recover is not None:
                            try:
                                await recover(case, attempt)
                            except ServiceError as error:
                                if error.code == "eval_source_active":
                                    return
                        await _commit(
                            self.store.resolve(job_id, self.owner, epoch, item.ordinal, "retry")
                        )
                        refreshed = await self.store.item(job_id, item.ordinal)
                        await self._run_item(
                            job_id, epoch, spec, refreshed, target, scorers, EvalResumeRequest()
                        )
                    elif request.skip_uncertain:
                        scores = [
                            EvalScore(name=s.name, status="skipped", reason="execution_unknown")
                            for s in spec.scorers
                        ]
                        result = EvalCaseResult(
                            case_id=case.id,
                            repetition=item.repetition,
                            status="error",
                            output=output,
                            scores=scores,
                            error_code="eval_skipped",
                            duration_ms=item.duration_ms,
                            scoring_ms=item.scoring_ms,
                        )
                        await _commit(
                            self.store.mutate(
                                job_id, self.owner, epoch, item.ordinal, result=result
                            )
                        )
                    return
            if item.phase == "pending":
                await _commit(
                    self.store.mutate(job_id, self.owner, epoch, item.ordinal, phase="executing")
                )
                try:
                    async with asyncio.timeout(spec.config.timeout_seconds):
                        value = await target.run(case.model_copy(deep=True), attempt)
                    output = EvalOutput.model_validate(value.model_dump())
                except EvalCleanupError:
                    raise
                except TimeoutError:
                    output, error_code = attempt.output, "eval_timeout"
                except Exception:
                    output, error_code = attempt.output, "eval_execution_error"
                duration = (time.monotonic() - started) * 1000
                execution_finished = True
                await _commit(
                    self.store.mutate(
                        job_id,
                        self.owner,
                        epoch,
                        item.ordinal,
                        phase="executed",
                        output=output,
                        error_code=error_code,
                        duration_ms=duration,
                    )
                )
            else:
                duration = item.duration_ms
            await _commit(
                self.store.mutate(
                    job_id, self.owner, epoch, item.ordinal, phase="scoring", error_code=error_code
                )
            )

            async def score_checkpoint(scores: list[EvalScore], added: float) -> None:
                await _commit(
                    self.store.mutate(
                        job_id,
                        self.owner,
                        epoch,
                        item.ordinal,
                        scores=scores,
                        error_code=error_code,
                        scoring_ms=item.scoring_ms + added,
                    )
                )

            score_started = time.monotonic()
            try:
                scores, added = await _score(
                    case,
                    output,
                    error_code,
                    scorers,
                    spec.scorers,
                    spec.config,
                    saved_scores=item.scores,
                    on_score=score_checkpoint,
                    attempt=attempt,
                )
            except EvalScoringUnknown:
                await _commit(
                    self.store.mutate(
                        job_id,
                        self.owner,
                        epoch,
                        item.ordinal,
                        phase="grading_uncertain",
                        output=output,
                        error_code=error_code,
                        duration_ms=duration,
                        scoring_ms=item.scoring_ms + (time.monotonic() - score_started) * 1000,
                    )
                )
                return
            result = EvalCaseResult(
                case_id=case.id,
                repetition=item.repetition,
                status=classify(output, scores, spec.config, error_code, case),
                output=output,
                scores=scores,
                error_code=error_code,
                duration_ms=duration,
                scoring_ms=item.scoring_ms + added,
            )
            await _commit(
                self.store.mutate(
                    job_id, self.owner, epoch, item.ordinal, result=result, error_code=error_code
                )
            )
        except asyncio.CancelledError:
            if item.phase == "pending" and attempt.output is not None:
                output = attempt.output
            phase = (
                "scoring" if output is not None and output.status == "completed" else "uncertain"
            )
            try:
                await _commit(
                    self.store.mutate(
                        job_id,
                        self.owner,
                        epoch,
                        item.ordinal,
                        phase=phase,
                        output=output or attempt.output,
                        error_code=error_code,
                        duration_ms=duration
                        if execution_finished
                        else (time.monotonic() - started) * 1000,
                        scoring_ms=item.scoring_ms + (time.monotonic() - score_started) * 1000
                        if score_started is not None
                        else item.scoring_ms,
                    )
                )
            except ServiceError:
                pass
            raise

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        for job_id, task in list(self.tasks.items()):
            if not task.done():
                await self.stop(job_id, "pause")
