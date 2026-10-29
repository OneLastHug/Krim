"""唯一Eval任务所有者；由原EvaluationService抽取，后端只转换契约。"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from krim_agent.contracts import ServiceError, fingerprint

from .domain import clone
from .lifecycle import settled, task_done
from .ports import AgentAdapter, EvalRepository, PagedEvalRepository, method_identities
from .runner import ItemWork, execute_item


class EvalKernel:
    def __init__(self, repository: EvalRepository) -> None:
        self.repository = repository
        self.tasks: dict[str, asyncio.Task[None]] = {}
        self.current: dict[str, ItemWork] = {}
        self._inflight: dict[tuple[str, str, int], ItemWork] = {}
        self._workers: dict[str, set[asyncio.Task[None]]] = {}
        self._failure_facts: dict[str, list[dict[str, str]]] = {}
        self._high_water: dict[str, dict[str, int]] = {}
        self._admission = asyncio.Lock()
        self._close_task: asyncio.Task[None] | None = None
        self._finalizing: set[str] = set()
        self._cancelled: set[str] = set()
        self.closed = False
        self.cleanup_unknown = False

    def _external(self) -> None:
        from .ports import _admission_fence

        task = asyncio.current_task()
        host = _admission_fence().get()
        owned = (
            None
            if host is None or host[0] is not self.repository
            else getattr(host[1], "_eval_owner", None)
        )
        if (
            owned is not None
            and owned[0] is self
            and owned[2][0]
            and any(owned[1] is t and not t.done() for t in self.tasks.values())
        ):
            raise ServiceError("self_wait")
        if (
            task in self.tasks.values()
            or any(task in (w.admission, w.scorer) for w in self.current.values())
            or any(task in group for group in self._workers.values())
            or any(task in (w.admission, w.scorer) for w in self._inflight.values())
        ):
            raise ServiceError("self_wait")

    async def submit(
        self, request: dict[str, Any], *, host_guard: Callable[[], None] | None = None
    ) -> dict[str, Any]:
        frozen = clone(request)
        return await settled(self._admit(frozen, host_guard=host_guard))

    async def _admit(
        self, request: dict[str, Any], *, host_guard: Callable[[], None] | None = None
    ) -> dict[str, Any]:
        from .ports import _admission_fence

        async with self._admission:
            if host_guard is not None:
                host_guard()
            if self.closed:
                raise ServiceError("closed", 503)
            if not self.repository.healthy or self.cleanup_unknown:
                raise ServiceError("storage_unavailable", 503)
            old = await self.repository.by_key(request)
            if host_guard is not None:
                host_guard()
            if old is not None:
                if old["request_hash"] != fingerprint(request):
                    raise ServiceError("key_conflict")
                return old
            if any(not t.done() for t in self.tasks.values()):
                raise ServiceError("capacity", 429)
            mode = self.repository.mode(request)
            adapter = self.repository.adapter(request) if mode == "execute" else None
            identities = None if adapter is None else method_identities(adapter)
            repository = self.repository
            paged = isinstance(repository, PagedEvalRepository)
            page_names = [
                "execution_header",
                "execution_page",
                "read_trial",
                "complete",
                "record_resource_receipt",
                "commit_with_receipt",
            ]
            page_methods = (
                tuple(
                    getattr(getattr(repository, n), "__func__", getattr(repository, n))
                    for n in page_names
                )
                if paged
                else ()
            )
            if adapter is not None:
                requested = request["execution"]["concurrency"]
                maximum = adapter.descriptor().get("max_concurrency", 1)
                if type(maximum) is not int or requested > maximum:
                    raise ServiceError("invalid_input", 422)
            metrics = self.repository.metrics()
            metric_methods = tuple(
                getattr(getattr(metrics, n), "__func__", getattr(metrics, n))
                for n in ["score", "identity", "result"]
            )

            def guard() -> None:
                if self.repository is not repository or (
                    paged
                    and tuple(
                        getattr(getattr(repository, n), "__func__", getattr(repository, n))
                        for n in page_names
                    )
                    != page_methods
                ):
                    raise ServiceError("target_binding_changed")
                if host_guard is not None:
                    host_guard()
                if adapter is not None and (
                    self.repository.adapter(request) is not adapter
                    or method_identities(adapter) != identities
                ):
                    raise ServiceError("target_binding_changed")
                if (request.get("schema_version") == "continuous-task/1" or paged) and (
                    self.repository.metrics() is not metrics
                    or tuple(
                        getattr(getattr(metrics, n), "__func__", getattr(metrics, n))
                        for n in ["score", "identity", "result"]
                    )
                    != metric_methods
                ):
                    raise ServiceError("target_binding_changed")

            guard()
            context = _admission_fence()
            token = context.set((self.repository, guard))
            try:
                snapshot, created = await self.repository.begin(request)
            finally:
                context.reset(token)
            if created:
                try:
                    guard()
                    task = asyncio.create_task(
                        self._drive(
                            snapshot["id"],
                            adapter,
                            guard,
                            float(snapshot["execution"]["batch_timeout_seconds"]),
                        )
                    )
                    self.tasks[snapshot["id"]] = task
                    task.add_done_callback(task_done)
                except ServiceError as error:
                    await self.repository.finish(snapshot["id"], "failed", error.code)
                    raise
                except Exception:
                    await self.repository.finish(snapshot["id"], "failed", "execution_error")
                    raise ServiceError("execution_error") from None
            return snapshot

    async def _drive(
        self,
        task_id: str,
        adapter: AgentAdapter | None,
        guard: Callable[[], None],
        batch_seconds: float,
    ) -> None:
        from .ports import _admission_fence

        if self.tasks.get(task_id) is not asyncio.current_task():
            raise ServiceError("permission_denied")
        failure: tuple[str, str] | None = None
        stopping = False
        children: set[asyncio.Task[None]] = set()
        first_failure: asyncio.Future[BaseException] = asyncio.get_running_loop().create_future()
        self._failure_facts[task_id] = []
        self._high_water[task_id] = {"queue": 0, "inflight": 0, "workers": 0}
        repository = self.repository
        context = _admission_fence()
        service = self
        producer_task = asyncio.current_task()
        producer_live = [True]
        from .domain import TrialSpec

        not_admitted: dict[tuple[str, int], tuple[TrialSpec, ItemWork, asyncio.Task[None]]] = {}
        not_admitted_binding: tuple[str, str, str, str] | None = None

        class DriverFence:
            _eval_owner = (service, producer_task, producer_live)

            def __call__(self) -> None:
                guard()

            def _no_admission_facts(
                self,
                bound_repository: object,
                bound_task: str,
                binding: tuple[str, str, str, str],
            ) -> tuple[tuple[str, str, str, str, str, int], ...]:
                # 这里只核事实归属，不再次调用已经拒绝的执行权限。
                if not not_admitted:
                    return ()
                if (
                    context.get() is not driver_context
                    or driver_context is None
                    or driver_context[1] is not self
                    or bound_repository is not repository
                    or service.repository is not repository
                    or bound_task != task_id
                    or not_admitted_binding != binding
                    or not producer_live[0]
                    or producer_task is None
                    or producer_task.done()
                    or service.tasks.get(task_id) is not producer_task
                    or task_id not in service._finalizing
                    or service._workers.get(task_id) is not children
                    or any(not child.done() for child in children)
                    or any(key[0] == task_id for key in service._inflight)
                    or len(not_admitted) > plan.concurrency
                ):
                    raise ServiceError("permission_denied")
                facts: list[tuple[str, str, str, str, str, int]] = []
                for key, (trial, work, worker_task) in sorted(not_admitted.items()):
                    if (
                        trial.task_id != task_id
                        or (trial.case_id, trial.repetition) != key
                        or worker_task not in children
                        or not worker_task.done()
                        or work.adapter is not adapter
                        or work.admission is not None
                        or work.ref is not None
                        or work.scorer is not None
                        or work.observation is not None
                        or work.cleanup_unknown
                    ):
                        raise ServiceError("permission_denied")
                    facts.append((*binding, *key))
                return tuple(facts)

        parent_token = context.set((repository, DriverFence()))
        driver_context = context.get()
        try:
            async with asyncio.timeout(batch_seconds):
                snapshot = await repository.snapshot(task_id)
                guard()
                plan = repository.plan(snapshot)
                if plan.paged:
                    not_admitted_binding = (
                        task_id,
                        snapshot["request_json"]["scope"],
                        snapshot["request_hash"],
                        snapshot["plan_fingerprint"],
                    )
                    if (
                        plan.plan_fingerprint != not_admitted_binding[3]
                        or fingerprint(snapshot["request_json"]) != not_admitted_binding[2]
                    ):
                        raise ServiceError("facts_corrupt")
                if plan.batch_timeout != batch_seconds:
                    raise ServiceError("facts_corrupt")
                paged_repository: PagedEvalRepository | None = None
                if plan.paged:
                    if not isinstance(repository, PagedEvalRepository):
                        raise ServiceError("facts_corrupt")
                    paged_repository = repository
                if plan.concurrency not in {1, 2}:
                    raise ServiceError("facts_corrupt")
                await repository.set_running(task_id)
                guard()
                queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=2)
                sentinel = object()

                def failed(error: BaseException) -> None:
                    nonlocal stopping
                    stopping = True
                    if not first_failure.done():
                        first_failure.set_result(error)
                    current = asyncio.current_task()
                    for child in children:
                        if child is not current and not child.done() and not child.cancelling():
                            child.cancel()

                async def put(value: Any) -> None:
                    if stopping:
                        raise asyncio.CancelledError
                    guard()
                    await queue.put(value)
                    guard()
                    self._high_water[task_id]["queue"] = max(
                        self._high_water[task_id]["queue"], queue.qsize()
                    )
                    if stopping:
                        raise asyncio.CancelledError

                async def producer() -> None:
                    try:
                        if plan.paged:
                            if paged_repository is None:
                                raise ServiceError("facts_corrupt")
                            cursor = None
                            while True:
                                page = await paged_repository.execution_page(
                                    task_id, cursor, limit=16
                                )
                                guard()
                                if page.plan_fingerprint != plan.plan_fingerprint:
                                    raise ServiceError("facts_corrupt")
                                for trial in page.trials:
                                    await put(trial)
                                cursor = page.next_cursor
                                if cursor is None:
                                    break
                        else:
                            for trial in plan.trials:
                                await put(trial)
                        for _ in range(plan.concurrency):
                            await put(sentinel)
                    except BaseException as error:
                        failed(error)
                        raise

                async def worker() -> None:
                    try:
                        while True:
                            trial = await queue.get()
                            guard()
                            if stopping:
                                raise asyncio.CancelledError
                            if trial is sentinel:
                                return
                            work = ItemWork(adapter)
                            key = (task_id, trial.case_id, trial.repetition)
                            self._inflight[key] = work
                            self.current.setdefault(task_id, work)
                            count = sum(k[0] == task_id for k in self._inflight)
                            self._high_water[task_id]["inflight"] = max(
                                self._high_water[task_id]["inflight"], count
                            )
                            incoming = (plan.scoring_inputs or {}).get(
                                (trial.case_id, trial.repetition), {}
                            )

                            call_live = [True]

                            class Fence:
                                _iteration_binding = (trial, work, key, call_live)

                                def __init__(self, incoming: dict[str, Any]) -> None:
                                    self._incoming = incoming

                                _eval_owner = (service, producer_task, call_live)

                                def __call__(self) -> None:
                                    if stopping:
                                        raise asyncio.CancelledError
                                    guard()
                                    if self._incoming.get("guard") is not None:
                                        self._incoming["guard"]()

                                def _record_not_admitted(
                                    self, actual_trial: TrialSpec, actual_work: ItemWork
                                ) -> None:
                                    trial, work, key, call_live = self._iteration_binding
                                    worker_task = asyncio.current_task()
                                    host = context.get()
                                    if (
                                        not plan.paged
                                        or plan.mode != "execute"
                                        or not_admitted_binding is None
                                        or host is None
                                        or host[0] is not repository
                                        or host[1] is not self
                                        or actual_trial is not trial
                                        or actual_work is not work
                                        or trial.task_id != task_id
                                        or trial.target != snapshot["target"]
                                        or not call_live[0]
                                        or not producer_live[0]
                                        or service.repository is not repository
                                        or service.tasks.get(task_id) is not producer_task
                                        or worker_task is None
                                        or worker_task not in children
                                        or service._workers.get(task_id) is not children
                                        or service._inflight.get(key) is not work
                                        or work.admission is not None
                                        or work.ref is not None
                                        or work.observation is not None
                                        or work.cleanup_unknown
                                        or len(not_admitted) >= plan.concurrency
                                    ):
                                        raise ServiceError("permission_denied")
                                    fact_key = (trial.case_id, trial.repetition)
                                    if fact_key in not_admitted:
                                        raise ServiceError("facts_corrupt")
                                    not_admitted[fact_key] = (trial, work, worker_task)

                                def failed(self, error: BaseException) -> None:
                                    failed(error)

                            fence = Fence(incoming)
                            token = context.set((repository, fence))
                            try:
                                fence()
                                if plan.mode != "execute":
                                    if plan.paged:
                                        if paged_repository is None:
                                            raise ServiceError("facts_corrupt")
                                        incoming = await paged_repository.read_trial(
                                            snapshot["request_json"]["scope"],
                                            task_id,
                                            trial.case_id,
                                            trial.repetition,
                                        )
                                        fence._incoming = incoming
                                        fence()
                                        trial.case["_continuous"] = incoming["_continuous"]
                                    work.observation = incoming.get("output")
                                await execute_item(
                                    trial,
                                    plan.rules,
                                    repository,
                                    work,
                                    repository.metrics(),
                                    item_timeout_seconds=plan.item_timeout,
                                    scorer_timeout_seconds=plan.scorer_timeout,
                                    guard=fence,
                                    retire_completed=plan.paged and plan.mode == "execute",
                                )
                            finally:
                                if work.cleanup_unknown:
                                    self.cleanup_unknown = True
                                call_live[0] = False
                                context.reset(token)
                                self._inflight.pop(key, None)
                                if self.current.get(task_id) is work:
                                    self.current.pop(task_id, None)
                                    remaining = next(
                                        (v for k, v in self._inflight.items() if k[0] == task_id),
                                        None,
                                    )
                                    if remaining is not None:
                                        self.current[task_id] = remaining
                    except BaseException as error:
                        failed(error)
                        raise

                self._workers[task_id] = children
                children.add(asyncio.create_task(producer()))
                for _ in range(plan.concurrency):
                    children.add(asyncio.create_task(worker()))
                self._high_water[task_id]["workers"] = plan.concurrency
                pending = set(children)
                while pending:
                    done, _ = await asyncio.wait(
                        pending | {first_failure}, return_when=asyncio.FIRST_COMPLETED
                    )
                    if first_failure.done():
                        raise first_failure.result()
                    for child in done:
                        if child in pending:
                            pending.remove(child)
                            if child.cancelled():
                                raise asyncio.CancelledError
                            error = child.exception()
                            if error is not None:
                                raise error
                if plan.paged:
                    if paged_repository is None:
                        raise ServiceError("facts_corrupt")
                    await paged_repository.complete(task_id)
                    guard()
        except asyncio.CancelledError:
            failure = ("aborted", "cancelled")
        except TimeoutError:
            failure = ("failed", "run_timeout")
        except ServiceError as error:
            failure = ("failed", error.code)
        except Exception:
            failure = ("failed", "execution_error")
        finally:
            stopping = True
            for child in children:
                if not child.done() and not child.cancelling():
                    child.cancel()
            outcomes = await settled(asyncio.gather(*children, return_exceptions=True))
            for outcome in outcomes:
                if isinstance(outcome, BaseException):
                    self._failure_facts[task_id].append(
                        {
                            "type": type(outcome).__name__,
                            "code": outcome.code
                            if isinstance(outcome, ServiceError)
                            else type(outcome).__name__,
                        }
                    )
            if not first_failure.done():
                first_failure.cancel()
            else:
                if not first_failure.cancelled():
                    first_error = first_failure.result()
                    self._failure_facts[task_id].append(
                        {
                            "type": type(first_error).__name__,
                            "code": first_error.code
                            if isinstance(first_error, ServiceError)
                            else type(first_error).__name__,
                        }
                    )
            if self.cleanup_unknown:
                failure = ("failed", "execution_unknown")
            self._finalizing.add(task_id)
            try:
                if failure is not None:
                    await repository.finish(task_id, *failure)
            finally:
                producer_live[0] = False
                context.reset(parent_token)
                self._workers.pop(task_id, None)
                self._finalizing.discard(task_id)
                self._cancelled.discard(task_id)

    async def wait(self, task_id: str) -> dict[str, Any]:
        self._external()
        task = self.tasks.get(task_id)
        if task is not None:
            await asyncio.shield(task)
        return await self.repository.snapshot(task_id)

    async def cancel(self, task_id: str) -> dict[str, Any]:
        self._external()
        return await settled(self._cancel(task_id))

    async def _cancel(self, task_id: str) -> dict[str, Any]:
        task = self.tasks.get(task_id)
        if task is None or task.done():
            return await self.repository.snapshot(task_id)
        error: Exception | None = None
        try:
            await self.repository.stop(task_id)
        except Exception as failure:
            error = failure
        finally:
            if (
                not task.done()
                and task_id not in self._finalizing
                and task_id not in self._cancelled
            ):
                self._cancelled.add(task_id)
                task.cancel()
            await settled(task)
        if error is not None:
            raise error
        return await self.repository.snapshot(task_id)

    async def close(self) -> None:
        self._external()
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await settled(self._close_task)

    async def _close(self) -> None:
        async with self._admission:
            self.closed = True
        outcomes = await asyncio.gather(
            *(self.cancel(key) for key, task in list(self.tasks.items()) if not task.done()),
            return_exceptions=True,
        )
        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                raise outcome
