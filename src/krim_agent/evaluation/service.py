"""一个有限批次所有者；借用唯一RunService/Store，不续算历史。"""

from __future__ import annotations

import asyncio
from typing import Any, cast

from pydantic import JsonValue

from krim_agent.contracts import ServiceError, encoded, fingerprint, parse_json
from krim_agent.runtime.contracts import settled
from krim_agent.runtime.service import RunService
from krim_agent.storage.sqlite import SQLiteStore

from .regression import gate_reports, make_report, validate_report
from .runner import ItemWork, execute_item
from .trace_cases import collect_candidate
from .types import (
    RULE_CONFIGS,
    Candidate,
    DatasetRevision,
    EvalRequest,
    EvalView,
    FreezeRequest,
)


def eval_view(snapshot: dict[str, Any]) -> EvalView:
    values = {k: snapshot[k] for k in EvalView.model_fields if k != "finished_count"}
    values["finished_count"] = sum(i["status"] == "finished" for i in snapshot["items"])
    return EvalView.model_validate(values)


class EvaluationService:
    def __init__(self, store: SQLiteStore, runs: RunService) -> None:
        self.store, self.runs = store, runs
        self.tasks: dict[str, asyncio.Task[None]] = {}
        self.current: dict[str, ItemWork] = {}
        self._admission = asyncio.Lock()
        self._close_task: asyncio.Task[None] | None = None
        self._finalizing: set[str] = set()
        self._cancelled: set[str] = set()
        self.closed = False

    def _external(self) -> None:
        task = asyncio.current_task()
        if task in self.tasks.values() or any(
            task in (w.admission, w.scorer) for w in self.current.values()
        ):
            raise ServiceError("self_wait")

    def targets(self) -> list[dict[str, JsonValue]]:
        return [
            cast(dict[str, JsonValue], parse_json(b.descriptor_json))
            for key, b in self.runs.bindings.items()
            if key != "demo"
        ]

    def rules(self) -> list[dict[str, JsonValue]]:
        return cast(
            list[dict[str, JsonValue]],
            parse_json(
                encoded(
                    [
                        {"name": name, "version": "1", "config": config}
                        for name, config in RULE_CONFIGS.items()
                    ]
                )
            ),
        )

    async def candidate(self, run_id: str, include_input: bool = False) -> Candidate:
        return collect_candidate(await self.store.snapshot(run_id), include_input)

    async def freeze(self, request: FreezeRequest) -> DatasetRevision:
        return await self.store.freeze_dataset(request)

    async def start(self, request: EvalRequest) -> EvalView:
        # 调用HTTP客户端取消不拥有批次；准入仍须收束，Application退出另行关闭自有任务。
        frozen = EvalRequest.model_validate(parse_json(encoded(request.model_dump(mode="json"))))
        return await settled(self._admit(frozen))

    async def _admit(self, request: EvalRequest) -> EvalView:
        async with self._admission:
            if self.closed:
                raise ServiceError("closed", 503)
            if not self.store.healthy:
                raise ServiceError("storage_unavailable", 503)
            old = await self.store.eval_by_key(request.request_key)
            if old is not None:
                if old["request_hash"] != fingerprint(request.model_dump(mode="json")):
                    raise ServiceError("key_conflict")
                return eval_view(old)
            if any(not t.done() for t in self.tasks.values()):
                raise ServiceError("capacity", 429)
            binding = self.runs.bindings.get(request.target_key)
            if binding is None:
                raise ServiceError("invalid_input", 422)
            binding.assert_current()
            target = cast(dict[str, JsonValue], parse_json(binding.descriptor_json))
            execution = request.execution.model_dump(mode="json") | {
                k: target[k] for k in ["limits", "policy", "pre", "post"]
            }
            try:
                snapshot, created = await self.store.begin_eval(request, target, execution)
            except ServiceError as error:
                if error.code == "not_found":
                    raise ServiceError("invalid_input", 422) from None
                raise
            if created:
                try:
                    task = asyncio.create_task(self._drive(snapshot["id"]))
                    self.tasks[snapshot["id"]] = task
                    task.add_done_callback(RunService._task_done)
                except Exception:
                    await self.store.finish_eval(snapshot["id"], "failed", "execution_error")
                    raise ServiceError("execution_error") from None
            return eval_view(snapshot)

    async def _drive(self, eval_id: str) -> None:
        failure: tuple[str, str] | None = None
        try:
            async with asyncio.timeout(600):
                await self.store.set_eval_running(eval_id)
                snapshot = await self.store.eval_snapshot(eval_id)
                request = EvalRequest.model_validate(snapshot["request_json"])
                cases = {
                    c.case_id: c for c in DatasetRevision.model_validate(snapshot["dataset"]).cases
                }
                for item in snapshot["items"]:
                    work = ItemWork(self.runs)
                    self.current[eval_id] = work
                    try:
                        await execute_item(
                            eval_id,
                            item["repetition"],
                            cases[item["case_id"]],
                            request.rules,
                            self.store,
                            work,
                        )
                    finally:
                        self.current.pop(eval_id, None)
        except asyncio.CancelledError:
            failure = ("aborted", "cancelled")
        except TimeoutError:
            failure = ("failed", "run_timeout")
        except ServiceError as error:
            failure = ("failed", error.code)
        except Exception:
            failure = ("failed", "execution_error")
        finally:
            self._finalizing.add(eval_id)
            try:
                if failure is not None:
                    await self.store.finish_eval(eval_id, *failure)
            finally:
                self._finalizing.discard(eval_id)
                self._cancelled.discard(eval_id)

    async def get(self, eval_id: str) -> EvalView:
        return eval_view(await self.store.eval_snapshot(eval_id))

    async def by_key(self, key: str) -> EvalView:
        snapshot = await self.store.eval_by_key(key)
        if snapshot is None:
            raise ServiceError("not_found", 404)
        return eval_view(snapshot)

    async def wait(self, eval_id: str) -> EvalView:
        self._external()
        task = self.tasks.get(eval_id)
        if task is not None:
            await asyncio.shield(task)
        return await self.get(eval_id)

    async def items(
        self, eval_id: str, revision: int, from_index: int = 0, limit: int = 8
    ) -> dict[str, Any]:
        snapshot = await self.store.eval_snapshot(eval_id, revision)
        if not 0 <= from_index <= len(snapshot["items"]) or not 1 <= limit <= 8:
            raise ServiceError("invalid_cursor", 416)
        return {
            "eval_id": eval_id,
            "revision": revision,
            "items": snapshot["items"][from_index : from_index + limit],
        }

    async def report(self, eval_id: str, revision: int | None = None) -> dict[str, Any]:
        snapshot = await self.store.eval_snapshot(eval_id, revision)
        return validate_report(
            snapshot["report"] if snapshot["report"] is not None else make_report(snapshot)
        )

    async def compare(
        self,
        baseline: str,
        candidate: str,
        baseline_revision: int,
        candidate_revision: int,
        baseline_hash: str,
        candidate_hash: str,
    ) -> dict[str, JsonValue]:
        # 已定稿报告不可变；每份读取自己的一致事务，不访问子Run补事实。
        before, after = (
            await self.report(baseline, baseline_revision),
            await self.report(candidate, candidate_revision),
        )
        if before["report_hash"] != baseline_hash or after["report_hash"] != candidate_hash:
            raise ServiceError("snapshot_changed")
        return gate_reports(before, after)

    async def cancel(self, eval_id: str) -> EvalView:
        self._external()
        return await settled(self._cancel(eval_id))

    async def _cancel(self, eval_id: str) -> EvalView:
        task = self.tasks.get(eval_id)
        if task is None or task.done():
            return await self.get(eval_id)
        error: Exception | None = None
        try:
            await self.store.stop_eval(eval_id)
        except Exception as failure:
            error = failure
        finally:
            if (
                not task.done()
                and eval_id not in self._finalizing
                and eval_id not in self._cancelled
            ):
                self._cancelled.add(eval_id)
                task.cancel()
            await settled(task)
        if error is not None:
            raise error
        return await self.get(eval_id)

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
