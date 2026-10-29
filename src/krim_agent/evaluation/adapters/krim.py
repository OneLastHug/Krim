"""v1兼容边界：只借用原RunService/Store，逐项算法仍是共享runner。"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, cast

from pydantic import JsonValue

from krim_agent import contracts as public_contracts
from krim_agent.contracts import ServiceError, fingerprint, parse_json
from krim_agent.runtime.service import RunService
from krim_agent.storage.sqlite import SQLiteStore

from .. import scorers
from ..domain import BatchPlan, CleanupResult, ExecutionRef, PreparedTrial, TrialSpec
from ..kernel import EvalKernel
from ..ports import AgentAdapter, MetricPort
from ..regression import gate_reports, make_report, validate_report
from ..targets import RunTarget
from ..trace_cases import collect_candidate, observe_snapshot
from ..types import (
    RULE_CONFIGS,
    Candidate,
    DatasetRevision,
    EvalOutput,
    EvalRequest,
    EvalView,
    FreezeRequest,
    Reference,
    RuleSpec,
)


def eval_view(snapshot: dict[str, Any]) -> EvalView:
    values = {k: snapshot[k] for k in EvalView.model_fields if k != "finished_count"}
    values["finished_count"] = sum(i["status"] == "finished" for i in snapshot["items"])
    return EvalView.model_validate(values)


class KrimAdapter:
    def __init__(self, store: SQLiteStore, runs: RunService, key: str) -> None:
        self.store, self.runs, self.key = store, runs, key

    def descriptor(self) -> dict[str, Any]:
        binding = self.runs.bindings.get(self.key)
        if binding is None:
            raise ServiceError("invalid_input", 422)
        binding.assert_current()
        return cast(dict[str, Any], parse_json(binding.descriptor_json))

    def assert_current(self, frozen: dict[str, Any]) -> None:
        if fingerprint(self.descriptor()) != fingerprint(frozen):
            raise ServiceError("target_binding_changed")

    async def prepare(self, trial: TrialSpec) -> PreparedTrial:
        return PreparedTrial(trial)

    async def execute(self, prepared: PreparedTrial) -> ExecutionRef:
        trial = prepared.spec
        run = await RunTarget(self.runs).start(trial.task_id, trial.case_id, trial.repetition)
        return ExecutionRef(run.id)

    async def collect(self, ref: ExecutionRef) -> dict[str, Any]:
        await self.runs.wait(ref.id)
        return observe_snapshot(await self.store.snapshot(ref.id)).model_dump(mode="json")

    async def cancel(self, ref: ExecutionRef) -> CleanupResult:
        await self.runs.cancel(ref.id)
        return CleanupResult(True)

    async def close(self) -> CleanupResult:
        # 此adapter没有借用资源的关闭权。
        return CleanupResult(True)


class LegacyMetrics:
    def identity(self, rule: dict[str, Any]) -> dict[str, str]:
        return scorers.result(RuleSpec.model_validate(rule), "scored", 1).rule

    def result(
        self,
        rule: dict[str, Any],
        status: str,
        value: int | None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        return scorers.result(RuleSpec.model_validate(rule), status, value, reason).model_dump(
            mode="json"
        )

    async def score(
        self,
        rule: dict[str, Any],
        case: dict[str, Any],
        output: dict[str, Any],
    ) -> dict[str, Any]:
        # 保留运行时插入点；取消/超时仍由唯一runner拥有并join这个Task。
        result = await scorers.score_rule(
            RuleSpec.model_validate(rule),
            Reference.model_validate(case["reference"]),
            EvalOutput.model_validate(output),
        )
        return result.model_dump(mode="json")


class LegacyRepository:
    def __init__(self, store: SQLiteStore, runs: RunService) -> None:
        self.store, self.runs = store, runs
        self._adapters: dict[str, KrimAdapter] = {}

    @property
    def healthy(self) -> bool:
        return self.store.healthy

    async def by_key(self, request: dict[str, Any]) -> dict[str, Any] | None:
        return await self.store.eval_by_key(request["request_key"])

    def adapter(self, request: dict[str, Any]) -> AgentAdapter:
        key = request["target_key"]
        if key not in self._adapters:
            self._adapters[key] = KrimAdapter(self.store, self.runs, key)
        return self._adapters[key]

    def metrics(self) -> MetricPort:
        return LegacyMetrics()

    async def begin(self, value: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        request = EvalRequest.model_validate(value)
        binding = self.runs.bindings.get(request.target_key)
        if binding is None:
            raise ServiceError("invalid_input", 422)
        binding.assert_current()
        target = cast(dict[str, JsonValue], parse_json(binding.descriptor_json))
        execution = request.execution.model_dump(mode="json") | {
            k: target[k] for k in ["limits", "policy", "pre", "post"]
        }
        try:
            if request.target_key == "target-http" and binding.settings.provider == "http":
                dataset = await self.store.dataset(request.dataset.name, request.dataset.version)
                if (
                    dataset.fingerprint != request.dataset.fingerprint
                    or len(dataset.cases) * request.execution.repetitions > 2
                ):
                    raise ServiceError("invalid_input", 422)
                execution["effective_timeouts"] = {
                    "schema_version": 1,
                    "source": "trusted-http-host/1",
                    "item_timeout_seconds": binding.settings.run_timeout + 30,
                    "batch_timeout_seconds": 600,
                }
            return await self.store.begin_eval(request, target, execution)
        except ServiceError as error:
            if error.code == "not_found":
                raise ServiceError("invalid_input", 422) from None
            raise

    async def snapshot(self, task_id: str) -> dict[str, Any]:
        return await self.store.eval_snapshot(task_id)

    async def set_running(self, task_id: str) -> None:
        await self.store.set_eval_running(task_id)

    def plan(self, snapshot: dict[str, Any]) -> BatchPlan:
        request = EvalRequest.model_validate(snapshot["request_json"])
        timeouts = snapshot["execution"].get("effective_timeouts")
        item_timeout = 12.0
        if timeouts is not None:
            if (
                not isinstance(timeouts, dict)
                or timeouts.get("schema_version") != 1
                or timeouts.get("source") != "trusted-http-host/1"
                or timeouts.get("batch_timeout_seconds") != 600
                or type(timeouts.get("item_timeout_seconds")) not in {int, float}
                or not 210 <= timeouts["item_timeout_seconds"] <= 270
                or timeouts["item_timeout_seconds"]
                != snapshot["target"]["limits"]["run_timeout"] + 30
            ):
                raise ServiceError("facts_corrupt")
            item_timeout = float(timeouts["item_timeout_seconds"])
        cases = {c.case_id: c for c in DatasetRevision.model_validate(snapshot["dataset"]).cases}
        return BatchPlan(
            [
                TrialSpec(
                    snapshot["id"],
                    i["case_id"],
                    i["repetition"],
                    cases[i["case_id"]].model_dump(mode="json"),
                    snapshot["target"],
                )
                for i in snapshot["items"]
            ],
            [r.model_dump(mode="json") for r in request.rules],
            item_timeout,
            1,
            600,
        )

    async def intent(self, trial: TrialSpec) -> None:
        # 原child_request/start_child已负责Run意图；不双写或伪造新Run。
        return None

    async def attach(self, trial: TrialSpec, ref: ExecutionRef) -> None:
        await self.store.attach_child(trial.task_id, trial.case_id, trial.repetition, ref.id)

    async def record(
        self,
        trial: TrialSpec,
        ref: ExecutionRef,
        observation: dict[str, Any] | None,
    ) -> None:
        # Run事实已经独立持久化；旧关联/评分原子性仍只由原Store负责。
        return None

    async def commit(self, trial: TrialSpec, body: dict[str, Any]) -> None:
        await self.store.commit_item(trial.task_id, trial.case_id, trial.repetition, body)

    async def stop(self, task_id: str) -> None:
        await self.store.stop_eval(task_id)

    async def finish(self, task_id: str, status: str, code: str) -> None:
        await self.store.finish_eval(task_id, status, code)

    def mode(self, request: dict[str, Any]) -> str:
        return "execute"


class EvaluationService:
    """保持旧构造/DTO和只读接口；不持有第二份_drive或执行循环。"""

    def __init__(self, store: SQLiteStore, runs: RunService) -> None:
        self.store, self.runs = store, runs
        self.kernel = EvalKernel(LegacyRepository(store, runs))
        self.tasks, self.current = self.kernel.tasks, self.kernel.current

    @property
    def closed(self) -> bool:
        return self.kernel.closed

    def _external(self) -> None:
        self.kernel._external()

    def targets(self) -> list[dict[str, JsonValue]]:
        return [
            cast(dict[str, JsonValue], parse_json(b.descriptor_json))
            for key, b in self.runs.bindings.items()
            if key in {"target-v1", "target-v2", "target-bad", "target-http"}
        ]

    def rules(self) -> list[dict[str, JsonValue]]:
        return [
            {"name": name, "version": "1", "config": dict(config)}
            for name, config in RULE_CONFIGS.items()
        ]

    async def candidate(self, run_id: str, include_input: bool = False) -> Candidate:
        return collect_candidate(await self.store.snapshot(run_id), include_input)

    async def freeze(self, request: FreezeRequest) -> DatasetRevision:
        return await self.store.freeze_dataset(request)

    async def start(self, request: EvalRequest) -> EvalView:
        return eval_view(await self.kernel.submit(request.model_dump(mode="json")))

    async def get(self, eval_id: str) -> EvalView:
        return eval_view(await self.store.eval_snapshot(eval_id))

    async def by_key(self, key: str) -> EvalView:
        snapshot = await self.store.eval_by_key(key)
        if snapshot is None:
            raise ServiceError("not_found", 404)
        return eval_view(snapshot)

    async def wait(self, eval_id: str) -> EvalView:
        return eval_view(await self.kernel.wait(eval_id))

    async def items(
        self,
        eval_id: str,
        revision: int,
        from_index: int = 0,
        limit: int = 8,
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
        before, after = (
            await self.report(baseline, baseline_revision),
            await self.report(
                candidate,
                candidate_revision,
            ),
        )
        if before["report_hash"] != baseline_hash or after["report_hash"] != candidate_hash:
            raise ServiceError("snapshot_changed")
        return gate_reports(before, after)

    async def cancel(self, eval_id: str) -> EvalView:
        return eval_view(await self.kernel.cancel(eval_id))

    async def close(self) -> None:
        await self.kernel.close()


class NeutralKrimAdapter:
    """中立Task借用真实RunService；不制造旧Eval FK，不关闭借用资源。"""

    def __init__(
        self,
        store: SQLiteStore,
        runs: RunService,
        key: str = "target-v2",
        *,
        neutral_key: str = "krim-script-v2",
        root: Path | None = None,
    ) -> None:
        self.store, self.runs, self.key = store, runs, key
        self.neutral_key, self.root = neutral_key, root
        self.jobs: dict[str, ExecutionRef] = {}
        self.closed = False
        self.execute_calls = 0

    def descriptor(self) -> dict[str, Any]:
        binding = self.runs.bindings.get(self.key)
        if binding is None:
            raise ServiceError("invalid_input")
        binding.assert_current()
        original = parse_json(binding.descriptor_json)
        return {
            "key": self.neutral_key,
            "version": "neutral-krim/1",
            "kind": "krim-run",
            "execution_conditions": {
                "schema_version": "krim-run/1",
                "run_target_key": self.key,
                "binding_descriptor": original,
                "binding_fingerprint": fingerprint(original),
                "public_runtime_conditions": {"run_timeout": binding.settings.run_timeout},
                "budget": {"item": 20, "task": 120},
            },
        }

    def assert_current(self, frozen: dict[str, Any]) -> None:
        if self.closed or fingerprint(self.descriptor()) != fingerprint(frozen):
            raise ServiceError("target_binding_changed")

    async def prepare(self, trial: TrialSpec) -> PreparedTrial:
        self.assert_current(trial.target)
        if self.root is None:
            return PreparedTrial(trial)
        from ..evidence import initial_files

        directory = self.root / trial.task_id / f"{trial.case_id}-{trial.repetition}"
        directory.mkdir(parents=True, exist_ok=False)
        initial = initial_files(directory, {"INPUT.txt": trial.case["instruction"]})
        return PreparedTrial(trial, str(directory), initial)

    async def execute(self, prepared: PreparedTrial) -> ExecutionRef:
        self.assert_current(prepared.spec.target)
        trial = prepared.spec
        key = f"pn2.krim.{trial.task_id}.{trial.case_id}.{trial.repetition}"
        request = public_contracts.RunRequest.model_validate(
            {"text": trial.case["instruction"], "request_key": key, "target": self.key}
        )
        old = await self.store.by_key(key)
        self.assert_current(trial.target)
        admitted = False
        if old is None:
            self.execute_calls += 1
            try:
                old = await self.runs.start(request)
                # 真实start已校验完整request；在任何后续await之前接管其返回的身份。
                admitted = old.request_key == key
            except Exception as error:
                try:
                    old = await self.store.by_key(key)
                except Exception as lookup_error:
                    self.closed = True
                    vars(self).setdefault("_unresolved_starts", {})[key] = request
                    raise ServiceError("execution_unknown") from lookup_error
                if old is None:
                    if isinstance(error, ServiceError):
                        raise error
                    self.closed = True
                    vars(self).setdefault("_unresolved_starts", {})[key] = request
                    raise ServiceError("execution_unknown") from error
        ref = ExecutionRef(old.id)
        if admitted:
            self.jobs[ref.id] = ref
        try:
            snapshot = await self.store.snapshot(old.id)
            if snapshot.request != request:
                raise ServiceError("key_conflict")
            self.assert_current(trial.target)
        except BaseException:
            if admitted:
                # 借用服务保持开放；只取消本次可信start交回的Run，join在异常回程之前完成。
                from ..lifecycle import settled

                try:
                    await settled(self.runs.cancel(ref.id))
                except Exception as error:
                    self.closed = True
                    raise ServiceError("execution_unknown") from error
            else:
                self.closed = True
                vars(self).setdefault("_unresolved_starts", {})[key] = request
            raise
        self.jobs[ref.id] = ref
        return ref

    async def collect(self, ref: ExecutionRef) -> dict[str, Any]:
        if self.jobs.get(ref.id) is not ref:
            raise ServiceError("permission_denied")
        await self.runs.wait(ref.id)
        from krim_agent.runtime.facts import project_facts

        from ..domain import Observation

        facts = project_facts(await self.store.snapshot(ref.id))
        return Observation(
            execution_ref=ref.id,
            exit_code=None,
            stdout=None,
            stderr=None,
            artifacts={},
            process=None,
            cleanup="closed",
            token_usage=facts.token_usage,
            usage_complete=facts.usage_complete,
            target_result={
                "run_id": ref.id,
                "status": facts.run.status,
                "code": facts.run.code,
                "text": facts.text,
                "model_steps": facts.model_steps,
                "tools": [t.model_dump(mode="json") for t in facts.tools],
            },
        ).model_dump(mode="json")

    async def cancel(self, ref: ExecutionRef) -> CleanupResult:
        if self.jobs.get(ref.id) is not ref:
            raise ServiceError("permission_denied")
        await self.runs.cancel(ref.id)
        return CleanupResult(True, observation=await self.collect(ref))

    async def close(self) -> CleanupResult:
        values = await asyncio.gather(
            *(self.cancel(ref) for ref in self.jobs.values()),
            return_exceptions=True,
        )
        self.closed = all(isinstance(v, CleanupResult) and v.closed for v in values)
        return CleanupResult(self.closed, None if self.closed else "cleanup_unknown")
