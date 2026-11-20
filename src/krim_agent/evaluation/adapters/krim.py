"""v1兼容边界：只借用原RunService/Store，逐项算法仍是共享runner。"""

from __future__ import annotations

import asyncio
from typing import Any, cast

from pydantic import JsonValue

from krim_agent.contracts import ServiceError, fingerprint, parse_json
from krim_agent.runtime.service import RunService
from krim_agent.security import PrincipalContext
from krim_agent.sessions.service import SessionService
from krim_agent.sessions.types import ControlRequest, SessionCreate, TurnRequest
from krim_agent.storage.sqlite import SQLiteStore

from .. import scorers
from ..domain import (
    BatchPlan,
    CleanupResult,
    ExecutionRef,
    PreparedTrial,
    SessionExecutionContext,
    TrialSpec,
    neutral_fingerprint,
)
from ..kernel import EvalKernel
from ..ports import AgentAdapter, MetricPort
from ..regression import gate_reports, make_report, validate_report
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

    @property
    def healthy(self) -> bool:
        return self.store.healthy

    async def by_key(self, request: dict[str, Any]) -> dict[str, Any] | None:
        return await self.store.eval_by_key(request["request_key"])

    def adapter(self, request: dict[str, Any]) -> AgentAdapter:
        raise ServiceError("execution_retired", 410)

    def metrics(self) -> MetricPort:
        return LegacyMetrics()

    async def begin(self, value: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        raise ServiceError("execution_retired", 410)

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
                # 只要求与冻结目标的运行时限及固定余量完全一致；合法短时配置不套绝对区间。
                or timeouts["item_timeout_seconds"]
                != (snapshot["target"]["limits"]["run_timeout"] or 570) + 30
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
        raise ServiceError("execution_retired", 410)

    async def attach(self, trial: TrialSpec, ref: ExecutionRef) -> None:
        raise ServiceError("execution_retired", 410)

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
        return []

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
        raise ServiceError("execution_retired", 410)

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
    """Run plain cases through the host's authorized session service."""

    def __init__(self, store: SQLiteStore, runs: RunService, sessions: SessionService,
                 key: str = "target-http", *, neutral_key: str = "krim-session") -> None:
        self.store, self.runs, self.sessions, self.key = store, runs, sessions, key
        self.neutral_key = neutral_key
        self.jobs: dict[str, dict[str, Any]] = {}
        self.active: set[str] = set()
        self.observations: dict[str, dict[str, Any]] = {}
        self.closed = False
        self.execute_calls = 0

    def descriptor(self) -> dict[str, Any]:
        if self.key != "target-http":
            raise ServiceError("workspace_unavailable", 409)
        binding = self.runs.bindings.get(self.key)
        if binding is None:
            raise ServiceError("provider_not_configured", 503)
        binding.assert_current()
        provider_type = type(binding.loop.provider)
        if (provider_type.__name__ == "UnconfiguredProvider"
                or provider_type.__module__ in {
                    "krim_agent.providers.fake", "krim_agent.providers.eval_script"
                }):
            raise ServiceError("provider_not_configured", 503)
        original = cast(dict[str, Any], parse_json(binding.descriptor_json))
        original_fingerprint = fingerprint(original)
        compact = dict(original)
        schemas = compact.get("tools")
        if isinstance(schemas, list):
            compact["tools"] = {
                "count": len(schemas),
                "fingerprint": fingerprint(schemas),
                "names": sorted(
                    item["name"] for item in schemas
                    if isinstance(item, dict) and isinstance(item.get("name"), str)
                ),
            }
        return {
            "key": self.neutral_key, "version": "krim-session/1", "kind": "krim-run",
            "max_concurrency": 1,
            "execution_conditions": {
                "schema_version": "krim-run/1", "run_target_key": self.key,
                "binding_descriptor": compact, "binding_fingerprint": original_fingerprint,
                "public_runtime_conditions": {
                    "run_timeout": binding.settings.run_timeout,
                    "submission": "SessionService.create-submit/1",
                },
                "budget": {"item": 20, "task": 120},
            },
        }

    def assert_current(self, frozen: dict[str, Any]) -> None:
        if self.closed or fingerprint(self.descriptor()) != fingerprint(frozen):
            raise ServiceError("target_binding_changed")

    def _principal(self, context: SessionExecutionContext) -> PrincipalContext:
        principal = self.sessions._owner_principal(context.principal_id)
        if principal.generation != context.principal_generation:
            raise ServiceError("permission_denied", 403)
        principal = self.sessions._live_principal(principal)
        principal.member(context.workspace_id, write=True)
        return principal

    @staticmethod
    def _keys(trial: TrialSpec) -> tuple[str, str]:
        base = f"eval.{trial.task_id}.{trial.case_id}.{trial.repetition}"
        return base + ".session", base + ".turn"

    async def prepare(self, trial: TrialSpec) -> PreparedTrial:
        self.assert_current(trial.target)
        context = trial.execution_context
        if (context is None or context.target_key != self.key
                or context.binding_fingerprint != neutral_fingerprint(trial.target)):
            raise ServiceError("permission_denied", 403)
        self._principal(context)
        if trial.case.get("files"):
            raise ServiceError("workspace_unavailable", 409)
        if trial.case.get("verifier") in {"krim-script/1", "agent-bench/1"}:
            raise ServiceError("target_retired", 410)
        return PreparedTrial(trial)

    async def _session(self, principal: PrincipalContext, context: SessionExecutionContext,
                       key: str) -> Any:
        try:
            return await self.sessions.by_key(principal, context.workspace_id, key)
        except ServiceError as error:
            if error.code != "not_found":
                raise
        try:
            return await self.sessions.create(
                principal, context.workspace_id,
                SessionCreate(request_key=key, title="Krim evaluation",
                              target_key=context.target_key),
            )
        except Exception as error:
            try:
                return await self.sessions.by_key(principal, context.workspace_id, key)
            except Exception as lookup_error:
                if isinstance(error, ServiceError):
                    raise error from None
                raise ServiceError("execution_unknown") from lookup_error

    async def execute(self, prepared: PreparedTrial) -> ExecutionRef:
        trial = prepared.spec
        self.assert_current(trial.target)
        context = trial.execution_context
        if context is None:
            raise ServiceError("permission_denied", 403)
        principal = self._principal(context)
        session_key, turn_key = self._keys(trial)
        session = await self._session(principal, context, session_key)
        if (session.owner_id != context.principal_id or session.workspace_id != context.workspace_id
                or session.target_key != self.key):
            raise ServiceError("facts_corrupt")
        try:
            turn = await self.sessions.turn_by_key(
                principal, context.workspace_id, session.id, turn_key
            )
        except ServiceError as error:
            if error.code != "not_found":
                raise
            if session.revision != 0:
                raise ServiceError("key_conflict") from None
            self.execute_calls += 1
            try:
                turn = await self.sessions.submit(
                    principal, context.workspace_id, session.id,
                    TurnRequest(request_key=turn_key, text=trial.case["instruction"],
                                expected_revision=session.revision),
                )
            except Exception as error:
                try:
                    turn = await self.sessions.turn_by_key(
                        principal, context.workspace_id, session.id, turn_key
                    )
                except Exception as lookup_error:
                    if isinstance(error, ServiceError):
                        raise error from None
                    raise ServiceError("execution_unknown") from lookup_error
        if turn.text != trial.case["instruction"] or turn.owner_id != context.principal_id:
            raise ServiceError("key_conflict")
        self.jobs[turn.id] = {
            "context": context, "session_id": session.id, "turn_id": turn.id,
            "task_id": trial.task_id, "case_id": trial.case_id,
            "repetition": trial.repetition,
        }
        self.active.add(turn.id)
        return ExecutionRef(turn.id)

    async def _close_session(self, principal: PrincipalContext,
                             context: SessionExecutionContext, record: dict[str, Any]) -> Any:
        session = await self.sessions.get(
            principal, context.workspace_id, record["session_id"], write=True
        )
        if session.status == "open":
            await self.sessions.control(
                principal, context.workspace_id, session.id, "close",
                ControlRequest(
                    request_key=(f"eval-close.{record['task_id']}.{record['case_id']}."
                                 f"{record['repetition']}"),
                    expected_revision=session.revision,
                ),
            )
            session = await self.sessions.get(
                principal, context.workspace_id, record["session_id"], write=True
            )
        return session

    async def collect(self, ref: ExecutionRef) -> dict[str, Any]:
        cached = self.observations.get(ref.id)
        if cached is not None:
            return cached
        record = self.jobs.get(ref.id)
        if record is None:
            raise ServiceError("permission_denied", 403)
        context = cast(SessionExecutionContext, record["context"])
        principal = self._principal(context)
        turn = await self.sessions.wait(
            principal, context.workspace_id, record["session_id"], record["turn_id"]
        )
        snapshot = None if turn.run_id is None else await self.store.snapshot(turn.run_id)
        observed = facts = None
        tools = None
        artifacts: dict[str, Any] = {}
        if snapshot is not None:
            linked = snapshot.session_context
            if (linked is None or linked.session_id != record["session_id"]
                    or linked.turn_id != record["turn_id"]
                    or linked.workspace_id != context.workspace_id
                    or snapshot.run.id != turn.run_id):
                raise ServiceError("facts_corrupt")
            observed = observe_snapshot(snapshot)
            from krim_agent.runtime.facts import project_facts
            facts = project_facts(snapshot)
            tools = [
                {"seq": event.seq, "event_id": event.event_id,
                 "kind": event.kind, "receipt": event.data}
                for event in snapshot.events if event.kind in {"tool_start", "tool_end"}
            ] or None
            if snapshot.work is not None:
                artifacts["work"] = {
                    "input_revision": snapshot.work.input_revision.model_dump(mode="json"),
                    "source_refs": list(snapshot.work.source_refs),
                    "verification": snapshot.work.verification,
                }
            refs = [
                {"event_id": event.event_id, "field": field, "id": event.data[field]}
                for event in snapshot.events
                for field in ("artifact_revision_id", "revision_id", "artifact_id")
                if isinstance(event.data.get(field), str)
            ]
            if refs:
                artifacts["references"] = refs

        cleanup, session_status = "unknown", "unknown"
        try:
            final_session = await self._close_session(principal, context, record)
            session_status = final_session.status
            cleanup = "closed" if session_status == "closed" else "unknown"
        except ServiceError:
            pass
        run = None if snapshot is None else snapshot.run
        target_result = {
            "schema_version": "krim-session/1", "session_id": record["session_id"],
            "session_status": session_status, "session_record_retained": True,
            "turn_id": turn.id, "turn_status": turn.status, "turn_code": turn.code,
            "context_fingerprint": turn.context_fingerprint,
            "run_id": None if run is None else run.id,
            "trace_id": None if run is None else run.trace_id,
            "status": turn.status if run is None else run.status,
            "code": turn.code if run is None else run.code,
            "text": None if observed is None else observed.text,
            "model_steps": None if observed is None else observed.model_steps,
            "tools": tools, "host_runtime": {"ownership": "borrowed", "retained": True},
        }
        from ..domain import Observation
        output = Observation(
            execution_ref=ref.id, exit_code=None, stdout=None, stderr=None,
            artifacts=artifacts, process=None, cleanup=cleanup,
            token_usage=None if facts is None else facts.token_usage,
            usage_complete=False if facts is None else facts.usage_complete,
            target_result=target_result,
        ).model_dump(mode="json")
        self.active.discard(ref.id)
        self.observations[ref.id] = output
        return output

    async def cancel(self, ref: ExecutionRef) -> CleanupResult:
        record = self.jobs.get(ref.id)
        if record is None:
            raise ServiceError("permission_denied", 403)
        context = cast(SessionExecutionContext, record["context"])
        principal = self._principal(context)
        session = await self.sessions.get(
            principal, context.workspace_id, record["session_id"], write=True
        )
        turn = await self.sessions.turn(
            principal, context.workspace_id, record["session_id"], record["turn_id"]
        )
        if turn.status in {"queued", "running"}:
            await self.sessions.control(
                principal, context.workspace_id, session.id, "cancel_turn",
                ControlRequest(
                    request_key=(f"eval-cancel.{record['task_id']}.{record['case_id']}."
                                 f"{record['repetition']}"),
                    expected_revision=session.revision,
                ), turn_id=turn.id,
            )
        observation = await self.collect(ref)
        closed = observation["cleanup"] == "closed"
        return CleanupResult(closed, None if closed else "cleanup_unknown", observation=observation)

    async def close(self) -> CleanupResult:
        outcomes = await asyncio.gather(
            *(self.cancel(ExecutionRef(identity)) for identity in tuple(self.active)),
            return_exceptions=True,
        )
        closed = all(isinstance(value, CleanupResult) and value.closed for value in outcomes)
        self.closed = True
        return CleanupResult(closed, None if closed else "cleanup_unknown")
