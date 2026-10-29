"""有限宿主自动化；复用既有评测、Run、Session 与 H1 持久边界。"""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Callable, Generator, Iterable
from contextlib import contextmanager
from contextvars import ContextVar
from types import MappingProxyType
from typing import Any, cast

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError
from krim_agent.runtime.service import RunService
from krim_agent.security import PrincipalContext
from krim_agent.sessions.service import SessionService
from krim_agent.storage import observation_outbox as outbox_sql
from krim_agent.storage.models import runs, turns
from krim_agent.storage.sqlite import SQLiteStore

from .adapters.krim_outbox import ActorViews, KrimOutboxPort
from .app import EvalApplication
from .continuous import ContinuousEvaluation
from .continuous_types import ScheduleSpec, window_request
from .domain import DatasetRef, neutral_fingerprint
from .hot_observations import HotObservationFlow
from .hot_types import CapturePolicy, CaptureRecord, FeedbackSignal
from .product import EvaluationProduct
from .repository import eval_schedule_windows, eval_schedules, meta, tasks


class EvaluationAutomation:
    """Application持有的单Task服务；不会接管输入对象的关闭权。

    `principals` 是宿主冻结的可信身份目录。它不是请求中的授权依据；请求主体
    必须与目录中的 principal_id、generation 和 memberships 完全对应。
    """

    def __init__(
        self,
        product: EvaluationProduct,
        principals: Iterable[PrincipalContext],
        *,
        clock: Callable[[], float] = time.time,
        poll_interval: float = 1.0,
    ) -> None:
        if not callable(clock) or not 0 < poll_interval <= 60:
            raise ServiceError("invalid_input", 422)
        frozen = tuple(principals)
        if not frozen or len(frozen) > 64 or any(
            not isinstance(value, PrincipalContext) for value in frozen
        ):
            raise ServiceError("invalid_input", 422)
        actors: dict[str, PrincipalContext] = {}
        scope_workspaces: dict[str, str] = {}
        for actor in frozen:
            if actor.principal_id in actors:
                raise ServiceError("invalid_input", 422)
            actors[actor.principal_id] = actor
            for membership in actor.memberships:
                scope = EvaluationProduct._scope(membership.workspace_id)
                old = scope_workspaces.get(scope)
                if old is not None and old != membership.workspace_id:
                    raise ServiceError("invalid_input", 422)
                if old is None and len(scope_workspaces) >= 256:
                    raise ServiceError("invalid_input", 422)
                scope_workspaces[scope] = membership.workspace_id
        self.product = product
        self.evaluation: EvalApplication = product.evaluation
        self.runs: RunService = product.runs
        self.sessions: SessionService = product.sessions
        self.store: SQLiteStore = product.sessions.store
        self._actors = MappingProxyType(actors)
        self._scope_workspaces = MappingProxyType(scope_workspaces)
        self._clock_source = clock
        self._clock_override: ContextVar[float | None] = ContextVar(
            "krim_evaluation_automation_clock", default=None
        )
        # 保持稳定 callable 身份；tick 请求通过 ContextVar 冻结该调用链的时钟。
        self.clock: Callable[[], float] = self._read_clock
        self.poll_interval = poll_interval
        self._task: asyncio.Task[None] | None = None
        self._start_lock = asyncio.Lock()
        self._wake = asyncio.Event()
        self._closing = False
        self._running = False
        self._last_error: str | None = None
        self._workspace_offset = 0
        self._schedule_offset = 0
        self._destination_id: str | None = None

    async def start(self) -> None:
        """读取真实Repository身份并启动唯一的后台轮询Task。"""
        async with self._start_lock:
            if self._closing:
                raise ServiceError("closed", 503)
            if self._task is not None:
                return
            self._destination_id = await self._repository_id()
            if self._closing:
                raise ServiceError("closed", 503)
            self._running = True
            self._task = asyncio.create_task(self._poll(), name="krim-evaluation-automation")

    async def close(self) -> None:
        """先禁止后续派发，再等待现有轮次真实收束；不关闭所借资源。"""
        self._closing = True
        self._wake.set()
        task = self._task
        if task is None:
            return
        if task is asyncio.current_task():
            raise ServiceError("target_binding_changed")
        await task

    def _ensure_open(self) -> None:
        if self._closing:
            raise ServiceError("closed", 503)
        if self.evaluation.repository.closed or self.store.closed:
            raise ServiceError("closed", 503)

    def _read_clock(self) -> float:
        frozen = self._clock_override.get()
        return self._clock_source() if frozen is None else frozen

    @contextmanager
    def _frozen_clock(self) -> Generator[float, None, None]:
        now = self.clock()
        token = self._clock_override.set(now)
        try:
            yield now
        finally:
            self._clock_override.reset(token)

    def _trusted(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        *,
        write: bool = False,
    ) -> PrincipalContext:
        self._ensure_open()
        trusted = self._actors.get(principal.principal_id)
        if trusted is None or trusted != principal:
            raise ServiceError("permission_denied", 403)
        trusted.member(workspace_id, write=write)
        return trusted

    async def _repository_id(self) -> str:
        repository = self.evaluation.repository
        async with repository.lock, repository.engine.begin() as conn:
            value = (
                await conn.execute(select(meta.c.body).where(meta.c.id == 1))
            ).scalar_one_or_none()
        if not isinstance(value, dict) or not isinstance(value.get("repository_id"), str):
            raise ServiceError("facts_corrupt")
        identity = value.get("repository_id")
        if not isinstance(identity, str) or not identity:
            raise ServiceError("facts_corrupt")
        return identity

    async def _workspace_facts(
        self, workspace_id: str
    ) -> tuple[CapturePolicy | None, list[dict[str, Any]]]:
        async def operation(
            conn: AsyncConnection,
        ) -> tuple[CapturePolicy | None, list[dict[str, Any]]]:
            return (
                await outbox_sql.policy(conn, workspace_id),
                await outbox_sql.rows(conn, workspace_id),
            )

        return await self.store.work_read(operation)

    async def set_capture(
        self, principal: PrincipalContext, workspace_id: str, enabled: bool
    ) -> dict[str, Any]:
        """由当前workspace admin显式启停本地终态采集。"""
        if type(enabled) is not bool:
            raise ServiceError("invalid_input", 422)
        actor = self._trusted(principal, workspace_id, write=True)
        if actor.member(workspace_id).role != "admin":
            raise ServiceError("permission_denied", 403)
        current, _rows = await self._workspace_facts(workspace_id)
        if enabled:
            # 任何采集正文/工具参数/产物的策略都不由此接口启用。
            policy = CapturePolicy(
                workspace_id=workspace_id,
                scope=EvaluationProduct._scope(workspace_id),
                observer=actor.principal_id,
                enabled=True,
                salt_id=_observer_generation_salt(actor),
                include_text=False,
                include_arguments=False,
                artifact_paths=[],
            )
        elif current is None or not current.enabled:
            return await self.status(actor, workspace_id)
        else:
            policy = current.model_copy(update={"enabled": False})
        port = KrimOutboxPort(self.store, self.sessions, actor, policy)
        await port.configure()
        self._wake.set()
        return await self.status(actor, workspace_id)

    async def submit_feedback(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        run_id: str,
        signal: FeedbackSignal,
    ) -> dict[str, Any]:
        """对本人可写的已捕获终态Run持久化反馈，不确认候选。"""
        actor = self._trusted(principal, workspace_id, write=True)
        signal = FeedbackSignal.model_validate(signal.model_dump(mode="json"))
        _current, rows = await self._workspace_facts(workspace_id)
        records: list[CaptureRecord] = []
        for row in rows:
            if row.get("run_id") != run_id:
                continue
            record = CaptureRecord.model_validate(row["body"])
            if (
                record.workspace_id != workspace_id
                or record.scope != EvaluationProduct._scope(workspace_id)
                or record.run_id != run_id
            ):
                raise ServiceError("facts_corrupt")
            records.append(record)
        if not records:
            raise ServiceError("not_found", 404)
        # H1 feedback要求原capture policy fingerprint；不借用或改写当前policy。
        record = records[0]
        port = KrimOutboxPort(self.store, self.sessions, actor, record.policy)
        event = await port.feedback(actor, run_id, signal)
        self._wake.set()
        return {"revision": event.revision, "key": event.key}

    async def feedback_receipt(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        run_id: str,
        key: str,
    ) -> dict[str, Any]:
        """只读恢复本人原反馈回执，不写入、不派发观测。"""
        try:
            key = FeedbackSignal(key=key, reason="other").key
        except (TypeError, ValueError):
            raise ServiceError("invalid_input", 422) from None
        actor = self._trusted(principal, workspace_id, write=True)
        scope = EvaluationProduct._scope(workspace_id)

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            linked = (
                await conn.execute(
                    select(turns.c.owner_id, turns.c.workspace_id).where(
                        turns.c.run_id == run_id
                    )
                )
            ).first()
            if linked is None or linked.workspace_id != workspace_id:
                raise ServiceError("not_found", 404)
            actor.owns(workspace_id, linked.owner_id, write=True)
            terminal = (
                await conn.execute(
                    select(runs.c.status).where(runs.c.id == run_id)
                )
            ).scalar_one_or_none()
            actor.owns(workspace_id, linked.owner_id, write=True)
            if terminal is None:
                raise ServiceError("not_found", 404)
            if terminal in {"queued", "running"}:
                raise ServiceError("invalid_input", 422)
            saved_rows = (
                await conn.execute(
                    select(outbox_sql.observation_outbox).where(
                        outbox_sql.observation_outbox.c.run_id == run_id,
                        outbox_sql.observation_outbox.c.workspace_id == workspace_id,
                    )
                )
            ).mappings().all()
            actor.owns(workspace_id, linked.owner_id, write=True)

            matches: list[dict[str, Any]] = []
            stale_generation = False
            for saved in saved_rows:
                record = CaptureRecord.model_validate(saved["body"])
                if (
                    saved["capture_id"] != record.capture_id
                    or saved["source_id"] != record.source_id
                    or saved["workspace_id"] != record.workspace_id
                    or saved["run_id"] != record.run_id
                    or saved["terminal_event_id"] != record.terminal_event_id
                    or saved["policy_fingerprint"] != record.policy_fingerprint
                    or record.workspace_id != workspace_id
                    or record.scope != scope
                    or record.run_id != run_id
                    or record.policy.workspace_id != workspace_id
                    or record.policy.scope != scope
                    or record.policy.fingerprint() != record.policy_fingerprint
                    or saved["feedback_revision"] != len(record.feedback)
                ):
                    raise ServiceError("facts_corrupt")
                events = [
                    event
                    for event in record.feedback
                    if event.actor == actor.principal_id and event.key == key
                ]
                if len(events) > 1:
                    raise ServiceError("facts_corrupt")
                if not events:
                    continue
                event = events[0]
                if event.generation != actor.generation:
                    stale_generation = True
                    continue
                matches.append(
                    {
                        "revision": event.revision,
                        "key": event.key,
                        "event": event.model_dump(mode="json"),
                        "record": record.model_dump(mode="json"),
                        "delivery": saved["delivery"],
                        "phase": saved["phase"],
                    }
                )

            if len(matches) > 1:
                raise ServiceError("facts_corrupt")
            if matches:
                return matches[0]
            if stale_generation:
                raise ServiceError("key_conflict")
            raise ServiceError("not_found", 404)

        fact = await self.store.work_read(operation)
        event = fact["event"]
        record = CaptureRecord.model_validate(fact["record"])
        delivery = fact["delivery"]
        phase = fact["phase"]
        if not isinstance(delivery, dict) or delivery.get("schema_version") != "hot-delivery/1":
            raise ServiceError("facts_corrupt")
        base = {
            "revision": fact["revision"],
            "key": fact["key"],
            "reason": event["reason"],
            "candidate_ids": [],
        }
        manifest = delivery.get("manifest")
        if manifest is None:
            if phase == "acked":
                raise ServiceError("facts_corrupt")
            return base | {
                "delivery_status": "pending"
                if phase in {"pending", "claimed"}
                else "not_enqueued"
            }
        try:
            units = outbox_sql.expanded_units(delivery)
        except (TypeError, ValueError):
            raise ServiceError("facts_corrupt") from None
        common = delivery.get("common")
        destination = common.get("destination_id") if isinstance(common, dict) else None
        if not isinstance(destination, str) or not destination:
            raise ServiceError("facts_corrupt")
        request_key = outbox_sql.delivery_key(
            record.source_id,
            record.capture_id,
            destination,
            record.scope,
            "feedback",
            actor.principal_id,
            key,
        )
        if (
            not isinstance(manifest, dict)
            or manifest.get("revision") != len(record.feedback)
            or not isinstance(manifest.get("keys"), list)
            or request_key not in manifest["keys"]
        ):
            raise ServiceError("facts_corrupt")
        unit = units.get(request_key)
        if (
            unit is None
            or unit.get("actor") != actor.principal_id
            or unit.get("generation") != actor.generation
            or unit.get("feedback") != {"key": key, "reason": event["reason"]}
        ):
            raise ServiceError("facts_corrupt")
        request = unit.get("request")
        if (
            not isinstance(request, dict)
            or request.get("request_key") != request_key
            or request.get("scope") != scope
            or request.get("sources")
            != [
                {
                    "task_id": None,
                    "case_id": None,
                    "run_id": run_id,
                    "repetition": 0,
                    "revision": None,
                    "evidence_hash": None,
                    "feedback": {"key": key, "reason": event["reason"]},
                }
            ]
        ):
            raise ServiceError("facts_corrupt")
        if unit["state"] == "not_started":
            return base | {"delivery_status": "pending"}
        imported = await self.evaluation.repository.import_observations(
            scope, request_key, unit["request_hash"], rows=None
        )
        if imported is None:
            if unit["state"] == "complete":
                raise ServiceError("facts_corrupt")
            return base | {"delivery_status": "pending"}
        if len(imported) != 1:
            raise ServiceError("facts_corrupt")
        imported_row = imported[0]
        candidate_id = imported_row.get("candidate_id", imported_row.get("id"))
        if not isinstance(candidate_id, str) or not candidate_id:
            raise ServiceError("facts_corrupt")
        try:
            candidate = await self.evaluation.repository.candidate(scope, candidate_id)
        except ServiceError as error:
            if error.code == "not_found":
                raise ServiceError("facts_corrupt") from None
            raise
        if (
            candidate.get("observation_id") != imported_row.get("id")
            or candidate.get("observation_hash") != imported_row.get("content_hash")
            or candidate.get("signals")
            != [{"key": key, "reason": event["reason"], "actor": actor.principal_id}]
        ):
            raise ServiceError("facts_corrupt")
        return base | {"candidate_ids": [candidate_id], "delivery_status": "linked"}

    async def status(
        self, principal: PrincipalContext, workspace_id: str
    ) -> dict[str, Any]:
        self._trusted(principal, workspace_id)
        policy, rows = await self._workspace_facts(workspace_id)
        phases = {"pending": 0, "claimed": 0, "acked": 0}
        for row in rows:
            phase = row.get("phase")
            if phase in phases:
                phases[cast(str, phase)] += 1
        counters = {"seen": 0, "enqueued": 0, "dropped": 0, "acked": 0}
        if policy is not None:
            async def read_counters(conn: AsyncConnection) -> dict[str, Any] | None:
                from sqlalchemy import select

                value = (
                    await conn.execute(
                        select(outbox_sql.capture_policies.c.counters).where(
                            outbox_sql.capture_policies.c.workspace_id == workspace_id
                        )
                    )
                ).scalar_one_or_none()
                return value

            saved = await self.store.work_read(read_counters)
            if not isinstance(saved, dict) or set(saved) != set(counters) or any(
                type(value) is not int or value < 0 for value in saved.values()
            ):
                raise ServiceError("facts_corrupt")
            counters = {key: saved[key] for key in counters}
        schedules_total, schedules_enabled = await self._schedule_counts(workspace_id)
        observer_ready = False
        if policy is not None:
            observer = self._actors.get(policy.observer)
            if observer is not None and _observer_generation_salt(observer) == policy.salt_id:
                try:
                    observer_ready = observer.member(workspace_id).role == "admin"
                except ServiceError:
                    observer_ready = False
        return {
            "automation": {
                "running": self._running and not self._closing,
                "last_error": self._last_error,
            },
            "capture": {
                "configured": policy is not None,
                "enabled": bool(policy and policy.enabled),
                "observer_ready": observer_ready,
                "pending": phases["pending"],
                "claimed": phases["claimed"],
                "acknowledged": phases["acked"],
                "counters": counters,
                "text_capture": False if policy is None else policy.include_text,
                "argument_capture": False if policy is None else policy.include_arguments,
                "artifact_count": 0 if policy is None else len(policy.artifact_paths),
            },
            "schedules": {
                "total": schedules_total,
                "enabled": schedules_enabled,
            },
        }

    async def list_schedules(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        offset: int = 0,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        self._trusted(principal, workspace_id)
        if (
            type(offset) is not int
            or type(limit) is not int
            or not 0 <= offset <= 100000
            or not 1 <= limit <= 50
        ):
            raise ServiceError("invalid_input", 422)
        scope = EvaluationProduct._scope(workspace_id)
        repository = self.evaluation.repository
        async with repository.lock, repository.engine.begin() as conn:
            if repository.closed:
                raise ServiceError("closed", 503)
            identities = (
                await conn.execute(
                    select(eval_schedules.c.schedule_id)
                    .where(eval_schedules.c.scope == scope)
                    .order_by(eval_schedules.c.schedule_id)
                    .offset(offset)
                    .limit(limit)
                )
            ).scalars().all()
        result = []
        for schedule_id in identities:
            row = await repository.schedule(scope, schedule_id)
            result.append(self._schedule_public_view(workspace_id, row))
        return result

    async def schedule(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        schedule_id: str,
    ) -> dict[str, Any]:
        self._trusted(principal, workspace_id)
        scope = EvaluationProduct._scope(workspace_id)
        row = await self.evaluation.repository.schedule(scope, schedule_id)
        return self._schedule_public_view(workspace_id, row)

    async def current_schedule_window(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        schedule_id: str,
    ) -> dict[str, Any]:
        """纯读计算当前窗口索引；不观察、预留或派发Task。"""
        self._trusted(principal, workspace_id)
        scope = EvaluationProduct._scope(workspace_id)
        row = await self.evaluation.repository.schedule(scope, schedule_id)
        try:
            spec = ScheduleSpec.model_validate(row["spec"])
            index = int((self.clock() - spec.epoch) // spec.interval_seconds)
        except (KeyError, TypeError, ValueError, ZeroDivisionError):
            raise ServiceError("facts_corrupt") from None
        return {
            "schedule_id": spec.schedule_id,
            "version": spec.version,
            "window_index": index,
            "enabled": row["enabled"] is True,
            "eligible": row["enabled"] is True and 0 <= index < spec.max_windows,
        }

    async def schedule_window(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        schedule_id: str,
        window_index: int,
    ) -> dict[str, Any]:
        """按原 index 只读恢复持久窗口记录及其绑定 Task。"""
        self._trusted(principal, workspace_id)
        if type(window_index) is not int or not 0 <= window_index <= 2:
            raise ServiceError("invalid_input", 422)
        scope = EvaluationProduct._scope(workspace_id)
        repository = self.evaluation.repository
        schedule = await repository.schedule(scope, schedule_id)
        try:
            spec = ScheduleSpec.model_validate(schedule["spec"])
        except (KeyError, TypeError, ValueError):
            raise ServiceError("facts_corrupt") from None
        if window_index >= spec.max_windows:
            raise ServiceError("not_found", 404)
        async with repository.lock, repository.engine.begin() as conn:
            if repository.closed:
                raise ServiceError("closed", 503)
            body = (
                await conn.execute(
                    select(eval_schedule_windows.c.body).where(
                        eval_schedule_windows.c.scope == scope,
                        eval_schedule_windows.c.schedule_id_version
                        == spec.schedule_id + "/" + spec.version,
                        eval_schedule_windows.c.dataset_envelope_fp
                        == spec.template.dataset_envelope_fp,
                        eval_schedule_windows.c.target_fp == schedule["target_fp"],
                        eval_schedule_windows.c.window_index == window_index,
                    )
                )
            ).scalar_one_or_none()
        result = {
            "schedule_id": spec.schedule_id,
            "version": spec.version,
            "window_index": window_index,
            "status": "unknown",
            "task": None,
        }
        if body is None:
            return result
        if not isinstance(body, dict) or type(body.get("window_index")) is not int:
            raise ServiceError("facts_corrupt")
        if body.get("status") == "missed":
            if set(body) != {"status", "task_id", "window_index"} or (
                body.get("task_id") is not None or body["window_index"] != window_index
            ):
                raise ServiceError("facts_corrupt")
            return result | {"status": "missed"}
        if body.get("status") != "reserved" or set(body) != {
            "status",
            "task_id",
            "window_index",
            "spec_hash",
            "request_hash",
        }:
            raise ServiceError("facts_corrupt")
        if (
            body["window_index"] != window_index
            or body["spec_hash"] != schedule["spec_hash"]
            or not isinstance(body["task_id"], str)
            or not isinstance(body["request_hash"], str)
        ):
            raise ServiceError("facts_corrupt")
        request = window_request(schedule, window_index)
        request_json = request.model_dump(mode="json")
        request_hash = neutral_fingerprint(request_json)
        if body["request_hash"] != request_hash:
            raise ServiceError("facts_corrupt")
        async with repository.lock, repository.engine.begin() as conn:
            if repository.closed:
                raise ServiceError("closed", 503)
            task_binding = (
                await conn.execute(
                    select(tasks.c.scope, tasks.c.request_key).where(
                        tasks.c.id == body["task_id"]
                    )
                )
            ).first()
        if (
            task_binding is None
            or task_binding.scope != scope
            or task_binding.request_key != request.request_key
        ):
            raise ServiceError("facts_corrupt")
        try:
            task = await repository.snapshot(body["task_id"])
        except ServiceError as error:
            if error.code == "not_found":
                raise ServiceError("facts_corrupt") from None
            raise
        if (
            task.get("id") != body["task_id"]
            or task.get("request_json") != request_json
            or task.get("request_hash") != request_hash
        ):
            raise ServiceError("facts_corrupt")
        public_task = await self.product.task(principal, workspace_id, body["task_id"])
        return result | {"status": "reserved", "task": _task_receipt(public_task)}

    def _schedule_public_view(
        self, workspace_id: str, row: dict[str, Any]
    ) -> dict[str, Any]:
        try:
            spec = ScheduleSpec.model_validate(row["spec"])
        except (KeyError, TypeError, ValueError):
            raise ServiceError("facts_corrupt") from None
        dataset = self.product._public_ref(
            workspace_id, spec.template.dataset.model_dump(mode="json")
        )
        public_spec = spec.model_dump(mode="json")
        public_spec.pop("scope", None)
        public_spec["template"].pop("scope", None)
        public_spec["template"]["dataset"] = dataset
        return _schedule_view(row) | {
            "dataset": dataset,
            "target_key": spec.template.target_key,
            "epoch": spec.epoch,
            "interval_seconds": spec.interval_seconds,
            "max_windows": spec.max_windows,
            "spec": public_spec,
            "spec_fingerprint": neutral_fingerprint(public_spec),
        }

    async def create_schedule(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        request: ScheduleSpec,
    ) -> dict[str, Any]:
        actor = self._trusted(principal, workspace_id, write=True)
        try:
            submitted = ScheduleSpec.model_validate(request.model_dump(mode="json"))
        except (TypeError, ValueError):
            raise ServiceError("invalid_input", 422) from None
        scope = EvaluationProduct._scope(workspace_id)
        template = submitted.template
        public_ref = DatasetRef.model_validate(template.dataset.model_dump(mode="json"))
        dataset = await self.product._dataset_ref(workspace_id, public_ref)
        self.product._target(template.target_key)
        safe_template = template.model_copy(update={"scope": scope, "dataset": dataset})
        frozen = ScheduleSpec.model_validate(
            submitted.model_copy(update={"scope": scope, "template": safe_template})
            .model_dump(mode="json")
        )
        result = await self._continuous(actor, workspace_id).create_schedule(frozen)
        self._wake.set()
        return self._schedule_public_view(workspace_id, result)

    async def tick_schedule(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        schedule_id: str,
        expected_window_index: int | None = None,
    ) -> dict[str, Any]:
        self._trusted(principal, workspace_id, write=True)
        if expected_window_index is not None and (
            type(expected_window_index) is not int or not 0 <= expected_window_index <= 2
        ):
            raise ServiceError("invalid_input", 422)
        scope = EvaluationProduct._scope(workspace_id)
        with self._frozen_clock() as now:
            row = await self.evaluation.repository.schedule(scope, schedule_id)
            try:
                spec = ScheduleSpec.model_validate(row["spec"])
                current_index = int((now - spec.epoch) // spec.interval_seconds)
            except (KeyError, TypeError, ValueError, ZeroDivisionError):
                raise ServiceError("facts_corrupt") from None
            if expected_window_index is not None and expected_window_index != current_index:
                raise ServiceError("snapshot_changed", 409)
            actor = self._schedule_actor(row, workspace_id)
            result = await self._continuous(actor, workspace_id).tick(schedule_id)
        self._wake.set()
        public_task = await self.product.task(principal, workspace_id, str(result["id"]))
        return _task_receipt(public_task)

    async def disable_schedule(
        self,
        principal: PrincipalContext,
        workspace_id: str,
        schedule_id: str,
        revision: int,
        key: str,
    ) -> dict[str, Any]:
        actor = self._trusted(principal, workspace_id, write=True)
        result = await self._continuous(actor, workspace_id).disable(schedule_id, revision, key)
        self._wake.set()
        return _schedule_view(result)

    def _continuous(self, actor: PrincipalContext, workspace_id: str) -> ContinuousEvaluation:
        return ContinuousEvaluation(
            self.evaluation.repository,
            self.evaluation.kernel,
            scope=EvaluationProduct._scope(workspace_id),
            actor=actor.principal_id,
            clock=self.clock,
        )

    def _schedule_actor(self, row: dict[str, Any], workspace_id: str) -> PrincipalContext:
        actor_id = row.get("actor")
        actor = self._actors.get(actor_id) if isinstance(actor_id, str) else None
        if actor is None:
            raise ServiceError("permission_denied", 403)
        actor.member(workspace_id, write=True)
        return actor

    async def _schedule_counts(self, workspace_id: str) -> tuple[int, int]:
        scope = EvaluationProduct._scope(workspace_id)
        async with (
            self.evaluation.repository.lock,
            self.evaluation.repository.engine.begin() as conn,
        ):
            total = (
                await conn.execute(
                    select(func.count()).select_from(eval_schedules).where(
                        eval_schedules.c.scope == scope
                    )
                )
            ).scalar_one()
            enabled = (
                await conn.execute(
                    select(func.count()).select_from(eval_schedules).where(
                        eval_schedules.c.scope == scope,
                        eval_schedules.c.body["enabled"].as_boolean().is_(True),
                    )
                )
            ).scalar_one()
        return int(total), int(enabled)

    async def _pending_policies(self, workspace_id: str) -> list[CapturePolicy]:
        _policy, rows = await self._workspace_facts(workspace_id)
        policies: dict[str, CapturePolicy] = {}
        for row in rows:
            if row.get("phase") not in {"pending", "claimed"}:
                continue
            record = CaptureRecord.model_validate(row["body"])
            if (
                record.workspace_id != workspace_id
                or record.scope != EvaluationProduct._scope(workspace_id)
                or record.run_id != row.get("run_id")
            ):
                raise ServiceError("facts_corrupt")
            policies[record.policy_fingerprint] = record.policy
        return list(policies.values())

    async def _consume_workspace(self, workspace_id: str) -> None:
        policies = await self._pending_policies(workspace_id)
        for policy in policies:
            if self._closing:
                return
            observer = self._actors.get(policy.observer)
            if observer is None or _observer_generation_salt(observer) != policy.salt_id:
                continue
            try:
                if observer.member(workspace_id).role != "admin":
                    continue
            except ServiceError:
                continue
            port = KrimOutboxPort(self.store, self.sessions, observer, policy)
            actors: dict[str, PrincipalContext] = {}
            for candidate in self._actors.values():
                try:
                    candidate.member(workspace_id)
                except ServiceError:
                    continue
                actors[candidate.principal_id] = candidate
            views = ActorViews(
                port,
                self.evaluation.repository,
                self.evaluation.kernel,
                actors,
            )
            destination = self._destination_id
            if destination is None:
                raise ServiceError("target_binding_changed")
            flow = HotObservationFlow(
                port,
                self.evaluation.repository,
                self.evaluation.kernel,
                destination,
                views.resolve,
                views.binding,
                observer.generation,
            )
            await flow.consume_once(limit=1)

    async def _tick_authorized_schedules(self) -> None:
        scopes = tuple(sorted(self._scope_workspaces))
        if not scopes or self._closing:
            return
        repository = self.evaluation.repository
        async with repository.lock, repository.engine.begin() as conn:
            total = (
                await conn.execute(
                    select(func.count()).select_from(eval_schedules).where(
                        eval_schedules.c.scope.in_(scopes),
                        eval_schedules.c.body["enabled"].as_boolean().is_(True),
                    )
                )
            ).scalar_one()
            if not total:
                self._schedule_offset = 0
                return
            if self._schedule_offset >= total:
                self._schedule_offset = 0
            rows = (
                await conn.execute(
                    select(
                        eval_schedules.c.scope,
                        eval_schedules.c.schedule_id,
                        eval_schedules.c.body,
                    )
                    .where(
                        eval_schedules.c.scope.in_(scopes),
                        eval_schedules.c.body["enabled"].as_boolean().is_(True),
                    )
                    .order_by(eval_schedules.c.scope, eval_schedules.c.schedule_id)
                    .offset(self._schedule_offset)
                    .limit(16)
                )
            ).all()
        self._schedule_offset = 0 if len(rows) < 16 else self._schedule_offset + len(rows)
        for row in rows:
            if self._closing:
                return
            workspace_id = self._scope_workspaces.get(row.scope)
            if workspace_id is None:
                continue
            body = row.body
            if not isinstance(body, dict) or body.get("enabled") is not True:
                continue
            try:
                spec = ScheduleSpec.model_validate(body.get("spec"))
                if (
                    spec.scope != EvaluationProduct._scope(workspace_id)
                    or spec.schedule_id != row.schedule_id
                ):
                    raise ServiceError("facts_corrupt")
                actor = self._schedule_actor(body, workspace_id)
                with self._frozen_clock() as now:
                    if (now - spec.epoch) // spec.interval_seconds < 0:
                        continue
                    await self._continuous(actor, workspace_id).tick(row.schedule_id)
            except ServiceError as error:
                self._last_error = error.code
            except Exception:
                self._last_error = "automation_failed"

    async def _poll_round(self) -> None:
        self._last_error = None
        workspaces = sorted(set(self._scope_workspaces.values()))
        if workspaces:
            offset = self._workspace_offset % len(workspaces)
            rotated = workspaces[offset:] + workspaces[:offset]
            selected = rotated[:16]
            self._workspace_offset = (offset + len(selected)) % len(workspaces)
        else:
            selected = []
        for workspace_id in selected:
            if self._closing:
                return
            try:
                await self._consume_workspace(workspace_id)
            except ServiceError as error:
                self._last_error = error.code
            except Exception:
                self._last_error = "automation_failed"
        if not self._closing:
            await self._tick_authorized_schedules()

    async def _poll(self) -> None:
        try:
            while not self._closing:
                self._wake.clear()
                await self._poll_round()
                if self._closing:
                    break
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=self.poll_interval)
                except TimeoutError:
                    pass
        except asyncio.CancelledError:
            raise
        except Exception:
            self._last_error = "automation_failed"
        finally:
            self._running = False


def _observer_generation_salt(principal: PrincipalContext) -> str:
    """把H1已有salt_id用作持久generation绑定，不赋予任何授权。"""
    digest = hashlib.sha256(
        f"{principal.principal_id}\0{principal.generation}".encode()
    ).hexdigest()[:24]
    return "observer-" + digest


def _task_receipt(task: dict[str, Any]) -> dict[str, Any]:
    return {
        key: task[key]
        for key in (
            "id", "request_key", "status", "code", "revision", "planned_count", "finished_count"
        )
    }


def _schedule_view(row: dict[str, Any]) -> dict[str, Any]:
    spec = row.get("spec")
    if not isinstance(spec, dict):
        raise ServiceError("facts_corrupt")
    control_revision = row.get("control_revision")
    enabled = row.get("enabled")
    disable_receipt = row.get("disable_receipt")
    if type(control_revision) is not int or control_revision not in {0, 1}:
        raise ServiceError("facts_corrupt")
    if control_revision == 0:
        if enabled is not True or disable_receipt is not None:
            raise ServiceError("facts_corrupt")
        receipt = None
    else:
        if (
            enabled is not False
            or not isinstance(disable_receipt, dict)
            or set(disable_receipt) != {"key", "request_hash"}
            or not isinstance(spec.get("scope"), str)
            or not isinstance(spec.get("schedule_id"), str)
        ):
            raise ServiceError("facts_corrupt")
        key = disable_receipt["key"]
        if (
            not isinstance(key, str)
            or not 1 <= len(key) <= 64
            or any(not char.isascii() or not (char.isalnum() or char in "_.-") for char in key)
            or disable_receipt["request_hash"]
            != neutral_fingerprint(
                {
                    "scope": spec["scope"],
                    "id": spec["schedule_id"],
                    "revision": 0,
                    "key": key,
                }
            )
        ):
            raise ServiceError("facts_corrupt")
        receipt = {
            "key": key,
            "decision": "disabled",
            "expected_revision": 0,
            "revision": 1,
        }
    return {
        "schedule_id": spec.get("schedule_id"),
        "version": spec.get("version"),
        "enabled": enabled,
        "revision": control_revision,
        "observed_through": row.get("observed_through"),
        "missed_windows": len(row.get("missed", [])) if isinstance(row.get("missed"), list) else 0,
        "control_receipt": receipt,
    }
