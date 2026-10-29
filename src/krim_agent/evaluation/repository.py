"""独立safe-eval/1持久边界；不导入旧Store，不伪造Run或自动重发目标。"""

from __future__ import annotations

import asyncio
import fcntl
import os
import stat
import time
import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, cast
from urllib.parse import quote

from sqlalchemy import JSON, Column, Integer, MetaData, String, Table, event, insert, select, update
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.pool import NullPool

from krim_agent.contracts import ServiceError

from .domain import (
    DATASET_BYTES,
    OBSERVATION_BYTES,
    PERSISTED_TASK_BYTES,
    REPORT_BYTES,
    SNAPSHOT_BYTES,
    BatchPlan,
    DatasetSpec,
    ExecutionRef,
    Observation,
    TaskSpec,
    TrialSpec,
    clone,
    execution_conditions,
    neutral_encoded,
    neutral_parse,
)
from .domain import (
    neutral_fingerprint as fingerprint,
)
from .lifecycle import settled
from .metrics import MetricRegistry
from .ports import AgentAdapter, MetricPort
from .reports import make_neutral_report, validate_neutral_report

FORMAT = "safe-eval/1"
APPLICATION_ID = 0x4B455631
metadata = MetaData()
meta = Table(
    "eval_meta",
    metadata,
    Column("id", Integer, primary_key=True),
    Column(
        "body",
        JSON,
        nullable=False,
    ),
)
datasets = Table(
    "dataset_revisions",
    metadata,
    Column("name", String, primary_key=True),
    Column(
        "version",
        String,
        primary_key=True,
    ),
    Column(
        "body",
        JSON,
        nullable=False,
    ),
)
tasks = Table(
    "eval_tasks",
    metadata,
    Column("id", String, primary_key=True),
    Column(
        "scope",
        String,
        nullable=False,
    ),
    Column("request_key", String, nullable=False),
    Column(
        "body",
        JSON,
        nullable=False,
    ),
)
trials = Table(
    "eval_trials",
    metadata,
    Column("task_id", String, primary_key=True),
    Column(
        "case_id",
        String,
        primary_key=True,
    ),
    Column("repetition", Integer, primary_key=True),
    Column(
        "body",
        JSON,
        nullable=False,
    ),
)
evidence = Table(
    "eval_evidence",
    metadata,
    Column("digest", String, primary_key=True),
    Column(
        "body",
        JSON,
        nullable=False,
    ),
)
events = Table(
    "eval_task_events",
    metadata,
    Column("task_id", String, primary_key=True),
    Column(
        "revision",
        Integer,
        primary_key=True,
    ),
    Column(
        "body",
        JSON,
        nullable=False,
    ),
)
reports = Table(
    "eval_reports",
    metadata,
    Column("task_id", String, primary_key=True),
    Column(
        "body",
        JSON,
        nullable=False,
    ),
)


source_observations = Table(
    "source_observations",
    metadata,
    Column("scope", String, primary_key=True),
    Column("observation_id", String, primary_key=True),
    Column("dedup_digest", String, nullable=False, unique=True),
    Column("body", JSON, nullable=False),
)


case_candidates = Table(
    "case_candidates",
    metadata,
    Column("scope", String, primary_key=True),
    Column("candidate_id", String, primary_key=True),
    Column("revision", Integer, primary_key=True),
    Column("body", JSON, nullable=False),
)


case_confirmations = Table(
    "case_confirmations",
    metadata,
    Column("scope", String, primary_key=True),
    Column("confirmation_key", String, primary_key=True),
    Column("body", JSON, nullable=False),
)


dataset_lineage = Table(
    "dataset_lineage",
    metadata,
    Column("dataset_name", String, primary_key=True),
    Column("version", String, primary_key=True),
    Column("freeze_key", String, nullable=False, unique=True),
    Column("body", JSON, nullable=False),
)


continuous_inputs = Table(
    "continuous_inputs",
    metadata,
    Column("task_id", String, primary_key=True),
    Column("case_id", String, primary_key=True),
    Column("repetition", Integer, primary_key=True),
    Column("body", JSON, nullable=False),
)


eval_schedules = Table(
    "eval_schedules",
    metadata,
    Column("scope", String, primary_key=True),
    Column("schedule_id", String, primary_key=True),
    Column("body", JSON, nullable=False),
)


eval_schedule_windows = Table(
    "eval_schedule_windows",
    metadata,
    Column("scope", String, primary_key=True),
    Column("schedule_id_version", String, primary_key=True),
    Column("dataset_envelope_fp", String, primary_key=True),
    Column("target_fp", String, primary_key=True),
    Column("window_index", Integer, primary_key=True),
    Column("body", JSON, nullable=False),
)


class Repository:
    def __init__(self, path: Path, readonly: bool = False) -> None:
        self.path, self.readonly = path.absolute(), readonly
        self.closed = False
        self.closing = False
        self.healthy = True
        self.lock = asyncio.Lock()
        self._writer_fd: int | None = None
        self._close_task: asyncio.Task[None] | None = None
        self.registry: dict[str, AgentAdapter] = {}
        self.metric_registry = MetricRegistry()
        self.resources: list[dict[str, Any]] = []
        url = (
            URL.create(
                "sqlite+aiosqlite",
                database=f"file:{quote(str(self.path), safe='/')}",
                query={"mode": "ro", "uri": "true"},
            )
            if readonly
            else URL.create(
                "sqlite+aiosqlite",
                database=str(self.path),
            )
        )
        self.engine = create_async_engine(
            url,
            poolclass=NullPool,
            json_serializer=neutral_encoded,
            json_deserializer=neutral_parse,
        )
        for kind in ["connect", "checkout", "reset", "checkin", "close"]:

            def record(*args: Any, kind: str = kind) -> None:
                self.resources.append(
                    {
                        "event": kind,
                        "connection": id(args[0]),
                        "time": time.monotonic(),
                    }
                )

            event.listen(self.engine.sync_engine, kind, record)

    @classmethod
    async def open(cls, path: Path, *, readonly: bool = False) -> Repository:
        from .scale_store import migrate

        repo = cls(path, readonly)
        try:
            try:
                st = await asyncio.to_thread(path.lstat)
            except FileNotFoundError:
                if readonly:
                    raise ServiceError("not_found", 404) from None
            else:
                if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1:
                    raise ServiceError("database_invalid")
            if not readonly:
                path.parent.mkdir(parents=True, exist_ok=True)
                fd = os.open(str(path) + ".lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
                repo._writer_fd = fd
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    raise ServiceError("database_busy") from None
            additions = {
                "source_observations",
                "case_candidates",
                "case_confirmations",
                "dataset_lineage",
                "continuous_inputs",
                "eval_schedules",
                "eval_schedule_windows",
            }
            scaled = {"dataset_cases", "eval_report_pages", "eval_resource_receipts"}
            legacy = set(metadata.tables) - additions - scaled
            version2 = set(metadata.tables) - scaled
            async with repo.engine.begin() as conn:
                await conn.exec_driver_sql("BEGIN" if readonly else "BEGIN IMMEDIATE")
                app = (await conn.exec_driver_sql("PRAGMA application_id")).scalar_one()
                version = (await conn.exec_driver_sql("PRAGMA user_version")).scalar_one()
                names = set(
                    (
                        await conn.exec_driver_sql(
                            "SELECT name FROM sqlite_master WHERE type='table'"
                        )
                    ).scalars()
                )
                if not names and not readonly and app == 0 and version == 0:
                    await conn.run_sync(metadata.create_all)
                    await conn.exec_driver_sql(f"PRAGMA application_id={APPLICATION_ID}")
                    await conn.exec_driver_sql("PRAGMA user_version=3")
                    marker = {
                        "format": FORMAT,
                        "cleanup_unknown": False,
                        "extension": "continuous-eval/1",
                        "repository_id": str(uuid.uuid4()),
                    }
                    await conn.execute(insert(meta).values(id=1, body=marker))
                else:
                    if app != APPLICATION_ID or (version, names) not in [
                        (1, legacy),
                        (2, version2),
                        (3, set(metadata.tables)),
                    ]:
                        raise ServiceError("database_invalid")
                    marker = (
                        await conn.execute(select(meta.c.body).where(meta.c.id == 1))
                    ).scalar_one()
                    if marker.get("format") != FORMAT:
                        raise ServiceError("database_invalid")
                    if version >= 2 and (
                        marker.get("extension") != "continuous-eval/1"
                        or not isinstance(marker.get("repository_id"), str)
                    ):
                        raise ServiceError("database_invalid")
                    if not readonly and version < 3:
                        if version == 1:
                            for name in sorted(additions):
                                await conn.run_sync(metadata.tables[name].create)
                            marker = marker | {
                                "extension": "continuous-eval/1",
                                "repository_id": str(uuid.uuid4()),
                            }
                            await conn.execute(
                                update(meta).where(meta.c.id == 1).values(body=marker)
                            )
                        await migrate(conn)
                        await conn.exec_driver_sql("PRAGMA user_version=3")
                repo.healthy = not marker.get("cleanup_unknown", False)
            if not readonly:
                await repo._recover()
            return repo
        except BaseException:
            await settled(repo.close())
            raise

    async def _recover(self) -> None:
        from .scale_store import recover

        async with self.lock, self.engine.begin() as conn:
            after: str | None = None
            while True:
                query = select(tasks.c.id)
                if after is not None:
                    query = query.where(tasks.c.id > after)
                identities = (
                    (await conn.execute(query.order_by(tasks.c.id).limit(16))).scalars().all()
                )
                if not identities:
                    break
                for identity in identities:
                    snapshot = await self._snapshot(conn, identity)
                    if snapshot["status"] not in {"queued", "running"}:
                        continue
                    if snapshot.get("layout") == 3:
                        snapshot = await recover(conn, snapshot, "interrupted", "execution_unknown")
                        if snapshot["cleanup_unknown"]:
                            self.healthy = False
                        await self._save(conn, snapshot, final=True)
                        continue
                    uncertain = any(
                        i["status"] in {"launch_intent", "executing"}
                        and (i["output"] or {}).get("cleanup") != "closed"
                        for i in snapshot["items"]
                    )
                    if uncertain:
                        marker = (
                            await conn.execute(select(meta.c.body).where(meta.c.id == 1))
                        ).scalar_one()
                        await conn.execute(
                            update(meta)
                            .where(meta.c.id == 1)
                            .values(body=marker | {"cleanup_unknown": True})
                        )
                        self.healthy = False
                    snapshot["status"], snapshot["code"] = "interrupted", "execution_unknown"
                    for item in snapshot["items"]:
                        if item["status"] != "finished":
                            item["status"] = (
                                "unknown"
                                if item["status"] in {"launch_intent", "executing"}
                                else "interrupted"
                            )
                    snapshot["finished_at"] = time.time()
                    await self._save(conn, snapshot, final=True)
                after = identities[-1]

    def register(self, key: str, adapter: AgentAdapter) -> None:
        if self.readonly or key in self.registry:
            raise ServiceError("readonly" if self.readonly else "invalid_input")
        self.registry[key] = adapter

    def adapter(self, request: dict[str, Any]) -> AgentAdapter:
        adapter = self.registry.get(request["target_key"])
        if adapter is None:
            raise ServiceError("invalid_input", 422)
        return adapter

    def metrics(self) -> MetricPort:
        return self.metric_registry

    async def _write(self, operation: Callable[[AsyncConnection], Awaitable[Any]]) -> Any:
        if self.closed or self.closing:
            raise ServiceError("closed")
        if self.readonly:
            raise ServiceError("readonly")
        async with self.lock:
            if self.closed or self.closing:
                raise ServiceError("closed")
            async with self.engine.begin() as conn:
                return await operation(conn)

    async def _confirmed(
        self,
        operation: Callable[[AsyncConnection], Awaitable[Any]],
        confirm: Callable[[], Awaitable[Any]],
    ) -> Any:
        async def complete() -> Any:
            try:
                return await self._write(operation)
            except ServiceError:
                raise
            except Exception:
                try:
                    found = await confirm()
                except ServiceError as error:
                    self.healthy = False
                    if error.code in {
                        "target_binding_changed",
                        "source_changed",
                        "permission_denied",
                    }:
                        raise
                    found = None
                except Exception:
                    found = None
                if found is not None:
                    return found
                self.healthy = False
                raise ServiceError("storage_unavailable", 503) from None

        return await settled(complete())

    async def freeze(self, spec: DatasetSpec) -> DatasetSpec:
        frozen = DatasetSpec.model_validate(clone(spec.model_dump(mode="json")))

        async def operation(conn: AsyncConnection) -> DatasetSpec:
            return await self._freeze_dataset_conn(conn, frozen)

        async def confirm() -> DatasetSpec | None:
            old = await self.dataset(frozen.name, frozen.version)
            return old if old == frozen else None

        return cast(DatasetSpec, await self._confirmed(operation, confirm))

    async def dataset(self, name: str, version: str) -> DatasetSpec:
        async with self.lock:
            if self.closed:
                raise ServiceError("closed")
            async with self.engine.begin() as conn:
                body = (
                    await conn.execute(
                        select(datasets.c.body).where(
                            datasets.c.name == name,
                            datasets.c.version == version,
                        )
                    )
                ).scalar_one_or_none()
                if body is None:
                    raise ServiceError("not_found", 404)
                return DatasetSpec.model_validate(body)

    async def by_key(self, request: dict[str, Any]) -> dict[str, Any] | None:
        async with self.lock:
            if self.closed:
                raise ServiceError("closed")
            async with self.engine.begin() as conn:
                body = (
                    await conn.execute(
                        select(tasks.c.body).where(
                            tasks.c.scope == request["scope"],
                            tasks.c.request_key == request["request_key"],
                        )
                    )
                ).scalar_one_or_none()
                return cast(dict[str, Any] | None, clone(body))

    async def snapshot(self, task_id: str) -> dict[str, Any]:
        if self.closed:
            raise ServiceError("closed")
        async with self.lock:
            if self.closed:
                raise ServiceError("closed")
            async with self.engine.begin() as conn:
                return await self._snapshot(conn, task_id)

    async def _snapshot(self, conn: AsyncConnection, task_id: str) -> dict[str, Any]:
        body = (
            await conn.execute(select(tasks.c.body).where(tasks.c.id == task_id))
        ).scalar_one_or_none()
        if body is None:
            raise ServiceError("not_found", 404)
        if isinstance(body, dict) and body.get("layout") == 3:
            # JSON列已由neutral_parse校验并新建对象；不再编码/解析同一紧凑快照。
            return body
        return cast(dict[str, Any], clone(body))

    async def begin(self, value: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        if value.get("schema_version") == "continuous-task/1":
            return await self._begin_continuous(value)
        request = TaskSpec.model_validate(value)
        adapter = self.adapter(value)
        target = clone(adapter.descriptor())
        adapter.assert_current(target)
        from .adapters.profiles import CampaignStoreBinding

        conditions_value = target.get("execution_conditions", {})
        binding = (
            CampaignStoreBinding(clone(conditions_value["admission"]))
            if conditions_value.get("schema_version") == "native-cli/1"
            else None
        )
        if binding is not None:
            binding.assert_store(self.path, writer_fd=self._writer_fd)
            if self.engine.url.database != str(binding.database) or self._writer_fd is None:
                raise ServiceError("target_binding_changed")
        elif conditions_value.get("schema_version") == "local-process/1" and (
            request.execution.item_timeout_seconds > 20
            or request.execution.batch_timeout_seconds > 120
        ):
            raise ServiceError("invalid_input", 422)
        self.metric_registry.bind([m.model_dump(mode="json") for m in request.metrics])
        dataset = await self.dataset(request.dataset.name, request.dataset.version)
        adapter.assert_current(target)
        if binding is not None:
            binding.assert_store(self.path, writer_fd=self._writer_fd)
            slot = binding.slot(value, dataset.model_dump(mode="json"))
            actual_budget = request.execution.model_dump(mode="json")
            common = conditions_value["common"]
            if (
                common.get("budget") != actual_budget
                or common.get("dataset_fingerprint") != dataset.ref().fingerprint
                or common.get(
                    "metric_fingerprint",
                )
                != fingerprint([m.model_dump(mode="json") for m in request.metrics])
                or slot.get("execution") != actual_budget
            ):
                raise ServiceError("invalid_input", 422)
        else:
            slot = None
        if dataset.ref() != request.dataset:
            raise ServiceError("invalid_input", 422)
        conditions = execution_conditions(target.get("execution_conditions"))
        from .continuous_store import reserve_capacity

        capacity = reserve_capacity(dataset, request.execution)
        if capacity["schema_version"] == "neutral-capacity/2":
            import shutil

            if shutil.disk_usage(self.path.parent).free < capacity["required_free_bytes"]:
                raise ServiceError("body_too_large", 422)
        elif (
            capacity["dataset_bytes"] > DATASET_BYTES
            or capacity["report_bytes"] != REPORT_BYTES
            or capacity["max_task_storage_bytes"] != PERSISTED_TASK_BYTES
        ):
            raise ServiceError("body_too_large", 422)
        task_id = str(uuid.uuid4())

        async def domain_check(conn: AsyncConnection) -> None:
            if binding is None:
                return
            binding.assert_store(self.path, writer_fd=self._writer_fd)
            databases = (await conn.exec_driver_sql("PRAGMA database_list")).all()
            if not any(r[1] == "main" and r[2] == str(binding.database) for r in databases):
                raise ServiceError("target_binding_changed")
            marker = (await conn.execute(select(meta.c.body))).scalar_one()
            if marker.get("native_campaign") != binding.marker:
                raise ServiceError("target_binding_changed")
            binding.assert_store(self.path, writer_fd=self._writer_fd)

        async def operation(conn: AsyncConnection) -> tuple[dict[str, Any], bool]:
            return await self._begin_task_conn(
                conn,
                request,
                value,
                adapter,
                target,
                dataset,
                conditions,
                capacity,
                task_id,
                binding,
                slot,
                domain_check,
            )

        async def confirm() -> tuple[dict[str, Any], bool] | None:
            if binding is not None:
                binding.assert_store(self.path, writer_fd=self._writer_fd)
            old = await self.by_key(value)
            if binding is not None:
                binding.assert_store(self.path, writer_fd=self._writer_fd)
            return (
                (old, old["id"] == task_id)
                if old is not None and old["request_hash"] == fingerprint(value)
                else None
            )

        return cast(tuple[dict[str, Any], bool], await self._confirmed(operation, confirm))

    def plan(self, snapshot: dict[str, Any]) -> BatchPlan:
        if snapshot.get("layout") == 3:
            budget = snapshot["execution"]
            return BatchPlan(
                [],
                snapshot["rules"],
                budget["item_timeout_seconds"],
                budget["scorer_timeout_seconds"],
                budget["batch_timeout_seconds"],
                mode=(snapshot.get("continuous") or {}).get("mode", "execute"),
                concurrency=budget["concurrency"],
                paged=True,
                plan_fingerprint=snapshot["plan_fingerprint"],
            )
        if snapshot.get("continuous") is not None:
            from .continuous_types import ContinuousRequest

            request = ContinuousRequest.model_validate(snapshot["request_json"])
            context = snapshot["continuous"]
            cases = {c["case_id"]: c for c in snapshot["dataset"]["cases"]}
            links = {c["case_id"]: c for c in context["lineage"]["cases"]}
            rules = snapshot["rules"]
            try:
                bound = [(r, self.metric_registry._lookup(r)) for r in rules]
            except ValueError:
                raise ServiceError("target_binding_changed") from None
            methods = tuple(
                getattr(self.metric_registry, name).__func__
                for name in ["score", "identity", "result"]
            )

            def guard() -> None:
                try:
                    if any(self.metric_registry._lookup(r) is not old for r, old in bound):
                        raise ValueError("metric_changed")
                    for name, expected in zip(
                        ["score", "identity", "result"], methods, strict=True
                    ):
                        method = getattr(self.metric_registry, name)
                        if getattr(method, "__func__", method) is not expected:
                            raise ValueError("metric_changed")
                except ValueError:
                    raise ServiceError("target_binding_changed") from None

            trials_plan, inputs = [], {}
            for item in snapshot["items"]:
                case = clone(cases[item["case_id"]])
                metadata = context["inputs"].get(f"{item['case_id']}:{item['repetition']}", {})
                case["_continuous"] = {
                    "lineage_case": links[item["case_id"]],
                    "mode": request.mode,
                    "known_hard_failure": metadata.get("known_hard_failure", False),
                    "source_context_hash": metadata.get("source_context_hash"),
                    "source_proof": clone(metadata.get("source_proof")),
                    "availability": metadata.get("availability"),
                }
                trials_plan.append(
                    TrialSpec(
                        snapshot["id"],
                        item["case_id"],
                        item["repetition"],
                        case,
                        snapshot["target"],
                    )
                )
                inputs[(item["case_id"], item["repetition"])] = {
                    "output": clone(item["output"]),
                    "guard": guard,
                }
            return BatchPlan(
                trials_plan,
                rules,
                request.execution.item_timeout_seconds,
                request.execution.scorer_timeout_seconds,
                request.execution.batch_timeout_seconds,
                mode=request.mode,
                scoring_inputs=inputs,
            )
        cases = {c["case_id"]: c for c in snapshot["dataset"]["cases"]}
        budget = TaskSpec.model_validate(snapshot["request_json"]).execution
        return BatchPlan(
            [
                TrialSpec(
                    snapshot["id"],
                    i["case_id"],
                    i["repetition"],
                    cases[i["case_id"]],
                    snapshot["target"],
                )
                for i in snapshot["items"]
            ],
            snapshot["rules"],
            budget.item_timeout_seconds,
            budget.scorer_timeout_seconds,
            budget.batch_timeout_seconds,
        )

    async def _save(
        self,
        conn: AsyncConnection,
        snapshot: dict[str, Any],
        *,
        final: bool = False,
        guard: Callable[[], None] = lambda: None,
    ) -> None:
        outer_guard = guard

        def publication_fence() -> None:
            outer_guard()
            if (
                snapshot.get("continuous") is not None
                and final
                and snapshot["status"] == "completed"
            ):
                for name in ["score", "identity", "result"]:
                    method = getattr(self.metric_registry, name)
                    if getattr(method, "__func__", method) is not getattr(MetricRegistry, name):
                        raise ServiceError("target_binding_changed")
                try:
                    for rule in snapshot["rules"]:
                        self.metric_registry._lookup(rule)
                except ValueError:
                    raise ServiceError("target_binding_changed") from None
                if snapshot["continuous"]["mode"] == "execute":
                    self.adapter(snapshot["request_json"]).assert_current(snapshot["target"])

        if snapshot.get("layout") == 3:
            from .scale_store import finalize

            publication_fence()
            snapshot["revision"] += 1
            if final:
                root = await finalize(conn, snapshot, publication_fence)
                snapshot["report_hash"] = root["report_hash"]
            neutral_encoded(snapshot, max_bytes=262144)
            await conn.execute(
                update(tasks).where(tasks.c.id == snapshot["id"]).values(body=snapshot)
            )
            publication_fence()
            await conn.execute(
                insert(events).values(
                    task_id=snapshot["id"],
                    revision=snapshot["revision"],
                    body={
                        "layout": 3,
                        "status": snapshot["status"],
                        "action": "final" if final else "admitted",
                    },
                )
            )
            publication_fence()
            return
        publication_fence()
        snapshot["revision"] += 1
        if final:
            snapshot["report"] = make_neutral_report(snapshot)
            await conn.execute(
                insert(reports).values(
                    task_id=snapshot["id"],
                    body=snapshot["report"],
                )
            )
            publication_fence()
        neutral_encoded(snapshot, max_bytes=SNAPSHOT_BYTES)
        await conn.execute(update(tasks).where(tasks.c.id == snapshot["id"]).values(body=snapshot))
        publication_fence()
        for item in snapshot["items"]:
            where = (
                trials.c.task_id == snapshot["id"],
                trials.c.case_id == item["case_id"],
                trials.c.repetition == item["repetition"],
            )
            old = (await conn.execute(select(trials.c.body).where(*where))).scalar_one_or_none()
            if old is None:
                await conn.execute(
                    insert(trials).values(
                        task_id=snapshot["id"],
                        case_id=item["case_id"],
                        repetition=item["repetition"],
                        body=item,
                    )
                )
                publication_fence()
            else:
                await conn.execute(update(trials).where(*where).values(body=item))
                publication_fence()
        await conn.execute(
            insert(events).values(
                task_id=snapshot["id"],
                revision=snapshot["revision"],
                body={
                    "status": snapshot["status"],
                    "items": [
                        {
                            "case_id": i["case_id"],
                            "repetition": i["repetition"],
                            "status": i["status"],
                            "execution_ref": i["execution_ref"],
                        }
                        for i in snapshot["items"]
                    ],
                },
            )
        )
        publication_fence()

    async def _mutate(
        self,
        task_id: str,
        change: Callable[[dict[str, Any]], bool],
        verify: Callable[[dict[str, Any]], bool],
        *,
        scale_action: tuple[str, TrialSpec | None, dict[str, Any]] | None = None,
    ) -> None:
        async def operation(conn: AsyncConnection) -> bool:
            snapshot = await self._snapshot(conn, task_id)
            if snapshot.get("layout") == 3 and scale_action is not None:
                from .ports import _admission_fence
                from .scale_store import mutate_trial

                host = _admission_fence().get()

                def fence() -> None:
                    if host is not None:
                        if host[0] is not self:
                            raise ServiceError("target_binding_changed")
                        host[1]()

                action, trial, payload = scale_action
                return await mutate_trial(
                    conn,
                    task_id,
                    None if trial is None else trial.case_id,
                    None if trial is None else trial.repetition,
                    action,
                    payload,
                    fence,
                    snapshot=snapshot,
                )
            if verify(snapshot):
                return True
            final = change(snapshot)
            await self._save(conn, snapshot, final=final)
            if snapshot.get("code") == "execution_unknown":
                await conn.execute(
                    update(meta)
                    .where(meta.c.id == 1)
                    .values(
                        body=(
                            await conn.execute(select(meta.c.body).where(meta.c.id == 1))
                        ).scalar_one()
                        | {"cleanup_unknown": True}
                    )
                )
                self.healthy = False
            return True

        async def confirm() -> bool | None:
            snapshot = await self.snapshot(task_id)
            if (
                snapshot.get("layout") == 3
                and scale_action is not None
                and scale_action[1] is not None
            ):
                trial = scale_action[1]
                snapshot["items"] = [
                    await self.read_trial(
                        snapshot["request_json"]["scope"], task_id, trial.case_id, trial.repetition
                    )
                ]
            return True if verify(snapshot) else None

        await self._confirmed(operation, confirm)

    async def set_running(self, task_id: str) -> None:
        def change(s: dict[str, Any]) -> bool:
            if s["status"] != "queued":
                raise ServiceError("closed")
            s["status"] = "running"
            return False

        await self._mutate(
            task_id, change, lambda s: s["status"] == "running", scale_action=("running", None, {})
        )

    @staticmethod
    def _item(snapshot: dict[str, Any], trial: TrialSpec) -> dict[str, Any]:
        return next(
            i
            for i in snapshot["items"]
            if i["case_id"] == trial.case_id and i["repetition"] == trial.repetition
        )

    async def intent(self, trial: TrialSpec) -> None:
        def change(s: dict[str, Any]) -> bool:
            i = self._item(s, trial)
            if i["status"] != "planned" or s["stop_requested"]:
                raise ServiceError("execution_unknown")
            i["status"] = "launch_intent"
            return False

        await self._mutate(
            trial.task_id,
            change,
            lambda s: self._item(s, trial)["status"] == "launch_intent",
            scale_action=("intent", trial, {}),
        )

    async def attach(self, trial: TrialSpec, ref: ExecutionRef) -> None:
        def change(s: dict[str, Any]) -> bool:
            item = self._item(s, trial)
            if item["execution_ref"] not in {None, ref.id}:
                raise ServiceError("key_conflict")
            if item["status"] != "launch_intent":
                raise ServiceError("execution_unknown")
            item["execution_ref"], item["status"] = ref.id, "executing"
            return False

        await self._mutate(
            trial.task_id,
            change,
            lambda s: self._item(s, trial)["execution_ref"] == ref.id,
            scale_action=("attach", trial, {"execution_ref": ref.id}),
        )

    async def record(
        self,
        trial: TrialSpec,
        ref: ExecutionRef,
        observation: dict[str, Any] | None,
    ) -> None:
        if observation is None:
            return
        frozen = Observation.model_validate(clone(observation)).model_dump(mode="json")
        if frozen["execution_ref"] != ref.id:
            raise ServiceError("facts_corrupt")
        digest = fingerprint(frozen, max_bytes=OBSERVATION_BYTES)

        async def operation(conn: AsyncConnection) -> bool:
            snapshot = await self._snapshot(conn, trial.task_id)
            if snapshot.get("layout") == 3:
                from .scale_store import mutate_trial

                return await mutate_trial(
                    conn,
                    trial.task_id,
                    trial.case_id,
                    trial.repetition,
                    "record",
                    {"execution_ref": ref.id, "output": frozen},
                    snapshot=snapshot,
                )
            old = (
                await conn.execute(select(evidence.c.body).where(evidence.c.digest == digest))
            ).scalar_one_or_none()
            if old is None:
                await conn.execute(insert(evidence).values(digest=digest, body=frozen))
            item = self._item(snapshot, trial)
            if item["execution_ref"] not in {None, ref.id}:
                raise ServiceError("key_conflict")
            if item["output"] is not None and fingerprint(item["output"]) != digest:
                raise ServiceError("key_conflict")
            if item["output"] is None:
                item["execution_ref"], item["output"] = ref.id, frozen
                await self._save(conn, snapshot)
            return True

        async def confirm() -> bool | None:
            snapshot = await self.snapshot(trial.task_id)
            item = (
                await self.read_trial(
                    snapshot["request_json"]["scope"],
                    trial.task_id,
                    trial.case_id,
                    trial.repetition,
                )
                if snapshot.get("layout") == 3
                else self._item(snapshot, trial)
            )
            return True if item["execution_ref"] == ref.id and item["output"] == frozen else None

        await self._confirmed(operation, confirm)

    async def commit(self, trial: TrialSpec, body: dict[str, Any]) -> None:
        frozen = clone(body)
        neutral_encoded(frozen.get("scores"), max_bytes=65536)

        def payload(item: dict[str, Any]) -> tuple[dict[str, Any], str]:
            value = clone(frozen)
            if item["output"] is not None:
                if value["output"] is None:
                    value["output"] = clone(item["output"])
                elif fingerprint(value["output"]) != fingerprint(item["output"]):
                    raise ServiceError("key_conflict")
            return value, fingerprint(value)

        def change(s: dict[str, Any]) -> bool:
            item = self._item(s, trial)
            value, digest = payload(item)
            if item["content_hash"] is not None:
                if item["content_hash"] != digest:
                    raise ServiceError("key_conflict")
                return False
            if s["status"] not in {"running", "queued"}:
                raise ServiceError("closed")
            item.update(value, status="finished", content_hash=digest)
            if all(i["status"] == "finished" for i in s["items"]):
                s["status"], s["finished_at"] = "completed", time.time()
                return True
            return False

        await self._mutate(
            trial.task_id,
            change,
            lambda s: self._item(s, trial)["content_hash"] == payload(self._item(s, trial))[1],
            scale_action=("commit", trial, frozen),
        )

    async def stop(self, task_id: str) -> None:
        def change(s: dict[str, Any]) -> bool:
            s["stop_requested"] = True
            return False

        await self._mutate(
            task_id, change, lambda s: s["stop_requested"], scale_action=("stop", None, {})
        )

    async def finish(self, task_id: str, status: str, code: str) -> None:
        try:
            old = await self.snapshot(task_id)
        except OSError as exc:
            self.healthy = False
            raise ServiceError("storage_unavailable", 503) from exc
        if old["status"] not in {"queued", "running"}:
            return
        if old.get("layout") == 3:
            from types import CodeType

            from .kernel import EvalKernel
            from .ports import _admission_fence
            from .scale_store import recover

            binding = (
                task_id,
                old["request_json"]["scope"],
                old["request_hash"],
                old["plan_fingerprint"],
            )
            host = _admission_fence().get()
            get_facts: (
                Callable[
                    [object, str, tuple[str, str, str, str]],
                    tuple[tuple[str, str, str, str, str, int], ...],
                ]
                | None
            ) = None
            trusted_facts: tuple[tuple[str, str, str, str, str, int], ...] = ()
            witness: tuple[str, str, str] | None = None
            if host is not None and host[0] is self:
                candidate = getattr(host[1], "_no_admission_facts", None)
                owner = getattr(host[1], "_eval_owner", None)
                driver_code = next(
                    (
                        item
                        for item in EvalKernel._drive.__code__.co_consts
                        if isinstance(item, CodeType) and item.co_name == "DriverFence"
                    ),
                    None,
                )
                fact_code = next(
                    (
                        item
                        for item in (() if driver_code is None else driver_code.co_consts)
                        if isinstance(item, CodeType) and item.co_name == "_no_admission_facts"
                    ),
                    None,
                )
                method = getattr(candidate, "__func__", None)
                if (
                    fact_code is not None
                    and candidate is not None
                    and getattr(method, "__code__", None) is fact_code
                    and owner is not None
                    and type(owner[0]) is EvalKernel
                    and owner[0].repository is self
                    and owner[0].tasks.get(task_id) is owner[1]
                    and owner[1] is (current_task := asyncio.current_task())
                    and current_task is not None
                    and not current_task.done()
                    and owner[2][0]
                    and task_id in owner[0]._finalizing
                ):
                    read_facts: Callable[
                        [object, str, tuple[str, str, str, str]],
                        tuple[tuple[str, str, str, str, str, int], ...],
                    ] = candidate
                    get_facts = read_facts
                    trusted_facts = read_facts(self, task_id, binding)

            def check_facts(current: dict[str, Any]) -> None:
                if get_facts is not None and (
                    (
                        current["id"],
                        current["request_json"]["scope"],
                        current["request_hash"],
                        current["plan_fingerprint"],
                    )
                    != binding
                    or fingerprint(current["request_json"]) != binding[2]
                    or get_facts(self, task_id, binding) != trusted_facts
                ):
                    raise ServiceError("permission_denied")

            async def operation(conn: AsyncConnection) -> bool:
                nonlocal witness
                current = await self._snapshot(conn, task_id)
                check_facts(current)
                if trusted_facts:
                    scope = (
                        await conn.execute(select(tasks.c.scope).where(tasks.c.id == task_id))
                    ).scalar_one()
                    check_facts(current)
                    if scope != binding[1]:
                        raise ServiceError("facts_corrupt")
                if current["status"] not in {"queued", "running"}:
                    return True
                terminal = await recover(conn, current, status, code, _not_admitted=trusted_facts)
                check_facts(current)
                if terminal["cleanup_unknown"]:
                    self.healthy = False
                await self._save(conn, terminal, final=True)
                check_facts(current)
                witness = (terminal["status"], terminal["code"], terminal["report_hash"])
                return True

            async def confirm() -> bool | None:
                current = await self.snapshot(task_id)
                check_facts(current)
                if trusted_facts:
                    if witness != (current["status"], current["code"], current.get("report_hash")):
                        return None
                    async with self.lock, self.engine.begin() as conn:
                        for fact in trusted_facts:
                            row = (
                                await conn.execute(
                                    select(trials.c.body).where(
                                        trials.c.task_id == task_id,
                                        trials.c.case_id == fact[4],
                                        trials.c.repetition == fact[5],
                                    )
                                )
                            ).scalar_one_or_none()
                            check_facts(current)
                            if (
                                row is None
                                or row["status"] != "interrupted"
                                or row.get("code") != code
                                or any(
                                    row.get(key) is not None
                                    for key in ["execution_ref", "output_digest", "content_hash"]
                                )
                            ):
                                return None
                    return True
                return True if current["status"] == status and current.get("report_hash") else None

            await self._confirmed(operation, confirm)
            return

        def change(s: dict[str, Any]) -> bool:
            s["status"], s["code"], s["finished_at"] = status, code, time.time()
            for item in s["items"]:
                if item["status"] != "finished":
                    item["status"] = "interrupted"
            return True

        await self._mutate(task_id, change, lambda s: s["status"] == status and s["code"] == code)

    async def report(self, task_id: str) -> dict[str, Any]:
        from .scale_reports import validate_manifest

        if self.closed:
            raise ServiceError("closed")

        async with self.lock, self.engine.begin() as conn:
            root = (
                await conn.execute(select(reports.c.body).where(reports.c.task_id == task_id))
            ).scalar_one_or_none()
        if root is not None and root.get("schema_version") in {
            "safe-eval/scale/1",
            "safe-eval/scale/2",
        }:
            return validate_manifest(clone(root))
        snapshot = await self.snapshot(task_id)
        return validate_neutral_report(
            snapshot["report"] if snapshot["report"] is not None else make_neutral_report(snapshot)
        )

    async def close(self) -> None:
        if self._close_task is None:
            self.closing = True
            self._close_task = asyncio.create_task(self._close())
        await settled(self._close_task)

    async def _close(self) -> None:
        async with self.lock:
            try:
                await self.engine.dispose()
            finally:
                if self._writer_fd is not None:
                    fcntl.flock(self._writer_fd, fcntl.LOCK_UN)
                    os.close(self._writer_fd)
                    self._writer_fd = None
                self.closed = True

    async def import_observations(
        self,
        scope: str,
        key: str,
        request_hash: str,
        rows: list[dict[str, Any]] | None = None,
        guard: Callable[[], None] = lambda: None,
    ) -> list[dict[str, Any]] | None:
        from .continuous_store import import_rows

        if rows is None:
            async with self.lock:
                if self.closed:
                    raise ServiceError("closed")
                async with self.engine.begin() as conn:
                    return await import_rows(conn, scope, key, request_hash, None, guard)

        async def operation(conn: AsyncConnection) -> Any:
            return await import_rows(conn, scope, key, request_hash, clone(rows), guard)

        async def confirm() -> Any:
            return await self.import_observations(scope, key, request_hash)

        return cast(list[dict[str, Any]], await self._confirmed(operation, confirm))

    async def observation(self, scope: str, observation_id: str) -> dict[str, Any]:
        from .continuous_store import observation

        async with self.lock:
            if self.closed:
                raise ServiceError("closed")
            async with self.engine.begin() as conn:
                return await observation(conn, scope, observation_id)

    async def candidate(self, scope: str, candidate_id: str) -> dict[str, Any]:
        from .continuous_store import candidate

        async with self.lock:
            if self.closed:
                raise ServiceError("closed")
            async with self.engine.begin() as conn:
                return await candidate(conn, scope, candidate_id)

    async def _freeze_dataset_conn(self, conn: AsyncConnection, frozen: DatasetSpec) -> DatasetSpec:
        from .scale_store import dataset_cases

        old = (
            await conn.execute(
                select(datasets.c.body).where(
                    datasets.c.name == frozen.name,
                    datasets.c.version == frozen.version,
                )
            )
        ).scalar_one_or_none()
        if old is not None:
            if fingerprint(old) != frozen.ref().fingerprint:
                raise ServiceError("revision_conflict")
            return DatasetSpec.model_validate(old)
        await conn.execute(
            insert(datasets).values(
                name=frozen.name,
                version=frozen.version,
                body=frozen.model_dump(mode="json"),
            )
        )
        for ordinal, case in enumerate(frozen.cases):
            body = case.model_dump(mode="json")
            await conn.execute(
                insert(dataset_cases).values(
                    name=frozen.name,
                    version=frozen.version,
                    case_id=case.case_id,
                    ordinal=ordinal,
                    case_hash=fingerprint(body),
                    body=body,
                )
            )
        return frozen

    async def confirm_candidate(
        self,
        value: dict[str, Any],
        actor: str,
        guard: Callable[[], None] = lambda: None,
    ) -> dict[str, Any]:
        from .continuous_store import confirm_case, confirmation_receipt

        frozen = clone(value)
        request_hash = fingerprint({"request": frozen, "actor": actor})

        async def operation(conn: AsyncConnection) -> Any:
            return await confirm_case(conn, frozen, actor, request_hash, guard)

        async def confirm() -> Any:
            async with self.lock, self.engine.begin() as conn:
                return await confirmation_receipt(
                    conn, frozen["scope"], frozen["request_key"], request_hash
                )

        return cast(dict[str, Any], await self._confirmed(operation, confirm))

    async def freeze_confirmed(
        self,
        value: dict[str, Any],
        guard: Callable[[], None] = lambda: None,
    ) -> dict[str, Any]:
        from .continuous_store import freeze_cases, lineage_receipt

        frozen = clone(value)
        request_hash = fingerprint(frozen)

        async def operation(conn: AsyncConnection) -> Any:
            return await freeze_cases(conn, frozen, request_hash, self._freeze_dataset_conn, guard)

        async def confirm() -> Any:
            async with self.lock, self.engine.begin() as conn:
                return await lineage_receipt(conn, frozen, request_hash)

        return cast(dict[str, Any], await self._confirmed(operation, confirm))

    async def _begin_task_conn(
        self,
        conn: AsyncConnection,
        request: TaskSpec,
        value: dict[str, Any],
        adapter: AgentAdapter | None,
        target: dict[str, Any],
        dataset: DatasetSpec,
        conditions: dict[str, Any],
        capacity: dict[str, Any],
        task_id: str,
        binding: Any = None,
        slot: Any = None,
        domain_check: Any = None,
        continuous: dict[str, Any] | None = None,
        scoring_inputs: list[dict[str, Any]] | None = None,
        guard: Callable[[], None] = lambda: None,
    ) -> tuple[dict[str, Any], bool]:
        if domain_check is not None:
            await domain_check(conn)
        guard()
        old = (
            await conn.execute(
                select(tasks.c.body).where(
                    tasks.c.scope == request.scope,
                    tasks.c.request_key == request.request_key,
                )
            )
        ).scalar_one_or_none()
        if old is not None:
            if old["request_hash"] != fingerprint(value):
                raise ServiceError("key_conflict")
            return old, False
        if (continuous or {}).get("schema_version") == "continuous-eval/2":
            from .continuous_store import freeze_inputs

            if (
                adapter is not None
                or continuous is None
                or continuous["mode"] not in {"observed_score", "rescore"}
            ):
                raise ServiceError("facts_corrupt")
            marker = (await conn.execute(select(meta.c.body).where(meta.c.id == 1))).scalar_one()
            guard()
            compact_context = clone(continuous)
            compact_context["lineage_fingerprint"] = fingerprint(compact_context.pop("lineage"))
            snapshot = dict(
                id=task_id,
                request_json=clone(value),
                request_hash=fingerprint(value),
                repository_id=marker["repository_id"],
                scope=request.scope,
                layout=3,
                dataset_ref=request.dataset.model_dump(mode="json"),
                case_index={
                    c.case_id: fingerprint(c.model_dump(mode="json")) for c in dataset.cases
                },
                target=target,
                rules=[m.model_dump(mode="json") for m in request.metrics],
                execution=request.execution.model_dump(mode="json"),
                environment=clone(compact_context["source_environment"]),
                capacity=capacity,
                status="queued",
                code=None,
                created_at=time.time(),
                finished_at=None,
                revision=0,
                stop_requested=False,
                planned_count=len(dataset.cases) * request.execution.repetitions,
                finished_count=0,
                report=None,
                continuous=compact_context,
            )
            snapshot["plan_fingerprint"] = fingerprint(
                {
                    "request_hash": snapshot["request_hash"],
                    "dataset_ref": snapshot["dataset_ref"],
                    "case_index": snapshot["case_index"],
                    "target": target,
                    "continuous": compact_context,
                }
            )
            neutral_encoded(snapshot, max_bytes=262144)
            await freeze_inputs(conn, snapshot, [], guard)
            guard()
            for c in sorted(snapshot["case_index"]):
                for repetition in range(request.execution.repetitions):
                    incoming = (
                        await conn.execute(
                            select(
                                continuous_inputs.c.body["execution_ref"]
                                .as_string()
                                .label("execution_ref"),
                                continuous_inputs.c.body["output_digest"]
                                .as_string()
                                .label("output_digest"),
                            ).where(
                                continuous_inputs.c.task_id == task_id,
                                continuous_inputs.c.case_id == c,
                                continuous_inputs.c.repetition == repetition,
                            )
                        )
                    ).one()
                    guard()
                    await conn.execute(
                        insert(trials).values(
                            task_id=task_id,
                            case_id=c,
                            repetition=repetition,
                            body=dict(
                                case_id=c,
                                repetition=repetition,
                                status="planned",
                                execution_ref=incoming.execution_ref,
                                output_digest=incoming.output_digest,
                                scores=None,
                                quality=None,
                                code=None,
                                content_hash=None,
                            ),
                        )
                    )
                    guard()
            await conn.execute(
                insert(tasks).values(
                    id=task_id, scope=request.scope, request_key=request.request_key, body=snapshot
                )
            )
            guard()
            await self._save(conn, snapshot, guard=guard)
            guard()
            if domain_check is not None:
                await domain_check(conn)
            guard()
            return snapshot, True
        snapshot = dict(
            id=task_id,
            request_json=clone(value),
            request_hash=fingerprint(value),
            dataset=dataset.model_dump(mode="json"),
            target=target,
            rules=[m.model_dump(mode="json") for m in request.metrics],
            execution=request.execution.model_dump(mode="json"),
            environment={
                "initial_state": "fresh-files/1",
                "evidence": "regular-bounded/1",
                "execution_conditions": clone(conditions),
            },
            capacity=capacity,
            status="queued",
            code=None,
            created_at=time.time(),
            finished_at=None,
            revision=0,
            stop_requested=False,
            planned_count=len(dataset.cases) * request.execution.repetitions,
            report=None,
            items=[],
        )
        if continuous is not None:
            snapshot["continuous"] = clone(continuous)
            if continuous["mode"] != "execute":
                if not isinstance(continuous.get("source_environment"), dict):
                    raise ServiceError("facts_corrupt")
                snapshot["environment"] = clone(continuous["source_environment"])
        if binding is not None:
            assert slot is not None
            snapshot["environment"]["native_admission"] = {
                "schema_version": "pn2-slot-reservation/1",
                "campaign_id": binding.admission["campaign_id"],
                "campaign_store_id": binding.admission["campaign_store_id"],
                "store_binding_digest": binding.admission["store_binding_digest"],
                "slot_id": slot["slot_id"],
                "scope": request.scope,
                "request_key": request.request_key,
                "request_hash": fingerprint(value),
                "task_id": task_id,
                "case_id": dataset.cases[0].case_id,
                "repetition": 0,
                "reserved": True,
            }
        for case in dataset.cases:
            for repetition in range(request.execution.repetitions):
                snapshot["items"].append(
                    dict(
                        case_id=case.case_id,
                        repetition=repetition,
                        status="planned",
                        execution_ref=None,
                        output=None,
                        scores=None,
                        quality=None,
                        code=None,
                        content_hash=None,
                    )
                )
        if continuous is not None:
            from .continuous_store import freeze_inputs

            await freeze_inputs(conn, snapshot, scoring_inputs or [], guard)
            guard()
        scaled = len(dataset.cases) > 8 or request.execution.concurrency == 2
        if scaled:
            original_items = snapshot.pop("items")
            snapshot.pop("dataset")
            snapshot.update(
                layout=3,
                dataset_ref=request.dataset.model_dump(mode="json"),
                case_index={
                    c.case_id: fingerprint(c.model_dump(mode="json")) for c in dataset.cases
                },
                finished_count=0,
            )
            if snapshot.get("continuous") is not None:
                context = snapshot["continuous"]
                context["lineage_fingerprint"] = fingerprint(context.pop("lineage"))
                context["inputs_fingerprint"] = fingerprint(context["inputs"])
            snapshot["plan_fingerprint"] = fingerprint(
                {
                    "request_hash": snapshot["request_hash"],
                    "dataset_ref": snapshot["dataset_ref"],
                    "case_index": snapshot["case_index"],
                    "target": target,
                    "continuous": snapshot.get("continuous"),
                }
            )
            neutral_encoded(snapshot, max_bytes=262144)
            for item in original_items:
                output = item.pop("output")
                item["output_digest"] = None if output is None else fingerprint(output)
                await conn.execute(
                    insert(trials).values(
                        task_id=task_id,
                        case_id=item["case_id"],
                        repetition=item["repetition"],
                        body=item,
                    )
                )
                guard()
        await conn.execute(
            insert(tasks).values(
                id=task_id,
                scope=request.scope,
                request_key=request.request_key,
                body=snapshot,
            )
        )
        guard()
        await self._save(conn, snapshot, guard=guard)
        guard()
        if domain_check is not None:
            await domain_check(conn)
        guard()
        if adapter is not None:
            adapter.assert_current(target)
        guard()
        return snapshot, True

    def mode(self, request: dict[str, Any]) -> str:
        if request.get("schema_version") != "continuous-task/1":
            return "execute"
        from .continuous_types import ContinuousRequest

        return ContinuousRequest.model_validate(request).mode

    async def _begin_continuous(self, value: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        self._continuous_clock: Callable[[], float]
        from .continuous_store import prepare_task, reserve_capacity, reserve_window
        from .continuous_types import ContinuousRequest
        from .ports import _admission_fence, method_identities

        host = _admission_fence().get()
        if host is not None:
            if host[0] is not self:
                raise ServiceError("target_binding_changed")
            host[1]()

        frozen = ContinuousRequest.model_validate(clone(value))
        request = TaskSpec(
            scope=frozen.scope,
            request_key=frozen.request_key,
            dataset=frozen.dataset,
            target_key=frozen.target_key,
            metrics=frozen.metrics,
            execution=frozen.execution,
        )
        rules = [m.model_dump(mode="json") for m in frozen.metrics]
        self.metric_registry.bind(rules)
        bound = [(r, self.metric_registry._lookup(r)) for r in rules]
        metric_methods = tuple(
            getattr(self.metric_registry, n).__func__ for n in ["score", "identity", "result"]
        )
        adapter = self.adapter(value) if frozen.mode == "execute" else None
        identities = None if adapter is None else method_identities(adapter)
        target = None if adapter is None else clone(adapter.descriptor())
        if adapter is not None:
            assert target is not None
            adapter.assert_current(target)
            if target.get("execution_conditions", {}).get("schema_version") == "native-cli/1":
                raise ServiceError("invalid_input", 422)
        clock = getattr(self, "_continuous_clock", time.time)

        def guard() -> None:
            if host is not None:
                if host[0] is not self:
                    raise ServiceError("target_binding_changed")
                host[1]()
            if getattr(self, "_continuous_clock", time.time) is not clock:
                raise ServiceError("target_binding_changed")
            for name, expected in zip(["score", "identity", "result"], metric_methods, strict=True):
                method = getattr(self.metric_registry, name)
                if getattr(method, "__func__", method) is not expected:
                    raise ServiceError("target_binding_changed")
            try:
                if any(self.metric_registry._lookup(r) is not expected for r, expected in bound):
                    raise ValueError("metric_changed")
            except ValueError:
                raise ServiceError("target_binding_changed") from None
            if adapter is not None:
                if self.adapter(value) is not adapter or method_identities(adapter) != identities:
                    raise ServiceError("target_binding_changed")
                assert target is not None
                adapter.assert_current(target)

        task_id = str(uuid.uuid4())

        from sqlalchemy import func, true

        # 仅冻结本次调用的存储所有者和控制键；局部见证不等于提交证明。
        owner, engine, owner_path, writer, owner_lock = (
            self,
            self.engine,
            self.path,
            self._writer_fd,
            self.lock,
        )
        path_identity = os.lstat(owner_path)
        writer_identity = None if writer is None else os.fstat(writer)
        owner_close = self._close_task
        calling_task = asyncio.current_task()
        reserved = (task_id, request.scope, request.request_key, fingerprint(value))
        created_witness: tuple[int, str | None, str | None, int, int] | None = None
        finish_unpublished = self.finish

        def _owner_current() -> None:
            try:
                path_now = os.lstat(owner_path)
                fd_now = None if writer is None else os.fstat(writer)
                valid = (
                    self is owner
                    and self.engine is engine
                    and self.path is owner_path
                    and self.lock is owner_lock
                    and self._writer_fd == writer
                    and self._close_task is owner_close
                    and not self.closed
                    and not self.closing
                    and not self.readonly
                    and writer_identity is not None
                    and fd_now is not None
                    and stat.S_ISREG(path_now.st_mode)
                    and path_now.st_nlink == 1
                    and (path_now.st_dev, path_now.st_ino)
                    == (path_identity.st_dev, path_identity.st_ino)
                    and (fd_now.st_dev, fd_now.st_ino)
                    == (writer_identity.st_dev, writer_identity.st_ino)
                )
            except OSError:
                valid = False
            if not valid:
                self.healthy = False
                raise ServiceError("storage_unavailable", 503)

        async def _unpublished_control(
            status: str, denial: str | None
        ) -> tuple[tuple[Any, ...], ...] | None:
            from .scale_store import eval_resource_receipts

            _owner_current()
            async with owner_lock, engine.begin() as conn:
                await conn.exec_driver_sql("BEGIN")
                _owner_current()
                control = (
                    (
                        await conn.execute(
                            select(
                                tasks.c.id,
                                tasks.c.scope,
                                tasks.c.request_key,
                                tasks.c.body["request_hash"].as_string().label("request_hash"),
                                tasks.c.body["status"].as_string().label("status"),
                                tasks.c.body["code"].as_string().label("code"),
                                tasks.c.body["layout"].as_integer().label("layout"),
                                tasks.c.body["repository_id"].as_string().label("repository_id"),
                                tasks.c.body["plan_fingerprint"].as_string().label("plan"),
                                tasks.c.body["revision"].as_integer().label("revision"),
                                tasks.c.body["planned_count"].as_integer().label("planned"),
                                tasks.c.body["finished_at"].as_float().label("finished_at"),
                                func.coalesce(
                                    tasks.c.body["report_hash"].as_string(),
                                    tasks.c.body["report"]["report_hash"].as_string(),
                                ).label("report_hash"),
                                tasks.c.body["cleanup_unknown"].as_boolean().label("unknown"),
                            ).where(tasks.c.id == reserved[0])
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                _owner_current()
                if control is None or created_witness is None:
                    return None
                persisted_key = (
                    control["id"],
                    control["scope"],
                    control["request_key"],
                    control["request_hash"],
                )
                persisted_witness = (
                    control["layout"] or 2,
                    control["repository_id"],
                    control["plan"],
                    control["planned"],
                )
                if (
                    persisted_key != reserved
                    or persisted_witness != created_witness[:4]
                    or control["status"] != status
                    or control["code"] != denial
                    or control["unknown"]
                ):
                    return None
                if status == "queued" and (
                    control["revision"] != created_witness[4]
                    or control["finished_at"] is not None
                    or control["report_hash"] is not None
                ):
                    return None
                if status == "failed" and (
                    control["finished_at"] is None
                    or not control["report_hash"]
                    or control["revision"] != created_witness[4] + 1
                ):
                    return None
                # 先核控制列，再读本条请求；不读取来源、Dataset或Observation正文。
                stored_request = (
                    await conn.execute(
                        select(tasks.c.body["request_json"]).where(tasks.c.id == reserved[0])
                    )
                ).scalar_one()
                _owner_current()
                if fingerprint(stored_request) != reserved[3]:
                    return None
                if created_witness[1] is not None:
                    repository_id = (
                        await conn.execute(
                            select(meta.c.body["repository_id"].as_string()).where(meta.c.id == 1)
                        )
                    ).scalar_one()
                    _owner_current()
                    if repository_id != created_witness[1]:
                        return None
                if created_witness[0] == 3:
                    projected = select(
                        trials.c.case_id,
                        trials.c.repetition,
                        trials.c.body["status"].as_string(),
                        trials.c.body["execution_ref"].as_string(),
                        trials.c.body["output_digest"].as_string(),
                        func.json_type(trials.c.body, "$.scores"),
                        trials.c.body["quality"].as_string(),
                        trials.c.body["code"].as_string(),
                        trials.c.body["content_hash"].as_string(),
                    )
                    projected = projected.where(trials.c.task_id == reserved[0])
                    projected = projected.order_by(trials.c.case_id, trials.c.repetition)
                else:
                    items = func.json_each(tasks.c.body["items"]).table_valued("key", "value")
                    projected = select(
                        func.json_extract(items.c.value, "$.case_id"),
                        func.json_extract(items.c.value, "$.repetition"),
                        func.json_extract(items.c.value, "$.status"),
                        func.json_extract(items.c.value, "$.execution_ref"),
                        func.json_type(items.c.value, "$.output"),
                        func.json_type(items.c.value, "$.scores"),
                        func.json_extract(items.c.value, "$.quality"),
                        func.json_extract(items.c.value, "$.code"),
                        func.json_extract(items.c.value, "$.content_hash"),
                    )
                    projected = projected.select_from(tasks.join(items, true()))
                    projected = projected.where(tasks.c.id == reserved[0])
                    projected = projected.order_by(items.c.key)
                controls = (await conn.execute(projected)).all()
                _owner_current()
                expected_status = "planned" if status == "queued" else "interrupted"
                if len(controls) != created_witness[3]:
                    return None
                for row in controls:
                    if row[2] != expected_status or row[5] not in (None, "null"):
                        return None
                    if any(value is not None for value in row[6:]):
                        return None
                    if frozen.mode == "execute" and (
                        row[3] is not None or row[4] not in (None, "null")
                    ):
                        return None
                receipt = (
                    await conn.execute(
                        select(eval_resource_receipts.c.task_id)
                        .where(eval_resource_receipts.c.task_id == reserved[0])
                        .limit(1)
                    )
                ).first()
                _owner_current()
                if receipt is not None:
                    return None
                refs = tuple((row[0], row[1], row[3], row[4]) for row in controls)
            _owner_current()
            return refs

        async def _converge_unpublished(denial: str) -> None:
            # Kernel仍持有原准入锁；本次begin尚未发布，不能有对应producer。
            before = await _unpublished_control("queued", None)
            if before is None:
                raise ServiceError("storage_unavailable", 503)
            _owner_current()
            await finish_unpublished(reserved[0], "failed", denial)
            _owner_current()
            after = await _unpublished_control("failed", denial)
            if after != before:
                raise ServiceError("storage_unavailable", 503)

        async def operation(conn: AsyncConnection) -> tuple[dict[str, Any], bool]:
            nonlocal created_witness
            await conn.exec_driver_sql("BEGIN IMMEDIATE")
            guard()
            old = (
                await conn.execute(
                    select(tasks.c.body).where(
                        tasks.c.scope == request.scope,
                        tasks.c.request_key == request.request_key,
                    )
                )
            ).scalar_one_or_none()
            guard()
            if old is not None:
                if old["request_hash"] != fingerprint(value):
                    raise ServiceError("key_conflict")
                return old, False
            spec, actual, conditions, context, inputs = await prepare_task(
                conn, frozen, target, guard, task_id=task_id
            )
            guard()
            capacity = reserve_capacity(spec, request.execution, context)
            if capacity["schema_version"] == "neutral-capacity/2":
                import shutil

                if shutil.disk_usage(self.path.parent).free < capacity["required_free_bytes"]:
                    raise ServiceError("body_too_large", 422)
            # Capacity and authorization happen before the one common Task insertion.
            await reserve_window(conn, frozen, context, task_id, clock, guard, insert_row=False)
            guard()
            result = await self._begin_task_conn(
                conn,
                request,
                value,
                adapter,
                actual,
                spec,
                conditions,
                capacity,
                task_id,
                continuous=context,
                scoring_inputs=inputs,
                guard=guard,
            )
            guard()
            if result[1]:
                await reserve_window(
                    conn, frozen, context, result[0]["id"], clock, guard, insert_row=True
                )
            guard()
            if result[1] is True and result[0]["id"] == task_id:
                created_witness = (
                    result[0]["layout"] if "layout" in result[0] else 2,
                    result[0]["repository_id"] if "repository_id" in result[0] else None,
                    result[0]["plan_fingerprint"] if "plan_fingerprint" in result[0] else None,
                    result[0]["planned_count"],
                    result[0]["revision"],
                )
            return result

        async def confirm() -> tuple[dict[str, Any], bool] | None:
            try:
                guard()
                old = await self.by_key(value)
                guard()
                if old is None or old["request_hash"] != fingerprint(value):
                    return None
                if (old.get("continuous") or {}).get("schema_version") == "continuous-eval/2":
                    from .continuous_store import freeze_inputs
                    from .scale_store import header

                    async with self.lock, self.engine.begin() as conn:
                        await conn.exec_driver_sql("BEGIN")
                        guard()
                        current = await header(conn, old["id"], request.scope, guard)
                        guard()
                        await freeze_inputs(conn, current, [], guard)
                        guard()
                return old, old["id"] == task_id
            except ServiceError as error:
                if error.code in {
                    "target_binding_changed",
                    "source_changed",
                    "permission_denied",
                }:
                    self.healthy = False
                    if created_witness is not None:
                        await settled(_converge_unpublished(error.code))
                    if calling_task is not None and calling_task.cancelling():
                        raise asyncio.CancelledError from None
                raise

        return cast(tuple[dict[str, Any], bool], await self._confirmed(operation, confirm))

    async def freeze_schedule(
        self,
        value: dict[str, Any],
        actor: str,
        guard: Callable[[], None] = lambda: None,
    ) -> dict[str, Any]:
        from .continuous_store import create_schedule, prepare_task
        from .continuous_types import ScheduleSpec
        from .ports import method_identities

        spec = ScheduleSpec.model_validate(clone(value))
        target_adapter = (
            self.adapter(spec.template.model_dump(mode="json"))
            if spec.template.mode == "execute"
            else None
        )
        identities = None if target_adapter is None else method_identities(target_adapter)
        target = None if target_adapter is None else clone(target_adapter.descriptor())
        clock = getattr(self, "_continuous_clock", time.time)

        def fence() -> None:
            guard()
            if clock is not getattr(self, "_continuous_clock", time.time):
                raise ServiceError("target_binding_changed")
            if target_adapter is not None:
                if (
                    self.adapter(spec.template.model_dump(mode="json")) is not target_adapter
                    or method_identities(target_adapter) != identities
                ):
                    raise ServiceError("target_binding_changed")
                assert target is not None
                target_adapter.assert_current(target)
                if target["execution_conditions"]["schema_version"] == "native-cli/1":
                    raise ServiceError("invalid_input", 422)

        async def operation(conn: AsyncConnection) -> Any:
            await conn.exec_driver_sql("BEGIN IMMEDIATE")
            fence()
            prepared = await prepare_task(conn, spec.template, target, fence)
            fence()
            return await create_schedule(conn, spec, prepared[1], actor, fence)

        async def confirm() -> Any:
            try:
                old = await self.schedule(spec.scope, spec.schedule_id)
                return old if old["spec"] == spec.model_dump(mode="json") else None
            except ServiceError:
                return None

        return cast(dict[str, Any], await self._confirmed(operation, confirm))

    async def schedule(
        self,
        scope: str,
        schedule_id: str,
        *,
        observe: bool = False,
        guard: Callable[[], None] = lambda: None,
    ) -> dict[str, Any]:
        from .continuous_store import observe_schedule, read_schedule

        if observe:
            clock = getattr(self, "_continuous_clock", time.time)

            async def operation(conn: AsyncConnection) -> Any:
                await conn.exec_driver_sql("BEGIN IMMEDIATE")
                guard()
                return await observe_schedule(conn, scope, schedule_id, clock, guard)

            async def confirm() -> Any:
                return await self.schedule(scope, schedule_id)

            return cast(dict[str, Any], await self._confirmed(operation, confirm))
        async with self.lock:
            guard()
            if self.closed:
                raise ServiceError("closed")
            async with self.engine.begin() as conn:
                row = await read_schedule(conn, scope, schedule_id)
                guard()
                return row

    async def disable_schedule(
        self,
        scope: str,
        schedule_id: str,
        revision: int,
        key: str,
        guard: Callable[[], None] = lambda: None,
    ) -> dict[str, Any]:
        from .continuous_store import disable_schedule

        async def operation(conn: AsyncConnection) -> Any:
            await conn.exec_driver_sql("BEGIN IMMEDIATE")
            guard()
            return await disable_schedule(conn, scope, schedule_id, revision, key, guard)

        async def confirm() -> Any:
            result = await self.schedule(scope, schedule_id)
            expected = fingerprint(
                {"scope": scope, "id": schedule_id, "revision": revision, "key": key}
            )
            return (
                result
                if (result.get("disable_receipt") or {}).get("request_hash") == expected
                else None
            )

        return cast(dict[str, Any], await self._confirmed(operation, confirm))

    async def trial_page(
        self, scope: str, task_id: str, *, cursor: str | None = None, limit: int = 16
    ) -> Any:
        from .scale_store import header
        from .scale_store import trials as paged_trials

        if self.closed:
            raise ServiceError("closed")
        async with self.lock, self.engine.begin() as conn:
            await conn.exec_driver_sql("BEGIN")
            value = await header(conn, task_id, scope)
            return await paged_trials(conn, value, cursor, limit)

    async def read_trial(
        self, scope: str, task_id: str, case_id: str, repetition: int
    ) -> dict[str, Any]:
        from .ports import _admission_fence
        from .scale_store import header, input_for_trial

        if self.closed:
            raise ServiceError("closed")
        host = _admission_fence().get()

        def guard() -> None:
            if host is not None:
                if host[0] is not self:
                    raise ServiceError("target_binding_changed")
                host[1]()

        guard()
        async with self.lock, self.engine.begin() as conn:
            await conn.exec_driver_sql("BEGIN")
            guard()
            value = await header(conn, task_id, scope, guard)
            return await input_for_trial(conn, value, case_id, repetition, guard)

    async def execution_header(self, task_id: str) -> Any:
        from .paging import PlanHeader

        snapshot = await self.snapshot(task_id)
        plan = self.plan(snapshot)
        return PlanHeader(
            task_id,
            snapshot["request_json"]["scope"],
            plan.mode,
            snapshot["planned_count"],
            plan.concurrency,
            plan.item_timeout,
            plan.scorer_timeout,
            plan.batch_timeout,
            plan.rules,
            snapshot["target"],
            snapshot["request_json"]["dataset"],
            plan.plan_fingerprint or "",
            snapshot.get("layout", 2),
        )

    async def execution_page(self, task_id: str, cursor: str | None, *, limit: int = 16) -> Any:
        from .paging import ExecutionPage
        from .ports import _admission_fence
        from .scale_store import checked_lineage, dataset_cases, header, input_for_trial
        from .scale_store import trials as paged_trials

        host = _admission_fence().get()

        def guard() -> None:
            if self.closed:
                raise ServiceError("closed")
            if host is not None:
                if host[0] is not self:
                    raise ServiceError("target_binding_changed")
                host[1]()

        guard()
        async with self.lock, self.engine.begin() as conn:
            await conn.exec_driver_sql("BEGIN")
            guard()
            value = await header(conn, task_id, guard=guard)
            page = await paged_trials(conn, value, cursor, limit, plan=True, guard=guard)
            case_ids = {item["case_id"] for item in page.rows}
            bodies = (
                await conn.execute(
                    select(dataset_cases.c.case_id, dataset_cases.c.body)
                    .where(
                        dataset_cases.c.name == value["dataset_ref"]["name"],
                        dataset_cases.c.version == value["dataset_ref"]["version"],
                        dataset_cases.c.case_id.in_(case_ids),
                    )
                    .limit(16)
                )
            ).all()
            guard()
            checked_cases = {row.case_id: row.body for row in bodies}
            if set(checked_cases) != case_ids:
                raise ServiceError("facts_corrupt")
            lineage = None
            if value.get("continuous", {}).get("mode") == "execute":
                lineage = await checked_lineage(conn, value, guard)
            result = []
            for item in page.rows:
                body = checked_cases[item["case_id"]]
                if fingerprint(body) != value["case_index"][item["case_id"]]:
                    raise ServiceError("facts_corrupt")
                case = clone(body)
                if value.get("continuous") is not None:
                    # execute不读正文；score的正文仅在实际worker取得一个Trial后读取。
                    if value["continuous"]["mode"] == "execute":
                        actual = await input_for_trial(
                            conn, value, item["case_id"], item["repetition"], guard, lineage=lineage
                        )
                        case["_continuous"] = actual["_continuous"]
                result.append(
                    TrialSpec(task_id, item["case_id"], item["repetition"], case, value["target"])
                )
            neutral_encoded([x.case for x in result], max_bytes=2097152)
            return ExecutionPage(result, page.next_cursor, value["plan_fingerprint"])

    async def record_resource_receipt(self, trial: TrialSpec, receipt: dict[str, Any]) -> None:
        from .ports import _admission_fence
        from .scale_store import eval_resource_receipts, save_resource_receipt

        frozen = clone(receipt)
        host = _admission_fence().get()

        def guard() -> None:
            if host is not None:
                if host[0] is not self:
                    raise ServiceError("target_binding_changed")
                host[1]()

        async def operation(conn: AsyncConnection) -> bool:
            return await save_resource_receipt(
                conn, trial.task_id, trial.case_id, trial.repetition, frozen, guard
            )

        async def confirm() -> bool | None:
            async with self.lock, self.engine.begin() as conn:
                old = (
                    await conn.execute(
                        select(eval_resource_receipts.c.body).where(
                            eval_resource_receipts.c.task_id == trial.task_id,
                            eval_resource_receipts.c.case_id == trial.case_id,
                            eval_resource_receipts.c.repetition == trial.repetition,
                        )
                    )
                ).scalar_one_or_none()
            return True if old == frozen else None

        await self._confirmed(operation, confirm)

    async def complete(self, task_id: str) -> None:
        from .ports import _admission_fence

        host = _admission_fence().get()
        if host is None or host[0] is not self:
            raise ServiceError("permission_denied")
        guard = host[1]

        async def operation(conn: AsyncConnection) -> bool:
            guard()
            snapshot = await self._snapshot(conn, task_id)
            guard()
            if snapshot["status"] == "completed":
                return True
            if (
                snapshot.get("layout") != 3
                or snapshot["status"] != "running"
                or snapshot["stop_requested"]
                or snapshot["finished_count"] != snapshot["planned_count"]
            ):
                raise ServiceError("closed")
            snapshot.update(status="completed", finished_at=time.time())
            await self._save(conn, snapshot, final=True, guard=guard)
            guard()
            return True

        async def confirm() -> bool | None:
            snapshot = await self.snapshot(task_id)
            return (
                True if snapshot["status"] == "completed" and snapshot.get("report_hash") else None
            )

        await self._confirmed(operation, confirm)

    async def case_page(
        self, scope: str, task_id: str, *, cursor: str | None = None, limit: int = 16
    ) -> Any:
        from sqlalchemy import exists

        from .paging import Page, decode_cursor, encode_cursor
        from .scale_store import dataset_cases, header

        if self.closed:
            raise ServiceError("closed")
        if type(limit) is not int or not 1 <= limit <= 16:
            raise ServiceError("invalid_input", 422)
        async with self.lock, self.engine.begin() as conn:
            await conn.exec_driver_sql("BEGIN")
            value = await header(conn, task_id, scope)
            binding = {
                "version": "eval-page/1",
                "repository_id": value["repository_id"],
                "scope": scope,
                "task_id": task_id,
                "dataset_fingerprint": value["dataset_ref"]["fingerprint"],
                "snapshot_binding": value["dataset_ref"]["fingerprint"],
                "filter_fingerprint": fingerprint({"projection": "cases"}),
                "order": "case_id-binary",
            }
            selected = None if cursor is None else decode_cursor(cursor, binding)
            ref = value["dataset_ref"]
            query = select(dataset_cases.c.body, dataset_cases.c.case_hash).where(
                dataset_cases.c.name == ref["name"], dataset_cases.c.version == ref["version"]
            )
            if selected is not None:
                key = selected["last_key"]
                if len(key) != 1 or not isinstance(key[0], str):
                    raise ServiceError("invalid_input", 422)
                previous = (
                    await conn.execute(query.where(dataset_cases.c.case_id == key[0]))
                ).one_or_none()
                if previous is None or fingerprint(previous.body) != selected["last_row_hash"]:
                    raise ServiceError("source_changed")
                query = query.where(dataset_cases.c.case_id > key[0])
            batch = (
                await conn.execute(
                    query.order_by(dataset_cases.c.case_id.collate("BINARY")).limit(limit)
                )
            ).all()
            rows: list[dict[str, Any]] = []
            for row in batch:
                if fingerprint(row.body) != row.case_hash:
                    raise ServiceError("facts_corrupt")
                if (
                    value.get("layout") == 3
                    and value["case_index"].get(row.body["case_id"]) != row.case_hash
                ):
                    raise ServiceError("facts_corrupt")
                if len(neutral_encoded(rows + [row.body], max_bytes=2097152).encode()) > 2097152:
                    raise ServiceError("body_too_large", 422)
                rows.append(clone(row.body))
            token = None
            if rows:
                more = (
                    await conn.execute(
                        select(
                            exists().where(
                                dataset_cases.c.name == ref["name"],
                                dataset_cases.c.version == ref["version"],
                                dataset_cases.c.case_id > rows[-1]["case_id"],
                            )
                        )
                    )
                ).scalar_one()
                if more:
                    token = encode_cursor(binding, [rows[-1]["case_id"]], fingerprint(rows[-1]))
            total = (
                len(value["case_index"])
                if value.get("layout") == 3
                else value["planned_count"] // value["repetitions"]
            )
            return Page(rows, token, binding, total)

    async def report_page(
        self, scope: str, task_id: str, *, cursor: str | None = None, limit: int = 16
    ) -> Any:
        from .paging import Page, decode_cursor, encode_cursor
        from .scale_reports import validate_manifest, validate_page
        from .scale_store import eval_report_pages, header

        if self.closed:
            raise ServiceError("closed")
        if type(limit) is not int or not 1 <= limit <= 16:
            raise ServiceError("invalid_input", 422)
        input_engine, input_path = self.engine, self.path
        input_methods = tuple(
            getattr(getattr(self, name), "__func__", getattr(self, name))
            for name in ["report", "report_page", "read_trial"]
        )

        def input_guard() -> None:
            if (
                self.closed
                or self.engine is not input_engine
                or self.path != input_path
                or tuple(
                    getattr(getattr(self, name), "__func__", getattr(self, name))
                    for name in ["report", "report_page", "read_trial"]
                )
                != input_methods
            ):
                raise ServiceError("target_binding_changed")

        input_guard()
        async with self.lock, self.engine.begin() as conn:
            await conn.exec_driver_sql("BEGIN")
            value = await header(conn, task_id, scope, input_guard)
            input_guard()
            raw = (
                await conn.execute(select(reports.c.body).where(reports.c.task_id == task_id))
            ).scalar_one_or_none()
            if raw is None:
                raise ServiceError("not_found", 404)
            root = validate_manifest(raw)
            input_guard()

            v2 = (value.get("continuous") or {}).get("schema_version") == "continuous-eval/2"
            if v2 != (root.get("schema_version") == "safe-eval/scale/2"):
                raise ServiceError("facts_corrupt")
            if v2:
                from .scale_store import checked_lineage

                if (
                    root["repository_id"] != value["repository_id"]
                    or root["scope"] != scope
                    or root["eval_id"] != value["id"]
                    or root["revision"] != value["revision"]
                    or root["request_hash"] != value["request_hash"]
                    or root["report_hash"] != value.get("report_hash")
                    or root["inputs_root"] != value["continuous"]["inputs_root"]
                    or fingerprint(root["inputs_index"])
                    != fingerprint(value["continuous"]["inputs_index"])
                ):
                    raise ServiceError("facts_corrupt")
                input_lineage = await checked_lineage(conn, value, input_guard)
                input_guard()
            input_guard()
            binding = {
                "version": "eval-page/1",
                "repository_id": value["repository_id"],
                "scope": scope,
                "task_id": task_id,
                "dataset_fingerprint": root["dataset_fingerprint"],
                "snapshot_binding": root["report_hash"],
                "filter_fingerprint": fingerprint({"projection": "report"}),
                "order": "case_id-binary,repetition",
            }
            selected = None if cursor is None else decode_cursor(cursor, binding)
            last = None if selected is None else tuple(selected["last_key"])
            if last is not None and (
                len(last) != 2 or not isinstance(last[0], str) or type(last[1]) is not int
            ):
                raise ServiceError("invalid_input", 422)
            rows: list[dict[str, Any]] = []
            found = last is None
            has_more = False
            for descriptor in root["pages"]:
                if last is not None and tuple(descriptor["last"]) < last:
                    continue
                raw_page = (
                    await conn.execute(
                        select(eval_report_pages.c.body).where(
                            eval_report_pages.c.task_id == task_id,
                            eval_report_pages.c.report_revision == root["revision"],
                            eval_report_pages.c.page_index == descriptor["index"],
                        )
                    )
                ).scalar_one_or_none()
                if raw_page is None:
                    raise ServiceError("facts_corrupt")
                input_guard()
                page_rows = validate_page(raw_page, descriptor, root)
                if v2:
                    from .scale_store import input_for_trial

                    for published in page_rows:
                        actual = await input_for_trial(
                            conn,
                            value,
                            published["case_id"],
                            published["repetition"],
                            input_guard,
                            lineage=input_lineage,
                        )
                        input_guard()
                        fields = [
                            "case_id",
                            "repetition",
                            "execution_ref",
                            "output_digest",
                            "status",
                            "scores",
                            "quality",
                            "code",
                            "content_hash",
                        ]
                        if fingerprint({k: actual[k] for k in fields}) != fingerprint(
                            {k: published[k] for k in fields}
                        ):
                            raise ServiceError("facts_corrupt")
                        del actual
                for row in page_rows:
                    key = (row["case_id"], row["repetition"])
                    if last is not None and selected is not None and key == last:
                        if fingerprint(row) != selected["last_row_hash"]:
                            raise ServiceError("source_changed")
                        found = True
                    if last is not None and key <= last:
                        continue
                    if len(rows) == limit:
                        has_more = True
                        break
                    neutral_encoded(rows + [row], max_bytes=2097152)
                    rows.append(clone(row))
                if has_more:
                    break
            if not found:
                raise ServiceError("source_changed")
            token = None
            if rows and has_more:
                token = encode_cursor(
                    binding, [rows[-1]["case_id"], rows[-1]["repetition"]], fingerprint(rows[-1])
                )
            return Page(rows, token, binding, root["planned_count"])

    async def compare_paged(self, scope: str, policy: Any) -> dict[str, Any]:
        from .continuous_types import ComparisonPolicy
        from .scale_reports import compare_case, fold_gate, fold_trial
        from .scale_store import header

        frozen = ComparisonPolicy.model_validate(policy.model_dump(mode="json"))
        initial = (
            self.engine,
            self.path,
            getattr(self.report_page, "__func__", self.report_page),
            getattr(self.report, "__func__", self.report),
        )

        def guard() -> None:
            if (
                self.closed
                or (
                    self.engine,
                    self.path,
                    getattr(self.report_page, "__func__", self.report_page),
                    getattr(self.report, "__func__", self.report),
                )
                != initial
            ):
                raise ServiceError("target_binding_changed")

        guard()
        async with self.lock, self.engine.begin() as conn:
            await conn.exec_driver_sql("BEGIN")
            for identity in [frozen.baseline_id, frozen.candidate_id]:
                await header(conn, identity, scope, guard)
        before = await self.report(frozen.baseline_id)
        guard()
        after = await self.report(frozen.candidate_id)
        guard()

        async def cases(identity: str, state: dict[str, Any], root: dict[str, Any]) -> Any:
            cursor = None
            group: list[dict[str, Any]] = []
            while True:
                page = await self.report_page(scope, identity, cursor=cursor, limit=16)
                guard()
                for row in page.rows:
                    fold_trial(state, row, root["rules"])
                    if group and group[-1]["case_id"] != row["case_id"]:
                        yield group
                        group = []
                    group.append(row)
                cursor = page.next_cursor
                if cursor is None:
                    break
            if group:
                yield group

        states: list[dict[str, Any]] = [{}, {}]
        old_rows, new_rows = (
            cases(frozen.baseline_id, states[0], before),
            cases(frozen.candidate_id, states[1], after),
        )
        reasons: list[dict[str, Any]] = []
        try:
            while True:
                old = await anext(old_rows, None)
                guard()
                new = await anext(new_rows, None)
                guard()
                if old is None or new is None:
                    if old is not None or new is not None:
                        raise ServiceError("facts_corrupt")
                    break
                if (
                    before.get("summary", {}).get("cell_coverage") == 1
                    and after.get("summary", {}).get("cell_coverage") == 1
                ):
                    reasons.extend(compare_case(old, new))
            return fold_gate(frozen, before, after, reasons, states[0], states[1])
        except (ServiceError, ValueError, KeyError, TypeError):
            return {
                "status": "invalid",
                "policy": "continuous-regression/1",
                "reasons": [{"code": "report_invalid"}],
            }
        finally:
            await old_rows.aclose()
            await new_rows.aclose()

    async def commit_with_receipt(
        self, trial: TrialSpec, body: dict[str, Any], receipt: dict[str, Any]
    ) -> None:
        """同一原事务提交评分和物理关闭收据；异常只确认原事实，不补写。"""
        from .ports import _admission_fence
        from .scale_store import (
            eval_resource_receipts,
            mutate_trial,
            save_resource_receipt,
            trial_facts,
        )

        frozen = clone(body)
        frozen_receipt = clone(receipt)
        neutral_encoded(frozen.get("scores"), max_bytes=65536)
        neutral_encoded(frozen_receipt, max_bytes=16384)
        host = _admission_fence().get()
        if host is None or host[0] is not self:
            raise ServiceError("permission_denied")
        guard = host[1]
        guard()

        async def operation(conn: AsyncConnection) -> bool:
            guard()
            snapshot = await self._snapshot(conn, trial.task_id)
            guard()
            if snapshot.get("layout") != 3:
                raise ServiceError("facts_corrupt")
            await mutate_trial(
                conn,
                trial.task_id,
                trial.case_id,
                trial.repetition,
                "commit",
                frozen,
                guard,
                snapshot=snapshot,
            )
            guard()
            await save_resource_receipt(
                conn, trial.task_id, trial.case_id, trial.repetition, frozen_receipt, guard
            )
            guard()
            return True

        async def confirm() -> bool | None:
            # 确认是停止后仍可做的只读事实核验，不重新获取effect许可。
            async with self.lock, self.engine.begin() as conn:
                await conn.exec_driver_sql("BEGIN")
                snapshot = await self._snapshot(conn, trial.task_id)
                if snapshot.get("layout") != 3 or snapshot.get("id") != trial.task_id:
                    return None
                raw = (
                    await conn.execute(
                        select(trials.c.body).where(
                            trials.c.task_id == trial.task_id,
                            trials.c.case_id == trial.case_id,
                            trials.c.repetition == trial.repetition,
                        )
                    )
                ).scalar_one_or_none()
                saved = (
                    await conn.execute(
                        select(eval_resource_receipts.c.body).where(
                            eval_resource_receipts.c.task_id == trial.task_id,
                            eval_resource_receipts.c.case_id == trial.case_id,
                            eval_resource_receipts.c.repetition == trial.repetition,
                        )
                    )
                ).scalar_one_or_none()
                if raw is None or saved != frozen_receipt:
                    return None
                actual = await trial_facts(conn, snapshot, raw)
                expected = clone(frozen)
                if actual["output"] is not None:
                    if expected["output"] is None:
                        expected["output"] = clone(actual["output"])
                    elif fingerprint(expected["output"]) != fingerprint(actual["output"]):
                        return None
                if (
                    actual.get("case_id") != trial.case_id
                    or actual.get("repetition") != trial.repetition
                    or actual.get("status") != "finished"
                    or actual.get("content_hash") != fingerprint(expected)
                    or actual.get("execution_ref") != frozen_receipt.get("execution_ref")
                    or frozen_receipt.get("task_id") != trial.task_id
                    or frozen_receipt.get("case_id") != trial.case_id
                    or frozen_receipt.get("repetition") != trial.repetition
                    or frozen_receipt.get("closed") is not True
                    or any(
                        actual.get(key) != expected.get(key)
                        for key in ["output", "scores", "quality", "code"]
                    )
                ):
                    return None
                return True

        await self._confirmed(operation, confirm)
