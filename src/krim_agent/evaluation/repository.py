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
            async with repo.engine.begin() as conn:
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
                    await conn.exec_driver_sql("PRAGMA user_version=1")
                    await conn.execute(
                        insert(meta).values(
                            id=1,
                            body={
                                "format": FORMAT,
                                "cleanup_unknown": False,
                            },
                        )
                    )
                elif app != APPLICATION_ID or version != 1 or names != set(metadata.tables):
                    raise ServiceError("database_invalid")
                marker = (
                    await conn.execute(select(meta.c.body).where(meta.c.id == 1))
                ).scalar_one()
                if marker.get("format") != FORMAT:
                    raise ServiceError("database_invalid")
                repo.healthy = not marker.get("cleanup_unknown", False)
            if not readonly:
                await repo._recover()
            return repo
        except BaseException:
            await settled(repo.close())
            raise

    async def _recover(self) -> None:
        async with self.lock, self.engine.begin() as conn:
            values = (await conn.execute(select(tasks.c.body))).scalars().all()
            for raw in values:
                snapshot = clone(raw)
                if snapshot["status"] in {"queued", "running"}:
                    uncertain = any(
                        i["status"] in {"launch_intent", "executing"}
                        and (i["output"] or {}).get("cleanup") != "closed"
                        for i in snapshot["items"]
                    )
                    if uncertain:
                        await conn.execute(
                            update(meta)
                            .where(meta.c.id == 1)
                            .values(body={"format": FORMAT, "cleanup_unknown": True})
                        )
                        self.healthy = False
                    snapshot["status"], snapshot["code"] = "interrupted", "execution_unknown"
                    for item in snapshot["items"]:
                        if item["status"] != "finished":
                            item["status"] = (
                                "unknown"
                                if item["status"]
                                in {
                                    "launch_intent",
                                    "executing",
                                }
                                else "interrupted"
                            )
                    snapshot["finished_at"] = time.time()
                    await self._save(conn, snapshot, final=True)

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
            return frozen

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
        return cast(dict[str, Any], clone(body))

    async def begin(self, value: dict[str, Any]) -> tuple[dict[str, Any], bool]:
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
        dataset_bytes = len(
            neutral_encoded(dataset.model_dump(mode="json"), max_bytes=DATASET_BYTES).encode()
        )
        planned = len(dataset.cases) * request.execution.repetitions
        # 含控制字符最大6倍转义、完整双流和元数据的每Trial预留。
        report_reservation = dataset_bytes + planned * (OBSERVATION_BYTES + 65536) + 1048576
        if report_reservation > REPORT_BYTES or 2 * report_reservation > SNAPSHOT_BYTES:
            raise ServiceError("body_too_large", 422)
        storage_reservation = 4 * dataset_bytes + 5 * planned * OBSERVATION_BYTES + 16 * 1048576
        if storage_reservation > PERSISTED_TASK_BYTES:
            raise ServiceError("body_too_large", 422)
        capacity = {
            "schema_version": "neutral-capacity/1",
            "dataset_bytes": dataset_bytes,
            "observation_bytes": OBSERVATION_BYTES,
            "report_bytes": REPORT_BYTES,
            "snapshot_bytes": SNAPSHOT_BYTES,
            "reserved_report_bytes": report_reservation,
            "reserved_storage_bytes": storage_reservation,
            "max_task_storage_bytes": PERSISTED_TASK_BYTES,
        }
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
            await domain_check(conn)
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
            await conn.execute(
                insert(tasks).values(
                    id=task_id,
                    scope=request.scope,
                    request_key=request.request_key,
                    body=snapshot,
                )
            )
            await self._save(conn, snapshot)
            await domain_check(conn)
            adapter.assert_current(target)
            return snapshot, True

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
        self, conn: AsyncConnection, snapshot: dict[str, Any], *, final: bool = False
    ) -> None:
        snapshot["revision"] += 1
        if final:
            snapshot["report"] = make_neutral_report(snapshot)
            await conn.execute(
                insert(reports).values(
                    task_id=snapshot["id"],
                    body=snapshot["report"],
                )
            )
        neutral_encoded(snapshot, max_bytes=SNAPSHOT_BYTES)
        await conn.execute(update(tasks).where(tasks.c.id == snapshot["id"]).values(body=snapshot))
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
            else:
                await conn.execute(update(trials).where(*where).values(body=item))
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

    async def _mutate(
        self,
        task_id: str,
        change: Callable[[dict[str, Any]], bool],
        verify: Callable[[dict[str, Any]], bool],
    ) -> None:
        async def operation(conn: AsyncConnection) -> bool:
            snapshot = await self._snapshot(conn, task_id)
            if verify(snapshot):
                return True
            final = change(snapshot)
            await self._save(conn, snapshot, final=final)
            if snapshot.get("code") == "execution_unknown":
                await conn.execute(
                    update(meta)
                    .where(meta.c.id == 1)
                    .values(
                        body={
                            "format": FORMAT,
                            "cleanup_unknown": True,
                        }
                    )
                )
                self.healthy = False
            return True

        async def confirm() -> bool | None:
            return True if verify(await self.snapshot(task_id)) else None

        await self._confirmed(operation, confirm)

    async def set_running(self, task_id: str) -> None:
        def change(s: dict[str, Any]) -> bool:
            if s["status"] != "queued":
                raise ServiceError("closed")
            s["status"] = "running"
            return False

        await self._mutate(task_id, change, lambda s: s["status"] == "running")

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
            old = (
                await conn.execute(select(evidence.c.body).where(evidence.c.digest == digest))
            ).scalar_one_or_none()
            if old is None:
                await conn.execute(insert(evidence).values(digest=digest, body=frozen))
            snapshot = await self._snapshot(conn, trial.task_id)
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
            item = self._item(await self.snapshot(trial.task_id), trial)
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
        )

    async def stop(self, task_id: str) -> None:
        def change(s: dict[str, Any]) -> bool:
            s["stop_requested"] = True
            return False

        await self._mutate(task_id, change, lambda s: s["stop_requested"])

    async def finish(self, task_id: str, status: str, code: str) -> None:
        old = await self.snapshot(task_id)
        if old["status"] not in {"queued", "running"}:
            return

        def change(s: dict[str, Any]) -> bool:
            s["status"], s["code"], s["finished_at"] = status, code, time.time()
            for item in s["items"]:
                if item["status"] != "finished":
                    item["status"] = "interrupted"
            return True

        await self._mutate(task_id, change, lambda s: s["status"] == status and s["code"] == code)

    async def report(self, task_id: str) -> dict[str, Any]:
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
