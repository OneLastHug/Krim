"""独立 SQLite：显式写端锁、有限事务、纯只读打开、提交回程只核实。"""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import secrets
import stat
import time
import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, TypeVar, cast

from pydantic import JsonValue
from sqlalchemy import UniqueConstraint, event, func, insert, select, update
from sqlalchemy.engine import URL, RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from krim_agent.contracts import (
    TERMINAL,
    RunEvent,
    RunRequest,
    RunStatus,
    RunView,
    ServiceError,
    check_json,
    encoded,
    fingerprint,
)
from krim_agent.evaluation.types import DatasetRevision, EvalRequest, FreezeRequest
from krim_agent.runtime.contracts import RunSnapshot, settled

from .models import dataset_revisions, eval_items, evaluations, metadata, run_events, runs

APPLICATION_ID = 0x4B4C4331
SCHEMA_VERSION = 2
T = TypeVar("T")


class SQLiteStore:
    def __init__(self, engine: AsyncEngine, fd: int, readonly: bool, max_events: int) -> None:
        self.engine, self.fd, self.readonly = engine, fd, readonly
        self.max_events = max_events
        self.lock = asyncio.Lock()
        self.healthy = True
        self.closed = False
        self.schema_version = SCHEMA_VERSION

    @staticmethod
    async def _validate_schema(conn: AsyncConnection, names: set[str]) -> None:
        for name in names:
            table = metadata.tables[name]
            info = (await conn.exec_driver_sql(f'PRAGMA table_info("{name}")')).all()
            expected = [
                (c.name, str(c.type).upper(), not c.nullable, c.primary_key) for c in table.columns
            ]
            actual = [(row[1], str(row[2]).upper(), bool(row[3]), bool(row[5])) for row in info]
            if actual != expected:
                raise ServiceError("database_invalid")
            uniques = set()
            for row in (await conn.exec_driver_sql(f'PRAGMA index_list("{name}")')).all():
                if row[2]:
                    cols = (
                        await conn.exec_driver_sql(
                            'PRAGMA index_info("' + str(row[1]).replace('"', '""') + '")'
                        )
                    ).all()
                    uniques.add(tuple(c[2] for c in cols))
            required = {
                tuple(c.name for c in constraint.columns)
                for constraint in table.constraints
                if isinstance(constraint, UniqueConstraint)
            }
            required.add(tuple(c.name for c in table.primary_key.columns))
            if uniques != required:
                raise ServiceError("database_invalid")
            foreign = {
                (row[3], row[2], row[4])
                for row in (await conn.exec_driver_sql(f'PRAGMA foreign_key_list("{name}")')).all()
            }
            wanted = {
                (fk.parent.name, fk.column.table.name, fk.column.name) for fk in table.foreign_keys
            }
            if foreign != wanted:
                raise ServiceError("database_invalid")

    @staticmethod
    async def _upgrade(conn: AsyncConnection) -> None:
        await conn.run_sync(
            lambda sync: metadata.create_all(
                sync, tables=[dataset_revisions, evaluations, eval_items]
            )
        )
        await conn.exec_driver_sql(f"PRAGMA user_version={SCHEMA_VERSION}")

    @staticmethod
    def _open_file(path: Path, readonly: bool) -> tuple[Path, int, bool]:
        # 可信本地启动元数据和非阻塞锁在首个 await 前取得，避免线程取消遗留 FD。
        path = path.absolute()
        for parent in reversed(path.parents):
            if parent.is_symlink():
                raise ServiceError("database_invalid")
            if not parent.exists():
                if readonly:
                    raise ServiceError("database_invalid")
                parent.mkdir(mode=0o700)
        flags = (
            (os.O_RDONLY if readonly else os.O_RDWR | os.O_CREAT)
            | os.O_NOFOLLOW
            | os.O_NONBLOCK
            | os.O_CLOEXEC
        )
        try:
            fd = os.open(path, flags, 0o600)
        except OSError:
            raise ServiceError("database_invalid") from None
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            os.close(fd)
            raise ServiceError("database_invalid")
        try:
            if not readonly:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            raise ServiceError("database_busy") from None
        fresh = os.fstat(fd).st_size == 0
        return path, fd, fresh

    @classmethod
    async def open(
        cls, path: Path, *, readonly: bool = False, max_events: int = 2048
    ) -> SQLiteStore:
        path, fd, fresh = cls._open_file(path, readonly)
        url = URL.create(
            "sqlite+aiosqlite",
            database="file:" + str(path),
            query={"mode": "ro" if readonly else "rw", "uri": "true"},
        )
        engine = create_async_engine(url, hide_parameters=True, connect_args={"timeout": 1})

        @event.listens_for(engine.sync_engine, "connect")
        def configure(connection: Any, _record: Any) -> None:
            connection.isolation_level = None
            cursor = connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

        @event.listens_for(engine.sync_engine, "begin")
        def begin(connection: Any) -> None:
            connection.exec_driver_sql("BEGIN")

        store = cls(engine, fd, readonly, max_events)
        try:
            async with engine.begin() as conn:
                application = (await conn.exec_driver_sql("PRAGMA application_id")).scalar_one()
                version = (await conn.exec_driver_sql("PRAGMA user_version")).scalar_one()
                tables = set(
                    (
                        await conn.exec_driver_sql(
                            "SELECT name FROM sqlite_master WHERE type='table'"
                        )
                    ).scalars()
                )
                if fresh and not readonly and application == 0 and version == 0 and not tables:
                    await conn.run_sync(metadata.create_all)
                    await conn.exec_driver_sql(f"PRAGMA application_id={APPLICATION_ID}")
                    await conn.exec_driver_sql(f"PRAGMA user_version={SCHEMA_VERSION}")
                else:
                    expected = {"runs", "run_events"} if version == 1 else set(metadata.tables)
                    if (
                        application != APPLICATION_ID
                        or version not in {1, SCHEMA_VERSION}
                        or tables != expected
                    ):
                        raise ServiceError("database_invalid")
                    await cls._validate_schema(conn, expected)
                    if version == 1:
                        if readonly:
                            store.schema_version = 1
                        else:
                            await cls._upgrade(conn)
            return store
        except BaseException:
            await engine.dispose()
            os.close(fd)
            raise ServiceError("database_invalid") from None

    @staticmethod
    def _view(row: RowMapping) -> RunView:
        return RunView.model_validate({key: row[key] for key in RunView.model_fields})

    async def _write(self, operation: Callable[[AsyncConnection], Awaitable[T]]) -> T:
        if self.readonly or self.closed:
            raise ServiceError("readonly" if self.readonly else "closed")
        try:
            async with self.lock, self.engine.begin() as conn:
                return await operation(conn)
        except ServiceError:
            raise
        except asyncio.CancelledError:
            raise
        except Exception:
            self.healthy = False
            raise ServiceError("storage_unavailable", 503) from None

    async def _row(self, conn: AsyncConnection, run_id: str) -> RowMapping:
        row = (await conn.execute(select(runs).where(runs.c.id == run_id))).mappings().first()
        if row is None:
            raise ServiceError("not_found", 404)
        return row

    async def _event(
        self,
        conn: AsyncConnection,
        run_id: str,
        kind: str,
        data: dict[str, JsonValue],
        event_id: str,
        *,
        terminal: bool = False,
    ) -> RunEvent:
        existing = (
            (
                await conn.execute(
                    select(run_events).where(
                        run_events.c.run_id == run_id, run_events.c.event_id == event_id
                    )
                )
            )
            .mappings()
            .first()
        )
        if existing is not None:
            found = RunEvent.model_validate(dict(existing))
            if found.kind != kind or encoded(found.data) != encoded(data):
                raise ServiceError("key_conflict")
            return found
        row = await self._row(conn, run_id)
        if row["status"] in TERMINAL:
            raise ServiceError("run_closed")
        seq = (
            await conn.execute(
                select(func.count()).select_from(run_events).where(run_events.c.run_id == run_id)
            )
        ).scalar_one()
        if not terminal and seq >= self.max_events - 1:
            raise ServiceError("event_limit")
        record = RunEvent(run_id=run_id, seq=seq, event_id=event_id, kind=kind, data=data)
        try:
            check_json(record.model_dump(mode="json"), max_bytes=65536)
        except ValueError:
            raise ServiceError("output_limit") from None
        await conn.execute(insert(run_events).values(**record.model_dump()))
        return record

    async def begin(
        self, request: RunRequest, target: dict[str, JsonValue]
    ) -> tuple[RunView, bool]:
        if not self.healthy:
            raise ServiceError("storage_unavailable", 503)
        run_id = str(uuid.uuid4())
        digest = fingerprint({"request": request.model_dump(mode="json"), "target": target})

        async def operation(conn: AsyncConnection) -> tuple[RunView, bool]:
            row = (
                (await conn.execute(select(runs).where(runs.c.request_key == request.request_key)))
                .mappings()
                .first()
            )
            if row is not None:
                if row["request_hash"] != digest:
                    raise ServiceError("key_conflict")
                return self._view(row), False
            view = RunView(
                id=run_id,
                request_key=request.request_key,
                status="queued",
                trace_id=secrets.token_hex(16),
                root_span_id=secrets.token_hex(8),
                created_at=time.time(),
            )
            await conn.execute(
                insert(runs).values(
                    **view.model_dump(),
                    request_hash=digest,
                    request=request.model_dump(mode="json"),
                    target=target,
                )
            )
            await self._event(
                conn, run_id, "run_started", {"span_id": view.root_span_id}, "start:" + run_id
            )
            return view, True

        prior_health = self.healthy
        try:
            return await settled(self._write(operation))
        except asyncio.CancelledError:
            raise
        except Exception as failure:
            if isinstance(failure, ServiceError) and failure.code != "storage_unavailable":
                raise
            # 只读核实原请求和本次预分配 ID；不重复事务或模型调用。
            found = await self.by_key(request.request_key)
            if found is not None:
                snap = await self.snapshot(found.id)
                actual = fingerprint(
                    {"request": snap.request.model_dump(mode="json"), "target": snap.target}
                )
                if actual == digest:
                    self.healthy = prior_health
                    return found, found.id == run_id
            self.healthy = False
            raise ServiceError("storage_unavailable", 503) from None

    async def append(
        self, run_id: str, kind: str, data: dict[str, JsonValue], *, event_id: str | None = None
    ) -> RunEvent:
        event_id = event_id or uuid.uuid4().hex
        # 先固定本次意图，禁止调用方在等待事务时改变参数。
        frozen = cast(dict[str, JsonValue], json.loads(encoded(data)))

        async def operation(conn: AsyncConnection) -> RunEvent:
            return await self._event(conn, run_id, kind, frozen, event_id)

        prior_health = self.healthy
        try:
            return await settled(self._write(operation))
        except asyncio.CancelledError:
            raise
        except Exception as failure:
            if isinstance(failure, ServiceError) and failure.code != "storage_unavailable":
                raise
            # COMMIT 回程丢失只核实原 event_id/原内容；不会再次调用 executor。
            try:
                snap = await self.snapshot(run_id)
                found = next((e for e in snap.events if e.event_id == event_id), None)
                if (
                    found is not None
                    and found.kind == kind
                    and encoded(found.data) == encoded(frozen)
                ):
                    self.healthy = prior_health
                    return found
            except Exception:
                pass
            self.healthy = False
            raise ServiceError("storage_unavailable", 503) from None

    async def finish(self, run_id: str, status: RunStatus, code: str | None = None) -> RunView:
        async def operation(conn: AsyncConnection) -> RunView:
            row = await self._row(conn, run_id)
            if row["status"] in TERMINAL:
                return self._view(row)
            await self._event(
                conn,
                run_id,
                "terminal",
                {"status": status, "code": code},
                "terminal:" + run_id,
                terminal=True,
            )
            await conn.execute(
                update(runs)
                .where(runs.c.id == run_id)
                .values(status=status, code=code, finished_at=time.time())
            )
            return self._view(await self._row(conn, run_id))

        prior_health = self.healthy
        try:
            return await settled(self._write(operation))
        except asyncio.CancelledError:
            raise
        except Exception as failure:
            if isinstance(failure, ServiceError) and failure.code != "storage_unavailable":
                raise
            snap = await self.snapshot(run_id)
            expected = {"status": status, "code": code}
            confirmed = any(
                e.event_id == "terminal:" + run_id and e.kind == "terminal" and e.data == expected
                for e in snap.events
            )
            if confirmed and snap.run.status == status and snap.run.code == code:
                self.healthy = prior_health
                return snap.run
            self.healthy = False
            raise ServiceError("storage_unavailable", 503) from None

    async def set_running(self, run_id: str) -> None:
        async def operation(conn: AsyncConnection) -> None:
            row = await self._row(conn, run_id)
            if row["status"] != "queued":
                raise ServiceError("run_closed")
            await conn.execute(update(runs).where(runs.c.id == run_id).values(status="running"))

        await settled(self._write(operation))

    async def request_stop(self, run_id: str) -> None:
        async def operation(conn: AsyncConnection) -> None:
            await self._row(conn, run_id)
            await conn.execute(
                update(runs)
                .where(runs.c.id == run_id, runs.c.status.not_in(TERMINAL))
                .values(stop_requested=True)
            )

        await settled(self._write(operation))

    async def snapshot(self, run_id: str) -> RunSnapshot:
        if self.closed:
            raise ServiceError("closed")

        async def operation() -> RunSnapshot:
            async with self.lock, self.engine.begin() as conn:
                return await self._snapshot(conn, run_id)

        try:
            # 读者取消只终止等待意图；完整读事务归还连接、释放锁后才交付取消。
            return await settled(operation())
        except ServiceError:
            raise
        except Exception:
            raise ServiceError("storage_unavailable", 503) from None

    async def _snapshot(self, conn: AsyncConnection, run_id: str) -> RunSnapshot:
        row = await self._row(conn, run_id)
        records = (
            (
                await conn.execute(
                    select(run_events)
                    .where(run_events.c.run_id == run_id)
                    .order_by(run_events.c.seq)
                )
            )
            .mappings()
            .all()
        )
        return RunSnapshot(
            self._view(row),
            RunRequest.model_validate(row["request"]),
            cast(dict[str, JsonValue], row["target"]),
            tuple(RunEvent.model_validate(dict(e)) for e in records),
        )

    async def by_key(self, key: str) -> RunView | None:
        if self.closed:
            raise ServiceError("closed")
        async with self.lock, self.engine.begin() as conn:
            row = (
                (await conn.execute(select(runs).where(runs.c.request_key == key)))
                .mappings()
                .first()
            )
            return self._view(row) if row is not None else None

    def _require_v2(self) -> None:
        if self.schema_version != 2:
            raise ServiceError("schema_upgrade_required")

    async def _eval_write(
        self,
        operation: Callable[[AsyncConnection], Awaitable[T]],
        confirm: Callable[[], Awaitable[T | None]],
    ) -> T:
        previous = self.healthy
        try:
            return await settled(self._write(operation))
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if isinstance(error, ServiceError) and error.code != "storage_unavailable":
                raise
            try:
                found = await confirm()
                if found is not None:
                    self.healthy = previous
                    return found
            except Exception:
                pass
            self.healthy = False
            raise ServiceError("storage_unavailable", 503) from None

    async def _dataset(self, conn: AsyncConnection, name: str, version: str) -> DatasetRevision:
        self._require_v2()
        row = (
            (
                await conn.execute(
                    select(dataset_revisions).where(
                        dataset_revisions.c.name == name, dataset_revisions.c.version == version
                    )
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise ServiceError("not_found", 404)
        try:
            result = DatasetRevision.model_validate(row["body"])
            if (result.name, result.version, result.fingerprint) != (
                name,
                version,
                row["fingerprint"],
            ):
                raise ValueError("修订身份损坏")
            return result
        except ValueError:
            raise ServiceError("facts_corrupt") from None

    async def dataset(self, name: str, version: str) -> DatasetRevision:
        if self.closed:
            raise ServiceError("closed")
        async with self.lock, self.engine.begin() as conn:
            return await self._dataset(conn, name, version)

    async def freeze_dataset(self, request: FreezeRequest) -> DatasetRevision:
        self._require_v2()
        if not self.healthy:
            raise ServiceError("storage_unavailable", 503)
        frozen = FreezeRequest.model_validate(json.loads(encoded(request.model_dump(mode="json"))))

        async def operation(conn: AsyncConnection) -> DatasetRevision:
            try:
                old = await self._dataset(conn, frozen.name, frozen.version)
            except ServiceError as error:
                if error.code != "not_found":
                    raise
            else:
                if fingerprint(old.selection().model_dump(mode="json")) != fingerprint(
                    frozen.model_dump(mode="json")
                ):
                    raise ServiceError("revision_conflict")
                return old
            from krim_agent.evaluation.trace_cases import freeze_revision
            from krim_agent.evaluation.types import RunSelection

            snapshots = {
                c.source_run_id: await self._snapshot(conn, c.source_run_id)
                for c in frozen.cases
                if isinstance(c, RunSelection)
            }
            revision = freeze_revision(frozen, snapshots)
            await conn.execute(
                insert(dataset_revisions).values(
                    name=revision.name,
                    version=revision.version,
                    fingerprint=revision.fingerprint,
                    body=revision.model_dump(mode="json"),
                    created_at=revision.created_at,
                )
            )
            return revision

        async def confirm() -> DatasetRevision | None:
            result = await self.dataset(frozen.name, frozen.version)
            return (
                result
                if fingerprint(result.selection().model_dump(mode="json"))
                == fingerprint(frozen.model_dump(mode="json"))
                else None
            )

        return await self._eval_write(operation, confirm)

    async def _eval_snapshot(self, conn: AsyncConnection, eval_id: str) -> dict[str, Any]:
        self._require_v2()
        row = (
            (await conn.execute(select(evaluations).where(evaluations.c.id == eval_id)))
            .mappings()
            .first()
        )
        if row is None:
            raise ServiceError("not_found", 404)
        items = (
            (
                await conn.execute(
                    select(eval_items)
                    .where(eval_items.c.eval_id == eval_id)
                    .order_by(eval_items.c.case_index, eval_items.c.repetition)
                )
            )
            .mappings()
            .all()
        )
        dataset = await self._dataset(conn, row["dataset_name"], row["dataset_version"])
        if dataset.fingerprint != row["dataset_fingerprint"] or len(items) != row["planned_count"]:
            raise ServiceError("facts_corrupt")
        return dict(row) | {
            "items": [dict(v) for v in items],
            "dataset": dataset.model_dump(mode="json"),
        }

    async def eval_snapshot(self, eval_id: str, revision: int | None = None) -> dict[str, Any]:
        if self.closed:
            raise ServiceError("closed")
        async with self.lock, self.engine.begin() as conn:
            snapshot = await self._eval_snapshot(conn, eval_id)
            if revision is not None and revision != snapshot["revision"]:
                raise ServiceError("snapshot_changed")
            return snapshot

    async def eval_by_key(self, key: str) -> dict[str, Any] | None:
        if self.closed:
            raise ServiceError("closed")
        self._require_v2()
        async with self.lock, self.engine.begin() as conn:
            found = (
                await conn.execute(select(evaluations.c.id).where(evaluations.c.request_key == key))
            ).scalar_one_or_none()
            return await self._eval_snapshot(conn, found) if found is not None else None

    async def begin_eval(
        self, request: EvalRequest, target: dict[str, JsonValue], execution: dict[str, JsonValue]
    ) -> tuple[dict[str, Any], bool]:
        self._require_v2()
        if not self.healthy:
            raise ServiceError("storage_unavailable", 503)
        frozen = EvalRequest.model_validate(json.loads(encoded(request.model_dump(mode="json"))))
        eval_id = str(uuid.uuid4())
        digest = fingerprint(frozen.model_dump(mode="json"))
        target = json.loads(encoded(target))
        execution = json.loads(encoded(execution))

        async def operation(conn: AsyncConnection) -> tuple[dict[str, Any], bool]:
            old = (
                (
                    await conn.execute(
                        select(evaluations).where(evaluations.c.request_key == frozen.request_key)
                    )
                )
                .mappings()
                .first()
            )
            if old is not None:
                if old["request_hash"] != digest:
                    raise ServiceError("key_conflict")
                return await self._eval_snapshot(conn, old["id"]), False
            dataset = await self._dataset(conn, frozen.dataset.name, frozen.dataset.version)
            if dataset.fingerprint != frozen.dataset.fingerprint:
                raise ServiceError("invalid_input", 422)
            now = time.time()
            await conn.execute(
                insert(evaluations).values(
                    id=eval_id,
                    request_key=frozen.request_key,
                    request_hash=digest,
                    request_json=frozen.model_dump(mode="json"),
                    dataset_name=dataset.name,
                    dataset_version=dataset.version,
                    dataset_fingerprint=dataset.fingerprint,
                    target=target,
                    rules=[r.model_dump(mode="json") for r in frozen.rules],
                    execution=execution,
                    status="queued",
                    code=None,
                    created_at=now,
                    finished_at=None,
                    stop_requested=False,
                    revision=1,
                    planned_count=len(dataset.cases) * frozen.execution.repetitions,
                    report=None,
                    report_hash=None,
                )
            )
            for index, case in enumerate(dataset.cases):
                for repetition in range(frozen.execution.repetitions):
                    await conn.execute(
                        insert(eval_items).values(
                            eval_id=eval_id,
                            case_id=case.case_id,
                            repetition=repetition,
                            case_index=index,
                            run_request_key=f"lc-eval.{eval_id}.{index}.{repetition}",
                            run_id=None,
                            status="pending",
                            code=None,
                            created_at=now,
                            finished_at=None,
                            output=None,
                            scores=None,
                            quality=None,
                            content_hash=None,
                        )
                    )
            return await self._eval_snapshot(conn, eval_id), True

        async def confirm() -> tuple[dict[str, Any], bool] | None:
            found = await self.eval_by_key(frozen.request_key)
            if found is not None and found["request_hash"] == digest:
                return found, found["id"] == eval_id
            return None

        return await self._eval_write(operation, confirm)

    async def child_request(
        self, eval_id: str, case_id: str, repetition: int
    ) -> tuple[RunRequest, str]:
        async with self.lock, self.engine.begin() as conn:
            snapshot = await self._eval_snapshot(conn, eval_id)
            if snapshot["status"] in TERMINAL or snapshot["stop_requested"]:
                raise ServiceError("closed")
            item = next(
                (
                    i
                    for i in snapshot["items"]
                    if i["case_id"] == case_id and i["repetition"] == repetition
                ),
                None,
            )
            if item is None:
                raise ServiceError("invalid_input", 422)
            case = next(c for c in snapshot["dataset"]["cases"] if c["case_id"] == case_id)
            request = RunRequest(
                text=case["input"],
                request_key=item["run_request_key"],
                target=snapshot["request_json"]["target_key"],
            )
            return request, fingerprint(snapshot["target"])

    async def attach_child(self, eval_id: str, case_id: str, repetition: int, run_id: str) -> None:
        async def operation(conn: AsyncConnection) -> bool:
            snapshot = await self._eval_snapshot(conn, eval_id)
            item = next(
                i
                for i in snapshot["items"]
                if i["case_id"] == case_id and i["repetition"] == repetition
            )
            run = await self._snapshot(conn, run_id)
            case = next(c for c in snapshot["dataset"]["cases"] if c["case_id"] == case_id)
            if (
                run.request.request_key != item["run_request_key"]
                or run.request.text != case["input"]
                or run.request.target != snapshot["request_json"]["target_key"]
                or fingerprint(run.target) != fingerprint(snapshot["target"])
                or item["run_id"] not in {None, run_id}
            ):
                raise ServiceError("key_conflict")
            if item["run_id"] is None:
                await conn.execute(
                    update(eval_items)
                    .where(
                        eval_items.c.eval_id == eval_id,
                        eval_items.c.case_id == case_id,
                        eval_items.c.repetition == repetition,
                    )
                    .values(run_id=run_id, status="running")
                )
                await conn.execute(
                    update(evaluations)
                    .where(evaluations.c.id == eval_id)
                    .values(revision=evaluations.c.revision + 1)
                )
            return True

        async def confirm() -> bool | None:
            snapshot = await self.eval_snapshot(eval_id)
            item = next(
                i
                for i in snapshot["items"]
                if i["case_id"] == case_id and i["repetition"] == repetition
            )
            run = await self.snapshot(run_id)
            return (
                True
                if item["run_id"] == run_id
                and run.request.request_key == item["run_request_key"]
                and fingerprint(run.target) == fingerprint(snapshot["target"])
                else None
            )

        await self._eval_write(operation, confirm)

    async def set_eval_running(self, eval_id: str) -> None:
        async def operation(conn: AsyncConnection) -> bool:
            snapshot = await self._eval_snapshot(conn, eval_id)
            if snapshot["status"] != "queued":
                raise ServiceError("closed")
            await conn.execute(
                update(evaluations)
                .where(evaluations.c.id == eval_id)
                .values(status="running", revision=evaluations.c.revision + 1)
            )
            return True

        async def confirm() -> bool | None:
            return True if (await self.eval_snapshot(eval_id))["status"] == "running" else None

        await self._eval_write(operation, confirm)

    async def stop_eval(self, eval_id: str) -> None:
        async def operation(conn: AsyncConnection) -> bool:
            snapshot = await self._eval_snapshot(conn, eval_id)
            if snapshot["status"] not in TERMINAL:
                await conn.execute(
                    update(evaluations)
                    .where(evaluations.c.id == eval_id)
                    .values(stop_requested=True, revision=evaluations.c.revision + 1)
                )
            return True

        async def confirm() -> bool | None:
            snapshot = await self.eval_snapshot(eval_id)
            return True if snapshot["stop_requested"] or snapshot["status"] in TERMINAL else None

        await self._eval_write(operation, confirm)

    async def _finalize_eval(
        self, conn: AsyncConnection, eval_id: str, status: str, code: str | None
    ) -> None:
        from krim_agent.evaluation.regression import make_report

        await conn.execute(
            update(evaluations)
            .where(evaluations.c.id == eval_id)
            .values(
                status=status,
                code=code,
                finished_at=time.time(),
                revision=evaluations.c.revision + 1,
            )
        )
        snapshot = await self._eval_snapshot(conn, eval_id)
        report = make_report(snapshot)
        await conn.execute(
            update(evaluations)
            .where(evaluations.c.id == eval_id)
            .values(report=report, report_hash=report["report_hash"])
        )

    async def commit_item(
        self, eval_id: str, case_id: str, repetition: int, body: dict[str, JsonValue]
    ) -> None:
        check_json(body, max_bytes=20480)
        body = json.loads(encoded(body))
        digest = fingerprint(body)

        async def operation(conn: AsyncConnection) -> bool:
            snapshot = await self._eval_snapshot(conn, eval_id)
            item = next(
                i
                for i in snapshot["items"]
                if i["case_id"] == case_id and i["repetition"] == repetition
            )
            if item["content_hash"] is not None:
                if item["content_hash"] != digest:
                    raise ServiceError("key_conflict")
                return True
            if snapshot["status"] in TERMINAL:
                raise ServiceError("closed")
            await conn.execute(
                update(eval_items)
                .where(
                    eval_items.c.eval_id == eval_id,
                    eval_items.c.case_id == case_id,
                    eval_items.c.repetition == repetition,
                )
                .values(status="finished", finished_at=time.time(), content_hash=digest, **body)
            )
            current = await self._eval_snapshot(conn, eval_id)
            if all(i["status"] == "finished" for i in current["items"]):
                await self._finalize_eval(conn, eval_id, "completed", None)
            else:
                await conn.execute(
                    update(evaluations)
                    .where(evaluations.c.id == eval_id)
                    .values(revision=evaluations.c.revision + 1)
                )
            return True

        async def confirm() -> bool | None:
            snapshot = await self.eval_snapshot(eval_id)
            item = next(
                i
                for i in snapshot["items"]
                if i["case_id"] == case_id and i["repetition"] == repetition
            )
            return True if item["content_hash"] == digest else None

        await self._eval_write(operation, confirm)

    async def finish_eval(self, eval_id: str, status: str, code: str | None = None) -> None:
        async def operation(conn: AsyncConnection) -> bool:
            snapshot = await self._eval_snapshot(conn, eval_id)
            if snapshot["status"] in TERMINAL:
                return True
            await conn.execute(
                update(eval_items)
                .where(eval_items.c.eval_id == eval_id, eval_items.c.status != "finished")
                .values(status="interrupted")
            )
            await self._finalize_eval(conn, eval_id, status, code)
            return True

        async def confirm() -> bool | None:
            snapshot = await self.eval_snapshot(eval_id)
            return (
                True
                if snapshot["status"] == status
                and snapshot["code"] == code
                and snapshot["report"] is not None
                else None
            )

        await self._eval_write(operation, confirm)

    async def recover(self) -> None:
        async def operation(conn: AsyncConnection) -> None:
            ids = list(
                (
                    await conn.execute(select(runs.c.id).where(runs.c.status.not_in(TERMINAL)))
                ).scalars()
            )
            for run_id in ids:
                await self._event(
                    conn,
                    run_id,
                    "terminal",
                    {"status": "interrupted", "code": "interrupted"},
                    "terminal:" + run_id,
                    terminal=True,
                )
                await conn.execute(
                    update(runs)
                    .where(runs.c.id == run_id)
                    .values(status="interrupted", code="interrupted", finished_at=time.time())
                )
            active = (
                (
                    await conn.execute(
                        select(evaluations.c.id).where(evaluations.c.status.not_in(TERMINAL))
                    )
                )
                .scalars()
                .all()
            )
            for eval_id in active:
                snapshot = await self._eval_snapshot(conn, eval_id)
                for item in snapshot["items"]:
                    if item["run_id"] is None:
                        found = (
                            await conn.execute(
                                select(runs.c.id).where(
                                    runs.c.request_key == item["run_request_key"]
                                )
                            )
                        ).scalar_one_or_none()
                        if found is not None:
                            run = await self._snapshot(conn, found)
                            case = next(
                                c
                                for c in snapshot["dataset"]["cases"]
                                if c["case_id"] == item["case_id"]
                            )
                            if run.request.text != case["input"] or fingerprint(
                                run.target
                            ) != fingerprint(snapshot["target"]):
                                raise ServiceError("facts_corrupt")
                            await conn.execute(
                                update(eval_items)
                                .where(
                                    eval_items.c.eval_id == eval_id,
                                    eval_items.c.case_id == item["case_id"],
                                    eval_items.c.repetition == item["repetition"],
                                )
                                .values(run_id=found)
                            )
                await conn.execute(
                    update(eval_items)
                    .where(eval_items.c.eval_id == eval_id, eval_items.c.status != "finished")
                    .values(status="interrupted")
                )
                await self._finalize_eval(conn, eval_id, "interrupted", "interrupted")

        await settled(self._write(operation))

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            await self.engine.dispose()
        finally:
            os.close(self.fd)
