"""真实旧生产链种库、SQLite迁移和提交丢回程；不读取旧故障账本。"""

import asyncio
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest
from sqlalchemy import event, select, update
from sqlalchemy.engine import Engine
from test_continuous_eval import DATABASES, confirmed_task, seed
from test_continuous_eval import continuous_resources as continuous_resources

from krim_agent.contracts import ServiceError
from krim_agent.evaluation.continuous_types import ConfirmationRequest, ImportRequest
from krim_agent.evaluation.repository import Repository, meta, source_observations, tasks

TEST_DIGEST = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
SOURCE_ROOT = Path(__file__).resolve().parents[2]


LEGACY_CODE = r"""import asyncio,json,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from krim_agent.evaluation.app import open_eval
from krim_agent.evaluation.domain import DatasetSpec,TaskSpec
from importlib.resources import files
async def main():
    path=Path(sys.argv[2])
    app=await open_eval(path)
    body=json.loads(files('krim_agent.evaluation.examples').joinpath('reconciliation.v1.json').read_text())
    spec=DatasetSpec.model_validate(body|{'cases':body['cases'][:1]})
    try:
        await app.repository.freeze(spec)
        req=TaskSpec(request_key='actual-v1',
            dataset=spec.ref(),
            metrics=app.repository.metric_registry.specs())
        view=await app.run(req)
        result=await app.kernel.wait(view['id'])
        report=await app.repository.report(view['id'])
        assert result['status']=='completed' and report['items'][0]['quality']=='passed'
        print(json.dumps({'task_id':view['id'],
            'request_hash':result['request_hash'],
            'report_hash':report['report_hash'],
            'source':__import__('krim_agent.evaluation.repository',
            fromlist=['x']).__file__,
            'exec':app.adapter.execute_calls}))
    finally:
        await app.close()
    assert app.repository.closed and all(t.done() for t in app.kernel.tasks.values())
    assert (all((j.process.returncode is not None
        and j.pidfd == -1
        and j.collector.done()
        for j in app.owner.jobs.values())))
asyncio.run(main())
"""


def legacy_database(tmp_path):
    root = Path(os.environ["KRIM_PN3_ROOT"])
    source = root / "materials/product/pn2-c39ff/source/src"
    database = tmp_path / "v1.sqlite"
    DATABASES.append(database)
    compile(LEGACY_CODE, "fixed-c39ff-seed", "exec")
    argv = [sys.executable, "-I", "-B", "-c", LEGACY_CODE, str(source), str(database)]
    env = {
        k: os.environ[k]
        for k in ["PATH", "LANG", "LD_LIBRARY_PATH", "TMPDIR", "HOME", "PYTHONDONTWRITEBYTECODE"]
    }
    with subprocess.Popen(
        argv, cwd=tmp_path, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ) as child:
        pidfd = os.pidfd_open(child.pid)
        try:
            stdout, stderr = child.communicate(timeout=30)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()
            os.close(pidfd)
        receipt = {
            "argv": argv,
            "cwd": str(tmp_path),
            "env": env,
            "pid": child.pid,
            "rc": child.returncode,
            "waited": child.poll() is not None,
            "pidfd_closed": True,
            "code_hash": hashlib.sha256(LEGACY_CODE.encode()).hexdigest(),
        }
    (tmp_path / "v1-command.json").write_text(json.dumps(receipt, indent=2))
    (tmp_path / "v1-stdout.txt").write_bytes(stdout)
    (tmp_path / "v1-stderr.txt").write_bytes(stderr)
    assert receipt["rc"] == 0, stderr.decode()
    result = json.loads(stdout)
    assert Path(result["source"]).is_relative_to(source)
    assert result["exec"] == 1
    with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 1
    return database, result


def logical_database(path):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as conn:
        names = sorted(
            r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        )
        return {
            name: conn.execute(f'SELECT * FROM "{name}" ORDER BY 1').fetchall() for name in names
        }


@pytest.mark.parametrize(
    "kind", ["v1_seed", "readonly", "rollback", "confirmation_lost", "task_lost", "scoring_crash"]
)
async def test_continuous_migration_and_atomic_keys(tmp_path, monkeypatch, kind):
    if kind in {"v1_seed", "readonly", "rollback"}:
        database, old = legacy_database(tmp_path)
        before = logical_database(database)
        binary = database.read_bytes()
        statements = []

        def observe(conn, cursor, statement, parameters, context, executemany):
            statements.append(statement)
            if kind == "rollback" and statement.lstrip().startswith(
                "CREATE TABLE case_confirmations"
            ):
                raise RuntimeError("controlled_mid_ddl_failure")

        event.listen(Engine, "before_cursor_execute", observe)
        try:
            if kind == "rollback":
                with pytest.raises(RuntimeError, match="controlled_mid_ddl_failure"):
                    await Repository.open(database)
            else:
                repo = await Repository.open(database, readonly=kind == "readonly")
                try:
                    snapshot = await repo.snapshot(old["task_id"])
                    assert snapshot["request_hash"] == old["request_hash"]
                    assert (await repo.report(old["task_id"]))["report_hash"] == old["report_hash"]
                finally:
                    await repo.close()
        finally:
            event.remove(Engine, "before_cursor_execute", observe)
        after = logical_database(database)
        for table, rows in before.items():
            if table != "eval_meta":
                assert after[table] == rows
        if kind in {"readonly", "rollback"}:
            assert after == before
            assert database.read_bytes() == binary
        if kind == "readonly":
            assert not any("CREATE TABLE" in s or s.startswith("UPDATE") for s in statements)
        else:
            begin = next(i for i, s in enumerate(statements) if s == "BEGIN IMMEDIATE")
            ddl = next(i for i, s in enumerate(statements) if s.lstrip().startswith("CREATE TABLE"))
            assert begin < ddl
            assert "case_candidates" in statements[ddl]
        if kind == "v1_seed":
            assert len(after) == 14
            marker = json.loads(after["eval_meta"][0][1])
            assert marker["repository_id"] and marker["extension"] == "continuous-eval/1"
            repo = await Repository.open(database)
            try:
                again = logical_database(database)
                assert again["eval_meta"] == after["eval_meta"]
            finally:
                await repo.close()
        else:
            writer = await Repository.open(database)
            await writer.close()
        (tmp_path / "migration-statements.json").write_text(json.dumps(statements, indent=2))
        return
    if kind == "scoring_crash":
        from test_continuous_eval import scoring_crash

        await scoring_crash(tmp_path)
        return
    app, dataset, selection, pipeline = await seed(tmp_path)
    try:
        rows = await pipeline.import_sources(
            ImportRequest(request_key="lost-source", sources=[selection])
        )
        draft = await app.repository.candidate("local", rows[0]["id"])
        original = app.repository._write
        lost = []

        async def committed_then_lost(operation):
            value = await original(operation)
            if not lost:
                lost.append(True)
                raise OSError("actual_commit_return_lost")
            return value

        monkeypatch.setattr(app.repository, "_write", committed_then_lost)
        if kind == "confirmation_lost":
            request = ConfirmationRequest(
                request_key="lost-confirm",
                candidate_id=draft["candidate_id"],
                expected_revision=draft["revision"],
                expected_hash=draft["hash"],
                case=dataset.cases[0],
                context="complete",
                reason="人工固定结果与初态",
            )
            result = await pipeline.confirm_case(request)
            assert result["status"] == "confirmed"
            assert (await pipeline.confirm_case(request))["id"] == result["id"]
            assert (await app.repository.candidate("local", draft["candidate_id"]))["revision"] == 1
            assert app.adapter.execute_calls == 1
        else:
            from krim_agent.evaluation.domain import TaskSpec

            request = TaskSpec(
                request_key="lost-task",
                dataset=dataset.ref(),
                metrics=app.repository.metric_registry.specs(),
            )
            result = await app.run(request)
            final = await app.kernel.wait(result["id"])
            assert final["status"] == "completed"
            assert (await app.run(request))["id"] == result["id"]
            assert app.adapter.execute_calls == 2
        assert len(lost) == 1
    finally:
        await app.close()


@pytest.mark.parametrize(
    "kind", ["source_drift", "metric_drift", "commit_cancel", "unknown_cleanup"]
)
async def test_continuous_sql_fence_and_writer_release(tmp_path, monkeypatch, kind):
    if kind == "source_drift":
        app, dataset, selection, pipeline = await seed(tmp_path)
        original_reader = pipeline.sources["current-eval"]
        changed = []

        def drift(conn, cursor, statement, parameters, context, executemany):
            if (
                not changed
                and "SELECT eval_tasks.body" in statement
                and selection.task_id in parameters
            ):
                from krim_agent.evaluation.sources import EvalSourceReader

                pipeline.sources["current-eval"] = EvalSourceReader(app.repository)
                changed.append(True)

        event.listen(app.repository.engine.sync_engine, "before_cursor_execute", drift)
        try:
            with pytest.raises(ServiceError, match="target_binding_changed"):
                await pipeline.import_sources(
                    ImportRequest(request_key="source-drift", sources=[selection])
                )
            assert changed and app.adapter.execute_calls == 1
            async with app.repository.engine.begin() as conn:
                assert not (await conn.execute(select(source_observations.c.body))).all()
        finally:
            event.remove(app.repository.engine.sync_engine, "before_cursor_execute", drift)
            pipeline.sources["current-eval"] = original_reader
            await app.close()
        return
    app, pipeline, request, source = await confirmed_task(tmp_path)
    release, entered, joined = asyncio.Event(), asyncio.Event(), asyncio.Event()
    try:
        if kind == "metric_drift":
            from krim_agent.evaluation.domain import MetricSpec
            from krim_agent.evaluation.metrics import RegisteredMetric

            replacement_calls = []

            async def paused(case, output, config):
                entered.set()
                try:
                    await release.wait()
                    return "scored", 1, None
                finally:
                    joined.set()

            async def refused(case, output, config):
                replacement_calls.append(True)
                raise AssertionError("replacement scorer must not enter")

            spec = MetricSpec(name="bounded_drift", version="1", implementation_digest=TEST_DIGEST)
            app.repository.metric_registry.register(spec, paused)
            request = request.model_copy(update={"metrics": request.metrics + [spec]})
            view = await pipeline.submit(request)
            await asyncio.wait_for(entered.wait(), 2)
            app.repository.metric_registry._registered[spec.name] = RegisteredMetric(spec, refused)
            release.set()
            result = await app.kernel.wait(view["id"])
            assert result["status"] == "failed" and result["code"] == "target_binding_changed"
            assert joined.is_set() and replacement_calls == []
            assert app.adapter.execute_calls == 1
        elif kind == "commit_cancel":
            original = app.repository._write

            async def write(operation):
                async def held(conn):
                    value = await operation(conn)
                    row = (
                        await conn.execute(
                            select(tasks.c.body).where(tasks.c.request_key == request.request_key)
                        )
                    ).scalar_one_or_none()
                    if row is not None and row["status"] == "completed" and not entered.is_set():
                        entered.set()
                        await release.wait()
                    return value

                return await original(held)

            monkeypatch.setattr(app.repository, "_write", write)
            view = await pipeline.submit(request)
            await asyncio.wait_for(entered.wait(), 2)
            producer = app.kernel.tasks[view["id"]]
            producer.cancel()
            producer.cancel()
            release.set()
            await asyncio.gather(producer, return_exceptions=True)
            result = await app.repository.snapshot(view["id"])
            assert result["status"] == "completed" and result["items"][0]["quality"] == "passed"
            assert (await pipeline.submit(request))["id"] == view["id"]
            assert app.adapter.execute_calls == 1
        else:
            from test_neutral_eval import pn1_repair_worker

            from krim_agent.evaluation.domain import Budget, clone

            original_read = asyncio.StreamReader.read

            async def held_read(stream, size=-1):
                value = await original_read(stream, size)
                if any(
                    stream is work.process.stdout
                    and work.prepared.spec.task_id != source["source"]["task_id"]
                    for work in app.owner.jobs.values()
                ):
                    entered.set()
                    await release.wait()
                return value

            monkeypatch.setattr(asyncio.StreamReader, "read", held_read)
            pn1_repair_worker(app, tmp_path, live=True)
            async with app.repository.engine.begin() as conn:
                marker = (await conn.execute(select(meta.c.body))).scalar_one()
            marker = clone(marker) | {
                "native_campaign": {"opaque_fixture_marker": "preserve-only-not-authority"}
            }

            async def mark(conn):
                await conn.execute(update(meta).values(body=marker))

            await app.repository._write(mark)
            request = request.model_copy(
                update={
                    "mode": "execute",
                    "observations": [],
                    "execution": Budget(item_timeout_seconds=1),
                    "request_key": "actual-unknown",
                }
            )
            view = await pipeline.submit(request)
            await asyncio.wait_for(entered.wait(), 3)
            result = await app.kernel.wait(view["id"])
            assert result["status"] == "failed" and result["code"] == "execution_unknown"
            assert result["items"][0]["output"] is None
            assert result["items"][0]["execution_ref"] is not None
            held_job = next(
                w for w in app.owner.jobs.values() if w.prepared.spec.task_id == view["id"]
            )
            assert held_job.cleanup_receipts[-1]["closed"] is False
            assert held_job.cleanup_receipts[-1]["reason"] == "cleanup_unknown"
            assert held_job.process.returncode is not None and held_job.pidfd >= 0
            assert app.kernel.cleanup_unknown and not app.repository.healthy
            async with app.repository.engine.begin() as conn:
                current = (await conn.execute(select(meta.c.body))).scalar_one()
            assert current == marker | {"cleanup_unknown": True}
            assert (await pipeline.submit(request))["id"] == view["id"]
            with pytest.raises(ServiceError, match="storage_unavailable"):
                await pipeline.submit(request.model_copy(update={"request_key": "must-not-replay"}))
            assert app.adapter.execute_calls == 2
            (tmp_path / "unknown-before-release.json").write_text(
                json.dumps(
                    {
                        "task_id": view["id"],
                        "marker": current,
                        "jobs": [
                            {
                                "pid": w.process.pid,
                                "wait_returncode": w.process.returncode,
                                "pidfd": w.pidfd,
                                "collector_done": w.collector.done(),
                            }
                            for w in app.owner.jobs.values()
                        ],
                    },
                    indent=2,
                )
            )
            release.set()
    finally:
        release.set()
        await app.close()
    writer = await Repository.open(app.repository.path)
    await writer.close()


async def test_pn3_repair_submit_host_fence(tmp_path, monkeypatch):
    from sqlalchemy import event

    app, pipeline, request, source = await confirmed_task(tmp_path)
    original_actor, original_scope = pipeline.actor, pipeline.scope
    entered = asyncio.Event()
    caller = None
    before = app.adapter.execute_calls
    existing_tasks = set(app.kernel.tasks)
    request = request.model_copy(
        update={"mode": "execute", "observations": [], "request_key": "repair-lock-fence"}
    )
    original_key = app.repository.by_key

    async def by_key(value):
        result = await original_key(value)
        if value.get("request_key") == request.request_key:
            entered.set()
        return result

    try:
        monkeypatch.setattr(app.repository, "by_key", by_key)
        await app.kernel._admission.acquire()
        caller = asyncio.create_task(pipeline.submit(request))
        await asyncio.wait_for(entered.wait(), 2)
        await asyncio.sleep(0)
        pipeline.actor = "revoked-during-admission"
        app.kernel._admission.release()
        with pytest.raises(ServiceError, match="target_binding_changed"):
            await caller
        pipeline.actor = original_actor
        old = await original_key(request.model_dump(mode="json"))
        if old is not None and old["id"] in app.kernel.tasks:
            await app.kernel.wait(old["id"])
        assert old is None and app.adapter.execute_calls == before
        assert set(app.kernel.tasks) == existing_tasks
        monkeypatch.setattr(app.repository, "by_key", original_key)
        request = request.model_copy(update={"request_key": "repair-sql-fence"})
        changed = []

        def drift(conn, cursor, statement, parameters, context, many):
            if not changed and statement.startswith("INSERT INTO eval_tasks"):
                pipeline.scope = "revoked-at-real-insert"
                changed.append(statement)

        event.listen(app.repository.engine.sync_engine, "after_cursor_execute", drift)
        try:
            with pytest.raises(ServiceError, match="target_binding_changed"):
                await pipeline.submit(request)
        finally:
            event.remove(app.repository.engine.sync_engine, "after_cursor_execute", drift)
            pipeline.scope = original_scope
        assert changed
        assert await original_key(request.model_dump(mode="json")) is None
        assert app.adapter.execute_calls == before and set(app.kernel.tasks) == existing_tasks
        (tmp_path / "repair-admission-fence.json").write_text(
            json.dumps(
                {
                    "before_exec": before,
                    "after_exec": app.adapter.execute_calls,
                    "actual_insert_barrier": changed,
                },
                indent=2,
            )
        )
    finally:
        pipeline.actor, pipeline.scope = original_actor, original_scope
        if app.kernel._admission.locked():
            app.kernel._admission.release()
        if caller is not None:
            await asyncio.gather(caller, return_exceptions=True)
        await app.close()


async def test_pn3_repair_committed_key_confirmation(tmp_path, monkeypatch):
    app, pipeline, request, source = await confirmed_task(tmp_path)
    original = app.repository._write
    actor = pipeline.actor
    committed = []
    before = app.adapter.execute_calls
    request = request.model_copy(
        update={"mode": "execute", "observations": [], "request_key": "repair-committed-key"}
    )

    async def write(operation):
        value = await original(operation)
        if isinstance(value, tuple) and len(value) == 2 and value[1] is True and not committed:
            committed.append(value[0]["id"])
            pipeline.actor = "revoked-after-real-commit"
            raise OSError("actual_committed_return_lost")
        return value

    try:
        monkeypatch.setattr(app.repository, "_write", write)
        with pytest.raises(ServiceError, match="target_binding_changed"):
            await pipeline.submit(request)
        pipeline.actor = actor
        old = await app.repository.by_key(request.model_dump(mode="json"))
        assert committed == [old["id"]]
        assert old["status"] == "failed" and old["code"] == "target_binding_changed"
        assert app.adapter.execute_calls == before
        assert (await pipeline.submit(request))["id"] == old["id"]
        assert app.adapter.execute_calls == before and old["id"] not in app.kernel.tasks
        (tmp_path / "repair-committed-key.json").write_text(
            json.dumps(
                {
                    "committed_ids": committed,
                    "original_key": old,
                    "before_exec": before,
                    "after_exec": app.adapter.execute_calls,
                },
                indent=2,
            )
        )
    finally:
        pipeline.actor = actor
        await app.close()


@pytest.mark.parametrize(
    "kind",
    [
        "source_denied",
        "permission_denied",
        "confirm_none",
        "confirm_io",
        "rolled_back",
        "foreign_key",
        "cleanup_io",
        "repeat_cancel",
    ],
)
async def test_pn3_repair_confirmation_boundaries(tmp_path, monkeypatch, kind, request):
    from krim_agent.evaluation.ports import _admission_fence

    node_id = request.node.nodeid
    app, pipeline, planned, source = await confirmed_task(tmp_path)
    repository = app.repository
    planned = planned.model_copy(
        update={"mode": "execute", "observations": [], "request_key": "boundary-" + kind}
    )
    value = planned.model_dump(mode="json")
    original_write = repository._write
    original_by_key = repository.by_key
    original_finish = repository.finish
    original_clock = getattr(repository, "_continuous_clock", None)
    entered, release = asyncio.Event(), asyncio.Event()
    baseline_tasks = asyncio.all_tasks()
    before_execute = app.adapter.execute_calls
    state = {
        "node": node_id,
        "kind": kind,
        "database": str(repository.path),
        "repository_object": id(repository),
        "engine_object": id(repository.engine),
        "writer_fd_at_start": repository._writer_fd,
        "request": value,
        "begin_calls": 0,
        "confirm_calls": 0,
        "finish_calls": 0,
        "deny": False,
        "lost": False,
        "reserved_ids": [],
        "actual_results": [],
        "sql": [],
        "error": None,
    }
    denial = "permission_denied" if kind == "permission_denied" else "source_changed"
    if kind in {"foreign_key", "cleanup_io", "repeat_cancel"}:
        denial = "target_binding_changed"
    caller = convergence_owner = None
    primary = None

    def authority():
        if state["deny"]:
            raise ServiceError(denial)

    def observed_sql(conn, cursor, statement, parameters, context, many):
        state["sql"].append(
            {
                "statement": statement,
                "parameters": repr(parameters)[:512],
                "after_denial": state["deny"],
            }
        )

    def remember(operation, result):
        bindings = dict(
            zip(
                operation.__code__.co_freevars,
                (cell.cell_contents for cell in (operation.__closure__ or ())),
                strict=True,
            )
        )
        state["reserved_ids"].append(bindings["task_id"])
        state["actual_results"].append({"id": result[0]["id"], "created": result[1]})

    async def write(operation):
        is_begin = operation.__qualname__.endswith("_begin_continuous.<locals>.operation")
        if not is_begin:
            return await original_write(operation)
        state["begin_calls"] += 1
        if kind == "rolled_back":
            async with repository.lock, repository.engine.begin() as connection:
                result = await operation(connection)
                remember(operation, result)
                await connection.rollback()
                state["rollback_transaction_closed"] = not connection.in_transaction()
            state["lost"] = state["deny"] = True
            raise OSError("real_rollback_return_lost")
        result = await original_write(operation)
        remember(operation, result)
        state["lost"] = True
        state["deny"] = kind not in {"confirm_none", "confirm_io"}
        raise OSError("real_committed_return_lost")

    async def by_key(incoming):
        if state["lost"] and incoming == value and kind in {"confirm_none", "confirm_io"}:
            state["confirm_calls"] += 1
            if kind == "confirm_io":
                raise OSError("real_confirmation_read_unavailable")
            return None
        return await original_by_key(incoming)

    async def finish(identity, status, code):
        nonlocal convergence_owner
        state["finish_calls"] += 1
        state["finish_arguments"] = [identity, status, code]
        convergence_owner = asyncio.current_task()
        if kind == "cleanup_io":
            raise OSError("actual_terminal_write_unavailable")
        if kind == "repeat_cancel":
            entered.set()
            await release.wait()
        return await original_finish(identity, status, code)

    context = _admission_fence()
    token = context.set((repository, authority))
    event.listen(repository.engine.sync_engine, "before_cursor_execute", observed_sql)
    try:
        foreign = None
        if kind == "foreign_key":
            foreign, created = await repository.begin(value)
            assert created is True
            assert foreign["status"] == "queued"
            state["foreign_before"] = foreign
        monkeypatch.setattr(repository, "_write", write)
        monkeypatch.setattr(repository, "by_key", by_key)
        monkeypatch.setattr(repository, "finish", finish)
        if kind == "repeat_cancel":
            caller = asyncio.create_task(repository.begin(value), name="entry10-boundary-caller")
            await asyncio.wait_for(entered.wait(), 2)
            assert convergence_owner is not None and not convergence_owner.done()
            caller.cancel("entry10-first-cancel")
            await asyncio.sleep(0)
            assert not caller.done() and not convergence_owner.cancelling()
            caller.cancel("entry10-second-cancel")
            await asyncio.sleep(0)
            assert caller.cancelling() == 2 and not caller.done()
            assert not convergence_owner.done()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await caller
            assert caller.done() and caller.cancelled()
            assert convergence_owner.done() and not convergence_owner.cancelled()
            state["outcome"] = "cancelled_after_convergence"
        else:
            expected = (
                "storage_unavailable"
                if kind in {"confirm_none", "confirm_io", "rolled_back", "cleanup_io"}
                else denial
            )
            with pytest.raises(ServiceError, match=expected) as rejected:
                await repository.begin(value)
            assert rejected.value.code == expected
            state["outcome"] = rejected.value.code
        state["denial_code"] = denial
        assert state["begin_calls"] == 1
        assert app.adapter.execute_calls == before_execute
        assert repository.healthy is False
        denied_sql = [row for row in state["sql"] if row["after_denial"]]
        assert not any(
            table in row["statement"]
            for row in denied_sql
            for table in [
                "FROM source_observations",
                "FROM dataset_revisions",
                "FROM dataset_lineage",
            ]
        )
        state["deny"] = False
        actual = await original_by_key(value)
        state["persisted"] = actual
        if kind == "rolled_back":
            assert actual is None
            assert state["rollback_transaction_closed"]
            assert state["finish_calls"] == 0
        elif kind == "foreign_key":
            assert actual == foreign
            assert state["actual_results"] == [{"id": foreign["id"], "created": False}]
            assert state["reserved_ids"][0] != foreign["id"]
            assert state["finish_calls"] == 0
        elif kind in {"confirm_none", "confirm_io", "cleanup_io"}:
            assert actual is not None and actual["status"] == "queued"
            assert actual["id"] == state["actual_results"][0]["id"]
            assert state["finish_calls"] == (1 if kind == "cleanup_io" else 0)
            state["terminal_convergence_confirmed"] = False
        else:
            assert actual is not None
            assert actual["id"] == state["actual_results"][0]["id"] == state["reserved_ids"][0]
            assert actual["status"] == "failed" and actual["code"] == denial
            assert state["finish_calls"] == 1
            assert actual["id"] not in app.kernel.tasks
            assert (await pipeline.submit(planned))["id"] == actual["id"]
            assert app.adapter.execute_calls == before_execute
            state["terminal_convergence_confirmed"] = True

        if kind == "confirm_none":
            false_calls = {"operation": 0, "confirmation": 0}

            async def false_operation(connection):
                false_calls["operation"] += 1
                await connection.execute(
                    update(tasks).where(tasks.c.id == actual["id"]).values(body=tasks.c.body)
                )
                return True

            async def false_write(operation):
                result = await original_write(operation)
                if operation is false_operation:
                    raise OSError("actual_false_confirmation_return_lost")
                return result

            async def false_confirm():
                false_calls["confirmation"] += 1
                async with repository.engine.begin() as connection:
                    identity = (
                        await connection.execute(
                            select(tasks.c.id).where(tasks.c.id == actual["id"])
                        )
                    ).scalar_one()
                return identity != actual["id"]

            monkeypatch.setattr(repository, "_write", false_write)
            assert await repository._confirmed(false_operation, false_confirm) is False
            assert false_calls == {"operation": 1, "confirmation": 1}
            assert repository.healthy is False
            state["false_is_not_none"] = false_calls
        state["after_execute"] = app.adapter.execute_calls
    except BaseException as error:
        primary = error
        state["error"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        state["deny"] = False
        release.set()
        if caller is not None:
            if not caller.done():
                caller.cancel()
            await asyncio.gather(caller, return_exceptions=True)
        context.reset(token)
        if original_clock is not None:
            repository._continuous_clock = original_clock
        try:
            await app.close()
        except BaseException as close_error:
            state["close_error"] = repr(close_error)
            if primary is None:
                raise
            primary.add_note("natural close failed: " + repr(close_error))
        finally:
            event.remove(repository.engine.sync_engine, "before_cursor_execute", observed_sql)
            state.update(
                repository_closed=repository.closed,
                writer_fd_after=repository._writer_fd,
                application_close_task_done=None
                if app._close_task is None
                else app._close_task.done(),
                connections=repository.resources,
                kernel_tasks={key: task.done() for key, task in app.kernel.tasks.items()},
                convergence_owner_done=None
                if convergence_owner is None
                else convergence_owner.done(),
                pending=[
                    task.get_name()
                    for task in asyncio.all_tasks()
                    if task not in baseline_tasks
                    and task is not asyncio.current_task()
                    and not task.done()
                ],
            )
            await asyncio.to_thread(
                (tmp_path / "entry10-confirmation-boundary.json").write_text,
                json.dumps(state, indent=2),
            )
