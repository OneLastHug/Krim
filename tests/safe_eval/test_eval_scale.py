"""PN3-S：真实单Task规模、分页与原owner收束，绝不以fixture回执替代执行。"""

import asyncio
import hashlib
import json
import time
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from importlib.resources import files
from pathlib import Path

import pytest
from sqlalchemy import event
from test_continuous_eval import continuous_resources as continuous_resources
from test_continuous_eval import seed

from krim_agent.evaluation.continuous_metrics import continuous_rules
from krim_agent.evaluation.continuous_types import (
    ComparisonPolicy,
    ConfirmationRequest,
    ContinuousRequest,
    FreezeConfirmed,
    ImportRequest,
)
from krim_agent.evaluation.domain import Budget, CaseSpec, DatasetRef, DatasetSpec, TaskSpec


def scale_cases(count=100):
    body = json.loads(
        files("krim_agent.evaluation.examples").joinpath("scale_reconciliation.v1.json").read_text()
    )
    return [CaseSpec.model_validate(c) for c in body["cases"][:count]]


async def confirmed_scale(tmp_path, count=100):
    app, initial, selection, flow = await seed(tmp_path)
    try:
        flow.redaction["artifact_paths"] = ["ledger.json", "summary.md"]
        confirmations = []
        for index, case in enumerate(scale_cases(count)):
            # 新Case来自明确人工重建；不把同一旧来源伪称100份原context。
            flow.sampling["salt_id"] = "pn3s-human-" + str(index)
            imported = await flow.import_sources(
                ImportRequest(request_key="scale-import-" + str(index), sources=[selection])
            )
            draft = await app.repository.candidate("local", imported[0]["id"])
            row = await flow.confirm_case(
                ConfirmationRequest(
                    request_key="scale-confirm-" + str(index),
                    candidate_id=draft["candidate_id"],
                    expected_revision=draft["revision"],
                    expected_hash=draft["hash"],
                    case=case,
                    context="manual_reconstructed",
                    reason="固定整数gold与独立初态；此次执行才能建立新Case的真实context证明。",
                )
            )
            assert row["status"] == "confirmed"
            assert row["context"] == "manual_reconstructed"
            confirmations.append(row["id"])
        frozen = await flow.freeze_dataset(
            FreezeConfirmed(
                request_key="scale-freeze",
                name="pn3-scale-reconciliation",
                version="1",
                confirmations=confirmations,
            )
        )
        assert len(set(confirmations)) == count
        request = ContinuousRequest(
            request_key="scale-execute",
            mode="execute",
            dataset=DatasetRef.model_validate(frozen["dataset_ref"]),
            dataset_envelope_fp=frozen["dataset_envelope_fp"],
            metrics=continuous_rules(app.repository.metric_registry),
            execution=Budget(repetitions=3, concurrency=2),
        )
        return app, flow, request
    except BaseException:
        await app.close()
        raise


def overlap_target(app, tmp_path):
    gate = tmp_path / "process-gate"
    gate.mkdir()
    real = app.adapter.worker
    code = (
        "import os,runpy,time\nfrom pathlib import Path\n"
        f"gate=Path({str(gate)!r})\n"
        "name=Path.cwd().name\n"
        "if name in {'scale-000-0','scale-000-1'}:\n"
        " (gate/name).write_text(str(os.getpid()))\n"
        " end=time.monotonic()+2\n"
        " while not (gate/'release').exists():\n"
        "  if time.monotonic()>=end: raise RuntimeError('fixture_gate_unreleased')\n"
        "  time.sleep(.005)\n"
        f"runpy.run_path({str(real)!r},run_name='__main__')\n"
    )
    compile(code, "fixed-scale-reference-wrapper", "exec")
    worker = tmp_path / "scale-reference-wrapper.py"
    worker.write_text(code)
    app.adapter.worker = worker
    app.adapter._descriptor["implementation_digest"] = hashlib.sha256(
        worker.read_bytes()
    ).hexdigest()
    app.adapter._descriptor["real_worker_digest"] = hashlib.sha256(real.read_bytes()).hexdigest()
    receipts = []
    intervals = []
    collect = app.adapter._collect

    async def observed(work):
        interval = {
            "task_id": work.prepared.spec.task_id,
            "ref": work.ref.id,
            "pid": work.process.pid,
            "start_monotonic": time.monotonic(),
        }
        intervals.append(interval)
        try:
            output = await collect(work)
        finally:
            interval.update(
                end_monotonic=time.monotonic(),
                returncode=work.process.returncode,
                pidfd=work.pidfd,
                collector_done=work.collector.done(),
                stdout_eof=work.stdout_eof,
                stderr_eof=work.stderr_eof,
            )
        receipts.append(
            {
                "task_id": work.prepared.spec.task_id,
                "case_id": work.prepared.spec.case_id,
                "repetition": work.prepared.spec.repetition,
                "ref": work.ref.id,
                "pid": work.process.pid,
                "wait_returncode": work.process.returncode,
                "pidfd": work.pidfd,
                "stdout_eof": work.stdout_eof,
                "stderr_eof": work.stderr_eof,
                "streams_joined": all(t.done() for t in work.stream_tasks),
                "started_at": output["process"]["started_at"],
                "finished_at": output["process"]["finished_at"],
                "group_empty": output["process"]["group_empty"],
                "uid": output["process"]["uid"],
            }
        )
        return output

    app.adapter._collect = observed
    return gate, receipts, intervals


async def all_report_rows(repo, task_id):
    rows, cursor = [], None
    while True:
        page = await repo.report_page("local", task_id, cursor=cursor, limit=16)
        assert 0 < len(page.rows) <= 16
        assert len(json.dumps(page.rows).encode()) <= 2097152
        rows.extend(page.rows)
        cursor = page.next_cursor
        if cursor is None:
            return rows


def observe_scale_stages(app, monkeypatch):
    """仅测试内有限观测：委托每个真实函数一次，不改变owner、事务或返回值。"""
    from krim_agent.evaluation import kernel, repository, runner, scale_store

    rows, bindings, transactions = [], [], []
    stack = ContextVar("test_scale_stage_parent", default=None)
    active = {}
    pending_transactions = {}

    @contextmanager
    def span(name, arguments=()):
        current = asyncio.current_task()
        row = {
            "call_id": len(rows),
            "parent_call_id": stack.get(),
            "stage": name,
            "async_task_observation_id": id(current),
            "async_task_name": None if current is None else current.get_name(),
            "start_monotonic": time.monotonic(),
        }
        for argument in arguments:
            if hasattr(argument, "task_id") and hasattr(argument, "case_id"):
                row.update(
                    task_id=argument.task_id,
                    case_id=argument.case_id,
                    repetition=argument.repetition,
                )
                break
        rows.append(row)
        active[row["call_id"]] = row
        token = stack.set(row["call_id"])
        try:
            yield
            row["outcome"] = "returned"
        except BaseException as error:
            row.update(
                outcome="raised", error=type(error).__name__, code=getattr(error, "code", None)
            )
            raise
        finally:
            row["end_monotonic"] = time.monotonic()
            active.pop(row["call_id"])
            stack.reset(token)

    def binding(label, original):
        path = Path(original.__code__.co_filename)
        bindings.append(
            {
                "stage": label,
                "module": original.__module__,
                "qualname": original.__qualname__,
                "source": str(path),
                "source_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "delegation": "same original callable exactly once per observed call",
            }
        )

    def observe_async(owner, name, label):
        original = getattr(owner, name)
        binding(label, original)

        @wraps(original)
        async def observed(*args, **kwargs):
            with span(label, args):
                return await original(*args, **kwargs)

        monkeypatch.setattr(owner, name, observed)

    def observe_sync(owner, name, label):
        original = getattr(owner, name)
        binding(label, original)

        @wraps(original)
        def observed(*args, **kwargs):
            with span(label, args):
                return original(*args, **kwargs)

        monkeypatch.setattr(owner, name, observed)

    original_write = app.repository._write
    binding("repository._write.total", original_write)

    @wraps(original_write)
    async def write_observed(operation):
        # 外层含锁/连接/operation/commit/退出；内层只包同一个原operation。
        label = "write.operation:" + operation.__qualname__

        @wraps(operation)
        async def operation_observed(conn):
            with span(label):
                return await operation(conn)

        with span("repository._write.total"):
            return await original_write(operation_observed)

    monkeypatch.setattr(app.repository, "_write", write_observed)
    for name in [
        "commit_with_receipt",
        "_snapshot",
        "_save",
        "_confirmed",
        "intent",
        "attach",
        "record",
        "commit",
        "record_resource_receipt",
        "execution_page",
        "complete",
    ]:
        observe_async(app.repository, name, "repository." + name)
    observe_async(app.kernel, "_drive", "kernel._drive.total")
    for name in [
        "header",
        "input_for_trial",
        "trial_facts",
        "checked_lineage",
        "mutate_trial",
        "save_resource_receipt",
        "finalize",
    ]:
        observe_async(scale_store, name, "scale_store." + name)
    observe_async(kernel, "execute_item", "runner.execute_item")
    observe_async(runner, "score_observation", "runner.score_observation")
    observe_sync(app.adapter, "assert_current", "adapter.assert_current")
    for owner, prefix, names in [
        (repository, "repository", ["clone", "neutral_encoded", "fingerprint"]),
        (scale_store, "scale_store", ["clone", "neutral_encoded", "neutral_fingerprint"]),
    ]:
        for name in names:
            observe_sync(owner, name, prefix + "." + name)

    def transaction_event(conn, kind):
        key = id(conn.connection.dbapi_connection)
        row = {
            "dbapi_connection_observation_id": key,
            "kind": kind,
            "before_monotonic": time.monotonic(),
            "parent_call_id": stack.get(),
        }
        transactions.append(row)
        pending_transactions[key] = row

    def before_commit(conn):
        transaction_event(conn, "commit_before_to_pool_reset")

    def before_rollback(conn):
        transaction_event(conn, "rollback_before_to_pool_reset")

    def reset(dbapi_connection, connection_record, reset_state):
        row = pending_transactions.pop(id(dbapi_connection), None)
        if row is not None:
            row["reset_monotonic"] = time.monotonic()

    engine = app.repository.engine.sync_engine
    event.listen(engine, "commit", before_commit)
    event.listen(engine, "rollback", before_rollback)
    event.listen(engine, "reset", reset)

    def finish():
        event.remove(engine, "commit", before_commit)
        event.remove(engine, "rollback", before_rollback)
        event.remove(engine, "reset", reset)
        return {
            "schema_version": "test-scale-stage-timing/1",
            "bindings": bindings,
            "spans": rows,
            "active_at_capture": list(active),
            "transactions": transactions,
            "transaction_ends_missing": list(pending_transactions),
            "semantics": (
                "单调调用边界，含await/调度；parent为嵌套观测，不是权限。"
                "并行/嵌套不相加为总耗时。commit事件在实际commit前，re"
                "set在归还时，区间不是纯fsync或CPU。"
            ),
            "original_functions_delegated_once": True,
            "extra_tasks_created": 0,
        }

    return span, finish


async def test_single_task_100x3_two_workers(tmp_path, monkeypatch):
    app, flow, request = await confirmed_scale(tmp_path)
    gate, receipts, intervals = overlap_target(app, tmp_path)
    phase, finish_stages = observe_scale_stages(app, monkeypatch)
    process_overlap = []
    before = app.owner.spawn_calls
    started = time.monotonic()
    view = result = None
    failure = close_failure = None
    sql = {}
    sql_started = {}

    def before_sql(conn, cursor, statement, parameters, context, executemany):
        sql_started[id(context)] = time.monotonic()

    def after_sql(conn, cursor, statement, parameters, context, executemany):
        begin = sql_started.pop(id(context))
        metric = sql.setdefault(statement, {"calls": 0, "seconds": 0.0})
        metric["calls"] += 1
        metric["seconds"] += time.monotonic() - begin

    engine = app.repository.engine.sync_engine
    event.listen(engine, "before_cursor_execute", before_sql)
    event.listen(engine, "after_cursor_execute", after_sql)
    try:
        with phase("main.submit"):
            view = await flow.submit(request)
        async with asyncio.timeout(2):
            while True:
                live = [
                    w
                    for w in app.owner.jobs.values()
                    if w.prepared.spec.task_id == view["id"]
                    and w.process.returncode is None
                    and w.pidfd >= 0
                ]
                if len(live) == 2:
                    # Popen/pidfd来自实际owner，普通gate只固定同时存活的观察窗口。
                    if all((gate / Path(w.prepared.directory).name).exists() for w in live):
                        process_overlap = [
                            {"pid": w.process.pid, "pidfd": w.pidfd, "at": time.monotonic()}
                            for w in live
                        ]
                        break
                await asyncio.sleep(0.005)
        assert len(process_overlap) == 2
        gate.joinpath("release").write_text("release both real processes")
        with phase("main.wait"):
            result = await app.kernel.wait(view["id"])
        with phase("main.report"):
            report = await app.repository.report(view["id"])
        assert result["status"] == "completed" and result["planned_count"] == 300
        rows = await all_report_rows(app.repository, view["id"])
        expected = {(f"scale-{i:03d}", r) for i in range(100) for r in range(3)}
        assert len(rows) == 300 and {(r["case_id"], r["repetition"]) for r in rows} == expected
        assert all(r["quality"] == "passed" for r in rows)
        assert len({r["execution_ref"] for r in rows}) == 300
        assert report["summary"]["planned"] == 300
        assert report["summary"]["planned_cells"] == 1200
        assert report["summary"]["cell_coverage"] == 1
        assert app.owner.spawn_calls - before == 300
        actual = [r for r in receipts if r["task_id"] == view["id"]]
        assert len(actual) == 300 and len({r["ref"] for r in actual}) == 300
        assert all(r["wait_returncode"] == 0 and r["pidfd"] == -1 for r in actual)
        assert all(
            r["stdout_eof"]
            and r["stderr_eof"]
            and r["streams_joined"]
            and r["group_empty"]
            and r["uid"] == 1001
            for r in actual
        )
        assert not [w for w in app.owner.jobs.values() if w.prepared.spec.task_id == view["id"]]
        high_water = app.kernel._high_water[view["id"]]
        assert high_water["workers"] == high_water["inflight"] == 2
        assert 0 < high_water["queue"] <= 2
        assert (await flow.submit(request))["id"] == view["id"]
        assert app.owner.spawn_calls - before == 300
        policy = ComparisonPolicy(
            baseline_id=view["id"],
            candidate_id=view["id"],
            baseline_hash=report["report_hash"],
            candidate_hash=report["report_hash"],
            allowed_subjects=[report["subject_fingerprint"]],
        )
        with phase("main.compare_paged"):
            assert (await app.repository.compare_paged("local", policy))["status"] == "pass"
    except BaseException as error:
        failure = {"type": type(error).__name__, "code": getattr(error, "code", None)}
        raise
    finally:
        gate.joinpath("release").write_text("fixture final release")
        try:
            with phase("main.close"):
                await app.close()
        except BaseException as error:
            close_failure = {"type": type(error).__name__, "code": getattr(error, "code", None)}
            raise
        finally:
            # 不在成功断言后才采证；timeout/取消/业务失败也保存真实内存观测。
            event.remove(engine, "before_cursor_execute", before_sql)
            event.remove(engine, "after_cursor_execute", after_sql)
            identity = None if view is None else view["id"]
            stages = finish_stages()
            (tmp_path / "scale-stage-timings.json").write_text(
                json.dumps(
                    {
                        "node": "test_single_task_100x3_two_workers",
                        "task_id": identity,
                        "database": str(app.repository.path),
                        **stages,
                    },
                    indent=2,
                )
            )
            (tmp_path / "scale-process-resources.json").write_text(
                json.dumps(
                    {
                        "node": "test_single_task_100x3_two_workers",
                        "task_id": identity,
                        "database": str(app.repository.path),
                        "request": request.model_dump(mode="json"),
                        "start_monotonic": started,
                        "end_monotonic": time.monotonic(),
                        "overlap": process_overlap,
                        "intervals": intervals,
                        "processes": receipts,
                        "spawn_delta": app.owner.spawn_calls - before,
                        "result": result,
                        "failure": failure,
                        "close_failure": close_failure,
                        "high_water": app.kernel._high_water.get(identity),
                        "failure_facts": app.kernel._failure_facts.get(identity),
                        "kernel_closed": app.kernel.closed,
                        "repository_closed": app.repository.closed,
                        "owner_closed": app.owner.closed,
                        "adapter_closed": app.adapter.closed,
                        "cleanup_unknown": app.kernel.cleanup_unknown,
                        "producer_done": {k: t.done() for k, t in app.kernel.tasks.items()},
                        "workers_remaining": len(app.kernel._workers),
                        "inflight_remaining": len(app.kernel._inflight),
                        "sql": sql,
                        "sql_incomplete": len(sql_started),
                        "retained_jobs": [
                            {
                                "ref": w.ref.id,
                                "pid": w.process.pid,
                                "returncode": w.process.returncode,
                                "pidfd": w.pidfd,
                                "collector_done": w.collector.done(),
                            }
                            for w in app.owner.jobs.values()
                        ],
                    },
                    indent=2,
                )
            )
    from krim_agent.evaluation.app import open_eval

    async with await open_eval(app.repository.path, readonly=True) as reader:
        assert await reader.repository.report(view["id"]) == report
        assert await all_report_rows(reader.repository, view["id"]) == rows
        assert reader.adapter is None and reader.repository.metric_registry.calls == 0


@pytest.mark.parametrize(
    "kind",
    [
        "cases",
        "trials",
        "reports",
        "cross_scope",
        "filter",
        "order",
        "version",
        "tamper",
        "stale_revision",
        "zero_limit",
        "byte_cap",
    ],
)
async def test_page_cursor_scope_order_and_bytes(tmp_path, monkeypatch, kind):
    if kind != "trials":
        import base64

        from sqlalchemy import select, update

        from krim_agent.contracts import ServiceError
        from krim_agent.evaluation.domain import neutral_encoded, neutral_fingerprint
        from krim_agent.evaluation.repository import tasks, trials

        app, _, _, _ = await seed(tmp_path)
        facts = {"kind": kind, "database": str(app.repository.path)}
        try:
            data = DatasetSpec(name="cursor-matrix", version="1", cases=scale_cases(9))
            await app.repository.freeze(data)
            request = TaskSpec(
                request_key="cursor-" + kind,
                dataset=data.ref(),
                metrics=app.repository.metric_registry.specs(),
                execution=Budget(concurrency=2),
            )
            view = await app.run(request)
            state = await app.kernel.wait(view["id"])
            assert state["status"] == "completed"
            facts["task_id"] = view["id"]
            method = (
                app.repository.case_page
                if kind == "cases"
                else app.repository.report_page
                if kind == "reports"
                else app.repository.trial_page
            )
            page = await method("local", view["id"], limit=1)
            assert page.next_cursor is not None
            facts["binding"] = page.binding
            if kind in {"cases", "reports"}:
                rows = list(page.rows)
                cursor = page.next_cursor
                while cursor is not None:
                    page = await method("local", view["id"], cursor=cursor, limit=2)
                    assert len(neutral_encoded(page.rows).encode()) <= 2097152
                    rows.extend(page.rows)
                    cursor = page.next_cursor
                assert [r["case_id"] for r in rows] == sorted(c.case_id for c in data.cases)
                assert len(rows) == len({r["case_id"] for r in rows}) == 9
            elif kind == "cross_scope":
                with pytest.raises(ServiceError) as error:
                    await method("other", view["id"], cursor=page.next_cursor, limit=1)
                assert error.value.code == "not_found"
            elif kind == "zero_limit":
                with pytest.raises(ServiceError) as error:
                    await method("local", view["id"], limit=0)
                assert error.value.code == "invalid_input"
            elif kind == "byte_cap":
                # 实际持久Trial投影超过2MiB；不缩小产品上限、不伪造返回页。
                async with app.repository.lock, app.repository.engine.begin() as conn:
                    row = (
                        await conn.execute(
                            select(trials.c.body).where(
                                trials.c.task_id == view["id"], trials.c.case_id == "scale-000"
                            )
                        )
                    ).scalar_one()
                    row["code"] = "x" * 2097153
                    await conn.execute(
                        update(trials)
                        .where(trials.c.task_id == view["id"], trials.c.case_id == "scale-000")
                        .values(body=row)
                    )
                with pytest.raises((ServiceError, ValueError)):
                    await method("local", view["id"], limit=1)
            elif kind == "stale_revision":
                async with app.repository.lock, app.repository.engine.begin() as conn:
                    row = (
                        await conn.execute(select(tasks.c.body).where(tasks.c.id == view["id"]))
                    ).scalar_one()
                    row["revision"] += 1
                    await conn.execute(
                        update(tasks).where(tasks.c.id == view["id"]).values(body=row)
                    )
                with pytest.raises(ServiceError) as error:
                    await method("local", view["id"], cursor=page.next_cursor, limit=1)
                assert error.value.code == "source_changed"
            else:
                token = json.loads(base64.urlsafe_b64decode(page.next_cursor))
                if kind == "filter":
                    token["filter_fingerprint"] = neutral_fingerprint({"projection": "cases"})
                elif kind == "order":
                    token["order"] = "repetition,case_id-binary"
                elif kind == "version":
                    token["version"] = "eval-page/2"
                else:
                    assert kind == "tamper"
                    token["last_key"] = ["scale-007", 0]
                if kind != "tamper":
                    token["checksum"] = neutral_fingerprint(
                        {k: v for k, v in token.items() if k != "checksum"}
                    )
                changed = base64.urlsafe_b64encode(neutral_encoded(token).encode()).decode()
                with pytest.raises(ServiceError) as error:
                    await method("local", view["id"], cursor=changed, limit=1)
                assert error.value.code == "invalid_input"
            assert (await app.run(request))["id"] == view["id"]
            facts["spawn_delta"] = app.adapter.execute_calls
        finally:
            await app.close()
            facts["closed"] = app.repository.closed and app.owner.closed
            await asyncio.to_thread(
                (tmp_path / "cursor-behavior.json").write_text, json.dumps(facts, indent=2)
            )
        return
    app, initial, selection, flow = await seed(tmp_path)
    try:
        data = DatasetSpec(name="page-source", version="1", cases=scale_cases(9))
        await app.repository.freeze(data)
        view = await app.run(
            TaskSpec(
                request_key="page-source",
                dataset=data.ref(),
                metrics=app.repository.metric_registry.specs(),
            )
        )
        assert (await app.kernel.wait(view["id"]))["status"] == "completed"
        snapshots = []
        original = app.repository._snapshot

        async def counted(conn, identity):
            value = await original(conn, identity)
            snapshots.append(
                {
                    "id": identity,
                    "has_full_items": "items" in value,
                    "bytes": len(json.dumps(value).encode()),
                }
            )
            return value

        monkeypatch.setattr(app.repository, "_snapshot", counted)
        page = await app.repository.trial_page("local", view["id"], limit=1)
        assert len(page.rows) == 1 and page.total_count == 9
        assert not any(s["has_full_items"] for s in snapshots), snapshots
        assert "output" not in page.rows[0] and "output_digest" in page.rows[0]
    finally:
        await app.close()


@pytest.mark.parametrize(
    "kind", ["legacy_next_trial", "paged_next_trial", "paged_after_await_reject"]
)
async def test_trial_fence_input_binding(tmp_path, monkeypatch, kind):
    from dataclasses import replace

    from krim_agent.contracts import ServiceError
    from krim_agent.evaluation import kernel as kernel_module
    from krim_agent.evaluation.continuous_types import ScoreSource
    from krim_agent.evaluation.ports import _admission_fence

    app, initial, selection, flow = await seed(tmp_path)
    evidence = {
        "kind": kind,
        "database": str(app.repository.path),
        "source_seed": selection.task_id,
        "guards": [],
        "retained_fence_checks": [],
        "worker_calls": [],
        "new_guard_calls": 0,
        "read_trial_returns": 0,
        "commit_calls": 0,
    }
    before_spawn = app.owner.spawn_calls
    before_score = app.repository.metric_registry.calls
    original_plan = app.repository.plan
    original_execute = kernel_module.execute_item
    original_read = app.repository.read_trial
    original_commit = app.repository.commit
    original_combined = app.repository.commit_with_receipt
    previous = {}
    guard_inputs = {}

    def guard_for(key):
        def checked():
            evidence["guards"].append(key)

        return checked

    def plan(snapshot):
        actual = original_plan(snapshot)
        evidence["actual_layout"] = snapshot.get("layout", 2)
        evidence["actual_mode"] = actual.mode
        case_ids = (
            list(snapshot["case_index"])
            if snapshot.get("layout") == 3
            else [case["case_id"] for case in snapshot["dataset"]["cases"]]
        )
        for case_id in case_ids:
            key = (case_id, 0)
            guard_inputs[key] = {"guard": guard_for(case_id)}
        # 只增加可信测试guard，不改真实计划的mode/Trial/分页/目标/预算。
        return replace(actual, scoring_inputs=guard_inputs)

    async def execute(trial, *args, **kwargs):
        current = asyncio.current_task()
        fence = kwargs["guard"]
        host = _admission_fence().get()
        assert host[0] is app.repository and host[1] is fence
        evidence["worker_calls"].append(
            {"worker": id(current), "case_id": trial.case_id, "fence": id(fence)}
        )
        if current in previous:
            old_case, old_fence = previous[current]
            start = len(evidence["guards"])
            old_fence()
            evidence["retained_fence_checks"].append(
                {
                    "worker": id(current),
                    "old_case": old_case,
                    "current_case": trial.case_id,
                    "old_fence": id(old_fence),
                    "observed_guards": evidence["guards"][start:],
                }
            )
        previous[current] = (trial.case_id, fence)
        return await original_execute(trial, *args, **kwargs)

    def reject_after_read():
        evidence["new_guard_calls"] += 1
        raise ServiceError("source_changed")

    async def read_trial(*args, **kwargs):
        actual = await original_read(*args, **kwargs)
        evidence["read_trial_returns"] += 1
        # 原读者恰await一次并返回真实持久证据，只加本次输入的拒绝guard。
        return actual | {"guard": reject_after_read}

    async def commit(*args, **kwargs):
        evidence["commit_calls"] += 1
        return await original_commit(*args, **kwargs)

    async def combined(*args, **kwargs):
        evidence["commit_calls"] += 1
        return await original_combined(*args, **kwargs)

    monkeypatch.setattr(app.repository, "plan", plan)
    monkeypatch.setattr(kernel_module, "execute_item", execute)
    monkeypatch.setattr(app.repository, "commit", commit)
    monkeypatch.setattr(app.repository, "commit_with_receipt", combined)
    view = None
    try:
        if kind == "paged_after_await_reject":
            flow.redaction["artifact_paths"] = ["ledger.json", "summary.md"]
            imported = await flow.import_sources(
                ImportRequest(request_key="fence-source", sources=[selection])
            )
            draft = await app.repository.candidate("local", imported[0]["id"])
            confirmed = await flow.confirm_case(
                ConfirmationRequest(
                    request_key="fence-confirm",
                    candidate_id=draft["candidate_id"],
                    expected_revision=draft["revision"],
                    expected_hash=draft["hash"],
                    case=initial.cases[0],
                    context="complete",
                    reason="真实种子Case、context与原证据不变，仅核分页返回后的输入guard。",
                )
            )
            frozen = await flow.freeze_dataset(
                FreezeConfirmed(
                    request_key="fence-freeze",
                    name="fence-score",
                    version="1",
                    confirmations=[confirmed["id"]],
                )
            )
            observation = await app.repository.observation("local", imported[0]["id"])
            request = ContinuousRequest(
                request_key="fence-after-await",
                mode="observed_score",
                dataset=DatasetRef.model_validate(frozen["dataset_ref"]),
                dataset_envelope_fp=frozen["dataset_envelope_fp"],
                metrics=continuous_rules(app.repository.metric_registry),
                execution=Budget(concurrency=2),
                observations=[
                    ScoreSource(
                        case_id=initial.cases[0].case_id,
                        repetition=0,
                        observation_id=observation["id"],
                        observation_hash=observation["content_hash"],
                    )
                ],
            )
            monkeypatch.setattr(app.repository, "read_trial", read_trial)
            view = await flow.submit(request)
        else:
            count = 2 if kind == "legacy_next_trial" else 9
            data = DatasetSpec(name="trial-fence", version="1", cases=scale_cases(count))
            await app.repository.freeze(data)
            request = TaskSpec(
                request_key="fence-" + kind,
                dataset=data.ref(),
                metrics=app.repository.metric_registry.specs(),
                execution=Budget(concurrency=1),
            )
            view = await app.run(request)
        result = await app.kernel.wait(view["id"])
        evidence.update(
            task_id=view["id"],
            result=result,
            spawn_delta=app.owner.spawn_calls - before_spawn,
            scorer_delta=app.repository.metric_registry.calls - before_score,
        )
        if kind == "paged_after_await_reject":
            assert evidence["actual_mode"] == "observed_score"
            assert evidence["actual_layout"] == 3
            assert evidence["read_trial_returns"] == 1
            assert evidence["new_guard_calls"] >= 1
            assert evidence["spawn_delta"] == evidence["scorer_delta"] == 0
            assert evidence["commit_calls"] == 0
            assert result["status"] == "failed" and result["code"] == "source_changed"
        else:
            assert result["status"] == "completed"
            assert evidence["spawn_delta"] == count
            assert evidence["scorer_delta"] == count * len(request.metrics)
            assert evidence["commit_calls"] == count
            assert len({row["worker"] for row in evidence["worker_calls"]}) == 1
            assert len(evidence["retained_fence_checks"]) == count - 1
            assert all(
                row["observed_guards"] == [row["old_case"]]
                for row in evidence["retained_fence_checks"]
            ), evidence["retained_fence_checks"]
        assert (await app.repository.by_key(request.model_dump(mode="json")))["id"] == view["id"]
        assert app.owner.spawn_calls - before_spawn == evidence["spawn_delta"]
    finally:
        await app.close()
        evidence["closed"] = app.repository.closed and app.owner.closed
        evidence["retained_jobs"] = [
            {
                "ref": job.ref.id,
                "returncode": job.process.returncode,
                "pidfd": job.pidfd,
                "collector_done": job.collector.done(),
                "streams_done": all(task.done() for task in job.stream_tasks),
                "stdout_eof": job.stdout_eof,
                "stderr_eof": job.stderr_eof,
                "cleanup": job.cleanup_receipts,
            }
            for job in app.owner.jobs.values()
        ]
        (tmp_path / "trial-fence-facts.json").write_text(json.dumps(evidence, indent=2))


@pytest.mark.parametrize(
    "kind",
    [
        "physical_closed_not_durable",
        "scorer_cancel",
        "commit_cancel",
        "durable_reply_cancel",
        "wrong_lease",
        "not_closed",
        "binding_drift",
    ],
)
async def test_combined_commit_retirement_windows(tmp_path, monkeypatch, kind):
    import sqlite3
    from contextlib import closing

    from krim_agent.contracts import ServiceError
    from krim_agent.evaluation import runner, scale_store
    from krim_agent.evaluation.adapters.local_process import LocalProcessAdapter
    from krim_agent.evaluation.metrics import MetricRegistry

    capture = {"kind": kind, "retired": [], "rejected_entry_calls": 0, "armed": False}
    ready, release = asyncio.Event(), asyncio.Event()
    blocked = {}
    original_retire = LocalProcessAdapter.retire
    original_receipt = LocalProcessAdapter.retirement_receipt
    original_score = MetricRegistry.score
    original_execute = LocalProcessAdapter.execute

    def physical(job):
        return {
            "ref": job.ref.id,
            "pid": job.process.pid,
            "returncode": job.process.returncode,
            "pidfd": job.pidfd,
            "collector_done": job.collector.done(),
            "stdout_eof": job.stdout_eof,
            "stderr_eof": job.stderr_eof,
            "streams_done": all(task.done() for task in job.stream_tasks),
        }

    def retired(adapter, ref, value):
        job = adapter.jobs[ref.id]
        capture["retired"].append(physical(job))
        return original_retire(adapter, ref, value)

    def receipt(adapter, ref):
        job = adapter.jobs[ref.id]
        if kind == "wrong_lease" and capture["armed"]:
            capture["physical_before_wrong_lease"] = physical(job)
            previous = job.lease
            job.lease = object()
            try:
                return original_receipt(adapter, ref)
            finally:
                job.lease = previous
        return original_receipt(adapter, ref)

    async def scored(registry, rule, case, output):
        value = await original_score(registry, rule, case, output)
        if kind == "scorer_cancel" and capture["armed"]:
            blocked["task"] = asyncio.current_task()
            capture["real_scorer_returned"] = value
            ready.set()
            await release.wait()
        return value

    async def executed(adapter, prepared):
        ref = await original_execute(adapter, prepared)
        job = adapter.jobs[ref.id]
        assert job.ref is ref
        assert adapter.owner.jobs.get(ref.id) is job
        assert job.prepared is prepared
        capture.setdefault("execute_observations", []).append(
            {
                "task_id": prepared.spec.task_id,
                "ref": ref.id,
                "pid": job.process.pid,
                "pidfd": job.pidfd,
                "armed": capture["armed"],
            }
        )
        if kind == "not_closed" and capture["armed"]:
            ready.set()
        return ref

    # 在可信fixture装配前设透明观察，原身份冻结自然绑定这些真实委托函数。
    monkeypatch.setattr(LocalProcessAdapter, "retire", retired)
    monkeypatch.setattr(LocalProcessAdapter, "retirement_receipt", receipt)
    monkeypatch.setattr(MetricRegistry, "score", scored)
    monkeypatch.setattr(LocalProcessAdapter, "execute", executed)
    app, _, _, _ = await seed(tmp_path)
    capture["armed"] = True
    capture["database"] = str(app.repository.path)
    before_spawn, before_score = app.owner.spawn_calls, app.repository.metric_registry.calls
    original_save = scale_store.save_resource_receipt
    original_write = app.repository._write
    original_scores = runner.score_observation
    gate = None
    view = None

    def facts(identity):
        with closing(sqlite3.connect(app.repository.path.as_uri() + "?mode=ro", uri=True)) as db:
            db.execute("PRAGMA query_only=ON")
            rows = [
                json.loads(row[0])
                for row in db.execute("SELECT body FROM eval_trials WHERE task_id=?", (identity,))
            ]
            receipts = db.execute(
                "SELECT count(*) FROM eval_resource_receipts WHERE task_id=?", (identity,)
            ).fetchone()[0]
        return {"trials": rows, "receipts": receipts}

    async def save(conn, task_id, case_id, repetition, value, *args, **kwargs):
        result = await original_save(conn, task_id, case_id, repetition, value, *args, **kwargs)
        if kind in {"physical_closed_not_durable", "commit_cancel"}:
            blocked["task"] = asyncio.current_task()
            capture["physical_before_durable"] = physical(app.adapter.jobs[value["execution_ref"]])
            capture["reader_before_durable"] = facts(task_id)
            ready.set()
            await release.wait()
        return result

    async def write(operation):
        result = await original_write(operation)
        if kind == "durable_reply_cancel" and "commit_with_receipt" in operation.__qualname__:
            capture["durable_before_reply"] = True
            ready.set()
            await release.wait()
        return result

    async def score_observation(*args, **kwargs):
        value = await original_scores(*args, **kwargs)
        if kind == "binding_drift":

            async def rejected(*unused_args, **unused_kwargs):
                capture["rejected_entry_calls"] += 1
                raise ServiceError("target_binding_changed")

            app.repository.commit_with_receipt = rejected
        return value

    monkeypatch.setattr(scale_store, "save_resource_receipt", save)
    monkeypatch.setattr(app.repository, "_write", write)
    monkeypatch.setattr(runner, "score_observation", score_observation)
    if kind == "durable_reply_cancel":
        original_combined = app.repository.commit_with_receipt

        async def combined(*args, **kwargs):
            blocked["caller"] = asyncio.current_task()
            return await original_combined(*args, **kwargs)

        monkeypatch.setattr(app.repository, "commit_with_receipt", combined)
    try:
        if kind == "not_closed":
            gate, _, _ = overlap_target(app, tmp_path)
        data = DatasetSpec(name="retirement-window", version="1", cases=scale_cases(1))
        await app.repository.freeze(data)
        request = TaskSpec(
            request_key="window-" + kind,
            dataset=data.ref(),
            metrics=app.repository.metric_registry.specs(),
            execution=Budget(concurrency=2),
        )
        view = await app.run(request)
        capture["task_id"] = view["id"]
        if kind == "not_closed":
            async with asyncio.timeout(2):
                await ready.wait()
            job = next(
                job for job in app.owner.jobs.values() if job.prepared.spec.task_id == view["id"]
            )
            capture["actual_live_job"] = physical(job)
            assert job.process.returncode is None and job.pidfd >= 0
            with pytest.raises(ServiceError) as error:
                app.adapter.retirement_receipt(job.ref)
            assert error.value.code == "execution_unknown"
            assert not capture["retired"]
            gate.joinpath("release").write_text("release actual live job")
        elif kind in {
            "physical_closed_not_durable",
            "scorer_cancel",
            "commit_cancel",
            "durable_reply_cancel",
        }:
            async with asyncio.timeout(2):
                await ready.wait()
            assert not capture["retired"]
            if kind == "physical_closed_not_durable":
                before = capture["reader_before_durable"]
                assert before["receipts"] == 0 and before["trials"][0]["status"] != "finished"
                assert capture["physical_before_durable"]["pidfd"] == -1
                release.set()
            elif kind == "durable_reply_cancel":
                before = facts(view["id"])
                assert before["receipts"] == 1 and before["trials"][0]["status"] == "finished"
                blocked["caller"].cancel()
                blocked["caller"].cancel()
                release.set()
            else:
                blocked["task"].cancel()
                blocked["task"].cancel()
                release.set()
        state = await app.kernel.wait(view["id"])
        actual = facts(view["id"])
        capture.update(
            result=state,
            final=actual,
            spawn_delta=app.owner.spawn_calls - before_spawn,
            scorer_delta=app.repository.metric_registry.calls - before_score,
        )
        assert capture["spawn_delta"] == 1
        assert capture["rejected_entry_calls"] == 0
        if kind in {"physical_closed_not_durable", "not_closed"}:
            assert state["status"] == "completed"
            assert actual["receipts"] == len(capture["retired"]) == 1
        else:
            assert state["status"] != "completed"
            assert not capture["retired"]
            assert any(job.prepared.spec.task_id == view["id"] for job in app.adapter.jobs.values())
            if kind == "durable_reply_cancel":
                assert actual["receipts"] == 1 and actual["trials"][0]["status"] == "finished"
            else:
                assert actual["receipts"] == 0 and actual["trials"][0]["status"] != "finished"
            if kind == "binding_drift":
                assert state["code"] == "target_binding_changed"
        assert (await app.repository.by_key(request.model_dump(mode="json")))["id"] == view["id"]
        assert app.owner.spawn_calls - before_spawn == 1
    finally:
        release.set()
        if gate is not None:
            gate.joinpath("release").write_text("fixture final release")
        await app.close()
        capture["closed"] = app.repository.closed and app.owner.closed
        capture["jobs_after_close"] = [
            physical(job) | {"cleanup": job.cleanup_receipts} for job in app.owner.jobs.values()
        ]
        capture.pop("armed", None)
        (tmp_path / "retirement-window-facts.json").write_text(json.dumps(capture, indent=2))


@pytest.mark.parametrize(
    "kind", ["confirmed", "return_lost", "unconfirmed", "unknown", "wrong_lease"]
)
async def test_retirement_after_durable_cleanup(tmp_path, monkeypatch, kind):
    if kind != "confirmed":
        import sqlite3
        from contextlib import closing

        from krim_agent.contracts import ServiceError
        from krim_agent.evaluation import scale_store
        from krim_agent.evaluation.adapters.local_process import LocalProcessAdapter

        if kind == "unknown":
            import ast
            import sys

            tree = ast.parse(await asyncio.to_thread(Path(__file__).read_text))
            function = next(
                n
                for n in tree.body
                if isinstance(n, ast.AsyncFunctionDef)
                and n.name == "test_worker_failure_and_cancel_join"
            )
            assignment = next(
                n.value
                for n in function.body
                if isinstance(n, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == "driver" for t in n.targets)
            )
            # 复用同一真实集成程序的durable-unknown注入，不调用另一测试或伪造收据。
            literals = [
                n.value
                for n in ast.walk(assignment)
                if isinstance(n, ast.Constant) and isinstance(n.value, str)
            ]
            core = next(v for v in literals if "async def main():" in v)
            observed = next(
                n
                for n in tree.body
                if isinstance(n, ast.AsyncFunctionDef) and n.name == "test_shared_modes_scale"
            )
            accepted = next(
                n.value.value
                for n in observed.body
                if isinstance(n, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == "driver" for t in n.targets)
            )
            checkpoint = next(
                n
                for n in ast.parse(accepted).body
                if isinstance(n, ast.FunctionDef) and n.name == "_write_scale_checkpoint"
            )
            driver = (
                "import asyncio,fcntl,hashlib,json,os,sqlite3,stat,sys,time\n"
                "from pathlib import Path\nfrom contextlib import closing\n"
                "source,root=map(Path,sys.argv[1:3]);kind='cleanup_unknown'\n"
                + ast.unparse(checkpoint)
                + "\n"
                + core
            )
            argv = [
                sys.executable,
                "-I",
                "-B",
                "-c",
                driver,
                str((await asyncio.to_thread(Path(__file__).resolve)).parents[2] / "src"),
                str(tmp_path),
            ]
            proc, process, facts = await _run_scale_evidence_process(
                argv,
                tmp_path,
                execution_timeout_seconds=60,
                source_database=None,
                destination_database=str(tmp_path / "lifecycle.sqlite"),
            )
            assert proc.returncode == 0 and process["cleanup_status"] == "closed"
            assert facts["durable_cleanup"] == "unknown" and not facts["retired"]
            return
        capture = {"kind": kind, "armed": False, "retired": [], "receipt_attempts": 0}
        original_retire = LocalProcessAdapter.retire
        original_receipt = LocalProcessAdapter.retirement_receipt

        def retired(adapter, ref, receipt):
            if capture["armed"]:
                capture["retired"].append(receipt)
            return original_retire(adapter, ref, receipt)

        def receipt(adapter, ref):
            job = adapter.jobs[ref.id]
            if capture["armed"]:
                capture["receipt_attempts"] += 1
                assert job.process.returncode is not None and job.pidfd == -1
                assert job.collector.done() and job.stdout_eof and job.stderr_eof
                if kind == "wrong_lease":
                    lease = job.lease
                    job.lease = object()
                    try:
                        return original_receipt(adapter, ref)
                    finally:
                        job.lease = lease
            return original_receipt(adapter, ref)

        monkeypatch.setattr(LocalProcessAdapter, "retire", retired)
        monkeypatch.setattr(LocalProcessAdapter, "retirement_receipt", receipt)
        app, _, _, _ = await seed(tmp_path)
        capture.update(armed=True, database=str(app.repository.path))
        write, save = app.repository._write, scale_store.save_resource_receipt
        lost = False

        async def written(operation):
            nonlocal lost
            result = await write(operation)
            if (
                kind == "return_lost"
                and "commit_with_receipt" in operation.__qualname__
                and not lost
            ):
                lost = True
                capture["committed_before_lost_return"] = True
                raise OSError("controlled_after_real_combined_commit")
            return result

        async def saved(*args, **kwargs):
            result = await save(*args, **kwargs)
            if kind == "unconfirmed":
                capture["actual_receipt_INSERT_before_rollback"] = True
                raise ServiceError("key_conflict")
            return result

        monkeypatch.setattr(app.repository, "_write", written)
        monkeypatch.setattr(scale_store, "save_resource_receipt", saved)
        try:
            data = DatasetSpec(name="retirement-" + kind, version="1", cases=scale_cases(1))
            await app.repository.freeze(data)
            request = TaskSpec(
                request_key="retirement-" + kind,
                dataset=data.ref(),
                metrics=app.repository.metric_registry.specs(),
                execution=Budget(concurrency=2),
            )
            before = app.owner.spawn_calls
            view = await app.run(request)
            state = await app.kernel.wait(view["id"])
            capture.update(task_id=view["id"], state=state)
            assert app.owner.spawn_calls - before == 1
            with closing(
                sqlite3.connect(app.repository.path.as_uri() + "?mode=ro", uri=True)
            ) as db:
                db.execute("PRAGMA query_only=ON")
                receipt_count = db.execute(
                    "SELECT count(*) FROM eval_resource_receipts WHERE task_id=?", (view["id"],)
                ).fetchone()[0]
            if kind == "return_lost":
                assert lost and state["status"] == "completed"
                assert receipt_count == len(capture["retired"]) == 1
            else:
                assert state["status"] == "failed" and not capture["retired"] and receipt_count == 0
                assert state["code"] == (
                    "key_conflict" if kind == "unconfirmed" else "permission_denied"
                )
                assert capture["receipt_attempts"] >= 1
            assert (await app.run(request))["id"] == view["id"]
            assert app.owner.spawn_calls - before == 1
        finally:
            capture["armed"] = False
            await app.close()
            capture["closed"] = app.repository.closed and app.owner.closed
            await asyncio.to_thread(
                (tmp_path / "retirement-behavior.json").write_text, json.dumps(capture, indent=2)
            )
        return
    app, initial, selection, flow = await seed(tmp_path)
    try:
        data = DatasetSpec(name="retire-source", version="1", cases=scale_cases(1))
        await app.repository.freeze(data)
        request = TaskSpec(
            request_key="retire",
            dataset=data.ref(),
            metrics=app.repository.metric_registry.specs(),
            execution=Budget(concurrency=2),
        )
        before = app.owner.spawn_calls
        view = await app.run(request)
        result = await app.kernel.wait(view["id"])
        assert result["status"] == "completed"
        assert app.owner.spawn_calls - before == 1
        retained = [w for w in app.owner.jobs.values() if w.prepared.spec.task_id == view["id"]]
        assert retained == [], [
            (w.ref.id, w.process.returncode, w.pidfd, w.collector.done()) for w in retained
        ]
        persisted = await app.repository.read_trial("local", view["id"], "scale-000", 0)
        assert persisted["output"]["cleanup"] == "closed"
        assert persisted["quality"] == "passed"
        assert (await app.run(request))["id"] == view["id"]
        assert app.owner.spawn_calls - before == 1
    finally:
        await app.close()


@pytest.mark.parametrize(
    "scenario",
    [
        "source_layout3_cold",
        "source_layout3_rescore_parent",
        "cli_task_pages",
        "cli_report_pages",
        "cli_compare_pages",
        "cli_continuous_compare_pages",
        "bare_root_not_gate",
        "legacy_cli",
    ],
)
async def test_scale_direct_consumers(tmp_path, scenario):
    if scenario != "source_layout3_cold":
        import ast
        import sys

        storage_path = Path(__file__).with_name("test_eval_scale_storage.py")
        storage_tree = ast.parse(await asyncio.to_thread(storage_path.read_text))
        storage_test = next(
            node
            for node in storage_tree.body
            if isinstance(node, ast.AsyncFunctionDef)
            and node.name == "test_scoring_input_representation"
        )
        scale_tree = ast.parse(await asyncio.to_thread(Path(__file__).read_text))
        scale_test = next(
            node
            for node in scale_tree.body
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "test_shared_modes_scale"
        )
        accepted = next(
            node.value.value
            for node in scale_test.body
            if isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "driver" for target in node.targets
            )
        )
        checkpoint = next(
            node
            for node in ast.parse(accepted).body
            if isinstance(node, ast.FunctionDef) and node.name == "_write_scale_checkpoint"
        )
        expression = next(
            node.value
            for node in storage_test.body
            if isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "driver" for target in node.targets
            )
        )
        common = eval(
            compile(ast.Expression(expression), "fixture-literal-only", "eval"),
            {"__builtins__": {}, "ast": ast, "checkpoint": checkpoint},
        )
        module = ast.parse(common)
        fixture = next(
            node
            for node in module.body
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "fixture"
        )
        for node in ast.walk(fixture):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                if node.func.id == "open_eval":
                    next(
                        k for k in node.keywords if k.arg == "execution_enabled"
                    ).value = ast.parse(
                        "kind in {'source_layout3_rescore_parent', 'legacy_cli'}", mode="eval"
                    ).body
                if node.func.id == "Budget":
                    next(k for k in node.keywords if k.arg == "concurrency").value = ast.parse(
                        "1 if kind == 'legacy_cli' else 2", mode="eval"
                    ).body
            if isinstance(node, ast.Assert) and isinstance(node.test, ast.Compare):
                if (
                    isinstance(node.test.comparators[0], ast.Constant)
                    and node.test.comparators[0].value == 27
                ):
                    node.test.comparators[0] = ast.parse("len(trial_values)", mode="eval").body
        main = next(
            node
            for node in module.body
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "main"
        )
        transaction = next(node for node in main.body if isinstance(node, ast.Try))
        transaction.body = ast.parse(
            r"""source_repo=await Repository.open(backup,readonly=True)
with closing(sqlite3.connect(backup.as_uri()+'?mode=ro',uri=True)) as db:
    db.execute('PRAGMA query_only=ON')
    source_header=json.loads(db.execute('SELECT body FROM eval_tasks WHERE id=?',
        (selected['task_id'],)).fetchone()[0])
    ref=source_header['dataset_ref']
    case_values=[json.loads(row[0]) for row in db.execute(
"""
            "        'SELECT body FROM dataset_cases WHERE name=? AND versi"
            "on=? ORDER BY case_id COLLATE BINARY LIMIT 9',"
            r"""
        (ref['name'],ref['version']))]
    if kind=='legacy_cli':case_values=case_values[:1]
    ids={c['case_id'] for c in case_values}
    trial_values=[json.loads(row[0]) for row in db.execute(
        'SELECT body FROM eval_trials WHERE task_id=? ORDER BY case_id,repetition',
        (selected['task_id'],)) if json.loads(row[0])['case_id'] in ids]
reader=EvalSourceReader(source_repo,scope='local',include_context=True)
app,flow,request=await fixture(root/'eval.sqlite',reader,case_values,trial_values,source_header)
from krim_agent.evaluation.continuous_types import ComparisonPolicy
from krim_agent.evaluation import cli,continuous_reports,reports
if kind=='source_layout3_rescore_parent':
    execute_request=ContinuousRequest.model_validate(request.model_dump(mode='json')|{
        'request_key':'direct-legacy-execute','mode':'execute','observations':[],
        'execution':Budget(repetitions=1,concurrency=2).model_dump(mode='json')})
    initial=app.owner.spawn_calls
    old_view=await flow.submit(execute_request);old_done=await app.kernel.wait(old_view['id'])
    old_report=await app.repository.report(old_view['id'])
    assert old_done['status']=='completed' and old_report['schema_version']=='safe-eval/scale/1'
    assert app.owner.spawn_calls-initial==9
    baseline=app.repository.metric_registry.calls;spawns=app.owner.spawn_calls
    old_rescore=ContinuousRequest.model_validate(execute_request.model_dump(mode='json')|{
        'request_key':'direct-legacy-rescore','mode':'rescore',
        'parent_task_id':old_view['id'],'parent_report_hash':old_report['report_hash']})
    old_child=await flow.submit(old_rescore);old_result=await app.kernel.wait(old_child['id'])
    old_child_report=await app.repository.report(old_child['id'])
    assert old_result['status']=='completed' and old_child_report['summary']['scored_cells']==36
    assert old_result['continuous']['parent']['task_id']==old_view['id']
    assert old_result['continuous']['parent']['report_hash']==old_report['report_hash']
    assert app.repository.metric_registry.calls-baseline==36 and app.owner.spawn_calls==spawns
    facts['proofs']['legacy_parent']=dict(id=old_view['id'],hash=old_report['report_hash'],
        seed_executions=9,rescore_id=old_child['id'],rescore_cells=36,rescore_execution_increment=0)
if kind=='legacy_cli':
    from krim_agent.evaluation.domain import TaskSpec
    request=TaskSpec(request_key='legacy-cli-seed',dataset=request.dataset,
        metrics=app.repository.metric_registry.specs(),execution=Budget(repetitions=1,concurrency=1))
    seed_before=app.owner.spawn_calls
    view=await app.run(request)
    facts['seed_execution_calls']=app.owner.spawn_calls-seed_before
else:view=await flow.submit(request)
done=await app.kernel.wait(view['id'])
report=await app.repository.report(view['id'])
assert done['status']=='completed'
if kind=='legacy_cli':
    assert app.owner.spawn_calls-seed_before==1
    facts['seed_execution_calls']=1
facts['Task_ids'].append(view['id']);facts['main_report_hash']=report['report_hash']
if kind=='source_layout3_rescore_parent':
    facts['measure_scale_rescore_publish']=True
    facts['complete_value_high_water']={key:0 for key in facts['complete_value_high_water']}
    baseline=app.repository.metric_registry.calls;spawns=app.owner.spawn_calls
    rescoring=ContinuousRequest.model_validate(request.model_dump(mode='json')|{
        'mode':'rescore','request_key':'direct-v2-rescore','observations':[],
        'parent_task_id':view['id'],'parent_report_hash':report['report_hash']})
    child=await flow.submit(rescoring);result=await app.kernel.wait(child['id'])
    actual=await app.repository.report(child['id'])
    assert result['status']=='completed' and actual['summary']['scored_cells']==108
"""
            "    assert actual['parent']['task_id']==view['id'] and actual["
            "'parent']['report_hash']==report['report_hash']"
            r"""
    facts['proofs']['rescore_publish_peak']=dict(facts['complete_value_high_water'])
    assert facts['complete_value_high_water']['output']>=1
"""
            "    assert all(value<=1 for value in facts['complete_value_hig"
            "h_water'].values()),facts['complete_value_high_water']"
            r"""
    assert all(all(value==0 for value in row.values()) for row in facts['outer_container_samples'])
    facts['measure_scale_rescore_publish']=False
    assert app.owner.spawn_calls==spawns and app.repository.metric_registry.calls-baseline==108
    facts['proofs']['v2_parent']=dict(id=view['id'],hash=report['report_hash'],
        child=child['id'],cells=108,target_execution_increment=0)
    facts['Task_ids'].extend([old_view['id'],old_child['id'],child['id']])
    with closing(sqlite3.connect(app.repository.path.as_uri()+'?mode=ro',uri=True)) as db:
        db.execute('PRAGMA query_only=ON')
        facts['seed_resource_receipts']=[json.loads(row[0]) for row in db.execute(
            'SELECT body FROM eval_resource_receipts WHERE task_id=?',(old_view['id'],))]
    assert len(facts['seed_resource_receipts'])==9
else:
    if kind!='legacy_cli':assert all(value==0 for value in facts['calls'].values())
    from krim_agent.evaluation.metrics import MetricRegistry
    codes[MetricRegistry.score.__code__]='cold_metric_score';facts['calls']['cold_metric_score']=0
    before_direct_calls=dict(facts['calls'])
    baseline=app.repository.metric_registry.calls
    policy=None if kind=='legacy_cli' else ComparisonPolicy(
        baseline_id=view['id'],candidate_id=view['id'],baseline_hash=report['report_hash'],
        candidate_hash=report['report_hash'],allowed_subjects=[report['subject_fingerprint']])
    page_reads=[]
    def observe_SQL(conn,cursor,statement,parameters,context,many):
        if 'eval_report_pages' in statement and statement.lstrip().upper().startswith('SELECT'):
            page_reads.append(dict(statement=statement,parameters=parameters))
    if kind=='bare_root_not_gate':
        assert report['schema_version']=='safe-eval/scale/2'
        assert reports.gate_reports(report,report)['status']=='invalid'
        invalid=continuous_reports.compare(policy,report,report)
"""
            "        assert invalid['status']=='invalid' and invalid['reaso"
            "ns']==[{'code':'report_pages_required'}]"
            r"""
        event.listen(app.repository.engine.sync_engine,'before_cursor_execute',observe_SQL)
        try:result=await app.repository.compare_paged('local',policy)
        finally:event.remove(app.repository.engine.sync_engine,'before_cursor_execute',observe_SQL)
        assert result['status']=='pass' and page_reads
        facts['proofs']['bare_root_rejected_verified_pages_accepted']=result
    else:
        await app.close()
        from sqlalchemy.engine import Engine
        event.listen(Engine,'before_cursor_execute',observe_SQL)
        try:
            async def run(arguments):
                args=cli.parser().parse_args(['--root',str(root),*arguments])
                return await cli.execute(args)
            if kind=='legacy_cli':
                assert done.get('layout',2)==2 and 'items' in done
                assert await run(['task','get',view['id']])==done
                assert (await run(['task','by-key',request.request_key]))['id']==view['id']
                assert await run(['report',view['id']])==report
                assert (await run(['compare',view['id'],view['id']]))['status']=='pass'
                facts['proofs']['legacy_shape_hash']=report['report_hash']
                history=json.loads(sys.argv[5])
                assert len(history)==2 and [item['version'] for item in history]==[1,2]
                facts['proofs']['historical_schemas']=[]
                for item in history:
                    historical_root=Path(item['root']);database=historical_root/'eval.sqlite'
                    before_hash=hashlib.sha256(database.read_bytes()).hexdigest()
                    assert before_hash==item['sha256']
                    with closing(sqlite3.connect(database.as_uri()+'?mode=ro',uri=True)) as db:
                        db.execute('PRAGMA query_only=ON')
                        assert db.execute('PRAGMA user_version').fetchone()[0]==item['version']
"""
            '                        names={r[0] for r in db.execute("SELEC'
            "T name FROM sqlite_master WHERE type='table'\")}"
            r"""
                    assert not {'dataset_cases','eval_report_pages','eval_resource_receipts'}&names
                    cold_before=dict(facts['calls']);opened=[];original_open=cli.open_eval
                    async def tracked_open(*args,**kwargs):
"""
            "                        actual=await original_open(*args,**kwa"
            "rgs);opened.append(actual);return actual"
            r"""
                    cli.open_eval=tracked_open
                    async def historical(arguments,where=historical_root):
"""
            "                        return await cli.execute(cli.parser()."
            "parse_args(['--root',str(where),*arguments]))"
            r"""
                    try:
                        old_facts=item['facts'];identity=old_facts['id']
                        old_view=await historical(['task','get',identity])
                        assert old_view['id']==identity
"""
            "                        assert (await historical(['task','by-k"
            "ey',old_facts['request']['request_key']]))['id']==identity"
            r"""
                        cold_report=await historical(['report',identity])
                        assert cold_report==old_facts['report']
                        assert cold_report['report_hash']==old_facts['report']['report_hash']
                        assert (await historical(['compare',identity,identity]))['status']=='pass'
                        assert all(actual.repository.closed and actual.repository._writer_fd is None
                            and actual.owner is None and actual.adapter is None
                            and actual.repository.metric_registry.calls==0 for actual in opened)
                        assert facts['calls']==cold_before
                        with closing(sqlite3.connect(database.as_uri()+'?mode=ro',uri=True)) as db:
                            db.execute('PRAGMA query_only=ON')
                            assert db.execute('PRAGMA user_version').fetchone()[0]==item['version']
"""
            '                            assert {r[0] for r in db.execute("'
            "SELECT name FROM sqlite_master WHERE type='table'\") }==names"
            r"""
                        assert hashlib.sha256(database.read_bytes()).hexdigest()==before_hash
                        # 负例是本轮健康种库的明确新副本，原冷读库bytes不变；不是历史失败库。
                        negative_root=Path(item['negative_root']);negative=negative_root/'eval.sqlite'
                        assert hashlib.sha256(negative.read_bytes()).hexdigest()==before_hash
                        with closing(sqlite3.connect(negative)) as db:
                            db.execute('UPDATE eval_tasks SET body=? WHERE id=?',
                                (json.dumps({'unauthorized_corrupt_body':True}),identity))
                            db.execute('UPDATE eval_reports SET body=? WHERE task_id=?',
                                (json.dumps({'unauthorized_corrupt_report':True}),identity))
                            db.commit()
                        body_reads=[]
                        def unauthorized_SQL(conn,cursor,statement,parameters,context,many):
                            if 'eval_tasks.body' in statement or 'eval_reports.body' in statement:
                                body_reads.append(dict(statement=statement,parameters=parameters))
                        event.listen(Engine,'before_cursor_execute',unauthorized_SQL)
                        try:
"""
            "                            for arguments in [['task','get',id"
            "entity],['report',identity],['compare',identity,identity]]:"
            r"""
                                body_reads.clear();caught=None;exception_type=None
                                try:await historical(arguments+['--scope','other'],negative_root)
"""
            "                                except Exception as error:caug"
            "ht=getattr(error,'code',None);exception_type=type(error).__nam"
            "e__"
            r"""
                                assert not body_reads,body_reads
                                assert caught=='not_found',(caught,exception_type)
                        finally:event.remove(Engine,'before_cursor_execute',unauthorized_SQL)
                        assert facts['calls']==cold_before
"""
            "                        assert all(actual.repository.closed an"
            "d actual.repository.metric_registry.calls==0 for actual in ope"
            "ned)"
            r"""
                        facts['proofs']['historical_schemas'].append(dict(version=item['version'],database=str(database),
                            source=old_facts['source'],Task_id=identity,report_hash=cold_report['report_hash'],
                            hash_before=before_hash,hash_after=hashlib.sha256(database.read_bytes()).hexdigest(),
"""
            "                            seed_execute_calls=old_facts['exec"
            "ute_calls'],cold_call_increments={k:0 for k in cold_before},"
            r"""
                            readonly_resources=[dict(closed=a.repository.closed,writer_fd=a.repository._writer_fd,
                                metric_calls=a.repository.metric_registry.calls) for a in opened],
                            negative_database=str(negative),unauthorized_body_reads=0))
                    finally:cli.open_eval=original_open
            elif kind=='cli_task_pages':
                for projection,expected in [('cases',9),('trials',27)]:
                    rows=[];cursor=None
                    while True:
                        argv=['task','get',view['id'],'--'+projection,'--limit','2']
                        if cursor is not None:argv+=['--cursor',cursor]
                        page=await run(argv);rows.extend(page['rows']);cursor=page['next_cursor']
                        if cursor is None:break
                    assert len(rows)==expected
"""
            "                    facts['proofs'][projection]=dict(rows=len("
            "rows),keys=[r['case_id'] for r in rows])"
            r"""
"""
            "                one=await run(['task','get',view['id'],'--case"
            "-id',case_values[0]['case_id'],'--repetition','0'])"
            r"""
                assert one['case_id']==case_values[0]['case_id'] and one['output'] is not None
                try:await run(['task','get',view['id'],'--cases','--scope','other'])
                except ServiceError as error:assert error.code=='not_found'
                else:raise AssertionError('cross_scope_cli_accepted')
                try:await run(['task','get',view['id'],'--cursor','bad'])
                except ValueError as error:assert str(error)=='page_projection_required'
                else:raise AssertionError('cursor_without_projection_accepted')
            elif kind=='cli_report_pages':
                rows=[];cursor=None
                while True:
                    argv=['report',view['id'],'--pages','--limit','16']
                    if cursor is not None:argv+=['--cursor',cursor]
                    page=await run(argv);rows.extend(page['rows']);cursor=page['next_cursor']
                    if cursor is None:break
                assert len(rows)==27 and {(r['case_id'],r['repetition']) for r in rows}=={
                    (c['case_id'],r) for c in case_values for r in range(3)}
                assert page_reads
                facts['proofs']['report_matrix_rows']=27
            elif kind=='cli_compare_pages':
                result=await run(['compare',view['id'],view['id'],'--pages'])
                assert result['status']=='pass' and page_reads
                facts['proofs']['generic_compare_verified_pages']=result
            elif kind=='cli_continuous_compare_pages':
                file=root/'comparison-policy.json';file.write_text(json.dumps(policy.model_dump(mode='json')))
                result=await run(['continuous','compare',str(file),'--pages'])
                assert result['status']=='pass' and page_reads
                invalid=policy.model_copy(update={'allowed_subjects':['0'*64]})
                file.write_text(json.dumps(invalid.model_dump(mode='json')))
                denied=await run(['continuous','compare',str(file),'--pages'])
"""
            "                assert denied['status']=='invalid' and denied["
            "'reasons']==[{'code':'unapproved_subject'}]"
            r"""
                facts['proofs']['explicit_policy']=dict(accepted=result,rejected=denied)
            else:raise AssertionError('unimplemented_direct_scenario')
            if kind in {'cli_task_pages','cli_report_pages','cli_compare_pages'}:
                # 新授权负例只损坏本参数的目的库；不动任何历史输入/失败库。
                with closing(sqlite3.connect(app.repository.path)) as db:
                    db.execute('UPDATE eval_tasks SET body=? WHERE id=?',
                        (json.dumps({'unauthorized_corrupt_body':True}),view['id']))
                    db.execute('UPDATE eval_reports SET body=? WHERE task_id=?',
                        (json.dumps({'unauthorized_corrupt_report':True}),view['id']))
                    db.commit()
                if kind=='cli_task_pages':
                    negatives=[['task','get',view['id']],['task','get',view['id'],'--cases'],
                        ['task','get',view['id'],'--trials'],
                        ['task','get',view['id'],'--case-id',case_values[0]['case_id'],'--repetition','0']]
"""
            "                elif kind=='cli_report_pages':negatives=[['rep"
            "ort',view['id']],['report',view['id'],'--pages']]"
            r"""
                else:negatives=[['compare',view['id'],view['id'],'--pages'],['compare',view['id'],view['id']]]
                unauthorized=[]
                def body_SQL(conn,cursor,statement,parameters,context,many):
                    if 'eval_tasks.body' in statement or 'eval_reports.body' in statement:
                        unauthorized.append(dict(statement=statement,parameters=parameters))
                event.listen(Engine,'before_cursor_execute',body_SQL)
                try:
                    for arguments in negatives:
                        unauthorized.clear();caught=None;exception_type=None
                        try:await run(arguments+['--scope','other'])
                        except Exception as error:
                            caught=getattr(error,'code',None);exception_type=type(error).__name__
                        facts['proofs'].setdefault('unauthorized_reads',[]).append(dict(
                            argv=arguments+['--scope','other'],code=caught,exception_type=exception_type,body_queries=list(unauthorized)))
                        assert not unauthorized,('unauthorized_body_read',unauthorized)
                        assert caught=='not_found',caught
                finally:event.remove(Engine,'before_cursor_execute',body_SQL)
        finally:event.remove(Engine,'before_cursor_execute',observe_SQL)
    facts['proofs']['actual_page_reads']=len(page_reads)
    assert app.repository.metric_registry.calls==baseline
    assert facts['calls']==before_direct_calls
    facts['proofs']['direct_execution_increments']={key:0 for key in before_direct_calls}
facts['stage']='completed'
"""
        ).body
        facts_assignment = next(
            node
            for node in module.body
            if isinstance(node, ast.Assign)
            and any(isinstance(name, ast.Name) and name.id == "facts" for name in node.targets)
        )
        next(k for k in facts_assignment.value.keywords if k.arg == "node").value = ast.parse(
            "'tests/safe_eval/test_eval_scale.py::test_scale_direct_consumers[' + kind + ']'",
            mode="eval",
        ).body
        driver = ast.unparse(ast.fix_missing_locations(module))
        compile(driver, "completion20-direct-consumer", "exec")
        inputs = json.loads(
            await asyncio.to_thread(
                (
                    Path("/artifacts/krim-agent-core-20261019")
                    / "vm-pn3-scale-implementation-20261023T152826061485"
                    / "static-supplement-03/completion20-child/APPROVED-CONTRACT.json"
                ).read_text
            )
        )["readonly_existing_inputs"]
        source = (await asyncio.to_thread(Path(__file__).resolve)).parents[2] / "src"
        history_records = []
        if scenario == "legacy_cli":
            import shutil

            from test_continuous_eval import DATABASES
            from test_eval_scale_storage import seed_history

            for version in [1, 2]:
                historical_root = tmp_path / ("historical-schema-" + str(version))
                await asyncio.to_thread(historical_root.mkdir)
                database, history_facts = await asyncio.to_thread(
                    seed_history, historical_root, version
                )
                target = historical_root / "eval.sqlite"
                await asyncio.to_thread(database.rename, target)
                DATABASES.append(target)
                negative_root = tmp_path / ("historical-negative-" + str(version))
                await asyncio.to_thread(negative_root.mkdir)
                negative = negative_root / "eval.sqlite"
                await asyncio.to_thread(shutil.copyfile, target, negative)
                DATABASES.append(negative)
                history_records.append(
                    {
                        "version": version,
                        "root": str(historical_root),
                        "negative_root": str(negative_root),
                        "facts": history_facts,
                        "sha256": hashlib.sha256(
                            await asyncio.to_thread(target.read_bytes)
                        ).hexdigest(),
                    }
                )
        argv = [
            sys.executable,
            "-I",
            "-B",
            "-c",
            driver,
            str(source),
            str(tmp_path),
            json.dumps(inputs),
            scenario,
            json.dumps(history_records),
        ]
        process, receipt, facts = await _run_scale_evidence_process(
            argv,
            tmp_path,
            execution_timeout_seconds=60,
            source_database=inputs["main_300_source"]["backup"]["path"],
            destination_database=str(tmp_path / "eval.sqlite"),
        )
        assert receipt["wait_returncode"] == process.returncode == 0, facts["error"]
        assert facts["stage"] == "completed"
        assert receipt["stdout_eof"] and receipt["stderr_eof"]
        return
    import os
    import signal
    import stat
    import sys

    assert scenario == "source_layout3_cold"
    contract_path = (
        Path("/artifacts/krim-agent-core-20261019")
        / "vm-pn3-scale-implementation-20261023T152826061485"
        / "static-supplement-03/scoring-source10/APPROVED-CONTRACT.json"
    )
    contract = json.loads(contract_path.read_text())
    selected = contract["readonly_existing_inputs"]["main_300_source"]
    backup = Path(selected["backup"]["path"])
    expected_backup = selected["backup"]["sha256"]
    for path in [backup, *backup.parents]:
        assert not path.is_symlink()
    known = await asyncio.to_thread(backup.lstat)
    assert stat.S_ISREG(known.st_mode) and known.st_nlink == 1
    assert hashlib.sha256(await asyncio.to_thread(backup.read_bytes)).hexdigest() == expected_backup
    facts_path = tmp_path / "source10-consumer.json"
    driver = r"""import asyncio,collections,hashlib,json,os,sqlite3,stat,sys,threading,traceback
from contextlib import closing
from pathlib import Path
source,backup,destination=map(Path,sys.argv[1:4])
expected_hash,task_id,report_hash=sys.argv[4:7]
sys.path.insert(0,str(source))
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.pool import Pool
from krim_agent.evaluation.repository import Repository
from krim_agent.evaluation.sources import EvalSourceReader
from krim_agent.evaluation import sources as source_module
from krim_agent.evaluation.domain import neutral_fingerprint
from krim_agent.evaluation.metrics import MetricRegistry
from krim_agent.evaluation.adapters.local_process import LocalProcessAdapter
case_id,repetition='scale-017',2
for p in [backup,*backup.parents]:
    assert not p.is_symlink()
st=backup.lstat()
assert stat.S_ISREG(st.st_mode) and st.st_nlink==1
before_hash=hashlib.sha256(backup.read_bytes()).hexdigest()
assert before_hash==expected_hash
uri=backup.as_uri()+'?mode=ro'
with closing(sqlite3.connect(uri,uri=True)) as db:
    db.execute('PRAGMA query_only=ON')
    assert db.execute('PRAGMA query_only').fetchone()[0]==1
    snapshot=json.loads(db.execute('SELECT body FROM eval_tasks WHERE id=? AND scope=?',
        (task_id,'local')).fetchone()[0])
    expected_trial=json.loads(db.execute(
        'SELECT body FROM eval_trials WHERE task_id=? AND case_id=? AND repetition=?',
        (task_id,case_id,repetition)).fetchone()[0])
    expected_output=json.loads(db.execute('SELECT body FROM eval_evidence WHERE digest=?',
        (expected_trial['output_digest'],)).fetchone()[0])
    expected_case=json.loads(db.execute(
        'SELECT body FROM dataset_cases WHERE name=? AND version=? AND case_id=?',
        (snapshot['dataset_ref']['name'],snapshot['dataset_ref']['version'],case_id)).fetchone()[0])
    root=json.loads(db.execute('SELECT body FROM eval_reports WHERE task_id=?',
        (task_id,)).fetchone()[0])
    marker=json.loads(db.execute('SELECT body FROM eval_meta WHERE id=1').fetchone()[0])
    assert db.execute('SELECT count(*) FROM eval_trials WHERE task_id=?',
        (task_id,)).fetchone()[0]==300
assert snapshot['layout']==3 and snapshot['status']=='completed'
assert root['report_hash']==report_hash
assert neutral_fingerprint(expected_output)==expected_trial['output_digest']
selected_page=next(
    p for p in root['pages']
    if tuple(p['first']) <= (case_id,repetition) <= tuple(p['last']))
facts=dict(node='tests/safe_eval/test_eval_scale.py::test_scale_direct_consumers[source_layout3_cold]',
    source_root=str(source),database=str(backup),URI=uri,uri_true=True,query_only=True,
    baseline_sqlite_closed=True,input_hash_before=before_hash,task_id=task_id,
    case_id=case_id,repetition=repetition,expected_report_hash=report_hash,
    expected_page=selected_page,expected_owner=marker['repository_id'],
    expected_output_hash=expected_trial['output_digest'],expected_revision=snapshot['revision'],
    expected_request_hash=snapshot['request_hash'],connections=[],sql=[],query_only_observations=[],
    calls={},error=None,value=None,unexpected=[])
codes={
    getattr(LocalProcessAdapter,n).__code__:'target_'+n
    for n in ['prepare','execute','collect','cancel']}
codes[MetricRegistry.score.__code__]='scorer'
codes[Repository.adapter.__code__]='adapter_lookup'
codes[asyncio.create_subprocess_exec.__code__]='subprocess'
facts['calls']={name:0 for name in codes.values()}
facts['calls']['provider_stream']=0
def profile(frame,kind,value):
    if kind=='call':
        name=codes.get(frame.f_code)
        if name is not None:facts['calls'][name]+=1
        if (
            frame.f_globals.get('__name__','').startswith('krim_agent.providers.')
            and frame.f_code.co_name=='stream'
        ):
            facts['calls']['provider_stream']+=1
def connected(dbapi,*args):
    cur=dbapi.cursor()
    try:cur.execute('PRAGMA query_only=ON')
    finally:cur.close()
def checked_out(dbapi,*args):
    cur=dbapi.cursor()
    try:
        cur.execute('PRAGMA query_only')
        facts['query_only_observations'].append(cur.fetchone()[0])
    finally:cur.close()
def sql(conn,cursor,statement,parameters,context,many):
    facts['sql'].append(dict(statement=statement,parameters=parameters))
listeners=[]
for name in ['connect','checkout','reset','checkin','close']:
    def observe(*args,name=name):
        facts['connections'].append(dict(event=name,connection=id(args[0])))
    event.listen(Pool,name,observe)
    listeners.append((name,observe))
event.listen(Pool,'connect',connected)
event.listen(Pool,'checkout',checked_out)
event.listen(Engine,'before_cursor_execute',sql)
async def main():
    repo=None
    before=asyncio.all_tasks()
    facts['fd_before']=len(os.listdir('/proc/self/fd'))
    facts['threads_before']=[(t.ident,t.name) for t in threading.enumerate()]
    loop=asyncio.get_running_loop()
    old_handler=loop.get_exception_handler()
    loop.set_exception_handler(lambda current,context:facts['unexpected'].append(str(context)))
    previous=sys.getprofile()
    sys.setprofile(profile)
    try:
        repo=await Repository.open(backup,readonly=True)
        reader=EvalSourceReader(repo,scope='local',include_context=True)
        facts['reader_file']=str(Path(source_module.__file__))
        facts['reader_file_sha256']=hashlib.sha256(Path(source_module.__file__).read_bytes()).hexdigest()
        assert Path(source_module.__file__).is_relative_to(source)
        facts['read_start_index']=len(facts['sql'])
        facts['value']=await reader.read('local',dict(task_id=task_id,case_id=case_id,
            repetition=repetition,revision=snapshot['revision'],evidence_hash=expected_trial['output_digest']))
        value=facts['value']
        assert value['output']==expected_output and value['case']==expected_case
        assert value['target']==snapshot['target'] and value['environment']==snapshot['environment']
        assert value['terminal'] is True
        origin=value['source']
        for key,expected in dict(repository_id=marker['repository_id'],task_id=task_id,
            case_id=case_id,repetition=repetition,revision=snapshot['revision'],
            request_hash=snapshot['request_hash'],execution_ref=expected_trial['execution_ref'],
            evidence_hash=expected_trial['output_digest'],report_hash=report_hash,status='completed',
            code=None,item_status='finished',item_quality='passed',item_code=None,
            reader_version='cold-eval/scale/1',reader_digest=facts['reader_file_sha256']).items():
            assert origin[key]==expected,(key,origin[key],expected)
        assert all(v==0 for v in facts['calls'].values()),facts['calls']
        first_read=facts['sql'][facts['read_start_index']]
        assert (
            'eval_tasks.scope' in first_read['statement']
            and 'eval_tasks.body' not in first_read['statement']
        )
        case_reads=[r for r in facts['sql'] if 'FROM dataset_cases' in r['statement']]
        page_reads=[r for r in facts['sql'] if 'FROM eval_report_pages' in r['statement']]
        assert case_reads and page_reads
        assert all(case_id in r['parameters'] for r in case_reads)
        assert all(r['parameters'][-1]==selected_page['index'] for r in page_reads)
        assert len({tuple(r['parameters']) for r in page_reads})==1
        assert not any('FROM dataset_revisions' in r['statement'] for r in facts['sql'])
        facts['case_reads']=case_reads
        facts['report_page_reads']=page_reads
        facts['expected_value_verified']=True
    except BaseException as error:
        facts['error']=dict(type=type(error).__name__,message=str(error),
            frames=[
                dict(file=t.filename,function=t.name,line=t.lineno)
                for t in traceback.extract_tb(error.__traceback__)])
        raise
    finally:
        try:
            if repo is not None:await repo.close()
        finally:
            sys.setprofile(previous)
            await asyncio.sleep(0)
            facts['repository_closed']=repo is not None and repo.closed
            facts['writer_fd']=None if repo is None else repo._writer_fd
            facts['pending']=[
                repr(t) for t in asyncio.all_tasks()
                if t not in before and not t.done()]
            facts['fd_after']=len(os.listdir('/proc/self/fd'))
            facts['threads_after']=[(t.ident,t.name) for t in threading.enumerate()]
            facts['input_hash_after']=hashlib.sha256(backup.read_bytes()).hexdigest()
            loop.set_exception_handler(old_handler)
            event.remove(Engine,'before_cursor_execute',sql)
            event.remove(Pool,'checkout',checked_out)
            event.remove(Pool,'connect',connected)
            for name,handler in listeners:event.remove(Pool,name,handler)
            destination.write_text(json.dumps(facts,indent=2))
            assert facts['input_hash_after']==facts['input_hash_before']
            assert facts['repository_closed'] and facts['writer_fd'] is None
            assert not facts['pending'] and not facts['unexpected']
            assert facts['fd_after']<=facts['fd_before']
            counts=collections.Counter(r['event'] for r in facts['connections'])
            assert counts['connect']==counts['close']
            assert counts['checkout']==counts['reset']==counts['checkin']
            assert (
                facts['query_only_observations']
                and all(v==1 for v in facts['query_only_observations'])
            )
asyncio.run(main())
"""
    compile(driver, "source10-consumer", "exec")
    argv = [
        sys.executable,
        "-I",
        "-B",
        "-c",
        driver,
        str((await asyncio.to_thread(Path(__file__).resolve)).parents[2] / "src"),
        str(backup),
        str(facts_path),
        expected_backup,
        selected["task_id"],
        selected["report_hash"],
    ]
    proc = await asyncio.create_subprocess_exec(
        *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    pidfd = os.pidfd_open(proc.pid)
    try:
        try:
            async with asyncio.timeout(60):
                stdout, stderr = await proc.communicate()
        finally:
            if proc.returncode is None:
                signal.pidfd_send_signal(pidfd, signal.SIGTERM)
                await proc.communicate()
        wait_code = await proc.wait()
    finally:
        os.close(pidfd)
    (tmp_path / "source10-stdout.txt").write_bytes(stdout)
    (tmp_path / "source10-stderr.txt").write_bytes(stderr)
    facts = json.loads(facts_path.read_text())
    process = {
        "argv": argv,
        "cwd": os.getcwd(),
        "pid": proc.pid,
        "uid": os.getuid(),
        "wait_returncode": wait_code,
        "returncode": proc.returncode,
        "pidfd_opened": True,
        "pidfd_closed": True,
        "stdout_eof": proc.stdout.at_eof(),
        "stderr_eof": proc.stderr.at_eof(),
        "driver_sha256": hashlib.sha256(driver.encode()).hexdigest(),
        "database": str(backup),
        "facts_file": str(facts_path),
    }
    (tmp_path / "source10-process.json").write_text(json.dumps(process, indent=2))
    assert hashlib.sha256(await asyncio.to_thread(backup.read_bytes)).hexdigest() == expected_backup
    assert wait_code == proc.returncode == 0, facts["error"]
    assert facts["expected_value_verified"]
    assert facts["repository_closed"] and facts["writer_fd"] is None
    assert all(value == 0 for value in facts["calls"].values())
    assert process["stdout_eof"] and process["stderr_eof"]


@pytest.mark.parametrize("mode", ["observed_score", "rescore", "schedule"])
async def test_shared_modes_scale(tmp_path, mode):
    if mode in ["rescore", "schedule"]:
        import ast
        import sys

        source_tree = ast.parse(await asyncio.to_thread(Path(__file__).read_text))
        function = next(
            node
            for node in source_tree.body
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "test_shared_modes_scale"
        )
        accepted_driver = next(
            node.value.value
            for node in function.body
            if isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "driver" for target in node.targets
            )
        )
        checkpoint = next(
            node
            for node in ast.parse(accepted_driver).body
            if isinstance(node, ast.FunctionDef) and node.name == "_write_scale_checkpoint"
        )
        contract_path = (
            Path("/artifacts/krim-agent-core-20261019")
            / "vm-pn3-scale-implementation-20261023T152826061485"
            / "static-supplement-03/completion20-child/APPROVED-CONTRACT.json"
        )
        selected = json.loads(await asyncio.to_thread(contract_path.read_text))[
            "readonly_existing_inputs"
        ]["main_300_source"]
        driver = (
            "import asyncio,collections,fcntl,hashlib,json,os,sqlite3,stat,sys,"
            "threading,time,traceback\n"
            "from contextlib import closing\nfrom pathlib import Path\n"
            "source,root=map(Path,sys.argv[1:3]);selected=json.loads(sys.argv[3]);mode=sys.argv[4]\n"
            + ast.unparse(checkpoint)
            + "\n"
            + r"""_write_scale_checkpoint('bootstrap','start')
sys.path.insert(0,str(source))
from sqlalchemy import event
from sqlalchemy.pool import Pool
from krim_agent.contracts import ServiceError
from krim_agent.evaluation.app import open_eval
from krim_agent.evaluation.repository import Repository
from krim_agent.evaluation.sources import EvalSourceReader
from krim_agent.evaluation.adapters.local_process import LocalProcessAdapter
from krim_agent.evaluation.domain import Budget,CaseSpec,DatasetRef,neutral_fingerprint
from krim_agent.evaluation.continuous_types import (
    ConfirmationRequest,ContinuousRequest,FreezeConfirmed,ImportRequest,ScheduleSpec,
    ScoreSource,SourceSelection,
)
from krim_agent.evaluation.continuous_metrics import continuous_rules
backup=Path(selected['backup']['path']);destination=root/'header12-destination.sqlite'
for path in [backup,*backup.parents]:
    assert not path.is_symlink()
known=backup.lstat()
assert stat.S_ISREG(known.st_mode) and known.st_nlink==1
before_hash=hashlib.sha256(backup.read_bytes()).hexdigest()
assert before_hash==selected['backup']['sha256']
with closing(sqlite3.connect(backup.as_uri()+'?mode=ro',uri=True)) as db:
    db.execute('PRAGMA query_only=ON')
    assert db.execute('PRAGMA query_only').fetchone()[0]==1
    source_header=json.loads(db.execute('SELECT body FROM eval_tasks WHERE id=? AND scope=?',
        (selected['task_id'],'local')).fetchone()[0])
    source_report=json.loads(db.execute('SELECT body FROM eval_reports WHERE task_id=?',
        (selected['task_id'],)).fetchone()[0])
    ref=source_header['dataset_ref']
    cases=[json.loads(row[0]) for row in db.execute(
        'SELECT body FROM dataset_cases WHERE name=? AND version=? ORDER BY case_id COLLATE BINARY',
        (ref['name'],ref['version']))]
    trials=[json.loads(row[0]) for row in db.execute(
        'SELECT body FROM eval_trials WHERE task_id=? ORDER BY case_id COLLATE BINARY,repetition',
        (selected['task_id'],))]
assert source_header['status']=='completed' and len(cases)==100 and len(trials)==300
assert source_report['report_hash']==selected['report_hash']==source_header['report_hash']
facts=dict(node='tests/safe_eval/test_eval_scale.py::test_shared_modes_scale['+mode+']',
    mode=mode,uid=os.getuid(),source_database=str(backup),source_hash_before=before_hash,
    source_task_id=selected['task_id'],source_report_hash=selected['report_hash'],
    destination_database=str(destination),stage='bootstrap',error=None,close_errors=[],
    audit_errors=[],connections=[],source_query_only=[],calls={},phases=[],
    parent=None,result=None,source_execution_calls_this_command=0)
codes={getattr(LocalProcessAdapter,name).__code__:'target_'+name
       for name in ['prepare','execute','collect','cancel']}
codes[Repository.adapter.__code__]='adapter_lookup'
facts['calls']={name:0 for name in codes.values()}
facts['calls']['provider_stream']=0
def profile(frame,kind,value):
    if kind=='call':
        label=codes.get(frame.f_code)
        if label is not None:facts['calls'][label]+=1
"""
            "        if frame.f_globals.get('__name__','').startswith('krim"
            "_agent.providers.') and frame.f_code.co_name=='stream':"
            r"""
            facts['calls']['provider_stream']+=1
def connected(dbapi,record):
    cur=dbapi.cursor()
    try:
        cur.execute('PRAGMA database_list');name=cur.fetchone()[2]
        record.info['completion20_role']='source' if name==str(backup) else 'destination'
        if name==str(backup):cur.execute('PRAGMA query_only=ON')
    finally:cur.close()
def checkout(dbapi,record,*args):
    if record.info.get('completion20_role')=='source':
        cur=dbapi.cursor()
        try:
            cur.execute('PRAGMA query_only');facts['source_query_only'].append(cur.fetchone()[0])
        finally:cur.close()
event.listen(Pool,'connect',connected);event.listen(Pool,'checkout',checkout)
listeners=[]
for name in ['connect','checkout','reset','checkin','close']:
    def observed(*args,name=name):
        facts['connections'].append(dict(event=name,connection=id(args[0]),
            role=args[1].info.get('completion20_role'),at=time.monotonic()))
    event.listen(Pool,name,observed);listeners.append((name,observed))
_write_scale_checkpoint('bootstrap','done')
async def main():
    app=source_repo=None
    before=asyncio.all_tasks();facts['fd_before']=len(os.listdir('/proc/self/fd'))
    facts['threads_before']=[(t.ident,t.name) for t in threading.enumerate()]
    loop=asyncio.get_running_loop();old_handler=loop.get_exception_handler()
    facts['unexpected']=[]
    loop.set_exception_handler(lambda current,context:facts['unexpected'].append(str(context)))
    previous=sys.getprofile();sys.setprofile(profile)
    clock_value=[100000.0]
    clock=lambda:clock_value[0]
    try:
        facts['stage']='open';_write_scale_checkpoint('open','start')
        source_repo=await Repository.open(backup,readonly=True)
        reader=EvalSourceReader(source_repo,scope='local',include_context=True)
        app=await open_eval(destination,execution_enabled=False,
            sources={'accepted-scale':reader},clock=clock)
        assert app.adapter is None and app.owner is None and app.kernel.repository is app.repository
        _write_scale_checkpoint('open','done')
        flow=app.continuous;flow.redaction['artifact_paths']=['ledger.json','summary.md']
        selected_scores=[];drafts={}
        facts['stage']='import';_write_scale_checkpoint('import','start')
        for offset in range(0,300,32):
            batch=trials[offset:offset+32]
"""
            "            rows=await flow.import_sources(ImportRequest(reque"
            "st_key='completion20-import-'+str(offset),"
            r"""
                source_alias='accepted-scale',sources=[SourceSelection(task_id=selected['task_id'],
                case_id=row['case_id'],repetition=row['repetition'],revision=source_header['revision'],
                evidence_hash=row['output_digest']) for row in batch]))
            assert len(rows)==len(batch)
            for row in rows:
                assert row['eligibility']=='eligible' and row['source_proof']['complete']
                selected_scores.append(ScoreSource(case_id=row['source']['case_id'],
                    repetition=row['source']['repetition'],observation_id=row['id'],
                    observation_hash=row['content_hash']))
                if row['source']['repetition']==0:drafts[row['source']['case_id']]=row['id']
            del rows
        assert len(selected_scores)==300 and len(drafts)==100
        _write_scale_checkpoint('import','done',sources=300)
        facts['stage']='confirm';_write_scale_checkpoint('confirm','start')
        confirmations=[]
        for case in cases:
            draft=await app.repository.candidate('local',drafts[case['case_id']])
"""
            "            value=await flow.confirm_case(ConfirmationRequest("
            "request_key='completion20-confirm-'+case['case_id'],"
            r"""
                candidate_id=draft['candidate_id'],expected_revision=draft['revision'],
                expected_hash=draft['hash'],case=CaseSpec.model_validate(case),context='complete',
                reason='固定真实来源及未改gold，独立确认本fixture。'))
            assert value['status']=='confirmed'
            confirmations.append(value['id'])
        frozen=await flow.freeze_dataset(FreezeConfirmed(request_key='completion20-freeze',
            name='completion20-'+mode,version='1',confirmations=confirmations))
        _write_scale_checkpoint('confirm','done',cases=100)
        rules=continuous_rules(app.repository.metric_registry)
        budget=Budget(repetitions=3,concurrency=2)
        assert budget.batch_timeout_seconds==120 and budget.item_timeout_seconds==20
        assert budget.scorer_timeout_seconds==1 and len(rules)==4
        observed_request=ContinuousRequest(request_key='completion20-parent',mode='observed_score',
            dataset=DatasetRef.model_validate(frozen['dataset_ref']),
            dataset_envelope_fp=frozen['dataset_envelope_fp'],metrics=rules,
            target_key=source_header['target']['key'],execution=budget,observations=selected_scores)
        assert all(value==0 for value in facts['calls'].values())
        if mode=='rescore':
            facts['stage']='parent';_write_scale_checkpoint('parent','start')
            started_count=app.repository.metric_registry.calls
            parent_view=await flow.submit(observed_request)
            parent_done=await app.kernel.wait(parent_view['id'])
            parent_report=await app.repository.report(parent_view['id'])
"""
            "            assert parent_done['status']=='completed' and pare"
            "nt_report['summary']['scored_cells']==1200"
            r"""
            parent_cells=app.repository.metric_registry.calls-started_count
            assert parent_cells==1200
            assert (await flow.submit(observed_request))['id']==parent_view['id']
            assert app.repository.metric_registry.calls-started_count==1200
            facts['parent']=dict(id=parent_view['id'],report_hash=parent_report['report_hash'],
                score_cells=parent_cells,planned_trials=parent_done['planned_count'],
                database=str(destination),kind=parent_report['kind'])
            _write_scale_checkpoint('parent','done',task_id=parent_view['id'],score_cells=1200)
            request=ContinuousRequest(request_key='completion20-rescore',mode='rescore',
                dataset=observed_request.dataset,dataset_envelope_fp=observed_request.dataset_envelope_fp,
                target_key=observed_request.target_key,metrics=rules,execution=budget,
                parent_task_id=parent_view['id'],parent_report_hash=parent_report['report_hash'])
            baseline_cells=app.repository.metric_registry.calls
            facts['stage']='submit';_write_scale_checkpoint('submit','start')
            view=await flow.submit(request)
        else:
            spec=ScheduleSpec(schedule_id='completion20-scoring-window',version='1',
                template=observed_request,epoch=100000,interval_seconds=60,max_windows=1)
            facts['stage']='schedule';_write_scale_checkpoint('schedule','start')
            schedule=await flow.create_schedule(spec)
            assert (await flow.create_schedule(spec))['spec_hash']==schedule['spec_hash']
            facts['schedule']=schedule
            _write_scale_checkpoint('schedule','done')
            baseline_cells=app.repository.metric_registry.calls
            assert baseline_cells==0
            facts['stage']='submit';_write_scale_checkpoint('submit','start')
            view=await flow.tick(spec.schedule_id)
            assert (await flow.tick(spec.schedule_id))['id']==view['id']
            request=None
        facts['task_id']=view['id'];_write_scale_checkpoint('submit','done',task_id=view['id'])
        facts['stage']='wait';_write_scale_checkpoint('wait','start',task_id=view['id'])
        done=await app.kernel.wait(view['id']);report=await app.repository.report(view['id'])
        assert done['status']=='completed' and done['planned_count']==300
        assert report['summary']['planned_cells']==report['summary']['scored_cells']==1200
        assert report['summary']['passed']==300 and report['summary']['token_usage'] is None
        assert app.repository.metric_registry.calls-baseline_cells==1200
        assert done['continuous']['schema_version']=='continuous-eval/2'
        if mode=='rescore':
            assert done['continuous']['parent']['task_id']==facts['parent']['id']
            assert done['continuous']['parent']['report_hash']==facts['parent']['report_hash']
        else:
            assert done['continuous']['schedule']['window_index']==0
            assert done['continuous']['schedule']['spec_hash']==facts['schedule']['spec_hash']
        facts['result']=dict(id=view['id'],report_hash=report['report_hash'],kind=report['kind'],
            status=done['status'],score_cells=app.repository.metric_registry.calls-baseline_cells,
            planned_trials=done['planned_count'],execution=done['execution'],
            inputs_root=done['continuous']['inputs_root'],parent=done['continuous']['parent'])
        _write_scale_checkpoint('wait','done',task_id=view['id'],score_cells=1200)
        rows=[];cursor=None
        while True:
            page=await app.repository.report_page('local',view['id'],cursor=cursor,limit=16)
            rows.extend(page.rows);cursor=page.next_cursor
            if cursor is None:break
        assert len(rows)==300 and {(r['case_id'],r['repetition']) for r in rows}=={
            (c['case_id'],r) for c in cases for r in range(3)}
        facts['report_pages_verified']=len(rows)
        with closing(sqlite3.connect(destination.as_uri()+'?mode=ro',uri=True)) as db:
            db.execute('PRAGMA query_only=ON')
"""
            "            count=db.execute('SELECT count(*) FROM continuous_"
            "inputs WHERE task_id=?',(view['id'],)).fetchone()[0]"
            r"""
        assert count==300
        before_key=app.repository.metric_registry.calls
        if mode=='rescore':assert (await flow.submit(request))['id']==view['id']
        else:
            assert (await flow.tick(spec.schedule_id))['id']==view['id']
            clock_value[0]=100060.0
            try:await flow.tick(spec.schedule_id)
            except ServiceError as error:
                assert error.code=='closed';facts['over_window_code']=error.code
            else:raise AssertionError('out_of_window_was_admitted')
        assert app.repository.metric_registry.calls==before_key
        assert all(value==0 for value in facts['calls'].values())
        facts['total_scoring_calls']=app.repository.metric_registry.calls
        facts['original_key_score_increment']=app.repository.metric_registry.calls-before_key
        facts['stage']='completed';_write_scale_checkpoint('original_key','done',task_id=view['id'])
    except BaseException as error:
        facts['error']=dict(type=type(error).__name__,code=getattr(error,'code',None),
            message=str(error),stage=facts['stage'],frames=[dict(file=t.filename,
            function=t.name,line=t.lineno) for t in traceback.extract_tb(error.__traceback__)])
        raise
    finally:
        _write_scale_checkpoint('cleanup','start',_defer_cleanup_error=True)
        for name,value in [('application',app),('source_repository',source_repo)]:
            if value is not None:
                try:await value.close()
                except BaseException as error:
                    facts['close_errors'].append(dict(owner=name,type=type(error).__name__,message=str(error)))
        sys.setprofile(previous);loop.set_exception_handler(old_handler)
        await asyncio.sleep(0)
        facts['source_closed']=source_repo is not None and source_repo.closed
        facts['destination_closed']=app is not None and app.repository.closed
        facts['kernel_closed']=app is not None and app.kernel.closed
"""
            "        facts['kernel_tasks']=[] if app is None else [dict(id="
            "k,done=t.done(),cancelled=t.cancelled())"
            r"""
            for k,t in app.kernel.tasks.items()]
        facts['inflight']=None if app is None else len(app.kernel._inflight)
        facts['writer_fd']=None if app is None else app.repository._writer_fd
        facts['pending']=[repr(t) for t in asyncio.all_tasks() if t not in before and not t.done()]
        facts['fd_after']=len(os.listdir('/proc/self/fd'))
        facts['threads_after']=[(t.ident,t.name) for t in threading.enumerate()]
        facts['source_hash_after']=hashlib.sha256(backup.read_bytes()).hexdigest()
        safe=(facts['source_closed'] and facts['destination_closed'] and facts['kernel_closed']
            and facts['inflight']==0 and facts['writer_fd'] is None
            and not facts['close_errors'] and not facts['pending'])
        if safe and destination.exists():
            fd=os.open(str(destination)+'.lock',os.O_RDWR|os.O_NOFOLLOW)
            try:
                fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);facts['writer_lock_released']=True
                fcntl.flock(fd,fcntl.LOCK_UN)
            finally:os.close(fd)
            try:
                with closing(sqlite3.connect(destination.as_uri()+'?mode=ro',uri=True)) as db:
                    db.execute('PRAGMA query_only=ON')
"""
            "                    facts['Task_rows']=[json.loads(row[0]) for"
            " row in db.execute('SELECT body FROM eval_tasks')]"
            r"""
"""
            "                    facts['window_count']=db.execute('SELECT c"
            "ount(*) FROM eval_schedule_windows').fetchone()[0]"
            r"""
                    path=root/'header12-destination-backup.sqlite'
                    with closing(sqlite3.connect(path)) as out:db.backup(out)
                facts['backup']=dict(path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest())
            except BaseException as error:
                facts['audit_errors'].append(dict(type=type(error).__name__,message=str(error)))
        for name,listener in listeners:event.remove(Pool,name,listener)
        event.remove(Pool,'checkout',checkout);event.remove(Pool,'connect',connected)
        (root/'header12-consumer.json').write_text(json.dumps(facts,indent=2))
        _write_scale_checkpoint('consumer','done',_defer_cleanup_error=True)
        assert facts['source_hash_after']==facts['source_hash_before']
        assert safe and facts['writer_lock_released'] and not facts['audit_errors']
"""
            "        assert not facts['pending'] and not facts['unexpected'"
            "] and facts['fd_after']<=facts['fd_before']"
            r"""
        counts=collections.Counter(row['event'] for row in facts['connections'])
"""
            "        assert counts['connect']==counts['close'] and counts['"
            "checkout']==counts['reset']==counts['checkin']"
            r"""
        assert facts['source_query_only'] and all(value==1 for value in facts['source_query_only'])
        if getattr(_write_scale_checkpoint,'evidence_errors',[]):
            raise RuntimeError('checkpoint_publication_failed')
asyncio.run(main())
"""
        )
        compile(driver, "completion20-scoring-consumer", "exec")
        source = (await asyncio.to_thread(Path(__file__).resolve)).parents[2] / "src"
        argv = [
            sys.executable,
            "-I",
            "-B",
            "-c",
            driver,
            str(source),
            str(tmp_path),
            json.dumps(selected),
            mode,
        ]
        process, receipt, facts = await _run_scale_evidence_process(
            argv,
            tmp_path,
            execution_timeout_seconds=450 if mode == "rescore" else 330,
            source_database=selected["backup"]["path"],
            destination_database=str(tmp_path / "header12-destination.sqlite"),
        )
        assert receipt["wait_returncode"] == process.returncode == 0, facts["error"]
        assert facts["stage"] == "completed" and facts["result"]["score_cells"] == 1200
        assert receipt["stdout_eof"] and receipt["stderr_eof"]
        return
    import stat
    import sys

    assert mode == "observed_score"
    contract_path = (
        Path("/artifacts/krim-agent-core-20261019")
        / "vm-pn3-scale-implementation-20261023T152826061485"
        / "static-supplement-03/scoring-header12/APPROVED-CONTRACT.json"
    )
    contract = json.loads(await asyncio.to_thread(contract_path.read_text))
    selected = contract["readonly_existing_inputs"]["main_300_source"]
    backup = Path(selected["backup"]["path"])
    for path in [backup, *backup.parents]:
        assert not path.is_symlink()
    known = await asyncio.to_thread(backup.lstat)
    assert stat.S_ISREG(known.st_mode) and known.st_nlink == 1
    assert (
        hashlib.sha256(await asyncio.to_thread(backup.read_bytes)).hexdigest()
        == selected["backup"]["sha256"]
    )
    driver = (
        "import asyncio,collections,fcntl,hashlib,json,os,sqlite3,stat,"
        "sys,threading,time,traceback"
        r"""
from contextlib import closing
from pathlib import Path
source,root=map(Path,sys.argv[1:3])
selected=json.loads(sys.argv[3])
def _write_scale_checkpoint(stage,status,*,_defer_cleanup_error=False,**known):
    # 与父process收据绑定；只写有限标量，不复制Observation或SQL参数。
    active=sys.exception()
    try:
        identity=getattr(_write_scale_checkpoint,'identity',None)
        if identity is None:
            path=root/'header12-process.json'
            st=path.lstat()
            if not stat.S_ISREG(st.st_mode) or st.st_nlink!=1:
                raise RuntimeError('checkpoint_identity_not_regular')
            process=json.loads(path.read_text())
            identity={key:process[key] for key in [
                'run_identity','driver_sha256','helper_ast_sha256','checkpoint_ast_sha256']}
            _write_scale_checkpoint.identity=identity
        sequence=getattr(_write_scale_checkpoint,'sequence',0)+1
        if sequence>128:
            raise RuntimeError('checkpoint_record_limit')
        fields={}
        for key,value in known.items():
            if not isinstance(key,str) or len(key)>64 or (
                value is not None and type(value) not in (str,int,float,bool)):
                raise RuntimeError('checkpoint_non_scalar')
            fields[key]=value[:512] if isinstance(value,str) else value
        row=dict(identity,sequence=sequence,stage=stage,status=status,
            monotonic_ns=time.monotonic_ns(),time_ns=time.time_ns(),
            pid=os.getpid(),uid=os.getuid(),known=fields)
        raw=(json.dumps(row,ensure_ascii=False,allow_nan=False)+'\n').encode()
        if len(raw)>4096:
            raise RuntimeError('checkpoint_record_bytes')
        fd=os.open(root/'header12-checkpoints.jsonl',
            os.O_WRONLY|os.O_CREAT|os.O_APPEND|os.O_NOFOLLOW,0o600)
        try:
            st=os.fstat(fd)
            if not stat.S_ISREG(st.st_mode) or st.st_nlink!=1 or st.st_uid!=os.getuid():
                raise RuntimeError('checkpoint_output_not_regular')
            if st.st_size+len(raw)>128*4096:
                raise RuntimeError('checkpoint_total_bytes')
            view=memoryview(raw)
            while view:
                count=os.write(fd,view)
                if count<=0:raise RuntimeError('checkpoint_short_write')
                view=view[count:]
            os.fsync(fd)
        finally:os.close(fd)
        _write_scale_checkpoint.sequence=sequence
    except BaseException as evidence_error:
        errors=getattr(_write_scale_checkpoint,'evidence_errors',None)
        if errors is None:
            errors=[]
            _write_scale_checkpoint.evidence_errors=errors
        if len(errors)<128:
            errors.append(dict(stage=stage,status=status,type=type(evidence_error).__name__,
                message=str(evidence_error)[:512],pid=os.getpid(),
                active_type=None if active is None else type(active).__name__,
                deferred=_defer_cleanup_error,monotonic_ns=time.monotonic_ns()))
        else:
            _write_scale_checkpoint.error_overflow=getattr(
                _write_scale_checkpoint,'error_overflow',0)+1
        if _defer_cleanup_error:
            if isinstance(globals().get('facts'),dict):
                facts['checkpoint_errors']=list(errors)
            return
        if active is not None:raise active from evidence_error
        raise
_write_scale_checkpoint('bootstrap','start')
sys.path.insert(0,str(source))
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.pool import Pool
from krim_agent.evaluation.app import open_eval
from krim_agent.evaluation.repository import Repository
from krim_agent.evaluation.sources import EvalSourceReader
from krim_agent.evaluation.domain import (
    Budget,CaseSpec,DatasetRef,neutral_encoded,neutral_fingerprint,
)
from krim_agent.evaluation.continuous_types import (
    ConfirmationRequest,ContinuousRequest,FreezeConfirmed,ImportRequest,ScoreSource,
    SourceSelection,
)
from krim_agent.evaluation.continuous_metrics import continuous_rules
from krim_agent.evaluation.metrics import MetricRegistry
from krim_agent.evaluation.adapters.local_process import LocalProcessAdapter
from krim_agent.evaluation import continuous_store

_write_scale_checkpoint('bootstrap','done')
_write_scale_checkpoint('source_read','start')
backup=Path(selected['backup']['path'])
destination=root/'header12-destination.sqlite'
for p in [backup,*backup.parents]:
    assert not p.is_symlink()
st=backup.lstat()
assert stat.S_ISREG(st.st_mode) and st.st_nlink==1
before_hash=hashlib.sha256(backup.read_bytes()).hexdigest()
assert before_hash==selected['backup']['sha256']
with closing(sqlite3.connect(backup.as_uri()+'?mode=ro',uri=True)) as db:
    db.execute('PRAGMA query_only=ON')
    assert db.execute('PRAGMA query_only').fetchone()[0]==1
    header=json.loads(db.execute(
        'SELECT body FROM eval_tasks WHERE id=? AND scope=?',
        (selected['task_id'],'local')).fetchone()[0])
    original_report=json.loads(db.execute(
        'SELECT body FROM eval_reports WHERE task_id=?',
        (selected['task_id'],)).fetchone()[0])
    marker=json.loads(db.execute('SELECT body FROM eval_meta WHERE id=1').fetchone()[0])
    ref=header['dataset_ref']
    cases=[json.loads(r[0]) for r in db.execute(
        'SELECT body FROM dataset_cases WHERE name=? AND version=? ORDER BY case_id COLLATE BINARY',
        (ref['name'],ref['version']))]
    trials=[json.loads(r[0]) for r in db.execute(
        'SELECT body FROM eval_trials WHERE task_id=? ORDER BY case_id COLLATE BINARY,repetition',
        (selected['task_id'],))]
assert header['layout']==3 and header['status']=='completed'
assert len(cases)==100 and len(trials)==300
assert original_report['report_hash']==selected['report_hash']==header['report_hash']
assert original_report['summary']['planned_cells']==1200
assert {(r['case_id'],r['repetition']) for r in trials}=={
    (c['case_id'],i) for c in cases for i in range(3)}
assert all(header['case_index'][c['case_id']]==neutral_fingerprint(c) for c in cases)
_write_scale_checkpoint('source_read','done',cases=len(cases),trials=len(trials),
    task_id=selected['task_id'])
facts=dict(
    node='tests/safe_eval/test_eval_scale.py::test_shared_modes_scale[observed_score]',
    uid=os.getuid(),source_root=str(source),source_database=str(backup),
    destination_database=str(destination),source_URI=backup.as_uri()+'?mode=ro',
    source_uri_true=True,baseline_query_only=True,baseline_sqlite_closed=True,
    source_hash_before=before_hash,source_task_id=selected['task_id'],
    source_report_hash=selected['report_hash'],source_owner=marker['repository_id'],
    source_revision=header['revision'],source_request_hash=header['request_hash'],
    source_existing_execution_count=300,source_execution_calls_this_command=0,
    source_existing_score_cells=1200,stage='start',error=None,connections=[],sql=[],
    read_query_only=[],calls={},observations=[],confirmations=[],phases=[],
    retained_output_samples={},header_attempts=[],encoded_contexts=[],exceptions=[],
    observation_integrity_count=0,close_errors=[],unexpected=[],
)
profile_codes={getattr(LocalProcessAdapter,n).__code__:'target_'+n
    for n in ['prepare','execute','collect','cancel']}
profile_codes[Repository.adapter.__code__]='adapter_lookup'
profile_codes[asyncio.create_subprocess_exec.__code__]='target_subprocess'
facts['calls']={name:0 for name in profile_codes.values()}
facts['calls'].update(provider_stream=0,metric_cells=0)
score_objects=set()
prepare_objects=set()
source_objects=set()
facts['source_read_count']=0
facts['prepare_count']=0
facts['destination_score_cells']=0
def serialized_size(value):
    return len(json.dumps(value,ensure_ascii=False,sort_keys=True,
        separators=(',',':'),allow_nan=False).encode())
def trace_exception(frame,kind,value):
    if kind=='exception' and facts['stage']=='submit':
        error=value[1]
        if not isinstance(error,(StopIteration,StopAsyncIteration)):
            facts['exceptions'].append(dict(type=type(error).__name__,message=str(error),
                file=frame.f_code.co_filename,function=frame.f_code.co_name,line=frame.f_lineno))
    return trace_exception
def trace(frame,kind,value):
    if frame.f_code in [neutral_encoded.__code__,Repository._begin_task_conn.__code__,
        continuous_store.prepare_task.__code__]:
        return trace_exception
    return None
def profile(frame,kind,result):
    if kind!='call' and kind!='return':
        return
    code=frame.f_code
    if kind=='call':
        name=profile_codes.get(code)
        if name is not None:
            facts['calls'][name]+=1
        if frame.f_globals.get('__name__','').startswith('krim_agent.providers.'):
            if code.co_name=='stream':facts['calls']['provider_stream']+=1
        if code is MetricRegistry.score.__code__:
            output=frame.f_locals.get('output')
            case=frame.f_locals.get('case')
            rule=frame.f_locals.get('rule')
            key=(case.get('case_id'),output.get('execution_ref'),rule.get('name'))
            if key not in score_objects:
                score_objects.add(key)
                facts['calls']['metric_cells']+=1
                facts['destination_score_cells']+=1
        if code is EvalSourceReader.read.__code__:
            key=neutral_fingerprint(frame.f_locals['selection'])
            source_objects.add(key)
            facts['source_read_count']=len(source_objects)
        if code is continuous_store.prepare_task.__code__:
            key=id(frame.f_locals['request'])
            prepare_objects.add(key)
            facts['prepare_count']=len(prepare_objects)
    if facts['stage']=='submit' and code.co_filename.endswith(
        ('/evaluation/continuous_store.py','/evaluation/repository.py')):
        local=frame.f_locals
        for name in ['inputs','scoring_inputs']:
            values=local.get(name)
            if isinstance(values,list):
                count=sum(isinstance(v,dict) and v.get('output') is not None for v in values)
                key=code.co_name+'.'+name
                sample=facts['retained_output_samples'].setdefault(key,
                    dict(events=0,peak=0,first_line=frame.f_lineno))
                sample['events']+=1
                sample['peak']=max(sample['peak'],count)
    if kind=='call' and code is neutral_encoded.__code__ and facts['stage']=='submit':
        value=frame.f_locals.get('value')
        if isinstance(value,dict):
            if value.get('layout')==3:
                facts['header_attempts'].append(dict(
                    bytes=serialized_size(value),cap=frame.f_locals.get('max_bytes'),
                    revision=value.get('revision'),planned_count=value.get('planned_count'),
                    context_inputs=len((value.get('continuous') or {}).get('inputs',{}))))
            elif value.get('schema_version')=='continuous-eval/1':
                facts['encoded_contexts'].append(dict(
                    bytes=serialized_size(value),cap=frame.f_locals.get('max_bytes'),
                    input_count=len(value.get('inputs',{}))))
def connected(dbapi,record):
    cur=dbapi.cursor()
    try:
        cur.execute('PRAGMA database_list')
        name=cur.fetchone()[2]
        record.info['header12_role']='source' if name==str(backup) else 'destination'
        if record.info['header12_role']=='source':cur.execute('PRAGMA query_only=ON')
    finally:cur.close()
def checked_out(dbapi,record,*args):
    if record.info.get('header12_role')=='source':
        cur=dbapi.cursor()
        try:
            cur.execute('PRAGMA query_only')
            facts['read_query_only'].append(cur.fetchone()[0])
        finally:cur.close()
def sql(conn,cursor,statement,parameters,context,many):
    facts['sql'].append(dict(
        database=str(conn.engine.url.database),stage=facts['stage'],
        statement=statement,parameters=parameters,at=time.monotonic()))
listeners=[]
event.listen(Pool,'connect',connected)
event.listen(Pool,'checkout',checked_out)
for name in ['connect','checkout','reset','checkin','close']:
    def observe(*args,name=name):
        record=args[1]
        facts['connections'].append(dict(event=name,connection=id(args[0]),
            role=record.info.get('header12_role'),at=time.monotonic()))
    event.listen(Pool,name,observe)
    listeners.append((name,observe))
event.listen(Engine,'before_cursor_execute',sql)

async def main():
    app=source_repo=None
    before=asyncio.all_tasks()
    facts['fd_before']=len(os.listdir('/proc/self/fd'))
    facts['threads_before']=[(t.ident,t.name) for t in threading.enumerate()]
    loop=asyncio.get_running_loop()
    previous_handler=loop.get_exception_handler()
    loop.set_exception_handler(lambda current,context:facts['unexpected'].append(str(context)))
    previous_profile=sys.getprofile()
    previous_trace=sys.gettrace()
    sys.setprofile(profile)
    sys.settrace(trace)
    started=time.monotonic()
    try:
        facts['stage']='open'
        _write_scale_checkpoint('open','start')
        source_repo=await Repository.open(backup,readonly=True)
        reader=EvalSourceReader(source_repo,scope='local',include_context=True)
        app=await open_eval(destination,execution_enabled=False,sources={'accepted-scale':reader})
        _write_scale_checkpoint('open','done')
        assert app.adapter is None and app.owner is None and app.kernel.repository is app.repository
        facts['single_kernel_id']=id(app.kernel)
        flow=app.continuous
        flow.redaction['artifact_paths']=['ledger.json','summary.md']
        selected_scores=[]
        drafts={}
        facts['stage']='import'
        _write_scale_checkpoint('import','start',trials=len(trials))
        for offset in range(0,len(trials),32):
            batch=trials[offset:offset+32]
            _write_scale_checkpoint('import_batch','start',offset=offset,count=len(batch))
            rows=await flow.import_sources(ImportRequest(
                request_key='header12-import-'+str(offset),source_alias='accepted-scale',
                sources=[SourceSelection(task_id=selected['task_id'],case_id=r['case_id'],
                    repetition=r['repetition'],revision=header['revision'],
                    evidence_hash=r['output_digest']) for r in batch]))
            assert len(rows)==len(batch)
            for row,original in zip(rows,batch,strict=True):
                origin=row['source']
                assert origin['repository_id']==marker['repository_id']
                assert origin['task_id']==selected['task_id']
                assert origin['report_hash']==selected['report_hash']
                assert origin['revision']==header['revision']
                assert origin['request_hash']==header['request_hash']
                assert origin['evidence_hash']==original['output_digest']
                assert origin['execution_ref']==original['execution_ref']
                assert row['target']==header['target'] and row['environment']==header['environment']
                assert row['eligibility']=='eligible'
                proof=row['source_proof']
                assert proof['complete'] and proof['terminal']
                assert proof['context_hash']==row['context_hash']
                assert proof['environment']==header['environment']
"""
        "                assert neutral_fingerprint({k:v for k,v in row"
        ".items() if k!='content_hash'})==row['content_hash']"
        r"""
                facts['observation_integrity_count']+=1
                cid,rep=origin['case_id'],origin['repetition']
                facts['observations'].append(dict(
                    case_id=cid,repetition=rep,id=row['id'],content_hash=row['content_hash'],
                    imported_output_hash=neutral_fingerprint(row['output']),
                    source_output_hash=original['output_digest'],source_ref=origin,
                    proof_hash=neutral_fingerprint(proof),availability=row['availability'],
                    source_context_hash=row['context_hash']))
                selected_scores.append(ScoreSource(case_id=cid,repetition=rep,
                    observation_id=row['id'],observation_hash=row['content_hash']))
                if rep==0:drafts[cid]=row['id']
            del rows
            _write_scale_checkpoint('import_batch','done',offset=offset,
                source_read_count=facts['source_read_count'])
        assert len(selected_scores)==300 and len(drafts)==100
        assert facts['source_read_count']==300
        _write_scale_checkpoint('import','done',source_read_count=facts['source_read_count'])
        facts['stage']='confirm'
        _write_scale_checkpoint('confirm','start',cases=len(cases))
        confirmations=[]
        for case in cases:
            draft=await app.repository.candidate('local',drafts[case['case_id']])
            confirmed=await flow.confirm_case(ConfirmationRequest(
                request_key='header12-confirm-'+case['case_id'],
                candidate_id=draft['candidate_id'],expected_revision=draft['revision'],
                expected_hash=draft['hash'],case=CaseSpec.model_validate(case),context='complete',
                reason='本轮明确确认既有真实Case、完整context和未改变的整数gold；不重执行来源。'))
            assert confirmed['status']=='confirmed' and confirmed['case']==case
            confirmations.append(confirmed['id'])
            facts['confirmations'].append(dict(id=confirmed['id'],
                case_id=case['case_id'],content_hash=confirmed['content_hash']))
        _write_scale_checkpoint('confirm','done',confirmations=len(confirmations))
        _write_scale_checkpoint('freeze','start')
        frozen=await flow.freeze_dataset(FreezeConfirmed(request_key='header12-freeze',
            name='header12-cases',version='1',confirmations=confirmations))
        _write_scale_checkpoint('freeze','done')
        facts['dataset']=frozen
        rules=continuous_rules(app.repository.metric_registry)
        assert len(rules)==4
        budget=Budget(repetitions=3,concurrency=2)
        assert budget.batch_timeout_seconds==120 and budget.item_timeout_seconds==20
        assert budget.scorer_timeout_seconds==1
        request=ContinuousRequest(request_key='header12-observed-score',mode='observed_score',
            dataset=DatasetRef.model_validate(frozen['dataset_ref']),
            dataset_envelope_fp=frozen['dataset_envelope_fp'],target_key=header['target']['key'],
            metrics=rules,execution=budget,observations=selected_scores)
        facts['request']=request.model_dump(mode='json')
        facts['request_hash']=neutral_fingerprint(facts['request'])
        facts['requested_matrix']=dict(cases=100,repetitions=3,trials=300,cells=1200)
        facts['pre_submit_calls']=dict(facts['calls'])
        assert all(v==0 for v in facts['calls'].values())
        facts['phases'].append(dict(stage='ready_to_submit',elapsed=time.monotonic()-started))
        facts['stage']='submit'
        _write_scale_checkpoint('submit','start',prepare_count=facts['prepare_count'])
        submitted=await flow.submit(request)
        facts['submitted_task_id']=submitted['id']
        _write_scale_checkpoint('submit','done',task_id=submitted['id'],
            prepare_count=facts['prepare_count'])
        facts['stage']='wait'
        _write_scale_checkpoint('wait','start',task_id=submitted['id'])
        completed=await app.kernel.wait(submitted['id'])
        _write_scale_checkpoint('wait','done',task_id=submitted['id'],
            observed_status=completed['status'],score_cells=facts['destination_score_cells'])
        facts['completed_header']=completed
        assert completed['status']=='completed' and completed['planned_count']==300
"""
        "        assert completed['target']==header['target'] and compl"
        "eted['environment']==header['environment']"
        r"""
        assert completed['layout']==3 and serialized_size(completed)<=262144
        assert completed['continuous']['schema_version']=='continuous-eval/2'
        assert 'inputs' not in completed['continuous']
        assert len(completed['continuous']['inputs_index'])==300
        assert max((x['peak'] for x in facts['retained_output_samples'].values()),default=0)<=1
        _write_scale_checkpoint('report','start',task_id=completed['id'])
        report=await app.repository.report(completed['id'])
        _write_scale_checkpoint('report','done',task_id=completed['id'])
        facts['report']=report
        assert report['summary']['planned']==300
        assert report['summary']['planned_cells']==report['summary']['scored_cells']==1200
        assert report['summary']['passed']==300
        assert facts['destination_score_cells']==1200 and facts['prepare_count']==1
        assert app.repository.metric_registry.calls==1200
        persisted=[]
        cursor=None
        _write_scale_checkpoint('page','start')
        while True:
            page=await app.repository.report_page('local',completed['id'],cursor=cursor,limit=16)
            persisted.extend(page.rows)
            cursor=page.next_cursor
"""
        "            _write_scale_checkpoint('page','observed',rows=len"
        "(persisted),has_next=cursor is not None)"
        r"""
            if cursor is None:break
        _write_scale_checkpoint('page','done',rows=len(persisted))
        assert len(persisted)==300
        assert {(r['case_id'],r['repetition']) for r in persisted}=={
            (c['case_id'],i) for c in cases for i in range(3)}
        facts['stage']='verify-inputs'
        _write_scale_checkpoint('input','start')
        with closing(sqlite3.connect(destination.as_uri()+'?mode=ro',uri=True)) as db:
            db.execute('PRAGMA query_only=ON')
            packets=[json.loads(r[0]) for r in db.execute(
                'SELECT body FROM continuous_inputs WHERE task_id=? ORDER BY case_id,repetition',
                (completed['id'],))]
        assert len(packets)==300
        originals={(r['case_id'],r['repetition']):r for r in facts['observations']}
        for packet in packets:
            metadata=packet['metadata']
            original=originals[(packet['case_id'],packet['repetition'])]
            assert neutral_fingerprint(packet)==completed['continuous']['inputs_index'][
                packet['case_id']+':'+str(packet['repetition'])]
            assert metadata['source_ref']['source']==original['source_ref']
            assert neutral_fingerprint(metadata['source_proof'])==original['proof_hash']
            assert metadata['availability']==original['availability']
            assert metadata['source_context_hash']==original['source_context_hash']
"""
        "            assert metadata['output_hash']==packet['output_dig"
        "est']==original['imported_output_hash']"
        r"""
        _write_scale_checkpoint('input','done',packets=len(packets))
        _write_scale_checkpoint('original_key','start')
        assert (await flow.submit(request))['id']==completed['id']
        _write_scale_checkpoint('original_key','done',task_id=completed['id'],
            prepare_count=facts['prepare_count'],score_cells=facts['destination_score_cells'])
        assert facts['prepare_count']==1 and facts['destination_score_cells']==1200
        assert all(v==0 for k,v in facts['calls'].items() if k!='metric_cells')
        facts['stage']='completed'
    except BaseException as error:
        facts['error']=dict(type=type(error).__name__,message=str(error),
            code=getattr(error,'code',None),stage=facts['stage'],
            frames=[dict(file=t.filename,function=t.name,line=t.lineno)
                for t in traceback.extract_tb(error.__traceback__)])
        _write_scale_checkpoint('execution','error',error_type=type(error).__name__,
            message=str(error),observed_stage=facts['stage'])
        raise
    finally:
        facts['elapsed_before_cleanup']=time.monotonic()-started
        facts['audit_errors']=[]
        facts['writer_lock_released']=None
        _write_scale_checkpoint('cleanup','start',observed_stage=facts['stage'],
"""
        "            primary_error_type=None if facts['error'] is None "
        "else facts['error']['type'],_defer_cleanup_error=True)"
        r"""
        for name,value in [('application',app),('source_repository',source_repo)]:
            if value is not None:
                _write_scale_checkpoint('close','start',owner=name,_defer_cleanup_error=True)
                try:await value.close()
                except BaseException as error:
                    facts['close_errors'].append(dict(owner=name,type=type(error).__name__,
                        message=str(error)))
                    _write_scale_checkpoint('close','error',owner=name,
                        error_type=type(error).__name__,message=str(error),_defer_cleanup_error=True)
                else:
                    _write_scale_checkpoint('close','done',owner=name,_defer_cleanup_error=True)
        sys.setprofile(previous_profile)
        sys.settrace(previous_trace)
        loop.set_exception_handler(previous_handler)
        await asyncio.sleep(0)
        facts['source_repository_closed']=source_repo is not None and source_repo.closed
        facts['destination_repository_closed']=app is not None and app.repository.closed
        facts['writer_fd']=None if app is None else app.repository._writer_fd
        facts['kernel_closed']=app is not None and app.kernel.closed
        facts['kernel_tasks']=[] if app is None else [
"""
        "            dict(task_id=k,done=t.done(),cancelled=t.cancelled"
        "()) for k,t in app.kernel.tasks.items()]"
        r"""
        facts['kernel_inflight']=None if app is None else len(app.kernel._inflight)
"""
        "        facts['metric_registry_actual_calls']=None if app is N"
        "one else app.repository.metric_registry.calls"
        r"""
        facts['kernel_workers']=None if app is None else {
            k:[t.done() for t in group] for k,group in app.kernel._workers.items()}
        facts['pending']=[repr(t) for t in asyncio.all_tasks() if t not in before and not t.done()]
        facts['source_hash_after']=hashlib.sha256(backup.read_bytes()).hexdigest()
        facts['elapsed_including_cleanup']=time.monotonic()-started
        facts['fd_after']=len(os.listdir('/proc/self/fd'))
        facts['threads_after']=[(t.ident,t.name) for t in threading.enumerate()]
        audit_safe=(not facts['close_errors'] and facts['source_repository_closed']
            and facts['destination_repository_closed'] and facts['writer_fd'] is None
            and facts['kernel_closed'] and not facts['pending']
            and facts['kernel_inflight']==0)
        _write_scale_checkpoint('cleanup','observed',source_closed=facts['source_repository_closed'],
            destination_closed=facts['destination_repository_closed'],
            writer_fd=facts['writer_fd'],pending=len(facts['pending']),audit_safe=audit_safe,_defer_cleanup_error=True)
        if destination.exists() and audit_safe:
            _write_scale_checkpoint('writer_lock','start',_defer_cleanup_error=True)
            try:
                fd=os.open(str(destination)+'.lock',os.O_RDWR|os.O_NOFOLLOW)
                try:
                    fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
                    facts['writer_lock_released']=True
                    fcntl.flock(fd,fcntl.LOCK_UN)
                finally:os.close(fd)
            except BaseException as error:
                facts['audit_errors'].append(dict(step='writer_lock',
                    type=type(error).__name__,message=str(error)))
                _write_scale_checkpoint('writer_lock','error',error_type=type(error).__name__,
                    message=str(error),_defer_cleanup_error=True)
            else:
                _write_scale_checkpoint('writer_lock','done',released=True,_defer_cleanup_error=True)
            if facts['writer_lock_released'] is True and not facts['audit_errors']:
                try:
                    _write_scale_checkpoint('database_audit','start',_defer_cleanup_error=True)
                    uri=destination.as_uri()+'?mode=ro'
                    backup_path=root/'header12-destination-backup.sqlite'
                    with closing(sqlite3.connect(uri,uri=True)) as db:
                        db.execute('PRAGMA query_only=ON')
                        assert db.execute('PRAGMA query_only').fetchone()[0]==1
"""
        "                        facts['destination_counts']={name:db.e"
        "xecute('SELECT count(*) FROM '+name).fetchone()[0]"
        r"""
                            for name in ['eval_tasks','eval_trials','continuous_inputs',
                                'source_observations','case_confirmations','dataset_cases']}
                        facts['destination_task_rows']=[json.loads(r[0]) for r in db.execute(
                            'SELECT body FROM eval_tasks')]
                        facts['destination_meta']=json.loads(db.execute(
                            'SELECT body FROM eval_meta WHERE id=1').fetchone()[0])
                        _write_scale_checkpoint('database_audit','done',_defer_cleanup_error=True)
                        _write_scale_checkpoint('backup','start',_defer_cleanup_error=True)
                        with closing(sqlite3.connect(backup_path)) as out:db.backup(out)
                        _write_scale_checkpoint('backup','done',_defer_cleanup_error=True)
                    facts['destination_consistent_readonly_backup']=dict(path=str(backup_path),
                        sha256=hashlib.sha256(backup_path.read_bytes()).hexdigest())
                except BaseException as error:
                    facts['audit_errors'].append(dict(step='database_audit_or_backup',
                        type=type(error).__name__,message=str(error)))
                    _write_scale_checkpoint('database_audit_or_backup','error',
                        error_type=type(error).__name__,message=str(error),_defer_cleanup_error=True)
        else:
            _write_scale_checkpoint('database_audit','skipped',audit_safe=audit_safe,_defer_cleanup_error=True)
        _write_scale_checkpoint('listeners','start',_defer_cleanup_error=True)
        try:
            event.remove(Engine,'before_cursor_execute',sql)
            event.remove(Pool,'checkout',checked_out)
            event.remove(Pool,'connect',connected)
            for name,handler in listeners:event.remove(Pool,name,handler)
        except BaseException as error:
            facts['audit_errors'].append(dict(step='listeners',
                type=type(error).__name__,message=str(error)))
            _write_scale_checkpoint('listeners','error',error_type=type(error).__name__,
                message=str(error),_defer_cleanup_error=True)
        else:
            _write_scale_checkpoint('listeners','done',_defer_cleanup_error=True)
        _write_scale_checkpoint('consumer','start',audit_errors=len(facts['audit_errors']),_defer_cleanup_error=True)
        (root/'header12-consumer.json').write_text(json.dumps(facts,indent=2))
        _write_scale_checkpoint('consumer','done',audit_errors=len(facts['audit_errors']),_defer_cleanup_error=True)
        assert facts['source_hash_after']==facts['source_hash_before']
        assert not facts['close_errors'] and facts['source_repository_closed']
        assert facts['destination_repository_closed'] and facts['writer_fd'] is None
        assert facts['kernel_closed'] and not facts['pending'] and not facts['unexpected']
        assert facts['fd_after']<=facts['fd_before']
        counts=collections.Counter(r['event'] for r in facts['connections'])
        assert counts['connect']==counts['close']
        assert counts['checkout']==counts['reset']==counts['checkin']
        assert facts['read_query_only'] and all(v==1 for v in facts['read_query_only'])
        checkpoint_errors=getattr(_write_scale_checkpoint,'evidence_errors',[])
        if checkpoint_errors:
            facts['checkpoint_errors']=list(checkpoint_errors)
            active=sys.exception()
            try:
                (root/'header12-consumer.json').write_text(json.dumps(facts,indent=2))
            except BaseException as report_error:
                if len(checkpoint_errors)<128:
                    checkpoint_errors.append(dict(stage='final_error_report',status='error',
                        type=type(report_error).__name__,message=str(report_error)[:512]))
            if active is not None:
                active.scale_checkpoint_errors=tuple(checkpoint_errors)
                active.add_note('scale_checkpoint_errors:'+json.dumps(checkpoint_errors))
            else:
                failure=RuntimeError('scale_checkpoint_publication_failed')
                failure.scale_checkpoint_errors=tuple(checkpoint_errors)
                raise failure
        if facts['audit_errors'] and facts['error'] is None:
            raise RuntimeError('scale_evidence_audit_failure')
asyncio.run(main())
"""
    )
    compile(driver, "header12-consumer", "exec")
    source = (await asyncio.to_thread(Path(__file__).resolve)).parents[2] / "src"
    argv = [
        sys.executable,
        "-I",
        "-B",
        "-c",
        driver,
        str(source),
        str(tmp_path),
        json.dumps(selected),
    ]
    proc, process, facts = await _run_scale_evidence_process(
        argv,
        tmp_path,
        execution_timeout_seconds=330,
        source_database=str(backup),
        destination_database=str(tmp_path / "header12-destination.sqlite"),
    )
    timed_out = process["timed_out"]
    wait_code = process["wait_returncode"]
    assert (
        hashlib.sha256(await asyncio.to_thread(backup.read_bytes)).hexdigest()
        == selected["backup"]["sha256"]
    )
    assert not timed_out
    assert wait_code == proc.returncode == 0, facts["error"]
    assert facts["stage"] == "completed"
    assert facts["destination_score_cells"] == 1200


async def _run_scale_evidence_process(
    argv,
    root,
    *,
    execution_timeout_seconds=210,
    source_database=None,
    destination_database=None,
    source_driver_sha256=None,
    cwd=None,
):
    """只管理本次实际创建的子进程；过程事实先落盘，再读取consumer。"""
    import ast
    import errno
    import os
    import signal
    import uuid

    root = Path(root)
    child_cwd = os.getcwd() if cwd is None else os.fspath(cwd)
    process_path = root / "header12-process.json"
    facts_path = root / "header12-consumer.json"
    source_read = asyncio.create_task(
        asyncio.to_thread(Path(__file__).read_text), name="scale-evidence-source-read"
    )
    try:
        source_text = await asyncio.shield(source_read)
    except asyncio.CancelledError as cancelled:
        while not source_read.done():
            try:
                await asyncio.shield(source_read)
            except asyncio.CancelledError:
                continue
            except BaseException:
                break
        try:
            source_read.result()
        except BaseException as read_error:
            cancelled.add_note("source read failed: " + repr(read_error))
            raise cancelled from read_error
        raise
    source_tree = ast.parse(source_text)
    helper_node = next(
        node
        for node in source_tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_run_scale_evidence_process"
    )
    driver_tree = ast.parse(argv[4])
    checkpoint_node = next(
        node
        for node in driver_tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_write_scale_checkpoint"
    )
    driver_digest = hashlib.sha256(argv[4].encode()).hexdigest()
    process = {
        "run_identity": uuid.uuid4().hex,
        "argv": argv,
        "cwd": child_cwd,
        "uid": os.getuid(),
        "pid": None,
        "returncode": None,
        "wait_returncode": None,
        "wait_observed": None,
        "pidfd": None,
        "pidfd_opened": None,
        "pidfd_closed": None,
        "pidfd_close_verified": None,
        "stdout_eof": None,
        "stderr_eof": None,
        "timed_out": None,
        "spawn_status": "not_started",
        "consumer_status": "not_read",
        "cleanup_status": "UNKNOWN",
        "events": [],
        "errors": [],
        "signal_operations": [],
        "receipt_file_close_checks": [],
        "driver_sha256": driver_digest,
        "source_driver_sha256": source_driver_sha256 or driver_digest,
        "helper_ast_sha256": hashlib.sha256(ast.dump(helper_node).encode()).hexdigest(),
        "checkpoint_ast_sha256": hashlib.sha256(ast.dump(checkpoint_node).encode()).hexdigest(),
        "source_database": source_database,
        "destination_database": destination_database,
        "facts_file": str(facts_path),
        "process_file": str(process_path),
        "execution_timeout_seconds": execution_timeout_seconds,
        "cleanup_timeout_seconds": 30,
    }

    publication_errors = []
    publication_error_overflow = 0
    last_published_sequence = None

    def save_event(operation, **known):
        nonlocal publication_error_overflow, last_published_sequence
        sequence = process.get("event_count", 0) + 1
        process["event_count"] = sequence
        event = {
            "sequence": sequence,
            "operation": operation,
            "monotonic_ns": time.monotonic_ns(),
            **known,
        }
        if len(process["events"]) < 128:
            process["events"].append(event)
        else:
            process["event_overflow"] = process.get("event_overflow", 0) + 1
        process["evidence_publication_errors"] = publication_errors
        process["publication_error_overflow"] = publication_error_overflow
        process["last_successful_publication_sequence"] = last_published_sequence
        temporary = process_path.with_suffix(".json.tmp")
        try:
            with temporary.open("w") as stream:
                stream.write(json.dumps(process, indent=2))
                stream.flush()
                os.fsync(stream.fileno())
                descriptor = stream.fileno()
            try:
                os.fstat(descriptor)
            except OSError as error:
                if error.errno != errno.EBADF:
                    raise
            else:
                raise RuntimeError("process_receipt_file_not_closed")
            process["receipt_file_close_checks"].append(True)
            os.replace(temporary, process_path)
            last_published_sequence = sequence
        except BaseException as error:
            # 不再调用发布器记录发布器错误；清理路径不能受其再次抛出影响。
            failure = {
                "sequence": sequence,
                "operation": operation,
                "type": type(error).__name__,
                "message": str(error)[:512],
            }
            if len(publication_errors) < 128:
                publication_errors.append(failure)
            else:
                publication_error_overflow += 1
            process["publication_error_overflow"] = publication_error_overflow
        process["last_successful_publication_sequence"] = last_published_sequence

    def save_error(operation, error):
        if len(process["errors"]) < 128:
            process["errors"].append(
                {
                    "operation": operation,
                    "type": type(error).__name__,
                    "message": str(error),
                }
            )
        else:
            process["error_overflow"] = process.get("error_overflow", 0) + 1
        save_event(operation + "_error", error_type=type(error).__name__)

    proc = pidfd = collector = None
    stdout = stderr = None
    primary = None
    before = asyncio.all_tasks()
    save_event("spawn_start")
    try:
        proc = await asyncio.create_subprocess_exec(
            *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=child_cwd
        )
        process.update(pid=proc.pid, spawn_status="started")
        save_event("spawned", pid=proc.pid)
        pidfd = os.pidfd_open(proc.pid)
        process.update(pidfd=pidfd, pidfd_opened=True)
        save_event("pidfd_opened", pid=proc.pid, descriptor=pidfd)
        collector = asyncio.create_task(proc.communicate(), name="scale-evidence-communicate")
        async with asyncio.timeout(execution_timeout_seconds):
            stdout, stderr = await asyncio.shield(collector)
        process["timed_out"] = False
        save_event("communicate_done", pid=proc.pid)
    except BaseException as error:
        primary = error
        if isinstance(error, TimeoutError):
            process["timed_out"] = True
        save_error("execution", error)
    finally:
        cleanup_deadline = asyncio.get_running_loop().time() + 30
        process["cleanup_started_monotonic"] = time.monotonic()
        save_event("cleanup_start")

        async def cleanup():
            nonlocal stdout, stderr
            waiter = None
            try:
                if proc is not None:
                    waiter = asyncio.create_task(proc.wait(), name="scale-evidence-wait")

                    def send(value):
                        row = {"pid": proc.pid, "signal": value, "status": "attempted"}
                        process["signal_operations"].append(row)
                        save_event("signal_start", pid=proc.pid, signal=value)
                        try:
                            if pidfd is not None:
                                signal.pidfd_send_signal(pidfd, value)
                                row["handle"] = "pidfd"
                            else:
                                proc.send_signal(value)
                                row["handle"] = "owned_subprocess"
                        except BaseException as error:
                            row.update(
                                status="error", type=type(error).__name__, message=str(error)
                            )
                            save_error("signal", error)
                        else:
                            row["status"] = "sent"
                            save_event("signal_done", pid=proc.pid, signal=value)

                    try:
                        if proc.returncode is None:
                            send(signal.SIGTERM)
                        try:
                            async with asyncio.timeout_at(
                                min(cleanup_deadline, asyncio.get_running_loop().time() + 5)
                            ):
                                code = await asyncio.shield(waiter)
                        except TimeoutError:
                            save_event("term_wait_timeout", pid=proc.pid)
                            if proc.returncode is None:
                                send(signal.SIGKILL)
                            async with asyncio.timeout_at(cleanup_deadline):
                                code = await asyncio.shield(waiter)
                        process.update(
                            wait_returncode=code, returncode=proc.returncode, wait_observed=True
                        )
                        save_event("wait_done", pid=proc.pid, wait_returncode=code)
                    except BaseException as error:
                        save_error("wait", error)
                    try:
                        if collector is None:
                            collector_task = asyncio.create_task(
                                proc.communicate(), name="scale-evidence-cleanup-communicate"
                            )
                        else:
                            collector_task = collector
                        async with asyncio.timeout_at(cleanup_deadline):
                            stdout, stderr = await asyncio.shield(collector_task)
                        save_event("streams_done", pid=proc.pid)
                    except BaseException as error:
                        save_error("communicate_cleanup", error)
                    finally:
                        if "collector_task" in locals() and not collector_task.done():
                            collector_task.cancel()
                            await asyncio.wait(
                                [collector_task],
                                timeout=max(
                                    0, cleanup_deadline - asyncio.get_running_loop().time()
                                ),
                            )
                    process.update(
                        stdout_eof=proc.stdout.at_eof(),
                        stderr_eof=proc.stderr.at_eof(),
                        returncode=proc.returncode,
                    )
            finally:
                if waiter is not None and not waiter.done():
                    waiter.cancel()
                    await asyncio.wait(
                        [waiter],
                        timeout=max(0, cleanup_deadline - asyncio.get_running_loop().time()),
                    )
                if pidfd is not None:
                    try:
                        os.close(pidfd)
                        process["pidfd_closed"] = True
                        try:
                            os.fstat(pidfd)
                        except OSError as error:
                            if error.errno != errno.EBADF:
                                raise
                            process["pidfd_close_verified"] = True
                        else:
                            raise RuntimeError("pidfd_not_closed")
                        save_event("pidfd_closed", pid=None if proc is None else proc.pid)
                    except BaseException as error:
                        save_error("pidfd_close", error)

        cleanup_task = asyncio.create_task(cleanup(), name="scale-evidence-cleanup")
        while not cleanup_task.done():
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError as error:
                if primary is None:
                    primary = error
                save_error("caller_cancel_during_cleanup", error)
            except BaseException:
                break
        try:
            cleanup_task.result()
        except BaseException as error:
            if primary is None:
                primary = error
            save_error("cleanup", error)
        process["pending_owned_tasks"] = [
            task.get_name()
            for task in asyncio.all_tasks()
            if task not in before and task is not asyncio.current_task() and not task.done()
        ]
        process["cleanup_elapsed_seconds"] = time.monotonic() - process["cleanup_started_monotonic"]
        if (
            process["wait_observed"] is True
            and process["stdout_eof"] is True
            and process["stderr_eof"] is True
            and process["pidfd_close_verified"] is True
            and not process["pending_owned_tasks"]
        ):
            process["cleanup_status"] = "closed"
        for name, value in [("stdout", stdout), ("stderr", stderr)]:
            if value is not None:
                try:
                    path = root / ("header12-" + name + ".txt")
                    with path.open("wb") as stream:
                        stream.write(value)
                    save_event(name + "_file_closed", bytes=len(value), closed=stream.closed)
                except BaseException as error:
                    save_error(name + "_write", error)
        save_event("process_saved_before_consumer", cleanup_status=process["cleanup_status"])

    facts = consumer_error = None
    try:
        facts = json.loads(facts_path.read_text())
        process["consumer_status"] = "read"
        save_event("consumer_read")
    except BaseException as error:
        consumer_error = error
        process["consumer_status"] = "error"
        save_error("consumer_read", error)
    if publication_errors or publication_error_overflow:
        failure = primary if primary is not None else consumer_error
        if failure is None:
            failure = RuntimeError("scale_evidence_publication_failed")
        # 固定异常渠道携带真实已知事实，不把内存状态称为已发布process。
        failure.scale_evidence_process = process
        failure.scale_evidence_io_errors = tuple(publication_errors)
        failure.add_note("scale_evidence_publication_errors:" + json.dumps(publication_errors))
        if primary is None:
            primary = failure
    if primary is not None:
        raise primary
    if consumer_error is not None:
        raise consumer_error
    if process["cleanup_status"] != "closed" or process["errors"]:
        raise RuntimeError("scale_evidence_process_incomplete")
    return proc, process, facts


@pytest.mark.parametrize(
    "kind",
    [
        "success",
        "nonzero_missing_consumer",
        "timeout_missing_consumer",
        "cleanup_audit_failure",
        "process_receipt_io_error",
        "checkpoint_io_error",
    ],
)
async def test_scale_evidence_receipts(tmp_path, kind):
    """四项是标准库子进程采证资格，不执行产品或任何业务数据库。"""
    if kind in ["process_receipt_io_error", "checkpoint_io_error"]:
        import ast
        import errno
        import os
        import signal
        import sys
        from unittest.mock import patch

        source_text = await asyncio.to_thread(Path(__file__).read_text)
        tree = ast.parse(source_text)
        scale = next(
            node
            for node in tree.body
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "test_shared_modes_scale"
        )
        scale_driver = next(
            node.value.value
            for node in scale.body
            if isinstance(node, ast.Assign)
            and any(isinstance(name, ast.Name) and name.id == "driver" for name in node.targets)
        )
        driver_tree = ast.parse(scale_driver)
        checkpoint = next(
            node
            for node in driver_tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "_write_scale_checkpoint"
        )
        main = next(
            node
            for node in driver_tree.body
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "main"
        )
        finalbody = next(node for node in main.body if isinstance(node, ast.Try)).finalbody
        checkpoint_calls = [
            node
            for row in finalbody
            for node in ast.walk(row)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "_write_scale_checkpoint"
        ]

        def call_source(stage, status):
            return ast.unparse(
                next(
                    node
                    for node in checkpoint_calls
                    if ast.literal_eval(node.args[0]) == stage
                    and ast.literal_eval(node.args[1]) == status
                )
            )

        last_assert = max(
            index for index, node in enumerate(finalbody) if isinstance(node, ast.Assert)
        )
        final_error_source = ast.unparse(
            ast.Module(body=finalbody[last_assert + 1 :], type_ignores=[])
        )
        checkpoint_source = ast.unparse(checkpoint)
        cleanup_start = call_source("cleanup", "start")
        close_start = call_source("close", "start")
        close_done = call_source("close", "done")
        driver = (
            "import asyncio,errno,hashlib,json,os,stat,sys,time,traceback\n"
            "from pathlib import Path\nfrom unittest.mock import patch\n"
            "root=Path(sys.argv[1]);kind=sys.argv[2]\n" + checkpoint_source + "\n"
            "_write_scale_checkpoint('bootstrap','start',fixture=kind)\n"
            "print('evidence17-stdout',flush=True)\n"
            "print('evidence17-stderr',file=sys.stderr,flush=True)\n"
            "if kind=='process_receipt_io_error':\n"
            " _write_scale_checkpoint('fixture_wait','start')\n time.sleep(60)\n"
            "facts={'qualification_only':True,'no_product_database':True,'stage':'fixture',"
            "'error':None,'audit_errors':[],'close_order':[],'resource_close_checks':[],"
            "'checkpoint_open_failures':[]}\n"
            "real_open=os.open\n"
            "def failing_open(path,*args,**kwargs):\n"
            " if Path(path)==root/'header12-checkpoints.jsonl':\n"
            "  facts['checkpoint_open_failures'].append({'type':'OSError','errno':errno.EIO})\n"
            "  raise OSError(errno.EIO,'fixture_persistent_checkpoint_io')\n"
            " return real_open(path,*args,**kwargs)\n"
            "class Resource:\n"
            " def __init__(self,name):\n"
            "  self.name=name;self.file=(root/(name+'.txt')).open('w');self.fd=self.file.fileno()\n"
            " async def close(self):\n"
            "  await asyncio.sleep(0)\n"
            "  self.file.close()\n"
            "  try:os.fstat(self.fd)\n"
            "  except OSError as error:\n"
            "   if error.errno!=errno.EBADF:raise\n"
            "   facts['resource_close_checks'].append({'owner':self.name,'EBADF':True})\n"
            "  else:raise RuntimeError('fixture_resource_not_closed')\n"
            "  facts['close_order'].append(self.name)\n"
            "async def main():\n"
            " app=Resource('application');source_repo=Resource('source_repository')\n"
            " loop=asyncio.get_running_loop();previous_handler=loop.get_exception_handler()\n"
            " previous_profile=sys.getprofile();previous_trace=sys.gettrace()\n"
            " try:\n  raise ValueError('fixture_primary_checkpoint_io')\n"
            " except BaseException as error:\n"
            "  facts['error']={'type':type(error).__name__,'message':str(error)}\n  raise\n"
            " finally:\n"
            "  with patch('os.open',failing_open):\n"
            "   " + cleanup_start + "\n"
            "   for name,value in [('application',app),('source_repository',source_repo)]:\n"
            "    " + close_start + "\n"
            "    await value.close()\n"
            "    " + close_done + "\n"
            "  sys.setprofile(previous_profile);sys.settrace(previous_trace)\n"
            "  loop.set_exception_handler(previous_handler)\n"
            "  facts['hooks_restored']=(sys.getprofile() is previous_profile and "
            "sys.gettrace() is previous_trace and loop.get_exception_handler() "
            "is previous_handler)\n"
            + "\n".join("  " + line for line in final_error_source.splitlines())
            + "\n"
            "async def observed():\n"
            " before=asyncio.all_tasks();facts['fd_before']=len(os.listdir('/proc/self/fd'))\n"
            " try:await main()\n"
            " except BaseException as error:\n"
            "  facts['propagated_error']={'type':type(error).__name__,'message':str(error),"
            "'notes':getattr(error,'__notes__',[])}\n"
            "  facts['exception_checkpoint_errors']=list(getattr(error,'scale_c"
            "heckpoint_errors',()))\n"
            "  traceback.print_exception(error)\n"
            " finally:\n"
            "  await asyncio.sleep(0)\n"
            "  facts['pending']=[t.get_name() for t in asyncio.all_tasks() "
            "if t not in before and t is not asyncio.current_task() and not t.done()]\n"
            "  facts['fd_after']=len(os.listdir('/proc/self/fd'))\n"
            "asyncio.run(observed())\n"
            "(root/'evidence17-io-child.json').write_text(json.dumps(facts,indent=2))\n"
            "sys.exit(23 if facts.get('propagated_error') else 0)\n"
        )
        compile(driver, "evidence17-stdlib-fixture", "exec")
        assert ast.dump(
            next(node for node in ast.parse(driver).body if isinstance(node, ast.FunctionDef))
        ) == ast.dump(checkpoint)
        assert all(
            any(
                keyword.arg == "_defer_cleanup_error" and keyword.value.value is True
                for keyword in node.keywords
            )
            for node in checkpoint_calls
        )
        argv = [sys.executable, "-I", "-B", "-c", driver, str(tmp_path), kind]
        fd_before = len(os.listdir("/proc/self/fd"))
        tasks_before = asyncio.all_tasks()
        attempts = []
        stale_bytes = None
        actual_error = returned = None
        original_replace = os.replace

        def failing_replace(src, dst):
            nonlocal stale_bytes
            if Path(dst) == tmp_path / "header12-process.json":
                candidate = json.loads(Path(src).read_text())
                operation = candidate["events"][-1]["operation"]
                if attempts or operation == "execution_error":
                    if stale_bytes is None:
                        stale_bytes = Path(dst).read_bytes()
                    attempts.append(
                        {
                            "operation": operation,
                            "pid": candidate["pid"],
                            "type": "OSError",
                            "errno": errno.EIO,
                        }
                    )
                    raise OSError(errno.EIO, "fixture_persistent_process_io")
            return original_replace(src, dst)

        with patch(
            "os.replace",
            failing_replace if kind == "process_receipt_io_error" else original_replace,
        ):
            try:
                returned = await _run_scale_evidence_process(
                    argv,
                    tmp_path,
                    execution_timeout_seconds=0.5 if kind == "process_receipt_io_error" else 5,
                    source_driver_sha256=hashlib.sha256(scale_driver.encode()).hexdigest(),
                )
            except BaseException as error:
                actual_error = error
        published = json.loads((tmp_path / "header12-process.json").read_text())
        observed = getattr(actual_error, "scale_evidence_process", None)
        if returned is not None:
            observed = returned[1]
        child_path = tmp_path / "evidence17-io-child.json"
        child = json.loads(child_path.read_text()) if child_path.exists() else None
        helper = next(
            node
            for node in tree.body
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "_run_scale_evidence_process"
        )
        qualification = {
            "node": "tests/safe_eval/test_eval_scale.py::test_scale_evidence_receipts["
            + kind
            + "]",
            "qualification_only": True,
            "no_product_database": True,
            "uid": os.getuid(),
            "published_process": published,
            "observed_process_via_exception_or_return": observed,
            "published_process_is_final": (
                published["events"][-1]["operation"] == "consumer_read"
                and published["cleanup_status"] == "closed"
            ),
            "persistent_process_io_attempts": attempts,
            "fixture_rescue": False,
            "exception": None
            if actual_error is None
            else {
                "type": type(actual_error).__name__,
                "message": str(actual_error),
                "notes": getattr(actual_error, "__notes__", []),
            },
            "child": child,
            "argv": argv,
            "fd_before": fd_before,
            "fd_after": len(os.listdir("/proc/self/fd")),
            "pending": [
                task.get_name()
                for task in asyncio.all_tasks()
                if task not in tasks_before and not task.done()
            ],
        }
        (tmp_path / "evidence17-io-qualification.json").write_text(
            json.dumps(qualification, indent=2)
        )
        assert observed is not None and observed["argv"] == argv
        assert observed["uid"] == os.getuid() == 1001 and observed["pid"] > 0
        assert (
            observed["helper_ast_sha256"] == hashlib.sha256(ast.dump(helper).encode()).hexdigest()
        )
        assert (
            observed["checkpoint_ast_sha256"]
            == hashlib.sha256(ast.dump(checkpoint).encode()).hexdigest()
        )
        assert observed["source_driver_sha256"] == hashlib.sha256(scale_driver.encode()).hexdigest()
        assert observed["driver_sha256"] == hashlib.sha256(driver.encode()).hexdigest()
        assert observed["wait_observed"] is True
        assert observed["wait_returncode"] == observed["returncode"]
        assert observed["stdout_eof"] is True and observed["stderr_eof"] is True
        assert observed["pidfd_opened"] is True and observed["pidfd_closed"] is True
        assert observed["pidfd_close_verified"] is True and observed["cleanup_status"] == "closed"
        assert not observed["pending_owned_tasks"] and not qualification["pending"]
        assert qualification["fd_after"] <= qualification["fd_before"]
        assert observed["cleanup_elapsed_seconds"] <= observed["cleanup_timeout_seconds"] == 30
        assert observed["receipt_file_close_checks"] and all(observed["receipt_file_close_checks"])
        with pytest.raises(OSError) as closed:
            os.fstat(observed["pidfd"])
        assert closed.value.errno == errno.EBADF
        if kind == "process_receipt_io_error":
            assert isinstance(actual_error, TimeoutError) and returned is None
            assert observed["timed_out"] is True and observed["returncode"] == -signal.SIGTERM
            assert observed["signal_operations"] == [
                {
                    "pid": observed["pid"],
                    "signal": signal.SIGTERM,
                    "status": "sent",
                    "handle": "pidfd",
                }
            ]
            assert [row["operation"] for row in observed["errors"]] == [
                "execution",
                "consumer_read",
            ]
            failures = observed["evidence_publication_errors"]
            assert [row["operation"] for row in failures] == [row["operation"] for row in attempts]
            assert all(row["pid"] == observed["pid"] for row in attempts)
            assert all(row["type"] == "OSError" for row in failures) and len(failures) >= 8
            assert attempts[0]["operation"] == "execution_error"
            assert "cleanup_start" in [row["operation"] for row in attempts]
            assert "signal_start" in [row["operation"] for row in attempts]
            assert not observed["publication_error_overflow"]
            assert published["events"][-1]["operation"] == "pidfd_opened"
            assert published["timed_out"] is None and published["wait_returncode"] is None
            assert (tmp_path / "header12-process.json").read_bytes() == stale_bytes
            assert not (tmp_path / "header12-consumer.json").exists()
            assert any(
                "scale_evidence_publication_errors" in note
                for note in qualification["exception"]["notes"]
            )
        else:
            assert actual_error is None and returned[0].pid == observed["pid"]
            assert observed["returncode"] == 23 and observed["timed_out"] is False
            assert not observed["signal_operations"] and not attempts
            assert child["close_order"] == ["application", "source_repository"]
            assert child["resource_close_checks"] == [
                {"owner": "application", "EBADF": True},
                {"owner": "source_repository", "EBADF": True},
            ]
            assert child["hooks_restored"] is True and not child["pending"]
            assert child["fd_after"] <= child["fd_before"]
            assert child["propagated_error"]["type"] == "ValueError"
            assert child["propagated_error"]["message"] == "fixture_primary_checkpoint_io"
            errors = child["exception_checkpoint_errors"]
            assert len(errors) == len(child["checkpoint_open_failures"]) == 5
            assert [(row["stage"], row["status"]) for row in errors] == [
                ("cleanup", "start"),
                ("close", "start"),
                ("close", "done"),
                ("close", "start"),
                ("close", "done"),
            ]
            assert all(
                row["deferred"] and row["active_type"] == "ValueError" and row["type"] == "OSError"
                for row in errors
            )
            assert any(
                "scale_checkpoint_errors" in note for note in child["propagated_error"]["notes"]
            )
        return
    import ast
    import os
    import signal
    import sys

    tree = ast.parse(await asyncio.to_thread(Path(__file__).read_text))
    scale = next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "test_shared_modes_scale"
    )
    scale_driver = next(
        node.value.value
        for node in scale.body
        if isinstance(node, ast.Assign)
        and any(isinstance(name, ast.Name) and name.id == "driver" for name in node.targets)
    )
    checkpoint = next(
        node
        for node in ast.parse(scale_driver).body
        if isinstance(node, ast.FunctionDef) and node.name == "_write_scale_checkpoint"
    )
    helper = next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_run_scale_evidence_process"
    )
    checkpoint_source = ast.unparse(checkpoint)
    driver = (
        "import hashlib,json,os,stat,sys,time\nfrom pathlib import Path\n"
        "root=Path(sys.argv[1]);kind=sys.argv[2]\n" + checkpoint_source + "\n"
        "_write_scale_checkpoint('bootstrap','start',fixture=kind,actual_cwd=os.getcwd())\n"
        "facts={'qualification_only':True,'no_product_database':True,'pid':os.getpid(),"
        "'error':None,'audit_errors':[],'actual_cwd':os.getcwd()}\n"
        "_write_scale_checkpoint('bootstrap','done')\n"
        "print('fixture-stdout',flush=True)\n"
        "print('fixture-stderr',file=sys.stderr,flush=True)\n"
        "if kind=='timeout_missing_consumer':\n"
        " _write_scale_checkpoint('fixture_wait','start')\n time.sleep(60)\n"
        "if kind=='nonzero_missing_consumer':\n sys.exit(23)\n"
        "if kind=='cleanup_audit_failure':\n"
        " try:\n  raise ValueError('fixture_original_failure')\n"
        " except BaseException as error:\n"
        "  facts['error']={'type':type(error).__name__,'message':str(error)}\n"
        "  _write_scale_checkpoint('execution','error',error_type=type(error)._"
        "_name__,message=str(error))\n"
        " finally:\n"
        "  _write_scale_checkpoint('cleanup','start',primary_error_type=facts['error']['type'])\n"
        "  (root/'header12-consumer.json').write_text(json.dumps(facts))\n"
        "  try:\n"
        "   _write_scale_checkpoint('fixture_audit','start')\n"
        "   raise RuntimeError('fixture_audit_failure')\n"
        "  except BaseException as error:\n"
        "   facts['audit_errors'].append({'type':type(error).__name__,'message':str(error)})\n"
        "   _write_scale_checkpoint('fixture_audit','error',error_type=type(err"
        "or).__name__,message=str(error))\n"
        "  (root/'header12-consumer.json').write_text(json.dumps(facts))\n"
        " sys.exit(23)\n"
        "(root/'header12-consumer.json').write_text(json.dumps(facts))\n"
        "_write_scale_checkpoint('consumer','done')\n"
    )
    compile(scale_driver, "evidence16-scale-driver", "exec")
    compile(driver, "evidence16-stdlib-fixture", "exec")
    assert ast.dump(
        next(node for node in ast.parse(driver).body if isinstance(node, ast.FunctionDef))
    ) == ast.dump(checkpoint)
    argv = [sys.executable, "-I", "-B", "-c", driver, str(tmp_path), kind]
    before = asyncio.all_tasks()
    fd_before = len(os.listdir("/proc/self/fd"))
    actual_error = None
    returned = None
    child_cwd = None
    if kind in {"success", "timeout_missing_consumer"}:
        child_cwd = tmp_path / "explicit-child-cwd"
        await asyncio.to_thread(child_cwd.mkdir)
    try:
        returned = await _run_scale_evidence_process(
            argv,
            tmp_path,
            execution_timeout_seconds=0.5 if kind == "timeout_missing_consumer" else 5,
            source_driver_sha256=hashlib.sha256(scale_driver.encode()).hexdigest(),
            cwd=child_cwd,
        )
    except BaseException as error:
        actual_error = error
    process = json.loads((tmp_path / "header12-process.json").read_text())
    raw_checkpoints = (tmp_path / "header12-checkpoints.jsonl").read_bytes().splitlines(True)
    checkpoints = [json.loads(row) for row in raw_checkpoints]
    after = asyncio.all_tasks()
    facts = {
        "node": "tests/safe_eval/test_eval_scale.py::test_scale_evidence_receipts[" + kind + "]",
        "qualification_only": True,
        "no_product_database": True,
        "uid": os.getuid(),
        "process": process,
        "checkpoints": checkpoints,
        "exception": None
        if actual_error is None
        else {
            "type": type(actual_error).__name__,
            "message": str(actual_error),
        },
        "fd_before": fd_before,
        "fd_after": len(os.listdir("/proc/self/fd")),
        "pending": [task.get_name() for task in after if task not in before and not task.done()],
    }
    (tmp_path / "evidence16-qualification.json").write_text(json.dumps(facts, indent=2))
    assert process["argv"] == argv
    assert process["cwd"] == (os.getcwd() if child_cwd is None else str(child_cwd))
    if kind in {"success", "timeout_missing_consumer"}:
        assert process["cwd"] != os.getcwd()
        assert checkpoints[0]["known"]["actual_cwd"] == str(child_cwd)
        if returned is not None:
            assert returned[2]["actual_cwd"] == str(child_cwd)
    assert process["uid"] == os.getuid() == 1001 and process["pid"] > 0
    assert process["driver_sha256"] == hashlib.sha256(driver.encode()).hexdigest()
    assert process["source_driver_sha256"] == hashlib.sha256(scale_driver.encode()).hexdigest()
    assert process["helper_ast_sha256"] == hashlib.sha256(ast.dump(helper).encode()).hexdigest()
    assert (
        process["checkpoint_ast_sha256"]
        == hashlib.sha256(ast.dump(checkpoint).encode()).hexdigest()
    )
    assert all(
        row["run_identity"] == process["run_identity"]
        and row["pid"] == process["pid"]
        and row["uid"] == process["uid"]
        and row["driver_sha256"] == process["driver_sha256"]
        and row["helper_ast_sha256"] == process["helper_ast_sha256"]
        and row["checkpoint_ast_sha256"] == process["checkpoint_ast_sha256"]
        for row in checkpoints
    )
    assert [row["sequence"] for row in checkpoints] == list(range(1, len(checkpoints) + 1))
    assert 0 < len(checkpoints) <= 128 and all(len(row) <= 4096 for row in raw_checkpoints)
    assert process["wait_observed"] is True
    assert process["wait_returncode"] == process["returncode"]
    assert process["pidfd_opened"] is True and process["pidfd_closed"] is True
    assert process["pidfd_close_verified"] is True
    assert process["stdout_eof"] is True and process["stderr_eof"] is True
    assert process["cleanup_status"] == "closed" and not process["pending_owned_tasks"]
    assert process["receipt_file_close_checks"] and all(process["receipt_file_close_checks"])
    assert process["cleanup_elapsed_seconds"] <= process["cleanup_timeout_seconds"] == 30
    assert (tmp_path / "header12-stdout.txt").read_text() == "fixture-stdout\n"
    assert (tmp_path / "header12-stderr.txt").read_text() == "fixture-stderr\n"
    operations = [event["operation"] for event in process["events"]]
    consumer_operation = "consumer_read_error" if "missing_consumer" in kind else "consumer_read"
    assert operations.index("spawned") < operations.index("wait_done")
    assert operations.index("wait_done") < operations.index("process_saved_before_consumer")
    assert operations.index("pidfd_closed") < operations.index("process_saved_before_consumer")
    assert operations.index("process_saved_before_consumer") < operations.index(consumer_operation)
    assert not facts["pending"] and facts["fd_after"] <= facts["fd_before"]
    assert process["source_database"] is None and process["destination_database"] is None
    if kind == "success":
        assert actual_error is None and returned[0].pid == process["pid"]
        assert process["returncode"] == 0 and process["timed_out"] is False
        assert process["consumer_status"] == "read" and not process["errors"]
        assert not process["signal_operations"] and returned[2]["pid"] == process["pid"]
    elif kind == "nonzero_missing_consumer":
        assert isinstance(actual_error, FileNotFoundError)
        assert process["returncode"] == 23 and process["timed_out"] is False
        assert process["consumer_status"] == "error" and not process["signal_operations"]
        assert [row["operation"] for row in process["errors"]] == ["consumer_read"]
        assert not (tmp_path / "header12-consumer.json").exists()
    elif kind == "timeout_missing_consumer":
        assert isinstance(actual_error, TimeoutError) and process["timed_out"] is True
        assert process["execution_timeout_seconds"] == 0.5
        assert process["signal_operations"] == [
            {
                "pid": process["pid"],
                "signal": signal.SIGTERM,
                "status": "sent",
                "handle": "pidfd",
            }
        ]
        assert process["returncode"] == -signal.SIGTERM
        assert [row["operation"] for row in process["errors"]] == ["execution", "consumer_read"]
        assert process["errors"][0]["type"] == "TimeoutError"
        assert checkpoints[-1]["stage"] == "fixture_wait"
        assert not (tmp_path / "header12-consumer.json").exists()
    else:
        assert actual_error is None and process["returncode"] == 23
        assert process["timed_out"] is False and not process["signal_operations"]
        assert returned[2]["error"] == {"type": "ValueError", "message": "fixture_original_failure"}
        assert returned[2]["audit_errors"] == [
            {"type": "RuntimeError", "message": "fixture_audit_failure"}
        ]
        assert [(row["stage"], row["status"]) for row in checkpoints] == [
            ("bootstrap", "start"),
            ("bootstrap", "done"),
            ("execution", "error"),
            ("cleanup", "start"),
            ("fixture_audit", "start"),
            ("fixture_audit", "error"),
        ]


@pytest.mark.parametrize(
    "kind",
    [
        "cases101",
        "repetitions4",
        "workers3",
        "dataset_bytes",
        "request_bytes",
        "storage_reservation",
        "target_concurrency",
    ],
)
async def test_capacity_rejected_before_effect(tmp_path, monkeypatch, kind):
    import shutil
    import sqlite3
    from contextlib import closing

    from pydantic import ValidationError

    from krim_agent.contracts import ServiceError
    from krim_agent.evaluation.domain import clone

    app, _, _, _ = await seed(tmp_path)
    capture = {"kind": kind, "database": str(app.repository.path)}

    def counts():
        with closing(sqlite3.connect(app.repository.path.as_uri() + "?mode=ro", uri=True)) as db:
            db.execute("PRAGMA query_only=ON")
            return [
                db.execute("SELECT count(*) FROM " + name).fetchone()[0]
                for name in [
                    "eval_tasks",
                    "eval_trials",
                    "eval_task_events",
                    "eval_resource_receipts",
                ]
            ]

    try:
        data = DatasetSpec(name="capacity-matrix", version="1", cases=scale_cases(9))
        await app.repository.freeze(data)
        baseline = counts()
        calls = (
            app.owner.spawn_calls,
            app.adapter.execute_calls,
            app.repository.metric_registry.calls,
        )
        request = TaskSpec(
            request_key="capacity-" + kind,
            dataset=data.ref(),
            metrics=app.repository.metric_registry.specs(),
            execution=Budget(concurrency=2),
        )
        if kind == "cases101":
            body = data.model_dump(mode="json")
            body["cases"] = [
                clone(body["cases"][0]) | {"case_id": "c-" + str(i)} for i in range(101)
            ]
            with pytest.raises(ValidationError):
                DatasetSpec.model_validate(body)
        elif kind in {"repetitions4", "workers3"}:
            with pytest.raises(ValidationError):
                Budget.model_validate(
                    {"repetitions": 4} if kind == "repetitions4" else {"concurrency": 3}
                )
        elif kind == "dataset_bytes":
            body = data.model_dump(mode="json")
            body["cases"] = [
                clone(body["cases"][0])
                | {"case_id": "large-" + str(i), "files": {"large.txt": "x" * 65536}}
                for i in range(100)
            ]
            with pytest.raises((ValidationError, ValueError)):
                DatasetSpec.model_validate(body)
        elif kind == "request_bytes":
            body = request.model_dump(mode="json")
            body["metrics"][0]["config"] = {"data": "x" * 65537}
            with pytest.raises((ValidationError, ValueError)):
                TaskSpec.model_validate(body)
        else:
            if kind == "storage_reservation":
                usage = shutil.disk_usage(tmp_path)
                monkeypatch.setattr(
                    shutil, "disk_usage", lambda path: type(usage)(usage.total, usage.total, 0)
                )
            else:
                app.adapter._descriptor["max_concurrency"] = 1
            with pytest.raises(ServiceError) as error:
                await app.run(request)
            assert error.value.code == (
                "body_too_large" if kind == "storage_reservation" else "invalid_input"
            )
            capture["rejection"] = error.value.code
        assert counts() == baseline
        assert calls == (
            app.owner.spawn_calls,
            app.adapter.execute_calls,
            app.repository.metric_registry.calls,
        )
        capture.update(before=baseline, after=counts(), calls=calls)
    finally:
        await app.close()
        capture["closed"] = app.repository.closed and app.owner.closed
        await asyncio.to_thread(
            (tmp_path / "capacity-before-effect.json").write_text, json.dumps(capture, indent=2)
        )


@pytest.mark.parametrize(
    "kind",
    [
        "before_intent",
        "admission_before_ref",
        "after_ref",
        "collect",
        "scorer",
        "commit_return_lost",
        "repeat_cancel",
        "close",
        "timeout",
        "worker_error",
        "cleanup_unknown",
    ],
)
async def test_worker_failure_and_cancel_join(tmp_path, kind):
    import ast
    import sys

    tree = ast.parse(await asyncio.to_thread(Path(__file__).read_text))
    observed = next(
        n
        for n in tree.body
        if isinstance(n, ast.AsyncFunctionDef) and n.name == "test_shared_modes_scale"
    )
    accepted = next(
        n.value.value
        for n in observed.body
        if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "driver" for t in n.targets)
    )
    checkpoint = next(
        n
        for n in ast.parse(accepted).body
        if isinstance(n, ast.FunctionDef) and n.name == "_write_scale_checkpoint"
    )
    driver = (
        "import asyncio,fcntl,hashlib,json,os,sqlite3,stat,sys,time\n"
        "from pathlib import Path\nfrom contextlib import closing\n"
        "source,root=map(Path,sys.argv[1:3]);kind=sys.argv[3]\n"
        + ast.unparse(checkpoint)
        + "\n"
        + r"""_write_scale_checkpoint('bootstrap','start')
sys.path.insert(0,str(source))
from importlib.resources import files
from sqlalchemy import event
from sqlalchemy.pool import Pool
from krim_agent.contracts import ServiceError
from krim_agent.evaluation.app import open_eval
from krim_agent.evaluation.domain import Budget,DatasetSpec,TaskSpec
from krim_agent.evaluation.adapters.local_process import LocalProcessAdapter
from krim_agent.evaluation.metrics import MetricRegistry
facts=dict(kind=kind,uid=os.getuid(),database=str(root/'lifecycle.sqlite'),
    barriers=[],physical=[],retired=[],connections=[],error=None,close_errors=[])
for name in ['connect','checkout','reset','checkin','close']:
"""
        "    def record(*args,name=name):facts['connections'].append(di"
        "ct(event=name,connection=id(args[0]),at=time.monotonic()))"
        r"""
    event.listen(Pool,name,record)
ready=release=None
phase_calls={};actual_refs={};collecting=set();peer_gate=None
async def barrier(stage,key):
    group=phase_calls.setdefault(stage,set());group.add(key)
    facts['barriers'].append(dict(stage=stage,key=key))
    if len(group)==(1 if kind=='before_intent' else 2):
        if kind!='before_intent':
            assert len(actual_refs)==2 and len(set(actual_refs.values()))==2
            jobs=list(application.adapter.jobs.values())
            facts['two_inflight']=dict(refs=list(actual_refs.values()),
                ItemWork_count=len(application.kernel._inflight),
                processes=[dict(ref=j.ref.id,pid=j.process.pid,pidfd=j.pidfd,
"""
        "                    running=j.process.returncode is None,colle"
        "ctor_done=j.collector.done()) for j in jobs])"
        r"""
            assert facts['two_inflight']['ItemWork_count']==2
"""
        "            if kind!='scorer':assert all(j.process.returncode "
        "is None and j.pidfd>=0 and not j.collector.done() for j in job"
        "s)"
        r"""
        ready.set()
    await release.wait()
prepare=LocalProcessAdapter.prepare;execute=LocalProcessAdapter.execute;collect=LocalProcessAdapter.collect
score=MetricRegistry.score;retire=LocalProcessAdapter.retire
def physical(job):
    return dict(ref=job.ref.id,pid=job.process.pid,returncode=job.process.returncode,
        pidfd=job.pidfd,collector_done=job.collector.done(),stdout_eof=job.stdout_eof,
        stderr_eof=job.stderr_eof,streams_done=all(t.done() for t in job.stream_tasks),
        cleanup=job.cleanup_receipts)
async def prepared(adapter,trial):
    value=await prepare(adapter,trial)
    if kind=='before_intent':await barrier('after_real_prepare_before_intent',trial.case_id)
    return value
async def executed(adapter,value):
    ref=await execute(adapter,value)
    facts['actual_ref']=ref.id;actual_refs[value.spec.case_id]=ref.id
    if len(actual_refs)==2 and kind in {'scorer','commit_return_lost','cleanup_unknown'}:
"""
        "        for key in actual_refs:(peer_gate/('release-'+key)).wr"
        "ite_text('release real reference process')"
        r"""
"""
        "    if kind=='admission_before_ref':await barrier('actual_spaw"
        "n_before_ref_return',value.spec.case_id)"
        r"""
    return ref
async def collected(adapter,ref):
    case_id=adapter.jobs[ref.id].prepared.spec.case_id
"""
        "    if kind in {'collect','close','repeat_cancel','timeout'}:a"
        "wait barrier('actual_ref_before_collect',case_id)"
        r"""
    if kind=='worker_error':
        collecting.add(case_id)
        async with asyncio.timeout(3):
            while len(collecting)!=2:await asyncio.sleep(.001)
        if case_id=='scale-000':
            assert len(actual_refs)==2 and len(application.kernel._inflight)==2
            facts['two_inflight']=dict(refs=list(actual_refs.values()),ItemWork_count=2,
                processes=[dict(ref=j.ref.id,pid=j.process.pid,pidfd=j.pidfd,
"""
        "                    running=j.process.returncode is None,colle"
        "ctor_done=j.collector.done()) for j in adapter.jobs.values()])"
        r"""
"""
        "            assert all(j['running'] and j['pidfd']>=0 and not "
        "j['collector_done'] for j in facts['two_inflight']['processes'"
        "])"
        r"""
            (peer_gate/'release-scale-000').write_text('only failing worker runs to a real result')
    result=await collect(adapter,ref)
    if kind=='worker_error' and case_id=='scale-000':
        facts['actual_observation']=result;raise OSError('controlled_worker_result_IO_failure')
    return result
async def scored(registry,rule,case,output):
    value=await score(registry,rule,case,output)
    if kind=='scorer':await barrier('actual_score_before_return',case['case_id'])
    return value
def retired(adapter,ref,receipt):
    facts['retired'].append(physical(adapter.jobs[ref.id]))
    return retire(adapter,ref,receipt)
LocalProcessAdapter.prepare=prepared;LocalProcessAdapter.execute=executed
LocalProcessAdapter.collect=collected;LocalProcessAdapter.retire=retired;MetricRegistry.score=scored
async def main():
    global ready,release,peer_gate,application
    ready=asyncio.Event();release=asyncio.Event();app=None;before=asyncio.all_tasks()
    facts['fd_before']=len(os.listdir('/proc/self/fd'))
    try:
        app=await open_eval(root/'lifecycle.sqlite');application=app
        if kind!='before_intent':
            peer_gate=root/'peer-gate';peer_gate.mkdir()
            reference=app.adapter.worker
            wrapper=root/'two-reference-workers.py'
            code=('import os,runpy,time\nfrom pathlib import Path\n'
                +f'gate=Path({str(peer_gate)!r})\n'
                +"key=Path.cwd().name.rsplit('-',1)[0]\n"
                +"(gate/key).write_text(str(os.getpid()))\nend=time.monotonic()+5\n"
                +"while not (gate/('release-'+key)).exists():\n"
                +" if time.monotonic()>=end:raise RuntimeError('fixture_peer_gate_unreleased')\n"
                +" time.sleep(.001)\n"+f'runpy.run_path({str(reference)!r},run_name="__main__")\n')
            compile(code,'two-real-reference-workers','exec');wrapper.write_text(code)
            facts['worker_code']=code;facts['worker_sha256']=hashlib.sha256(code.encode()).hexdigest()
            app.adapter.worker=wrapper;app.adapter._descriptor['implementation_digest']=hashlib.sha256(wrapper.read_bytes()).hexdigest()
        body=json.loads(files('krim_agent.evaluation.examples').joinpath('scale_reconciliation.v1.json').read_text())
"""
        "        data=DatasetSpec.model_validate(body|{'cases':body['ca"
        "ses'][:1 if kind=='before_intent' else 2]})"
        r"""
        await app.repository.freeze(data)
        original_attach=app.repository.attach
        async def attached(*args,**kwargs):
            result=await original_attach(*args,**kwargs)
            if kind=='after_ref':await barrier('after_actual_ref_attach',args[0].case_id)
            return result
        app.repository.attach=attached
        original_write=app.repository._write;original_snapshot=app.repository._snapshot;lost=False
        async def snapshot(*args,**kwargs):
            if kind=='cleanup_unknown' and lost:raise OSError('controlled_confirmation_unavailable')
            return await original_snapshot(*args,**kwargs)
        async def written(operation):
            nonlocal lost
            value=await original_write(operation)
"""
        "            if kind in {'commit_return_lost','cleanup_unknown'"
        "} and 'commit_with_receipt' in operation.__qualname__ and not "
        "lost:"
        r"""
                lost=True;facts['actual_commit_before_lost_return']=True
                raise OSError('controlled_return_lost_after_commit')
            return value
        app.repository._write=written;app.repository._snapshot=snapshot
        request=TaskSpec(request_key='lifecycle-'+kind,dataset=data.ref(),
            metrics=app.repository.metric_registry.specs(),execution=Budget(concurrency=2,
            item_timeout_seconds=1 if kind=='timeout' else 20))
        view=await app.run(request);facts['task_id']=view['id']
        if kind not in {'worker_error','commit_return_lost','cleanup_unknown'}:
            async with asyncio.timeout(3):await ready.wait()
            if kind=='admission_before_ref':
                callers=[w for w in app.kernel._inflight.values() if w.admission is not None]
                assert callers and any(not w.admission.done() for w in callers)
            if kind=='close':
                close_task=asyncio.create_task(app.close())
                async with asyncio.timeout(3):
                    while not app.kernel.tasks[view['id']].cancelling():await asyncio.sleep(.001)
                release.set();await close_task
                state=None
            elif kind=='timeout':
                state=await app.kernel.wait(view['id']);release.set()
            else:
                cancelling=asyncio.create_task(app.kernel.cancel(view['id']))
                async with asyncio.timeout(3):
                    while not app.kernel.tasks[view['id']].cancelling():await asyncio.sleep(.001)
                release.set()
                state=await cancelling
                if kind=='repeat_cancel':
                    assert (await app.kernel.cancel(view['id']))['id']==view['id']
                    assert (await app.kernel.cancel(view['id']))['id']==view['id']
        else:
            try:state=await app.kernel.wait(view['id'])
            except ServiceError as error:
                assert kind=='cleanup_unknown' and error.code=='storage_unavailable'
                facts['durable_cleanup']='unknown';facts['error']=error.code;state=None
                assert not facts['retired'] and len(app.adapter.jobs)==2
        facts['state']=state
        if state is not None:
"""
        "            if kind=='commit_return_lost':assert state['status"
        "']=='completed' and lost and len(facts['retired'])==2"
        r"""
            elif kind=='timeout':
                assert state['status']=='completed'
                row=await app.repository.read_trial('local',view['id'],data.cases[0].case_id,0)
                assert row['quality']=='timed_out' and row['code']=='run_timeout'
"""
        "            elif kind=='worker_error':assert state['status']=="
        "'failed' and state['code']=='execution_error'"
        r"""
            else:assert state['status']=='aborted' and state['code']=='cancelled'
            assert (await app.run(request))['id']==view['id']
        assert app.owner.spawn_calls==(0 if kind=='before_intent' else 2)
        facts['spawn_calls']=app.owner.spawn_calls;facts['metric_calls']=app.repository.metric_registry.calls
        assert all(t.done() for t in app.kernel.tasks.values()) and not app.kernel._inflight
    finally:
        release.set()
        if app is not None:
            try:await app.close()
"""
        "            except BaseException as error:facts['close_errors'"
        "].append(dict(type=type(error).__name__,code=getattr(error,'co"
        "de',None)))"
        r"""
            facts.update(repository_closed=app.repository.closed,writer_fd=app.repository._writer_fd,
                owner_closed=app.owner.closed,kernel_closed=app.kernel.closed,
                repository_resources=app.repository.resources)
            facts['physical']=[physical(job) for job in app.owner.jobs.values()]
            assert app.repository.closed and app.repository._writer_fd is None and app.owner.closed
            assert all(r['returncode'] is not None and r['pidfd']==-1 and r['collector_done']
"""
        "                and r['stdout_eof'] and r['stderr_eof'] and r["
        "'streams_done'] for r in facts['physical'])"
        r"""
            assert not facts['close_errors']
"""
        "        facts['pending']=[repr(t) for t in asyncio.all_tasks()"
        " if t is not asyncio.current_task() and t not in before and no"
        "t t.done()]"
        r"""
        facts['fd_after']=len(os.listdir('/proc/self/fd'))
        assert not facts['pending'] and facts['fd_after']<=facts['fd_before']
        # 持久确认unknown的库只保留原件与hash；绝不SQL-open/recover/backup。
        if facts.get('durable_cleanup')!='unknown':
"""
        "            with closing(sqlite3.connect((root/'lifecycle.sqli"
        "te').as_uri()+'?mode=ro',uri=True)) as db:"
        r"""
                db.execute('PRAGMA query_only=ON')
"""
        "                with closing(sqlite3.connect(root/'lifecycle-b"
        "ackup.sqlite')) as dest:db.backup(dest)"
        r"""
            facts['backup']=str(root/'lifecycle-backup.sqlite')
        facts['database_sha256']=hashlib.sha256((root/'lifecycle.sqlite').read_bytes()).hexdigest()
        (root/'header12-consumer.json').write_text(json.dumps(facts,indent=2))
asyncio.run(main())
"""
    )
    compile(driver, "completion20-lifecycle", "exec")
    source = (await asyncio.to_thread(Path(__file__).resolve)).parents[2] / "src"
    argv = [sys.executable, "-I", "-B", "-c", driver, str(source), str(tmp_path), kind]
    proc, process, facts = await _run_scale_evidence_process(
        argv,
        tmp_path,
        execution_timeout_seconds=60,
        source_database=None,
        destination_database=str(tmp_path / "lifecycle.sqlite"),
    )
    assert proc.returncode == 0 and not process["timed_out"]
    assert facts["repository_closed"] and facts["owner_closed"] and not facts["pending"]


@pytest.mark.parametrize(
    "kind",
    [
        "same_key",
        "conflict",
        "cross_scope",
        "host_drift",
        "page_drift",
        "worker_entry",
        "self_wait",
    ],
)
async def test_same_key_fence_and_single_writer(tmp_path, monkeypatch, kind):
    from krim_agent.contracts import ServiceError
    from krim_agent.evaluation.repository import Repository

    app, _, _, _ = await seed(tmp_path)
    capture = {"kind": kind, "database": str(app.repository.path), "rejected_entries": 0}
    second = None
    before = app.owner.spawn_calls
    old_descriptor = dict(app.adapter._descriptor)
    old_execute = app.adapter.execute
    old_read = app.repository.read_trial
    capture["spawn_before"] = before
    capture["intent_observations"] = []
    try:
        with pytest.raises(ServiceError) as error:
            await Repository.open(app.repository.path)
        assert error.value.code == "database_busy"
        capture["active_second_writer"] = error.value.code
        data = DatasetSpec(name="key-matrix", version="1", cases=scale_cases(1))
        await app.repository.freeze(data)
        request = TaskSpec(
            request_key="key-" + kind,
            dataset=data.ref(),
            metrics=app.repository.metric_registry.specs(),
            execution=Budget(concurrency=2),
        )
        before = app.owner.spawn_calls
        original_intent = app.repository.intent
        original_page = app.repository.execution_page
        old_descriptor = dict(app.adapter._descriptor)
        old_execute = app.adapter.execute
        old_read = app.repository.read_trial

        async def rejected(*args, **kwargs):
            capture["rejected_entries"] += 1
            raise ServiceError("target_binding_changed")

        async def intent(trial):
            if kind == "self_wait":
                capture["self_wait"] = []
                for operation in [
                    lambda: app.kernel.wait(trial.task_id),
                    lambda: app.kernel.cancel(trial.task_id),
                    app.kernel.close,
                ]:
                    with pytest.raises(ServiceError) as error:
                        await operation()
                    assert error.value.code == "self_wait"
                    capture["self_wait"].append(error.value.code)
                assert app.kernel._close_task is None
                assert not app.kernel.tasks[trial.task_id].cancelling()
            value = await original_intent(trial)
            work = app.kernel._inflight[(trial.task_id, trial.case_id, trial.repetition)]
            capture["intent_observations"].append(
                {
                    "task_id": trial.task_id,
                    "case_id": trial.case_id,
                    "repetition": trial.repetition,
                    "admission": None if work.admission is None else id(work.admission),
                    "ref": None if work.ref is None else work.ref.id,
                }
            )
            if kind == "host_drift":
                app.adapter._descriptor["version"] = "changed-after-real-intent"
            elif kind == "worker_entry":
                app.adapter.execute = rejected
            return value

        async def page(*args, **kwargs):
            result = await original_page(*args, **kwargs)
            if kind == "page_drift":
                app.repository.read_trial = rejected
            return result

        monkeypatch.setattr(app.repository, "intent", intent)
        monkeypatch.setattr(app.repository, "execution_page", page)
        view = await app.run(request)
        state = await app.kernel.wait(view["id"])
        capture.update(
            task_id=view["id"],
            state=state,
            spawn_after=app.owner.spawn_calls,
            spawn_delta=app.owner.spawn_calls - before,
        )
        # 恢复测试制造的绑定漂移后才做外部关闭；不让替身负责清理。
        app.adapter._descriptor = old_descriptor
        app.adapter.execute = old_execute
        app.repository.read_trial = old_read
        capture["trial"] = await app.repository.read_trial("local", view["id"], "scale-000", 0)
        if kind in {"host_drift", "page_drift", "worker_entry"}:
            assert state["status"] == "failed" and state["code"] == "target_binding_changed"
            assert not state["cleanup_unknown"]
            replay_before = (
                app.owner.spawn_calls,
                app.adapter.execute_calls,
                app.repository.metric_registry.calls,
            )
            assert (await app.run(request))["id"] == view["id"]
            assert (
                app.owner.spawn_calls,
                app.adapter.execute_calls,
                app.repository.metric_registry.calls,
            ) == replay_before
            capture["original_key_replay_delta"] = [0, 0, 0]
            assert app.owner.spawn_calls == before and capture["rejected_entries"] == 0
        else:
            assert state["status"] == "completed" and app.owner.spawn_calls - before == 1
            if kind in {"same_key", "self_wait"}:
                assert (await app.run(request))["id"] == view["id"]
            elif kind == "conflict":
                changed = request.model_copy(
                    update={"execution": Budget(repetitions=2, concurrency=2)}
                )
                with pytest.raises(ServiceError) as error:
                    await app.run(changed)
                assert error.value.code == "key_conflict"
            else:
                cross = request.model_copy(update={"scope": "other"})
                other = await app.run(cross)
                assert other["id"] != view["id"]
                assert (await app.kernel.wait(other["id"]))["status"] == "completed"
                with pytest.raises(ServiceError) as error:
                    await app.repository.read_trial("other", view["id"], "scale-000", 0)
                assert error.value.code == "not_found"
        capture["spawn_delta"] = app.owner.spawn_calls - before
    finally:
        capture["spawn_after_finally"] = app.owner.spawn_calls
        capture["spawn_delta_finally"] = app.owner.spawn_calls - before
        capture["remaining_work"] = [
            {
                "task_id": key[0],
                "case_id": key[1],
                "repetition": key[2],
                "admission": None if work.admission is None else id(work.admission),
                "ref": None if work.ref is None else work.ref.id,
            }
            for key, work in app.kernel._inflight.items()
        ]
        app.adapter._descriptor = old_descriptor
        app.adapter.execute = old_execute
        app.repository.read_trial = old_read
        await app.close()
        assert app.repository._writer_fd is None
        second = await Repository.open(app.repository.path)
        try:
            assert second._writer_fd is not None
            capture["reopened_after_release"] = True
        finally:
            await second.close()
        capture["closed"] = app.repository.closed and second.closed and app.owner.closed
        await asyncio.to_thread(
            (tmp_path / "keys-fence-writer.json").write_text, json.dumps(capture, indent=2)
        )


@pytest.mark.parametrize(
    "kind", ["self", "one_case_regressed", "incomplete", "conditions", "tamper"]
)
async def test_compact_reports_per_case_gate(tmp_path, monkeypatch, kind):
    from sqlalchemy import select, update
    from test_continuous_eval import APPLICATIONS

    from krim_agent.contracts import ServiceError
    from krim_agent.evaluation.app import open_eval
    from krim_agent.evaluation.scale_store import eval_report_pages

    app, _, _, _ = await seed(tmp_path)
    capture = {"kind": kind, "database": str(app.repository.path)}
    control = tmp_path / "candidate-control"
    marker = tmp_path / "candidate-inflight"
    try:
        data = DatasetSpec(name="gate-matrix", version="1", cases=scale_cases(9))
        await app.repository.freeze(data)
        if kind in {"one_case_regressed", "incomplete"}:
            reference = app.adapter.worker
            worker = tmp_path / "gate-reference.py"
            code = (
                "import os,runpy,time\nfrom pathlib import Path\n"
                + f"control=Path({str(control)!r});marker=Path({str(marker)!r})\n"
                + f"kind={kind!r}\n"
                + "changed=control.exists() and Path.cwd().name=='scale-004-0'\n"
                + "if changed and kind=='incomplete':\n"
                + " Path('outputs/ledger.json').write_text('{partial_actual_output')\n"
                + " marker.write_text(str(os.getpid()))\n"
                + " end=time.monotonic()+5\n"
                + " while control.exists():\n"
                + "  if time.monotonic()>=end:raise RuntimeError('fixture_gate_unreleased')\n"
                + "  time.sleep(.001)\n"
                + "try:\n"
                + f" runpy.run_path({str(reference)!r},run_name='__main__')\n"
                + "except SystemExit as result:\n"
                + " if result.code not in (None,0):raise\n"
                + "if changed and kind=='one_case_regressed':\n"
                + " Path('outputs/ledger.json').write_text('{}\\n')\n"
            )
            compile(code, "real-gate-reference", "exec")
            await asyncio.to_thread(worker.write_text, code)
            app.adapter.worker = worker
            app.adapter._descriptor["implementation_digest"] = hashlib.sha256(
                code.encode()
            ).hexdigest()
            capture["worker_code"] = code
            capture["worker_sha256"] = hashlib.sha256(code.encode()).hexdigest()
        request = TaskSpec(
            request_key="gate-baseline",
            dataset=data.ref(),
            metrics=app.repository.metric_registry.specs(),
            execution=Budget(concurrency=2),
        )
        before = await app.run(request)
        assert (await app.kernel.wait(before["id"]))["status"] == "completed"
        old = await app.repository.report(before["id"])
        if kind == "self":
            after, new = before, old
        else:
            candidate_request = request.model_copy(update={"request_key": "gate-candidate"})
            if kind == "conditions":
                path = app.repository.path
                await app.close()
                capture["previous_owner_closed_before_new_conditions"] = (
                    app.owner.closed and app.repository.closed
                )
                assert capture["previous_owner_closed_before_new_conditions"]
                with monkeypatch.context() as conditions:
                    conditions.setenv("LANG", "C")
                    app = await open_eval(path)
                    APPLICATIONS.append(app)
                    after = await app.run(candidate_request)
                    assert (await app.kernel.wait(after["id"]))["status"] == "completed"
                    new = await app.repository.report(after["id"])
                    await app.close()
                assert new["conditions_fingerprint"] != old["conditions_fingerprint"]
                assert new["subject_fingerprint"] == old["subject_fingerprint"]
            else:
                if kind in {"one_case_regressed", "incomplete"}:
                    await asyncio.to_thread(control.write_text, "candidate target behavior")
                after = await app.run(candidate_request)
                if kind == "incomplete":
                    import threading

                    observer_stop = threading.Event()
                    observer_done = threading.Event()
                    marker_ready = asyncio.Event()
                    observer_loop = asyncio.get_running_loop()

                    def observe_marker():
                        try:
                            while not observer_stop.is_set():
                                if marker.exists():
                                    observer_loop.call_soon_threadsafe(marker_ready.set)
                                    return True
                                observer_stop.wait(0.001)
                            return False
                        finally:
                            observer_done.set()

                    marker_observer = asyncio.create_task(
                        asyncio.to_thread(observe_marker), name="scale-marker-observer"
                    )
                    marker_failure = None
                    try:
                        async with asyncio.timeout(3):
                            await asyncio.shield(marker_observer)
                            await marker_ready.wait()
                    except BaseException as error:
                        marker_failure = error
                    finally:
                        observer_stop.set()
                        while not marker_observer.done():
                            try:
                                await asyncio.shield(marker_observer)
                            except asyncio.CancelledError as error:
                                if marker_failure is None:
                                    marker_failure = error
                            except BaseException:
                                break
                        try:
                            marker_observed = marker_observer.result()
                        except BaseException as error:
                            if marker_failure is None:
                                marker_failure = error
                            elif error is not marker_failure:
                                marker_failure.add_note("marker observer failed: " + repr(error))
                        capture["marker_observer"] = {
                            "task_joined": marker_observer.done(),
                            "thread_work_joined": observer_done.is_set(),
                            "stop_set": observer_stop.is_set(),
                            "ready_from_actual_marker": marker_ready.is_set(),
                        }
                        if marker_failure is not None:
                            raise marker_failure
                    assert marker_observed
                    active = [
                        job
                        for job in app.owner.jobs.values()
                        if job.prepared.spec.case_id == "scale-004"
                        and job.prepared.spec.task_id == after["id"]
                    ]
                    assert len(active) == 1 and active[0].process.returncode is None
                    capture["actual_incomplete_process"] = {
                        "pid": active[0].process.pid,
                        "ref": active[0].ref.id,
                        "pidfd": active[0].pidfd,
                    }
                    result = await app.kernel.cancel(after["id"])
                    assert result["status"] == "aborted" and result["code"] == "cancelled"
                else:
                    assert (await app.kernel.wait(after["id"]))["status"] == "completed"
                new = await app.repository.report(after["id"])
        policy = ComparisonPolicy(
            baseline_id=before["id"],
            candidate_id=after["id"],
            baseline_hash=old["report_hash"],
            candidate_hash=new["report_hash"],
            allowed_subjects=list(
                dict.fromkeys([old["subject_fingerprint"], new["subject_fingerprint"]])
            ),
        )
        if kind == "tamper":
            async with app.repository.lock, app.repository.engine.begin() as conn:
                row = (
                    await conn.execute(
                        select(eval_report_pages.c.body).where(
                            eval_report_pages.c.task_id == after["id"],
                            eval_report_pages.c.page_index == 0,
                        )
                    )
                ).scalar_one()
                row["items"][0]["quality"] = "failed"
                await conn.execute(
                    update(eval_report_pages)
                    .where(
                        eval_report_pages.c.task_id == after["id"],
                        eval_report_pages.c.page_index == 0,
                    )
                    .values(body=row)
                )
        if app.repository.closed:
            reader = await open_eval(app.repository.path, readonly=True)
            try:
                result = await reader.repository.compare_paged("local", policy)
            finally:
                await reader.close()
        else:
            try:
                result = await app.repository.compare_paged("local", policy)
            except ServiceError as error:
                assert kind == "tamper" and error.code in {"facts_corrupt", "source_changed"}
                result = {"status": "invalid", "error": error.code}
        expected = (
            "pass" if kind == "self" else "fail" if kind == "one_case_regressed" else "invalid"
        )
        assert result["status"] == expected
        if kind == "one_case_regressed":
            assert any(r.get("case_id") == "scale-004" for r in result["reasons"])
            assert new["summary"]["pass_rate"] < 1 and new["summary"]["cell_coverage"] == 1
            row = await app.repository.read_trial("local", after["id"], "scale-004", 0)
            assert row["output"]["artifacts"]["ledger.json"]["text"] == "{}\n"
            assert any(score["value"] == 0 for score in row["scores"])
            capture["real_regressed_trial"] = row
        elif kind == "incomplete":
            assert new["summary"]["cell_coverage"] < 1 and not new["complete"]
            assert result["reasons"] == [{"code": "incomplete_coverage"}]
        elif kind == "conditions":
            assert result["reasons"] == [{"code": "incompatible_conditions_fingerprint"}]
        assert old["dataset_fingerprint"] == new["dataset_fingerprint"] == data.ref().fingerprint
        capture.update(
            baseline_id=before["id"],
            candidate_id=after["id"],
            result=result,
            baseline=old,
            candidate=new,
        )
    finally:
        if await asyncio.to_thread(control.exists):
            await asyncio.to_thread(control.unlink)
        await app.close()
        capture["closed"] = app.repository.closed and app.owner.closed
        await asyncio.to_thread(
            (tmp_path / "gate-per-case.json").write_text, json.dumps(capture, indent=2)
        )


@pytest.mark.parametrize("kind", ["intent", "executing", "committed", "queued"])
async def test_restart_never_replays(tmp_path, kind):
    import ast
    import sys

    tree = ast.parse(await asyncio.to_thread(Path(__file__).read_text))
    observed = next(
        n
        for n in tree.body
        if isinstance(n, ast.AsyncFunctionDef) and n.name == "test_shared_modes_scale"
    )
    accepted = next(
        n.value.value
        for n in observed.body
        if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "driver" for t in n.targets)
    )
    checkpoint = next(
        n
        for n in ast.parse(accepted).body
        if isinstance(n, ast.FunctionDef) and n.name == "_write_scale_checkpoint"
    )
    driver = (
        "import asyncio,hashlib,json,os,sqlite3,stat,sys,time\n"
        "from pathlib import Path\nfrom contextlib import closing\n"
        "source,root=map(Path,sys.argv[1:3]);kind=sys.argv[3]\n"
        + ast.unparse(checkpoint)
        + "\n"
        + r"""_write_scale_checkpoint('bootstrap','start')
sys.path.insert(0,str(source))
from importlib.resources import files
from sqlalchemy import event
from sqlalchemy.pool import Pool
from krim_agent.evaluation.app import open_eval
from krim_agent.evaluation.repository import Repository
from krim_agent.evaluation.domain import Budget,DatasetSpec,TaskSpec
class DriverLoss(BaseException):pass
facts=dict(kind=kind,uid=os.getuid(),database=str(root/'restart.sqlite'),connections=[],physical=[],error=None)
for name in ['connect','checkout','reset','checkin','close']:
"""
        "    def record(*args,name=name):facts['connections'].append(di"
        "ct(event=name,connection=id(args[0]),at=time.monotonic()))"
        r"""
    event.listen(Pool,name,record)
async def main():
    app=None;reopened=None;reader=None;before=asyncio.all_tasks()
    facts['fd_before']=len(os.listdir('/proc/self/fd'))
    try:
        app=await open_eval(root/'restart.sqlite')
        body=json.loads(files('krim_agent.evaluation.examples').joinpath('scale_reconciliation.v1.json').read_text())
        data=DatasetSpec.model_validate(body|{'cases':body['cases'][:1]})
        await app.repository.freeze(data)
        request=TaskSpec(request_key='restart-'+kind,dataset=data.ref(),metrics=app.repository.metric_registry.specs(),
            execution=Budget(concurrency=2))
        original_intent=app.repository.intent;original_attach=app.repository.attach
        original_commit=app.repository.commit_with_receipt;original_record=app.repository.record
        async def intent(*args,**kwargs):
            value=await original_intent(*args,**kwargs)
            if kind=='intent':raise DriverLoss('after_actual_intent_commit')
            return value
        async def attach(*args,**kwargs):
            value=await original_attach(*args,**kwargs)
            if kind=='executing':raise DriverLoss('after_actual_ref_attach_commit')
            return value
        async def commit(*args,**kwargs):
            value=await original_commit(*args,**kwargs)
            if kind=='committed':raise DriverLoss('after_actual_trial_and_receipt_commit')
            return value
        async def record(*args,**kwargs):
            if kind=='executing':raise DriverLoss('recording_unavailable_at_driver_loss')
            return await original_record(*args,**kwargs)
        app.repository.intent=intent;app.repository.attach=attach
        app.repository.commit_with_receipt=commit;app.repository.record=record
        if kind=='queued':
            view,created=await app.repository.begin(request.model_dump(mode='json'));assert created
        else:
            view=await app.run(request)
            try:await app.kernel.wait(view['id'])
            except DriverLoss as error:facts['loss_boundary']=str(error)
            else:raise AssertionError('driver_loss_not_observed')
        facts['task_id']=view['id'];facts['before_restart_spawn']=app.owner.spawn_calls
        assert app.owner.spawn_calls==(1 if kind in {'executing','committed'} else 0)
        app.repository.intent=original_intent;app.repository.attach=original_attach
        app.repository.commit_with_receipt=original_commit;app.repository.record=original_record
        await app.close()
        facts['physical']=[dict(pid=w.process.pid,returncode=w.process.returncode,pidfd=w.pidfd,
            collector_done=w.collector.done(),stdout_eof=w.stdout_eof,stderr_eof=w.stderr_eof,
            streams_done=all(t.done() for t in w.stream_tasks),cleanup=w.cleanup_receipts)
            for w in app.owner.jobs.values()]
"""
        "        assert app.owner.closed and app.repository.closed and "
        "all(t.done() for t in app.kernel.tasks.values())"
        r"""
        # 新夹具的持久损失，不读取任何历史故障库；资源先实际闭合再打开原writer恢复。
        reopened=await open_eval(root/'restart.sqlite',execution_enabled=False)
        state=await reopened.repository.snapshot(view['id'])
        row=await reopened.repository.read_trial('local',view['id'],data.cases[0].case_id,0)
        assert state['status']=='interrupted' and state['code']=='execution_unknown'
        assert row['status']==('unknown' if kind in {'intent','executing'} else
                              'finished' if kind=='committed' else 'interrupted')
        assert reopened.owner is None and reopened.repository.metric_registry.calls==0
        assert (await reopened.repository.by_key(request.model_dump(mode='json')))['id']==view['id']
        assert state.get('cleanup_unknown',False)==(kind in {'intent','executing'})
        facts.update(recovered_status=state['status'],trial_status=row['status'],
            cleanup_unknown=state.get('cleanup_unknown',False),restart_target_calls=0,
            restart_metric_calls=reopened.repository.metric_registry.calls)
        await reopened.close()
        reader=await Repository.open(root/'restart.sqlite',readonly=True)
        assert (await reader.by_key(request.model_dump(mode='json')))['id']==view['id']
    finally:
        if reader is not None:await reader.close()
        if reopened is not None:await reopened.close()
        if app is not None:await app.close()
"""
        "        facts['pending']=[repr(t) for t in asyncio.all_tasks()"
        " if t not in before and t is not asyncio.current_task() and no"
        "t t.done()]"
        r"""
        facts['fd_after']=len(os.listdir('/proc/self/fd'))
"""
        "        facts['closed']=app is not None and app.repository.clo"
        "sed and app.owner.closed and (reopened is None or reopened.rep"
        "ository.closed)"
        r"""
        assert facts['closed'] and not facts['pending'] and facts['fd_after']<=facts['fd_before']
"""
        "        assert all(w['pidfd']==-1 and w['returncode'] is not N"
        "one and w['collector_done'] and w['stdout_eof'] and w['stderr_"
        "eof'] and w['streams_done'] for w in facts['physical'])"
        r"""
        if not facts.get('cleanup_unknown',True):
"""
        "            with closing(sqlite3.connect((root/'restart.sqlite"
        "').as_uri()+'?mode=ro',uri=True)) as db:"
        r"""
                db.execute('PRAGMA query_only=ON')
                with closing(sqlite3.connect(root/'restart-backup.sqlite')) as dest:db.backup(dest)
            facts['backup']=str(root/'restart-backup.sqlite')
        else:facts['backup']='not_attempted_durable_unknown'
        facts['database_hash']=hashlib.sha256((root/'restart.sqlite').read_bytes()).hexdigest()
        (root/'header12-consumer.json').write_text(json.dumps(facts,indent=2))
asyncio.run(main())
"""
    )
    source = (await asyncio.to_thread(Path(__file__).resolve)).parents[2] / "src"
    argv = [sys.executable, "-I", "-B", "-c", driver, str(source), str(tmp_path), kind]
    proc, process, facts = await _run_scale_evidence_process(
        argv,
        tmp_path,
        execution_timeout_seconds=60,
        source_database=None,
        destination_database=str(tmp_path / "restart.sqlite"),
    )
    assert proc.returncode == 0 and process["cleanup_status"] == "closed"
    assert facts["closed"] and not facts["pending"]


@pytest.mark.parametrize("kind", ["external_finish", "mixed_intent", "finish_return_lost"])
async def test_no_admission_terminal_facts(tmp_path, kind):
    import ast
    import sys

    tree = ast.parse(await asyncio.to_thread(Path(__file__).read_text))
    shared = next(
        n
        for n in tree.body
        if isinstance(n, ast.AsyncFunctionDef) and n.name == "test_shared_modes_scale"
    )
    accepted = next(
        n.value.value
        for n in shared.body
        if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "driver" for t in n.targets)
    )
    checkpoint = next(
        n
        for n in ast.parse(accepted).body
        if isinstance(n, ast.FunctionDef) and n.name == "_write_scale_checkpoint"
    )
    driver = (
        "import asyncio,hashlib,json,os,sqlite3,stat,sys,time\n"
        "from pathlib import Path\nfrom contextlib import closing\n"
        "source,root=map(Path,sys.argv[1:3]);kind=sys.argv[3]\n"
        + ast.unparse(checkpoint)
        + "\n"
        + r"""_write_scale_checkpoint('bootstrap','start')
sys.path.insert(0,str(source))
from importlib.resources import files
from sqlalchemy import event,select
from sqlalchemy.pool import Pool
from krim_agent.evaluation.app import open_eval
from krim_agent.evaluation.domain import Budget,DatasetSpec,TaskSpec
from krim_agent.evaluation.repository import tasks,trials
facts=dict(node='tests/safe_eval/test_eval_scale.py::test_no_admission_terminal_facts['+kind+']',
    kind=kind,uid=os.getuid(),database=str(root/'no-admission.sqlite'),connections=[],
    calls={'prepare':0,'execute':0,'begin':0,'provider_stream':0},error=None,close_errors=[],
    intent_observations=[],terminal_commits=0,lost_returns=0)
observed={};observed_frames=set();provider_frames=set()
facts['profile_call_events']=0
for name in ['connect','checkout','reset','checkin','close']:
    def observe(*args,name=name):
        facts['connections'].append(dict(event=name,connection=id(args[0]),at=time.monotonic()))
    event.listen(Pool,name,observe)
def profile(frame,action,value):
    if action!='call':return
    facts['profile_call_events']+=1
    label=observed.get('codes',{}).get(frame.f_code)
    if label is not None and frame.f_locals.get('self') is observed.get('adapter'):
        # 协程恢复也产生call事件；同一真实帧只计首次进入。
        if frame not in observed_frames:
            observed_frames.add(frame)
            facts['calls'][label]+=1
"""
        "    if frame.f_globals.get('__name__','').startswith('krim_age"
        "nt.providers.') and frame.f_code.co_name=='stream':"
        r"""
        if frame not in provider_frames:
            provider_frames.add(frame)
            facts['calls']['provider_stream']+=1
async def main():
    app=None;old_descriptor=None;before=asyncio.all_tasks();sys.setprofile(profile)
    facts['fd_before']=len(os.listdir('/proc/self/fd'))
    try:
        app=await open_eval(root/'no-admission.sqlite')
        body=json.loads(files('krim_agent.evaluation.examples').joinpath('scale_reconciliation.v1.json').read_text())
"""
        "        data=DatasetSpec.model_validate(body|{'name':'no-admis"
        "sion-'+kind,'cases':body['cases'][:2 if kind=='mixed_intent' e"
        "lse 1]})"
        r"""
        await app.repository.freeze(data)
        request=TaskSpec(request_key='no-admission-'+kind,dataset=data.ref(),
            metrics=app.repository.metric_registry.specs(),execution=Budget(concurrency=2))
        old_descriptor=dict(app.adapter._descriptor)
        observed['adapter']=app.adapter
        observed['codes']={getattr(app.adapter,name).__func__.__code__:name
            for name in ['prepare','execute']}
        facts['observer_binding']={name:dict(filename=code.co_filename,
            qualname=code.co_qualname,first_line=code.co_firstlineno)
            for code,name in observed['codes'].items()}
        facts['observer_instance']=id(app.adapter)
        original_begin=app.repository.begin
        async def begin(*args,**kwargs):
            facts['calls']['begin']+=1
            return await original_begin(*args,**kwargs)
        app.repository.begin=begin
        if kind=='external_finish':
            value,created=await app.repository.begin(request.model_dump(mode='json'))
            assert created
            await app.repository.set_running(value['id'])
            page=await app.repository.execution_page(value['id'],None,limit=16)
            await app.repository.intent(page.trials[0])
            facts['external_intent_task']=value['id']
            await app.repository.finish(value['id'],'failed','target_binding_changed')
        else:
            second_intent=asyncio.Event();hold=asyncio.Event()
            original_intent=app.repository.intent;original_write=app.repository._write
            async def intent(trial):
                await original_intent(trial)
                work=app.kernel._inflight[(trial.task_id,trial.case_id,trial.repetition)]
                facts['intent_observations'].append(dict(task_id=trial.task_id,case_id=trial.case_id,
"""
        "                    repetition=trial.repetition,admission=None"
        " if work.admission is None else id(work.admission),"
        r"""
                    execution_ref=None if work.ref is None else work.ref.id))
                facts['task_id']=trial.task_id
                if kind=='mixed_intent' and trial.case_id=='scale-001':
                    second_intent.set()
                    await hold.wait()
                if kind=='mixed_intent':await second_intent.wait()
                app.adapter._descriptor['version']='changed-after-real-intent'
            async def write(operation):
                result=await original_write(operation)
                if facts.get('task_id'):
                    saved=await app.repository.snapshot(facts['task_id'])
                    if saved['status']=='failed' and saved.get('report_hash'):
                        facts['terminal_commits']+=1
                        facts['committed_before_lost_return']=dict(id=saved['id'],code=saved['code'],report_hash=saved['report_hash'])
                        if kind=='finish_return_lost' and not facts['lost_returns']:
                            facts['lost_returns']+=1
                            raise OSError('controlled-finish-commit-return-lost')
                return result
            app.repository.intent=intent
            if kind=='finish_return_lost':app.repository._write=write
            value=await app.run(request)
            await app.kernel.wait(value['id'])
        app.adapter._descriptor=old_descriptor
"""
        "        state=await app.repository.snapshot(value['id']);repor"
        "t=await app.repository.report(value['id'])"
        r"""
        async with app.repository.lock,app.repository.engine.begin() as conn:
"""
        "            rows=(await conn.execute(select(trials.c.body).whe"
        "re(trials.c.task_id==value['id']).order_by(trials.c.case_id)))"
        ".scalars().all()"
        r"""
        facts.update(task_id=value['id'],state=state,rows=rows,report_hash=report['report_hash'],
            spawn_calls=app.owner.spawn_calls,metric_calls=app.repository.metric_registry.calls,
            repository_healthy=app.repository.healthy,kernel_cleanup_unknown=app.kernel.cleanup_unknown)
        facts['observer_active']=sys.getprofile() is profile
        facts['observed_call_frames']=len(observed_frames)
        facts['observer_methods_unchanged']=all(
            getattr(app.adapter,name).__func__.__code__ is code
            for code,name in observed['codes'].items())
        facts['adapter_execute_calls']=app.adapter.execute_calls
        assert facts['observer_active'] and facts['profile_call_events']>0
        assert facts['observer_methods_unchanged'] and observed['adapter'] is app.adapter
        assert facts['observed_call_frames']==facts['calls']['prepare']+facts['calls']['execute']
        assert app.adapter.execute_calls==0
        assert state['status']=='failed' and state['finished_count']==0
"""
        "        assert all(row['execution_ref'] is None and row.get('o"
        "utput_digest') is None and row.get('content_hash') is None and"
        " not row['scores'] for row in rows)"
        r"""
"""
        "        assert app.owner.spawn_calls==0 and facts['calls']['ex"
        "ecute']==0 and app.repository.metric_registry.calls==0"
        r"""
        if kind=='external_finish':
"""
        "            assert rows[0]['status']=='unknown' and state['cod"
        "e']=='execution_unknown' and state['cleanup_unknown']"
        r"""
            assert not app.repository.healthy and facts['calls']['prepare']==0
        elif kind=='mixed_intent':
"""
        "            assert [(row['case_id'],row['status']) for row in "
        "rows]==[('scale-000','interrupted'),('scale-001','unknown')]"
        r"""
            assert rows[0]['code']=='target_binding_changed'
"""
        "            assert state['code']=='execution_unknown' and stat"
        "e['cleanup_unknown'] and not app.repository.healthy"
        r"""
            assert facts['calls']['prepare']==2
        else:
            assert rows[0]['status']=='interrupted' and rows[0]['code']=='target_binding_changed'
            assert state['code']=='target_binding_changed' and not state['cleanup_unknown']
            assert app.repository.healthy and not app.kernel.cleanup_unknown
            assert facts['terminal_commits']==1 and facts['lost_returns']==1
"""
        "            assert facts['committed_before_lost_return']==dict"
        "(id=value['id'],code=state['code'],report_hash=report['report_"
        "hash'])"
        r"""
            before_replay=dict(facts['calls']);metric_before=app.repository.metric_registry.calls
            assert (await app.run(request))['id']==value['id']
"""
        "            assert facts['calls']==before_replay and app.repos"
        "itory.metric_registry.calls==metric_before"
        r"""
"""
        "            assert facts['terminal_commits']==1 and facts['los"
        "t_returns']==1 and facts['calls']['prepare']==1"
        r"""
            facts['original_key_zero_replay']=True
        assert facts['calls']['provider_stream']==0
        facts['stage']='completed'
    except BaseException as error:
        facts['error']=dict(type=type(error).__name__,message=str(error));raise
    finally:
        sys.setprofile(None)
        observed_frames.clear();provider_frames.clear();observed.clear()
        if app is not None:
            if old_descriptor is not None:app.adapter._descriptor=old_descriptor
            try:await app.close()
"""
        "            except BaseException as error:facts['close_errors'"
        "].append(dict(type=type(error).__name__,message=str(error)));r"
        "aise"
        r"""
            finally:
                facts.update(repository_closed=app.repository.closed,writer_fd=app.repository._writer_fd,
"""
        "                    owner_closed=app.owner.closed,producer_don"
        "e=all(t.done() for t in app.kernel.tasks.values()),"
        r"""
                    inflight_empty=not app.kernel._inflight)
"""
        "        facts['pending']=[t.get_name() for t in asyncio.all_ta"
        "sks() if t not in before and not t.done()]"
        r"""
        facts['fd_after']=len(os.listdir('/proc/self/fd'))
        facts['database_hash']=hashlib.sha256((root/'no-admission.sqlite').read_bytes()).hexdigest()
"""
        "        with closing(sqlite3.connect((root/'no-admission.sqlit"
        "e').as_uri()+'?mode=ro',uri=True)) as db:"
        r"""
            db.execute('PRAGMA query_only=ON')
"""
        "            with closing(sqlite3.connect(root/'no-admission-ba"
        "ckup.sqlite')) as backup:db.backup(backup)"
        r"""
        facts['backup']=str(root/'no-admission-backup.sqlite')
        facts['backup_sha256']=hashlib.sha256((root/'no-admission-backup.sqlite').read_bytes()).hexdigest()
        (root/'header12-consumer.json').write_text(json.dumps(facts,indent=2))
asyncio.run(main())
"""
    )
    source = (await asyncio.to_thread(Path(__file__).resolve)).parents[2] / "src"
    argv = [sys.executable, "-I", "-B", "-c", driver, str(source), str(tmp_path), kind]
    proc, process, facts = await _run_scale_evidence_process(
        argv,
        tmp_path,
        execution_timeout_seconds=30,
        source_database=None,
        destination_database=str(tmp_path / "no-admission.sqlite"),
    )
    assert proc.returncode == 0 and process["cleanup_status"] == "closed"
    assert facts["repository_closed"] and facts["owner_closed"] and facts["writer_fd"] is None
    assert facts["producer_done"] and facts["inflight_empty"] and not facts["pending"]
    assert not facts["close_errors"] and facts["fd_after"] <= facts["fd_before"]


async def test_no_admission_fence_iteration_binding():
    import os
    from contextvars import copy_context

    from sqlalchemy import select
    from test_continuous_eval import APPLICATIONS

    from krim_agent.contracts import ServiceError
    from krim_agent.evaluation.app import open_eval
    from krim_agent.evaluation.ports import _admission_fence
    from krim_agent.evaluation.repository import trials

    root = Path(os.environ["KRIM_PN3_EVIDENCE"]) / "iteration-binding"
    await asyncio.to_thread(root.mkdir)
    database = root / "iteration-binding.sqlite"
    baseline_tasks = asyncio.all_tasks()
    fd_before = len(await asyncio.to_thread(os.listdir, "/proc/self/fd"))
    app = None
    descriptor = None
    first = {}
    capture = {
        "node": "tests/safe_eval/test_eval_scale.py::test_no_admission_fence_iteration_binding",
        "uid": os.getuid(),
        "database": str(database),
        "intent_cases": [],
        "rejections": [],
        "error": None,
    }
    try:
        app = await open_eval(database)
        APPLICATIONS.append(app)
        # 九Case是原分页布局的最小输入；仅前两Trial进入同一个worker。
        data = DatasetSpec(name="iteration-binding", version="1", cases=scale_cases(9))
        await app.repository.freeze(data)
        request = TaskSpec(
            request_key="iteration-binding",
            dataset=data.ref(),
            metrics=app.repository.metric_registry.specs(),
            execution=Budget(concurrency=1),
        )
        descriptor = dict(app.adapter._descriptor)
        original_intent = app.repository.intent
        context = _admission_fence()

        async def intent(trial):
            returned = await original_intent(trial)
            host = context.get()
            assert host is not None and host[0] is app.repository
            fence = host[1]
            work = app.kernel._inflight[(trial.task_id, trial.case_id, trial.repetition)]
            worker = asyncio.current_task()
            capture["intent_cases"].append(trial.case_id)
            if not first:
                first.update(
                    trial=trial,
                    work=work,
                    fence=fence,
                    context=copy_context(),
                    worker=worker,
                    live=fence._eval_owner[2],
                )
                assert first["live"][0]
            else:
                assert capture["intent_cases"] == ["scale-000", "scale-001"]
                assert worker is first["worker"]
                assert not first["live"][0] and fence._eval_owner[2][0]
                assert first["fence"]._eval_owner[2] is first["live"]
                state = await app.repository.snapshot(trial.task_id)
                assert state["layout"] == 3 and state["finished_count"] == 1
                assert app.adapter.execute_calls == app.owner.spawn_calls == 1
                for actual_trial, actual_work in [
                    (trial, work),
                    (first["trial"], first["work"]),
                ]:
                    with pytest.raises(ServiceError) as rejected:
                        first["context"].run(
                            first["fence"]._record_not_admitted, actual_trial, actual_work
                        )
                    assert rejected.value.code == "permission_denied"
                    capture["rejections"].append(rejected.value.code)
                assert context.get() is host
                capture["same_worker"] = True
                capture["prior_call_revoked"] = not first["live"][0]
                # 让真实post-intent fence拒绝，由当前Fence签发原未派发事实。
                app.adapter._descriptor["version"] = "iteration-binding-drift"
            return returned

        app.repository.intent = intent
        value = await app.run(request)
        state = await app.kernel.wait(value["id"])
        assert state["status"] == "failed" and state["code"] == "target_binding_changed"
        assert state["finished_count"] == 1 and not state["cleanup_unknown"]
        assert not app.kernel.cleanup_unknown and app.repository.healthy
        assert capture["intent_cases"] == ["scale-000", "scale-001"]
        assert capture["rejections"] == ["permission_denied", "permission_denied"]
        assert app.adapter.execute_calls == app.owner.spawn_calls == 1
        async with app.repository.lock, app.repository.engine.begin() as conn:
            rows = (
                (
                    await conn.execute(
                        select(trials.c.body)
                        .where(trials.c.task_id == value["id"])
                        .order_by(trials.c.case_id, trials.c.repetition)
                    )
                )
                .scalars()
                .all()
            )
        assert len(rows) == 9
        assert rows[0]["status"] == "finished" and rows[0]["execution_ref"] is not None
        assert rows[1]["status"] == "interrupted"
        assert rows[1]["code"] == "target_binding_changed"
        assert rows[1]["execution_ref"] is None and rows[1]["content_hash"] is None
        assert not rows[1]["scores"]
        report = await app.repository.report(value["id"])
        app.adapter._descriptor = descriptor
        counts = (
            app.owner.spawn_calls,
            app.adapter.execute_calls,
            app.repository.metric_registry.calls,
        )
        assert (await app.run(request))["id"] == value["id"]
        assert counts == (
            app.owner.spawn_calls,
            app.adapter.execute_calls,
            app.repository.metric_registry.calls,
        )
        capture.update(
            task_id=value["id"],
            state=state,
            rows=rows,
            report_hash=report["report_hash"],
            original_key_zero_replay=True,
            execute_calls=app.adapter.execute_calls,
            spawn_calls=app.owner.spawn_calls,
            fixture_backup_owner="continuous_resources",
        )
    except BaseException as error:
        capture["error"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        if app is not None:
            if descriptor is not None:
                app.adapter._descriptor = descriptor
            try:
                await app.close()
            finally:
                capture.update(
                    repository_closed=app.repository.closed,
                    owner_closed=app.owner.closed,
                    writer_fd=app.repository._writer_fd,
                    producer_done=all(t.done() for t in app.kernel.tasks.values()),
                    inflight_empty=not app.kernel._inflight,
                    repository_events=app.repository.resources,
                )
        capture["pending"] = [
            t.get_name() for t in asyncio.all_tasks() if t not in baseline_tasks and not t.done()
        ]
        capture["fd_before"] = fd_before
        capture["fd_after"] = len(await asyncio.to_thread(os.listdir, "/proc/self/fd"))
        await asyncio.to_thread(
            (root / "iteration-binding.json").write_text, json.dumps(capture, indent=2)
        )
    assert capture["repository_closed"] and capture["owner_closed"]
    assert capture["writer_fd"] is None and capture["producer_done"]
    assert capture["inflight_empty"] and not capture["pending"]
    assert capture["fd_after"] <= fd_before


async def test_scale_evidence_source_read_cancel():
    import os
    import sys
    import threading
    from unittest.mock import patch

    root = Path(os.environ["KRIM_PN3_EVIDENCE"]) / "source-read-cancel"
    await asyncio.to_thread(root.mkdir)
    loop = asyncio.get_running_loop()
    source = Path(__file__)
    real_read = Path.read_text
    started = asyncio.Event()
    release = threading.Event()
    thread_done = threading.Event()
    before = asyncio.all_tasks()
    fd_before = len(await asyncio.to_thread(os.listdir, "/proc/self/fd"))
    helper = read_task = None
    spawn_calls = 0
    read_calls = 0
    capture = {
        "node": "tests/safe_eval/test_eval_scale.py::test_scale_evidence_source_read_cancel",
        "uid": os.getuid(),
        "database": None,
        "error": None,
    }

    def blocked_read(path, *args, **kwargs):
        nonlocal read_calls
        if path != source:
            return real_read(path, *args, **kwargs)
        read_calls += 1
        loop.call_soon_threadsafe(started.set)
        try:
            release.wait()
            return real_read(path, *args, **kwargs)
        finally:
            thread_done.set()

    async def rejected_spawn(*args, **kwargs):
        nonlocal spawn_calls
        spawn_calls += 1
        raise AssertionError("source-read cancellation must precede every spawn")

    async def next_turn():
        turn = loop.create_future()
        loop.call_soon(turn.set_result, None)
        await turn

    try:
        with (
            patch.object(Path, "read_text", blocked_read),
            patch.object(asyncio, "create_subprocess_exec", rejected_spawn),
        ):
            helper = asyncio.create_task(
                _run_scale_evidence_process(
                    [sys.executable, "-I", "-B", "-c", "pass"],
                    root,
                    execution_timeout_seconds=210,
                ),
                name="source-read-cancel-helper",
            )
            try:
                async with asyncio.timeout(3):
                    await started.wait()
                reads = [
                    t
                    for t in asyncio.all_tasks()
                    if t not in before and t.get_name() == "scale-evidence-source-read"
                ]
                assert len(reads) == 1
                read_task = reads[0]
                helper.cancel("source-read-first-cancel")
                await next_turn()
                assert helper.cancelling() == 1 and not helper.done()
                helper.cancel("source-read-second-cancel")
                await next_turn()
                assert helper.cancelling() == 2 and not helper.done()
                assert not read_task.done() and not read_task.cancelling()
                assert not thread_done.is_set() and spawn_calls == 0
                release.set()
                with pytest.raises(asyncio.CancelledError) as cancelled:
                    await helper
                assert cancelled.value.args == ("source-read-first-cancel",)
                assert helper.done() and helper.cancelled()
                assert read_task.done() and not read_task.cancelled()
                assert isinstance(read_task.result(), str)
                assert read_calls == 1 and thread_done.is_set() and spawn_calls == 0
                capture.update(repeated_cancel_joined=True, original_cancel_propagated=True)
            finally:
                release.set()
                if not helper.done():
                    helper.cancel("source-read-test-cleanup")
                while not helper.done():
                    try:
                        await asyncio.shield(helper)
                    except asyncio.CancelledError:
                        continue
                    except BaseException:
                        break
                if not helper.cancelled():
                    helper.exception()
                if read_task is not None:
                    while not read_task.done():
                        try:
                            await asyncio.shield(read_task)
                        except asyncio.CancelledError:
                            continue
                    read_task.result()
    except BaseException as error:
        capture["error"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        capture.update(
            spawn_calls=spawn_calls,
            read_calls=read_calls,
            thread_work_joined=thread_done.is_set(),
            read_task_done=None if read_task is None else read_task.done(),
            helper_done=None if helper is None else helper.done(),
            fd_before=fd_before,
            fd_after=len(await asyncio.to_thread(os.listdir, "/proc/self/fd")),
            pending=[t.get_name() for t in asyncio.all_tasks() if t not in before and not t.done()],
            borrowed_executor_threads_not_claimed_zero=True,
        )
        await asyncio.to_thread(
            (root / "source-read-cancel.json").write_text, json.dumps(capture, indent=2)
        )
    assert capture["spawn_calls"] == 0 and capture["thread_work_joined"]
    assert capture["read_task_done"] and capture["helper_done"]
    assert not capture["pending"] and capture["fd_after"] <= fd_before
