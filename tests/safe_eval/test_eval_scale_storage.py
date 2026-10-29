"""本轮新合成历史库；真实旧产品种库，不借旧失败账本。"""

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest
from test_continuous_eval import DATABASES
from test_continuous_eval import continuous_resources as continuous_resources

from krim_agent.evaluation.repository import Repository

HISTORY_CODE = (
    "import asyncio,json,sys\n"
    "from pathlib import Path\n"
    "sys.path.insert(0,sys.argv[1])\n"
    "from importlib.resources import files\n"
    "from krim_agent.evaluation.app import open_eval\n"
    "from krim_agent.evaluation.domain import DatasetSpec,TaskSpec\n"
    "async def main():\n"
    "    app=await open_eval(Path(sys.argv[2]))\n"
    "    try:\n"
    "        body=json.loads(files('krim_agent.evaluation.examples').join"
    "path('reconciliation.v1.json').read_text())\n"
    "        data=DatasetSpec.model_validate(body|{'cases':body['cases']["
    ":1]})\n"
    "        await app.repository.freeze(data)\n"
    "        request=TaskSpec(request_key='scale-history',dataset=data.re"
    "f(),metrics=app.repository.metric_registry.specs())\n"
    "        view=await app.run(request)\n"
    "        state=await app.kernel.wait(view['id'])\n"
    "        report=await app.repository.report(view['id'])\n"
    "        assert state['status']=='completed'\n"
    "        result={'id':view['id'],'request':request.model_dump(mode='j"
    "son'),'report':report,\n"
    "                'source':__import__('krim_agent.evaluation.repositor"
    "y',fromlist=['x']).__file__,\n"
    "                'execute_calls':app.adapter.execute_calls}\n"
    "    finally:\n"
    "        await app.close()\n"
    "    assert app.repository.closed and app.repository._writer_fd is No"
    "ne\n"
    "    assert all(t.done() for t in app.kernel.tasks.values())\n"
    "    assert all(j.process.returncode is not None and j.pidfd==-1 and "
    "j.collector.done() for j in app.owner.jobs.values())\n"
    "    print(json.dumps(result))\n"
    "asyncio.run(main())\n"
)


def seed_history(tmp_path, version):
    root = Path(os.environ["KRIM_PN3_ROOT"])
    folder = "pn2-c39ff" if version == 1 else "pn3-c-ec1b"
    source = root / "materials/product" / folder / "source/src"
    database = tmp_path / "history.sqlite"
    DATABASES.append(database)
    compile(HISTORY_CODE, "fixed-scale-history", "exec")
    argv = [sys.executable, "-I", "-B", "-c", HISTORY_CODE, str(source), str(database)]
    env = {
        k: os.environ[k]
        for k in ["PATH", "LANG", "LD_LIBRARY_PATH", "HOME", "TMPDIR", "PYTHONDONTWRITEBYTECODE"]
    }
    with subprocess.Popen(
        argv, cwd=tmp_path, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ) as child:
        pidfd = os.pidfd_open(child.pid)
        try:
            out, err = child.communicate(timeout=30)
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
            "returncode": child.returncode,
            "waited": child.poll() is not None,
            "pidfd_closed": True,
            "code_sha256": hashlib.sha256(HISTORY_CODE.encode()).hexdigest(),
        }
    (tmp_path / "history-command.json").write_text(json.dumps(receipt, indent=2))
    (tmp_path / "history-stdout.txt").write_bytes(out)
    (tmp_path / "history-stderr.txt").write_bytes(err)
    assert receipt["returncode"] == 0, err.decode()
    facts = json.loads(out)
    assert Path(facts["source"]).is_relative_to(source)
    assert facts["execute_calls"] == 1
    with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == version
    return database, facts


@pytest.mark.parametrize("kind", ["v1", "v2", "readonly", "rollback", "legacy_hashes"])
async def test_scale_schema_migration_preserves_legacy(tmp_path, monkeypatch, kind):
    import asyncio

    if kind in {"readonly", "rollback", "legacy_hashes"}:
        from sqlalchemy.ext.asyncio import AsyncConnection

        database, facts = await asyncio.to_thread(seed_history, tmp_path, 2)

        def records():
            with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as conn:
                conn.execute("PRAGMA query_only=ON")
                names = {
                    r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                }
                rows = {
                    name: conn.execute("SELECT * FROM " + name + " ORDER BY 1,2").fetchall()
                    for name in sorted(names)
                }
                return conn.execute("PRAGMA user_version").fetchone()[0], names, rows

        version, names, rows = await asyncio.to_thread(records)
        before_hash = hashlib.sha256(await asyncio.to_thread(database.read_bytes)).hexdigest()
        captured = {
            "kind": kind,
            "database": str(database),
            "task_id": facts["id"],
            "version_before": version,
            "database_hash_before": before_hash,
        }
        if kind == "readonly":
            reader = await Repository.open(database, readonly=True)
            try:
                assert await reader.report(facts["id"]) == facts["report"]
                assert (await reader.by_key(facts["request"]))["id"] == facts["id"]
            finally:
                await reader.close()
            assert await asyncio.to_thread(records) == (version, names, rows)
            assert (
                hashlib.sha256(await asyncio.to_thread(database.read_bytes)).hexdigest()
                == before_hash
            )
        elif kind == "rollback":
            run_sync = AsyncConnection.run_sync
            captured["actual_DDL"] = []

            async def fault(conn, operation, *args, **kwargs):
                value = await run_sync(conn, operation, *args, **kwargs)
                table = getattr(operation, "__self__", None)
                if getattr(table, "name", None) in {"dataset_cases", "eval_report_pages"}:
                    captured["actual_DDL"].append(table.name)
                    if table.name == "eval_report_pages":
                        raise OSError("controlled_after_actual_second_DDL_before_commit")
                return value

            monkeypatch.setattr(AsyncConnection, "run_sync", fault)
            with pytest.raises(OSError, match="controlled_after_actual_second_DDL"):
                await Repository.open(database)
            monkeypatch.setattr(AsyncConnection, "run_sync", run_sync)
            assert captured["actual_DDL"] == ["dataset_cases", "eval_report_pages"]
            assert await asyncio.to_thread(records) == (version, names, rows)
            writer = await Repository.open(database)
            try:
                assert await writer.report(facts["id"]) == facts["report"]
            finally:
                await writer.close()
        else:
            writer = await Repository.open(database)
            try:
                assert await writer.report(facts["id"]) == facts["report"]
                assert (await writer.by_key(facts["request"]))["id"] == facts["id"]
            finally:
                await writer.close()
            current_version, current_names, current_rows = await asyncio.to_thread(records)
            assert current_version == 3 and names < current_names
            for name in names:
                assert current_rows[name] == rows[name]
            captured["report_hash_preserved"] = facts["report"]["report_hash"]
        await asyncio.to_thread(
            (tmp_path / "migration-behavior.json").write_text, json.dumps(captured, indent=2)
        )
        return
    database, facts = await asyncio.to_thread(seed_history, tmp_path, 1 if kind == "v1" else 2)
    writer = await Repository.open(database)
    try:
        with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as conn:
            assert conn.execute("PRAGMA user_version").fetchone()[0] == 3
            names = {
                r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            assert {"dataset_cases", "eval_report_pages", "eval_resource_receipts"} <= names
        assert await writer.report(facts["id"]) == facts["report"]
        assert (await writer.by_key(facts["request"]))["id"] == facts["id"]
    finally:
        await writer.close()
    reader = await Repository.open(database, readonly=True)
    try:
        assert await reader.report(facts["id"]) == facts["report"]
        assert (await reader.by_key(facts["request"]))["id"] == facts["id"]
    finally:
        await reader.close()
    second = await Repository.open(database)
    try:
        assert await second.report(facts["id"]) == facts["report"]
    finally:
        await second.close()


@pytest.mark.parametrize(
    "kind",
    [
        "atomic_visibility",
        "receipt_failure_rollback",
        "commit_reply_lost",
        "confirmation_unavailable",
        "trial_only_confirmation",
        "receipt_only_confirmation",
        "same_key_conflict",
        "two_worker_count",
    ],
)
async def test_atomic_trial_receipt_commit(tmp_path, monkeypatch, kind):
    from test_continuous_eval import seed
    from test_eval_scale import scale_cases

    from krim_agent.contracts import ServiceError
    from krim_agent.evaluation import scale_store
    from krim_agent.evaluation.domain import Budget, DatasetSpec, TaskSpec, clone

    app, _, _, _ = await seed(tmp_path)
    capture = {
        "kind": kind,
        "database": str(app.repository.path),
        "before": {},
        "readers": [],
        "combined_writes": 0,
        "combined_confirms": 0,
        "reply_faults": 0,
        "receipt_SQL_executed": 0,
        "conflicts": [],
    }
    before_spawn = app.owner.spawn_calls
    before_score = app.repository.metric_registry.calls
    fail_confirmation = False
    lost_once = False
    original_mutate = scale_store.mutate_trial
    original_receipt = scale_store.save_resource_receipt
    original_write = app.repository._write
    original_confirmed = app.repository._confirmed
    original_snapshot = app.repository._snapshot

    def reader(identity, case_id, repetition):
        # 真正第二SQLite读取者，明确只读URI且显式关闭；不走writer的锁。
        with closing(sqlite3.connect(app.repository.path.as_uri() + "?mode=ro", uri=True)) as db:
            db.execute("PRAGMA query_only=ON")
            task = json.loads(
                db.execute("SELECT body FROM eval_tasks WHERE id=?", (identity,)).fetchone()[0]
            )
            row = json.loads(
                db.execute(
                    "SELECT body FROM eval_trials WHERE task_id=? AND case_id=? AND repetition=?",
                    (identity, case_id, repetition),
                ).fetchone()[0]
            )
            receipts = db.execute(
                "SELECT count(*) FROM eval_resource_receipts WHERE task_id=?", (identity,)
            ).fetchone()[0]
            events = db.execute(
                "SELECT count(*) FROM eval_task_events WHERE task_id=? "
                "AND json_extract(body,'$.action')='commit'",
                (identity,),
            ).fetchone()[0]
        return {"task": task, "trial": row, "receipts": receipts, "commit_events": events}

    async def mutate(conn, task_id, case_id, repetition, action, payload, *args, **kwargs):
        if action == "commit":
            key = f"{case_id}:{repetition}"
            capture["task_id"] = task_id
            capture["before"].setdefault(key, reader(task_id, case_id, repetition))
        return await original_mutate(
            conn, task_id, case_id, repetition, action, payload, *args, **kwargs
        )

    async def receipt(conn, task_id, case_id, repetition, value, *args, **kwargs):
        result = await original_receipt(conn, task_id, case_id, repetition, value, *args, **kwargs)
        capture["receipt_SQL_executed"] += 1
        visible = reader(task_id, case_id, repetition)
        capture["readers"].append(visible)
        if kind == "atomic_visibility":
            assert visible["trial"]["status"] != "finished"
            assert visible["receipts"] == 0
        if kind == "receipt_failure_rollback":
            # 原save已在实际事务中写入收据，然后才注入明确业务失败。
            raise ServiceError("key_conflict")
        return result

    async def snapshot(conn, identity):
        nonlocal fail_confirmation
        if fail_confirmation:
            fail_confirmation = False
            capture["confirmation_read_failed"] = True
            raise OSError("controlled_read_unavailable_after_actual_commit")
        return await original_snapshot(conn, identity)

    async def confirmed(operation, confirm):
        combined = "commit_with_receipt" in operation.__qualname__

        async def observed_confirm():
            if combined:
                capture["combined_confirms"] += 1
            return await confirm()

        return await original_confirmed(operation, observed_confirm)

    async def write(operation):
        nonlocal lost_once, fail_confirmation
        combined = "commit_with_receipt" in operation.__qualname__
        if combined:
            capture["combined_writes"] += 1
        result = await original_write(operation)
        if (
            combined
            and not lost_once
            and kind
            in {
                "commit_reply_lost",
                "confirmation_unavailable",
                "trial_only_confirmation",
                "receipt_only_confirmation",
            }
        ):
            lost_once = True
            identity = capture["task_id"]
            if kind in {"trial_only_confirmation", "receipt_only_confirmation"}:
                async with app.repository.lock, app.repository.engine.begin() as conn:
                    if kind == "trial_only_confirmation":
                        await conn.exec_driver_sql(
                            "DELETE FROM eval_resource_receipts WHERE task_id=?", (identity,)
                        )
                    else:
                        old = capture["before"]["scale-000:0"]["trial"]
                        await conn.exec_driver_sql(
                            "UPDATE eval_trials SET body=? WHERE task_id=?",
                            (json.dumps(old), identity),
                        )
                capture["injected_half_state"] = reader(identity, "scale-000", 0)
            if kind == "confirmation_unavailable":
                fail_confirmation = True
            capture["reply_faults"] += 1
            raise OSError("controlled_reply_lost_after_real_write_return")
        return result

    monkeypatch.setattr(scale_store, "mutate_trial", mutate)
    monkeypatch.setattr(scale_store, "save_resource_receipt", receipt)
    monkeypatch.setattr(app.repository, "_write", write)
    monkeypatch.setattr(app.repository, "_confirmed", confirmed)
    monkeypatch.setattr(app.repository, "_snapshot", snapshot)
    if kind == "same_key_conflict":
        original_combined = app.repository.commit_with_receipt

        async def repeated(trial, body, value):
            await original_combined(trial, body, value)
            first = reader(trial.task_id, trial.case_id, trial.repetition)
            await original_combined(trial, body, value)
            assert reader(trial.task_id, trial.case_id, trial.repetition) == first
            alternatives = [
                (clone(body) | {"quality": "failed"}, clone(value)),
                (clone(body), clone(value) | {"pid": value["pid"] + 1}),
                (clone(body), clone(value) | {"execution_ref": "different-ref"}),
            ]
            for changed_body, changed_receipt in alternatives:
                with pytest.raises(ServiceError) as error:
                    await original_combined(trial, changed_body, changed_receipt)
                assert error.value.code in {"key_conflict", "facts_corrupt"}
                capture["conflicts"].append(error.value.code)
                assert reader(trial.task_id, trial.case_id, trial.repetition) == first

        monkeypatch.setattr(app.repository, "commit_with_receipt", repeated)
    try:
        count = 2 if kind == "two_worker_count" else 1
        dataset = DatasetSpec(name="atomic-commit", version="1", cases=scale_cases(count))
        await app.repository.freeze(dataset)
        request = TaskSpec(
            request_key="atomic-" + kind,
            dataset=dataset.ref(),
            metrics=app.repository.metric_registry.specs(),
            execution=Budget(concurrency=2),
        )
        view = await app.run(request)
        state = await app.kernel.wait(view["id"])
        actual = reader(view["id"], "scale-000", 0)
        capture.update(
            task_id=view["id"],
            final=actual,
            result=state,
            spawn_delta=app.owner.spawn_calls - before_spawn,
            scorer_delta=app.repository.metric_registry.calls - before_score,
        )
        assert capture["spawn_delta"] == count
        assert capture["scorer_delta"] == count * len(request.metrics)
        assert capture["receipt_SQL_executed"] >= 1
        if kind == "receipt_failure_rollback":
            assert state["status"] == "failed"
            assert actual["trial"]["status"] != "finished"
            assert actual["trial"]["scores"] is None
            assert actual["task"]["finished_count"] == 0
            assert actual["commit_events"] == actual["receipts"] == 0
            assert actual["trial"]["execution_ref"] is not None
            assert actual["trial"]["output_digest"] is not None
            old = capture["before"]["scale-000:0"]
            assert actual["task"]["revision"] == old["task"]["revision"] + 1
        elif kind in {
            "confirmation_unavailable",
            "trial_only_confirmation",
            "receipt_only_confirmation",
        }:
            assert state["status"] == "failed" and state["code"] == "storage_unavailable"
            assert capture["reply_faults"] == capture["combined_confirms"] == 1
            assert capture["combined_writes"] == 1
            assert (
                len([j for j in app.adapter.jobs.values() if j.prepared.spec.task_id == view["id"]])
                == 1
            )
        else:
            assert state["status"] == "completed"
            assert actual["task"]["finished_count"] == count
            assert actual["receipts"] == actual["commit_events"] == count
            assert capture["combined_writes"] == (5 if kind == "same_key_conflict" else count)
            if kind == "commit_reply_lost":
                assert capture["reply_faults"] == capture["combined_confirms"] == 1
            if kind == "same_key_conflict":
                assert len(capture["conflicts"]) == 3
        old = (
            await app.run(request)
            if app.repository.healthy
            else await app.repository.by_key(request.model_dump(mode="json"))
        )
        assert old["id"] == view["id"]
        assert app.owner.spawn_calls - before_spawn == count
        assert app.repository.metric_registry.calls - before_score == count * len(request.metrics)
    finally:
        await app.close()
        capture["closed"] = app.repository.closed and app.owner.closed
        capture["jobs"] = [
            {
                "ref": job.ref.id,
                "pid": job.process.pid,
                "returncode": job.process.returncode,
                "pidfd": job.pidfd,
                "collector_done": job.collector.done(),
                "stdout_eof": job.stdout_eof,
                "stderr_eof": job.stderr_eof,
                "stream_joined": all(task.done() for task in job.stream_tasks),
                "cleanup": job.cleanup_receipts,
            }
            for job in app.owner.jobs.values()
        ]
        (tmp_path / "atomic-commit-facts.json").write_text(json.dumps(capture, indent=2))


@pytest.mark.parametrize(
    "scenario",
    [
        "observed_score_roundtrip",
        "rescore_roundtrip",
        "row_missing",
        "row_changed",
        "frozen_slot_missing",
        "frozen_slot_wrong_type",
        "frozen_slot_changed",
        "inputs_wrong_type",
        "bool_int_mismatch",
        "output_hash_mismatch",
        "legacy_layout2",
        "cross_scope",
    ],
)
async def test_frozen_metadata_integrity(tmp_path, monkeypatch, scenario):
    import asyncio
    import copy
    import traceback
    from contextlib import closing

    import test_continuous_eval as continuous_test
    from sqlalchemy import delete, select, update

    from krim_agent.contracts import ServiceError
    from krim_agent.evaluation import scale_store
    from krim_agent.evaluation.domain import Budget, neutral_fingerprint
    from krim_agent.evaluation.repository import continuous_inputs, tasks

    app, dataset, selection, flow = await continuous_test.seed(tmp_path)
    capture = {
        "scenario": scenario,
        "database": str(app.repository.path),
        "source_seed": selection.task_id,
        "seed_counts": {
            "spawn": app.owner.spawn_calls,
            "scorer": app.repository.metric_registry.calls,
        },
        "commit_calls": 0,
        "input_calls": 0,
        "input_errors": [],
        "read_returns": 0,
    }
    ready, release = asyncio.Event(), asyncio.Event()
    probe = None
    original_read = app.repository.read_trial
    original_input = scale_store.input_for_trial
    original_commit = app.repository.commit
    original_combined = app.repository.commit_with_receipt

    async def commit(*args, **kwargs):
        capture["commit_calls"] += 1
        return await original_commit(*args, **kwargs)

    async def combined(*args, **kwargs):
        capture["commit_calls"] += 1
        return await original_combined(*args, **kwargs)

    async def input_seen(*args, **kwargs):
        capture["input_calls"] += 1
        try:
            return await original_input(*args, **kwargs)
        except ServiceError as error:
            capture["input_errors"].append(
                {
                    "code": error.code,
                    "frames": [
                        {"file": f.filename, "function": f.name, "line": f.lineno}
                        for f in traceback.extract_tb(error.__traceback__)
                    ],
                }
            )
            raise

    async def held_read(*args):
        ready.set()
        await release.wait()
        value = await original_read(*args)
        capture["read_returns"] += 1
        return value

    async def inspect(conn):
        snapshot = (
            await conn.execute(select(tasks.c.body).where(tasks.c.id == capture["task_id"]))
        ).scalar_one()
        metadata = (
            await conn.execute(
                select(continuous_inputs.c.body).where(
                    continuous_inputs.c.task_id == capture["task_id"],
                    continuous_inputs.c.case_id == "basic",
                    continuous_inputs.c.repetition == 0,
                )
            )
        ).scalar_one_or_none()
        return {"snapshot": snapshot, "metadata": metadata}

    async def corrupt(conn):
        assert app.repository.lock.locked()
        value = await inspect(conn)
        capture["before_fault"] = copy.deepcopy(value)
        snapshot, metadata = value["snapshot"], value["metadata"]
        frozen = snapshot["continuous"]["inputs"]["basic:0"]
        if scenario == "row_missing":
            await conn.execute(
                delete(continuous_inputs).where(continuous_inputs.c.task_id == capture["task_id"])
            )
        elif scenario == "row_changed":
            metadata["source_context_hash"] = "0" * 64
        elif scenario == "frozen_slot_missing":
            del snapshot["continuous"]["inputs"]["basic:0"]
        elif scenario == "frozen_slot_wrong_type":
            snapshot["continuous"]["inputs"]["basic:0"] = neutral_fingerprint(frozen)
        elif scenario == "frozen_slot_changed":
            frozen["source_context_hash"] = "0" * 64
        elif scenario == "inputs_wrong_type":
            snapshot["continuous"]["inputs"] = []
        elif scenario == "bool_int_mismatch":
            assert type(metadata["known_hard_failure"]) is bool
            metadata["known_hard_failure"] = int(metadata["known_hard_failure"])
            assert metadata == frozen
            assert neutral_fingerprint(metadata) != neutral_fingerprint(frozen)
            capture["ordinary_equal_but_fingerprint_differs"] = True
        elif scenario == "output_hash_mismatch":
            metadata["output_hash"] = frozen["output_hash"] = "0" * 64
            assert neutral_fingerprint(metadata) == neutral_fingerprint(frozen)
            assert metadata["output_hash"] != neutral_fingerprint(capture["healthy_read"]["output"])
        else:
            raise AssertionError(scenario)
        if scenario != "row_missing":
            await conn.execute(
                update(continuous_inputs)
                .where(continuous_inputs.c.task_id == capture["task_id"])
                .values(body=metadata)
            )
        await conn.execute(
            update(tasks).where(tasks.c.id == capture["task_id"]).values(body=snapshot)
        )
        capture["mutation_used_existing_writer_lock"] = True
        capture["after_fault"] = await inspect(conn)

    try:
        seed_snapshot = await app.kernel.wait(selection.task_id)
        seed_report = await app.repository.report(selection.task_id)
        capture["seed_report_hash"] = seed_report["report_hash"]
        if scenario == "legacy_layout2":
            capture["task_id"] = selection.task_id
            assert seed_snapshot.get("layout", 2) == 2
            before = (app.owner.spawn_calls, app.repository.metric_registry.calls)
            actual = await original_read("local", selection.task_id, "basic", 0)
            assert actual["output"] == seed_snapshot["items"][0]["output"]
            request_json = seed_snapshot["request_json"]
            assert (await app.repository.by_key(request_json))["id"] == selection.task_id
            reader = await Repository.open(app.repository.path, readonly=True)
            try:
                assert (await reader.by_key(request_json))["id"] == selection.task_id
                assert await reader.report(selection.task_id) == seed_report
                assert await reader.read_trial("local", selection.task_id, "basic", 0) == actual
            finally:
                await reader.close()
            assert before == (app.owner.spawn_calls, app.repository.metric_registry.calls)
            capture.update(
                legacy_preserved=True,
                cold_read_same_facts=True,
                zero_replay=True,
                spawn_delta=0,
                scorer_delta=0,
            )
            return
        flow.redaction["artifact_paths"] = ["ledger.json", "summary.md"]
        imported = (
            await flow.import_sources(
                continuous_test.ImportRequest(request_key="metadata-import", sources=[selection])
            )
        )[0]
        request = await continuous_test._pn3_repair_confirm(
            app, flow, dataset, imported, name="metadata"
        )
        if scenario == "rescore_roundtrip":
            parent_before = (app.owner.spawn_calls, app.repository.metric_registry.calls)
            parent = await flow.submit(request)
            parent_snapshot = await app.kernel.wait(parent["id"])
            parent_report = await app.repository.report(parent["id"])
            assert parent_snapshot.get("layout", 2) == 2
            assert parent_snapshot["status"] == "completed"
            capture["parent_counts"] = {
                "spawn": app.owner.spawn_calls - parent_before[0],
                "scorer": app.repository.metric_registry.calls - parent_before[1],
            }
            assert capture["parent_counts"] == {"spawn": 0, "scorer": len(request.metrics)}
            capture["parent_task_id"] = parent["id"]
            capture["parent_report_hash"] = parent_report["report_hash"]
            request = request.model_copy(
                update={
                    "request_key": "metadata-rescore",
                    "mode": "rescore",
                    "observations": [],
                    "parent_task_id": parent["id"],
                    "parent_report_hash": parent_report["report_hash"],
                }
            )
        request = request.model_copy(update={"execution": Budget(concurrency=2)})
        monkeypatch.setattr(app.repository, "commit", commit)
        monkeypatch.setattr(app.repository, "commit_with_receipt", combined)
        before_spawn, before_score = app.owner.spawn_calls, app.repository.metric_registry.calls
        with continuous_test.CallCounts(app) as calls:
            view = await flow.submit(request)
            capture["task_id"] = view["id"]
            result = await app.kernel.wait(view["id"])
            report = await app.repository.report(view["id"])
        capture.update(
            request=request.model_dump(mode="json"),
            result=result,
            report_hash=report["report_hash"],
            scoring_interval_counts=calls.counts,
            spawn_delta=app.owner.spawn_calls - before_spawn,
            scorer_delta=app.repository.metric_registry.calls - before_score,
        )
        assert result["layout"] == 3 and result["status"] == "completed"
        assert capture["spawn_delta"] == 0
        assert capture["scorer_delta"] == len(request.metrics)
        assert capture["commit_calls"] == 1
        assert all(value == 0 for value in calls.counts.values())
        healthy = await original_read("local", view["id"], "basic", 0)
        capture["healthy_read"] = healthy
        persisted = await app.repository._write(inspect)
        capture["healthy_frozen"] = copy.deepcopy(persisted)
        frozen = persisted["snapshot"]["continuous"]["inputs"]["basic:0"]
        assert isinstance(frozen, dict) and isinstance(persisted["metadata"], dict)
        assert neutral_fingerprint(frozen) == neutral_fingerprint(persisted["metadata"])
        assert frozen["output_hash"] == neutral_fingerprint(healthy["output"])
        assert healthy["output"] == imported["output"]
        assert healthy["_continuous"]["source_proof"] == frozen["source_proof"]
        assert healthy["_continuous"]["source_context_hash"] == frozen["source_context_hash"]
        assert len(healthy["scores"]) == len(request.metrics)
        counters = (app.owner.spawn_calls, app.repository.metric_registry.calls)
        assert (await flow.submit(request))["id"] == view["id"]
        assert (await app.repository.by_key(request.model_dump(mode="json")))["id"] == view["id"]
        reader = await Repository.open(app.repository.path, readonly=True)
        try:
            assert (await reader.by_key(request.model_dump(mode="json")))["id"] == view["id"]
            assert await reader.report(view["id"]) == report
            assert await reader.read_trial("local", view["id"], "basic", 0) == healthy
        finally:
            await reader.close()
        assert counters == (app.owner.spawn_calls, app.repository.metric_registry.calls)
        capture.update(cold_read_same_facts=True, zero_replay=True)
        if scenario == "rescore_roundtrip":
            assert await app.repository.report(parent["id"]) == parent_report
        if scenario in {"observed_score_roundtrip", "rescore_roundtrip"}:
            return
        monkeypatch.setattr(scale_store, "input_for_trial", input_seen)
        monkeypatch.setattr(app.repository, "read_trial", held_read)
        scope = "other-scope" if scenario == "cross_scope" else "local"
        probe = asyncio.create_task(app.repository.read_trial(scope, view["id"], "basic", 0))
        await asyncio.wait_for(ready.wait(), 2)
        before_probe = (app.owner.spawn_calls, app.repository.metric_registry.calls)
        before_commit = capture["commit_calls"]
        if scenario != "cross_scope":
            await app.repository._write(corrupt)
        release.set()
        with pytest.raises(ServiceError) as rejected:
            await probe
        expected = "not_found" if scenario == "cross_scope" else "facts_corrupt"
        assert rejected.value.code == expected
        assert capture["read_returns"] == 0
        if scenario == "cross_scope":
            assert capture["input_calls"] == 0
            assert not capture["input_errors"]
        else:
            assert capture["input_calls"] == 1
            assert capture["input_errors"][0]["code"] == "facts_corrupt"
            assert capture["input_errors"][0]["frames"][-1]["function"] == "input_for_trial"
        assert before_probe == (app.owner.spawn_calls, app.repository.metric_registry.calls)
        assert capture["commit_calls"] == before_commit
        capture.update(
            rejection=expected,
            probe_spawn_delta=app.owner.spawn_calls - before_probe[0],
            probe_scorer_delta=app.repository.metric_registry.calls - before_probe[1],
            probe_commit_delta=capture["commit_calls"] - before_commit,
            synthetic_corruption=scenario != "cross_scope",
        )
        with closing(sqlite3.connect(app.repository.path.as_uri() + "?mode=ro", uri=True)) as db:
            db.execute("PRAGMA query_only=ON")
            assert db.execute(
                "SELECT id FROM eval_tasks WHERE request_key=?", (request.request_key,)
            ).fetchall() == [(view["id"],)]
        assert await app.repository.report(selection.task_id) == seed_report
    finally:
        release.set()
        if probe is not None:
            await asyncio.gather(probe, return_exceptions=True)
            capture["probe_joined"] = probe.done()
        try:
            await app.close()
        finally:
            capture["closed"] = app.repository.closed and app.owner.closed
            (tmp_path / "metadata-integrity-facts.json").write_text(json.dumps(capture, indent=2))


@pytest.mark.parametrize(
    "scenario",
    [
        "write_read_complete",
        "write_read_finish",
        "wrong_scope",
        "wrong_meta_owner",
        "conflicting_body_scope",
        "conflicting_body_owner",
    ],
)
async def test_v2_header_projection(tmp_path, scenario):
    import asyncio
    import signal
    import stat

    contract_path = (
        Path("/artifacts/krim-agent-core-20261019")
        / "vm-pn3-scale-implementation-20261023T152826061485"
        / "static-supplement-03/projection15-child/APPROVED-CONTRACT.json"
    )
    contract = json.loads(await asyncio.to_thread(contract_path.read_text))
    selected = contract["readonly_existing_inputs"]["main_300_source"]
    source_db = Path(selected["backup"]["path"])
    for path in [source_db, *source_db.parents]:
        assert not path.is_symlink()
    source_stat = await asyncio.to_thread(source_db.lstat)
    assert stat.S_ISREG(source_stat.st_mode) and source_stat.st_nlink == 1
    assert (
        hashlib.sha256(await asyncio.to_thread(source_db.read_bytes)).hexdigest()
        == selected["backup"]["sha256"]
    )
    driver = r"""import asyncio,collections,fcntl,hashlib,json,os
import sqlite3,stat,sys,threading,time,traceback
from contextlib import closing
from pathlib import Path
source,root=map(Path,sys.argv[1:3])
scenario=sys.argv[3]
selected=json.loads(sys.argv[4])
sys.path.insert(0,str(source))
from sqlalchemy import event,select,update
from sqlalchemy.engine import Engine
from sqlalchemy.pool import Pool
from krim_agent.contracts import ServiceError
from krim_agent.evaluation.app import open_eval
from krim_agent.evaluation.repository import Repository,meta,tasks,continuous_inputs
from krim_agent.evaluation.sources import EvalSourceReader
from krim_agent.evaluation.domain import Budget,CaseSpec,DatasetRef,neutral_fingerprint
from krim_agent.evaluation.continuous_types import (
    ConfirmationRequest,ContinuousRequest,FreezeConfirmed,ImportRequest,
    ScoreSource,SourceSelection,
)
from krim_agent.evaluation.continuous_metrics import continuous_rules
from krim_agent.evaluation.adapters.local_process import LocalProcessAdapter
from krim_agent.evaluation import scale_store

assert Path(scale_store.__file__).is_relative_to(source)
backup=Path(selected['backup']['path'])
destination=root/'projection15.sqlite'
readonly_paths={str(backup)}
assert stat.S_ISREG(backup.lstat().st_mode) and backup.lstat().st_nlink==1
source_hash=hashlib.sha256(backup.read_bytes()).hexdigest()
assert source_hash==selected['backup']['sha256']
with closing(sqlite3.connect(backup.as_uri()+'?mode=ro',uri=True)) as db:
    db.execute('PRAGMA query_only=ON')
    assert db.execute('PRAGMA query_only').fetchone()[0]==1
    source_header=json.loads(db.execute(
        'SELECT body FROM eval_tasks WHERE id=? AND scope=?',
        (selected['task_id'],'local')).fetchone()[0])
    source_report=json.loads(db.execute(
        'SELECT body FROM eval_reports WHERE task_id=?',
        (selected['task_id'],)).fetchone()[0])
    ref=source_header['dataset_ref']
    cases=[json.loads(row[0]) for row in db.execute(
        'SELECT body FROM dataset_cases WHERE name=? AND version=? '
        'ORDER BY case_id COLLATE BINARY LIMIT 9',(ref['name'],ref['version']))]
assert source_report['report_hash']==selected['report_hash']
assert source_header['status']=='completed' and source_header['planned_count']==300
assert len(cases)==9
facts=dict(
    node='tests/safe_eval/test_eval_scale_storage.py::test_v2_header_projection['+scenario+']',
    scenario=scenario,uid=os.getuid(),source_root=str(source),source_database=str(backup),
    source_URI=backup.as_uri()+'?mode=ro',uri_true=True,query_only=True,
    baseline_sqlite_closed=True,source_hash_before=source_hash,
    product_file=str(Path(scale_store.__file__)),
    product_sha256=hashlib.sha256(Path(scale_store.__file__).read_bytes()).hexdigest(),
    destination_database=str(destination),phase='start',error=None,
    sql=[],connections=[],query_only_observations=[],calls={},faults=[],
    finalize_observations=[],finalize_unfinished=[],close_errors=[],unexpected=[],
)
code_counts={getattr(LocalProcessAdapter,name).__code__:'target_'+name
    for name in ['prepare','execute','collect','cancel']}
code_counts[Repository.adapter.__code__]='adapter_lookup'
code_counts[asyncio.create_subprocess_exec.__code__]='target_subprocess'
facts['calls']={name:0 for name in code_counts.values()}
facts['calls']['provider_stream']=0
prepare_ids=set()
finalize_args={}
def profile(frame,kind,value):
    if kind=='call':
        name=code_counts.get(frame.f_code)
        if name is not None:facts['calls'][name]+=1
        if frame.f_globals.get('__name__','').startswith('krim_agent.providers.'):
            if frame.f_code.co_name=='stream':facts['calls']['provider_stream']+=1
        if frame.f_code.co_name=='prepare_task' and frame.f_globals.get('__name__')==(
            'krim_agent.evaluation.continuous_store'):
            prepare_ids.add(id(frame.f_locals['request']))
        if frame.f_code is scale_store.finalize.__code__:
            if id(frame) not in finalize_args:
                incoming=frame.f_locals['value']
                finalize_args[id(frame)]=(incoming,neutral_fingerprint(incoming),{
                    k:incoming.get(k) for k in ['id','status','code','revision','finished_at']})
    if kind=='return' and frame.f_code is scale_store.finalize.__code__:
        if isinstance(value,dict) and value.get('schema_version')=='safe-eval/scale/2':
            incoming,before,terminal=finalize_args.pop(id(frame))
            facts['finalize_observations'].append(dict(
                incoming_hash_before=before,incoming_hash_after=neutral_fingerprint(incoming),
                incoming_terminal=terminal,report_status=value['status'],
                report_code=value['code'],report_revision=value['revision'],
                caller_body_scope_present='scope' in incoming,
                caller_body_owner_present='repository_id' in incoming,
                report_owner=value['repository_id'],report_scope=value['scope']))
def connected(dbapi,record):
    cur=dbapi.cursor()
    try:
        cur.execute('PRAGMA database_list')
        name=cur.fetchone()[2]
        record.info['projection15_path']=name
        record.info['projection15_role']='source' if name==str(backup) else 'destination'
        if name in readonly_paths:cur.execute('PRAGMA query_only=ON')
    finally:cur.close()
def checked_out(dbapi,record,*args):
    if record.info.get('projection15_path') in readonly_paths:
        cur=dbapi.cursor()
        try:
            cur.execute('PRAGMA query_only')
            facts['query_only_observations'].append(dict(
                path=record.info['projection15_path'],value=cur.fetchone()[0]))
        finally:cur.close()
def sql(conn,cursor,statement,parameters,context,many):
    facts['sql'].append(dict(statement=statement,parameters=parameters,
        database=str(conn.engine.url.database),phase=facts['phase'],at=time.monotonic()))
listeners=[]
event.listen(Pool,'connect',connected)
event.listen(Pool,'checkout',checked_out)
for name in ['connect','checkout','reset','checkin','close']:
    def observe(*args,name=name):
        facts['connections'].append(dict(event=name,connection=id(args[0]),
            role=args[1].info.get('projection15_role'),at=time.monotonic()))
    event.listen(Pool,name,observe)
    listeners.append((name,observe))
event.listen(Engine,'before_cursor_execute',sql)

async def captured(repo,identity):
    async with repo.lock,repo.engine.begin() as conn:
        await conn.exec_driver_sql('BEGIN')
        actual_scope=(await conn.execute(select(tasks.c.scope).where(
            tasks.c.id==identity))).scalar_one()
        marker=(await conn.execute(select(meta.c.body).where(meta.c.id==1))).scalar_one()
        body=(await conn.execute(select(tasks.c.body).where(tasks.c.id==identity))).scalar_one()
        return actual_scope,marker,body

async def main():
    source_repo=app=cold=None
    gate=asyncio.Event()
    written=asyncio.Event()
    before=asyncio.all_tasks()
    facts['fd_before']=len(os.listdir('/proc/self/fd'))
    facts['threads_before']=[(t.ident,t.name) for t in threading.enumerate()]
    loop=asyncio.get_running_loop()
    previous_handler=loop.get_exception_handler()
    loop.set_exception_handler(lambda current,context:facts['unexpected'].append(str(context)))
    previous_profile=sys.getprofile()
    sys.setprofile(profile)
    try:
        facts['phase']='import'
        source_repo=await Repository.open(backup,readonly=True)
        reader=EvalSourceReader(source_repo,scope='local',include_context=True)
        app=await open_eval(destination,execution_enabled=False,sources={'accepted':reader})
        assert app.adapter is None and app.owner is None
        flow=app.continuous
        flow.redaction['artifact_paths']=['ledger.json','summary.md']
        choices=[SourceSelection(task_id=selected['task_id'],case_id=c['case_id'],
            repetition=r,revision=source_header['revision']) for c in cases for r in range(3)]
        observations=await flow.import_sources(ImportRequest(
            request_key='projection15-import',source_alias='accepted',sources=choices))
        assert len(observations)==27
        for row in observations:
            assert row['eligibility']=='eligible'
            assert row['source']['task_id']==selected['task_id']
            assert row['source']['report_hash']==selected['report_hash']
            assert row['source_proof']['complete'] and row['source_proof']['terminal']
        confirmations=[]
        for case in cases:
            row=next(x for x in observations if x['source']['case_id']==case['case_id']
                and x['source']['repetition']==0)
            draft=await app.repository.candidate('local',row['id'])
            confirmed=await flow.confirm_case(ConfirmationRequest(
                request_key='projection15-confirm-'+case['case_id'],
                candidate_id=draft['candidate_id'],expected_revision=draft['revision'],
                expected_hash=draft['hash'],case=CaseSpec.model_validate(case),context='complete',
                reason='真实已接受来源的相同Case与未改gold，有限投影回归。'))
            confirmations.append(confirmed['id'])
        frozen=await flow.freeze_dataset(FreezeConfirmed(request_key='projection15-freeze',
            name='projection15-cases',version='1',confirmations=confirmations))
        request=ContinuousRequest(request_key='projection15-score',mode='observed_score',
            dataset=DatasetRef.model_validate(frozen['dataset_ref']),
            dataset_envelope_fp=frozen['dataset_envelope_fp'],
            target_key=source_header['target']['key'],
            metrics=continuous_rules(app.repository.metric_registry),
            execution=Budget(repetitions=3,concurrency=2),observations=[
                ScoreSource(case_id=x['source']['case_id'],repetition=x['source']['repetition'],
                    observation_id=x['id'],observation_hash=x['content_hash'])
                for x in observations])
        facts['request']=request.model_dump(mode='json')
        actual_running=app.repository.set_running
        async def held_running(identity):
            await actual_running(identity)
            facts['actual_set_running_count']=facts.get('actual_set_running_count',0)+1
            written.set()
            await gate.wait()
        facts['phase']='admit_and_write'
        if scenario=='write_read_finish':
            initial,created=await app.repository.begin(request.model_dump(mode='json'))
            assert created
            await actual_running(initial['id'])
            facts['actual_set_running_count']=1
            facts['admission_consumer']='Repository.begin/set_running/finish'
            written.set()
        else:
            app.repository.set_running=held_running
            initial=await flow.submit(request)
            facts['admission_consumer']='ContinuousEvaluation.submit/EvalKernel'
        identity=initial['id']
        facts['task_id']=identity
        await asyncio.wait_for(written.wait(),timeout=120)
        actual_scope,marker,body=await captured(app.repository,identity)
        facts['written_body']=body
        facts['actual_column_scope']=actual_scope
        facts['actual_meta']=marker
        assert body['layout']==3 and body['status']=='running'
        assert body['planned_count']==27 and body['continuous']['inputs_count']==27
        assert body['continuous']['schema_version']=='continuous-eval/2'
        assert 'scope' not in body and 'repository_id' not in body
        input_root=body['continuous']['inputs_root']
        facts['input_root']=input_root
        facts['phase']='real_read_after_write'
        async with app.repository.lock,app.repository.engine.begin() as conn:
            await conn.exec_driver_sql('BEGIN')
            projection=await scale_store.header(conn,identity,'local')
        assert projection['scope']==actual_scope=='local'
        assert projection['repository_id']==marker['repository_id']
        read=await app.repository.read_trial('local',identity,cases[0]['case_id'],0)
        assert read['output'] is not None and read['_continuous']['source_proof']['complete']
        facts['read_after_write']=dict(execution_ref=read['execution_ref'],
            output_hash=neutral_fingerprint(read['output']),owner=projection['repository_id'],
            scope=projection['scope'],input_root=projection['continuous']['inputs_root'])
        assert app.repository.metric_registry.calls==0
        if scenario=='wrong_scope':
            facts['phase']='wrong_scope'
            start=len(facts['sql'])
            try:
                await app.repository.read_trial('another-scope',identity,cases[0]['case_id'],0)
            except ServiceError as error:
                assert error.code=='not_found'
                facts['wrong_scope_code']=error.code
            else:raise AssertionError('wrong scope accepted')
            attempted=facts['sql'][start:]
            body_reads=[x for x in attempted if 'eval_tasks.body' in x['statement']]
            facts['wrong_scope_SQL']=attempted
            facts['wrong_scope_body_reads']=len(body_reads)
            assert len(body_reads)==0
        if scenario=='write_read_finish':
            facts['phase']='finish'
            await app.repository.finish(identity,'interrupted','cancelled')
            facts['metrics_before_gate_release']=app.repository.metric_registry.calls
            assert facts['metrics_before_gate_release']==0
        facts['phase']='join_actual_run'
        gate.set()
        terminal=await app.kernel.wait(identity)
        facts['terminal']=terminal
        expected='interrupted' if scenario=='write_read_finish' else 'completed'
        assert terminal['status']==expected
        if expected=='interrupted':assert terminal['code']=='cancelled'
        assert terminal['finished_at'] is not None
        assert 'scope' not in terminal and 'repository_id' not in terminal
        report=await app.repository.report(identity)
        facts['report']=report
        assert report['status']==expected and report['planned_count']==27
        assert report['summary']['planned_cells']==108
        assert report['summary']['scored_cells']==(0 if expected=='interrupted' else 108)
        assert report['summary']['passed']==(0 if expected=='interrupted' else 27)
        assert app.repository.metric_registry.calls==(0 if expected=='interrupted' else 108)
        assert report['scope']==actual_scope and report['repository_id']==marker['repository_id']
        assert report['inputs_root']==input_root
        assert len(facts['finalize_observations'])==1
        watch=facts['finalize_observations'][0]
        assert watch['incoming_hash_before']==watch['incoming_hash_after']
        assert not watch['caller_body_scope_present'] and not watch['caller_body_owner_present']
        assert watch['incoming_terminal']['status']==watch['report_status']==expected
        assert (
            watch['incoming_terminal']['revision']==watch['report_revision']==terminal['revision']
        )
        assert watch['incoming_terminal']['finished_at']==terminal['finished_at']
        before_cells=app.repository.metric_registry.calls
        before_prepare=len(prepare_ids)
        assert (await flow.submit(request))['id']==identity
        assert (
            app.repository.metric_registry.calls==before_cells
            and len(prepare_ids)==before_prepare==1
        )
        facts['idempotency']=dict(task_id=identity,metric_calls=before_cells,
            prepare_count=before_prepare)
        if scenario in {'wrong_meta_owner','conflicting_body_scope','conflicting_body_owner'}:
            facts['phase']='faults'
            different_owner='00000000-0000-4000-8000-000000000015'
            values=[different_owner,None,'',True,7] if scenario=='wrong_meta_owner' else [
                'different-scope' if scenario=='conflicting_body_scope' else different_owner,
                None,'',True,7]
            for position,bad in enumerate(values):
                async def fault(conn):
                    if scenario=='wrong_meta_owner':
                        changed=dict(marker)
                        if position==1:changed.pop('repository_id')
                        else:changed['repository_id']=bad
                        await conn.execute(update(meta).where(meta.c.id==1).values(body=changed))
                        return changed
                    changed=dict(terminal)
                    key='scope' if scenario=='conflicting_body_scope' else 'repository_id'
                    changed[key]=bad
                    await conn.execute(
                        update(tasks).where(tasks.c.id==identity).values(body=changed))
                    return changed
                written_fault=await app.repository._write(fault)
                try:
                    await app.repository.read_trial('local',identity,cases[0]['case_id'],0)
                except ServiceError as error:
                    assert error.code=='facts_corrupt'
                    failure_code=error.code
                else:raise AssertionError('corrupt authority accepted')
                _,current_meta,current_body=await captured(app.repository,identity)
                assert current_body['continuous']['inputs_root']==input_root
                facts['faults'].append(dict(index=position,value=bad,code=failure_code,
                    changed_value=written_fault,meta=current_meta,body=current_body))
        facts['metric_calls']=app.repository.metric_registry.calls
        facts['kernel_failure_facts']=app.kernel._failure_facts
        facts['phase']='close_before_cold'
        await app.close()
        assert app.repository.closed and app.repository._writer_fd is None
        if scenario in {'write_read_complete','write_read_finish','wrong_scope'}:
            readonly_paths.add(str(destination))
            cold=await Repository.open(destination,readonly=True)
            cold_state=await cold.snapshot(identity)
            cold_report=await cold.report(identity)
            cold_trial=await cold.read_trial('local',identity,cases[0]['case_id'],0)
            assert (
                cold_state['status']==expected
                and cold_report['report_hash']==report['report_hash']
            )
            assert cold_report['inputs_root']==input_root
            assert 'scope' not in cold_state and 'repository_id' not in cold_state
            assert cold_trial['execution_ref']==read['execution_ref']
            rows=[]
            cursor=None
            while True:
                page=await cold.report_page('local',identity,cursor=cursor,limit=16)
                rows.extend(page.rows)
                cursor=page.next_cursor
                if cursor is None:break
            assert len(rows)==27
            assert {(x['case_id'],x['repetition']) for x in rows}=={
                (case['case_id'],r) for case in cases for r in range(3)}
            facts['cold']=dict(status=cold_state['status'],report_hash=cold_report['report_hash'],
                page_rows=len(rows),owner=cold_report['repository_id'],scope=cold_report['scope'])
            await cold.close()
        assert all(x==0 for x in facts['calls'].values())
        facts['phase']='verified'
    except BaseException as error:
        facts['error']=dict(type=type(error).__name__,message=str(error),
            code=getattr(error,'code',None),phase=facts['phase'],
            frames=[dict(file=x.filename,function=x.name,line=x.lineno)
                for x in traceback.extract_tb(error.__traceback__)])
        raise
    finally:
        gate.set()
        for owner,value in [('cold',cold),('application',app),('source',source_repo)]:
            if value is not None:
                try:await value.close()
                except BaseException as error:
                    facts['close_errors'].append(dict(owner=owner,type=type(error).__name__,
                        message=str(error)))
        sys.setprofile(previous_profile)
        loop.set_exception_handler(previous_handler)
        await asyncio.sleep(0)
        facts['finalize_unfinished']=[dict(before=x[1],terminal=x[2])
            for x in finalize_args.values()]
        facts['source_closed']=source_repo is not None and source_repo.closed
        facts['destination_closed']=app is not None and app.repository.closed
        facts['cold_closed']=cold is None or cold.closed
        facts['writer_fd']=None if app is None else app.repository._writer_fd
        facts['kernel_closed']=app is not None and app.kernel.closed
        facts['kernel_tasks']=[] if app is None else [
            dict(id=k,done=v.done(),cancelled=v.cancelled()) for k,v in app.kernel.tasks.items()]
        facts['kernel_inflight']=None if app is None else len(app.kernel._inflight)
        facts['kernel_workers']=None if app is None else {
            k:[t.done() for t in group] for k,group in app.kernel._workers.items()}
        facts['pending']=[repr(t) for t in asyncio.all_tasks() if t not in before and not t.done()]
        facts['source_hash_after']=hashlib.sha256(backup.read_bytes()).hexdigest()
        facts['fd_after']=len(os.listdir('/proc/self/fd'))
        facts['threads_after']=[(t.ident,t.name) for t in threading.enumerate()]
        if destination.exists():
            consistent=root/'projection15-backup.sqlite'
            with closing(sqlite3.connect(destination.as_uri()+'?mode=ro',uri=True)) as db:
                db.execute('PRAGMA query_only=ON')
                facts['persisted_tasks']=[json.loads(row[0])
                    for row in db.execute('SELECT body FROM eval_tasks')]
                facts['persisted_meta']=json.loads(db.execute(
                    'SELECT body FROM eval_meta WHERE id=1').fetchone()[0])
                facts['counts']={name:db.execute('SELECT count(*) FROM '+name).fetchone()[0]
                    for name in ['eval_tasks','eval_trials','continuous_inputs',
                        'eval_report_pages']}
                with closing(sqlite3.connect(consistent)) as target:db.backup(target)
            facts['consistent_backup']=dict(path=str(consistent),
                sha256=hashlib.sha256(consistent.read_bytes()).hexdigest())
            fd=os.open(str(destination)+'.lock',os.O_RDWR|os.O_NOFOLLOW)
            try:
                fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
                facts['writer_lock_released']=True
                fcntl.flock(fd,fcntl.LOCK_UN)
            finally:os.close(fd)
        event.remove(Engine,'before_cursor_execute',sql)
        event.remove(Pool,'connect',connected)
        event.remove(Pool,'checkout',checked_out)
        for name,handler in listeners:event.remove(Pool,name,handler)
        (root/'projection15-consumer.json').write_text(json.dumps(facts,indent=2))
        assert facts['source_closed'] and facts['destination_closed'] and facts['cold_closed']
        assert facts['writer_fd'] is None and facts['kernel_closed']
        assert not facts['pending'] and not facts['close_errors'] and not facts['unexpected']
        assert facts['fd_after']<=facts['fd_before']
        assert facts['source_hash_after']==facts['source_hash_before']
        counter=collections.Counter(x['event'] for x in facts['connections'])
        assert counter['connect']==counter['close']
        assert counter['checkout']==counter['reset']==counter['checkin']
        assert facts['query_only_observations']
        assert all(x['value']==1 for x in facts['query_only_observations'])
asyncio.run(main())
"""
    compile(driver, "projection15-driver-syntax", "exec")
    source = (await asyncio.to_thread(Path(__file__).resolve)).parents[2] / "src"
    argv = [
        sys.executable,
        "-I",
        "-B",
        "-c",
        driver,
        str(source),
        str(tmp_path),
        scenario,
        json.dumps(selected),
    ]
    proc = await asyncio.create_subprocess_exec(
        *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    pidfd = os.pidfd_open(proc.pid)
    timed_out = False
    try:
        try:
            async with asyncio.timeout(180):
                stdout, stderr = await proc.communicate()
        except TimeoutError:
            timed_out = True
            signal.pidfd_send_signal(pidfd, signal.SIGTERM)
            stdout, stderr = await proc.communicate()
        wait_code = await proc.wait()
    finally:
        os.close(pidfd)
    await asyncio.to_thread((tmp_path / "projection15-stdout.txt").write_bytes, stdout)
    await asyncio.to_thread((tmp_path / "projection15-stderr.txt").write_bytes, stderr)
    facts_path = tmp_path / "projection15-consumer.json"
    facts = json.loads(await asyncio.to_thread(facts_path.read_text))
    process = {
        "argv": argv,
        "cwd": os.getcwd(),
        "uid": os.getuid(),
        "pid": proc.pid,
        "wait_returncode": wait_code,
        "returncode": proc.returncode,
        "pidfd_opened": True,
        "pidfd_closed": True,
        "stdout_eof": proc.stdout.at_eof(),
        "stderr_eof": proc.stderr.at_eof(),
        "timed_out": timed_out,
        "driver_sha256": hashlib.sha256(driver.encode()).hexdigest(),
        "facts_file": str(facts_path),
        "source_database": str(source_db),
        "destination_database": str(tmp_path / "projection15.sqlite"),
    }
    await asyncio.to_thread(
        (tmp_path / "projection15-process.json").write_text, json.dumps(process, indent=2)
    )
    assert not timed_out
    assert wait_code == proc.returncode == 0, facts["error"]
    assert facts["phase"] == "verified"
    assert facts["source_hash_after"] == selected["backup"]["sha256"]


@pytest.mark.parametrize(
    "kind",
    [
        "layout2_readonly",
        "layout3_dictionary_readonly",
        "paged_input_roundtrip",
        "unknown_format",
        "row_missing",
        "row_tamper",
        "index_tamper",
        "bool_int_mismatch",
        "output_hash_mismatch",
        "rollback_after_input",
        "rollback_before_task_commit",
        "commit_reply_lost",
        "authorized_source_before_body",
        "binding_after_await",
        "source_wrong_owner",
        "source_case_trial_mismatch",
    ],
)
async def test_scoring_input_representation(tmp_path, kind):
    import ast
    import asyncio

    from test_eval_scale import _run_scale_evidence_process

    scale_path = Path(__file__).with_name("test_eval_scale.py")
    tree = ast.parse(await asyncio.to_thread(scale_path.read_text))
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "test_shared_modes_scale"
    )
    accepted = next(
        node.value.value
        for node in function.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "driver" for target in node.targets)
    )
    checkpoint = next(
        node
        for node in ast.parse(accepted).body
        if isinstance(node, ast.FunctionDef) and node.name == "_write_scale_checkpoint"
    )
    contract_path = (
        Path("/artifacts/krim-agent-core-20261019")
        / "vm-pn3-scale-implementation-20261023T152826061485"
        / "static-supplement-03/completion20-child/APPROVED-CONTRACT.json"
    )
    inputs = json.loads(await asyncio.to_thread(contract_path.read_text))[
        "readonly_existing_inputs"
    ]
    driver = (
        "import asyncio,collections,fcntl,hashlib,inspect,json,os,sqlite3,stat,sys,time,traceback\n"
        "from contextlib import closing\nfrom pathlib import Path\n"
        "source,root=map(Path,sys.argv[1:3]);inputs=json.loads(sys.argv[3]);kind=sys.argv[4]\n"
        + ast.unparse(checkpoint)
        + "\n"
        + r"""_write_scale_checkpoint('bootstrap','start')
sys.path.insert(0,str(source))
from sqlalchemy import event,select,update
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.pool import Pool
from krim_agent.contracts import ServiceError
from krim_agent.evaluation.app import open_eval
from krim_agent.evaluation.repository import Repository,continuous_inputs,tasks
from krim_agent.evaluation import continuous_store
from krim_agent.evaluation.sources import EvalSourceReader
"""
        "from krim_agent.evaluation.domain import Budget,CaseSpec,Datas"
        "etRef,neutral_encoded,neutral_fingerprint,clone"
        r"""
from krim_agent.evaluation.continuous_types import (
    ConfirmationRequest,ContinuousRequest,FreezeConfirmed,ImportRequest,ScoreSource,SourceSelection,
)
from krim_agent.evaluation.continuous_metrics import continuous_rules
from krim_agent.evaluation.adapters.local_process import LocalProcessAdapter
legacy=kind in {'layout2_readonly','layout3_dictionary_readonly'}
selected=inputs['old_layout2_and_layout3_dictionary'] if legacy else inputs['main_300_source']
backup=Path(selected['backup']['path'])
for path in [backup,*backup.parents]:assert not path.is_symlink()
st=backup.lstat();assert stat.S_ISREG(st.st_mode) and st.st_nlink==1
before_hash=hashlib.sha256(backup.read_bytes()).hexdigest()
assert before_hash==selected['backup']['sha256']
facts=dict(node='tests/safe_eval/test_eval_scale_storage.py::test_scoring_input_representation['+kind+']',
    kind=kind,uid=os.getuid(),source_database=str(backup),source_hash_before=before_hash,
    stage='bootstrap',error=None,close_errors=[],audit_errors=[],connections=[],query_only=[],
    calls={},Task_ids=[],proofs={},source_prepare_calls=0,active_source_calls=0,
    source_call_high_water=0,complete_value_high_water={'output':0,'metadata':0,'packet':0},
    outer_container_samples=[],faults=[],backups=[])
codes={getattr(LocalProcessAdapter,name).__code__:'target_'+name
       for name in ['prepare','execute','collect','cancel']}
codes[Repository.adapter.__code__]='adapter_lookup'
facts['calls']={label:0 for label in codes.values()};facts['calls']['provider_stream']=0
original_input=continuous_store._source_input
original_prepare=continuous_store.prepare_task
async def input_observed(*args,**kwargs):
    facts['active_source_calls']+=1
    facts['source_call_high_water']=max(facts['source_call_high_water'],facts['active_source_calls'])
    try:return await original_input(*args,**kwargs)
    finally:facts['active_source_calls']-=1
async def prepare_observed(*args,**kwargs):
    facts['source_prepare_calls']+=1
    return await original_prepare(*args,**kwargs)
continuous_store._source_input=input_observed;continuous_store.prepare_task=prepare_observed
def values(value,seen,found):
    if id(value) in seen:return
    seen.add(id(value))
    if isinstance(value,dict):
"""
        "        if {'execution_ref','exit_code','stdout','stderr','art"
        "ifacts','process','cleanup'}<=set(value):found['output'].add(i"
        "d(value))"
        r"""
"""
        "        if {'source_ref','output_hash','availability','case_id"
        "'}<=set(value):found['metadata'].add(id(value))"
        r"""
        if value.get('schema_version')=='continuous-input/1':found['packet'].add(id(value))
        for item in value.values():values(item,seen,found)
    elif isinstance(value,(list,tuple)):
        for item in value:values(item,seen,found)
def traced(frame,action,value):
    if action=='line' and frame.f_code.co_name in {'_source_input','publish'}:
        found={key:set() for key in facts['complete_value_high_water']}
        current=frame
"""
        "        while current is not None and current.f_code.co_filena"
        "me.endswith('/evaluation/continuous_store.py'):"
        r"""
            values(current.f_locals,set(),found);current=current.f_back
        for key,identities in found.items():
            facts['complete_value_high_water'][key]=max(facts['complete_value_high_water'][key],len(identities))
    return traced
def trace(frame,action,value):
"""
        "    if kind=='source_layout3_rescore_parent' and not facts.get"
        "('measure_scale_rescore_publish',False):return None"
        r"""
"""
        "    if frame.f_code.co_filename.endswith('/evaluation/continuo"
        "us_store.py') and frame.f_code.co_name in {'_source_input','pu"
        "blish'}:"
        r"""
        return traced
    if frame.f_code is original_prepare.__code__ and action=='call':
        # 仅观察本消费者与调用栈容器，不遍历生成器或借GC制造峰值。
        found={key:set() for key in facts['complete_value_high_water']}
        values(frame.f_locals,set(),found)
        facts['outer_container_samples'].append({key:len(value) for key,value in found.items()})
    return None
def profile(frame,action,value):
    if action=='call':
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
        cur.execute('PRAGMA database_list');database=cur.fetchone()[2]
        record.info['representation_role']='source' if database==str(backup) else database
        if database==str(backup):cur.execute('PRAGMA query_only=ON')
    finally:cur.close()
def checkout(dbapi,record,*args):
    if record.info.get('representation_role')=='source':
        cur=dbapi.cursor()
        try:cur.execute('PRAGMA query_only');facts['query_only'].append(cur.fetchone()[0])
        finally:cur.close()
event.listen(Pool,'connect',connected);event.listen(Pool,'checkout',checkout);listeners=[]
for name in ['connect','checkout','reset','checkin','close']:
    def observe(*args,name=name):
        facts['connections'].append(dict(event=name,connection=id(args[0]),
            role=args[1].info.get('representation_role'),at=time.monotonic()))
    event.listen(Pool,name,observe);listeners.append((name,observe))
async def fixture(path,reader,case_values,trial_values,source_header):
    _write_scale_checkpoint('fixture_open','start',database=str(path))
    app=await open_eval(path,execution_enabled=False,sources={'fixed-source':reader})
    apps.append(app)
    _write_scale_checkpoint('fixture_open','done',database=str(path))
    flow=app.continuous;flow.redaction['artifact_paths']=['ledger.json','summary.md']
    _write_scale_checkpoint('fixture_import','start',database=str(path),count=len(trial_values))
    rows=await flow.import_sources(ImportRequest(request_key='representation-import',
        source_alias='fixed-source',sources=[SourceSelection(task_id=source_header['id'],
        case_id=t['case_id'],repetition=t['repetition'],revision=source_header['revision'],
        evidence_hash=t['output_digest']) for t in trial_values]))
    _write_scale_checkpoint('fixture_import','done',database=str(path),count=len(rows))
    assert len(rows)==27
    selected_scores=[ScoreSource(case_id=row['source']['case_id'],repetition=row['source']['repetition'],
        observation_id=row['id'],observation_hash=row['content_hash']) for row in rows]
    drafts={row['source']['case_id']:row['id'] for row in rows if row['source']['repetition']==0}
    del rows
    confirmed=[]
    for case in case_values:
        _write_scale_checkpoint('fixture_confirm','start',database=str(path),case_id=case['case_id'])
        draft=await app.repository.candidate('local',drafts[case['case_id']])
"""
        "        row=await flow.confirm_case(ConfirmationRequest(reques"
        "t_key='representation-confirm-'+case['case_id'],"
        r"""
            candidate_id=draft['candidate_id'],expected_revision=draft['revision'],expected_hash=draft['hash'],
            case=CaseSpec.model_validate(case),context='complete',reason='固定真实来源/完整context，未改gold。'))
        confirmed.append(row['id'])
        _write_scale_checkpoint('fixture_confirm','done',database=str(path),case_id=case['case_id'],confirmation_id=row['id'])
    _write_scale_checkpoint('fixture_freeze','start',database=str(path),count=len(confirmed))
    frozen=await flow.freeze_dataset(FreezeConfirmed(request_key='representation-freeze',
        name='representation-cases',version='1',confirmations=confirmed))
    _write_scale_checkpoint('fixture_freeze','done',database=str(path),dataset_fingerprint=frozen['dataset_ref']['fingerprint'])
    request=ContinuousRequest(request_key='representation-score',mode='observed_score',
        dataset=DatasetRef.model_validate(frozen['dataset_ref']),dataset_envelope_fp=frozen['dataset_envelope_fp'],
        target_key=source_header['target']['key'],metrics=continuous_rules(app.repository.metric_registry),
        execution=Budget(repetitions=3,concurrency=2),observations=selected_scores)
    return app,flow,request
async def main():
    global apps
    apps=[];source_repo=None;before=asyncio.all_tasks()
    facts['fd_before']=len(os.listdir('/proc/self/fd'));old_profile=sys.getprofile();old_trace=sys.gettrace()
    sys.setprofile(profile);sys.settrace(trace)
    try:
        _write_scale_checkpoint('source_open','start')
        source_repo=await Repository.open(backup,readonly=True)
        _write_scale_checkpoint('source_open','done')
        if legacy:
            identity=selected['layout2_task_id' if kind=='layout2_readonly' else 'layout3_task_id']
            with closing(sqlite3.connect(backup.as_uri()+'?mode=ro',uri=True)) as db:
                db.execute('PRAGMA query_only=ON')
"""
        "                raw=json.loads(db.execute('SELECT body FROM ev"
        "al_tasks WHERE id=?',(identity,)).fetchone()[0])"
        r"""
"""
        "                report_before=json.loads(db.execute('SELECT bo"
        "dy FROM eval_reports WHERE task_id=?',(identity,)).fetchone()["
        "0])"
        r"""
            snapshot=await source_repo.snapshot(identity);report=await source_repo.report(identity)
            assert report==report_before
            assert (await source_repo.by_key(raw['request_json']))['id']==identity
            if kind=='layout2_readonly':
                assert snapshot.get('layout',2)==2 and snapshot['items']==raw['items']
            else:
"""
        "                assert snapshot['layout']==3 and snapshot['con"
        "tinuous']['schema_version']=='continuous-eval/1'"
        r"""
                with closing(sqlite3.connect(backup.as_uri()+'?mode=ro',uri=True)) as db:
                    db.execute('PRAGMA query_only=ON')
"""
        "                    key=db.execute('SELECT case_id,repetition "
        "FROM eval_trials WHERE task_id=? ORDER BY case_id,repetition L"
        "IMIT 1',(identity,)).fetchone()"
        r"""
                row=await source_repo.read_trial('local',identity,key[0],key[1])
"""
        "                assert neutral_fingerprint(row['output'])==sna"
        "pshot['continuous']['inputs'][key[0]+':'+str(key[1])]['output_"
        "hash']"
        r"""
            facts['Task_ids']=[identity];facts['proofs']['legacy_report_hash']=report['report_hash']
"""
        "            assert source_repo.metric_registry.calls==0 and al"
        "l(value==0 for value in facts['calls'].values())"
        r"""
            facts['stage']='completed'
            return
        with closing(sqlite3.connect(backup.as_uri()+'?mode=ro',uri=True)) as db:
            db.execute('PRAGMA query_only=ON')
"""
        "            source_header=json.loads(db.execute('SELECT body F"
        "ROM eval_tasks WHERE id=?',(selected['task_id'],)).fetchone()["
        "0])"
        r"""
            ref=source_header['dataset_ref']
            case_values=[json.loads(row[0]) for row in db.execute(
"""
        "                'SELECT body FROM dataset_cases WHERE name=? A"
        "ND version=? ORDER BY case_id COLLATE BINARY LIMIT 9',"
        r"""
                (ref['name'],ref['version']))]
            ids={c['case_id'] for c in case_values}
            trial_values=[json.loads(row[0]) for row in db.execute(
"""
        "                'SELECT body FROM eval_trials WHERE task_id=? "
        "ORDER BY case_id,repetition',(selected['task_id'],))"
        r"""
                if json.loads(row[0])['case_id'] in ids]
        assert len(case_values)==9 and len(trial_values)==27
        reader=EvalSourceReader(source_repo,scope='local',include_context=True)
"""
        "        app,flow,request=await fixture(root/'representation.sq"
        "lite',reader,case_values,trial_values,source_header)"
        r"""
        original_execute=AsyncConnection.execute;inserted=0;second_readers=[]
        async def fault(conn,statement,*args,**kwargs):
            nonlocal inserted
            result=await original_execute(conn,statement,*args,**kwargs)
            if conn.engine is app.repository.engine and getattr(statement,'is_insert',False):
                table=statement.table.name
                if table=='continuous_inputs':
                    inserted+=1
                    if kind=='rollback_after_input' and inserted==3:
                        facts['faults'].append('after_actual_third_input_INSERT')
                        raise OSError('controlled_after_input_INSERT')
                if table=='eval_tasks' and kind=='rollback_before_task_commit':
"""
        "                    with closing(sqlite3.connect(app.repositor"
        "y.path.as_uri()+'?mode=ro',uri=True)) as db:"
        r"""
                        db.execute('PRAGMA query_only=ON')
"""
        "                        second_readers.append(dict(Task=db.exe"
        "cute('SELECT count(*) FROM eval_tasks').fetchone()[0],"
        r"""
"""
        "                            inputs=db.execute('SELECT count(*)"
        " FROM continuous_inputs').fetchone()[0]))"
        r"""
                    facts['faults'].append('after_actual_Task_INSERT_before_commit')
                    raise OSError('controlled_before_Task_commit')
            return result
        AsyncConnection.execute=fault
        original_write=app.repository._write;lost=False
        async def write(operation):
            nonlocal lost
            result=await original_write(operation)
"""
        "            if kind=='commit_reply_lost' and not lost and '_be"
        "gin_continuous' in operation.__qualname__:"
        r"""
                lost=True;facts['faults'].append('reply_lost_after_actual_commit')
                facts['lost_Task']=result[0]['id'];facts['lost_root']=result[0]['continuous']['inputs_root']
                raise OSError('controlled_reply_lost')
            return result
        app.repository._write=write
        try:
            try:
                _write_scale_checkpoint('fixture_submit','start',database=str(app.repository.path))
                view=await flow.submit(request)
                _write_scale_checkpoint('fixture_submit','done',database=str(app.repository.path),task_id=view['id'])
            except ServiceError as error:
                assert kind in {'rollback_after_input','rollback_before_task_commit'}
                assert error.code=='storage_unavailable'
                facts['proofs']['failure_code']=error.code
"""
        "                with closing(sqlite3.connect(app.repository.pa"
        "th.as_uri()+'?mode=ro',uri=True)) as db:"
        r"""
                    db.execute('PRAGMA query_only=ON')
                    counts=[db.execute('SELECT count(*) FROM '+table).fetchone()[0]
"""
        "                        for table in ['eval_tasks','eval_trial"
        "s','continuous_inputs','eval_schedule_windows']]"
        r"""
                assert counts==[0,0,0,0]
                facts['proofs']['rollback_counts']=counts;facts['proofs']['second_readers']=second_readers
"""
        "                if kind=='rollback_before_task_commit':assert "
        "second_readers==[{'Task':0,'inputs':0}]"
        r"""
                else:assert inserted==3
                facts['stage']='completed'
                return
        finally:
            AsyncConnection.execute=original_execute
        _write_scale_checkpoint('fixture_wait','start',database=str(app.repository.path),task_id=view['id'])
        done=await app.kernel.wait(view['id']);report=await app.repository.report(view['id'])
        _write_scale_checkpoint('fixture_wait','done',database=str(app.repository.path),task_id=view['id'],task_status=done['status'],cells=report['summary']['scored_cells'])
        assert done['status']=='completed' and done['planned_count']==27
        assert report['summary']['scored_cells']==108 and app.repository.metric_registry.calls==108
        assert done['continuous']['schema_version']=='continuous-eval/2'
        facts['Task_ids'].append(view['id']);facts['main_report_hash']=report['report_hash']
        if kind=='commit_reply_lost':
            assert lost and view['id']==facts['lost_Task']
            assert done['continuous']['inputs_root']==facts['lost_root']
            assert (await flow.submit(request))['id']==view['id']
            assert facts['source_prepare_calls']==1 and app.repository.metric_registry.calls==108
            facts['proofs']['original_key_zero_replay']=True
        case_id=case_values[0]['case_id'];repetition=0
        snapshot=await app.repository.snapshot(view['id'])
        async with app.repository.lock,app.repository.engine.begin() as conn:
            original_packet=(await conn.execute(select(continuous_inputs.c.body).where(
                continuous_inputs.c.task_id==view['id'],continuous_inputs.c.case_id==case_id,
                continuous_inputs.c.repetition==repetition))).scalar_one()
        packet=clone(original_packet)
        body=clone(snapshot)
        if kind in {'paged_input_roundtrip','commit_reply_lost'}:
            assert set(snapshot['continuous']['inputs_index'])=={c['case_id']+':'+str(r)
                for c in case_values for r in range(3)}
            for c in case_values:
                for r in range(3):
                    actual=await app.repository.read_trial('local',view['id'],c['case_id'],r)
"""
        "                    assert actual['output'] is not None and ac"
        "tual['_continuous']['source_proof']['complete']"
        r"""
            assert facts['complete_value_high_water']['output']>=1
            assert facts['source_call_high_water']==1
            assert all(value<=1 for value in facts['complete_value_high_water'].values())
"""
        "            assert all(all(value==0 for value in row.values())"
        " for row in facts['outer_container_samples'])"
        r"""
            assert len(neutral_encoded(snapshot,max_bytes=262144).encode())<=262144
"""
        "            assert len(neutral_encoded(snapshot['continuous']["
        "'inputs_index'],max_bytes=65536).encode())<=65536"
        r"""
            facts['proofs']['roundtrip']=dict(trials=27,cells=108,inputs_root=snapshot['continuous']['inputs_root'])
        elif kind=='authorized_source_before_body':
            async with app.repository.lock,app.repository.engine.begin() as conn:
"""
        "                await conn.execute(update(tasks).where(tasks.c"
        ".id==view['id']).values(body={'malformed':'body'}))"
        r"""
            queries=[]
            def sql(conn,cursor,statement,parameters,context,many):queries.append(statement)
            event.listen(app.repository.engine.sync_engine,'before_cursor_execute',sql)
            try:
                try:await EvalSourceReader(app.repository,scope='other').read('other',
                    dict(task_id=view['id'],case_id=case_id,repetition=0))
                except ServiceError as error:assert error.code=='not_found'
                else:raise AssertionError('wrong_scope_read_corrupted_body')
            finally:event.remove(app.repository.engine.sync_engine,'before_cursor_execute',sql)
            assert queries and all('eval_tasks.body' not in query for query in queries)
            facts['proofs']['authorization_queries']=queries
        elif kind=='binding_after_await':
            import types
            original_snapshot=app.repository.snapshot;original_read=app.repository.read_trial
            entered=[0]
            async def reject(self,*args,**kwargs):
                entered[0]+=1;raise AssertionError('replacement_entered')
            async def held(self,identity):
                actual=await original_snapshot(identity)
                self.read_trial=types.MethodType(reject,self)
                return actual
            app.repository.snapshot=types.MethodType(held,app.repository)
            cold=EvalSourceReader(app.repository,scope='local')
            try:
                try:await cold.read('local',dict(task_id=view['id'],case_id=case_id,repetition=0))
                except ServiceError as error:assert error.code=='target_binding_changed'
                else:raise AssertionError('changed_binding_accepted')
            finally:
                app.repository.snapshot=original_snapshot;app.repository.read_trial=original_read
            assert entered[0]==0;facts['proofs']['replacement_entries']=entered[0]
        else:
            if kind=='unknown_format':packet['schema_version']='continuous-input/999'
"""
        "            elif kind=='row_tamper':packet['metadata']['availa"
        "bility']['artifacts']='controlled_tamper'"
        r"""
            elif kind=='bool_int_mismatch':
                assert packet['metadata']['known_hard_failure'] is False
                packet['metadata']['known_hard_failure']=0
"""
        "                assert packet==original_packet and neutral_fin"
        "gerprint(packet)!=neutral_fingerprint(original_packet)"
        r"""
            elif kind=='output_hash_mismatch':
                packet['metadata']['output_hash']='0'*64
                body['continuous']['inputs_index'][case_id+':0']=neutral_fingerprint(packet,max_bytes=1048576)
                binding=packet['binding_B'];index=body['continuous']['inputs_index']
                body['continuous']['inputs_root']=neutral_fingerprint(dict(
                    schema_version='continuous-input-index/1',binding=binding,count=27,
"""
        "                    leaves=[[c,r,index[c+':'+str(r)]] for c in"
        " sorted(body['case_index']) for r in range(3)]))"
        r"""
                body['plan_fingerprint']=neutral_fingerprint({key:body[key]
                    for key in ['request_hash','dataset_ref','case_index','target','continuous']})
                assert neutral_fingerprint(packet)==body['continuous']['inputs_index'][case_id+':0']
                facts['proofs']['packet_index_root_self_consistent_before_independent_output']=True
            elif kind=='index_tamper':body['continuous']['inputs_index'][case_id+':0']='0'*64
            elif kind=='source_wrong_owner':
                other,other_flow,other_request=await fixture(root/'foreign-owner.sqlite',reader,
                    case_values,trial_values,source_header)
                _write_scale_checkpoint('foreign_submit','start',database=str(other.repository.path))
                other_view=await other_flow.submit(other_request)
                _write_scale_checkpoint('foreign_submit','done',database=str(other.repository.path),task_id=other_view['id'])
                _write_scale_checkpoint('foreign_wait','start',task_id=other_view['id'])
                other_done=await other.kernel.wait(other_view['id'])
                _write_scale_checkpoint('foreign_wait','done',task_id=other_view['id'],task_status=other_done['status'],cells=other.repository.metric_registry.calls)
                assert other_done['status']=='completed'
                _write_scale_checkpoint('foreign_packet','start',task_id=other_view['id'])
                async with other.repository.lock,other.repository.engine.begin() as conn:
                    packet=(await conn.execute(select(continuous_inputs.c.body).where(
                        continuous_inputs.c.task_id==other_view['id'],continuous_inputs.c.case_id==case_id,
                        continuous_inputs.c.repetition==0))).scalar_one()
                _write_scale_checkpoint('foreign_packet','done',task_id=other_view['id'],repository_id=packet['binding_B']['repository_id'])
"""
        "                assert packet['binding_B']['repository_id']!=o"
        "riginal_packet['binding_B']['repository_id']"
        r"""
                facts['foreign_Task']=other_view['id'];facts['Task_ids'].append(other_view['id'])
            elif kind=='source_case_trial_mismatch':
                async with app.repository.lock,app.repository.engine.begin() as conn:
                    packet=(await conn.execute(select(continuous_inputs.c.body).where(
                        continuous_inputs.c.task_id==view['id'],continuous_inputs.c.case_id==case_values[1]['case_id'],
                        continuous_inputs.c.repetition==1))).scalar_one()
                assert (packet['case_id'],packet['repetition'])!=(case_id,0)
            _write_scale_checkpoint('packet_replacement','start',kind=kind,task_id=view['id'])
            async with app.repository.lock,app.repository.engine.begin() as conn:
                if kind=='row_missing':
"""
        "                    await conn.exec_driver_sql('DELETE FROM co"
        "ntinuous_inputs WHERE task_id=? AND case_id=? AND repetition=?"
        "',"
        r"""
                        (view['id'],case_id,0))
                else:
"""
        "                    await conn.execute(update(continuous_input"
        "s).where(continuous_inputs.c.task_id==view['id'],"
        r"""
                        continuous_inputs.c.case_id==case_id,continuous_inputs.c.repetition==0).values(body=packet))
                if kind in {'index_tamper','output_hash_mismatch'}:
"""
        "                    await conn.execute(update(tasks).where(tas"
        "ks.c.id==view['id']).values(body=body))"
        r"""
            _write_scale_checkpoint('packet_replacement','done',kind=kind,task_id=view['id'])
            _write_scale_checkpoint('packet_rejection','start',kind=kind)
            try:await app.repository.read_trial('local',view['id'],case_id,0)
            except ServiceError as error:
                assert error.code=='facts_corrupt'
                _write_scale_checkpoint('packet_rejection','done',kind=kind,code=error.code)
                frames=traceback.extract_tb(error.__traceback__)
                facts['proofs']['rejection']=dict(code=error.code,frames=[dict(file=v.filename,
                    function=v.name,line=v.lineno) for v in frames])
                if kind=='output_hash_mismatch':
                    frame=next(v for v in frames if v.name=='input_for_trial')
                    text=Path(frame.filename).read_text().splitlines()
                    assert 'metadata_row["output_hash"]' in text[frame.lineno-2]
            else:raise AssertionError('corrupted_source_accepted')
        assert all(value==0 for value in facts['calls'].values())
        facts['stage']='completed'
    except BaseException as error:
        facts['error']=dict(type=type(error).__name__,code=getattr(error,'code',None),message=str(error),
            frames=[dict(file=t.filename,function=t.name,line=t.lineno)
                for t in traceback.extract_tb(error.__traceback__)])
        raise
    finally:
        primary=sys.exception()
        _write_scale_checkpoint('cleanup','start',_defer_cleanup_error=True)
        for app in reversed(apps):
            _write_scale_checkpoint('owner_close','start',database=str(app.repository.path),_defer_cleanup_error=True)
            try:
                await app.close()
                _write_scale_checkpoint('owner_close','done',database=str(app.repository.path),closed=app.repository.closed,_defer_cleanup_error=True)
"""
        "            except BaseException as error:facts['close_errors'"
        "].append(dict(type=type(error).__name__,message=str(error)))"
        r"""
        if source_repo is not None:
            _write_scale_checkpoint('source_close','start',_defer_cleanup_error=True)
            await source_repo.close()
            _write_scale_checkpoint('source_close','done',closed=source_repo.closed,_defer_cleanup_error=True)
        continuous_store._source_input=original_input;continuous_store.prepare_task=original_prepare
        sys.setprofile(old_profile);sys.settrace(old_trace)
        await asyncio.sleep(0)
        facts['repositories_closed']=all(app.repository.closed and app.kernel.closed
            and app.repository._writer_fd is None and not app.kernel._inflight for app in apps)
        facts['source_closed']=source_repo is not None and source_repo.closed
        facts['pending']=[repr(t) for t in asyncio.all_tasks() if t not in before and not t.done()]
        facts['fd_after']=len(os.listdir('/proc/self/fd'))
        facts['source_hash_after']=hashlib.sha256(backup.read_bytes()).hexdigest()
"""
        "        if facts['repositories_closed'] and facts['source_clos"
        "ed'] and not facts['pending'] and not facts['close_errors']:"
        r"""
            for app in apps:
                _write_scale_checkpoint('writer_release','start',database=str(app.repository.path),_defer_cleanup_error=True)
                fd=os.open(str(app.repository.path)+'.lock',os.O_RDWR|os.O_NOFOLLOW)
                try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);fcntl.flock(fd,fcntl.LOCK_UN)
                finally:os.close(fd)
                _write_scale_checkpoint('writer_release','done',database=str(app.repository.path),released=True,_defer_cleanup_error=True)
                path=root/(app.repository.path.stem+'-backup.sqlite')
"""
        "                with closing(sqlite3.connect(app.repository.pa"
        "th.as_uri()+'?mode=ro',uri=True)) as db:"
        r"""
                    db.execute('PRAGMA query_only=ON')
                    with closing(sqlite3.connect(path)) as out:db.backup(out)
                facts['backups'].append(dict(database=str(app.repository.path),path=str(path),
                    sha256=hashlib.sha256(path.read_bytes()).hexdigest(),writer_lock_released=True))
        for name,handler in listeners:event.remove(Pool,name,handler)
        event.remove(Pool,'checkout',checkout);event.remove(Pool,'connect',connected)
        _write_scale_checkpoint('consumer','start',_defer_cleanup_error=True)
        publication_error=None
        try:(root/'header12-consumer.json').write_text(json.dumps(facts,indent=2))
        except Exception as error:
            publication_error=error
            facts['consumer_publication_error']=dict(type=type(error).__name__,message=str(error))
        else:_write_scale_checkpoint('consumer','done',_defer_cleanup_error=True)
        validation_error=None
        try:
            assert facts['source_hash_after']==facts['source_hash_before']
            assert facts['repositories_closed'] and facts['source_closed'] and not facts['pending']
            assert not facts['close_errors'] and facts['fd_after']<=facts['fd_before']
            counts=collections.Counter(v['event'] for v in facts['connections'])
"""
        "            assert counts['connect']==counts['close'] and coun"
        "ts['checkout']==counts['reset']==counts['checkin']"
        r"""
            assert facts['query_only'] and all(v==1 for v in facts['query_only'])
        except Exception as error:
            validation_error=error
            facts['cleanup_validation_error']=dict(type=type(error).__name__,message=str(error))
        evidence_errors=list(getattr(_write_scale_checkpoint,'evidence_errors',[]))
        evidence_overflow=getattr(_write_scale_checkpoint,'error_overflow',0)
        if evidence_errors or evidence_overflow:
            facts['checkpoint_errors']=evidence_errors
            facts['checkpoint_error_overflow']=evidence_overflow
            try:(root/'header12-consumer.json').write_text(json.dumps(facts,indent=2))
            except Exception as error:
                facts['checkpoint_error_publication_error']=dict(type=type(error).__name__,message=str(error))
                if publication_error is None:publication_error=error
        secondary=[error for error in [validation_error,publication_error] if error is not None]
        if evidence_errors or evidence_overflow:
            secondary.append(RuntimeError('checkpoint_evidence_incomplete:'+json.dumps(dict(errors=evidence_errors,overflow=evidence_overflow))))
        if primary is not None:
"""
        "            for error in secondary:primary.add_note('cleanup_o"
        "r_evidence_error:'+type(error).__name__+':'+str(error))"
        r"""
        elif secondary:raise secondary[0]
asyncio.run(main())
"""
    )
    compile(driver, "completion20-representation", "exec")
    source = (await asyncio.to_thread(Path(__file__).resolve)).parents[2] / "src"
    argv = [
        sys.executable,
        "-I",
        "-B",
        "-c",
        driver,
        str(source),
        str(tmp_path),
        json.dumps(inputs),
        kind,
    ]
    process, receipt, facts = await _run_scale_evidence_process(
        argv,
        tmp_path,
        execution_timeout_seconds=60 if kind == "source_wrong_owner" else 30,
        source_database=str(
            inputs[
                "old_layout2_and_layout3_dictionary"
                if kind in {"layout2_readonly", "layout3_dictionary_readonly"}
                else "main_300_source"
            ]["backup"]["path"]
        ),
        destination_database=str(tmp_path / "representation.sqlite"),
    )
    assert receipt["wait_returncode"] == process.returncode == 0, facts["error"]
    assert facts["stage"] == "completed"
    assert receipt["stdout_eof"] and receipt["stderr_eof"]
