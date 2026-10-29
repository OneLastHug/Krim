"""H1 schema与真实旧源码子程序草稿；材料待封，不读取旧业务库。"""

import asyncio
import hashlib
import json
import os
import signal
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

import pytest
from sqlalchemy import event
from sqlalchemy.engine import Engine

from krim_agent.contracts import ServiceError
from krim_agent.storage.sqlite import SQLiteStore

SCHEMA4_DRIVER = (
    "import asyncio,json,sys\nfrom pathlib import Path\nsource,database,receipt=map(Path,sy"
    "s.argv[1:4]);mode=sys.argv[4]\nsys.path.insert(0,str(source/'src'))\nfrom krim_agent.s"
    "torage.sqlite import SQLiteStore,SCHEMA_VERSION\nfrom krim_agent.composition import op"
    "en_application\nfrom krim_agent.contracts import RunRequest\nfrom krim_agent.runtime.f"
    "acts import project_facts\nfrom krim_agent.observability import project_trace\nassert "
    "SCHEMA_VERSION==4\nassert Path(sys.modules[SQLiteStore.__module__].__file__).is_relati"
    "ve_to(source/'src')\nasync def main():\n    if mode=='upgrade':\n        store=await S"
    "QLiteStore.open(database)\n        try:assert store.schema_version==4\n        finally"
    ":await store.close()\n        result={'version':4,'mode':mode,'closed':store.closed}\n"
    "    else:\n        app=await open_application(database)\n        selected_provider=app"
    ".service.bindings['target-v2'].loop.provider\n        try:\n            run=await app."
    "service.start(RunRequest(text='/add 2 3',request_key='h1-old-source',target='target-v2"
    "'))\n            await app.service.wait(run.id)\n            snapshot=await app.store."
    "snapshot(run.id)\n            assert snapshot.run.status=='completed'\n            ass"
    "ert selected_provider.calls==2 and selected_provider.active==0\n            result={'v"
    "ersion':app.store.schema_version,'run_id':run.id,\n                'facts':project_fac"
    "ts(snapshot).model_dump(mode='json'),\n                'trace':project_trace(snapshot)"
    ".model_dump(mode='json'),\n                'provider_calls':selected_provider.calls}\n"
    "        finally:await app.close()\n        result.update(closed=app.store.closed,produ"
    "cer_done=all(t.done() for t in app.service.tasks.values()),\n            provider_clos"
    "ed=selected_provider.closed,provider_streams_closed=selected_provider.streams_closed)"
    "\n        assert result['provider_closed'] and result['provider_streams_closed']==resu"
    "lt['provider_calls']\n    receipt.write_text(json.dumps(result,indent=2))\nasyncio.run"
    "(main())\n"
)

RECOVERY_DRIVER = (
    "import asyncio,json,sys\nfrom pathlib import Path\nsource,database,ready=map(Path,sys."
    "argv[1:4])\nsys.path.insert(0,str(source/'src'));sys.path.insert(0,str(source/'tests/a"
    "gent_core'))\nfrom fixtures.session_server import WireFixture\nfrom krim_agent.securit"
    "y import LOCAL\nfrom krim_agent.sessions.types import SessionCreate,TurnRequest\nfrom "
    "krim_agent.evaluation.hot_types import CapturePolicy\nfrom krim_agent.evaluation.adapt"
    "ers.krim_outbox import KrimOutboxPort\nasync def main():\n    wire=await WireFixture()"
    ".start();wire.hold_at.add(1)\n    try:\n        async with wire.application(database) "
    "as app:\n            port=KrimOutboxPort(app.store,app.sessions,LOCAL,CapturePolicy(en"
    "abled=True))\n            await port.configure()\n            session=await app.sessio"
    "ns.create(LOCAL,'local',SessionCreate(request_key='crash-session',target_key='demo'))"
    "\n            turn=await app.sessions.submit(LOCAL,'local',session.id,TurnRequest(requ"
    "est_key='crash-turn',text='/hold',expected_revision=0))\n            await wire.wait_r"
    "equests(1)\n            current=await app.sessions.turn(LOCAL,'local',session.id,turn."
    "id)\n            assert current.run_id is not None\n            ready.write_text(json."
    "dumps({'run_id':current.run_id,'requests':len(wire.requests),'database':str(database)}"
    "))\n            await asyncio.Event().wait()\n    finally:await wire.close()\nasyncio."
    "run(main())\n"
)


async def run_program(
    code,
    arguments,
    directory,
    *,
    timeout=60,  # noqa: ASYNC109  # 自有子进程预算参数
    cwd=None,
    interpreter=None,
):
    """只管理本次自有子程序；任何超时/信号都保留失败，不制造GREEN。"""
    directory.mkdir(parents=True, exist_ok=True)
    argv = [str(interpreter or sys.executable), "-I", "-B", "-c", code, *map(str, arguments)]
    process = await asyncio.create_subprocess_exec(
        *argv,
        cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    pidfd = os.pidfd_open(process.pid)
    readers = [
        asyncio.create_task(process.stdout.read(1048577)),
        asyncio.create_task(process.stderr.read(1048577)),
    ]
    fault = None
    signals = []
    try:
        async with asyncio.timeout(timeout):
            await process.wait()
    except BaseException as error:
        fault = {"type": type(error).__name__, "message": str(error)}
        if process.returncode is None:
            os.killpg(process.pid, signal.SIGTERM)
            signals.append("SIGTERM")
            try:
                async with asyncio.timeout(30):
                    await process.wait()
            except TimeoutError:
                os.killpg(process.pid, signal.SIGKILL)
                signals.append("SIGKILL")
                await process.wait()
        raise
    finally:
        stdout, stderr = await asyncio.gather(*readers)
        os.close(pidfd)
        facts = {
            "argv": argv,
            "cwd": str(cwd or Path.cwd()),
            "pid": process.pid,
            "wait_returncode": process.returncode,
            "pidfd_closed": True,
            "stdout_eof": process.stdout.at_eof(),
            "stderr_eof": process.stderr.at_eof(),
            "driver_sha256": hashlib.sha256(code.encode()).hexdigest(),
            "signals": signals,
            "fault": fault,
            "internal_cleanup": "unknown" if fault is not None else "read child receipt",
        }
        (directory / "process.json").write_text(json.dumps(facts, indent=2))
        (directory / "stdout.txt").write_bytes(stdout)
        (directory / "stderr.txt").write_bytes(stderr)
    assert process.returncode == 0, stderr.decode()
    assert len(stdout) <= 1048576 and len(stderr) <= 1048576
    assert not signals and facts["stdout_eof"] and facts["stderr_eof"]
    return facts


async def old_schema4(directory, *, database=None, mode="seed"):
    source = Path(os.environ["KRIM_PN3_HOT_SCHEMA4_SOURCE"])
    assert source.is_absolute() and (source / "src/krim_agent/storage/sqlite.py").is_file()
    directory.mkdir(parents=True, exist_ok=True)
    database = database or directory / "old.sqlite"
    receipt = directory / "schema4-producer.json"
    await run_program(SCHEMA4_DRIVER, [source, database, receipt, mode], directory / "producer")
    result = json.loads(receipt.read_text())
    assert result["version"] == 4 and result["closed"]
    return database, result


async def upgrade_v4_from_fixed_source(database):
    # 供已批准旧readonly4消费者借用；仅升级本次新合成schema3，不碰历史DB。
    return await old_schema4(
        database.parent / "fixed-schema4-upgrade", database=database, mode="upgrade"
    )


async def recover_real_source(root):
    from krim_agent.composition import open_application
    from krim_agent.evaluation.adapters.krim_outbox import KrimOutboxPort
    from krim_agent.evaluation.hot_types import CapturePolicy
    from krim_agent.security import LOCAL

    source = (
        Path(__file__).resolve()  # noqa: ASYNC240  # 固定本地身份检查
    ).parents[2]
    database, ready = root / "recovery.sqlite", root / "recovery-ready.json"
    argv = [
        sys.executable,
        "-I",
        "-B",
        "-c",
        RECOVERY_DRIVER,
        str(source),
        str(database),
        str(ready),
    ]
    process = await asyncio.create_subprocess_exec(
        *argv,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    pidfd = os.pidfd_open(process.pid)
    reads = [asyncio.create_task(process.stdout.read()), asyncio.create_task(process.stderr.read())]
    try:
        async with asyncio.timeout(5):
            while not await asyncio.to_thread(ready.exists):
                assert process.returncode is None
                await asyncio.sleep(0.005)
        record = json.loads(ready.read_text())
    finally:
        if process.returncode is None:
            os.killpg(process.pid, signal.SIGKILL)
        await process.wait()
        stdout, stderr = await asyncio.gather(*reads)
        os.close(pidfd)
        (root / "recovery-child-process.json").write_text(
            json.dumps(
                {
                    "argv": argv,
                    "pid": process.pid,
                    "wait": process.returncode,
                    "pidfd_closed": True,
                    "internal_cleanup": "unknown",
                    "stdout_eof": process.stdout.at_eof(),
                    "stderr_eof": process.stderr.at_eof(),
                },
                indent=2,
            )
        )
        (root / "recovery-child-stderr.txt").write_bytes(stderr)
    assert process.returncode == -9
    app = await open_application(database)
    try:
        snap = await app.store.snapshot(record["run_id"])
        port = KrimOutboxPort(app.store, app.sessions, LOCAL, CapturePolicy(enabled=True))
        captures = await port.rows()
        assert len(captures) == 1 and captures[0]["run_id"] == record["run_id"]
        return {
            "status": snap.run.status,
            "captures": len(captures),
            "provider_replay": app.provider.calls,
            "producer_wait": process.returncode,
            "producer_internal_cleanup": "unknown",
        }
    finally:
        await app.close()


def table_rows(database):
    with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as conn:
        names = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        return {name: conn.execute('SELECT * FROM "' + name + '"').fetchall() for name in names}


@pytest.mark.parametrize(
    "kind",
    [
        "empty",
        "v4_upgrade",
        "v4_readonly",
        "v5_readonly",
        "interrupted_upgrade",
        "wrong_schema",
        "disabled_legacy_bytes",
        "v5_eval_roundtrip",
        "v1_eval_rejected",
        "ddl_capture_meta",
        "ddl_capture_policies",
        "ddl_observation_outbox",
        "commit_failure",
    ],
)
async def test_source_schema5_migration(tmp_path, monkeypatch, kind):
    import sqlite3

    database = tmp_path / "schema.sqlite"
    if kind in {"v5_eval_roundtrip", "v1_eval_rejected"}:
        from test_session_storage import downgrade

        from krim_agent.composition import open_application
        from krim_agent.evaluation.types import EvalRequest, FreezeRequest

        if kind == "v1_eval_rejected":
            store = await SQLiteStore.open(database)
            await store.close()
            downgrade(database, 1)
            before = database.read_bytes()
            reader = await SQLiteStore.open(database, readonly=True)
            try:
                with pytest.raises(ServiceError, match="schema_upgrade_required"):
                    await reader.dataset("absent", "1")
            finally:
                await reader.close()
            assert database.read_bytes() == before
        else:
            app = await open_application(database)
            target_provider = None
            primary_error = None
            evidence = {
                "database": str(database),
                "request": None,
                "dataset_ref": None,
                "eval_id": None,
                "run_id": None,
                "wait": None,
                "report_hash": None,
                "scores": None,
                "output": None,
                "summary": None,
                "execution": None,
                "provider_before": None,
                "provider_after": None,
                "provider_after_replay": None,
                "original_key": None,
                "cleanup": None,
                "error": None,
            }
            try:
                from krim_agent.contracts import fingerprint

                target_provider = app.service.bindings["target-v2"].loop.provider
                evidence["provider_before"] = {
                    "binding": "target-v2",
                    "identity": id(target_provider),
                    "calls": target_provider.calls,
                    "active": target_provider.active,
                    "streams_closed": target_provider.streams_closed,
                    "closed": target_provider.closed,
                }
                frozen = await app.evaluations.freeze(
                    FreezeRequest.model_validate(
                        {
                            "name": "h1-legacy-eval",
                            "version": "1",
                            "cases": [
                                {
                                    "case_id": "one",
                                    "source_kind": "manual",
                                    "input": "/add 2 3",
                                    "reason": "独立合成参考",
                                    "reference": {
                                        "expected": "5",
                                        "expected_status": "completed",
                                        "expected_code": None,
                                        "expected_tools": None,
                                        "trajectory": None,
                                        "expected_schema": None,
                                    },
                                }
                            ],
                        }
                    )
                )
                ref = {k: getattr(frozen, k) for k in ["name", "version", "fingerprint"]}
                evidence["dataset_ref"] = ref
                request = EvalRequest.model_validate(
                    {
                        "request_key": "h1-v5-eval",
                        "dataset": ref,
                        "target_key": "target-v2",
                        "rules": [
                            {
                                "name": "exact_match",
                                "version": "1",
                                "config": {"normalize_whitespace": False},
                            },
                            {"name": "status_code", "version": "1", "config": {"match_code": True}},
                        ],
                        "execution": {
                            "repetitions": 1,
                            "concurrency": 1,
                            "item_timeout_seconds": 12,
                            "scorer_timeout_seconds": 1,
                            "batch_timeout_seconds": 600,
                        },
                    }
                )
                evidence["request"] = request.model_dump(mode="json")
                view = await app.evaluations.start(request)
                evidence["eval_id"] = view.id
                completed = await app.evaluations.wait(view.id)
                evidence["wait"] = completed.model_dump(mode="json")
                assert completed.id == view.id and completed.request_key == "h1-v5-eval"
                assert completed.status == "completed" and completed.code is None
                assert completed.planned_count == completed.finished_count == 1
                report = await app.evaluations.report(view.id)
                assert report is not None and app.store.schema_version == 5
                assert (await app.evaluations.by_key("h1-v5-eval")).id == view.id
                evidence.update(
                    report_hash=report["report_hash"],
                    summary=report["summary"],
                    execution=report["execution"],
                )
                assert (
                    report["complete"] is True
                    and report["status"] == "completed"
                    and report["code"] is None
                )
                assert (
                    report["eval_id"] == completed.id and report["revision"] == completed.revision
                )
                assert report["planned_count"] == 1 and len(report["items"]) == 1
                assert (
                    report["dataset_ref"] == ref
                    and report["dataset_fingerprint"] == frozen.fingerprint
                )
                assert report["dataset"] == frozen.model_dump(mode="json")
                expected_execution = request.execution.model_dump(mode="json")
                assert {
                    key: report["execution"][key] for key in expected_execution
                } == expected_execution
                assert all(type(report["execution"][key]) is int for key in expected_execution)
                assert {
                    key: report["execution"][key] for key in ["limits", "policy", "pre", "post"]
                } == {
                    key: report["target_descriptor"][key]
                    for key in ["limits", "policy", "pre", "post"]
                }
                assert report["target_descriptor"] == json.loads(
                    app.service.bindings["target-v2"].descriptor_json
                )
                assert report["report_hash"] == fingerprint(
                    {key: value for key, value in report.items() if key != "report_hash"}
                )
                item = report["items"][0]
                evidence.update(run_id=item["run_id"], scores=item["scores"], output=item["output"])
                assert item["case_id"] == "one" and item["repetition"] == 0
                assert (
                    item["status"] == "finished"
                    and item["code"] is None
                    and item["quality"] == "passed"
                )
                assert item["run_id"] is not None and item["output"]["run_id"] == item["run_id"]
                assert item["output"]["status"] == "completed" and item["output"]["code"] is None
                assert item["output"]["text"] == "5" and item["output"]["model_steps"] == 2
                assert item["output"]["facts_complete"] is True
                assert len(item["scores"]) == 2
                assert {score["rule"]["name"] for score in item["scores"]} == {
                    "exact_match",
                    "status_code",
                }
                assert all(
                    score["rule"]["version"] == "1"
                    and score["status"] == "scored"
                    and score["value"] == 1
                    for score in item["scores"]
                )
                assert report["summary"]["planned_cells"] == report["summary"]["scored_cells"] == 2
                assert report["summary"]["passed"] == report["summary"]["planned"] == 1
                assert report["summary"]["pass_rate"] == report["summary"]["cell_coverage"] == 1
                assert (
                    report["summary"]["errors"]
                    == report["summary"]["skipped"]
                    == report["summary"]["unexecuted_cells"]
                    == 0
                )
                persisted = await app.store.eval_snapshot(view.id)
                assert (
                    persisted["report"] == report
                    and persisted["report_hash"] == report["report_hash"]
                )
                assert persisted["request_json"] == request.model_dump(mode="json")
                assert (
                    len(persisted["items"]) == 1
                    and persisted["items"][0]["run_id"] == item["run_id"]
                )
                child = await app.store.snapshot(item["run_id"])
                child_facts = await app.service.result(item["run_id"])
                assert child.run.id == child_facts.run.id == item["run_id"]
                assert child.run.status == "completed" and child.run.code is None
                assert child.request.target == "target-v2" and child.request.text == "/add 2 3"
                assert child.request.request_key == persisted["items"][0]["run_request_key"]
                assert child.target == persisted["target"] == report["target_descriptor"]
                assert child_facts.text == "5" and child_facts.model_steps == 2
                assert len(child_facts.tools) == len(item["output"]["tools"]) == 1
                tool = child_facts.tools[0]
                assert tool.name == "add" and tool.status == "ok" and tool.code is None
                assert tool.executor_started is True and tool.result_available is True
                assert tool.arguments == {"left": 2.0, "right": 3.0} and tool.value == {"sum": 5.0}
                assert item["output"]["tools"][0]["call_id"] == tool.call_id
                assert item["output"]["tools"][0]["value_fingerprint"] == fingerprint(tool.value)
                assert sum(event.kind == "tool_start" for event in child.events) == 1
                assert sum(event.kind == "tool_end" for event in child.events) == 1
                assert target_provider is app.service.bindings["target-v2"].loop.provider
                evidence["provider_after"] = {
                    "identity": id(target_provider),
                    "calls": target_provider.calls,
                    "active": target_provider.active,
                    "streams_closed": target_provider.streams_closed,
                }
                assert target_provider.calls - evidence["provider_before"]["calls"] == 2
                assert (
                    target_provider.streams_closed - evidence["provider_before"]["streams_closed"]
                    == 2
                )
                assert (
                    target_provider.streams_closed == target_provider.calls
                    and target_provider.active == 0
                )
                run_tasks = dict(app.service.tasks)
                eval_tasks = dict(app.evaluations.tasks)
                assert set(run_tasks) == {item["run_id"]} and set(eval_tasks) == {view.id}
                assert all(task.done() for task in run_tasks.values()) and all(
                    task.done() for task in eval_tasks.values()
                )
                fetched = await app.evaluations.get(view.id)
                found = await app.evaluations.by_key("h1-v5-eval")
                replayed = await app.evaluations.start(request)
                assert fetched == found == replayed == completed
                assert await app.evaluations.report(view.id) == report
                assert await app.store.eval_snapshot(view.id) == persisted
                assert (await app.store.snapshot(item["run_id"])).run == child.run
                assert app.service.tasks == run_tasks and app.evaluations.tasks == eval_tasks
                assert target_provider.calls == evidence["provider_after"]["calls"]
                assert (
                    target_provider.streams_closed == evidence["provider_after"]["streams_closed"]
                )
                assert target_provider.active == 0
                evidence["provider_after_replay"] = {
                    "identity": id(target_provider),
                    "calls": target_provider.calls,
                    "active": target_provider.active,
                    "streams_closed": target_provider.streams_closed,
                }
                evidence["original_key"] = {
                    "get_eval_id": fetched.id,
                    "by_key_eval_id": found.id,
                    "start_eval_id": replayed.id,
                    "report_hash": report["report_hash"],
                    "child_run_ids": [row["run_id"] for row in persisted["items"]],
                    "run_producers": {key: id(task) for key, task in run_tasks.items()},
                    "eval_producers": {key: id(task) for key, task in eval_tasks.items()},
                }
            except BaseException as error:
                primary_error = error
                evidence["error"] = {
                    "type": type(error).__name__,
                    "message": str(error)[:2048],
                    "message_truncated": len(str(error)) > 2048,
                }
                raise
            finally:
                await app.close()
                evidence["cleanup"] = {
                    "store_closed": app.store.closed,
                    "eval_service_closed": app.evaluations.closed,
                    "run_service_closed": app.service.closed,
                    "eval_producers_done": all(
                        task.done() for task in app.evaluations.tasks.values()
                    ),
                    "run_producers_done": all(task.done() for task in app.service.tasks.values()),
                    "eval_producers": [
                        {
                            "eval_id": key,
                            "identity": id(task),
                            "done": task.done(),
                            "cancelled": task.cancelled(),
                        }
                        for key, task in app.evaluations.tasks.items()
                    ],
                    "run_producers": [
                        {
                            "run_id": key,
                            "identity": id(task),
                            "done": task.done(),
                            "cancelled": task.cancelled(),
                        }
                        for key, task in app.service.tasks.items()
                    ],
                    "target_provider_closed": None
                    if target_provider is None
                    else target_provider.closed,
                    "target_provider_active": None
                    if target_provider is None
                    else target_provider.active,
                    "target_provider_calls": None
                    if target_provider is None
                    else target_provider.calls,
                    "target_provider_streams_closed": None
                    if target_provider is None
                    else target_provider.streams_closed,
                }
                try:
                    (tmp_path / "v5-eval-roundtrip.json").write_text(json.dumps(evidence, indent=2))
                except Exception as error:
                    if primary_error is None:
                        raise
                    primary_error.add_note("v5-eval-roundtrip evidence write failed: " + str(error))
                if primary_error is None:
                    assert (
                        evidence["cleanup"]["store_closed"]
                        and evidence["cleanup"]["eval_service_closed"]
                    )
                    assert evidence["cleanup"]["run_service_closed"]
                    assert (
                        evidence["cleanup"]["eval_producers_done"]
                        and evidence["cleanup"]["run_producers_done"]
                    )
                    assert (
                        evidence["cleanup"]["target_provider_closed"]
                        and evidence["cleanup"]["target_provider_active"] == 0
                    )
        return
    if kind == "empty":
        store = await SQLiteStore.open(database)
        await store.close()
        assert store.schema_version == 5
        assert {"capture_meta", "capture_policies", "observation_outbox"} <= set(
            table_rows(database)
        )
        return
    if kind in {"v5_readonly", "wrong_schema"}:
        store = await SQLiteStore.open(database)
        await store.close()
        if kind == "wrong_schema":
            with closing(sqlite3.connect(database)) as conn:
                conn.execute("PRAGMA user_version=6")
                conn.commit()
            before = database.read_bytes()
            with pytest.raises(ServiceError, match="database_invalid"):
                await SQLiteStore.open(database)
            assert database.read_bytes() == before
        else:
            before = database.read_bytes()
            reader = await SQLiteStore.open(database, readonly=True)
            assert reader.schema_version == 5
            await reader.close()
            assert database.read_bytes() == before
        return
    database, seed = await old_schema4(tmp_path / "old-producer", database=database)
    before = database.read_bytes()
    original_rows = table_rows(database)
    if kind == "v4_readonly":
        reader = await SQLiteStore.open(database, readonly=True)
        assert reader.schema_version == 4
        await reader.close()
        assert database.read_bytes() == before
        return
    failure = kind.startswith("ddl_") or kind in {"commit_failure", "interrupted_upgrade"}
    hits = []

    def fail_sql(conn, cursor, statement, parameters, context, executemany):
        table = kind.removeprefix("ddl_")
        if kind.startswith("ddl_") and "CREATE TABLE " + table in statement:
            hits.append(statement)
            raise OSError("actual schema DDL failure")
        if kind == "interrupted_upgrade" and "CREATE TABLE capture_policies" in statement:
            hits.append(statement)
            raise asyncio.CancelledError

    def fail_commit(conn):
        if kind == "commit_failure":
            hits.append("commit")
            raise OSError("actual migration commit failure")

    if failure:
        event.listen(Engine, "after_cursor_execute", fail_sql)
        event.listen(Engine, "commit", fail_commit)
        try:
            with pytest.raises(ServiceError, match="database_invalid"):
                await SQLiteStore.open(database)
        finally:
            event.remove(Engine, "after_cursor_execute", fail_sql)
            event.remove(Engine, "commit", fail_commit)
        assert hits and database.read_bytes() == before and table_rows(database) == original_rows
    store = await SQLiteStore.open(database)
    try:
        assert store.schema_version == 5
        after = table_rows(database)
        assert {k: after[k] for k in original_rows} == original_rows
        if kind == "disabled_legacy_bytes":
            from krim_agent.observability import project_trace
            from krim_agent.runtime.facts import project_facts

            snap = await store.snapshot(seed["run_id"])
            assert project_facts(snap).model_dump(mode="json") == seed["facts"]
            assert project_trace(snap).model_dump(mode="json") == seed["trace"]
            assert after["observation_outbox"] == []
    finally:
        await store.close()
    second = await SQLiteStore.open(database)
    await second.close()
    with pytest.raises(OSError):
        os.fstat(store.fd)
