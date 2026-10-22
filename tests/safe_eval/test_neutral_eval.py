"""真实普通进程→产物→独立固定参考→报告；不运行商业目标。"""

import asyncio
import copy
import hashlib
import json
import os
import sys
from dataclasses import replace
from importlib.resources import files
from pathlib import Path

import pytest

from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.evaluation.app import open_eval
from krim_agent.evaluation.domain import Budget, DatasetSpec, TaskSpec
from krim_agent.evaluation.reports import gate_reports, validate_neutral_report
from krim_agent.evaluation.repository import Repository
from krim_agent.evaluation.runner import ItemWork


def example():
    return DatasetSpec.model_validate(
        json.loads(
            files("krim_agent.evaluation.examples").joinpath("reconciliation.v1.json").read_text()
        )
    )


@pytest.mark.parametrize("case_id", ["basic", "duplicates_returns", "expected_rejection"])
async def test_non_krim_task_produces_scored_artifacts(tmp_path, case_id):
    source = example()
    dataset = DatasetSpec(
        name=source.name,
        version=source.version,
        cases=[c for c in source.cases if c.case_id == case_id],
    )
    async with await open_eval(tmp_path / "ordinary.sqlite") as app:
        assert app.adapter.descriptor()["version"] == "reconcile-reference/1"
        frozen = await app.repository.freeze(dataset)
        request = TaskSpec(
            request_key="ordinary",
            dataset=frozen.ref(),
            metrics=app.repository.metric_registry.specs(),
        )
        started = await app.run(request)
        result = await app.kernel.wait(started["id"])
        report = await app.repository.report(result["id"])
        item = report["items"][0]
        assert result["status"] == "completed"
        assert item["output"]["exit_code"] == (2 if case_id == "expected_rejection" else 0)
        assert item["quality"] == "passed", item
        assert report["summary"]["cell_coverage"] == 1
        assert app.adapter.execute_calls == 1
        assert item["output"]["token_usage"] is None and item["output"]["cost"] is None
        assert (await app.run(request))["id"] == started["id"]
        assert app.adapter.execute_calls == 1
    assert app.repository.closed and all(t.done() for t in app.kernel.tasks.values())
    assert all(
        j.process.returncode is not None and j.pidfd == -1 for j in app.adapter.jobs.values()
    )


def single():
    source = example()
    return DatasetSpec(
        name=source.name,
        version=source.version,
        cases=[c for c in source.cases if c.case_id == "basic"],
    )


async def requested(app, key="ordinary", **updates):
    dataset = await app.repository.freeze(single())
    return TaskSpec(
        request_key=key,
        dataset=dataset.ref(),
        metrics=app.repository.metric_registry.specs(),
        **updates,
    )


def resources(app, tmp_path, name):
    records = {
        "uid": os.getuid(),
        "repository_closed": app.repository.closed,
        "connections": app.repository.resources,
        "kernel_tasks": {k: t.done() for k, t in app.kernel.tasks.items()},
        "execute_calls": app.adapter.execute_calls,
        "scorer_calls": app.repository.metric_registry.calls,
        "jobs": [
            {
                "execution_ref": j.ref.id,
                "pid": j.process.pid,
                "wait_returncode": j.process.returncode,
                "pidfd_closed": j.pidfd == -1,
                "collector_joined": j.collector.done(),
                "facts": j.collector.result()
                if j.collector.done() and j.collector.exception() is None
                else None,
            }
            for j in app.adapter.jobs.values()
        ],
    }
    (tmp_path / f"{name}-resources.json").write_text(json.dumps(records, indent=2))
    assert records["repository_closed"] and all(records["kernel_tasks"].values())
    assert all(
        j["pidfd_closed"] and j["collector_joined"] and j["wait_returncode"] is not None
        for j in records["jobs"]
    )
    events = [v["event"] for v in records["connections"]]
    assert events.count("checkout") == events.count("checkin")
    assert events.count("connect") == events.count("close")
    assert events.count("reset") == events.count("checkin")


@pytest.mark.parametrize("kind", ["manual", "files"])
async def test_dataset_freeze_isolated_and_idempotent(tmp_path, monkeypatch, kind):
    async with await open_eval(tmp_path / "freeze.sqlite") as app:
        dataset = single()
        expected = dataset.model_dump(mode="json")
        entered, release = asyncio.Event(), asyncio.Event()
        original = app.repository._write

        async def blocked(operation):
            entered.set()
            await release.wait()
            return await original(operation)

        monkeypatch.setattr(app.repository, "_write", blocked)
        task = asyncio.create_task(app.repository.freeze(dataset))
        await asyncio.wait_for(entered.wait(), 2)
        if kind == "files":
            dataset.cases[0].files["transactions.csv"] = "caller mutated"
        else:
            dataset.cases[0].reference["files"]["ledger.json"]["accounts"]["a"] = -99
        release.set()
        result = await task
        assert result.model_dump(mode="json") == expected
        monkeypatch.setattr(app.repository, "_write", original)
        assert await app.repository.freeze(DatasetSpec.model_validate(expected)) == result
        with pytest.raises(ServiceError, match="revision_conflict"):
            await app.repository.freeze(dataset)
        request = await requested(app)
        before = request.model_dump(mode="json")
        entered.clear()
        release.clear()
        by_key = app.repository.by_key

        async def block_key(value):
            entered.set()
            await release.wait()
            return await by_key(value)

        monkeypatch.setattr(app.repository, "by_key", block_key)
        submit = asyncio.create_task(app.run(request))
        await asyncio.wait_for(entered.wait(), 2)
        request.metrics[0].config["caller"] = ["mutated"]
        release.set()
        view = await submit
        result = await app.kernel.wait(view["id"])
        assert result["request_json"] == before
        assert result["request_hash"] == fingerprint(before)
        with pytest.raises(ServiceError, match="key_conflict"):
            await app.run(request)
    resources(app, tmp_path, "freeze")


@pytest.mark.parametrize(
    "point",
    [
        "intent_return_lost",
        "started_before_attach",
        "result_return_lost",
        "readonly_reopen",
    ],
)
async def test_intent_commit_and_result_unknown_zero_replay(tmp_path, monkeypatch, point):
    database = tmp_path / "unknown.sqlite"
    app = await open_eval(database)
    try:
        request = await requested(app)
        original = app.repository._write
        lost = False

        async def lose(operation):
            nonlocal lost
            value = await original(operation)
            old = await app.repository.by_key(request.model_dump(mode="json"))
            item = old["items"][0] if old is not None else None
            match = item is not None and (
                point == "intent_return_lost"
                and item["status"] == "launch_intent"
                or point == "result_return_lost"
                and item["content_hash"] is not None
            )
            if match and not lost:
                lost = True
                raise OSError("SYNTHETIC_COMMIT_RETURN_LOST")
            return value

        monkeypatch.setattr(app.repository, "_write", lose)
        if point == "started_before_attach":

            async def unknown(*args):
                raise ServiceError("execution_unknown")

            monkeypatch.setattr(app.repository, "attach", unknown)
        view = await app.run(request)
        result = await app.kernel.wait(view["id"])
        if point == "started_before_attach":
            assert result["status"] == "failed" and result["code"] == "execution_unknown"
            assert result["items"][0]["output"]["process"]["stdout_eof"] is True
            with pytest.raises(ServiceError, match="storage_unavailable"):
                await app.run(request.model_copy(update={"request_key": "new"}))
        else:
            assert result["status"] == "completed"
            assert (await app.run(request))["id"] == view["id"]
        assert app.adapter.execute_calls == 1
        scorer_calls = app.repository.metric_registry.calls
        report = await app.repository.report(view["id"])
        if point in {"intent_return_lost", "result_return_lost"}:
            assert lost
    finally:
        await app.close()
    resources(app, tmp_path, point)
    before = hashlib.sha256(database.read_bytes()).hexdigest()
    async with await open_eval(database, readonly=True) as cold:
        assert cold.adapter is None and cold.repository.registry == {}
        assert (await cold.repository.by_key(request.model_dump(mode="json")))["id"] == view["id"]
        assert await cold.repository.report(view["id"]) == report
        assert cold.repository.metric_registry.calls == 0
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before
    assert app.adapter.execute_calls == 1 and app.repository.metric_registry.calls == scorer_calls


@pytest.mark.parametrize(
    "action",
    [
        "cancel",
        "repeat_cancel",
        "timeout",
        "close",
        "scorer_failure",
    ],
)
async def test_cancel_timeout_and_close_join_owned_work(tmp_path, monkeypatch, action):
    app = await open_eval(tmp_path / "cancel.sqlite")
    entered, release, cleaning = asyncio.Event(), asyncio.Event(), asyncio.Event()
    pending = []
    try:
        request = await requested(
            app,
            execution=Budget(item_timeout_seconds=1 if action == "timeout" else 20),
        )
        if action == "scorer_failure":
            registry = app.repository.metric_registry
            registered = registry._registered["artifact_match"]

            async def broken(*args):
                raise ValueError("SYNTHETIC_PLUGIN_ERROR")

            registry._registered["artifact_match"] = replace(registered, function=broken)
        else:
            original = app.adapter.execute

            async def held(prepared):
                ref = await original(prepared)
                entered.set()
                await release.wait()
                return ref

            monkeypatch.setattr(app.adapter, "execute", held)
            # 冻结实际宿主测试装配的方法身份，后续不允许任何漂移。
            app.adapter._methods = app.adapter._identities()
            clean = ItemWork.cleanup

            async def measured(work):
                cleaning.set()
                await clean(work)

            monkeypatch.setattr(ItemWork, "cleanup", measured)
        view = await app.run(request)
        if action == "scorer_failure":
            result = await app.kernel.wait(view["id"])
            score = result["items"][0]["scores"][0]
            assert score["status"] == "error" and score["reason"] == "scorer_error"
            assert result["items"][0]["output"]["exit_code"] == 0
        else:
            await asyncio.wait_for(entered.wait(), 2)
            assert app.adapter.execute_calls == 1
            if action == "close":
                pending.append(asyncio.create_task(app.close()))
            elif action == "timeout":
                pending.append(asyncio.create_task(app.kernel.wait(view["id"])))
            else:
                pending.append(asyncio.create_task(app.kernel.cancel(view["id"])))
                if action == "repeat_cancel":
                    pending.append(asyncio.create_task(app.kernel.cancel(view["id"])))
            await asyncio.wait_for(cleaning.wait(), 2)
            assert all(not t.done() for t in pending)
            with pytest.raises(ServiceError, match="database_busy"):
                await Repository.open(app.repository.path)
            if action == "repeat_cancel":
                pending[0].cancel()
                pending[0].cancel()
            release.set()
            outcomes = await asyncio.gather(*pending, return_exceptions=True)
            if action == "repeat_cancel":
                assert isinstance(outcomes[0], asyncio.CancelledError)
            result = await app.repository.snapshot(view["id"]) if action != "close" else None
            if result is not None:
                assert result["status"] == ("completed" if action == "timeout" else "aborted")
                assert result["items"][0]["quality"] == (
                    "timed_out" if action == "timeout" else None
                )
    finally:
        release.set()
        await asyncio.gather(*pending, return_exceptions=True)
        await app.close()
    resources(app, tmp_path, action)
    writer = await Repository.open(app.repository.path)
    await writer.close()


@pytest.mark.parametrize(
    "kind",
    [
        "missing",
        "invalid_artifact",
        "plugin_error",
        "higher_mean_case_regressed",
    ],
)
async def test_metrics_evidence_coverage_and_case_regression(tmp_path, monkeypatch, kind):
    async with await open_eval(tmp_path / "metrics.sqlite") as app:
        if kind == "higher_mean_case_regressed":
            dataset = await app.repository.freeze(example())
            request = TaskSpec(
                request_key="baseline",
                dataset=dataset.ref(),
                metrics=app.repository.metric_registry.specs(),
            )
        else:
            request = await requested(app)
        registry = app.repository.metric_registry
        old = registry._registered["artifact_match"]
        if kind == "missing":

            async def missing(case, output, config):
                case = copy.deepcopy(case)
                case["reference"].pop("files")
                return await old.function(case, output, config)

            registry._registered["artifact_match"] = replace(old, function=missing)
        elif kind == "invalid_artifact":

            async def invalid(case, output, config):
                output["artifacts"]["ledger.json"]["text"] = "{bad"
                return await old.function(case, output, config)

            registry._registered["artifact_match"] = replace(old, function=invalid)
        elif kind == "plugin_error":

            async def error(*args):
                raise ValueError("PLUGIN_FAILED")

            registry._registered["artifact_match"] = replace(old, function=error)
        view = await app.run(request)
        await app.kernel.wait(view["id"])
        report = await app.repository.report(view["id"])
        if kind == "missing":
            assert report["summary"]["planned_cells"] == 2
            assert report["summary"]["cell_coverage"] == 0.5
            assert report["items"][0]["quality"] == "unscored"
        elif kind == "invalid_artifact":
            assert report["items"][0]["quality"] == "failed"
            assert report["items"][0]["scores"][0]["reason"] == "invalid_artifact"
            assert report["items"][0]["scores"][1]["value"] == 1
        elif kind == "plugin_error":
            assert report["items"][0]["quality"] == "error"
            assert report["items"][0]["scores"][0]["reason"] == "scorer_error"
        else:
            # 同一个共享统计/Gate，以计划单元重算；均值上升也拒逐Case退化。
            before, after = copy.deepcopy(report), copy.deepcopy(report)
            for item in before["items"][1:]:
                item["scores"][0]["value"] = 0
                item["quality"] = "failed"
            after["items"][0]["scores"][0]["value"] = 0
            after["items"][0]["quality"] = "failed"
            from krim_agent.evaluation.reports import summarize

            for value in [before, after]:
                value["summary"] = summarize(value["items"], value["rules"], neutral=True)
                value["report_hash"] = fingerprint(
                    {k: v for k, v in value.items() if k != "report_hash"}
                )
            assert after["summary"]["mean"] > before["summary"]["mean"]
            assert gate_reports(before, after)["status"] == "fail"
            for field in ["dataset_fingerprint", "execution_fingerprint", "rules_fingerprint"]:
                changed = copy.deepcopy(after)
                changed[field] = "0" * 64
                changed["report_hash"] = fingerprint(
                    {k: v for k, v in changed.items() if k != "report_hash"}
                )
                assert gate_reports(before, changed)["status"] == "invalid"
        corrupted = copy.deepcopy(report)
        corrupted["summary"]["passed"] += 1
        with pytest.raises(ServiceError, match="facts_corrupt"):
            validate_neutral_report(corrupted)
    resources(app, tmp_path, kind)


@pytest.mark.parametrize("where", ["admitted", "prepare_after_await", "collect_after_await"])
async def test_adapter_binding_drift_before_side_effect(tmp_path, monkeypatch, where):
    app = await open_eval(tmp_path / "fence.sqlite")
    entered, release = asyncio.Event(), asyncio.Event()
    replacement_calls = 0
    task = None
    try:
        request = await requested(app)
        if where == "admitted":
            original = app.repository.set_running

            async def held(task_id):
                await original(task_id)
                entered.set()
                await release.wait()

            monkeypatch.setattr(app.repository, "set_running", held)
        else:
            name = "prepare" if where == "prepare_after_await" else "collect"
            original = getattr(app.adapter, name)

            async def held(value):
                result = await original(value)
                entered.set()
                await release.wait()
                return result

            monkeypatch.setattr(app.adapter, name, held)
            app.adapter._methods = app.adapter._identities()
        view = await app.run(request)
        task = asyncio.create_task(app.kernel.wait(view["id"]))
        await asyncio.wait_for(entered.wait(), 2)

        async def forbidden(prepared):
            nonlocal replacement_calls
            replacement_calls += 1
            raise AssertionError("replacement must not execute")

        monkeypatch.setattr(app.adapter, "execute", forbidden)
        release.set()
        result = await task
        assert result["code"] == "target_binding_changed"
        assert replacement_calls == 0
        assert app.adapter.execute_calls == (1 if where == "collect_after_await" else 0)
        if where == "collect_after_await":
            assert result["items"][0]["output"]["exit_code"] == 0
            assert result["items"][0]["output"]["artifacts"]["ledger.json"]["sha256"]
    finally:
        release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await app.close()
    resources(app, tmp_path, where)


@pytest.mark.parametrize("mode", ["source_import", "readonly_report"])
async def test_core_dependency_direction_and_readonly_paths(tmp_path, monkeypatch, mode):
    if mode == "source_import":
        source = Path(__file__).parents[2] / "src"
        code = (
            f"import sys;sys.path.insert(0,{str(source)!r});"
            + "import krim_agent.evaluation.cli;import krim_agent.evaluation.app;"
            + (
                "assert not any(any(k==p or k.startswith(p+'.') for p in ['krim_a"
                "gent.composition','krim_agent.runtime','krim_agent.execution','k"
                "rim_agent.workspace','krim_agent.providers']) for k in sys.modul"
                "es);print(krim_agent.evaluation.app.__file__)"
            )
        )
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            "-B",
            "-c",
            code,
            cwd=tmp_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        assert process.returncode == 0, stderr.decode()
        assert str(source).encode() in stdout
    else:
        database = tmp_path / "readonly.sqlite"
        app = await open_eval(database)
        request = await requested(app)
        view = await app.run(request)
        await app.kernel.wait(view["id"])
        expected = await app.repository.report(view["id"])
        await app.close()
        from krim_agent.evaluation import app as module

        def forbidden(*args):
            raise AssertionError("readonly must not assemble adapter")

        monkeypatch.setattr(module, "LocalProcessAdapter", forbidden)
        before = hashlib.sha256(database.read_bytes()).hexdigest()
        async with await open_eval(database, readonly=True) as reader:
            assert reader.adapter is None
            assert await reader.repository.report(view["id"]) == expected
        assert hashlib.sha256(database.read_bytes()).hexdigest() == before


async def test_cleanup_unknown_blocks_new_dispatch(tmp_path, monkeypatch):
    from krim_agent.evaluation.domain import CleanupResult

    app = await open_eval(tmp_path / "cleanup.sqlite")
    try:
        request = await requested(app)
        original = app.adapter.cancel

        async def unknown(ref):
            result = await original(ref)
            return CleanupResult(False, "unknown", result.observation)

        monkeypatch.setattr(app.adapter, "cancel", unknown)
        app.adapter._methods = app.adapter._identities()

        async def failed(*args):
            raise ServiceError("execution_unknown")

        monkeypatch.setattr(app.repository, "attach", failed)
        view = await app.run(request)
        result = await app.kernel.wait(view["id"])
        assert result["code"] == "execution_unknown"
        assert app.kernel.cleanup_unknown and not app.repository.healthy
        with pytest.raises(ServiceError, match="storage_unavailable"):
            await app.run(request.model_copy(update={"request_key": "must-not-start"}))
        assert app.adapter.execute_calls == 1
        monkeypatch.setattr(app.adapter, "cancel", original)
    finally:
        await app.close()
    resources(app, tmp_path, "cleanup-unknown")


async def test_recovery_intent_is_unknown_without_automatic_replay(tmp_path):
    database = tmp_path / "interrupted.sqlite"
    app = await open_eval(database)
    request = await requested(app)
    snapshot, created = await app.repository.begin(request.model_dump(mode="json"))
    assert created
    trial = app.repository.plan(snapshot).trials[0]
    await app.repository.intent(trial)
    await app.close()
    assert app.adapter.execute_calls == 0
    async with await open_eval(database) as reopened:
        snapshot = await reopened.repository.snapshot(snapshot["id"])
        assert snapshot["status"] == "interrupted"
        assert snapshot["items"][0]["status"] == "unknown"
        assert not reopened.repository.healthy
        assert reopened.adapter.execute_calls == 0
        assert reopened.repository.metric_registry.calls == 0
        with pytest.raises(ServiceError, match="storage_unavailable"):
            await reopened.run(request.model_copy(update={"request_key": "new"}))


@pytest.mark.parametrize("kind", ["symlink", "fifo", "capacity", "race"])
def test_bounded_artifact_evidence_rejects_nonregular_and_replacement(tmp_path, monkeypatch, kind):
    from krim_agent.evaluation.evidence import bounded_files

    directory = tmp_path / "outputs"
    directory.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("synthetic external sentinel")
    if kind == "symlink":
        (directory / "data").symlink_to(outside)
    elif kind == "fifo":
        os.mkfifo(directory / "data")
    elif kind == "capacity":
        (directory / "data").write_bytes(b"a" * 1048577)
    else:
        (directory / "data").write_text("before")
        original = os.open

        def replace_leaf(path, flags, *args, **kwargs):
            if path == "data":
                (directory / "data").unlink()
                (directory / "data").symlink_to(outside)
            return original(path, flags, *args, **kwargs)

        monkeypatch.setattr(os, "open", replace_leaf)
    with pytest.raises((ValueError, OSError)):
        bounded_files(directory)
    assert outside.read_text() == "synthetic external sentinel"


async def test_repository_close_joins_real_transaction_before_writer_release(tmp_path, monkeypatch):
    database = tmp_path / "writer.sqlite"
    app = await open_eval(database)
    entered, release = asyncio.Event(), asyncio.Event()
    original = app.repository._write

    async def block_commit(operation):
        async def actual(conn):
            result = await operation(conn)
            entered.set()
            await release.wait()
            return result

        return await original(actual)

    monkeypatch.setattr(app.repository, "_write", block_commit)
    freeze = asyncio.create_task(app.repository.freeze(single()))
    await asyncio.wait_for(entered.wait(), 2)
    close = asyncio.create_task(app.close())
    try:
        with pytest.raises(ServiceError, match="database_busy"):
            await Repository.open(database)
        assert not close.done()
    finally:
        release.set()
        await asyncio.gather(freeze, close)
    assert app.repository.closed
    writer = await Repository.open(database)
    assert (await writer.dataset("reconciliation", "1")).cases
    await writer.close()
    resources(app, tmp_path, "writer-close")


def pn1_repair_worker(app, tmp_path, *, live=False, large=False):
    """宿主注册的有限fixture包装真实参考程序，绝不接受Dataset argv。"""
    actual = app.adapter.worker
    worker = tmp_path / "repair-reference.py"
    worker.write_text(
        "import pathlib,runpy,sys,time\n"
        "try:\n"
        f"    runpy.run_path({str(actual)!r},run_name='__main__')\n"
        "except SystemExit as result:\n"
        "    if result.code not in (0,None): raise\n"
        + (
            "pathlib.Path('outputs/blob.txt').write_text('x'*1048000)\n"
            "sys.stdout.write('o'*250000);sys.stdout.flush()\n"
            "sys.stderr.write('e'*120000);sys.stderr.flush()\n"
            if large
            else ""
        )
        + (
            "pathlib.Path('alive').write_text('actual-worker')\n"
            "while not pathlib.Path('release').exists(): time.sleep(0.005)\n"
            if live
            else ""
        )
    )
    app.adapter.worker = worker
    app.adapter._descriptor["implementation_digest"] = hashlib.sha256(
        worker.read_bytes()
    ).hexdigest()


@pytest.mark.parametrize("window", ["execute_before_ref", "collect_wait"])
async def test_pn1_repair_timeout_preserves_real_observation(tmp_path, monkeypatch, window):
    from sqlalchemy import select

    from krim_agent.evaluation.repository import evidence, trials

    database = tmp_path / "repair-timeout.sqlite"
    app = await open_eval(database)
    entered, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    waiting = None
    try:
        pn1_repair_worker(app, tmp_path, live=True)
        request = await requested(app, execution=Budget(item_timeout_seconds=1))
        method = "execute" if window == "execute_before_ref" else "collect"
        original = getattr(app.adapter, method)

        async def held(value):
            if window == "execute_before_ref":
                ref = await original(value)
                entered.set()
                await release.wait()
                return ref
            entered.set()
            return await original(value)

        monkeypatch.setattr(app.adapter, method, held)
        app.adapter._methods = app.adapter._identities()
        cleanup = ItemWork.cleanup

        async def measured(work):
            cleaning.set()
            return await cleanup(work)

        monkeypatch.setattr(ItemWork, "cleanup", measured)
        view = await app.run(request)
        await asyncio.wait_for(entered.wait(), 2)
        job = next(iter(app.adapter.jobs.values()))
        async with asyncio.timeout(2):
            while not (Path(job.prepared.directory) / "alive").exists():
                assert job.process.returncode is None
                await asyncio.sleep(0.005)
        assert job.process.returncode is None
        waiting = asyncio.create_task(app.kernel.wait(view["id"]))
        await asyncio.wait_for(cleaning.wait(), 2)
        if window == "execute_before_ref":
            assert not waiting.done() and job.process.returncode is None
        release.set()
        result = await waiting
        report = await app.repository.report(view["id"])
        observation = job.collector.result()
        assert result["status"] == "completed"
        assert result["items"][0]["quality"] == "timed_out"
        assert result["items"][0]["execution_ref"] == job.ref.id
        assert result["items"][0]["output"] == observation
        assert report["items"][0]["output"] == observation
        assert observation["exit_code"] == -9
        assert observation["artifacts"]["ledger.json"]["sha256"]
        assert observation["process"]["stdout_eof"] and observation["process"]["stderr_eof"]
        assert job.pidfd == -1 and job.collector.done()
        assert report["summary"]["planned_cells"] == report["summary"]["skipped"] == 2
        assert report["summary"]["cell_coverage"] == 0
        async with app.repository.engine.begin() as connection:
            stored = (await connection.execute(select(trials.c.body))).scalar_one()
            observed = (await connection.execute(select(evidence.c.body))).scalars().all()
        assert stored["output"] == observation and observation in observed
        assert app.adapter.execute_calls == 1 and app.repository.metric_registry.calls == 0
        assert (await app.run(request))["id"] == view["id"]
        trial = app.repository.plan(result).trials[0]
        body = {k: result["items"][0][k] for k in ["output", "scores", "quality", "code"]}
        body["output"] = None
        await app.repository.commit(trial, body)
        assert (await app.repository.snapshot(view["id"]))["items"][0]["output"] == observation
    finally:
        release.set()
        if waiting is not None:
            await asyncio.gather(waiting, return_exceptions=True)
        await app.close()
    resources(app, tmp_path, "repair-timeout")
    async with await open_eval(database, readonly=True) as reader:
        assert (await reader.repository.by_key(request.model_dump(mode="json")))["id"] == view["id"]
        assert (await reader.repository.report(view["id"]))["items"][0]["output"] == observation
        assert reader.adapter is None and reader.repository.metric_registry.calls == 0
    assert app.adapter.execute_calls == 1


@pytest.mark.parametrize("kind", ["dataset_snapshot", "artifact_and_streams"])
async def test_pn1_repair_capacity_closes_persistence_and_cold_reads(tmp_path, kind):
    database = tmp_path / "repair-capacity.sqlite"
    app = await open_eval(database)
    try:
        if kind == "dataset_snapshot":
            source = single().model_dump(mode="json")
            cases = []
            for index in range(8):
                case = copy.deepcopy(source["cases"][0])
                case["case_id"] = f"case{index}"
                case["files"]["padding.txt"] = "x" * 65536
                cases.append(case)
            dataset = DatasetSpec(name="capacity", version="1", cases=cases)
        else:
            pn1_repair_worker(app, tmp_path, large=True)
            dataset = single()
        frozen = await app.repository.freeze(dataset)
        request = TaskSpec(
            request_key="capacity",
            dataset=frozen.ref(),
            metrics=app.repository.metric_registry.specs(),
        )
        view = await app.run(request)
        result = await app.kernel.wait(view["id"])
        assert result["status"] == "completed"
        assert all(i["quality"] == "passed" for i in result["items"])
        assert len(json.dumps(result).encode()) > 1048576
        report = await app.repository.report(view["id"])
        assert (await app.repository.by_key(request.model_dump(mode="json")))["id"] == view["id"]
        if kind == "artifact_and_streams":
            output = result["items"][0]["output"]
            assert len(output["artifacts"]["blob.txt"]["text"]) == 1048000
            assert len(output["stdout"]) >= 250000 and len(output["stderr"]) == 120000
            assert len(json.dumps(output).encode()) > 1048576
        assert app.adapter.execute_calls == (8 if kind == "dataset_snapshot" else 1)
    finally:
        await app.close()
    resources(app, tmp_path, "repair-capacity")
    async with await open_eval(database, readonly=True) as cold:
        assert await cold.repository.report(view["id"]) == report
        assert (await cold.repository.snapshot(view["id"]))["status"] == "completed"
        assert gate_reports(report, await cold.repository.report(view["id"]))["status"] == "pass"
        assert cold.repository.metric_registry.calls == 0 and cold.adapter is None


async def test_pn1_repair_oversize_rejected_before_dispatch(tmp_path):
    async with await open_eval(tmp_path / "repair-oversize.sqlite") as app:
        source = single().model_dump(mode="json")
        cases = []
        for index in range(8):
            case = copy.deepcopy(source["cases"][0])
            case["case_id"] = f"case{index}"
            case["files"].update({"padding-a.txt": "a" * 65536, "padding-b.txt": "b" * 65536})
            cases.append(case)
        with pytest.raises((ValueError, ServiceError)):
            spec = DatasetSpec(name="over", version="1", cases=cases)
            frozen = await app.repository.freeze(spec)
            await app.run(
                TaskSpec(
                    request_key="over",
                    dataset=frozen.ref(),
                    metrics=app.repository.metric_registry.specs(),
                )
            )
        assert app.adapter.execute_calls == 0


@pytest.mark.parametrize("change", ["environment", "interpreter"])
async def test_pn1_repair_execution_conditions_drift_has_zero_launch(tmp_path, monkeypatch, change):
    app = await open_eval(tmp_path / "repair-drift.sqlite")
    entered, release = asyncio.Event(), asyncio.Event()
    try:
        request = await requested(app)
        original = app.repository.set_running

        async def held(task_id):
            await original(task_id)
            entered.set()
            await release.wait()

        monkeypatch.setattr(app.repository, "set_running", held)
        view = await app.run(request)
        await asyncio.wait_for(entered.wait(), 2)
        with monkeypatch.context() as patch:
            if change == "environment":
                patch.setenv("LANG", "C")
            else:
                alias = tmp_path / "python-alias"
                alias.symlink_to(await asyncio.to_thread(Path(sys.executable).resolve))
                patch.setattr(sys, "executable", str(alias))
            release.set()
            result = await app.kernel.wait(view["id"])
        assert result["code"] == "target_binding_changed"
        assert app.adapter.execute_calls == 0 and app.adapter.jobs == {}
    finally:
        release.set()
        await app.close()
    resources(app, tmp_path, "repair-drift")


@pytest.mark.parametrize("change", ["environment", "target_version"])
async def test_pn1_repair_real_reports_conditions_are_a_gate(tmp_path, monkeypatch, change):
    from sqlalchemy import select

    from krim_agent.evaluation.repository import reports as report_table

    reports = []
    for index in range(2):
        with monkeypatch.context() as patch:
            if change == "environment" and index == 1:
                patch.setenv("LANG", "C")
            async with await open_eval(tmp_path / f"repair-compare-{index}.sqlite") as app:
                if change == "target_version" and index == 1:
                    app.adapter._descriptor["version"] = "reconcile-reference/2"
                request = await requested(app)
                view = await app.run(request)
                await app.kernel.wait(view["id"])
                report = await app.repository.report(view["id"])
                async with app.repository.engine.begin() as connection:
                    persisted = (await connection.execute(select(report_table.c.body))).scalar_one()
                assert report == persisted
                assert validate_neutral_report(report) == report
                assert app.adapter.execute_calls == 1
                reports.append(report)
    result = gate_reports(*reports)
    assert result["status"] == ("invalid" if change == "environment" else "pass")
