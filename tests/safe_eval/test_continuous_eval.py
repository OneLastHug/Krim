"""PN3真实冷读、确认和共享内核消费者；fixture不复制产品实现。"""

import asyncio
import hashlib
import json
import os
import sqlite3
import stat
import sys
import threading
import time
from contextlib import closing
from importlib.resources import files
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import event
from sqlalchemy.pool import Pool

from krim_agent.contracts import ServiceError
from krim_agent.evaluation.app import open_eval
from krim_agent.evaluation.continuous import ContinuousEvaluation
from krim_agent.evaluation.continuous_metrics import continuous_rules
from krim_agent.evaluation.continuous_types import (
    ConfirmationRequest,
    ContinuousRequest,
    FreezeConfirmed,
    ImportRequest,
    ScoreSource,
    SourceSelection,
)
from krim_agent.evaluation.domain import DatasetSpec, TaskSpec, neutral_fingerprint
from krim_agent.evaluation.repository import Repository
from krim_agent.evaluation.sources import EvalSourceReader

TEST_DIGEST = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
SOURCE_ROOT = Path(__file__).resolve().parents[2]

DATABASES = []
APPLICATIONS = []

CRASH_CODE = r"""import asyncio,hashlib,json,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from importlib.resources import files
from krim_agent.evaluation.app import open_eval
from krim_agent.evaluation.domain import DatasetSpec,TaskSpec,DatasetRef,MetricSpec
from krim_agent.evaluation.continuous_types import (
    ImportRequest,
    SourceSelection,
    ConfirmationRequest,
    FreezeConfirmed,
    ContinuousRequest,
    ScoreSource,
    ScheduleSpec,
)
from krim_agent.evaluation.continuous_metrics import continuous_rules
async def main():
    root=Path(sys.argv[2]);mode=sys.argv[3];digest=sys.argv[4]
    app=await open_eval(root/'crash.sqlite',clock=lambda:100000.0)
    def ready(identity):
        body={'task_id':identity,
            'mode':mode,
            'source_exec':app.adapter.execute_calls,
            'request':request.model_dump(mode='json'),
            'jobs':[{'pid':j.process.pid,
            'returncode':j.process.returncode,
            'pidfd':j.pidfd,
            'collector_done':j.collector.done()}
            for j in app.owner.jobs.values()]}
        temporary=root/'ready.tmp';
        temporary.write_text(json.dumps(body));
        temporary.rename(root/'ready.json')
    try:
        body=json.loads(files('krim_agent.evaluation.examples').joinpath('reconciliation.v1.json').read_text())
        data=DatasetSpec.model_validate(body|{'cases':body['cases'][:1]})
        await app.repository.freeze(data)
        seeded=await app.run(TaskSpec(request_key='crash-source',
            dataset=data.ref(),
            metrics=app.repository.metric_registry.specs()))
        await app.kernel.wait(seeded['id'])
        flow=app.continuous;flow.redaction['artifact_paths']=['ledger.json','summary.md']
        row=(await flow.import_sources(ImportRequest(request_key='crash-import',
            sources=[SourceSelection(task_id=seeded['id'],
            case_id='basic')])))[0]
        draft=await app.repository.candidate('local',row['id'])
        confirmed=await flow.confirm_case(ConfirmationRequest(request_key='crash-confirm',
            candidate_id=draft['candidate_id'],
            expected_revision=draft['revision'],
            expected_hash=draft['hash'],
            case=data.cases[0],
            context='complete',
            reason='固定合成来源及人工参考'))
        frozen=await flow.freeze_dataset(FreezeConfirmed(request_key='crash-freeze',
            name='crash-cases',
            version='1',
            confirmations=[confirmed['id']]))
        request=ContinuousRequest(request_key='crash-score',
            mode='observed_score',
            dataset=DatasetRef.model_validate(frozen['dataset_ref']),
            dataset_envelope_fp=frozen['dataset_envelope_fp'],
            metrics=continuous_rules(app.repository.metric_registry),
            observations=[ScoreSource(case_id='basic',
            repetition=0,
            observation_id=row['id'],
            observation_hash=row['content_hash'])])
        if mode=='scoring':
            async def pause(case,output,config):
                actual=await app.repository.by_key({'scope':'local',
                    'request_key':request.request_key})
                ready(actual['id'])
                await asyncio.Event().wait()
                return 'scored',1,None
            metric=MetricSpec(name='bounded_crash',version='1',implementation_digest=digest)
            app.repository.metric_registry.register(metric,pause)
            request=ContinuousRequest.model_validate(request.model_dump(mode='json')|{'metrics':[m.model_dump(mode='json')
                for m in request.metrics+[metric]]})
            view=await flow.submit(request)
            await app.kernel.wait(view['id'])
        else:
            request=ContinuousRequest.model_validate(request.model_dump(mode='json')|{'mode':'execute',
                'observations':[],
                'request_key':'window-template'})
            await flow.create_schedule(ScheduleSpec(schedule_id='bounded',
                version='1',
                template=request,
                epoch=100000,
                interval_seconds=60,
                max_windows=3))
            original=app.repository._begin_continuous
            async def held(value):
                result=await original(value)
                ready(result[0]['id'])
                await asyncio.Event().wait()
                return result
            app.repository._begin_continuous=held
            await flow.tick('bounded')
    finally:
        await app.close()
asyncio.run(main())
"""


@pytest_asyncio.fixture(autouse=True)
async def continuous_resources(request, tmp_path, monkeypatch, recwarn):
    DATABASES.clear()
    APPLICATIONS.clear()
    connections, listeners, unexpected = [], [], []
    before = asyncio.all_tasks()
    fd_before = len(os.listdir("/proc/self/fd"))
    threads_before = [(t.ident, t.name) for t in threading.enumerate()]
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda current, context: unexpected.append(str(context)))
    for kind in ["connect", "checkout", "reset", "checkin", "close"]:

        def record(*args, kind=kind):
            connections.append({"event": kind, "connection": id(args[0]), "at": time.monotonic()})

        event.listen(Pool, kind, record)
        listeners.append((kind, record))
    original = Repository.open

    async def opened(cls, path, **kwargs):
        DATABASES.append(Path(path))
        return await original(path, **kwargs)

    monkeypatch.setattr(Repository, "open", classmethod(opened))
    yield
    await asyncio.sleep(0)
    pending = [
        t
        for t in asyncio.all_tasks()
        if t is not asyncio.current_task() and t not in before and not t.done()
    ]
    records = []
    for index, database in enumerate(dict.fromkeys(DATABASES)):
        info = {"path": str(database)}
        if database.exists():
            st = database.lstat()
            assert stat.S_ISREG(st.st_mode) and st.st_nlink == 1
            backup = tmp_path / f"continuous-backup-{index}.sqlite"
            with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as src:
                with closing(sqlite3.connect(backup)) as dst:
                    src.backup(dst)
            info.update(backup=str(backup), sha256=hashlib.sha256(backup.read_bytes()).hexdigest())
        records.append(info)
    jobs = []
    for app in APPLICATIONS:
        if app.owner is not None:
            for work in app.owner.jobs.values():
                jobs.append(
                    {
                        "pid": work.process.pid,
                        "returncode": work.process.returncode,
                        "pidfd": work.pidfd,
                        "collector_joined": work.collector is not None and work.collector.done(),
                        "stdout_eof": work.stdout_eof,
                        "stderr_eof": work.stderr_eof,
                        "streams_joined": all(t.done() for t in work.stream_tasks),
                        "owner_closed": app.owner.closed,
                        "owned_lease": work.lease is app.adapter._lease,
                        "cleanup_receipts": work.cleanup_receipts,
                    }
                )
    receipt = {
        "node": request.node.nodeid,
        "uid": os.getuid(),
        "databases": records,
        "connections": connections,
        "fd_before": fd_before,
        "fd_after": len(os.listdir("/proc/self/fd")),
        "pending": [repr(t) for t in pending],
        "unexpected": unexpected,
        "jobs": jobs,
        "warnings": [str(w.message) for w in recwarn],
        "consumer": __file__,
        "threads_before": threads_before,
        "threads_after": [(t.ident, t.name) for t in threading.enumerate()],
        "borrowed_executor_threads_not_claimed_zero": True,
        "repositories": [
            {
                "closed": a.repository.closed,
                "writer_fd": a.repository._writer_fd,
                "events": a.repository.resources,
            }
            for a in APPLICATIONS
        ],
    }
    (tmp_path / "continuous-resources.json").write_text(json.dumps(receipt, indent=2))
    loop.set_exception_handler(previous)
    for kind, listener in listeners:
        event.remove(Pool, kind, listener)
    assert os.getuid() == 1001
    assert not pending and not unexpected and not recwarn
    counts = [r["event"] for r in connections]
    assert counts.count("connect") == counts.count("close")
    assert counts.count("checkout") == counts.count("reset") == counts.count("checkin")
    states = {}
    for row in connections:
        identity, kind = row["connection"], row["event"]
        state = states.get(identity, "closed")
        if kind == "connect":
            assert state == "closed", (state, row)
            states[identity] = "idle"
        elif kind == "checkout":
            assert state == "idle", (state, row)
            states[identity] = "out"
        elif kind == "reset":
            assert state == "out", (state, row)
            states[identity] = "reset"
        elif kind == "checkin":
            assert state == "reset", (state, row)
            states[identity] = "idle"
        else:
            assert kind == "close" and state == "idle", (state, row)
            states[identity] = "closed"
    assert all(v == "closed" for v in states.values()), states
    assert receipt["fd_after"] <= fd_before, receipt
    assert all(
        j["stdout_eof"]
        and j["stderr_eof"]
        and j["streams_joined"]
        and j["owner_closed"]
        and j["owned_lease"]
        for j in jobs
    )
    assert all(
        j["returncode"] is not None and j["pidfd"] == -1 and j["collector_joined"] for j in jobs
    )
    assert all(
        a.repository.closed and all(t.done() for t in a.kernel.tasks.values()) for a in APPLICATIONS
    )


async def seed(tmp_path):
    app = await open_eval(tmp_path / "eval.sqlite")
    APPLICATIONS.append(app)
    body = json.loads(
        files("krim_agent.evaluation.examples").joinpath("reconciliation.v1.json").read_text()
    )
    dataset = DatasetSpec.model_validate(body | {"cases": body["cases"][:1]})
    await app.repository.freeze(dataset)
    request = TaskSpec(
        dataset=dataset.ref(),
        request_key="source-01",
        metrics=app.repository.metric_registry.specs(),
    )
    view = await app.run(request)
    snapshot = await app.kernel.wait(view["id"])
    assert snapshot["status"] == "completed" and snapshot["items"][0]["quality"] == "passed"
    selection = SourceSelection(task_id=view["id"], case_id=dataset.cases[0].case_id)
    reader = EvalSourceReader(app.repository)
    pipeline = ContinuousEvaluation(
        app.repository, app.kernel, actor="synthetic-reviewer", sources={"current-eval": reader}
    )
    return app, dataset, selection, pipeline


@pytest.mark.parametrize(
    "kind", ["terminal", "incomplete", "context", "wrong_scope", "redaction", "sample", "dedup"]
)
async def test_observation_import_scope_and_eligibility(tmp_path, monkeypatch, kind):
    app, dataset, selection, pipeline = await seed(tmp_path)
    try:
        request = ImportRequest(request_key="import-01", sources=[selection])
        if kind == "wrong_scope":
            with pytest.raises(ServiceError, match="not_found"):
                await pipeline.import_sources(request.model_copy(update={"scope": "other"}))
            assert app.adapter.execute_calls == 1
            return
        original = pipeline.sources["current-eval"].read
        if kind in {"incomplete", "context", "redaction"}:

            async def changed(*args):
                value = await original(*args)
                if kind == "incomplete":
                    value["terminal"] = False
                elif kind == "context":
                    value["case"] = None
                else:
                    assert value["output"]["stdout"] == "reconciliation_complete\n"
                return value

            monkeypatch.setattr(pipeline.sources["current-eval"], "read", changed)
        if kind == "sample":
            pipeline.sampling = {
                "version": "explicit-id-hash/1",
                "salt_id": "fixture",
                "numerator": 0,
                "denominator": 1,
            }
            assert await pipeline.import_sources(request) == []
            assert app.adapter.execute_calls == 1
            return
        rows = await pipeline.import_sources(request)
        assert len(rows) == 1
        row = await app.repository.observation("local", rows[0]["id"])
        assert row["source"]["task_id"] == selection.task_id
        assert (
            row["source"]["execution_ref"]
            == (await app.repository.snapshot(selection.task_id))["items"][0]["execution_ref"]
        )
        assert row["source"]["evidence_hash"] == neutral_fingerprint(
            (await original("local", selection.model_dump(mode="json")))["output"]
        )
        assert row["eligibility"] == (
            "ineligible"
            if kind == "incomplete"
            else "needs_context"
            if kind == "context"
            else "eligible"
        )
        assert row["output"]["stdout"] is None and row["output"]["stderr"] is None
        assert (await pipeline.import_sources(request))[0]["id"] == row["id"]
        if kind == "dedup":
            other = request.model_copy(update={"request_key": "import-02"})
            assert (await pipeline.import_sources(other))[0]["id"] == row["id"]
        assert app.adapter.execute_calls == 1
        assert Path(
            __import__("krim_agent.evaluation.continuous", fromlist=["x"]).__file__
        ).is_relative_to(Path(os.environ["KRIM_PN3_ROOT"]) / "source/src")
    finally:
        await app.close()


class CallCounts:
    """Python调用观察保留真实函数和进程；计数仅限指定评分区间。"""

    def __init__(self, app):
        self.codes = {}
        for name in ["prepare", "execute", "collect", "cancel"]:
            self.codes[getattr(app.adapter, name).__func__.__code__] = "adapter_" + name
        self.codes[Repository.adapter.__code__] = "adapter_lookup"
        self.codes[asyncio.create_subprocess_exec.__code__] = "subprocess"
        self.counts = {name: 0 for name in self.codes.values()}
        self.counts["provider_stream"] = 0

    def __enter__(self):
        self.previous = sys.getprofile()

        def observe(frame, event, value):
            if event != "call":
                return
            name = self.codes.get(frame.f_code)
            if name is not None:
                self.counts[name] += 1
            if (
                frame.f_globals.get("__name__", "").startswith("krim_agent.providers.")
                and frame.f_code.co_name == "stream"
            ):
                self.counts["provider_stream"] += 1

        sys.setprofile(observe)
        return self

    def __exit__(self, *args):
        sys.setprofile(self.previous)


async def confirmed_task(tmp_path, *, bad_reference=False):
    app, dataset, selection, pipeline = await seed(tmp_path)
    pipeline.redaction["artifact_paths"] = ["ledger.json", "summary.md"]
    rows = await pipeline.import_sources(
        ImportRequest(request_key="scoring-source", sources=[selection])
    )
    draft = await app.repository.candidate("local", rows[0]["id"])
    case = dataset.cases[0].model_copy(deep=True)
    if bad_reference:
        case.reference["files"]["ledger.json"]["total_cents"] = -999
    confirmed = await pipeline.confirm_case(
        ConfirmationRequest(
            request_key="score-confirm",
            candidate_id=draft["candidate_id"],
            expected_revision=draft["revision"],
            expected_hash=draft["hash"],
            case=case,
            context="complete",
            reason="人工固定输入、初态和结果，不采用目标自报完成。",
        )
    )
    frozen = await pipeline.freeze_dataset(
        FreezeConfirmed(
            request_key="score-freeze",
            name="score-cases",
            version="1",
            confirmations=[confirmed["id"]],
        )
    )
    from krim_agent.evaluation.domain import DatasetRef

    request = ContinuousRequest(
        request_key="score-request",
        mode="observed_score",
        dataset=DatasetRef.model_validate(frozen["dataset_ref"]),
        dataset_envelope_fp=frozen["dataset_envelope_fp"],
        metrics=continuous_rules(app.repository.metric_registry),
        observations=[
            ScoreSource(
                case_id=case.case_id,
                repetition=0,
                observation_id=rows[0]["id"],
                observation_hash=rows[0]["content_hash"],
            )
        ],
    )
    return app, pipeline, request, rows[0]


@pytest.mark.parametrize(
    "kind", ["rescore", "observed_score", "missing", "known_failure", "cancel", "return_lost"]
)
async def test_score_modes_preserve_evidence_zero_execution(tmp_path, monkeypatch, kind):
    app, pipeline, request, observation = await confirmed_task(
        tmp_path, bad_reference=kind == "known_failure"
    )
    try:
        original_report = await app.repository.report(observation["source"]["task_id"])
        if kind in {"rescore", "known_failure"}:
            source_request = request.model_copy(
                update={"mode": "execute", "observations": [], "request_key": "execute-source"}
            )
            view = await pipeline.submit(source_request)
            await app.kernel.wait(view["id"])
            parent = await app.repository.report(view["id"])
            request = request.model_copy(
                update={
                    "mode": "rescore",
                    "observations": [],
                    "parent_task_id": view["id"],
                    "parent_report_hash": parent["report_hash"],
                }
            )
            if kind == "known_failure":
                assert parent["items"][0]["quality"] == "failed"
        if kind == "missing":
            original_set = app.repository.set_running
            descriptor = json.loads(json.dumps(app.adapter._descriptor))

            async def drift_after_admission(identity):
                await original_set(identity)
                app.adapter._descriptor["execution_conditions"]["env"]["LANG"] = "controlled-drift"

            monkeypatch.setattr(app.repository, "set_running", drift_after_admission)
            failed = await app.run(
                TaskSpec(
                    request_key="actual-missing",
                    dataset=request.dataset,
                    metrics=app.repository.metric_registry.specs(),
                )
            )
            await app.kernel.wait(failed["id"])
            app.adapter._descriptor = descriptor
            monkeypatch.setattr(app.repository, "set_running", original_set)
            missing = await pipeline.import_sources(
                ImportRequest(
                    request_key="missing-source",
                    sources=[SourceSelection(task_id=failed["id"], case_id="basic")],
                )
            )
            assert missing[0]["output"] is None and missing[0]["eligibility"] == "needs_context"
            request = request.model_copy(
                update={
                    "observations": [
                        ScoreSource(
                            case_id="basic",
                            repetition=0,
                            observation_id=missing[0]["id"],
                            observation_hash=missing[0]["content_hash"],
                        )
                    ]
                }
            )
        entered, joined = asyncio.Event(), asyncio.Event()
        if kind == "cancel":
            from krim_agent.evaluation.domain import MetricSpec

            async def pause(case, output, config):
                entered.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    joined.set()

            metric = MetricSpec(
                name="bounded_pause", version="1", implementation_digest=TEST_DIGEST
            )
            app.repository.metric_registry.register(metric, pause)
            request = request.model_copy(update={"metrics": request.metrics + [metric]})
        lost = []
        if kind == "return_lost":
            original_write = app.repository._write

            async def committed_then_lost(operation):
                value = await original_write(operation)
                if not lost:
                    lost.append(True)
                    raise OSError("scoring_commit_return_lost")
                return value

            monkeypatch.setattr(app.repository, "_write", committed_then_lost)
        request = ContinuousRequest.model_validate(request.model_dump(mode="json"))
        before_exec, before_spawn = app.adapter.execute_calls, app.owner.spawn_calls
        with CallCounts(app) as calls:
            view = await pipeline.submit(request)
            if kind == "cancel":
                await asyncio.wait_for(entered.wait(), 2)
                result = await app.kernel.cancel(view["id"])
                assert result["status"] == "aborted" and joined.is_set()
                assert (await app.kernel.cancel(view["id"]))["id"] == view["id"]
            else:
                result = await app.kernel.wait(view["id"])
            assert app.adapter.execute_calls == before_exec
            assert app.owner.spawn_calls == before_spawn
            assert all(n == 0 for n in calls.counts.values()), calls.counts
            assert (await pipeline.submit(request))["id"] == view["id"]
        report = await app.repository.report(view["id"])
        assert report["kind"] == request.mode
        assert report["request_hash"] == neutral_fingerprint(request.model_dump(mode="json"))
        if kind not in {"missing", "cancel", "known_failure"}:
            assert result["status"] == "completed" and report["items"][0]["quality"] == "passed"
        if kind == "missing":
            assert (
                report["summary"]["cell_coverage"] == 0
                and report["items"][0]["quality"] == "unscored"
            )
        if kind == "known_failure":
            assert report["items"][0]["quality"] == "failed"
        assert (await app.repository.report(observation["source"]["task_id"])) == original_report
        assert (await app.repository.observation("local", observation["id"]))[
            "content_hash"
        ] == observation["content_hash"]
        assert report["summary"]["token_usage"] is None
        if kind == "return_lost":
            assert len(lost) == 1
        (tmp_path / "zero-execution.json").write_text(
            json.dumps(
                {
                    "node": kind,
                    "counts": calls.counts,
                    "before_exec": before_exec,
                    "after_exec": app.adapter.execute_calls,
                    "source_report_hash": original_report["report_hash"],
                },
                indent=2,
            )
        )
    finally:
        await app.close()


@pytest.mark.parametrize(
    "kind", ["confirmed", "edited", "reconstructed", "unconfirmed", "revision_conflict"]
)
async def test_candidate_confirmation_freeze_and_execute(tmp_path, kind):
    app, dataset, selection, pipeline = await seed(tmp_path)
    try:
        if kind == "reconstructed":
            pipeline.sources["current-eval"] = EvalSourceReader(
                app.repository, include_context=False
            )
        rows = await pipeline.import_sources(
            ImportRequest(request_key="candidate-source", sources=[selection])
        )
        if kind == "reconstructed":
            assert rows[0]["eligibility"] == "needs_context"
        draft = await app.repository.candidate("local", rows[0]["id"])
        assert draft["status"] == "pending"
        freeze = FreezeConfirmed(
            request_key="freeze-01",
            name="confirmed",
            version="1",
            confirmations=[draft["candidate_id"]],
        )
        if kind == "unconfirmed":
            with pytest.raises(ServiceError, match="not_found"):
                await pipeline.freeze_dataset(freeze)
            assert app.adapter.execute_calls == 1
            assert (await app.repository.candidate("local", draft["candidate_id"]))[
                "status"
            ] == "pending"
            return
        case = dataset.cases[0]
        if kind == "edited":
            case = case.model_copy(
                update={"instruction": case.instruction + " Retain the rejection rules."}
            )
        confirmation_request = ConfirmationRequest(
            request_key="confirm-01",
            candidate_id=draft["candidate_id"],
            expected_revision=draft["revision"],
            expected_hash=draft["hash"],
            case=case,
            context="manual_reconstructed" if kind == "reconstructed" else "complete",
            reason="人工检查了输入CSV、明确结果和独立初态。",
        )
        if kind == "edited":
            edited = await pipeline.confirm_case(
                confirmation_request.model_copy(
                    update={"action": "edit", "request_key": "edit-first"}
                )
            )
            assert edited["status"] == "pending"
            updated_draft = await app.repository.candidate("local", draft["candidate_id"])
            assert updated_draft["revision"] == draft["revision"] + 1
            confirmation_request = confirmation_request.model_copy(
                update={
                    "expected_revision": updated_draft["revision"],
                    "expected_hash": updated_draft["hash"],
                }
            )
        confirmed = await pipeline.confirm_case(confirmation_request)
        assert confirmed["status"] == "confirmed"
        assert confirmed["actor"] == "synthetic-reviewer"
        assert confirmed["case"] == case.model_dump(mode="json")
        assert (await pipeline.confirm_case(confirmation_request))["id"] == confirmed["id"]
        frozen = await pipeline.freeze_dataset(
            freeze.model_copy(update={"confirmations": [confirmed["id"]]})
        )
        assert frozen["lineage"]["cases"][0]["confirmation_hash"] == confirmed["content_hash"]
        assert frozen["dataset_envelope_fp"] == neutral_fingerprint(
            {"dataset_ref": frozen["dataset_ref"], "lineage": frozen["lineage"]}
        )
        from krim_agent.evaluation.domain import DatasetRef

        request = TaskSpec(
            dataset=DatasetRef.model_validate(frozen["dataset_ref"]),
            request_key="confirmed-execute",
            metrics=app.repository.metric_registry.specs(),
        )
        view = await app.run(request)
        result = await app.kernel.wait(view["id"])
        assert view["id"] != selection.task_id
        assert result["status"] == "completed" and result["items"][0]["quality"] == "passed"
        assert app.adapter.execute_calls == 2
        assert (
            result["items"][0]["execution_ref"]
            != (await app.repository.snapshot(selection.task_id))["items"][0]["execution_ref"]
        )
        if kind == "revision_conflict":
            with pytest.raises(ServiceError, match="key_conflict"):
                await pipeline.confirm_case(
                    confirmation_request.model_copy(update={"reason": "changed"})
                )
            with pytest.raises(ServiceError, match="snapshot_changed") as conflict:
                await pipeline.confirm_case(
                    confirmation_request.model_copy(
                        update={"request_key": "changed", "expected_hash": "0" * 64}
                    )
                )
            assert conflict.value.code == "snapshot_changed"
            assert (await app.repository.candidate("local", draft["candidate_id"]))["revision"] == 1
        if kind == "edited":
            from krim_agent.evaluation.continuous_types import Feedback

            feedback_request = ImportRequest(
                request_key="feedback-import",
                sources=[
                    selection.model_copy(
                        update={
                            "feedback": Feedback(
                                key="feedback-one", reason="人工反馈只产生候选，不自动成为参考。"
                            )
                        }
                    )
                ],
            )
            feedback = (await pipeline.import_sources(feedback_request))[0]
            assert (
                feedback["id"] == rows[0]["id"]
                and feedback["candidate_id"] != draft["candidate_id"]
            )
            fdraft = await app.repository.candidate("local", feedback["candidate_id"])
            rejected = await pipeline.confirm_case(
                confirmation_request.model_copy(
                    update={
                        "action": "reject",
                        "request_key": "reject-feedback",
                        "candidate_id": fdraft["candidate_id"],
                        "expected_revision": fdraft["revision"],
                        "expected_hash": fdraft["hash"],
                    }
                )
            )
            assert rejected["status"] == "rejected"
            with pytest.raises(ServiceError, match="source_not_eligible"):
                await pipeline.freeze_dataset(
                    FreezeConfirmed(
                        request_key="reject-freeze",
                        name="rejected",
                        version="1",
                        confirmations=[rejected["id"]],
                    )
                )
            assert (await pipeline.import_sources(feedback_request))[0]["candidate_id"] == feedback[
                "candidate_id"
            ]
        assert (await app.repository.observation("local", rows[0]["id"]))["content_hash"] == rows[
            0
        ]["content_hash"]
    finally:
        await app.close()


@pytest.mark.parametrize(
    "kind", ["version_axis", "conditions", "missing", "hard_failure", "mixed_modes", "seed"]
)
async def test_case_gate_subject_conditions_and_denominators(tmp_path, monkeypatch, kind):
    from krim_agent.evaluation.continuous_reports import compare
    from krim_agent.evaluation.continuous_types import ComparisonPolicy

    app, pipeline, observed, source = await confirmed_task(tmp_path)
    try:
        request = observed.model_copy(
            update={"mode": "execute", "observations": [], "request_key": "gate-before"}
        )
        first = await pipeline.submit(request)
        await app.kernel.wait(first["id"])
        before = await app.repository.report(first["id"])
        assert before["items"][0]["quality"] == "passed"
        if kind in {"version_axis", "hard_failure"}:
            text = app.adapter.worker.read_text()
            if kind == "hard_failure":
                text = text.replace('"total_cents": total,', '"total_cents": total + 1,')
            worker = tmp_path / "actual-candidate-worker.py"
            worker.write_text(text + "\n# independent candidate byte revision\n")
            app.adapter.worker = worker
            app.adapter._descriptor["implementation_digest"] = hashlib.sha256(
                worker.read_bytes()
            ).hexdigest()
            app.adapter._descriptor["version"] = "reconcile-candidate/2"
        if kind == "conditions":
            await app.close()
            monkeypatch.setenv("LANG", "C")
            app = await open_eval(tmp_path / "eval.sqlite")
            APPLICATIONS.append(app)
            pipeline = app.continuous
        if kind == "missing":
            from krim_agent.evaluation.domain import MetricSpec

            async def forbidden(case, output, config):
                raise AssertionError("missing evidence must not invoke rule")

            metric = MetricSpec(
                name="native_evidence_needed",
                version="1",
                implementation_digest=TEST_DIGEST,
                required_evidence=["native"],
            )
            app.repository.metric_registry.register(metric, forbidden)
            request = request.model_copy(update={"metrics": request.metrics + [metric]})
        if kind == "mixed_modes":
            request = observed
        request = request.model_copy(update={"request_key": "gate-after"})
        request = ContinuousRequest.model_validate(request.model_dump(mode="json"))
        second = await pipeline.submit(request)
        await app.kernel.wait(second["id"])
        after = await app.repository.report(second["id"])
        subjects = list(
            dict.fromkeys([before["subject_fingerprint"], after["subject_fingerprint"]])
        )
        policy = ComparisonPolicy(
            baseline_id=first["id"],
            candidate_id=second["id"],
            baseline_hash=before["report_hash"],
            candidate_hash=after["report_hash"],
            allowed_subjects=subjects,
        )
        result = compare(policy, before, after)
        if kind == "version_axis":
            assert before["subject_fingerprint"] != after["subject_fingerprint"]
            assert before["conditions_fingerprint"] == after["conditions_fingerprint"]
            assert result["status"] == "pass"
            denied = policy.model_copy(update={"allowed_subjects": [before["subject_fingerprint"]]})
            assert compare(denied, before, after)["reasons"][0]["code"] == "unapproved_subject"
        elif kind == "hard_failure":
            assert after["items"][0]["quality"] == "failed"
            assert result["status"] == "fail"
            assert any(
                r["code"] == "passed_to_nonpassed" and r["case_id"] == "basic"
                for r in result["reasons"]
            )
        elif kind == "conditions":
            assert before["conditions_fingerprint"] != after["conditions_fingerprint"]
            assert (
                result["status"] == "invalid"
                and result["reasons"][0]["code"] == "incompatible_conditions_fingerprint"
            )
        elif kind == "missing":
            assert after["summary"]["planned_cells"] == 5 and after["summary"]["scored_cells"] == 4
            assert (
                result["status"] == "invalid"
                and result["reasons"][0]["code"] == "incomplete_coverage"
            )
        elif kind == "mixed_modes":
            assert (
                result["status"] == "invalid"
                and result["reasons"][0]["code"] == "incompatible_kind"
            )
        else:
            from pydantic import ValidationError

            before_count = app.adapter.execute_calls
            with pytest.raises(ValidationError):
                await pipeline.submit(
                    request.model_copy(
                        update={"request_key": "unsupported-seed", "seed_policy": "invented-seed"}
                    )
                )
            assert app.adapter.execute_calls == before_count
            assert before["seed_policy"] == after["seed_policy"] == "unseeded/1"
            assert result["status"] == "pass"
        (tmp_path / "comparison.json").write_text(
            json.dumps(
                {
                    "before": before["report_hash"],
                    "after": after["report_hash"],
                    "policy": policy.model_dump(mode="json"),
                    "gate": result,
                },
                indent=2,
            )
        )
    finally:
        await app.close()


@pytest.mark.parametrize("kind", ["readonly", "score_writer"])
async def test_continuous_dependency_and_cold_read(tmp_path, monkeypatch, kind):
    app, pipeline, request, observation = await confirmed_task(tmp_path)
    database = app.repository.path
    try:
        if kind == "score_writer":
            from krim_agent.composition import open_application
            from krim_agent.contracts import RunRequest
            from krim_agent.evaluation.adapters.krim_source import KrimSourceReader
            from krim_agent.security import LOCAL, Membership, PrincipalContext

            source_database = tmp_path / "actual-krim.sqlite"
            DATABASES.append(source_database)
            host = await open_application(source_database)
            try:
                run = await host.service.start(
                    RunRequest(text="/add 2 3", request_key="cold-krim-source", target="target-v2")
                )
                await host.service.wait(run.id)
                facts = await host.service.result(run.id)
                assert facts.text == "5" and facts.tools[0].executor_started is True
                pipeline.sources["krim-source"] = KrimSourceReader(
                    host.store, host.sessions, LOCAL, allow_legacy=True
                )
                imported = await pipeline.import_sources(
                    ImportRequest(
                        request_key="krim-import",
                        source_alias="krim-source",
                        sources=[SourceSelection(run_id=run.id)],
                    )
                )
                assert imported[0]["source"]["run_id"] == run.id
                assert imported[0]["source"]["terminal_event_id"] is not None
                assert imported[0]["output"]["cleanup"] == "unknown"
                assert imported[0]["eligibility"] == "needs_context"
                assert "text" not in imported[0]["output"]["target_result"]
                outsider = PrincipalContext(
                    "outsider", 1, (Membership(workspace_id="other"),), True
                )
                denied = KrimSourceReader(host.store, host.sessions, outsider, allow_legacy=True)
                with pytest.raises(ServiceError, match="not_found"):
                    await denied.read(
                        "local", SourceSelection(run_id=run.id).model_dump(mode="json")
                    )
                provider_calls = host.service.bindings["target-v2"].loop.provider.calls
                assert provider_calls == 2
                assert (
                    await pipeline.import_sources(
                        ImportRequest(
                            request_key="krim-import",
                            source_alias="krim-source",
                            sources=[SourceSelection(run_id=run.id)],
                        )
                    )
                )[0]["id"] == imported[0]["id"]
                assert host.service.bindings["target-v2"].loop.provider.calls == provider_calls
            finally:
                await host.close()
            assert host.store.closed and all(t.done() for t in host.service.tasks.values())
        view = await pipeline.submit(request)
        await app.kernel.wait(view["id"])
        report = await app.repository.report(view["id"])
    finally:
        await app.close()
    frozen = database.read_bytes()
    from krim_agent.evaluation import app as module

    def forbidden(*args, **kwargs):
        raise AssertionError("pure scoring/read must not assemble target or owner")

    monkeypatch.setattr(module, "ProcessOwner", forbidden)
    monkeypatch.setattr(module, "LocalProcessAdapter", forbidden)
    cold = await open_eval(database, readonly=kind == "readonly", execution_enabled=False)
    APPLICATIONS.append(cold)
    try:
        assert cold.owner is None and cold.adapter is None and cold.leases == []
        assert await cold.repository.report(view["id"]) == report
        assert (await cold.continuous.submit(request))["id"] == view["id"]
        if kind == "score_writer":
            again = await cold.continuous.submit(
                request.model_copy(update={"request_key": "cold-new-score"})
            )
            await cold.kernel.wait(again["id"])
            assert (await cold.repository.report(again["id"]))["items"][0]["quality"] == "passed"
        else:
            with pytest.raises(ServiceError, match="readonly"):
                await cold.continuous.submit(
                    request.model_copy(update={"request_key": "readonly-new"})
                )
    finally:
        await cold.close()
    if kind == "readonly":
        assert database.read_bytes() == frozen
    writer = await Repository.open(database)
    await writer.close()
    if kind == "score_writer":
        from test_continuous_package import run_cli_flow

        actual = run_cli_flow(tmp_path / "source-cli", installed=False)
        assert actual["target_spawns"] == 2


@pytest.mark.parametrize("kind", ["max_24", "capacity", "deadline", "repeat_cancel", "close"])
async def test_continuous_limits_and_cancellation(tmp_path, kind):
    from krim_agent.evaluation.domain import Budget, CaseSpec, DatasetRef, MetricSpec

    app, pipeline, request, source = await confirmed_task(tmp_path)
    release, entered, joined = asyncio.Event(), asyncio.Event(), asyncio.Event()
    try:
        if kind == "max_24":
            from sqlalchemy import select

            from krim_agent.evaluation.repository import dataset_lineage

            async with app.repository.engine.begin() as conn:
                frozen = (await conn.execute(select(dataset_lineage.c.body))).scalars().one()
            ids = [frozen["lineage"]["cases"][0]["confirmation_id"]]
            base = (
                await app.repository.dataset(request.dataset.name, request.dataset.version)
            ).cases[0]
            for index in range(1, 8):
                pipeline.sampling["salt_id"] = "capacity-" + str(index)
                row = (
                    await pipeline.import_sources(
                        ImportRequest(
                            request_key="capacity-import-" + str(index),
                            sources=[
                                SourceSelection(
                                    task_id=source["source"]["task_id"], case_id="basic"
                                )
                            ],
                        )
                    )
                )[0]
                draft = await app.repository.candidate("local", row["id"])
                case = CaseSpec.model_validate(
                    base.model_dump(mode="json")
                    | {
                        "case_id": "case-" + str(index),
                        "instruction": base.instruction + " Capacity case " + str(index),
                    }
                )
                confirmed = await pipeline.confirm_case(
                    ConfirmationRequest(
                        request_key="capacity-confirm-" + str(index),
                        candidate_id=draft["candidate_id"],
                        expected_revision=draft["revision"],
                        expected_hash=draft["hash"],
                        case=case,
                        context="complete",
                        reason="人工固定容量用例，不声明样本多样性。",
                    )
                )
                ids.append(confirmed["id"])
            large = await pipeline.freeze_dataset(
                FreezeConfirmed(
                    request_key="capacity-freeze", name="capacity", version="1", confirmations=ids
                )
            )
            request = request.model_copy(
                update={
                    "mode": "execute",
                    "observations": [],
                    "dataset": DatasetRef.model_validate(large["dataset_ref"]),
                    "dataset_envelope_fp": large["dataset_envelope_fp"],
                    "execution": Budget(repetitions=3),
                }
            )
            view = await pipeline.submit(request)
            result = await app.kernel.wait(view["id"])
            report = await app.repository.report(view["id"])
            assert result["status"] == "completed" and result["planned_count"] == 24
            assert (
                report["summary"]["planned_cells"] == 96 and report["summary"]["cell_coverage"] == 1
            )
            assert all(i["quality"] == "passed" for i in result["items"])
            assert len({i["execution_ref"] for i in result["items"]}) == 24
            assert app.adapter.execute_calls == 25
            assert (await pipeline.submit(request))["id"] == view["id"]
            return

        async def pause(case, output, config):
            entered.set()
            try:
                with pytest.raises(ServiceError, match="self_wait"):
                    await app.kernel.close()
                assert app.kernel._close_task is None
                await release.wait()
                return "scored", 1, None
            finally:
                joined.set()

        metric = MetricSpec(name="bounded_limit", version="1", implementation_digest=TEST_DIGEST)
        app.repository.metric_registry.register(metric, pause)
        request = request.model_copy(update={"metrics": request.metrics + [metric]})
        if kind == "deadline":
            request = request.model_copy(update={"execution": Budget(batch_timeout_seconds=1)})
        view = await pipeline.submit(request)
        await asyncio.wait_for(entered.wait(), 2)
        if kind == "capacity":
            with pytest.raises(ServiceError, match="capacity"):
                await pipeline.submit(request.model_copy(update={"request_key": "another-task"}))
            assert (await pipeline.submit(request))["id"] == view["id"]
            release.set()
            result = await app.kernel.wait(view["id"])
            assert result["status"] == "completed"
        elif kind == "deadline":
            result = await app.kernel.wait(view["id"])
            assert result["status"] == "failed" and result["code"] == "run_timeout"
        elif kind == "repeat_cancel":
            a = asyncio.create_task(app.kernel.cancel(view["id"]))
            b = asyncio.create_task(app.kernel.cancel(view["id"]))
            await asyncio.sleep(0)
            a.cancel()
            a.cancel()
            await asyncio.gather(a, b, return_exceptions=True)
            result = await app.kernel.wait(view["id"])
            assert result["status"] == "aborted"
        else:
            a = asyncio.create_task(app.close())
            b = asyncio.create_task(app.close())
            await asyncio.gather(a, b)
            reader = await Repository.open(app.repository.path, readonly=True)
            try:
                result = await reader.snapshot(view["id"])
                assert result["status"] == "aborted"
            finally:
                await reader.close()
        assert joined.is_set() and app.adapter.execute_calls == 1
    finally:
        release.set()
        await app.close()


async def crash_process(tmp_path, mode):
    import signal
    import subprocess

    root = SOURCE_ROOT
    code_hash = hashlib.sha256(CRASH_CODE.encode()).hexdigest()
    compile(CRASH_CODE, "current-product-crash", "exec")
    argv = [
        sys.executable,
        "-I",
        "-B",
        "-c",
        CRASH_CODE,
        str(root / "src"),
        str(tmp_path),
        mode,
        code_hash,
    ]
    env = {
        k: os.environ[k]
        for k in ["PATH", "LANG", "LD_LIBRARY_PATH", "TMPDIR", "HOME", "PYTHONDONTWRITEBYTECODE"]
    }
    child = await asyncio.to_thread(
        subprocess.Popen,
        argv,
        cwd=tmp_path,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    pidfd = os.pidfd_open(child.pid)
    marker = tmp_path / "ready.json"
    DATABASES.append(tmp_path / "crash.sqlite")
    try:
        async with asyncio.timeout(10):
            while not marker.exists():
                assert child.poll() is None
                await asyncio.sleep(0.005)
        st = marker.lstat()
        assert stat.S_ISREG(st.st_mode) and st.st_nlink == 1
        receipt = json.loads(marker.read_text())
        assert receipt["source_exec"] == 1
        assert all(
            j["returncode"] is not None and j["pidfd"] == -1 and j["collector_done"]
            for j in receipt["jobs"]
        )
        signal.pidfd_send_signal(pidfd, signal.SIGKILL)
        stdout, stderr = await asyncio.to_thread(child.communicate, timeout=5)
        assert child.returncode == -signal.SIGKILL
    finally:
        if child.poll() is None:
            signal.pidfd_send_signal(pidfd, signal.SIGKILL)
            child.wait(timeout=5)
        os.close(pidfd)
        if child.stdout is not None:
            child.stdout.close()
        if child.stderr is not None:
            child.stderr.close()
    (tmp_path / "crash-stdout.txt").write_bytes(stdout)
    (tmp_path / "crash-stderr.txt").write_bytes(stderr)
    (tmp_path / "crash-command.json").write_text(
        json.dumps(
            {
                "argv": argv,
                "cwd": str(tmp_path),
                "env": env,
                "code_hash": code_hash,
                "pid": child.pid,
                "pidfd_closed": True,
                "wait_returncode": child.returncode,
                "internal_finally": "unknown_after_SIGKILL",
                "source": str(root / "src"),
            },
            indent=2,
        )
    )
    # A fresh fixture database, after an already-committed transaction; never an
    # old failure ledger. Preserve a readonly consistent image before recovery.
    backup = tmp_path / "before-recover.sqlite"
    with closing(
        sqlite3.connect((tmp_path / "crash.sqlite").as_uri() + "?mode=ro", uri=True)
    ) as src:
        with closing(sqlite3.connect(backup)) as dst:
            src.backup(dst)
    return tmp_path / "crash.sqlite", receipt


async def scoring_crash(tmp_path):
    database, receipt = await crash_process(tmp_path, "scoring")
    app = await open_eval(database, execution_enabled=False)
    APPLICATIONS.append(app)
    try:
        result = await app.repository.snapshot(receipt["task_id"])
        assert result["status"] == "interrupted" and result["code"] == "execution_unknown"
        assert result["items"][0]["output"] is not None and result["items"][0]["scores"] is None
        request = ContinuousRequest.model_validate(receipt["request"])
        assert (await app.continuous.submit(request))["id"] == receipt["task_id"]
        assert app.kernel.tasks == {} and app.owner is None and app.adapter is None
    finally:
        await app.close()


# PN3_REPAIR01：反例仅追加；沿用真实SQLite、内核和进程owner。
async def _pn3_repair_confirm(app, pipeline, dataset, row, *, context="complete", name="repair"):
    from krim_agent.evaluation.domain import DatasetRef

    draft = await app.repository.candidate("local", row["id"])
    confirmed = await pipeline.confirm_case(
        ConfirmationRequest(
            request_key=name + "-confirm",
            candidate_id=draft["candidate_id"],
            expected_revision=draft["revision"],
            expected_hash=draft["hash"],
            case=dataset.cases[0],
            context=context,
            reason="本轮合成来源的人工参考；缺失证明不由人工参考补造。",
        )
    )
    frozen = await pipeline.freeze_dataset(
        FreezeConfirmed(
            request_key=name + "-freeze", name=name, version="1", confirmations=[confirmed["id"]]
        )
    )
    return ContinuousRequest(
        request_key=name + "-score",
        mode="observed_score",
        dataset=DatasetRef.model_validate(frozen["dataset_ref"]),
        dataset_envelope_fp=frozen["dataset_envelope_fp"],
        metrics=continuous_rules(app.repository.metric_registry),
        observations=[
            ScoreSource(
                case_id="basic",
                repetition=0,
                observation_id=row["id"],
                observation_hash=row["content_hash"],
            )
        ],
    )


def _pn3_repair_gate(report):
    from krim_agent.evaluation.continuous_reports import compare
    from krim_agent.evaluation.continuous_types import ComparisonPolicy

    return compare(
        ComparisonPolicy(
            baseline_id=report["eval_id"],
            candidate_id=report["eval_id"],
            baseline_hash=report["report_hash"],
            candidate_hash=report["report_hash"],
            allowed_subjects=[report["subject_fingerprint"]],
        ),
        report,
        report,
    )


async def test_pn3_repair_source_missing_context_rescore(tmp_path):
    app, dataset, selection, pipeline = await seed(tmp_path)
    try:
        pipeline.sources["current-eval"] = EvalSourceReader(app.repository, include_context=False)
        pipeline.redaction["artifact_paths"] = ["ledger.json", "summary.md"]
        row = (
            await pipeline.import_sources(
                ImportRequest(request_key="repair-context", sources=[selection])
            )
        )[0]
        source_report = await app.repository.report(selection.task_id)
        request = await _pn3_repair_confirm(
            app, pipeline, dataset, row, context="manual_reconstructed"
        )
        assert (
            row["context_hash"] == neutral_fingerprint(None)
            and row["eligibility"] == "needs_context"
        )
        reports = []
        with CallCounts(app) as calls:
            for _index in range(2):
                view = await pipeline.submit(request)
                await app.kernel.wait(view["id"])
                report = await app.repository.report(view["id"])
                reports.append(report)
                assert report["items"][0]["quality"] == "unscored"
                assert (
                    report["source_inputs"]["basic:0"]["source_context_hash"] == row["context_hash"]
                )
                assert report["environment"] == row["environment"]
                assert _pn3_repair_gate(report)["status"] != "pass"
                assert (await pipeline.submit(request))["id"] == view["id"]
                request = request.model_copy(
                    update={
                        "request_key": "repair-context-rescore",
                        "mode": "rescore",
                        "observations": [],
                        "parent_task_id": view["id"],
                        "parent_report_hash": report["report_hash"],
                    }
                )
            assert all(value == 0 for value in calls.counts.values()), calls.counts
        assert await app.repository.report(selection.task_id) == source_report
        assert await app.repository.observation("local", row["id"]) == row
        (tmp_path / "repair-context-proof.json").write_text(
            json.dumps({"counts": calls.counts, "reports": reports}, indent=2)
        )
    finally:
        await app.close()


async def test_pn3_repair_source_nonterminal_output(tmp_path, monkeypatch):

    source, dataset, selection, original = await seed(tmp_path)
    destination = await open_eval(tmp_path / "destination.sqlite", execution_enabled=False)
    APPLICATIONS.append(destination)
    pipeline = ContinuousEvaluation(
        destination.repository,
        destination.kernel,
        sources={"current-eval": EvalSourceReader(source.repository)},
    )
    pipeline.redaction["artifact_paths"] = ["ledger.json", "summary.md"]
    entered, release = asyncio.Event(), asyncio.Event()
    active = None
    try:
        row = (
            await pipeline.import_sources(
                ImportRequest(request_key="terminal-seed", sources=[selection])
            )
        )[0]
        request = await _pn3_repair_confirm(destination, pipeline, dataset, row)

        original_commit = source.repository.commit

        async def pause(trial, body):
            entered.set()
            await release.wait()
            return await original_commit(trial, body)

        monkeypatch.setattr(source.repository, "commit", pause)
        active = await source.run(
            TaskSpec(
                request_key="actual-running-source",
                dataset=dataset.ref(),
                metrics=source.repository.metric_registry.specs(),
            )
        )
        await asyncio.wait_for(entered.wait(), 2)
        running = await source.repository.snapshot(active["id"])
        assert running["status"] == "running" and running["items"][0]["output"] is not None
        imported = (
            await pipeline.import_sources(
                ImportRequest(
                    request_key="actual-nonterminal",
                    sources=[SourceSelection(task_id=active["id"], case_id="basic")],
                )
            )
        )[0]
        assert imported["eligibility"] == "ineligible" and "not_terminal" in imported["reasons"]
        request = request.model_copy(
            update={
                "observations": [
                    ScoreSource(
                        case_id="basic",
                        repetition=0,
                        observation_id=imported["id"],
                        observation_hash=imported["content_hash"],
                    )
                ]
            }
        )
        with CallCounts(source) as calls:
            view = await pipeline.submit(request)
            await destination.kernel.wait(view["id"])
            report = await destination.repository.report(view["id"])
            assert report["items"][0]["quality"] != "passed"
            integrity = next(
                score
                for score in report["items"][0]["scores"]
                if score["rule"]["name"] == "fact_integrity"
            )
            assert integrity["status"] == "skipped"
            assert _pn3_repair_gate(report)["status"] != "pass"
            assert all(value == 0 for value in calls.counts.values()), calls.counts
        assert await source.repository.snapshot(active["id"]) == running
        assert await destination.repository.observation("local", imported["id"]) == imported
        (tmp_path / "repair-nonterminal-proof.json").write_text(
            json.dumps({"running": running, "report": report, "counts": calls.counts}, indent=2)
        )
    finally:
        release.set()
        if active is not None:
            await source.kernel.wait(active["id"])
        await destination.close()
        await source.close()


async def test_pn3_repair_source_environment_hard_failure(tmp_path, monkeypatch):
    from sqlalchemy import update

    from krim_agent.evaluation.repository import tasks

    app, pipeline, request, old_source = await confirmed_task(tmp_path, bad_reference=True)
    original_begin = app.repository._begin_task_conn
    try:

        async def tagged(*args, **kwargs):
            value, created = await original_begin(*args, **kwargs)
            if created:
                value["environment"]["source_fixture_marker"] = "actual-origin-metadata"
                await args[0].execute(
                    update(tasks).where(tasks.c.id == value["id"]).values(body=value)
                )
            return value, created

        monkeypatch.setattr(app.repository, "_begin_task_conn", tagged)
        parent = await pipeline.submit(
            request.model_copy(
                update={
                    "mode": "execute",
                    "observations": [],
                    "request_key": "repair-failure-execute",
                }
            )
        )
        await app.kernel.wait(parent["id"])
        monkeypatch.setattr(app.repository, "_begin_task_conn", original_begin)
        source_report = await app.repository.report(parent["id"])
        assert source_report["items"][0]["quality"] == "failed"
        row = (
            await pipeline.import_sources(
                ImportRequest(
                    request_key="repair-failure-import",
                    sources=[SourceSelection(task_id=parent["id"], case_id="basic")],
                )
            )
        )[0]
        assert row["environment"]["source_fixture_marker"] == "actual-origin-metadata"
        score_request = request.model_copy(
            update={
                "request_key": "repair-failure-score",
                "observations": [
                    ScoreSource(
                        case_id="basic",
                        repetition=0,
                        observation_id=row["id"],
                        observation_hash=row["content_hash"],
                    )
                ],
            }
        )
        with CallCounts(app) as calls:
            for _index in range(2):
                view = await pipeline.submit(score_request)
                await app.kernel.wait(view["id"])
                report = await app.repository.report(view["id"])
                assert report["source_inputs"]["basic:0"]["known_hard_failure"] is True
                integrity = next(
                    s for s in report["items"][0]["scores"] if s["rule"]["name"] == "fact_integrity"
                )
                assert integrity["status"] == "scored" and integrity["value"] == 0
                assert report["environment"] == row["environment"]
                assert (
                    report["items"][0]["quality"] == "failed"
                    and _pn3_repair_gate(report)["status"] != "pass"
                )
                score_request = score_request.model_copy(
                    update={
                        "request_key": "repair-failure-rescore",
                        "mode": "rescore",
                        "observations": [],
                        "parent_task_id": view["id"],
                        "parent_report_hash": report["report_hash"],
                    }
                )
            assert all(value == 0 for value in calls.counts.values()), calls.counts
        assert await app.repository.report(parent["id"]) == source_report
        assert await app.repository.observation("local", row["id"]) == row
    finally:
        await app.close()


async def _pn3_repair_corrupt_record(tmp_path, monkeypatch, *, continuous):
    from krim_agent.evaluation.metrics import MetricRegistry

    if continuous:
        app, pipeline, request, source = await confirmed_task(tmp_path)
        request = request.model_copy(
            update={
                "mode": "execute",
                "observations": [],
                "request_key": "corrupt-continuous",
            }
        )
    else:
        app, dataset, selection, pipeline = await seed(tmp_path)
        request = TaskSpec(
            request_key="corrupt-neutral",
            dataset=dataset.ref(),
            metrics=app.repository.metric_registry.specs(),
        )
    original = app.repository.record
    rejected, scoring = [], []
    before_exec, before_spawn = app.adapter.execute_calls, app.owner.spawn_calls

    async def record(trial, ref, output):
        # 修改真实collect交回的副本；拒绝由未修改的原record产生。
        if not rejected and output is not None:
            assert output["execution_ref"] == ref.id
            output["execution_ref"] = "00000000-0000-0000-0000-000000000000"
        try:
            return await original(trial, ref, output)
        except ServiceError as error:
            rejected.append(
                {"code": error.code, "real_ref": ref.id, "output": json.loads(json.dumps(output))}
            )
            raise

    def observe(frame, event, argument):
        if event == "call" and frame.f_code is MetricRegistry.score.__code__:
            scoring.append(True)

    try:
        monkeypatch.setattr(app.repository, "record", record)
        previous = sys.getprofile()
        sys.setprofile(observe)
        try:
            view = await (pipeline.submit(request) if continuous else app.run(request))
            result = await app.kernel.wait(view["id"])
        finally:
            sys.setprofile(previous)
        assert len(rejected) == 1 and rejected[0]["code"] == "facts_corrupt"
        assert result["status"] == "failed" and result["code"] == "facts_corrupt"
        assert scoring == []
        item = result["items"][0]
        assert item["execution_ref"] == rejected[0]["real_ref"]
        assert (
            item["output"] is not None and item["output"]["execution_ref"] == item["execution_ref"]
        )
        assert item["quality"] != "passed"
        report = await app.repository.report(view["id"])
        assert report["status"] == "failed" and report["complete"] is False
        assert app.adapter.execute_calls == before_exec + 1
        assert app.owner.spawn_calls == before_spawn + 1
        assert (await (pipeline.submit(request) if continuous else app.run(request)))["id"] == view[
            "id"
        ]
        assert app.owner.spawn_calls == before_spawn + 1
        (tmp_path / "repair-corrupt-proof.json").write_text(
            json.dumps(
                {
                    "rejected": rejected,
                    "scoring_calls": len(scoring),
                    "report": report,
                    "real_record_module": original.__func__.__module__,
                    "real_record_first_line": original.__func__.__code__.co_firstlineno,
                },
                indent=2,
            )
        )
    finally:
        await app.close()


async def test_pn3_repair_facts_corrupt_neutral(tmp_path, monkeypatch):
    await _pn3_repair_corrupt_record(tmp_path, monkeypatch, continuous=False)


async def test_pn3_repair_facts_corrupt_continuous(tmp_path, monkeypatch):
    await _pn3_repair_corrupt_record(tmp_path, monkeypatch, continuous=True)


async def _pn3_repair_process_fixture(tmp_path, constraints, *, arguments=True, name="process"):
    from krim_agent.composition import open_application
    from krim_agent.contracts import RunRequest
    from krim_agent.evaluation.adapters.krim_source import KrimSourceReader
    from krim_agent.evaluation.domain import CaseSpec
    from krim_agent.security import LOCAL

    path = tmp_path / "process-source.sqlite"
    DATABASES.append(path)
    host = await open_application(path)
    app = None
    try:
        run = await host.service.start(
            RunRequest(text="/add 2 3", request_key="process-source", target="target-v2")
        )
        await host.service.wait(run.id)
        actual = await host.service.result(run.id)
        assert actual.text == "5" and actual.tools[0].executor_started is True
        app = await open_eval(tmp_path / "process-score.sqlite", execution_enabled=False)
        APPLICATIONS.append(app)
        pipeline = ContinuousEvaluation(
            app.repository,
            app.kernel,
            sources={
                "current-eval": KrimSourceReader(
                    host.store, host.sessions, LOCAL, allow_legacy=True
                )
            },
        )
        pipeline.redaction["include_arguments"] = arguments
        pipeline.redaction["include_text"] = True
        row = (
            await pipeline.import_sources(
                ImportRequest(
                    request_key=name + "-import",
                    sources=[SourceSelection(run_id=run.id)],
                )
            )
        )[0]
        case = CaseSpec(
            case_id="basic",
            instruction="/add 2 3",
            files={"context.txt": "synthetic fixture"},
            verifier="krim-script/1",
            reference={"expected": "5", "process_constraints": constraints},
        )
        dataset = DatasetSpec(name="process", version="1", cases=[case])
        request = await _pn3_repair_confirm(
            app,
            pipeline,
            dataset,
            row,
            context="manual_reconstructed",
            name=name,
        )
        process = app.repository.metric_registry._registered["process_constraints"].spec
        request = request.model_copy(
            update={
                "target_key": row["target"]["key"],
                "metrics": continuous_rules(app.repository.metric_registry, "krim") + [process],
            }
        )
        return app, pipeline, request, row, host
    except BaseException:
        if app is not None:
            await app.close()
        await host.close()
        raise


async def _pn3_repair_process_score(app, pipeline, request, row, host, tmp_path):
    before = host.service.bindings["target-v2"].loop.provider.calls
    source = await app.repository.observation("local", row["id"])
    view = await pipeline.submit(request)
    await app.kernel.wait(view["id"])
    report = await app.repository.report(view["id"])
    score = next(
        value
        for value in report["items"][0]["scores"]
        if value["rule"]["name"] == "process_constraints"
    )
    assert host.service.bindings["target-v2"].loop.provider.calls == before
    assert app.owner is None and app.adapter is None
    assert await app.repository.observation("local", row["id"]) == source
    assert (await pipeline.submit(request))["id"] == view["id"]
    assert _pn3_repair_gate(report)["status"] != "pass"  # 原Run没有独立cleanup证明。
    (tmp_path / "repair-process-proof.json").write_text(
        json.dumps(
            {
                "source": source,
                "report": report,
                "provider_before": before,
                "provider_after": host.service.bindings["target-v2"].loop.provider.calls,
            },
            indent=2,
        )
    )
    return score


async def test_pn3_repair_process_shape(tmp_path):
    from pydantic import ValidationError

    from krim_agent.evaluation.domain import CaseSpec

    invalid = [
        [],
        {"unknown": True},
        {"min_completed": True},
        {"min_completed": -1},
        {"allowed_tools": "add"},
        {"forbidden_tools": ["add", "add"]},
        {"arguments": {"add": []}},
        {"approvals": True},
        {"approvals": [{"name": "add", "decision": "maybe"}]},
        {"approvals": [{"name": "add", "decision": []}]},
    ]
    for constraints in invalid:
        case = CaseSpec(
            case_id="basic",
            instruction="/add 2 3",
            files={"context.txt": "synthetic fixture"},
            verifier="krim-script/1",
            reference={"expected": "5", "process_constraints": constraints},
        )
        with pytest.raises(ValidationError):
            ConfirmationRequest(
                request_key="invalid-shape",
                candidate_id="fixture",
                expected_revision=0,
                expected_hash="0" * 64,
                case=case,
                context="manual_reconstructed",
                reason="shape",
            )


async def test_pn3_repair_process_rule_omission(tmp_path):
    app, pipeline, request, row, host = await _pn3_repair_process_fixture(
        tmp_path, {"min_completed": 1}
    )
    try:
        request = request.model_copy(
            update={
                "metrics": [
                    value for value in request.metrics if value.name != "process_constraints"
                ],
            }
        )
        with pytest.raises(ServiceError, match="invalid_input"):
            await pipeline.submit(request)
        assert await app.repository.by_key(request.model_dump(mode="json")) is None
        assert app.kernel.tasks == {} and app.owner is None
        assert (await app.repository.observation("local", row["id"]))["content_hash"] == row[
            "content_hash"
        ]
    finally:
        await app.close()
        await host.close()


async def test_pn3_repair_process_missing_evidence(tmp_path):
    for index, constraints in enumerate(
        [
            {"arguments": {"add": {"left": 2.0}}},
            {"approvals": [{"name": "add", "decision": "allow"}]},
        ]
    ):
        root = tmp_path / str(index)
        root.mkdir()
        app, pipeline, request, row, host = await _pn3_repair_process_fixture(
            root, constraints, arguments=False
        )
        try:
            score = await _pn3_repair_process_score(app, pipeline, request, row, host, root)
            assert score["status"] == "skipped" and score["value"] is None
        finally:
            await app.close()
            await host.close()


async def test_pn3_repair_process_violation(tmp_path):
    for index, constraints in enumerate(
        [
            {"min_completed": 2},
            {"forbidden_tools": ["add"]},
            {"allowed_tools": ["multiply"]},
            {"arguments": {"add": {"left": 999.0}}},
        ]
    ):
        root = tmp_path / str(index)
        root.mkdir()
        app, pipeline, request, row, host = await _pn3_repair_process_fixture(root, constraints)
        try:
            score = await _pn3_repair_process_score(app, pipeline, request, row, host, root)
            assert score["status"] == "scored" and score["value"] == 0
        finally:
            await app.close()
            await host.close()


async def test_pn3_repair_process_satisfied(tmp_path):
    constraints = {
        "min_completed": 1,
        "allowed_tools": ["add"],
        "arguments": {"add": {"left": 2.0, "right": 3.0}},
    }
    app, pipeline, request, row, host = await _pn3_repair_process_fixture(tmp_path, constraints)
    try:
        assert row["output"]["target_result"]["tools"][0]["executor_started"] is True
        score = await _pn3_repair_process_score(app, pipeline, request, row, host, tmp_path)
        assert score["status"] == "scored" and score["value"] == 1
        from krim_agent.evaluation.continuous_metrics import process_constraints

        output = json.loads(json.dumps(row["output"]))
        output["target_result"]["tools"][0]["status"] = "proposed"
        output["target_result"]["tools"][0]["executor_started"] = False
        status, value, reason = await process_constraints(
            {"reference": {"process_constraints": constraints}}, output, {}
        )
        assert status == "skipped" and value is None  # 提案负例不能伪装成真实已完成动作。
    finally:
        await app.close()
        await host.close()


async def test_pn3_repair_source_cross_scope(tmp_path, monkeypatch):
    app, dataset, selection, pipeline = await seed(tmp_path)
    original = app.repository.snapshot
    entered = []

    async def snapshot(identity):
        entered.append(identity)
        return await original(identity)

    try:
        monkeypatch.setattr(app.repository, "snapshot", snapshot)
        reader = EvalSourceReader(app.repository, scope="other")
        with pytest.raises(ServiceError, match="not_found") as rejected:
            await reader.read("other", selection.model_dump(mode="json"))
        assert rejected.value.status == 404 and entered == []
        assert app.adapter.execute_calls == 1
        (tmp_path / "repair-preauth.json").write_text(
            json.dumps({"snapshot_calls": entered, "code": rejected.value.code})
        )
    finally:
        monkeypatch.setattr(app.repository, "snapshot", original)
        await app.close()


async def _pn3_repair_session_source(tmp_path):
    from krim_agent.composition import open_application
    from krim_agent.security import Membership, PrincipalContext
    from krim_agent.sessions.types import SessionCreate, TurnRequest

    path = tmp_path / "session-source.sqlite"
    DATABASES.append(path)
    host = await open_application(path)
    owner = PrincipalContext("source-owner", 1, (Membership(workspace_id="local"),))
    other = PrincipalContext("other-owner", 1, (Membership(workspace_id="local"),))
    try:
        session = await host.sessions.create(
            owner, "local", SessionCreate(request_key="session", target_key="target-v2")
        )
        turn = await host.sessions.submit(
            owner,
            "local",
            session.id,
            TurnRequest(request_key="turn", text="/add 2 3", expected_revision=session.revision),
        )
        async with asyncio.timeout(3):
            while turn.run_id is None:
                await asyncio.sleep(0)
                turn = await host.sessions.turn(owner, "local", session.id, turn.id)
        await host.service.wait(turn.run_id)
        return host, owner, other, turn.run_id, path
    except BaseException:
        await host.close()
        raise


async def test_pn3_repair_source_other_owner(tmp_path, monkeypatch):
    from krim_agent.evaluation.adapters.krim_source import KrimSourceReader

    host, owner, other, run_id, source_path = await _pn3_repair_session_source(tmp_path)
    original = host.store.snapshot
    entered = []

    async def snapshot(identity):
        entered.append(identity)
        return await original(identity)

    try:
        monkeypatch.setattr(host.store, "snapshot", snapshot)
        reader = KrimSourceReader(host.store, host.sessions, other)
        before = host.service.bindings["target-v2"].loop.provider.calls
        with pytest.raises(ServiceError, match="not_found") as rejected:
            await reader.read("local", {"run_id": run_id})
        assert rejected.value.status == 404 and entered == []
        assert host.service.bindings["target-v2"].loop.provider.calls == before
        (tmp_path / "repair-owner-preauth.json").write_text(
            json.dumps({"snapshot_calls": entered, "run_id": run_id, "code": rejected.value.code})
        )
    finally:
        monkeypatch.setattr(host.store, "snapshot", original)
        await host.close()


async def test_pn3_repair_source_unauthorized_corrupt(tmp_path, monkeypatch):
    from sqlalchemy import select, update

    from krim_agent.evaluation.adapters.krim_source import KrimSourceReader
    from krim_agent.evaluation.repository import tasks
    from krim_agent.storage.models import run_events

    app, dataset, selection, pipeline = await seed(tmp_path)
    original = app.repository.snapshot
    entered = []

    async def snapshot(identity):
        entered.append(identity)
        return await original(identity)

    saved = await original(selection.task_id)
    try:
        async with app.repository.engine.begin() as conn:
            await conn.execute(
                update(tasks)
                .where(tasks.c.id == selection.task_id)
                .values(body={"corrupt": "unauthorized-fixture"})
            )
        monkeypatch.setattr(app.repository, "snapshot", snapshot)
        reader = EvalSourceReader(app.repository, scope="other")
        with pytest.raises(ServiceError, match="not_found") as rejected:
            await reader.read("other", selection.model_dump(mode="json"))
        assert rejected.value.status == 404 and entered == []
    finally:
        try:
            _pn3_repair_backup(app.repository.path, tmp_path / "unauthorized-corrupt-eval.sqlite")
        finally:
            monkeypatch.setattr(app.repository, "snapshot", original)
            try:
                async with app.repository.engine.begin() as conn:
                    await conn.execute(
                        update(tasks).where(tasks.c.id == selection.task_id).values(body=saved)
                    )
            finally:
                await app.close()
    host, owner, other, run_id, source_path = await _pn3_repair_session_source(tmp_path)
    original = host.store.snapshot
    entered.clear()

    async def run_snapshot(identity):
        entered.append(identity)
        return await original(identity)

    async with host.store.engine.begin() as conn:
        event = (
            await conn.execute(
                select(run_events.c.data).where(
                    run_events.c.run_id == run_id, run_events.c.seq == 0
                )
            )
        ).scalar_one()
    try:
        async with host.store.engine.begin() as conn:
            await conn.execute(
                update(run_events)
                .where(run_events.c.run_id == run_id, run_events.c.seq == 0)
                .values(data={"corrupt": "unauthorized-fixture"})
            )
        monkeypatch.setattr(host.store, "snapshot", run_snapshot)
        reader = KrimSourceReader(host.store, host.sessions, other)
        with pytest.raises(ServiceError, match="not_found") as rejected:
            await reader.read("local", {"run_id": run_id})
        assert rejected.value.status == 404 and entered == []
    finally:
        try:
            _pn3_repair_backup(source_path, tmp_path / "unauthorized-corrupt-run.sqlite")
        finally:
            monkeypatch.setattr(host.store, "snapshot", original)
            try:
                async with host.store.engine.begin() as conn:
                    await conn.execute(
                        update(run_events)
                        .where(run_events.c.run_id == run_id, run_events.c.seq == 0)
                        .values(data=event)
                    )
            finally:
                await host.close()


def _pn3_repair_backup(source, destination):
    import sqlite3
    from contextlib import closing

    with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)) as reader:
        with closing(sqlite3.connect(destination)) as writer:
            reader.backup(writer)
