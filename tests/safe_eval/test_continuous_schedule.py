"""固定窗口、同kernel事务准入与重启；不启动后台调度器。"""

import asyncio
import hashlib
import json
from pathlib import Path

import pytest
from sqlalchemy import select
from test_continuous_eval import APPLICATIONS, confirmed_task
from test_continuous_eval import continuous_resources as continuous_resources

from krim_agent.contracts import ServiceError
from krim_agent.evaluation.app import open_eval
from krim_agent.evaluation.continuous import ContinuousEvaluation
from krim_agent.evaluation.continuous_types import ScheduleSpec
from krim_agent.evaluation.repository import eval_schedule_windows, tasks

TEST_DIGEST = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
SOURCE_ROOT = Path(__file__).resolve().parents[2]


class Clock:
    def __init__(self, now=100000):
        self.now = now

    def __call__(self):
        return float(self.now)


async def scheduled(tmp_path, *, mode="execute", repetitions=1, live=False):
    app, original, request, source = await confirmed_task(tmp_path)
    if live:
        from test_neutral_eval import pn1_repair_worker

        pn1_repair_worker(app, tmp_path, live=True)
    clock = Clock()
    pipeline = ContinuousEvaluation(
        app.repository, app.kernel, actor=original.actor, sources=original.sources, clock=clock
    )
    template = request.model_copy(
        update={"mode": mode, "observations": [] if mode == "execute" else request.observations}
    )
    from krim_agent.evaluation.domain import Budget

    template = template.model_copy(update={"execution": Budget(repetitions=repetitions)})
    spec = ScheduleSpec(
        schedule_id="bounded",
        version="1",
        template=template,
        epoch=100000,
        interval_seconds=60,
        max_windows=3,
    )
    frozen = await pipeline.create_schedule(spec)
    assert frozen["enabled"] and frozen["control_revision"] == 0
    assert (await pipeline.create_schedule(spec))["spec_hash"] == frozen["spec_hash"]
    return app, pipeline, clock, spec


async def schedule_rows(app):
    async with app.repository.engine.begin() as conn:
        task_rows = (
            (await conn.execute(select(tasks.c.body).where(tasks.c.request_key.like("schedule:%"))))
            .scalars()
            .all()
        )
        windows = (await conn.execute(select(eval_schedule_windows.c.body))).scalars().all()
    return task_rows, windows


@pytest.mark.parametrize(
    "kind",
    ["concurrent", "return_lost", "restart", "missed", "busy", "expired", "changed_template"],
)
async def test_schedule_window_admission_and_recovery(tmp_path, monkeypatch, kind):
    if kind == "restart":
        from test_continuous_eval import crash_process

        database, receipt = await crash_process(tmp_path, "reservation")
        app = await open_eval(database, clock=Clock())
        APPLICATIONS.append(app)
        try:
            view = await app.continuous.tick("bounded")
            assert view["id"] == receipt["task_id"] and view["status"] == "interrupted"
            assert app.kernel.tasks == {} and app.adapter.execute_calls == 0
        finally:
            await app.close()
        return
    app, pipeline, clock, spec = await scheduled(tmp_path)
    release = asyncio.Event()
    try:
        if kind == "changed_template":
            with pytest.raises(ServiceError, match="key_conflict"):
                await pipeline.create_schedule(spec.model_copy(update={"version": "2"}))
            with pytest.raises(ServiceError, match="key_conflict"):
                await pipeline.create_schedule(spec.model_copy(update={"max_windows": 2}))
            assert await schedule_rows(app) == ([], [])
            assert app.adapter.execute_calls == 1
            return
        if kind == "expired":
            clock.now += 180
            with pytest.raises(ServiceError, match="closed"):
                await pipeline.tick(spec.schedule_id)
            jobs, windows = await schedule_rows(app)
            assert (
                jobs == [] and all(w["status"] == "missed" for w in windows) and len(windows) == 3
            )
            assert app.adapter.execute_calls == 1
            return
        if kind == "busy":
            from krim_agent.evaluation.domain import MetricSpec

            entered = asyncio.Event()

            async def hold(case, output, config):
                entered.set()
                await release.wait()
                return "scored", 1, None

            metric = MetricSpec(name="bounded_busy", version="1", implementation_digest=TEST_DIGEST)
            app.repository.metric_registry.register(metric, hold)
            # Consume only the original imported source, not a second app/kernel.
            observed = (await app.repository.schedule("local", spec.schedule_id))["spec"][
                "template"
            ]
            from krim_agent.evaluation.continuous_types import ContinuousRequest, ScoreSource

            confirmed = spec.template
            link = (
                await app.repository.dataset(confirmed.dataset.name, confirmed.dataset.version)
            ).cases[0]
            async with app.repository.engine.begin() as conn:
                from krim_agent.evaluation.repository import dataset_lineage

                lineage = (await conn.execute(select(dataset_lineage.c.body))).scalars().first()
            ref = lineage["lineage"]["cases"][0]["source_refs"]
            busy_request = ContinuousRequest.model_validate(
                observed
                | {
                    "mode": "observed_score",
                    "request_key": "busy-source",
                    "observations": [
                        ScoreSource(
                            case_id=link.case_id,
                            repetition=0,
                            observation_id=ref["observation_id"],
                            observation_hash=ref["observation_hash"],
                        ).model_dump(mode="json")
                    ],
                    "metrics": [
                        m.model_dump(mode="json") for m in spec.template.metrics + [metric]
                    ],
                }
            )
            busy = await pipeline.submit(busy_request)
            await asyncio.wait_for(entered.wait(), 2)
            with pytest.raises(ServiceError, match="capacity"):
                await pipeline.tick(spec.schedule_id)
            assert await schedule_rows(app) == ([], [])
            release.set()
            await app.kernel.wait(busy["id"])
        if kind == "missed":
            clock.now += 120
        lost = []
        if kind == "return_lost":
            original = app.repository._write

            async def committed_then_lost(operation):
                value = await original(operation)
                if (
                    isinstance(value, tuple)
                    and value[1] is True
                    and value[0]["request_json"].get("schedule") is not None
                    and not lost
                ):
                    lost.append(True)
                    raise OSError("schedule_commit_return_lost")
                return value

            monkeypatch.setattr(app.repository, "_write", committed_then_lost)
        a, b = await asyncio.gather(
            pipeline.tick(spec.schedule_id), pipeline.tick(spec.schedule_id)
        )
        jobs, windows = await schedule_rows(app)
        assert len(jobs) == 1
        assert a["id"] == b["id"] == jobs[0]["id"]
        result = await app.kernel.wait(a["id"])
        assert result["status"] == "completed" and result["items"][0]["quality"] == "passed"
        assert app.adapter.execute_calls == 2
        assert len([w for w in windows if w["status"] == "reserved"]) == 1
        assert len([w for w in windows if w["status"] == "missed"]) == (
            2 if kind == "missed" else 0
        )
        assert (await pipeline.tick(spec.schedule_id))["id"] == a["id"]
        if kind == "return_lost":
            assert len(lost) == 1
        (tmp_path / "schedule-result.json").write_text(
            json.dumps(
                {"task_id": a["id"], "rows": windows, "exec": app.adapter.execute_calls}, indent=2
            )
        )
    finally:
        release.set()
        await app.close()


@pytest.mark.parametrize("kind", ["max_windows", "disabled", "wrong_scope", "cancel", "unknown"])
async def test_schedule_bound_budget_cancel_and_scope(tmp_path, monkeypatch, kind):
    app, pipeline, clock, spec = await scheduled(
        tmp_path, repetitions=3 if kind == "max_windows" else 1, live=kind == "cancel"
    )
    try:
        if kind == "max_windows":
            ids = []
            for index in range(3):
                clock.now = spec.epoch + 60 * index
                view = await pipeline.tick(spec.schedule_id)
                result = await app.kernel.wait(view["id"])
                assert result["status"] == "completed" and result["planned_count"] == 3
                assert all(i["quality"] == "passed" for i in result["items"])
                assert (await pipeline.tick(spec.schedule_id))["id"] == view["id"]
                ids.append(view["id"])
            assert len(set(ids)) == 3 and app.adapter.execute_calls == 10
            clock.now += 60
            with pytest.raises(ServiceError, match="closed"):
                await pipeline.tick(spec.schedule_id)
            jobs, windows = await schedule_rows(app)
            assert len(jobs) == len(windows) == 3 and sum(j["planned_count"] for j in jobs) == 9
            from pydantic import ValidationError

            with pytest.raises(ValidationError):
                await pipeline.create_schedule(
                    spec.model_copy(update={"schedule_id": "too-many", "max_windows": 4})
                )
        elif kind == "disabled":
            disabled = await pipeline.disable(spec.schedule_id, 0, "disable-once")
            assert disabled["enabled"] is False and disabled["control_revision"] == 1
            assert await pipeline.disable(spec.schedule_id, 0, "disable-once") == disabled
            with pytest.raises(ServiceError, match="snapshot_changed"):
                await pipeline.disable(spec.schedule_id, 0, "different-key")
            with pytest.raises(ServiceError, match="key_conflict"):
                await pipeline.disable(spec.schedule_id, 1, "disable-once")
            assert (await pipeline.create_schedule(spec))["enabled"] is False
            with pytest.raises(ServiceError, match="closed"):
                await pipeline.tick(spec.schedule_id)
            assert await schedule_rows(app) == ([], [])
            assert app.adapter.execute_calls == 1
        elif kind == "wrong_scope":
            other = ContinuousEvaluation(app.repository, app.kernel, scope="other", clock=clock)
            with pytest.raises(ServiceError, match="not_found"):
                await other.tick(spec.schedule_id)
            with pytest.raises(ServiceError, match="not_found"):
                await other.create_schedule(spec)
            with pytest.raises(ServiceError, match="not_found"):
                await other.disable(spec.schedule_id, 0, "wrong-scope")
            assert await schedule_rows(app) == ([], [])
            assert app.adapter.execute_calls == 1
        elif kind == "cancel":
            view = await pipeline.tick(spec.schedule_id)
            async with asyncio.timeout(2):
                while True:
                    job = next(
                        (
                            j
                            for j in app.owner.jobs.values()
                            if j.prepared.spec.task_id == view["id"]
                        ),
                        None,
                    )
                    if job is not None and (Path(job.prepared.directory) / "alive").exists():
                        break
                    await asyncio.sleep(0.005)
            result = await app.kernel.cancel(view["id"])
            assert result["status"] == "aborted"
            assert job.process.returncode is not None and job.pidfd == -1 and job.collector.done()
            assert (await app.kernel.cancel(view["id"]))["id"] == view["id"]
            assert (await pipeline.tick(spec.schedule_id))["id"] == view["id"]
            assert app.adapter.execute_calls == 2
            assert len((await schedule_rows(app))[1]) == 1
        else:
            original_write, original_lookup = app.repository._write, app.repository.by_key
            lost = []

            async def lookup(value):
                if lost:
                    raise OSError("controlled_confirmation_unavailable")
                return await original_lookup(value)

            async def committed_unknown(operation):
                value = await original_write(operation)
                if (
                    isinstance(value, tuple)
                    and value[1]
                    and value[0]["request_json"].get("schedule") is not None
                    and not lost
                ):
                    lost.append(value[0]["id"])
                    raise OSError("committed_task_return_unknown")
                return value

            monkeypatch.setattr(app.repository, "_write", committed_unknown)
            monkeypatch.setattr(app.repository, "by_key", lookup)
            with pytest.raises(ServiceError, match="storage_unavailable"):
                await pipeline.tick(spec.schedule_id)
            assert len(lost) == 1 and app.adapter.execute_calls == 1
            monkeypatch.setattr(app.repository, "by_key", original_lookup)
            monkeypatch.setattr(app.repository, "_write", original_write)
            recovered = await pipeline.tick(spec.schedule_id)
            assert recovered["id"] == lost[0] and recovered["status"] == "queued"
            assert lost[0] not in app.kernel.tasks
            jobs, windows = await schedule_rows(app)
            assert len(jobs) == len(windows) == 1 and windows[0]["task_id"] == lost[0]
            await app.close()
            new = await open_eval(tmp_path / "eval.sqlite", clock=clock)
            APPLICATIONS.append(new)
            try:
                settled = await new.continuous.tick(spec.schedule_id)
                assert settled["id"] == lost[0] and settled["status"] == "interrupted"
                assert new.adapter.execute_calls == 0 and new.kernel.tasks == {}
            finally:
                await new.close()
    finally:
        await app.close()


async def test_pn3_repair_tick_observe_host_fence(tmp_path, monkeypatch):
    from sqlalchemy import event

    app, pipeline, clock, spec = await scheduled(tmp_path)
    original = app.repository._write
    actor = pipeline.actor
    active, barriers = [], []
    before = app.adapter.execute_calls
    existing_tasks = set(app.kernel.tasks)
    initial = await app.repository.schedule("local", spec.schedule_id)
    clock.now += 120

    async def write(operation):
        async def observed(conn):
            active.append(True)
            try:
                return await operation(conn)
            finally:
                active.pop()

        return await original(observed)

    def drift(conn, cursor, statement, parameters, context, many):
        if active and not barriers and "SELECT eval_schedules.body" in statement:
            barriers.append(statement)
            pipeline.actor = "revoked-at-observe-sql"

    try:
        monkeypatch.setattr(app.repository, "_write", write)
        event.listen(app.repository.engine.sync_engine, "after_cursor_execute", drift)
        try:
            with pytest.raises(ServiceError, match="target_binding_changed"):
                await pipeline.tick(spec.schedule_id)
        finally:
            event.remove(app.repository.engine.sync_engine, "after_cursor_execute", drift)
            pipeline.actor = actor
        rows, windows = await schedule_rows(app)
        assert barriers and rows == [] and windows == []
        assert await app.repository.schedule("local", spec.schedule_id) == initial
        assert app.adapter.execute_calls == before and set(app.kernel.tasks) == existing_tasks
        (tmp_path / "repair-observe-fence.json").write_text(
            json.dumps(
                {
                    "sql_barriers": barriers,
                    "before_exec": before,
                    "after_exec": app.adapter.execute_calls,
                    "schedule": initial,
                },
                indent=2,
            )
        )
    finally:
        pipeline.actor = actor
        await app.close()
