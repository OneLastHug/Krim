"""真实 SDK 持久任务、部分取消、恢复、租约/幂等与进度只读性。"""

import asyncio
import json

import pytest
from alembic import command
from sqlalchemy import text

from krim_agent.config import Settings
from krim_agent.evaluation import (
    CallableScorer,
    CallableTarget,
    EvalCase,
    EvalConfig,
    EvalDataset,
    EvalOutput,
    GenerationTarget,
    SuccessScorer,
)
from krim_agent.evaluation.jobs import EvalJobSpec
from krim_agent.evaluation.service import EvaluationService
from krim_agent.persistence.eval_store import EvaluationStore
from krim_agent.persistence.migration import migration_config
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import FakeProvider, ModelText
from krim_agent.sdk import PythonAI


def data(*values):
    return EvalDataset(
        name="jobs",
        cases=[EvalCase(id=str(i), input=value, expected=value) for i, value in enumerate(values)],
    )


def echo_target():
    async def run(case, _attempt):
        return EvalOutput(value=case.input)

    return CallableTarget("echo", run)


async def test_job_persists_final_report_progress_items_and_events_are_read_only(repository):
    service = EvaluationService(repository.database)
    try:
        handle = await service.start(
            data("a", "b"),
            echo_target(),
            config=EvalConfig(repetitions=2, concurrency=2),
            request_key="one",
        )
        assert (await handle.wait()).status == "completed"
        report = await handle.report()
        assert report.summary.passed == 4
        before = (await handle.view()).event_count
        for _ in range(2):
            assert (await handle.items(limit=1)).items[0].case_id == "0"
            assert (await handle.partial_report()).summary.passed == 4
            assert (await handle.view()).completed == 4
        assert (await handle.view()).event_count == before
        events = [e async for e in handle.events() if e]
        assert [e.seq for e in events] == list(range(before))
        assert events[0].kind == "created" and events[-1].data["status"] == "completed"
        assert (
            await service.start(
                data("a", "b"),
                echo_target(),
                config=EvalConfig(repetitions=2, concurrency=2),
                request_key="one",
            )
        ).id == handle.id
        with pytest.raises(ServiceError, match="其他评估"):
            await service.start(data("changed"), echo_target(), request_key="one")
        with pytest.raises(ServiceError) as error:
            [e async for e in handle.events(before + 1)]
        assert error.value.code == "eval_cursor_invalid"
    finally:
        await service.close()


async def test_pause_keeps_completed_rows_and_uncertain_partial_result_then_skip_resume(repository):
    entered = asyncio.Event()
    closed = asyncio.Event()
    calls = []

    async def target(case, attempt):
        calls.append(case.id)
        if case.input == "wait":
            attempt.output = EvalOutput(value="partial", status="aborted")
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                closed.set()
        return EvalOutput(value=case.input)

    selected = CallableTarget("pause", target)
    service = EvaluationService(repository.database)
    try:
        handle = await service.start(data("done", "wait", "after"), selected)
        await asyncio.wait_for(entered.wait(), 5)
        assert (await handle.pause()).status == "paused"
        assert closed.is_set()
        partial = await handle.partial_report()
        assert partial.job.completed == 1 and partial.job.total == 3 and partial.complete is False
        assert (await handle.items()).items[1].output.value == "partial"
        with pytest.raises(ServiceError):
            await handle.report()
        await handle.resume()
        assert (await handle.wait()).status == "interrupted"
        assert calls == ["0", "1", "2"]
        await handle.resume(skip_uncertain=True)
        assert (await handle.wait()).status == "completed"
        report = await handle.report()
        assert report.summary.passed == 2 and report.summary.errors == 1
        assert calls == ["0", "1", "2"]
    finally:
        await service.close()


async def test_explicit_retry_records_generation_history_and_new_idempotency_key(repository):
    entered = asyncio.Event()
    seen = []

    async def target(case, attempt):
        seen.append(attempt.idempotency_key)
        if attempt.generation == 1:
            entered.set()
            await asyncio.Event().wait()
        return EvalOutput(value=case.input)

    service = EvaluationService(repository.database)
    try:
        handle = await service.start(data("retry"), CallableTarget("retry", target))
        await asyncio.wait_for(entered.wait(), 5)
        await handle.pause()
        await handle.resume(retry_uncertain=True)
        assert (await handle.wait()).status == "completed"
        item = (await handle.items()).items[0]
        assert item.generation == 2 and len(item.history) == 1
        assert len(seen) == 2 and seen[0] != seen[1]
    finally:
        await service.close()


async def test_completed_output_and_completed_score_resume_without_reexecuting(repository):
    calls = 0
    entered = asyncio.Event()

    async def target(case, _attempt):
        nonlocal calls
        calls += 1
        return EvalOutput(value=case.input)

    counts = [0, 0]

    def first(_case, _output):
        counts[0] += 1
        return True

    async def second(_case, _output):
        counts[1] += 1
        if counts[1] == 1:
            entered.set()
            await asyncio.Event().wait()
        return True

    scorers = [CallableScorer("first", first), CallableScorer("second", second)]
    service = EvaluationService(repository.database)
    try:
        handle = await service.start(
            data("x"), CallableTarget("score-recovery", target), scorers=scorers
        )
        await asyncio.wait_for(entered.wait(), 5)
        execution_ms = (await handle.items()).items[0].duration_ms
        await handle.pause()
        item = (await handle.items()).items[0]
        assert item.output.value == "x" and len(item.scores) == 1
        assert item.duration_ms == execution_ms
        await handle.resume()
        assert (await handle.wait()).status == "completed"
        assert calls == 1 and counts == [1, 2]
    finally:
        await service.close()


async def test_real_generation_cancel_stops_producer_and_saves_partial():
    entered = asyncio.Event()
    closed = asyncio.Event()

    class Paused:
        name = "paused-job"

        async def stream(self, _messages, _tools):
            yield ModelText(text="partial")
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                closed.set()

    async with await PythonAI.create(provider=Paused()) as ai:
        handle = await ai.start_evaluation(data("x"))
        await asyncio.wait_for(entered.wait(), 5)
        assert (await handle.cancel()).status == "cancelled"
        assert closed.is_set()
        item = (await handle.items()).items[0]
        assert item.output.value == "partial" and item.output.status == "aborted"
        assert (await ai.runtime.repository.get_run(item.output.run_id)).status == "aborted"
        assert ai.runtime.production_task(item.output.run_id).done()


async def test_cold_sdk_reads_and_resumes_remaining_cases_without_repeating_done(tmp_path):
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path / 'cold.sqlite'}")

    class Counting(FakeProvider):
        def __init__(self):
            super().__init__(delay=0)
            self.calls = 0

        async def stream(self, messages, tools):
            self.calls += 1
            async for value in super().stream(messages, tools):
                yield value

    provider = Counting()
    async with await PythonAI.create(settings=settings, provider=provider) as ai:
        handle = await ai.start_evaluation(
            EvalDataset(
                name="cold",
                cases=[
                    EvalCase(id="add", input="/add 2 3", expected='[fake] 工具结果：{"sum": 5.0}')
                ],
            )
        )
        assert (await handle.wait()).status == "completed"
        job_id = handle.id
        assert provider.calls == 2
    cold = Counting()
    async with await PythonAI.create(settings=settings, provider=cold) as ai:
        handle = ai.evaluation(job_id)
        assert (await handle.report()).summary.passed == 1
        assert (await handle.items()).items[0].phase == "done"
        assert cold.calls == 0
        with pytest.raises(ServiceError):
            await ai.resume_evaluation(job_id)


async def test_job_owner_cas_rejects_second_owner_and_stale_epoch(repository):
    store = EvaluationStore(repository.database, lease_seconds=2)
    target = echo_target()
    scorer = SuccessScorer()
    spec = EvalJobSpec(
        dataset=data("x"),
        target=target.describe(),
        config=EvalConfig(),
        scorers=[scorer.describe()],
    )
    job_id, _ = await store.create(spec)
    epoch = await store.claim(job_id, "first")
    with pytest.raises(ServiceError):
        await store.claim(job_id, "second")
    from krim_agent.persistence.eval_models import EvalJobRow

    async with repository.database.sessions.begin() as session:
        row = await session.get(EvalJobRow, job_id)
        row.lease_expires_at = 0
    assert (await store.view(job_id)).can_resume
    cold = EvaluationService(repository.database)

    async def read_events():
        return [event async for event in cold.handle(job_id).events() if event]

    assert len(await asyncio.wait_for(read_events(), 2)) == 2
    second = await store.claim(job_id, "second")
    assert second > epoch
    with pytest.raises(ServiceError):
        await store.mutate(job_id, "first", epoch, 0, phase="executing")
    await store.mutate(job_id, "second", second, 0, phase="executing")
    await store.finish(job_id, "second", second, "paused")


async def test_source_binding_written_before_provider_call_and_output_recovered_read_only(tmp_path):
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path / 'binding.sqlite'}")
    provider = FakeProvider(delay=0)
    async with await PythonAI.create(settings=settings, provider=provider) as ai:
        service = ai.evaluation_service()
        target = GenerationTarget(ai)
        spec = EvalJobSpec(
            dataset=data("/add 2 3"),
            target=target.describe(),
            config=EvalConfig(),
            scorers=[SuccessScorer().describe()],
        )
        job_id, _ = await service.store.create(spec)
        epoch = await service.store.claim(job_id, service.owner)
        topic = await ai.runtime.repository.create_topic("stored")
        from krim_agent.evaluation.runner import EvalAttempt

        attempt = EvalAttempt(job_id, "0", 1)
        bindings = {"target": {"topic_id": topic.id, "request_key": attempt.idempotency_key}}
        await service.store.mutate(
            job_id, service.owner, epoch, 0, phase="executing", bindings=bindings
        )
        result = await ai.generate_text(
            "/add 2 3", topic_id=topic.id, idempotency_key=attempt.idempotency_key
        )
        assert len(result.audits) == 1
        await service.store.finish(job_id, service.owner, epoch, "paused")
        # 上次恢复只留下“待裁决”；原生产稍后已完成，不能把旧标记带入评分。
        epoch = await service.store.claim(job_id, service.owner)
        await service.store.mutate(
            job_id,
            service.owner,
            epoch,
            0,
            phase="uncertain",
            error_code="eval_resolution_required",
        )
        await service.store.finish(job_id, service.owner, epoch, "interrupted")
        resumed = await service.resume(job_id, target=target, scorers=[SuccessScorer()])
        assert (await resumed.wait()).status == "completed"
        assert (await resumed.report()).summary.passed == 1
        assert len(await ai.runtime.repository.audits(result.run.id)) == 1
        assert (await resumed.report()).results[0].output.run_id == result.run.id


async def test_migration_protects_nonempty_eval_history(repository):
    service = EvaluationService(repository.database)
    try:
        handle = await service.start(data("x"), echo_target())
        await handle.wait()
        with pytest.raises(RuntimeError, match="评估"):
            async with repository.database.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(migration_config(sync), "0019_trace_spans")
                )
        assert (await handle.report()).summary.passed == 1
        async with repository.database.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar() == "0051_checkpoint_restore_models"
    finally:
        await service.close()


async def test_real_process_crash_after_model_completion_resumes_with_zero_new_calls(tmp_path):
    import os
    import sys
    from pathlib import Path

    path = tmp_path / "crashed.sqlite"
    marker = tmp_path / "marker.json"
    script = Path(__file__).parent / "fixtures/eval_crash_worker.py"
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(script),
        str(path),
        str(marker),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=dict(os.environ),
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), 20)
    assert process.returncode == 73, (stdout + stderr).decode()
    record = json.loads(marker.read_text("utf-8"))
    # 模拟进程已消失后的合法租约接管；原活跃 owner 不能被立即抢占。
    await asyncio.sleep(10.05)

    class Counting(FakeProvider):
        calls = 0

        async def stream(self, messages, tools):
            self.calls += 1
            async for value in super().stream(messages, tools):
                yield value

    provider = Counting(delay=0)
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{path}"), provider=provider
    ) as ai:
        handle = await ai.resume_evaluation(record["job_id"], scorers=[SuccessScorer()])
        assert (await handle.wait()).status == "completed"
        assert (await handle.report()).summary.passed == 1 and provider.calls == 0
        run_id = record["bindings"]["target"]["run_id"]
        assert len(await ai.runtime.repository.audits(run_id)) == 1


async def test_cross_connection_creation_and_claim_are_atomic_and_scoped(tmp_path):
    from krim_agent.persistence import Database

    url = f"sqlite+aiosqlite:///{tmp_path / 'atomic.sqlite'}"
    first, second = Database(url), Database(url)
    await first.initialize()
    await second.initialize()
    stores = [EvaluationStore(first), EvaluationStore(second)]
    spec = EvalJobSpec(
        dataset=data("x"),
        target=echo_target().describe(),
        config=EvalConfig(),
        scorers=[SuccessScorer().describe()],
    )
    try:
        created = await asyncio.gather(*(s.create(spec, "same") for s in stores))
        assert created[0][0] == created[1][0] and sum(c[1] for c in created) == 1
        job_id = created[0][0]
        claims = await asyncio.gather(
            stores[0].claim(job_id, "one"),
            stores[1].claim(job_id, "two"),
            return_exceptions=True,
        )
        assert sum(isinstance(c, int) for c in claims) == 1
        assert sum(isinstance(c, ServiceError) for c in claims) == 1
        assert (await stores[0].view(job_id)).event_count == 2
        scoped = EvaluationStore(first, scope="api")
        other, _ = await scoped.create(spec, "same")
        assert other != job_id
        with pytest.raises(ServiceError) as missing:
            await scoped.view(job_id)
        assert missing.value.code == "eval_not_found"
    finally:
        await second.close()
        await first.close()


async def test_cold_cancel_persists_control_and_cli_resume_never_opens_active_producer(
    tmp_path, monkeypatch
):
    import argparse

    from krim_agent.evaluation.job_cli import _local
    from krim_agent.persistence import Database

    path = tmp_path / "control.sqlite"
    database = Database(f"sqlite+aiosqlite:///{path}")
    await database.initialize()
    store = EvaluationStore(database)
    spec = EvalJobSpec(
        dataset=data("x"),
        target=echo_target().describe(),
        config=EvalConfig(),
        scorers=[SuccessScorer().describe()],
    )

    async def forbidden(**_kwargs):
        raise AssertionError("读取/活跃任务拒绝不应创建或恢复生产 Runtime")

    monkeypatch.setattr(PythonAI, "create", forbidden)
    try:
        job_id, _ = await store.create(spec)
        await store.control(job_id, "cancel")
        view = await store.view(job_id)
        assert view.status == "cancelled" and view.can_resume
        epoch = await store.claim(job_id, "active")
        with pytest.raises(ServiceError) as busy:
            await _local(
                argparse.Namespace(
                    action="resume", database=str(path), scope="python", job_id=job_id
                )
            )
        assert busy.value.code == "eval_busy_or_terminal"
        await store.finish(job_id, "active", epoch, "paused")
        with pytest.raises(ValueError):
            await store.finish(job_id, "active", epoch, "completed")
    finally:
        await database.close()
