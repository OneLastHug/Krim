import asyncio
import json

import pytest
from conftest import EventProvider, registry_for

from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import RunRequest, ServiceError
from krim_agent.providers.types import ModelText, ModelToolCall
from krim_agent.sdk.text import RunSDK


@pytest.mark.asyncio
async def test_sdk_two_model_steps_one_real_add_and_shared_ids(tmp_path):
    async with await open_application(tmp_path / "sdk.db") as app:
        sdk = RunSDK(app.service)
        run = await sdk.start("/add 2 3", "sdk")
        await sdk.wait(run.id)
        result = await sdk.result(run.id)
        trace = await app.service.trace(run.id)
        assert result.text == "5" and result.run.status == "completed"
        assert result.model_steps == 2 and app.provider.calls == 2
        assert len(result.tools) == 1 and result.tools[0].value == {"sum": 5.0}
        assert trace.complete and {s.run_id for s in trace.spans} == {run.id}
        assert all(s.kind == "run" or s.parent_span_id == run.root_span_id for s in trace.spans)
        tool = result.tools[0]
        assert any(
            s.span_id == tool.span_id and s.attributes["step_index"] == tool.step_index
            for s in trace.spans
        )
        assert result.token_usage is None and result.usage_complete is False
        before = app.provider.calls
        await sdk.result(run.id)
        await app.service.trace(run.id)
        await app.service.events(run.id)
        assert app.provider.calls == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode,code",
    [
        ("duplicate", "duplicate_tool_call"),
        ("unknown-event", "provider_event"),
        ("too-many-tools", "tool_limit"),
        ("steps", "step_limit"),
        ("output", "output_limit"),
        ("events", "event_limit"),
    ],
)
async def test_provider_and_capacity_boundaries_stop_production(tmp_path, mode, code):
    call = ModelToolCall(id="duplicate", name="echo", arguments={"text": "real"})
    events = (
        [call, call]
        if mode == "duplicate"
        else [object()]
        if mode == "unknown-event"
        else [call, call.model_copy(update={"id": "second"})]
        if mode == "too-many-tools"
        else [ModelText(text="too large")]
        if mode == "output"
        else [call]
    )
    settings = Settings(
        max_tools_per_step=1 if mode == "too-many-tools" else 4,
        max_steps=1 if mode == "steps" else 8,
        max_output_bytes=1 if mode == "output" else 262144,
        max_events=4 if mode == "events" else 2048,
    )
    calls = []

    async def execute(args):
        calls.append(1)
        return "real"

    provider = EventProvider(events)
    async with await open_application(
        tmp_path / (mode + ".db"),
        settings=settings,
        registry=registry_for(execute),
        provider=provider,
    ) as app:
        run = await app.service.start(RunRequest(text="fixture", request_key=mode))
        await app.service.wait(run.id)
        facts = await app.service.result(run.id)
        assert facts.run.status == "failed" and facts.run.code == code
        assert provider.calls == 1 and provider.active == 0
        assert len(calls) == (1 if mode == "steps" else 0)


@pytest.mark.asyncio
async def test_idempotency_input_and_active_capacity_do_not_create_extra_runs(tmp_path):
    settings = Settings(max_active_runs=1, max_input_bytes=100)
    async with await open_application(tmp_path / "capacity.db", settings=settings) as app:
        run = await app.service.start(RunRequest(text="/slow", request_key="same"))
        again = await app.service.start(RunRequest(text="/slow", request_key="same"))
        assert again.id == run.id
        with pytest.raises(ServiceError, match="key_conflict"):
            await app.service.start(RunRequest(text="different", request_key="same"))
        with pytest.raises(ServiceError, match="capacity"):
            await app.service.start(RunRequest(text="new", request_key="new"))
        assert await app.store.by_key("new") is None
        with pytest.raises(ServiceError, match="input_limit"):
            await app.service.start(RunRequest(text="x" * 101, request_key="long"))
        assert await app.store.by_key("long") is None
        await app.service.cancel(run.id)
        assert app.provider.active == 0


@pytest.mark.asyncio
async def test_cancel_during_tool_and_repeated_close_wait_for_cleanup(tmp_path):
    entered = asyncio.Event()
    cleaned = asyncio.Event()

    async def execute(args):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0.02)
            cleaned.set()

    app = await open_application(tmp_path / "cancel.db", registry=registry_for(execute))
    run = await app.service.start(RunRequest(text="/echo wait", request_key="cancel"))
    await asyncio.wait_for(entered.wait(), 2)
    await asyncio.gather(app.service.cancel(run.id), app.service.cancel(run.id))
    assert cleaned.is_set() and (await app.service.get(run.id)).status == "aborted"
    await asyncio.gather(app.close(), app.close())
    assert app.store.closed and all(t.done() for t in app.service.tasks.values())


@pytest.mark.asyncio
async def test_provider_timeout_and_cancel_close_stream_before_return(tmp_path):
    async with await open_application(
        tmp_path / "timeout.db", settings=Settings(run_timeout=0.06)
    ) as app:
        run = await app.service.start(RunRequest(text="/slow", request_key="timeout"))
        await app.service.wait(run.id)
        assert (await app.service.get(run.id)).code == "run_timeout"
        assert app.provider.active == 0 and app.provider.streams_closed == app.provider.calls
    async with await open_application(tmp_path / "cancel-provider.db") as app:
        run = await app.service.start(RunRequest(text="/slow", request_key="cancel"))
        async with asyncio.timeout(2):
            await app.provider.entered.wait()
        await app.service.cancel(run.id)
        assert app.provider.active == 0 and app.provider.streams_closed == 1


@pytest.mark.asyncio
async def test_start_cancellation_during_write_finishes_admission_then_cleans_producer(tmp_path):
    async with await open_application(tmp_path / "startup.db") as app:
        entered = asyncio.Event()
        release = asyncio.Event()
        original = app.store.begin

        async def blocked(*args, **kwargs):
            entered.set()
            await release.wait()
            return await original(*args, **kwargs)

        app.store.begin = blocked
        job = asyncio.create_task(
            app.service.start(RunRequest(text="/slow", request_key="startup"))
        )
        await entered.wait()
        job.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await job
        run = await app.service.by_key("startup")
        assert run.status == "aborted" and all(task.done() for task in app.service.tasks.values())
        assert app.provider.active == 0


@pytest.mark.asyncio
async def test_close_racing_start_and_tool_self_close_does_not_deadlock(tmp_path):
    app = await open_application(tmp_path / "race.db")
    entered = asyncio.Event()
    release = asyncio.Event()
    original = app.store.begin

    async def blocked(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    app.store.begin = blocked
    start = asyncio.create_task(app.service.start(RunRequest(text="/slow", request_key="race")))
    await entered.wait()
    close = asyncio.create_task(app.close())
    release.set()
    await start
    await asyncio.wait_for(close, 2)
    assert app.store.closed and app.provider.active == 0

    async def self_close(args):
        await other.close()
        return "bad"

    async with await open_application(
        tmp_path / "self.db", registry=registry_for(self_close)
    ) as other:
        run = await other.service.start(RunRequest(text="/echo x", request_key="self"))
        await asyncio.wait_for(other.service.wait(run.id), 2)
        assert (await other.service.get(run.id)).code == "self_wait"


@pytest.mark.asyncio
async def test_timeout_after_provider_entry_closes_real_stream(tmp_path):
    async with await open_application(
        tmp_path / "entered-timeout.db", settings=Settings(run_timeout=1.5)
    ) as app:
        run = await app.service.start(RunRequest(text="/slow", request_key="entered"))
        async with asyncio.timeout(1):
            await app.provider.entered.wait()
        await app.service.wait(run.id)
        assert (await app.service.get(run.id)).code == "run_timeout"
        assert (
            app.provider.calls == 1
            and app.provider.streams_closed == 1
            and app.provider.active == 0
        )


@pytest.mark.asyncio
async def test_cold_facts_retain_provider_call_and_exact_arguments(tmp_path):
    async with await open_application(tmp_path / "lineage.db") as app:
        run = await app.service.start(RunRequest(text="/add 2 3", request_key="lineage"))
        await app.service.wait(run.id)
        result = await app.service.result(run.id)
        assert result.tools[0].arguments == {"left": 2.0, "right": 3.0}
        model = next(e for e in result.events if e.kind == "model_end")
        assert result.tools[0].provider_call_id == model.data["tool_calls"][0]["id"]
        assert model.data["tool_calls"][0]["arguments"] == result.tools[0].arguments


@pytest.mark.asyncio
async def test_event_capacity_failure_never_claims_complete_trace(tmp_path):
    async with await open_application(
        tmp_path / "event-trace.db", settings=Settings(max_events=4)
    ) as app:
        run = await app.service.start(RunRequest(text="/add 2 3", request_key="limit"))
        await app.service.wait(run.id)
        assert (await app.service.get(run.id)).code == "event_limit"
        assert not (await app.service.trace(run.id)).complete


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["decrease", "after-final", "partial", "complete"])
async def test_usage_events_keep_known_partial_facts_without_inventing_complete_totals(
    tmp_path, mode
):
    from krim_agent.providers.types import ModelUsage
    from krim_agent.usage import TokenUsage

    events = [ModelText(text="answer")]
    if mode == "decrease":
        events += [
            ModelUsage(usage=TokenUsage(input_tokens=10)),
            ModelUsage(usage=TokenUsage(input_tokens=9)),
        ]
    elif mode == "partial":
        events += [ModelUsage(usage=TokenUsage(input_tokens=2))]
    else:
        events += [
            ModelUsage(
                usage=TokenUsage(input_tokens=2, output_tokens=1, total_tokens=3), final=True
            )
        ]
        if mode == "after-final":
            events += [ModelText(text="late")]
    provider = EventProvider(events)
    async with await open_application(tmp_path / (mode + "-usage.db"), provider=provider) as app:
        run = await app.service.start(RunRequest(text="test", request_key=mode))
        await app.service.wait(run.id)
        facts = await app.service.result(run.id)
        assert facts.usage_complete is (mode == "complete")
        if mode == "complete":
            assert facts.token_usage == {"input_tokens": 2, "output_tokens": 1, "total_tokens": 3}
        else:
            assert all(value is None for value in facts.token_usage.values())
        if mode in {"decrease", "after-final"}:
            assert facts.run.code == "provider_event"
        if mode == "decrease":
            assert (
                next(e for e in facts.events if e.kind == "usage").data["usage"]["input_tokens"]
                == 10
            )
        assert provider.calls == 1 and provider.active == 0


@pytest.mark.asyncio
async def test_total_tool_budget_blocks_second_execution(tmp_path):
    calls = []

    async def execute(args):
        calls.append(1)
        return "real"

    class Provider(EventProvider):
        async def stream(self, messages, tools):
            self.calls += 1
            yield ModelToolCall(id=str(self.calls), name="echo", arguments={"text": "x"})

    provider = Provider([])
    async with await open_application(
        tmp_path / "total-tools.db",
        provider=provider,
        registry=registry_for(execute),
        settings=Settings(max_tool_calls=1),
    ) as app:
        run = await app.service.start(RunRequest(text="x", request_key="tools"))
        await app.service.wait(run.id)
        assert (await app.service.get(run.id)).code == "tool_limit"
        assert calls == [1] and provider.calls == 2


@pytest.mark.asyncio
async def test_tool_timeout_waits_for_actual_executor_cleanup(tmp_path):
    entered = asyncio.Event()
    cleaned = asyncio.Event()

    async def execute(args):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaned.set()

    async with await open_application(
        tmp_path / "tool-timeout.db",
        registry=registry_for(execute),
        settings=Settings(tool_timeout=0.7),
    ) as app:
        run = await app.service.start(RunRequest(text="/echo wait", request_key="tool-timeout"))
        await asyncio.wait_for(entered.wait(), 0.6)
        await app.service.wait(run.id)
        assert (await app.service.get(run.id)).code == "tool_timeout" and cleaned.is_set()


@pytest.mark.asyncio
async def test_cancelled_waiter_does_not_cancel_producer(tmp_path):
    async with await open_application(tmp_path / "waiter.db") as app:
        run = await app.service.start(RunRequest(text="/slow", request_key="waiter"))
        await app.provider.entered.wait()
        waiter = asyncio.create_task(app.service.wait(run.id))
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert (await app.service.get(run.id)).status == "running" and app.provider.active == 1
        await app.service.cancel(run.id)
        assert app.provider.active == 0


@pytest.mark.asyncio
async def test_cancellation_inside_start_transaction_waits_for_original_commit(tmp_path):
    import sqlite3

    database = tmp_path / "inside-start.db"
    async with await open_application(database) as app:
        entered = asyncio.Event()
        release = asyncio.Event()
        original = app.store._event

        async def block(conn, run_id, kind, *args, **kwargs):
            event = await original(conn, run_id, kind, *args, **kwargs)
            if kind == "run_started":
                entered.set()
                await release.wait()
            return event

        app.store._event = block
        start = asyncio.create_task(
            app.service.start(RunRequest(text="/slow", request_key="inside"))
        )
        await entered.wait()
        with sqlite3.connect("file:" + str(database) + "?mode=ro", uri=True) as reader:
            assert reader.execute("SELECT count(*) FROM runs").fetchone()[0] == 0
        start.cancel()
        await asyncio.sleep(0)
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await start
        run = await app.service.by_key("inside")
        assert run.status == "aborted" and app.provider.active == 0
        facts = await app.service.result(run.id)
        assert sum(e.kind == "run_started" for e in facts.events) == 1
        assert sum(e.kind == "terminal" for e in facts.events) == 1
        assert all(task.done() for task in app.service.tasks.values())


@pytest.mark.asyncio
async def test_cancel_initial_read_failure_still_joins_real_producer(tmp_path):
    entered, cleaning, release, cleaned = (asyncio.Event() for _ in range(4))

    async def execute(args):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await release.wait()
            cleaned.set()

    app = await open_application(
        tmp_path / "cancel-read-failure.db", registry=registry_for(execute)
    )
    original = app.store.snapshot
    cancel = None
    try:
        run = await app.service.start(RunRequest(text="/echo wait", request_key="read-failure"))
        await asyncio.wait_for(entered.wait(), 2)
        producer = app.service.tasks[run.id]
        failed = False

        async def fail_first_read(run_id):
            nonlocal failed
            if not failed:
                failed = True
                raise ServiceError("storage_unavailable", 503)
            return await original(run_id)

        app.store.snapshot = fail_first_read
        cancel = asyncio.create_task(app.service.cancel(run.id))
        await asyncio.wait_for(cleaning.wait(), 1)
        assert not cancel.done() and not producer.done() and not cleaned.is_set()
        release.set()
        with pytest.raises(ServiceError, match="storage_unavailable"):
            await cancel
        assert failed and cleaned.is_set() and producer.done()
        assert app.provider.active == 0
        assert (await app.service.get(run.id)).status == "aborted"
        await app.service.cancel(run.id)
        (tmp_path / "cleanup-observed.json").write_text(
            json.dumps(
                {
                    "initial_read_failed": failed,
                    "cleaned": cleaned.is_set(),
                    "task_done": producer.done(),
                    "provider_active": app.provider.active,
                }
            )
        )
    finally:
        release.set()
        app.store.snapshot = original
        if cancel is not None:
            await asyncio.gather(cancel, return_exceptions=True)
        await app.close()


@pytest.mark.asyncio
async def test_close_failure_joins_every_run_before_shared_resources_and_writer_unlock(tmp_path):
    from krim_agent.storage.sqlite import SQLiteStore

    entered = {name: asyncio.Event() for name in ("bad", "held")}
    cleaning, release, failed = (asyncio.Event() for _ in range(3))
    order = []

    async def execute(args):
        entered[args.text].set()
        try:
            await asyncio.Event().wait()
        finally:
            if args.text == "held":
                cleaning.set()
                await release.wait()
            order.append("clean-" + args.text)

    path = tmp_path / "close-multiple-failure.db"
    app = await open_application(path, registry=registry_for(execute))
    original_event, original_close = app.store._event, app.store.close
    original_provider_close = app.provider.close

    async def provider_close():
        order.append("provider-close")
        await original_provider_close()

    async def store_close():
        order.append("store-close")
        await original_close()

    app.provider.close, app.store.close = provider_close, store_close
    closes = []
    try:
        bad = await app.service.start(RunRequest(text="/echo bad", request_key="bad"))
        held = await app.service.start(RunRequest(text="/echo held", request_key="held"))
        await asyncio.wait_for(asyncio.gather(*(e.wait() for e in entered.values())), 2)

        async def fail_terminal(conn, run_id, kind, *args, **kwargs):
            if run_id == bad.id and kind == "terminal":
                failed.set()
                raise RuntimeError("synthetic terminal transaction failure")
            return await original_event(conn, run_id, kind, *args, **kwargs)

        app.store._event = fail_terminal
        closes = [asyncio.create_task(app.close()) for _ in range(2)]
        await asyncio.wait_for(asyncio.gather(cleaning.wait(), failed.wait()), 2)
        # 实际事务失败后，给关闭路径机会返回；held 的清理屏障仍未释放。
        done, _ = await asyncio.wait(closes, timeout=0.1)
        assert not done, "close 在另一 producer 清理完成前返回"
        assert not app.service.tasks[held.id].done()
        assert not app.provider.closed and not app.store.closed
        with pytest.raises(ServiceError, match="database_busy"):
            await SQLiteStore.open(path)
        closes[0].cancel()
        await asyncio.sleep(0)
        closes[0].cancel()
        assert not closes[0].done()
        release.set()
        outcomes = await asyncio.gather(*closes, return_exceptions=True)
        assert all(isinstance(error, ServiceError) for error in outcomes)
        assert all(task.done() for task in app.service.tasks.values())
        assert app.provider.closed and app.store.closed and app.provider.active == 0
        assert (
            order.index("clean-held") < order.index("provider-close") < order.index("store-close")
        )
        assert order.index("clean-bad") < order.index("provider-close")
        with pytest.raises(ServiceError, match="storage_unavailable"):
            await app.close()
        second = await SQLiteStore.open(path)
        await second.close()
        (tmp_path / "close-observed.json").write_text(
            json.dumps(
                {
                    "order": order,
                    "held_close_pending": not done,
                    "all_tasks_done": all(task.done() for task in app.service.tasks.values()),
                    "provider_closed": app.provider.closed,
                    "database_closed": app.store.closed,
                    "second_writer_rejected_while_held": True,
                    "second_writer_admitted_after_close": True,
                    "errors": [error.code for error in outcomes],
                }
            )
        )
    finally:
        release.set()
        app.store._event = original_event
        await asyncio.gather(*app.service.tasks.values(), *closes, return_exceptions=True)
        await asyncio.gather(app.close(), return_exceptions=True)


@pytest.mark.asyncio
async def test_cancel_stop_write_failure_and_repeated_caller_cancel_wait_for_cleanup(tmp_path):
    entered, cleaning, release, cleaned = (asyncio.Event() for _ in range(4))

    async def execute(args):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await release.wait()
            cleaned.set()

    async with await open_application(
        tmp_path / "stop-write-failure.db", registry=registry_for(execute)
    ) as app:
        run = await app.service.start(RunRequest(text="/echo hold", request_key="stop-write"))
        await asyncio.wait_for(entered.wait(), 2)
        original_stop = app.store.request_stop

        async def fail_stop(run_id):
            raise ServiceError("storage_unavailable", 503)

        app.store.request_stop = fail_stop
        caller = asyncio.create_task(app.service.cancel(run.id))
        try:
            await asyncio.wait_for(cleaning.wait(), 2)
            caller.cancel()
            await asyncio.sleep(0)
            caller.cancel()
            await asyncio.sleep(0)
            assert not caller.done() and not app.service.tasks[run.id].done()
            release.set()
            with pytest.raises(ServiceError, match="storage_unavailable"):
                await caller
            assert cleaned.is_set() and app.service.tasks[run.id].done()
            assert app.provider.active == 0 and (await app.service.get(run.id)).status == "aborted"
        finally:
            release.set()
            app.store.request_stop = original_stop
            await asyncio.gather(caller, return_exceptions=True)
