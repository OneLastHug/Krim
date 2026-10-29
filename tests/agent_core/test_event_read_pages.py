import asyncio

import pytest

from krim_agent.composition import open_application
from krim_agent.contracts import RunRequest, ServiceError


async def queued_run(app, key: str):
    run, created = await app.store.begin(
        RunRequest(text="read event pages", request_key=key), {"test": True}
    )
    assert created
    return run


@pytest.mark.asyncio
async def test_event_pages_cover_boundaries_and_terminal_stream_without_full_snapshots(
    tmp_path, monkeypatch
):
    async with await open_application(tmp_path / "event-pages.sqlite") as app:
        run = await queued_run(app, "event-pages")
        for index in range(65):
            await app.store.append(
                run.id, "progress", {"index": index}, event_id=f"progress-{index}"
            )

        async def forbidden_snapshot(*_args, **_kwargs):
            raise AssertionError("event reads must not load full snapshots")

        monkeypatch.setattr(app.store, "snapshot", forbidden_snapshot)
        page = await app.store.event_page(run.id, 0, 64)
        assert page.status == "queued" and page.event_count == 66
        first = await app.service.events(run.id, 0, 64)
        second = await app.service.events(run.id, 64, 64)
        assert [item.seq for item in first] == list(range(64))
        assert [item.seq for item in second] == [64, 65]
        assert await app.service.events(run.id, 66) == []

        for cursor, limit in [(-1, 1), (67, 1), (0, 0), (0, 65)]:
            with pytest.raises(ServiceError) as rejected:
                await app.service.events(run.id, cursor, limit)
            assert rejected.value.code == "invalid_cursor"
            assert rejected.value.status == 416

        await app.store.finish(run.id, "completed")
        streamed = [item async for item in app.service.stream(run.id)]
        assert [item.seq for item in streamed] == list(range(67))
        assert streamed[-1].kind == "terminal"
        assert streamed[-1].data["status"] == "completed"


@pytest.mark.asyncio
async def test_stream_waits_on_empty_page_then_delivers_new_event(tmp_path, monkeypatch):
    async with await open_application(tmp_path / "event-wait.sqlite") as app:
        run = await queued_run(app, "event-wait")

        async def forbidden_snapshot(*_args, **_kwargs):
            raise AssertionError("stream polling must not load full snapshots")

        monkeypatch.setattr(app.store, "snapshot", forbidden_snapshot)
        stream = app.service.stream(run.id, from_seq=1)
        waiting = asyncio.create_task(anext(stream))
        try:
            await asyncio.sleep(0.06)
            assert not waiting.done()
            appended = await app.store.append(
                run.id, "progress", {"ready": True}, event_id="ready"
            )
            assert await asyncio.wait_for(waiting, 1) == appended
        finally:
            if not waiting.done():
                waiting.cancel()
                await asyncio.gather(waiting, return_exceptions=True)
            await stream.aclose()


@pytest.mark.asyncio
async def test_cancelling_event_page_waits_for_read_transaction_to_close(tmp_path, monkeypatch):
    async with await open_application(tmp_path / "event-cancel.sqlite") as app:
        run = await queued_run(app, "event-cancel")
        entered, release = asyncio.Event(), asyncio.Event()
        original = app.store._event_page
        transaction = None

        async def paused(conn, run_id, from_seq, limit):
            nonlocal transaction
            page = await original(conn, run_id, from_seq, limit)
            transaction = conn.get_transaction()
            entered.set()
            await release.wait()
            return page

        monkeypatch.setattr(app.store, "_event_page", paused)
        reader = asyncio.create_task(app.store.event_page(run.id, 0, 64))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            assert transaction is not None and transaction.is_active
            reader.cancel()
            await asyncio.sleep(0)
            assert not reader.done()
            assert app.store.lock.locked()
            assert transaction.is_active

            release.set()
            with pytest.raises(asyncio.CancelledError):
                await reader
            assert not app.store.lock.locked()
            assert not transaction.is_active
        finally:
            release.set()
            if not reader.done():
                await asyncio.gather(reader, return_exceptions=True)
