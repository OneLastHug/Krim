"""Streaming bounds without Playwright or a live application."""

import asyncio

import pytest

from krim_agent.dev_browser import BrowserError
from krim_agent.dev_browser.downloads import collect_download


@pytest.mark.asyncio
async def test_chunks_are_bounded_before_aggregation():
    async def chunks():
        yield b"12"
        yield b"34"

    assert await collect_download(chunks(), 4) == b"1234"
    with pytest.raises(BrowserError, match="browser_artifact_limit"):
        await collect_download(chunks(), 3)


@pytest.mark.asyncio
async def test_overflow_does_not_pull_following_chunk():
    pulled = []

    async def chunks():
        pulled.append(1)
        yield b"12345"
        pulled.append(2)
        yield b"secret"

    with pytest.raises(BrowserError, match="browser_artifact_limit"):
        await collect_download(chunks(), 4)
    assert pulled == [1]


@pytest.mark.asyncio
async def test_cancellation_unwinds_source():
    entered, closed = asyncio.Event(), asyncio.Event()

    async def chunks():
        try:
            entered.set()
            await asyncio.Event().wait()
            yield b"unreachable"
        finally:
            closed.set()

    task = asyncio.create_task(collect_download(chunks(), 4))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed.is_set()


@pytest.mark.asyncio
async def test_stream_has_no_text_or_header_coercion():
    async def chunks():
        yield "not bytes"

    with pytest.raises(BrowserError, match="browser_artifact_limit"):
        await collect_download(chunks(), 100)
