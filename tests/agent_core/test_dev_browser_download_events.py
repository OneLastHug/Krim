"""Download event metadata never becomes a host path or untrusted URL fetch."""

from types import SimpleNamespace

import pytest
from test_dev_browser_session import ORIGIN, session

from krim_agent.dev_browser import BrowserError


class Download:
    def __init__(self, url):
        self.url = url
        self.cancelled = False
        self.suggested_filename = "../../private/secret"

    async def cancel(self):
        self.cancelled = True


class Pending:
    def __init__(self, download):
        self.download = download

    async def __aenter__(self):
        async def value():
            return self.download

        return SimpleNamespace(value=value())

    async def __aexit__(self, *args):
        return False


@pytest.mark.asyncio
async def test_click_download_cancels_native_save_and_reads_bounded_relative_target():
    reads = []

    async def read(path, maximum):
        reads.append((path, maximum))
        return b"fixed download"

    context, _, adapter = await session(read_download=read)
    download = Download(ORIGIN + "/file?test=1")
    context.page.expect_download = lambda **kwargs: Pending(download)
    try:
        artifact = await adapter.click_download("#download")
        assert download.cancelled
        assert reads == [("/file?test=1", 1048576)]
        assert artifact.data == b"fixed download"
        assert "secret" not in repr(artifact.metadata)
        assert "filename" not in artifact.metadata
    finally:
        await adapter.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        "https://foreign.example/download",
        "blob:" + ORIGIN + "/blob",
        "data:text/plain,secret",
        ORIGIN + "/.krim-preview/session",
    ],
)
async def test_download_event_cannot_choose_external_or_privileged_source(url):
    async def read(*args):
        pytest.fail("denied URL must never reach downloader")

    context, _, adapter = await session(read_download=read)
    download = Download(url)
    context.page.expect_download = lambda **kwargs: Pending(download)
    with pytest.raises(BrowserError, match="browser_download_denied"):
        await adapter.click_download("#download")
    assert download.cancelled and context.closed.is_set()


@pytest.mark.asyncio
async def test_download_disabled_without_authorized_reader():
    context, _, adapter = await session()
    with pytest.raises(BrowserError, match="browser_download_unavailable"):
        await adapter.click_download("#download")
    assert context.closed.is_set()
