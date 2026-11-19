"""Disk references remain within their root, immutable, and cancellation-safe."""

import asyncio
import threading

import httpx
import pytest
from pydantic import SecretStr

from krim_agent.contracts import ServiceError
from krim_agent.web_access import TavilyWebAccess, WebAccessConfig

URL = "https://example.com/article"


def access(root, *, search=False):
    def handler(request):
        if search:
            return httpx.Response(200, json={"results": [
                {"title": "One", "url": URL, "content": "a" * 100},
                {"title": "Two", "url": URL, "content": "b" * 100},
            ]})
        return httpx.Response(200, json={"results": [{"url": URL, "raw_content": "x" * 10000}]})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return TavilyWebAccess(WebAccessConfig(api_key=SecretStr("test-only-key")),
                           client=client, result_root=root), client


async def test_scope_directory_symlink_is_rejected_without_external_write(tmp_path):
    root, outside = tmp_path / "results", tmp_path / "outside"
    outside.mkdir()
    web, client = access(root)
    path = root / "results" / web._scope_digest("run")
    path.parent.mkdir(mode=0o700)
    path.symlink_to(outside, target_is_directory=True)
    try:
        with pytest.raises(ServiceError, match="facts_corrupt"):
            await web.fetch(URL, scope="run")
        assert list(outside.iterdir()) == []
        assert web.snapshot("run")["results"] == []
    finally:
        await web.close()
        await client.aclose()


async def test_existing_web_root_and_nested_directories_must_be_private(tmp_path):
    broad = tmp_path / "public-root"
    broad.mkdir(mode=0o755)
    rejected_client = httpx.AsyncClient()
    try:
        with pytest.raises(ServiceError, match="web_config"):
            TavilyWebAccess(WebAccessConfig(api_key=SecretStr("test-only-key")),
                            client=rejected_client, result_root=broad)
    finally:
        await rejected_client.aclose()
    web, client = access(tmp_path / "private-root")
    try:
        (web._result_root / "results").mkdir(mode=0o755)
        with pytest.raises(ServiceError, match="web_config"):
            await web.fetch(URL, scope="run")
        assert web.snapshot("run")["results"] == []
    finally:
        await web.close()
        await client.aclose()


async def test_page_rechecks_hash_and_rejects_final_file_symlink(tmp_path):
    web, client = access(tmp_path / "results")
    try:
        result = await web.fetch(URL, scope="run")
        ref = result["results"][0]["result_ref"]
        state = web.snapshot("run")
        web.restore("run", state)
        path = web._result_path("run", ref)
        path.write_text("tampered", encoding="utf-8")
        with pytest.raises(ServiceError, match="facts_corrupt"):
            await web.fetch(result_ref=ref, scope="run")
        path.unlink()
        outside = tmp_path / "other"
        outside.write_text("secret", encoding="utf-8")
        path.symlink_to(outside)
        with pytest.raises(ServiceError, match="facts_corrupt"):
            await web.fetch(result_ref=ref, scope="run")
    finally:
        await web.close()
        await client.aclose()


async def test_search_cancellation_discards_all_unreturned_results(tmp_path, monkeypatch):
    web, client = access(tmp_path / "results", search=True)
    entered, release = threading.Event(), threading.Event()
    original = web._write_result_file
    calls = 0

    def write(path, content):
        nonlocal calls
        calls += 1
        if calls == 2:
            entered.set()
            release.wait(timeout=5)
        return original(path, content)

    monkeypatch.setattr(web, "_write_result_file", write)
    task = asyncio.create_task(web.search("query", scope="run"))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert web.snapshot("run")["results"] == []
        assert not [p for p in (tmp_path / "results").rglob("*") if p.is_file()]
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await web.close()
        await client.aclose()


async def test_restore_exactly_replaces_scope_reference_view(tmp_path):
    web, client = access(tmp_path / "results")
    try:
        state = web.snapshot("run")
        result = await web.fetch(URL, scope="run")
        await web.fetch(URL, scope="other")
        web.restore("run", state)
        assert web.snapshot("run")["results"] == []
        assert len(web.snapshot("other")["results"]) == 1
        with pytest.raises(ServiceError, match="not_found"):
            await web.fetch(result_ref=result["results"][0]["result_ref"], scope="run")
    finally:
        await web.close()
        await client.aclose()
