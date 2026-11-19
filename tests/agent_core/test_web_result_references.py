import asyncio
import json
import threading

import httpx
import pytest
from pydantic import SecretStr, ValidationError

from krim_agent.contracts import ServiceError
from krim_agent.tools.core import Invocation
from krim_agent.tools.web import FetchInput, web_tools
from krim_agent.web_access import TavilyWebAccess, WebAccessConfig

_SECRET = "tvly-test-secret"
_URL = "https://example.com/article"


def make_access(handler, *, root=None, key=_SECRET, endpoint="https://api.tavily.com"):
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), trust_env=False, follow_redirects=False
    )
    access = TavilyWebAccess(
        WebAccessConfig(
            api_key=SecretStr(key) if key is not None else None,
            endpoint=endpoint,
        ),
        client=client,
        result_root=root,
    )
    return access, client


def extract_response(content, *, title="Article"):
    return httpx.Response(
        200,
        json={
            "results": [{"url": _URL, "title": title, "raw_content": content}],
            "failed_results": [],
            "usage": {"credits": 1},
            "request_id": "extract-1",
        },
    )


async def test_fetch_pages_entire_body_over_two_mib_without_repeating_provider_request(tmp_path):
    calls = 0
    content = "alpha🙂\n" * 300_000

    async def handler(_request):
        nonlocal calls
        calls += 1
        return extract_response(content)

    access, client = make_access(handler, root=tmp_path / "web-results")
    try:
        first = await access.fetch(_URL, scope="run-large")
        ref = first["results"][0]["result_ref"]
        assert first["results"][0]["total_chars"] == len(content)
        assert first["results"][0]["next_offset"] == len(first["results"][0]["content"])
        assert calls == 1

        chunks = []
        offset = 0
        while True:
            page = await access.fetch(
                result_ref=ref, offset=offset, max_chars=15_000, scope="run-large"
            )
            record = page["results"][0]
            chunks.append(record["content"])
            following = record["next_offset"]
            if following is None:
                break
            assert following == offset + len(record["content"])
            offset = following

        assert "".join(chunks) == content
        assert calls == 1
    finally:
        await access.close()
        await client.aclose()


async def test_disk_result_redacts_secret_crossing_every_write_boundary(tmp_path):
    import krim_agent.web_access.tavily as module

    chunk = module._RESULT_CHUNK_BYTES
    overlap = len(_SECRET) - 1
    split = chunk - overlap
    content = ("a" * (split - 3) + _SECRET + "b" * (chunk + 5) + _SECRET
               + "c" * (chunk - 1) + _SECRET)

    async def handler(_request):
        return extract_response(content)

    access, client = make_access(handler, root=tmp_path / "redacted")
    try:
        first = await access.fetch(_URL, scope="run-secret")
        item = first["results"][0]
        ref = item["result_ref"]
        path = access._result_path("run-secret", ref)
        saved = path.read_text(encoding="utf-8")
        assert saved == content.replace(_SECRET, "[redacted]")
        assert _SECRET not in saved
        assert item["total_chars"] == len(saved)
    finally:
        await access.close()
        await client.aclose()


async def test_persistent_root_snapshot_restores_same_scope_without_paths_or_secret(tmp_path):
    calls = 0
    root = tmp_path / "persistent-web-results"

    async def handler(_request):
        nonlocal calls
        calls += 1
        return extract_response("A" * 32_000)

    access, client = make_access(handler, root=root)
    first = await access.fetch(_URL, max_chars=100, scope="run-a")
    ref = first["results"][0]["result_ref"]
    snapshot = access.snapshot("run-a")
    assert set(snapshot) == {"version", "scope_hash", "configuration_digest", "results"}
    assert set(snapshot["results"][0]) == {"id", "source", "hash"}
    assert json.dumps(snapshot)
    assert str(snapshot).find(str(root)) == -1
    assert _SECRET not in str(snapshot)
    await access.close()
    await client.aclose()

    restored, restored_client = make_access(handler, root=root)
    try:
        restored.restore("run-a", snapshot)
        page = await restored.fetch(
            result_ref=ref, offset=100, max_chars=100, scope="run-a"
        )
        assert page["results"][0]["content"] == "A" * 100
        assert calls == 1
        with pytest.raises(ServiceError) as error:
            await restored.fetch(result_ref=ref, scope="run-b")
        assert error.value.code == "not_found"
    finally:
        await restored.close()
        await restored_client.aclose()


async def test_restore_rejects_missing_or_corrupt_persistent_content_and_digest_drift(tmp_path):
    async def handler(_request):
        return extract_response("body" * 20)

    root = tmp_path / "persistent-web-results"
    access, client = make_access(handler, root=root)
    result = await access.fetch(_URL, scope="run-checkpoint")
    snapshot = access.snapshot("run-checkpoint")
    ref = result["results"][0]["result_ref"]
    path = access._result_path("run-checkpoint", ref)
    await access.close()
    await client.aclose()

    changed, changed_client = make_access(
        handler, root=root, endpoint="https://other.example"
    )
    try:
        with pytest.raises(ServiceError) as error:
            changed.restore("run-checkpoint", snapshot)
        assert error.value.code == "tool_binding_changed"
    finally:
        await changed.close()
        await changed_client.aclose()

    path.write_text("tampered", encoding="utf-8")
    restored, restored_client = make_access(handler, root=root)
    try:
        with pytest.raises(ServiceError) as error:
            restored.restore("run-checkpoint", snapshot)
        assert error.value.code == "facts_corrupt"
    finally:
        await restored.close()
        await restored_client.aclose()

    path.unlink()
    missing, missing_client = make_access(handler, root=root)
    try:
        with pytest.raises(ServiceError) as error:
            missing.restore("run-checkpoint", snapshot)
        assert error.value.code == "not_found"
    finally:
        await missing.close()
        await missing_client.aclose()


async def test_default_temp_results_are_deleted_on_close_and_not_recoverable_after_restart():
    async def handler(_request):
        return extract_response("body")

    access, client = make_access(handler)
    result = await access.fetch(_URL, scope="run-temp")
    snapshot = access.snapshot("run-temp")
    root = access._result_root
    await access.close()
    assert not root.exists()
    await client.aclose()

    restarted, restarted_client = make_access(handler)
    try:
        with pytest.raises(ServiceError) as error:
            restarted.restore("run-temp", snapshot)
        assert error.value.code == "not_found"
        with pytest.raises(ServiceError) as error:
            await restarted.fetch(result_ref=result["results"][0]["result_ref"], scope="run-temp")
        assert error.value.code == "not_found"
    finally:
        await restarted.close()
        await restarted_client.aclose()


async def test_cancel_during_disk_publication_does_not_leave_an_unpublished_artifact(
    tmp_path, monkeypatch
):
    started = threading.Event()
    release = threading.Event()

    async def handler(_request):
        return extract_response("x" * 200_000)

    root = tmp_path / "cancelled-results"
    access, client = make_access(handler, root=root)
    write_result = access._write_result_file

    def slow_write(path, content):
        started.set()
        release.wait(timeout=5)
        return write_result(path, content)

    monkeypatch.setattr(access, "_write_result_file", slow_write)
    task = asyncio.create_task(access.fetch(_URL, scope="run-cancel"))
    try:
        assert await asyncio.to_thread(started.wait, 2)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert access.snapshot("run-cancel")["results"] == []
        assert not list(root.rglob("*.tmp"))
        published = (
            [path for path in (root / "results").rglob("*") if path.is_file()]
            if (root / "results").exists()
            else []
        )
        assert published == []
    finally:
        release.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await access.close()
        await client.aclose()


async def test_http_tools_bind_references_to_invocation_run_and_preserve_fetch_url_input():
    calls = 0

    async def handler(_request):
        nonlocal calls
        calls += 1
        return extract_response("body" * 20)

    access, client = make_access(handler)
    specs = {spec.name: spec for spec in web_tools(access)}
    invocation = Invocation("run-one", "call", "span", 0, "web_fetch", "{}", "a" * 64)
    try:
        assert FetchInput.model_validate({"url": _URL}).url == _URL
        for arguments in ({}, {"url": _URL, "result_ref": "wr_" + "0" * 32}):
            with pytest.raises(ValidationError):
                FetchInput.model_validate(arguments)

        first = await specs["web_fetch"].external_executor(
            invocation, FetchInput.model_validate({"url": _URL, "max_chars": 4})
        )
        ref = first["results"][0]["result_ref"]
        second = await specs["web_fetch"].external_executor(
            invocation,
            FetchInput.model_validate({"result_ref": ref, "offset": 4, "max_chars": 4}),
        )
        assert second["results"][0]["content"] == "body"
        assert calls == 1

        other_run = Invocation("run-two", "call-2", "span-2", 0, "web_fetch", "{}", "b" * 64)
        with pytest.raises(ServiceError) as error:
            await specs["web_fetch"].external_executor(
                other_run, FetchInput.model_validate({"result_ref": ref})
            )
        assert error.value.code == "not_found"
        assert "oneOf" in FetchInput.model_json_schema()
        assert "result_ref" in specs["web_fetch"].descriptor()["output_schema"]["properties"][
            "results"
        ]["items"]["properties"]
    finally:
        await access.close()
        await client.aclose()


async def test_missing_credentials_still_fail_for_external_fetch_and_result_refs():
    calls = 0

    async def handler(_request):
        nonlocal calls
        calls += 1
        return extract_response("body")

    access, client = make_access(handler, key=None)
    try:
        for call in (
            access.fetch(_URL),
            access.fetch(result_ref="wr_" + "0" * 32, scope="run-no-key"),
        ):
            with pytest.raises(ServiceError) as error:
                await call
            assert error.value.code == "web_config"
        assert calls == 0
    finally:
        await access.close()
        await client.aclose()
