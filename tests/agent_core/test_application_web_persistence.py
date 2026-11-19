"""Application-owned Web storage survives close/reopen without refetch."""

import json
import stat

import httpx
import pytest
from pydantic import SecretStr

from krim_agent.composition import open_application
from krim_agent.context.serialization import fingerprint
from krim_agent.contracts import RunRequest
from krim_agent.runtime.run_tools import open_run_tools
from krim_agent.security import LOCAL
from krim_agent.tools.executor import ToolExecutor
from krim_agent.tools.registry import ToolRegistry
from krim_agent.tools.runtime import ToolRuntimeConfig
from krim_agent.tools.session import SessionToolScope
from krim_agent.web_access import TavilyWebAccess, WebAccessConfig


@pytest.mark.parametrize("explicit_root", [False, True])
async def test_application_reopen_restores_same_run_web_reference(
    tmp_path, monkeypatch, explicit_root,
):
    import krim_agent.tools.runtime as runtime_module

    database = tmp_path / "application.sqlite"
    root = (tmp_path / "explicit-results" if explicit_root else
            database.with_name(database.name + ".web-results"))
    calls = []
    url = "https://example.com/article"
    content = "persisted Unicode 正文🙂\n" * 10000

    def handler(request):
        calls.append(str(request.url))
        return httpx.Response(200, json={
            "results": [{"url": url, "raw_content": content}],
        })

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    def backend(config, *, result_root=None):
        return TavilyWebAccess(config, client=client, result_root=result_root)

    monkeypatch.setattr(runtime_module, "TavilyWebAccess", backend)
    config = ToolRuntimeConfig(
        web=WebAccessConfig(api_key=SecretStr("persistence-test-key")),
        web_result_root=root if explicit_root else None,
    )
    scope = None

    async def resolve(call):
        assert scope is not None and call.run_id == scope.run_id
        return scope

    try:
        async with await open_application(database, tool_config=config) as app:
            run, _ = await app.store.begin(
                RunRequest(text="persist web reference", request_key="web-persistence"), {},
            )
            scope = SessionToolScope(LOCAL, "local", "session", "turn", run.id)
            tools = await open_run_tools(
                ToolExecutor(ToolRegistry(()), None), scope=scope, resolve=resolve,
                host=app.tool_runtime,
            )
            try:
                result = await app.tool_runtime.web.fetch(url, max_chars=100, scope=run.id)
                ref = result["results"][0]["result_ref"]
                state = json.loads(json.dumps(tools.checkpoint()))
                await app.store.write_checkpoint(run.id, state, fingerprint(state))
                assert app.tool_runtime.web._result_root == root
                assert root.is_dir() and not root.is_symlink()
                assert stat.S_IMODE(root.stat().st_mode) == 0o700
                assert str(root) not in json.dumps(state)
                await app.store.finish(run.id, "completed")
            finally:
                await tools.close()
        assert root.exists() and len(calls) == 1

        async with await open_application(database, tool_config=config) as reopened:
            persisted = await reopened.store.snapshot(run.id)
            assert persisted.run.id == scope.run_id
            stored, digest = await reopened.store.read_checkpoint(run.id)
            assert fingerprint(stored) == digest
            tools = await open_run_tools(
                ToolExecutor(ToolRegistry(()), None), scope=scope, resolve=resolve,
                host=reopened.tool_runtime,
            )
            try:
                tools.restore(stored)
                page = await reopened.tool_runtime.web.fetch(
                    result_ref=ref, offset=100, max_chars=15000, scope=run.id,
                )
                assert page["results"][0]["content"] == content[100:15100]
                assert page["results"][0]["total_chars"] == len(content)
                assert len(calls) == 1
                assert reopened.tool_runtime.web._result_root == root
            finally:
                await tools.close()
    finally:
        await client.aclose()
