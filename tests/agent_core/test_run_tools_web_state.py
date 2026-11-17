"""Real Web reference state participates in the single-run tool checkpoint."""

import json

import httpx
from pydantic import SecretStr

from krim_agent.runtime.run_tools import open_run_tools
from krim_agent.security import LOCAL
from krim_agent.tools.core import Invocation
from krim_agent.tools.executor import ToolExecutor
from krim_agent.tools.registry import ToolRegistry
from krim_agent.tools.runtime import ToolRuntime
from krim_agent.tools.session import SessionToolScope
from krim_agent.tools.web import FetchInput, web_tools
from krim_agent.web_access import TavilyWebAccess, WebAccessConfig


async def test_web_disk_state_restores_in_new_run_tools_without_refetch(tmp_path):
    calls = []
    root = tmp_path / "web"
    url = "https://example.com/docs"

    def handler(request):
        calls.append(str(request.url))
        return httpx.Response(200, json={"results": [{"url": url, "raw_content": "a" * 40000}]})

    scope = SessionToolScope(LOCAL, "local", "session", "turn", "run")

    async def resolve(call):
        return scope

    async def policy(call):
        return "allow"

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    config = WebAccessConfig(api_key=SecretStr("test-key"))
    backend = TavilyWebAccess(config, client=client, result_root=root)
    host = ToolRuntime(ToolRegistry(web_tools(backend)), policy, web=backend)
    first = await open_run_tools(
        ToolExecutor(ToolRegistry(()), None), scope=scope, resolve=resolve, host=host
    )
    spec = first.executor.registry.get("web_fetch")
    call = Invocation("run", "call", "span", 0, spec.name, "{}", spec.fingerprint)
    result = await spec.external_executor(call, FetchInput(url=url, max_chars=100))
    ref = result["results"][0]["result_ref"]
    state = json.loads(json.dumps(first.checkpoint()))
    assert "web" in state and str(root) not in json.dumps(state)
    await first.close()
    await host.close()

    restored_backend = TavilyWebAccess(config, client=client, result_root=root)
    restored_host = ToolRuntime(
        ToolRegistry(web_tools(restored_backend)), policy, web=restored_backend
    )
    restored = await open_run_tools(
        ToolExecutor(ToolRegistry(()), None), scope=scope, resolve=resolve, host=restored_host
    )
    try:
        restored.restore(state)
        spec = restored.executor.registry.get("web_fetch")
        result = await spec.external_executor(
            call, FetchInput(result_ref=ref, offset=100, max_chars=100)
        )
        assert result["results"][0]["content"] == "a" * 100
        assert len(calls) == 1
    finally:
        await restored.close()
        await restored_host.close()
        await client.aclose()
