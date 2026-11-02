"""真实协议、精确审批、SDK与HTTP只读目录联合验证。"""

import asyncio

import httpx
import pytest
from fixtures.web_tool_server import fixture

from krim_agent.contracts import ApprovalReply
from krim_agent.sdk.text import RunSDK


async def approved_run(application, text, key):
    sdk = RunSDK(application.service)
    run = await sdk.start(text, key)
    waiter = asyncio.create_task(sdk.wait(run.id))
    approvals = []
    async with asyncio.timeout(20):
        while not waiter.done():
            pending = await application.service.approvals(run.id)
            for request in pending:
                approvals.append(request)
                reply = ApprovalReply(
                    request_key="approval-" + key,
                    allow=True,
                    **{
                        field: request[field]
                        for field in ("call_id", "arguments_hash", "tool_hash")
                    },
                )
                await application.service.reply_approval(run.id, request["request_id"], reply)
            await asyncio.sleep(0.01)
        await waiter
    return await sdk.result(run.id), approvals


@pytest.mark.parametrize("override", [False, True])
async def test_real_http_and_stdio_paths_approval_and_catalog(tmp_path, override):
    async with fixture(tmp_path, override=override) as (api, application, counts):
        async with (
            api.router.lifespan_context(api),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api),
                base_url="http://127.0.0.1",
                headers={"Authorization": "Bearer browser-test-key"},
            ) as client,
        ):
            catalog = await client.get("/v2/targets/demo/tools")
            assert catalog.status_code == 200
            tools = {item["name"]: item for item in catalog.json()}
            active = tools["web_search"]
            assert active["source"]["kind"] == ("mcp" if override else "http")
            assert (
                active["fingerprint"]
                == application.service.bindings["demo"]
                .loop.executor.registry.get("web_search")
                .fingerprint
            )
            assert "fixture-web-key" not in catalog.text
            assert "127.0.0.1" not in catalog.text
            result, approvals = await approved_run(application, "protocol text", "search")
            assert result.run.status == "completed" and len(approvals) == 1
            assert approvals[0]["tool_hash"] == active["fingerprint"]
            assert approvals[0]["source"] == active["source"]
            assert approvals[0]["risk"] == active["risk"]
            assert result.tools[0].executor_started and result.tools[0].result_available
            assert "protocol text" in result.text
            assert counts["search"] == (0 if override else 1)
            read, approvals = await approved_run(
                application, "fetch https://example.com/article", "read"
            )
            assert read.run.status == "completed" and len(approvals) == 1
            assert "HTTP article" in read.text and counts["extract"] == 1
            unauthorized = await client.get(
                "/v2/targets/demo/tools", headers={"Authorization": "bad"}
            )
            assert unauthorized.status_code == 401
            assert (await client.get("/v2/targets/unknown/tools")).status_code == 404


async def test_mcp_only_does_not_require_http_key_or_fallback(tmp_path):
    async with fixture(tmp_path, override=True, no_http_key=True) as (_, application, counts):
        result, approvals = await approved_run(application, "MCP-only", "mcp-only")
        assert result.run.status == "completed" and len(approvals) == 1
        assert counts == {"search": 0, "extract": 0}
        await application.tool_runtime.connections[0].close()
        failed, _ = await approved_run(application, "closed", "closed")
        assert failed.run.status == "failed" and failed.tools[0].code == "mcp_unavailable"
        assert counts == {"search": 0, "extract": 0}
