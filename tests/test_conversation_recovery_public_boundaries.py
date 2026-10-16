"""公开恢复严格选择/跨Scope/排队/真实HTTP回程未知/CLI本地拒绝。"""

import asyncio
import json
import sys

import httpx
import pytest
from test_conversation_recovery import wanted
from test_conversation_recovery_public_consumers import setup
from test_tree_move_public_consumers import api_for, tcp_for

from krim_agent.application.agents import AgentPromptRequest
from krim_agent.cli import main as cli_main
from krim_agent.client import KrimClient
from krim_agent.protocol.domain import ServiceError


async def test_rest_strict_query_body_private_fields_and_other_agent_cannot_recover_original(
    runtime, monkeypatch
):
    service, agent, scope, _ = await setup(runtime)
    try:
        preview = await agent.preview_conversation_recovery()
        request = wanted(preview)
        before = await runtime.repository.message_tree(scope.topic_id)
        async with api_for(runtime, service, monkeypatch) as (_, api):
            base = f"/v1/agents/{agent.agent_id}/conversation-recovery"
            for path, params in [
                ("/preview", {"owner_id": "forged"}),
                ("/preview", {"confirmed": "true"}),
                ("/by-key", {"request_key": "key", "scope": "other"}),
                ("/by-key", [("request_key", "a"), ("request_key", "a")]),
            ]:
                response = await api.get(base + path, params=params)
                assert response.status_code == 422, response.text
            for path in ("/preview", "/by-key?request_key=key"):
                assert (await api.request("GET", base + path, json={})).status_code == 422
            assert (await api.post(base, json=request.model_dump(mode="json"))).status_code == 409
            assert (
                await api.post(
                    base,
                    params=[("confirmed", "true"), ("confirmed", "true")],
                    json=request.model_dump(mode="json"),
                )
            ).status_code == 422
            for extra in (
                {"owner_id": "forged"},
                {"tool_result": {"status": "done"}},
                {"system_text": "upload"},
            ):
                assert (
                    await api.post(
                        base + "?confirmed=true", json={**request.model_dump(mode="json"), **extra}
                    )
                ).status_code == 422
            other = await service.spawn("recovery-owner")
            foreign = f"/v1/agents/{other.agent_id}/conversation-recovery"
            assert (
                await api.get(foreign + "/by-key", params={"request_key": request.request_key})
            ).json() is None
            assert (
                await api.post(foreign + "?confirmed=true", json=request.model_dump(mode="json"))
            ).status_code == 409
        assert await runtime.repository.message_tree(scope.topic_id) == before
        assert (
            not service.conversation_recoveries.operations
            and not service.conversation_recoveries.active
        )
    finally:
        await service.close()


async def test_actual_queued_agent_cannot_be_recovered_while_waiting_original_semaphore(runtime):
    service, agent, scope, _ = await setup(runtime)
    worker = None
    try:
        request = wanted(await agent.preview_conversation_recovery())
        queued = asyncio.Event()

        class Queue(asyncio.Semaphore):
            async def acquire(self):
                queued.set()
                return await super().acquire()

        service.manager._semaphore = Queue(0)
        await service.start(
            agent.agent_id, AgentPromptRequest(prompt="原预算内排队", idempotency_key="queued")
        )
        worker = service._tasks[agent.agent_id][1]
        await asyncio.wait_for(queued.wait(), 2)
        assert service._handle(agent.agent_id).pending_start
        with pytest.raises(ServiceError) as error:
            await agent.recover_conversation(request, confirmed=True)
        assert error.value.code == "agent_busy"
        with pytest.raises(ServiceError):
            await agent.preview_conversation_recovery()
        assert await agent.conversation_recovery_by_key(request.request_key) is None
        assert (await runtime.repository.message_tree(scope.topic_id)).messages[
            -1
        ].role == "assistant"
        await service.stop(agent.agent_id)
        await asyncio.gather(worker, return_exceptions=True)
    finally:
        if worker is not None and not worker.done():
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        await service.close()


async def test_lost_real_post_reply_only_gets_original_key_and_never_posts_again(
    runtime, monkeypatch
):
    service, agent, scope, _ = await setup(runtime)
    calls = []

    class Lost(httpx.BaseTransport):
        def __init__(self):
            self.original = httpx.HTTPTransport()

        def handle_request(self, request):
            calls.append(request.method)
            response = self.original.handle_request(request)
            if request.method == "POST":
                response.read()
                response.close()
                raise httpx.ReadError("SQL已提交，只有HTTP回程丢失", request=request)
            return response

        def close(self):
            self.original.close()

    try:
        request = wanted(await agent.preview_conversation_recovery(), "lost-reply")
        before = await runtime.repository.message_tree(scope.topic_id)
        async with api_for(runtime, service, monkeypatch) as (app, _), tcp_for(app) as url:

            def consume():
                with KrimClient(url) as client:
                    client._http.close()
                    client._http = httpx.Client(base_url=url, transport=Lost())
                    with pytest.raises(httpx.ReadError):
                        client.agent_conversation_recovery_apply(
                            agent.agent_id, request, confirmed=True
                        )
                    return client.agent_conversation_recovery_by_key(
                        agent.agent_id, request.request_key
                    )

            result = await asyncio.to_thread(consume)
            assert result.status == "context_recovered" and calls == ["POST", "GET"]
            after = await runtime.repository.message_tree(scope.topic_id)
            assert len(after.messages) == len(before.messages) + 1
            assert after.messages[:-1] == before.messages
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["selection", "private", "oversize", "version", "confirmation"])
def test_cli_invalid_input_fails_before_any_http(tmp_path, monkeypatch, fault):
    def forbidden(*_args, **_kwargs):
        pytest.fail("本地无效恢复参数不能发送HTTP")

    monkeypatch.setattr(httpx.Client, "send", forbidden)
    file = tmp_path / "recovery.json"
    body = {
        "source_run_id": "run",
        "expected_path_hash": "a" * 64,
        "expected_session_revision": 0,
        "request_key": "original-key",
    }
    if fault == "private":
        body["owner_id"] = "private"
    if fault == "version":
        body["expected_session_revision"] = None
    file.write_text(json.dumps(body))
    if fault == "oversize":
        file.write_bytes(b" " * 65537)
    arguments = ["conversation-recoveries", "apply", "agent", "--request", str(file)]
    if fault != "confirmation":
        arguments.append("--confirm")
    if fault == "selection":
        arguments = ["conversation-recoveries", "preview", "agent", "--request", str(file)]
    monkeypatch.setattr(sys, "argv", ["krim", *arguments])
    with pytest.raises(SystemExit) as error:
        cli_main()
    assert error.value.code == 2
