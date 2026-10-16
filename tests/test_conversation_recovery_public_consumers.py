"""SDK/HTTP/TCP Client/独立CLI同一Host分支与原未闭合工具组。"""

import asyncio
import json
import sys

import pytest
from test_conversation_recovery import wanted
from test_conversation_recovery_guards import actor_source
from test_tree_move_public_consumers import api_for, tcp_for

from krim_agent.client import KrimClient
from krim_agent.persistence.tree import MessageTree
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import ModelText
from krim_agent.sdk import AgentSession


async def setup(runtime):
    service, view, scope, store = await actor_source(runtime)
    handle = service._handle(view.agent_id)
    session = await handle.session()
    handle.turn = session.turn
    handle.current_run = await runtime.repository.get_run(session.current_run_id)
    return service, AgentSession(service, view.agent_id), scope, store


async def cli(url, agent_id, action, value=None, *options):
    arguments = [
        sys.executable,
        "-I",
        "-m",
        "krim_agent",
        "conversation-recoveries",
        action,
        agent_id,
    ]
    if value is not None:
        arguments.append(value)
    process = await asyncio.create_subprocess_exec(
        *arguments,
        "--url",
        url,
        *options,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        output, error = await asyncio.wait_for(process.communicate(), 15)
        assert process.returncode == 0, error.decode()
        return json.loads(output)
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()


async def test_sdk_http_tcp_cli_same_record_and_new_model_only_after_explicit_input(
    runtime, tmp_path, monkeypatch
):
    service, agent, scope, _ = await setup(runtime)
    try:
        original = await runtime.repository.message_tree(scope.topic_id)
        preview = await agent.preview_conversation_recovery()
        assert (
            preview.available
            and preview.agent_id == agent.agent_id
            and preview.session_revision is not None
        )
        request = wanted(preview, "public-recovery")
        with pytest.raises(ServiceError) as error:
            await agent.recover_conversation(request)
        assert error.value.code == "conversation_recovery_confirmation"
        assert await runtime.repository.message_tree(scope.topic_id) == original
        async with api_for(runtime, service, monkeypatch) as (app, api), tcp_for(app) as url:
            base = f"/v1/agents/{agent.agent_id}/conversation-recovery"
            assert (await api.get(base + "/preview")).json() == preview.model_dump(mode="json")
            response = await api.post(
                base + "?confirmed=true", json=request.model_dump(mode="json")
            )
            assert response.status_code == 202, response.text
            actual = await agent.conversation_recovery_by_key(request.request_key)
            assert response.json() == actual.model_dump(mode="json")
            assert (
                actual.status == "context_recovered"
                and not (await agent.preview_conversation_recovery()).available
            )

            def reads():
                with KrimClient(url) as client:
                    assert (
                        client.agent_conversation_recovery_by_key(
                            agent.agent_id, request.request_key
                        )
                        == actual
                    )
                    assert (
                        client.agent_conversation_recovery_by_key(agent.agent_id, "missing") is None
                    )
                    assert not client.agent_conversation_recovery_preview(agent.agent_id).available
                    assert (
                        client.agent_conversation_recovery_apply(
                            agent.agent_id, request, confirmed=True
                        )
                        == actual
                    )

            before = await runtime.repository.message_tree(scope.topic_id)
            await asyncio.to_thread(reads)
            assert await runtime.repository.message_tree(scope.topic_id) == before
            assert await cli(url, agent.agent_id, "by-key", request.request_key) == response.json()
            assert await cli(url, agent.agent_id, "by-key", "missing") is None
            assert not (await cli(url, agent.agent_id, "preview"))["available"]
            file = tmp_path / "request.json"
            file.write_text(request.model_dump_json())
            assert (
                await cli(url, agent.agent_id, "apply", None, "--request", str(file), "--confirm")
                == response.json()
            )
            assert await runtime.repository.message_tree(scope.topic_id) == before

            class Reader:
                name = "offline-public-conversation-reader"
                calls = 0

                async def stream(self, messages, tools):
                    self.calls += 1
                    MessageTree([]).require_complete_tools(list(messages))
                    assert any(message.id == actual.recovery_message_id for message in messages)
                    yield ModelText(text="公开恢复后新输入读取同Host记录")

            reader = Reader()
            service._handle(agent.agent_id).runtime.provider = reader
            assert reader.calls == 0
            finished = await agent.execute("明确的新只读输入", idempotency_key="new-public-input")
            assert finished.status == "completed" and reader.calls == 1
            assert finished.turn == 2 and (await service.get(agent.agent_id)).session.turn == 2
            after = await runtime.repository.message_tree(scope.topic_id)
            assert await agent.recover_conversation(request, confirmed=True) == actual
            assert await runtime.repository.message_tree(scope.topic_id) == after
        assert (
            not service.conversation_recoveries.active
            and not service.conversation_recoveries.operations
        )
    finally:
        await service.close()
