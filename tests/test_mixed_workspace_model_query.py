"""模型读取原混合账本、原Run/call上下文、最终Hook回程守卫及零新attempt。"""

import json

import pytest
from pydantic import ValidationError
from test_mixed_workspace_batch_fences import attempt_counts

from krim_agent.application.agents import AgentService
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.sdk import AgentSession
from krim_agent.tools import ShortCircuit, ToolContext, ToolError
from krim_agent.tools.workspace_mixed_queries import mixed_batch_query_tool
from krim_agent.workspace_change import FileChangeSpec
from krim_agent.workspace_directory import DirectoryChangeSpec
from krim_agent.workspace_mixed_batch import (
    MixedBatchQuery,
    MixedBatchQueryResult,
    MixedBatchRequest,
    MixedBatchTarget,
)


async def allow(_definition, _context):
    return "allow"


async def setup(runtime, root, *, enabled=True):
    runtime.tool_policy = allow
    service = AgentService(runtime, runtime.settings)
    service.register_workspace("files", root, version="v1", writable=True, default=True)
    if enabled:
        service.register_mixed_batch_queries()
    agent = AgentSession(service, (await service.spawn("default")).agent_id)
    file = await agent.preview_file_change(
        FileChangeSpec(path="file.txt", operation="write", content="data", expected_sha256=None),
        request_key="file",
    )
    directory = await agent.preview_directory_change(
        DirectoryChangeSpec(path="empty", operation="create", mode=0o750), request_key="directory"
    )
    request = MixedBatchRequest(
        action="apply",
        request_key="batch",
        targets=[
            MixedBatchTarget(source="file", proposal_id=file.proposal_id, expected_revision=0),
            MixedBatchTarget(
                source="directory", proposal_id=directory.proposal_id, expected_revision=0
            ),
        ],
    )
    accepted = await agent.start_mixed_change_batch(request)
    batch = await agent.wait_mixed_change_batch(accepted.batch_id)
    assert batch.status == "done", batch.model_dump_json()
    return service, agent, batch


async def test_model_loop_reads_original_sdk_batch_key_history_with_original_run_call(
    runtime, tmp_path
):
    class Provider:
        name = "offline-mixed-query"
        calls = 0
        batch_id = None

        async def stream(self, messages, tools):
            self.calls += 1
            assert "workspace_mixed_query" in {tool.name for tool in tools}
            if self.calls == 1:
                yield ModelToolCall(
                    id="mixed-get-call",
                    name="workspace_mixed_query",
                    arguments={"kind": "batch", "batch_id": self.batch_id},
                )
            elif self.calls == 2:
                original = json.loads(messages[-1].content)
                assert (
                    original["batch"]["batch_id"] == self.batch_id
                    and original["batch"]["status"] == "done"
                )
                yield ModelToolCall(
                    id="mixed-key-call",
                    name="workspace_mixed_query",
                    arguments={"kind": "by_key", "request_key": "batch"},
                )
            elif self.calls == 3:
                assert json.loads(messages[-1].content)["batch"]["batch_id"] == self.batch_id
                yield ModelToolCall(
                    id="mixed-history-call",
                    name="workspace_mixed_query",
                    arguments={"kind": "history", "limit": 1},
                )
            else:
                history = json.loads(messages[-1].content)["history"]
                assert (
                    history["total"] == 1
                    and history["items"][0]["batch_id"] == self.batch_id
                    and history["items"][0]["done_count"] == 2
                )
                yield ModelText(text="已读取原混合批次")

    provider = Provider()
    runtime.provider = provider
    service, agent, batch = await setup(runtime, tmp_path)
    provider.batch_id = batch.batch_id
    try:
        before = await attempt_counts(runtime.repository.database)
        result = await agent.execute("读取已有混合结果", idempotency_key="query-turn")
        assert result.status == "completed" and result.text == "已读取原混合批次", (
            result.model_dump_json()
        )
        assert provider.calls == 4 and await attempt_counts(
            runtime.repository.database
        ) == before == (1, 1)
        messages = await runtime.repository.run_messages(result.run_id)
        calls = [message for message in messages if message.role == "tool"]
        assert [item.details["call_id"] for item in calls] == [
            "mixed-get-call",
            "mixed-key-call",
            "mixed-history-call",
        ]
        assert all(
            item.details["name"] == "workspace_mixed_query" and not item.details["is_error"]
            for item in calls
        )
        assert (await agent.mixed_change_batch(batch.batch_id)) == batch
    finally:
        await service.close()


async def test_explicit_enable_keeps_old_frozen_agent_tools_and_requires_workspace(
    runtime, tmp_path
):
    service, old, batch = await setup(runtime, tmp_path, enabled=False)
    try:
        old_tools = {item.name for item in service._handle(old.agent_id).runtime.tools.definitions}
        assert "workspace_mixed_query" not in old_tools
        service.register_mixed_batch_queries()
        assert {
            item.name for item in service._handle(old.agent_id).runtime.tools.definitions
        } == old_tools
        with pytest.raises(ServiceError) as unavailable:
            await service._workspace_call(
                old.agent_id, "workspace_mixed_query", {"kind": "batch", "batch_id": batch.batch_id}
            )
        assert unavailable.value.code == "tool_not_found"
        current = AgentSession(service, (await service.spawn("default")).agent_id)
        assert "workspace_mixed_query" in {
            item.name for item in service._handle(current.agent_id).runtime.tools.definitions
        }
        with pytest.raises(ServiceError) as foreign:
            await service._workspace_call(
                current.agent_id,
                "workspace_mixed_query",
                {"kind": "batch", "batch_id": batch.batch_id},
            )
        assert foreign.value.code == "workspace_mixed_batch_not_found"
    finally:
        await service.close()


@pytest.mark.parametrize(
    "payload",
    [
        {"kind": "batch"},
        {"kind": "batch", "batch_id": "id", "request_key": "key"},
        {"kind": "by_key"},
        {"kind": "history", "batch_id": "id"},
        {"kind": "history", "limit": 33},
        {"kind": "history", "limit": True},
        {"kind": "batch", "batch_id": "id", "cursor": "bad"},
        {"kind": "by_key", "request_key": "x\x7f"},
        {"kind": "history", "owner_id": "evil"},
        {"kind": "history", "scope": "session:other"},
        {"kind": "history", "fence": {}},
        {"kind": "history", "root": "/tmp"},
    ],
)
def test_query_strict_selector_and_internal_fields_are_rejected(payload):
    with pytest.raises(ValidationError):
        MixedBatchQuery.model_validate(payload)


@pytest.mark.parametrize("hook", ["short", "post"])
async def test_hook_cannot_forge_or_rewrite_original_query(runtime, tmp_path, hook):
    service, agent, batch = await setup(runtime, tmp_path)
    try:
        registry = service._handle(agent.agent_id).runtime.tools
        if hook == "short":

            async def pre(definition, _context):
                if definition.name == "workspace_mixed_query":
                    return ShortCircuit(
                        MixedBatchQueryResult(kind="by_key", batch=None).model_dump(mode="json")
                    )
                return None

            registry.add_global_pre(pre)
        else:

            async def post(definition, value, _context):
                if definition.name == "workspace_mixed_query":
                    value["batch"]["request_key"] = "invented"
                return value

            registry.add_global_post(post)
        with pytest.raises(ServiceError) as forged:
            await service._workspace_call(
                agent.agent_id, "workspace_mixed_query", {"kind": "by_key", "request_key": "batch"}
            )
        assert forged.value.code == "workspace_mixed_batch_query_unverified"
        assert await attempt_counts(runtime.repository.database) == (1, 1)
    finally:
        await service.close()


async def test_query_backend_missing_and_closed_or_forged_model_run_are_rejected(runtime, tmp_path):
    definition = mixed_batch_query_tool()
    context = ToolContext(runtime.tools, origin="host")
    with pytest.raises(ToolError) as missing:
        await definition.executor(MixedBatchQuery(kind="history"), context)
    assert missing.value.code == "workspace_mixed_batch_backend_missing"
    service, agent, _batch = await setup(runtime, tmp_path)
    try:
        handle = service._handle(agent.agent_id)
        for run_id in ("local", "missing"):
            context = ToolContext(
                handle.runtime.tools,
                topic_id=handle.topic_id,
                agent_id=agent.agent_id,
                session_id=agent.session_id,
                workspace=handle.definition.workspace,
                workspace_mixed_queries=service.manager.mixed_queries,
                run_id=run_id,
                origin="model",
            )
            with pytest.raises(ToolError) as denied:
                await service.manager.mixed_queries.tool_query({"kind": "history"}, context)
            assert denied.value.code == "workspace_run_mismatch"
    finally:
        await service.close()
