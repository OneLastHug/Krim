"""真实模型混合写工具复用原Run/call/context/审批/deadline，零后台重放。"""

import asyncio
import json
from threading import Event

import pytest
from test_mixed_workspace_batch_fences import attempt_counts
from test_workspace_changes import wait_event

from krim_agent.application.agents import AgentService
from krim_agent.persistence.database import (
    MixedWorkspaceBatchModelSourceRow,
    WorkspaceDirectoryAttemptRow,
    WorkspaceEditAttemptRow,
)
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.sdk import AgentSession
from krim_agent.tools import ShortCircuit
from krim_agent.workspace_change import FileChangeSpec
from krim_agent.workspace_directory import DirectoryChangeSpec
from krim_agent.workspace_mixed_batch import MixedBatchRequest, MixedBatchTarget
from krim_agent.workspace_writer import WorkspaceWriter


class Provider:
    name = "offline-model-mixed-write"
    calls = 0
    request = None
    result = None

    async def stream(self, messages, tools):
        self.calls += 1
        if self.calls == 1:
            assert "workspace_mixed_start" in {item.name for item in tools}
            yield ModelToolCall(
                id="parent-mixed-call",
                name="workspace_mixed_start",
                arguments=self.request.model_dump(),
            )
        else:
            self.result = json.loads(messages[-1].content)
            yield ModelText(text="已读取混合原回执")


async def setup(runtime, root):
    provider = Provider()
    runtime.provider = provider

    async def allow(_definition, _context):
        return "allow"

    runtime.tool_policy = allow
    service = AgentService(runtime, runtime.settings)
    service.register_workspace("files", root, version="v1", writable=True, default=True)
    service.register_mixed_batch_writes()
    agent = AgentSession(service, (await service.spawn("default")).agent_id)
    file = await agent.preview_file_change(
        FileChangeSpec(path="file.txt", operation="write", content="data", expected_sha256=None),
        request_key="file",
    )
    directory = await agent.preview_directory_change(
        DirectoryChangeSpec(path="empty", operation="create", mode=0o750), request_key="directory"
    )
    provider.request = MixedBatchRequest(
        action="apply",
        request_key="model",
        targets=[
            MixedBatchTarget(source="file", proposal_id=file.proposal_id, expected_revision=0),
            MixedBatchTarget(
                source="directory", proposal_id=directory.proposal_id, expected_revision=0
            ),
        ],
    )
    return service, agent, provider


async def test_real_model_write_shares_context_and_persistent_run_parent_call(runtime, tmp_path):
    service, agent, provider = await setup(runtime, tmp_path)
    observations = []
    try:

        async def observe(definition, context):
            if definition.name in {"workspace_mixed_start", "workspace_change", "directory_change"}:
                observations.append(
                    (
                        definition.name,
                        context.run_id,
                        context.origin,
                        context.depth,
                        context.call_id,
                        context.budget_account_ids,
                        len(context.execution_timeouts),
                    )
                )

        service._handle(agent.agent_id).runtime.tools.add_global_pre(observe)
        result = await agent.execute("执行原混合提案", idempotency_key="mixed-turn")
        assert result.status == "completed", result.model_dump_json()
        assert provider.result["status"] == "done" and provider.calls == 2
        batch = await agent.mixed_change_batch(provider.result["batch_id"])
        assert batch.model_dump(mode="json") == provider.result
        assert (tmp_path / "file.txt").read_text() == "data" and (tmp_path / "empty").is_dir()
        assert [value[0] for value in observations] == [
            "workspace_mixed_start",
            "workspace_change",
            "directory_change",
        ]
        assert all(value[1] == result.run_id and value[2] == "model" for value in observations)
        assert [value[3] for value in observations] == [1, 2, 2]
        assert observations[1][5] == observations[0][5] == observations[2][5]
        assert observations[1][6] > observations[0][6] and observations[2][6] > observations[0][6]
        async with runtime.repository.database.sessions() as sql:
            source = await sql.get(MixedWorkspaceBatchModelSourceRow, batch.batch_id)
            assert source.run_id == result.run_id and source.call_id == "parent-mixed-call"
            for item, table in zip(
                batch.items, (WorkspaceEditAttemptRow, WorkspaceDirectoryAttemptRow), strict=True
            ):
                original = await sql.get(table, item.operation.operation_id)
                assert original.run_id == result.run_id and original.call_id == source.call_id
        audits = await runtime.repository.audits(result.run_id)
        assert len(audits) == 3 and len({audit.call_id for audit in audits}) == 3
        assert await attempt_counts(runtime.repository.database) == (1, 1)
        assert not service.manager.mixed_batch_writer._tasks
    finally:
        await service.close()


async def test_model_source_specific_deny_is_partial_and_no_authorization_promotion(
    runtime, tmp_path
):
    service, agent, provider = await setup(runtime, tmp_path)
    try:

        async def policy(definition, context):
            assert context.origin == "model"
            return "deny" if definition.name == "directory_change" else "allow"

        service._handle(agent.agent_id).runtime.tool_policy = policy
        result = await agent.execute("执行受限混合提案", idempotency_key="deny-turn")
        assert result.status == "completed"
        assert provider.result["status"] == "partial"
        assert provider.result["items"][0]["status"] == "done"
        assert provider.result["items"][1]["operation"] is None
        assert provider.result["items"][1]["error_code"] == "permission_denied"
        assert (tmp_path / "file.txt").exists() and not (tmp_path / "empty").exists()
        assert await attempt_counts(runtime.repository.database) == (1, 0)
    finally:
        await service.close()


@pytest.mark.parametrize("hook", ["short", "redirect", "post", "origin", "source_call"])
async def test_model_nested_hook_cannot_rewrite_or_invent_original_operation(
    runtime, tmp_path, hook
):
    service, agent, provider = await setup(runtime, tmp_path)
    try:
        registry = service._handle(agent.agent_id).runtime.tools

        async def pre(definition, context):
            if definition.name == "workspace_change":
                if hook == "short":
                    return ShortCircuit({"status": "done", "operation_id": "invented"})
                if hook == "redirect":
                    context.arguments["request_key"] = "other-key"
                if hook == "origin":
                    context.origin = "host"
                if hook == "source_call":
                    context.source_call_id = "other-call"
            return None

        async def post(definition, value, _context):
            if hook == "post" and definition.name == "workspace_change":
                value["status"] = "rejected"
            return value

        registry.add_global_pre(pre)
        registry.add_global_post(post)
        result = await agent.execute("原混合提案", idempotency_key="hook-turn")
        assert result.status == "completed"
        if hook == "post":
            assert provider.result["status"] == "done"
            assert provider.result["items"][0]["operation"]["status"] == "done"
        else:
            assert provider.result["status"] == "partial"
            assert provider.result["items"][0]["operation"] is None
            assert not (tmp_path / "file.txt").exists()
        assert (tmp_path / "empty").is_dir()
    finally:
        await service.close()


async def test_model_cancel_closes_persistent_parent_before_unstarted_children(runtime, tmp_path):
    service, agent, provider = await setup(runtime, tmp_path)
    entered = asyncio.Event()
    try:

        async def wait(definition, context):
            if definition.name == "workspace_change":
                entered.set()
                async with context.human_wait():
                    await asyncio.Event().wait()

        service._handle(agent.agent_id).runtime.tools.add_global_pre(wait)
        task = asyncio.create_task(agent.execute("原混合提案", idempotency_key="cancel-turn"))
        await asyncio.wait_for(entered.wait(), 10)
        await agent.stop()
        result = await asyncio.wait_for(task, 10)
        assert result.status in {"aborted", "failed"}
        batch = await agent.mixed_change_batch_by_key("model")
        assert batch.producer_status == "cancelled" and all(
            item.operation is None for item in batch.items
        )
        assert await attempt_counts(runtime.repository.database) == (0, 0)
        assert not (tmp_path / "file.txt").exists() and not (tmp_path / "empty").exists()
    finally:
        await service.close()


async def test_model_stop_closes_parent_before_waiting_for_admitted_writer(
    runtime, tmp_path, monkeypatch
):
    service, agent, _provider = await setup(runtime, tmp_path)
    entered, release = Event(), Event()
    parent_closed = asyncio.Event()
    original = WorkspaceWriter.apply
    execution = None
    stopping = None
    try:

        def blocked(writer, prepared, **kwargs):
            entered.set()
            assert release.wait(10)
            return original(writer, prepared, **kwargs)

        monkeypatch.setattr(WorkspaceWriter, "apply", blocked)
        mixed_writer = service.manager.mixed_batch_writer
        original_cancel = mixed_writer.store.cancel

        async def cancel(scope, batch_id):
            result = await original_cancel(scope, batch_id)
            parent_closed.set()
            return result

        monkeypatch.setattr(mixed_writer.store, "cancel", cancel)
        execution = asyncio.create_task(agent.execute("原混合提案", idempotency_key="writer-stop"))
        await wait_event(entered)
        stopping = asyncio.create_task(agent.stop())
        await asyncio.wait_for(parent_closed.wait(), 3)
        assert not release.is_set() and not stopping.done()
        release.set()
        await stopping
        result = await execution
        assert result.status in {"aborted", "failed"}
        assert await attempt_counts(runtime.repository.database) == (1, 0)
        assert not (tmp_path / "file.txt").exists() and not (tmp_path / "empty").exists()
    finally:
        release.set()
        if stopping is not None:
            await asyncio.gather(stopping, return_exceptions=True)
        if execution is not None:
            await asyncio.gather(execution, return_exceptions=True)
        await service.close()
