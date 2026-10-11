"""独立安装包确认同call只读重试不解除原模型生产者取消关联。"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import krim_agent
from krim_agent.config import Settings
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.sdk import FileChangeSpec, PythonAI
from krim_agent.workspace_directory import DirectoryChangeSpec
from krim_agent.workspace_mixed_batch import MixedBatchRequest, MixedBatchTarget


class Provider:
    name = "offline-model-mixed-cancel-wheel"

    def __init__(self):
        self.request = None
        self.calls = 0

    async def stream(self, _messages, _tools):
        self.calls += 1
        if self.calls == 1:
            yield ModelToolCall(
                id="wheel-model-cancel-call",
                name="workspace_mixed_start",
                arguments=self.request.model_dump(),
            )
        else:
            yield ModelText(text="原批次已取消")


async def main():
    assert "/site-packages/" in str(krim_agent.__file__)
    with TemporaryDirectory(prefix="krim-mixed-cancel-", dir="/data/tmp/root") as directory:
        root = Path(directory)
        files = root / "files"
        files.mkdir()
        provider = Provider()
        async with await PythonAI.create(
            settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
            provider=provider,
        ) as ai:

            async def allow(_definition, _context):
                return "allow"

            ai.runtime.tool_policy = allow
            service = ai.agents()
            service.register_workspace("files", files, version="v1", writable=True, default=True)
            service.register_mixed_batch_writes()
            agent = await ai.agent()
            file = await agent.preview_file_change(
                FileChangeSpec(
                    path="file.txt", operation="write", content="data", expected_sha256=None
                ),
                request_key="file",
            )
            directory = await agent.preview_directory_change(
                DirectoryChangeSpec(path="empty", operation="create", mode=0o750),
                request_key="directory",
            )
            provider.request = MixedBatchRequest(
                action="apply",
                request_key="model",
                targets=[
                    MixedBatchTarget(
                        source="file", proposal_id=file.proposal_id, expected_revision=0
                    ),
                    MixedBatchTarget(
                        source="directory", proposal_id=directory.proposal_id, expected_revision=0
                    ),
                ],
            )
            writer = service.manager.mixed_batch_writer
            parent_context = None
            entered, closed = asyncio.Event(), asyncio.Event()

            async def pause_heartbeat(_state, _worker):
                await asyncio.Event().wait()

            writer._renew = pause_heartbeat

            async def pause(definition, context):
                nonlocal parent_context
                if definition.name == "workspace_mixed_start":
                    parent_context = context
                if definition.name == "workspace_change":
                    entered.set()
                    try:
                        async with context.human_wait():
                            await asyncio.Event().wait()
                    finally:
                        closed.set()

            service._handle(agent.agent_id).runtime.tools.add_global_pre(pause)
            execution = asyncio.create_task(agent.execute("取消原模型批次", idempotency_key="run"))
            try:
                await asyncio.wait_for(entered.wait(), 10)
                assert parent_context is not None
                repeated = await writer.tool_start(provider.request.model_dump(), parent_context)
                assert repeated["producer_status"] == "running" and not closed.is_set()
                cancelled = await agent.cancel_mixed_change_batch(repeated["batch_id"])
                assert cancelled.producer_status == "cancelled" and closed.is_set()
                assert not writer._tasks and not writer._batches
                result = await asyncio.wait_for(execution, 10)
                assert result.status in {"aborted", "failed"}
                assert await agent.mixed_change_batch_by_key("model") == cancelled
                assert all(item.operation is None for item in cancelled.items)
                assert list(files.iterdir()) == [] and provider.calls == 1
                print(
                    json.dumps(
                        {
                            "package": krim_agent.__file__,
                            "same_call_read_retry": True,
                            "public_cancel_drained": True,
                            "zero_attempts": True,
                            "zero_file_effects": True,
                            "provider_calls": provider.calls,
                        }
                    )
                )
            finally:
                await service.close()
                await asyncio.gather(execution, return_exceptions=True)


asyncio.run(main())
