"""项目外wheel的显式模型原批次消费；只读循环不增加原attempt。"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy import func, select

import krim_agent
from krim_agent.config import Settings
from krim_agent.persistence.database import WorkspaceDirectoryAttemptRow, WorkspaceEditAttemptRow
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.sdk import FileChangeSpec, PythonAI
from krim_agent.workspace_directory import DirectoryChangeSpec
from krim_agent.workspace_mixed_batch import MixedBatchRequest, MixedBatchTarget


class Provider:
    name = "offline-mixed-wheel-query"
    calls = 0
    identifier = None

    async def stream(self, messages, tools):
        self.calls += 1
        assert "workspace_mixed_query" in {item.name for item in tools}
        if self.calls == 1:
            yield ModelToolCall(
                id="wheel-query-call",
                name="workspace_mixed_query",
                arguments={"kind": "batch", "batch_id": self.identifier},
            )
        elif self.calls == 2:
            value = json.loads(messages[-1].content)
            assert (
                value["batch"]["batch_id"] == self.identifier and value["batch"]["status"] == "done"
            )
            yield ModelToolCall(
                id="wheel-history-call",
                name="workspace_mixed_query",
                arguments={"kind": "history", "limit": 1},
            )
        else:
            value = json.loads(messages[-1].content)
            assert value["history"]["items"][0]["batch_id"] == self.identifier
            yield ModelText(text="已读取原结果")


async def main():
    assert "/site-packages/" in str(krim_agent.__file__)
    with TemporaryDirectory(prefix="krim-mixed-query-wheel-", dir="/data/tmp/root") as folder:
        root = Path(folder)
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
            service.register_mixed_batch_queries()
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
            accepted = await agent.start_mixed_change_batch(
                MixedBatchRequest(
                    action="apply",
                    request_key="mixed",
                    targets=[
                        MixedBatchTarget(
                            source="file", proposal_id=file.proposal_id, expected_revision=0
                        ),
                        MixedBatchTarget(
                            source="directory",
                            proposal_id=directory.proposal_id,
                            expected_revision=0,
                        ),
                    ],
                )
            )
            original = await agent.wait_mixed_change_batch(accepted.batch_id)
            assert original.status == "done"
            provider.identifier = original.batch_id
            result = await agent.execute("只读原混合结果", idempotency_key="wheel-read")
            assert result.status == "completed" and result.text == "已读取原结果"
            assert await agent.mixed_change_batch(original.batch_id) == original
            messages = await ai.runtime.repository.run_messages(result.run_id)
            tool_messages = [message for message in messages if message.role == "tool"]
            assert [message.details["call_id"] for message in tool_messages] == [
                "wheel-query-call",
                "wheel-history-call",
            ]
            async with ai.runtime.repository.database.sessions() as sql:
                assert (
                    await sql.scalar(select(func.count()).select_from(WorkspaceEditAttemptRow)) == 1
                )
                assert (
                    await sql.scalar(select(func.count()).select_from(WorkspaceDirectoryAttemptRow))
                    == 1
                )
            print(
                json.dumps(
                    {
                        "package": krim_agent.__file__,
                        "real_local_model_loop": True,
                        "original_run_call": True,
                        "same_batch": True,
                        "read_zero_new_attempts": True,
                        "provider_calls": provider.calls,
                    }
                )
            )


asyncio.run(main())
