"""0040独立wheel模型混合写循环，原context/Run/call与原文件目录账本。"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy import select, text

import krim_agent
from krim_agent.config import Settings
from krim_agent.persistence.database import (
    MixedWorkspaceBatchModelSourceRow,
    WorkspaceDirectoryAttemptRow,
    WorkspaceEditAttemptRow,
)
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.sdk import FileChangeSpec, PythonAI
from krim_agent.workspace_directory import DirectoryChangeSpec
from krim_agent.workspace_mixed_batch import MixedBatchRequest, MixedBatchTarget


class Provider:
    name = "offline-model-mixed-execute-wheel"
    calls = 0
    request = None
    result = None

    async def stream(self, messages, tools):
        self.calls += 1
        if self.calls == 1:
            assert "workspace_mixed_start" in {tool.name for tool in tools}
            yield ModelToolCall(
                id="wheel-model-batch",
                name="workspace_mixed_start",
                arguments=self.request.model_dump(),
            )
        else:
            self.result = json.loads(messages[-1].content)
            assert self.result["status"] == "done"
            yield ModelText(text="完成原混合批次")


async def main():
    assert "/site-packages/" in str(krim_agent.__file__)
    with TemporaryDirectory(
        prefix="krim-model-mixed-execute-wheel-", dir="/data/tmp/root"
    ) as folder:
        root = Path(folder)
        (root / "files").mkdir()
        provider = Provider()
        async with await PythonAI.create(
            settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
            provider=provider,
        ) as ai:

            async def allow(_definition, context):
                return "allow"

            ai.runtime.tool_policy = allow
            service = ai.agents()
            service.register_workspace(
                "files", root / "files", version="v1", writable=True, default=True
            )
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
                request_key="mixed-model",
                targets=[
                    MixedBatchTarget(
                        source="file", proposal_id=file.proposal_id, expected_revision=0
                    ),
                    MixedBatchTarget(
                        source="directory", proposal_id=directory.proposal_id, expected_revision=0
                    ),
                ],
            )
            observed = []

            async def observe(definition, context):
                if definition.name in {
                    "workspace_mixed_start",
                    "workspace_change",
                    "directory_change",
                }:
                    observed.append(
                        (definition.name, context.run_id, context.origin, context.depth)
                    )

            service._handle(agent.agent_id).runtime.tools.add_global_pre(observe)
            result = await agent.execute("原上下文执行混合提案", idempotency_key="model-mixed")
            assert result.status == "completed" and result.text == "完成原混合批次"
            assert [item[3] for item in observed] == [1, 2, 2]
            assert all(item[1] == result.run_id and item[2] == "model" for item in observed)
            batch = await agent.mixed_change_batch(provider.result["batch_id"])
            assert batch.model_dump(mode="json") == provider.result
            assert (root / "files" / "file.txt").read_text() == "data" and (
                root / "files" / "empty"
            ).is_dir()
            async with ai.runtime.repository.database.sessions() as sql:
                source = await sql.get(MixedWorkspaceBatchModelSourceRow, batch.batch_id)
                assert source.run_id == result.run_id and source.call_id == "wheel-model-batch"
                assert len(list(await sql.scalars(select(WorkspaceEditAttemptRow)))) == 1
                assert len(list(await sql.scalars(select(WorkspaceDirectoryAttemptRow)))) == 1
            async with ai.runtime.repository.database.engine.connect() as sql:
                assert (
                    await sql.execute(text("SELECT version_num FROM alembic_version"))
                ).scalar_one() == "0040_mixed_batch_model_sources"
                assert not (await sql.execute(text("PRAGMA foreign_key_check"))).all()
            print(
                json.dumps(
                    {
                        "package": krim_agent.__file__,
                        "schema": "0040_mixed_batch_model_sources",
                        "same_model_context": True,
                        "original_run_call": True,
                        "real_file_directory": True,
                        "original_attempts_once": True,
                        "provider_calls": provider.calls,
                    }
                )
            )


asyncio.run(main())
