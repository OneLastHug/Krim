"""真实独立进程：文件发布前/后崩溃，重新供给根目录后核实原操作。"""

import asyncio
import json
import os
import sys
from pathlib import Path

from sqlalchemy import text

import krim_agent
from krim_agent.config import Settings
from krim_agent.providers import FakeProvider
from krim_agent.sdk import FileChangeSpec, PythonAI, WorkspaceChangeAction
from krim_agent.workspace_writer import WorkspaceWriter


class CountingProvider(FakeProvider):
    def __init__(self):
        super().__init__(delay=0)
        self.calls = 0

    async def stream(self, messages, tools):
        self.calls += 1
        async for part in super().stream(messages, tools):
            yield part


async def allow(_definition, _context):
    return "allow"


async def main():
    database_path, root, mode, metadata_path = sys.argv[1:5]
    metadata = Path(metadata_path)
    provider = CountingProvider()
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{database_path}"),
        provider=provider,
    ) as ai:
        ai.runtime.tool_policy = allow
        service = ai.agents()
        service.register_workspace("local", root, version="v1", default=True, writable=True)
        if mode.startswith("crash") or mode == "create":
            agent = await ai.agent(persistent=True)
            proposal = await agent.preview_file_change(
                FileChangeSpec(
                    path="file.txt",
                    operation="write",
                    expected_sha256=(await agent.read_file("file.txt")).sha256,
                    content="new\n",
                ),
                request_key="preview",
            )
            request = WorkspaceChangeAction(
                proposal_id=proposal.proposal_id,
                action="apply",
                expected_revision=0,
                request_key="apply",
            )
            await asyncio.to_thread(
                metadata.write_text,
                json.dumps(
                    {
                        "agent_id": agent.agent_id,
                        "proposal_id": proposal.proposal_id,
                        "request": request.model_dump(),
                    }
                ),
            )
            original = WorkspaceWriter.apply
            if mode.startswith("crash"):

                def crash(self, *args, **kwargs):
                    if mode == "crash-after":
                        original(self, *args, **kwargs)
                    os._exit(23)

                WorkspaceWriter.apply = crash
            result = await agent.change_file(request)
            assert result.status == "done"
        else:
            values = json.loads(await asyncio.to_thread(metadata.read_text))
            assert await service.restore_persistent() == 1
            agent = ai.agent_session(values["agent_id"])
            request = WorkspaceChangeAction.model_validate(values["request"])
            result = await agent.change_file(request)
            if mode == "recover":
                assert result.status == "uncertain"
                result = await agent.reconcile_file_change(result.operation_id)
            assert result.status in {"done", "rejected"}
            proposal = await agent.file_change(values["proposal_id"])
            frozen = await service.workspace_edits.get(
                f"session:{agent.session_id}", proposal.proposal_id
            )
            assert frozen.prepared.before.data == b"old\n" and frozen.prepared.after == b"new\n"
            if result.status == "done":
                undone = await agent.change_file(
                    WorkspaceChangeAction(
                        proposal_id=proposal.proposal_id,
                        action="undo",
                        expected_revision=proposal.revision,
                        request_key="undo",
                    )
                )
                assert undone.status == "done"
            for attempt in await agent.file_change_history(proposal.proposal_id):
                assert not (await agent.finalize_file_change(attempt.operation_id)).cleanup_pending
            assert (await asyncio.to_thread(Path(root, "file.txt").read_text)) == "old\n"
            assert not await asyncio.to_thread(lambda: list(Path(root).glob("._krim-tx-*")))
        async with ai.runtime.repository.database.engine.connect() as connection:
            head = (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar()
        print(
            json.dumps(
                {
                    "status": result.status,
                    "calls": provider.calls,
                    "operation_id": result.operation_id,
                    "package": krim_agent.__file__,
                    "migration": head,
                }
            )
        )


asyncio.run(main())
