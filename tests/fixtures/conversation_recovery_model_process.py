"""原模型真实效果后强杀；显式分支后新模型只读同unknown，不重放旧生产。"""

import asyncio
import json
import os
import runpy
import sys
import time
from pathlib import Path

from sqlalchemy import select

import krim_agent
import krim_agent.workspace_missing_directory_writer as physical
from krim_agent.application.agents import AgentService
from krim_agent.config import Settings
from krim_agent.persistence.conversation_recovery import (
    ConversationRecoveryScope,
    ConversationRecoveryStore,
)
from krim_agent.persistence.database import (
    WorkspaceMissingDirectoryRestoreAttemptRow,
    WorkspaceMissingDirectoryRestoreModelSourceRow,
)
from krim_agent.persistence.tree import MessageTree
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.sdk import AgentSession, PythonAI
from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader
from krim_agent.workspace_missing_directory_writer import WorkspaceMissingDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_conversation_recovery import wanted  # noqa: E402
from test_model_missing_directory_restores import QUERY, Provider  # noqa: E402

phase, folder = sys.argv[1:3]
root = Path(folder)
if phase == "produce":
    script = Path(__file__).with_name("model_missing_directory_restore_process.py")
    sys.argv = [str(script), "produce", "effect", folder]
    runpy.run_path(str(script), run_name="__main__")
    raise SystemExit(0)


async def recover():
    metadata = json.loads((root / "metadata.json").read_text())
    provider = Provider()
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=provider,
    ) as ai:

        async def allow(_definition, _context):
            return "allow"

        ai.runtime.tool_policy = allow
        service = AgentService(ai.runtime, ai.runtime.settings)
        service.register_workspace(
            "files", root / "files", version="1", writable=True, default=True
        )
        service.register_directory_checkpoints()
        service.register_missing_directory_restores()
        service.register_missing_directory_checkpoint_restore_controls()
        async with ai.runtime.repository.database.sessions() as sql:
            attempt = await sql.get(
                WorkspaceMissingDirectoryRestoreAttemptRow, metadata["operation_id"]
            )
            deadline = attempt.lease_expires_at
        if deadline is not None:
            await asyncio.sleep(max(0, deadline - time.time()) + 0.05)
        replays = []

        def forbidden(*_args, **_kwargs):
            replays.append(True)
            raise AssertionError("恢复及新模型只读查询不能重放旧模型/Reader/Writer/namespace")

        provider.stream = forbidden
        WorkspaceDirectoryAbsenceReader.capture = forbidden
        WorkspaceTreeSnapshotReader.capture = forbidden
        WorkspaceMissingDirectoryRestoreWriter.prepare = forbidden
        WorkspaceMissingDirectoryRestoreWriter.apply = forbidden
        physical.rename_at = forbidden
        try:
            assert await service.restore_persistent() == 1, service.restore_errors
            handle = service._handle(metadata["agent_id"])
            agent = AgentSession(service, handle.agent_id)
            scope = ConversationRecoveryScope(
                handle.topic_id, handle.session_id, handle.agent_id, service.manager.owner_id
            )
            store = ConversationRecoveryStore(ai.runtime.repository)
            original_tree = await ai.runtime.repository.message_tree(handle.topic_id)
            original_events = await ai.runtime.repository.list_events(metadata["run_id"])
            original_run = await ai.runtime.repository.get_run(metadata["run_id"])
            async with ai.runtime.repository.database.sessions() as sql:
                old_sources = [
                    (item.phase, item.run_id, item.call_id)
                    for item in await sql.scalars(
                        select(WorkspaceMissingDirectoryRestoreModelSourceRow).where(
                            WorkspaceMissingDirectoryRestoreModelSourceRow.plan_id
                            == metadata["plan_id"]
                        )
                    )
                ]

            if phase == "recover":
                try:
                    await agent.execute("不可直接越过未闭合组", idempotency_key="still-blocked")
                except ServiceError as error:
                    assert error.code == "incomplete_tool_group"
                else:
                    raise AssertionError("恢复前应继续拒绝")
                preview = await store.preview(scope)
                assert preview.available and preview.source_run_id == metadata["run_id"]
                assert preview.pending_calls[0].selectors["plan_id"] == metadata["plan_id"]
                request = wanted(preview, "real-cold-recovery")
                result = await store.apply(scope, request, confirmed=True)
                (root / "recovered.json").write_text(result.model_dump_json())
            else:
                from krim_agent.conversation_recovery import ConversationRecoveryRecord

                previous = ConversationRecoveryRecord.model_validate_json(
                    (root / "recovered.json").read_text()
                )
                result = await store.by_key(scope, "real-cold-recovery")
                assert result == previous
                request = previous.request
                assert await store.apply(scope, request, confirmed=True) == previous

            changed = await ai.runtime.repository.message_tree(handle.topic_id)
            assert [item for item in changed.messages if item.id != result.recovery_message_id] == [
                item for item in original_tree.messages if item.id != result.recovery_message_id
            ]
            assert await ai.runtime.repository.list_events(metadata["run_id"]) == original_events
            assert await ai.runtime.repository.get_run(metadata["run_id"]) == original_run
            MessageTree(changed.messages).require_complete_tools(
                await ai.runtime.repository.list_messages(handle.topic_id)
            )
            current = await service.workspace_missing_directory_restore(
                handle.agent_id, metadata["plan_id"]
            )
            assert current.phase == "uncertain" and current.operation_id == metadata["operation_id"]
            assert (root / "files/tree").stat().st_ino == metadata["inode"]

            class Reader:
                name = Provider.name
                calls = 0
                value = None

                async def stream(self, messages, tools):
                    self.calls += 1
                    MessageTree([]).require_complete_tools(list(messages))
                    assert any(
                        message.role == "system"
                        and message.details.get("kind") == "conversation_recovery"
                        for message in messages
                    )
                    if self.calls == 1:
                        yield ModelToolCall(
                            id="new-read-original-plan",
                            name=QUERY,
                            arguments={"kind": "id", "plan_id": metadata["plan_id"]},
                        )
                    else:
                        self.value = json.loads(messages[-1].content)["plan"]
                        assert (
                            self.value["phase"] == "uncertain"
                            and self.value["operation_id"] == metadata["operation_id"]
                        )
                        yield ModelText(text="原工具组已隔离，原效果仍待核实；未重放执行")

            reader = Reader()
            handle.runtime.provider = reader
            finished = await agent.execute(
                "只读核实原plan，不重发执行", idempotency_key="new-read-only"
            )
            assert finished.status == "completed" and reader.calls == 2, finished
            assert finished.run_id != metadata["run_id"] and handle.turn == 2
            assert await store.by_key(scope, "real-cold-recovery") == result
            before = await ai.runtime.repository.message_tree(handle.topic_id)
            assert await store.apply(scope, request, confirmed=True) == result
            assert await ai.runtime.repository.message_tree(handle.topic_id) == before
            assert await ai.runtime.repository.list_events(metadata["run_id"]) == original_events
            async with ai.runtime.repository.database.sessions() as sql:
                new_sources = [
                    (item.phase, item.run_id, item.call_id)
                    for item in await sql.scalars(
                        select(WorkspaceMissingDirectoryRestoreModelSourceRow).where(
                            WorkspaceMissingDirectoryRestoreModelSourceRow.plan_id
                            == metadata["plan_id"]
                        )
                    )
                ]
            assert new_sources == old_sources and not replays
            assert (root / "files/tree/nested/nul").read_bytes() == b"\0\xff"
            print(
                json.dumps(
                    {
                        "pid": os.getpid(),
                        "plan_id": metadata["plan_id"],
                        "operation_id": metadata["operation_id"],
                        "original_run_id": metadata["run_id"],
                        "new_run_id": finished.run_id,
                        "new_provider_calls": reader.calls,
                        "recovery_message_id": result.recovery_message_id,
                        "turn": handle.turn,
                        "source_unchanged": True,
                        "replays": len(replays),
                        "unknown_preserved": True,
                        "inode": metadata["inode"],
                        "natural_effect_lease_expiry": deadline is not None,
                        "package": str(krim_agent.__file__),
                    }
                ),
                flush=True,
            )
        finally:
            await service.close()


asyncio.run(recover())
