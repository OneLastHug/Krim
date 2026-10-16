"""真正Model效果后强杀，公开SDK/HTTP/TCP/CLI恢复并新轮次只读同原unknown。"""

import asyncio
import json
import os
import runpy
import sys
import time
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import select

import krim_agent
import krim_agent.api.server as server
import krim_agent.workspace_missing_directory_writer as physical
from krim_agent.application.agents import AgentService
from krim_agent.client import KrimClient
from krim_agent.config import Settings
from krim_agent.persistence.database import (
    WorkspaceMissingDirectoryRestoreAttemptRow,
    WorkspaceMissingDirectoryRestoreModelSourceRow,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.sdk import AgentSession, PythonAI
from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader
from krim_agent.workspace_missing_directory_writer import WorkspaceMissingDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_conversation_recovery import wanted  # noqa: E402
from test_conversation_recovery_public_consumers import cli  # noqa: E402
from test_model_missing_directory_restores import QUERY, Provider  # noqa: E402
from test_tree_move_public_consumers import tcp_for  # noqa: E402

phase, folder = sys.argv[1:3]
root = Path(folder)
if phase == "produce":
    script = Path(__file__).with_name("model_missing_directory_restore_process.py")
    sys.argv = [str(script), "produce", "effect", folder]
    runpy.run_path(str(script), run_name="__main__")
    raise SystemExit(0)


async def main():
    metadata = json.loads((root / "metadata.json").read_text())
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=Provider(),
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
            raise AssertionError("公共恢复禁止旧Provider/Reader/Writer/namespace重放")

        ai.runtime.provider.stream = forbidden
        WorkspaceDirectoryAbsenceReader.capture = forbidden
        WorkspaceTreeSnapshotReader.capture = forbidden
        WorkspaceMissingDirectoryRestoreWriter.prepare = forbidden
        WorkspaceMissingDirectoryRestoreWriter.apply = forbidden
        physical.rename_at = forbidden
        try:
            assert await service.restore_persistent() == 1, service.restore_errors
            agent = AgentSession(service, metadata["agent_id"])
            handle = service._handle(agent.agent_id)
            if phase == "readback":
                from krim_agent.conversation_recovery import ConversationRecoveryRecord

                saved = ConversationRecoveryRecord.model_validate_json(
                    (root / "public-record.json").read_text()
                )
                before = await ai.runtime.repository.message_tree(handle.topic_id)
                with patch.object(server, "AgentService", lambda *_args, **_kwargs: service):
                    app = server.create_app(runtime=ai.runtime)
                async with tcp_for(app) as url:

                    def read():
                        with KrimClient(url) as client:
                            assert (
                                client.agent_conversation_recovery_by_key(
                                    agent.agent_id, saved.request.request_key
                                )
                                == saved
                            )
                            assert (
                                client.agent_conversation_recovery_apply(
                                    agent.agent_id, saved.request, confirmed=True
                                )
                                == saved
                            )

                    await asyncio.to_thread(read)
                    assert await cli(
                        url, agent.agent_id, "by-key", saved.request.request_key
                    ) == saved.model_dump(mode="json")
                assert await ai.runtime.repository.message_tree(handle.topic_id) == before
                assert not replays
                print(
                    json.dumps(
                        {
                            "pid": os.getpid(),
                            "warm_readback": True,
                            "recovery_message_id": saved.recovery_message_id,
                            "replays": 0,
                            "new_query_calls": 0,
                            "package": str(krim_agent.__file__),
                        }
                    ),
                    flush=True,
                )
                return
            old_tree = await ai.runtime.repository.message_tree(handle.topic_id)
            old_events = await ai.runtime.repository.list_events(metadata["run_id"])
            async with ai.runtime.repository.database.sessions() as sql:
                old_sources = [
                    (row.phase, row.run_id, row.call_id)
                    for row in await sql.scalars(
                        select(WorkspaceMissingDirectoryRestoreModelSourceRow).where(
                            WorkspaceMissingDirectoryRestoreModelSourceRow.plan_id
                            == metadata["plan_id"]
                        )
                    )
                ]
            try:
                await agent.execute("原未闭合上下文仍拒绝", idempotency_key="before-recovery")
            except ServiceError as error:
                assert error.code == "incomplete_tool_group"
            else:
                raise AssertionError("公开恢复前不能跳过原完整组校验")
            preview = await agent.preview_conversation_recovery()
            assert preview.available
            request = wanted(preview, "public-cold-recovery")
            with patch.object(server, "AgentService", lambda *_args, **_kwargs: service):
                app = server.create_app(runtime=ai.runtime)
            async with tcp_for(app) as url:

                def apply():
                    with KrimClient(url) as client:
                        assert client.agent_conversation_recovery_preview(agent.agent_id) == preview
                        return client.agent_conversation_recovery_apply(
                            agent.agent_id, request, confirmed=True
                        )

                result = await asyncio.to_thread(apply)
                assert await agent.conversation_recovery_by_key(request.request_key) == result
                assert await cli(
                    url, agent.agent_id, "by-key", request.request_key
                ) == result.model_dump(mode="json")
                assert not (await cli(url, agent.agent_id, "preview"))["available"]
                changed = await ai.runtime.repository.message_tree(handle.topic_id)
                assert changed.messages[:-1] == old_tree.messages

                class Reader:
                    name = Provider.name
                    calls = 0

                    async def stream(self, messages, tools):
                        self.calls += 1
                        assert any(message.id == result.recovery_message_id for message in messages)
                        if self.calls == 1:
                            yield ModelToolCall(
                                id="public-new-query",
                                name=QUERY,
                                arguments={"kind": "id", "plan_id": metadata["plan_id"]},
                            )
                        else:
                            view = json.loads(messages[-1].content)["plan"]
                            assert (
                                view["phase"] == "uncertain"
                                and view["operation_id"] == metadata["operation_id"]
                            )
                            yield ModelText(text="公开恢复后只读原unknown，旧执行0重放")

                reader = Reader()
                handle.runtime.provider = reader
                final = await agent.execute("只读原操作", idempotency_key="public-new-read")
                assert final.status == "completed" and reader.calls == 2 and handle.turn == 2, final
                after = await ai.runtime.repository.message_tree(handle.topic_id)
                request_file = root / "recovery-request.json"
                request_file.write_text(request.model_dump_json())
                assert await cli(
                    url, agent.agent_id, "apply", None, "--request", str(request_file), "--confirm"
                ) == result.model_dump(mode="json")
                assert await ai.runtime.repository.message_tree(handle.topic_id) == after
            assert await ai.runtime.repository.list_events(metadata["run_id"]) == old_events
            async with ai.runtime.repository.database.sessions() as sql:
                current_sources = [
                    (row.phase, row.run_id, row.call_id)
                    for row in await sql.scalars(
                        select(WorkspaceMissingDirectoryRestoreModelSourceRow).where(
                            WorkspaceMissingDirectoryRestoreModelSourceRow.plan_id
                            == metadata["plan_id"]
                        )
                    )
                ]
            assert current_sources == old_sources and not replays
            (root / "public-record.json").write_text(result.model_dump_json())
            assert (root / "files/tree").stat().st_ino == metadata["inode"] and (
                root / "files/tree/nested/nul"
            ).read_bytes() == b"\0\xff"
            print(
                json.dumps(
                    {
                        "pid": os.getpid(),
                        "agent_id": agent.agent_id,
                        "operation_id": metadata["operation_id"],
                        "plan_id": metadata["plan_id"],
                        "old_run_id": metadata["run_id"],
                        "new_run_id": final.run_id,
                        "recovery_message_id": result.recovery_message_id,
                        "public_consumers_same": True,
                        "original_sources_same": True,
                        "original_events_same": True,
                        "replays": len(replays),
                        "new_query_calls": reader.calls,
                        "inode": metadata["inode"],
                        "binary_hex": "00ff",
                        "natural_effect_lease_expiry": deadline is not None,
                        "package": str(krim_agent.__file__),
                    }
                ),
                flush=True,
            )
        finally:
            await service.close()


asyncio.run(main())
