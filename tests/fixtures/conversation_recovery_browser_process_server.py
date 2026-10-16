"""真实模型效果后SIGKILL；独立浏览器服务及正常重启进程均保留原来源。"""

import asyncio
import json
import os
import signal
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory

import uvicorn
from pydantic import SecretStr
from sqlalchemy import select, update
from starlette.background import BackgroundTask
from starlette.responses import JSONResponse

import krim_agent
import krim_agent.api.server as server_module
import krim_agent.workspace_missing_directory_writer as physical
from krim_agent.application.agents import AgentService
from krim_agent.client import KrimClient
from krim_agent.config import Settings
from krim_agent.conversation_recovery import ConversationRecoveryRecord
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.database import (
    AgentHandleRow,
    AgentSessionRow,
    WorkspaceMissingDirectoryRestoreAttemptRow,
    WorkspaceMissingDirectoryRestoreModelSourceRow,
)
from krim_agent.persistence.tree import MessageTree
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.sdk import AgentSession, PythonAI
from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader
from krim_agent.workspace_missing_directory_writer import WorkspaceMissingDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_conversation_recovery_public_consumers import cli  # noqa: E402
from test_model_missing_directory_restores import QUERY, Provider  # noqa: E402

if os.environ.get("KRIM_REQUIRE_INSTALLED_PACKAGE") == "1":
    assert "/site-packages/" in str(krim_agent.__file__), krim_agent.__file__


async def sources(repository, plan_id):
    async with repository.database.sessions() as sql:
        rows = await sql.execute(
            select(WorkspaceMissingDirectoryRestoreModelSourceRow.__table__)
            .where(WorkspaceMissingDirectoryRestoreModelSourceRow.plan_id == plan_id)
            .order_by(WorkspaceMissingDirectoryRestoreModelSourceRow.phase)
        )
        return json.loads(json.dumps([dict(row) for row in rows.mappings()], default=str))


async def prepare(root):
    producer = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(Path(__file__).with_name("model_missing_directory_restore_process.py")),
        "produce",
        "effect",
        str(root),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
    try:
        line = await asyncio.wait_for(producer.stdout.readline(), 25)
        if not line:
            _, error = await producer.communicate()
            raise AssertionError(error.decode())
        proof = json.loads(line)
        assert proof["pid"] == producer.pid and proof["origin"] == "model"
        producer.kill()
        _, error = await asyncio.wait_for(producer.communicate(), 5)
        assert producer.returncode == -9, error.decode()
        metadata = json.loads((root / "metadata.json").read_text())
        async with database.sessions() as sql, sql.begin():
            await sql.execute(
                update(AgentHandleRow)
                .where(AgentHandleRow.agent_id == metadata["agent_id"])
                .values(lease_expires_at=time.time() - 1)
            )
            attempt = await sql.get(
                WorkspaceMissingDirectoryRestoreAttemptRow, metadata["operation_id"]
            )
            deadline = attempt.lease_expires_at
            session = await sql.get(AgentSessionRow, metadata["agent_id"])
            topic_id = session.topic_id
        if deadline is not None:
            await asyncio.sleep(max(0, deadline - time.time()) + 0.05)
        repository = KrimRepository(database)
        tree = await repository.message_tree(topic_id)
        baseline = {
            "messages": [message.model_dump(mode="json") for message in tree.messages],
            "events": [
                event.model_dump(mode="json")
                for event in await repository.list_events(metadata["run_id"])
            ],
            "sources": await sources(repository, metadata["plan_id"]),
            "producer": proof,
            "natural_effect_lease_expiry": deadline is not None,
        }
        (root / "original-facts.json").write_text(json.dumps(baseline, ensure_ascii=False))
    finally:
        if producer.returncode is None:
            producer.kill()
            await producer.communicate()
        await database.close()


async def worker(root, phase):
    metadata = json.loads((root / "metadata.json").read_text())
    baseline = json.loads((root / "original-facts.json").read_text())
    replays = []

    def forbidden(*_args, **_kwargs):
        replays.append(True)
        raise AssertionError("浏览器会话恢复不得重放旧Provider/Reader/Writer/namespace")

    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}",
        api_key=SecretStr("browser-test-key"),
    )
    async with await PythonAI.create(settings=settings, provider=Provider()) as ai:

        async def allow(_definition, _context):
            return "allow"

        ai.runtime.tool_policy = allow
        service = AgentService(ai.runtime, settings)
        service.register_workspace(
            "files", root / "files", version="1", writable=True, default=True
        )
        service.register_directory_checkpoints()
        service.register_missing_directory_restores()
        service.register_missing_directory_checkpoint_restore_controls()
        ai.runtime.provider.stream = forbidden
        WorkspaceDirectoryAbsenceReader.capture = forbidden
        WorkspaceTreeSnapshotReader.capture = forbidden
        WorkspaceMissingDirectoryRestoreWriter.prepare = forbidden
        WorkspaceMissingDirectoryRestoreWriter.apply = forbidden
        physical.rename_at = forbidden
        assert await service.restore_persistent() == 1, service.restore_errors
        restored_events = [
            event.model_dump(mode="json")
            for event in await ai.runtime.repository.list_events(metadata["run_id"])
        ]
        if phase == "cold":
            # 原恢复器真实追加一个interrupted终帧；已落盘的每个旧帧必须完整保持。
            assert restored_events[: len(baseline["events"])] == baseline["events"]
            assert len(restored_events) == len(baseline["events"]) + 1
            assert restored_events[-1]["kind"] == "end"
            assert restored_events[-1]["data"]["status"] == "interrupted"
            baseline["events"] = restored_events
            (root / "original-facts.json").write_text(json.dumps(baseline, ensure_ascii=False))
        else:
            assert restored_events == baseline["events"]
        agent = AgentSession(service, metadata["agent_id"])
        handle = service._handle(agent.agent_id)

        class Reader:
            name = Provider.name
            calls = 0

            async def stream(self, messages, tools):
                self.calls += 1
                MessageTree([]).require_complete_tools(list(messages))
                assert any(
                    message.role == "system"
                    and message.details.get("kind") == "conversation_recovery"
                    and message.details.get("origin") == "host"
                    for message in messages
                )
                if self.calls == 1:
                    yield ModelToolCall(
                        id="browser-new-read",
                        name=QUERY,
                        arguments={"kind": "id", "plan_id": metadata["plan_id"]},
                    )
                else:
                    assert self.calls == 2
                    plan = json.loads(messages[-1].content)["plan"]
                    assert (
                        plan["phase"] == "uncertain"
                        and plan["operation_id"] == metadata["operation_id"]
                    )
                    yield ModelText(text="浏览器显式新轮次只读原unknown，旧生产0重放")

        reader = Reader()
        if phase == "cold":
            handle.runtime.provider = reader
        server_module.AgentService = lambda *_args, **_kwargs: service
        app = server_module.create_app(runtime=ai.runtime)
        web = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=8883, log_level="error"))

        @app.get("/_test/conversation-process")
        async def facts(request_key: str = "", consumers: bool = False):
            repository = ai.runtime.repository
            tree = await repository.message_tree(handle.topic_id)
            actual = {message.id: message.model_dump(mode="json") for message in tree.messages}
            assert all(actual[message["id"]] == message for message in baseline["messages"])
            assert [
                event.model_dump(mode="json")
                for event in await repository.list_events(metadata["run_id"])
            ] == baseline["events"]
            assert await sources(repository, metadata["plan_id"]) == baseline["sources"]
            assert (await repository.get_run(metadata["run_id"])).status == "interrupted"
            plan = await agent.missing_directory_checkpoint_restore(metadata["plan_id"])
            assert plan.phase == "uncertain"
            record = None
            if request_key:
                record = await agent.conversation_recovery_by_key(request_key)
                assert record is not None
            elif phase == "warm":
                saved = ConversationRecoveryRecord.model_validate_json(
                    (root / "public-record.json").read_text()
                )
                record = await agent.conversation_recovery_by_key(saved.request.request_key)
                assert record == saved
            if consumers:
                assert record is not None

                def read():
                    with KrimClient("http://127.0.0.1:8883", "browser-test-key") as client:
                        return client.agent_conversation_recovery_by_key(
                            agent.agent_id, record.request.request_key
                        )

                assert await asyncio.to_thread(read) == record
                assert await cli(
                    "http://127.0.0.1:8883",
                    agent.agent_id,
                    "by-key",
                    record.request.request_key,
                ) == record.model_dump(mode="json")
                (root / "public-record.json").write_text(record.model_dump_json())
            assert (root / "files/tree").stat().st_ino == metadata["inode"]
            assert (root / "files/tree/nested/nul").read_bytes() == b"\0\xff"
            assert not replays
            view = await service.get(agent.agent_id)
            return {
                "agent_id": agent.agent_id,
                "name": view.name,
                "topic_id": handle.topic_id,
                "pid": os.getpid(),
                "producer_pid": baseline["producer"]["pid"],
                "phase": phase,
                "recovery_message_id": record.recovery_message_id if record else None,
                "operation_id": metadata["operation_id"],
                "plan_id": metadata["plan_id"],
                "source_run_id": metadata["run_id"],
                "original_sources_same": True,
                "original_messages_same": True,
                "original_events_same": True,
                "original_run_status": "interrupted",
                "workspace_phase": plan.phase,
                "inode": metadata["inode"],
                "binary_hex": "00ff",
                "replays": len(replays),
                "new_query_calls": reader.calls,
                "natural_effect_lease_expiry": baseline["natural_effect_lease_expiry"],
                "consumers_same": consumers,
                "turn": view.turn,
                "package": str(krim_agent.__file__),
            }

        @app.post("/_test/conversation-process/restart")
        async def restart():
            assert phase == "cold" and (root / "public-record.json").exists()

            def shutdown():
                web.should_exit = True

            return JSONResponse({"pid": os.getpid()}, background=BackgroundTask(shutdown))

        try:
            await web.serve()
        finally:
            await service.close()


async def supervise(root):
    await prepare(root)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for name in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(name, stop.set)
    for phase in ("cold", "warm"):
        child = await asyncio.create_subprocess_exec(
            sys.executable, "-I", __file__, "--worker", str(root), phase
        )
        waiter = asyncio.create_task(child.wait())
        stopper = asyncio.create_task(stop.wait())
        try:
            await asyncio.wait([waiter, stopper], return_when=asyncio.FIRST_COMPLETED)
            if stop.is_set():
                return
            assert child.returncode == 0
        finally:
            stopper.cancel()
            await asyncio.gather(stopper, return_exceptions=True)
            if child.returncode is None:
                child.terminate()
                try:
                    await asyncio.wait_for(waiter, 5)
                except TimeoutError:
                    child.kill()
                    await child.wait()


if len(sys.argv) > 1 and sys.argv[1] == "--worker":
    asyncio.run(worker(Path(sys.argv[2]), sys.argv[3]))
else:
    with TemporaryDirectory(
        prefix="krim-conversation-browser-kill-", dir="/data/tmp/root"
    ) as folder:
        asyncio.run(supervise(Path(folder)))
