"""真实旧0029安装包创建历史，当前安装包保留历史并执行新命令。"""

import asyncio
import json
import sys
from pathlib import Path

from sqlalchemy import text

import krim_agent
from krim_agent.persistence import Database, KrimRepository
from krim_agent.protocol.domain import ChatRequest


async def main():
    path, mode = sys.argv[1:]
    database = Database(f"sqlite+aiosqlite:///{path}")
    await database.initialize()
    repository = KrimRepository(database)
    state_path = Path(path + ".json")
    try:
        if mode == "old":
            topic = await repository.create_topic("0029 history")
            run, _ = await repository.begin_run(
                topic.id, ChatRequest(text="before", idempotency_key="original")
            )
            await repository.add_message(run, "assistant", "retained")
            await repository.finish(run.id, "completed", None, [])
            state = {"topic": topic.id, "run": run.id}
            await asyncio.to_thread(state_path.write_text, json.dumps(state))
        else:
            from krim_agent.execution import CommandSpec, LinuxSandboxRunner
            from krim_agent.execution.records import CommandRequest
            from krim_agent.execution.service import DurableCommandService
            from krim_agent.persistence.commands import CommandStore

            state = json.loads(await asyncio.to_thread(state_path.read_text))
            messages = await repository.list_messages(state["topic"])
            assert [item.content for item in messages] == ["before", "retained"]
            original, created = await repository.begin_run(
                state["topic"], ChatRequest(text="before", idempotency_key="original")
            )
            assert not created and original.id == state["run"]
            async with DurableCommandService(
                CommandStore(database), LinuxSandboxRunner(), owned=True
            ) as service:
                handle = await service.start(
                    "topic:" + state["topic"],
                    CommandRequest(
                        request_key="new",
                        command=CommandSpec(argv=["/usr/bin/python3", "-c", "print('0030')"]),
                    ),
                )
                result = await handle.wait()
                assert result.outcome == "completed" and await handle.output() == (b"0030\n", b"")
        async with database.engine.connect() as connection:
            head = (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one()
            assert head == (
                "0029_workspace_restore_plans"
                if mode == "old"
                else "0031_command_artifact_proposals"
            )
            assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
        assert "/site-packages/" in krim_agent.__file__
        print(
            json.dumps(
                {
                    "package": krim_agent.__file__,
                    "mode": mode,
                    "schema": head,
                    "original_run": state["run"],
                }
            )
        )
    finally:
        await database.close()


asyncio.run(main())
