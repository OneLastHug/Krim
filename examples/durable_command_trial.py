"""Krim Agent持久命令演练：真实产物、关闭服务、冷读取原记录。"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from krim_agent.execution import CommandSpec, LinuxSandboxRunner
from krim_agent.execution.records import CommandRequest
from krim_agent.execution.service import DurableCommandService
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.commands import CommandStore


async def main() -> None:
    with TemporaryDirectory(prefix="krim-command-trial-") as directory:
        url = f"sqlite+aiosqlite:///{Path(directory) / 'commands.sqlite'}"
        database = Database(url)
        await database.initialize()
        try:
            topic = await KrimRepository(database).create_topic("命令记录演练")
            scope = "topic:" + topic.id
            async with DurableCommandService(
                CommandStore(database), LinuxSandboxRunner(), owned=True
            ) as service:
                handle = await service.start(
                    scope,
                    CommandRequest(
                        request_key="trial",
                        command=CommandSpec(
                            argv=[
                                "/usr/bin/python3",
                                "-I",
                                "-c",
                                "from pathlib import Path; "
                                "Path('out.txt').write_text(Path('in.txt').read_text().upper()); "
                                "print('saved command')",
                            ]
                        ),
                    ),
                    {"in.txt": b"Krim Agent\n"},
                )
                finished = await handle.wait()
                assert finished.outcome == "completed" and finished.processes_reaped
                identifier = handle.command_id
        finally:
            await database.close()

        # 冷读取只需要数据库，不创建执行器，也不重新运行原命令。
        cold = Database(url)
        await cold.initialize()
        try:
            store = CommandStore(cold)
            saved = await store.by_key(scope, "trial")
            assert saved is not None and saved.command_id == identifier
            output = await store.output(scope, identifier)
            files = await store.files(scope, identifier, "artifact")
            assert output == (b"saved command\n", b"")
            assert files["out.txt"].data == b"KRIM AGENT\n"
            print(
                json.dumps(
                    {
                        "command_id": identifier,
                        "outcome": saved.outcome,
                        "cold_reader_created_runner": False,
                        "output": output[0].decode(),
                        "artifact": files["out.txt"].data.decode(),
                        "events": await store.events(scope, identifier),
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
        finally:
            await cold.close()


if __name__ == "__main__":
    asyncio.run(main())
