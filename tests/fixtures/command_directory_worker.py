"""实际独立进程保存/冷读取目录来源，读取进程没有执行器。"""

import asyncio
import json
import sys
from pathlib import Path

import krim_agent
from krim_agent.execution import CommandSpec, LinuxSandboxRunner
from krim_agent.execution.records import CommandRequest
from krim_agent.execution.service import DurableCommandHandle, DurableCommandService
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.commands import CommandStore


async def main():
    folder, mode = sys.argv[1:]
    root = Path(folder)
    database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
    await database.initialize()
    try:
        store = CommandStore(database)
        if mode == "write":
            repository = KrimRepository(database)
            scope = "topic:" + (await repository.create_topic("directory process")).id
            runner = LinuxSandboxRunner()
            service = DurableCommandService(store, runner, owned=True)
            try:
                handle = await service.start(
                    scope,
                    CommandRequest(
                        request_key="directories",
                        command=CommandSpec(
                            argv=[
                                "/usr/bin/python3",
                                "-I",
                                "-c",
                                "import os;os.mkdir('empty');os.chmod('empty',0o750);"
                                "os.makedirs('nested/empty');open('out.bin','wb').write(bytes([0,255]))",
                            ]
                        ),
                    ),
                    {"input/file.txt": b"original"},
                    directories=("explicit/empty",),
                )
                assert (await handle.wait()).outcome == "completed"
                state = {"scope": scope, "command_id": handle.command_id}
                (root / "receipt.json").write_text(json.dumps(state))
                catalog = await handle.directory_catalog()
            finally:
                await service.close()
        else:
            state = json.loads((root / "receipt.json").read_text())
            reader = DurableCommandHandle(store, state["scope"], state["command_id"])
            catalog = await reader.directory_catalog()
            assert (await reader.files())["out.bin"].data == b"\0\xff"
        assert catalog.recorded and catalog.complete
        print(
            json.dumps({"package": krim_agent.__file__, "catalog": catalog.model_dump(mode="json")})
        )
    finally:
        await database.close()


asyncio.run(main())
