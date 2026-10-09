"""独立进程命令生产/崩溃后读取；同样可在项目外wheel运行。"""

import asyncio
import json
import sys

import krim_agent
from krim_agent.execution import CommandSpec, LinuxSandboxRunner, SandboxLimits
from krim_agent.execution.records import CommandRequest
from krim_agent.execution.service import DurableCommandService
from krim_agent.persistence import Database
from krim_agent.persistence.commands import CommandStore


class CountingRunner(LinuxSandboxRunner):
    def __init__(self):
        super().__init__(limits=SandboxLimits(wall_seconds=30))
        self.calls = 0

    async def start(self, *args, **kwargs):
        self.calls += 1
        return await super().start(*args, **kwargs)


async def main():
    path, scope, mode = sys.argv[1:]
    database = Database(f"sqlite+aiosqlite:///{path}")
    await database.initialize()
    store = CommandStore(database, lease_seconds=1.5)
    runner = CountingRunner()
    try:
        async with DurableCommandService(store, runner, owned=True) as service:
            previous = await store.by_key(scope, "process")
            if previous is None:
                code = (
                    "import time;print('ready',flush=True);time.sleep(30)"
                    if mode == "crash"
                    else "import uuid;print(str(uuid.uuid4()));"
                    "open('out','wb').write(open('in','rb').read()+b'\\xff')"
                )
                request = CommandRequest(
                    request_key="process",
                    command=CommandSpec(argv=["/usr/bin/python3", "-I", "-c", code]),
                )
            else:
                request = previous.request
            handle = await service.start(scope, request, {"in": b"input\0"})
            if mode == "crash":
                async with asyncio.timeout(10):
                    while True:
                        view = await handle.view()
                        if view.stdout_bytes:
                            break
                        assert not view.terminal, view
                        await asyncio.sleep(0.05)
                print(json.dumps({"command_id": handle.command_id, "ready": True}), flush=True)
                await asyncio.Event().wait()
            result = await handle.wait()
            output = await handle.output()
            artifacts = await handle.files()
            print(
                json.dumps(
                    {
                        "package": krim_agent.__file__,
                        "command_id": handle.command_id,
                        "phase": result.phase,
                        "outcome": result.outcome,
                        "dispatched": result.dispatched,
                        "started": result.started,
                        "processes_reaped": result.processes_reaped,
                        "stdout": output[0].decode(),
                        "calls": runner.calls,
                        "binary_artifact": artifacts.get("out").data == b"input\0\xff"
                        if "out" in artifacts
                        else False,
                        "events": await store.events(scope, handle.command_id),
                    }
                ),
                flush=True,
            )
    finally:
        await database.close()


asyncio.run(main())
