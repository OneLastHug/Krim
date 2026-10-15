"""原命令效果/整树统计SQL提交后强杀，冷恢复只读原账本。"""

import asyncio
import json
import os
import sys
from pathlib import Path

import httpx

import krim_agent.api.server as server_module
from krim_agent.config import Settings
from krim_agent.execution import CgroupRoot, LinuxSandboxRunner
from krim_agent.execution.cgroups import CgroupLease
from krim_agent.providers import FakeProvider
from krim_agent.sdk import CommandLookup, PythonAI

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_agent_commands import request  # noqa: E402
from test_execution_tree_consumers import setup  # noqa: E402


async def main():
    phase, directory, cgroup = sys.argv[1:4]
    root = Path(directory)
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=FakeProvider(delay=0),
    ) as ai:
        with CgroupRoot(cgroup) as kernel:
            if phase == "produce":
                service, agent, _ = await setup(ai.runtime, kernel, persistent=True)
                original = service.manager.commands.store.finish

                async def hold(scope, identifier, token, result):
                    view = await original(scope, identifier, token, result)
                    resource = await service.manager.commands.store.resources(scope, identifier)
                    value = {
                        "host_pid": os.getpid(),
                        "agent_id": agent.agent_id,
                        "command_id": identifier,
                        "resource": resource.model_dump(mode="json"),
                        "execution_id": result.execution_id,
                    }
                    (root / "metadata.json").write_text(json.dumps(value))
                    print(json.dumps(value), flush=True)
                    await asyncio.to_thread(sys.stdin.readline)
                    return view

                service.manager.commands.store.finish = hold
                await agent.execute_command(
                    request(
                        "open('artifact.bin','wb').write(b'\\0\\xff'); print('tree-ledger')",
                        key="original-tree",
                    )
                )
                await asyncio.Future()
            else:
                metadata = json.loads((root / "metadata.json").read_text())
                service = ai.agents()
                from test_execution_tree_quotas import limits

                runner = LinuxSandboxRunner(limits=limits(), cgroup_root=kernel)
                service.register_execution("tree", runner, version="1", owned=True)
                calls = 0

                def forbidden(*_args, **_kwargs):
                    nonlocal calls
                    calls += 1
                    raise AssertionError("原统计冷读取不能重跑Model/内核/命令")

                FakeProvider.stream = forbidden
                CgroupLease.observe = forbidden
                runner.start = forbidden
                assert await service.restore_persistent() == 1, service.restore_errors
                agent = ai.agent_session(metadata["agent_id"])
                resources = await agent.command_resources(
                    CommandLookup(command_id=metadata["command_id"])
                )
                assert resources.model_dump(mode="json") == metadata["resource"]
                assert resources.processes_reaped and resources.usage.populated is False
                data = await service.manager.commands.store.artifact(
                    resources.scope_id, resources.command_id, "artifact.bin"
                )
                assert data.data == b"\0\xff"
                server_module.AgentService = lambda *_args, **_kwargs: service
                app = server_module.create_app(runtime=ai.runtime)
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
                ) as api:
                    reply = await api.get(
                        f"/v1/agents/{agent.agent_id}/commands/{resources.command_id}/resources"
                    )
                    assert reply.status_code == 200 and reply.json() == metadata["resource"]
                assert calls == 0
                print(
                    json.dumps(
                        {
                            "command_id": resources.command_id,
                            "scope_id": resources.scope_id,
                            "replay_calls": calls,
                            "same_sdk_http": True,
                            "usage": resources.usage.model_dump(mode="json"),
                        }
                    ),
                    flush=True,
                )


asyncio.run(main())
