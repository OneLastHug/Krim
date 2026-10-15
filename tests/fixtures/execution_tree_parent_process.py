"""真实宿主被强杀：原ready/run前或用户命令启动后均不能遗留组内后代。"""

import asyncio
import json
import os
import sys

from krim_agent.execution import CgroupRoot, CommandSpec, LinuxSandboxRunner, SandboxLimits
from krim_agent.execution.tree_models import SandboxTreeLimits


async def main():
    phase, path = sys.argv[1:3]
    with CgroupRoot(path) as root:
        async with LinuxSandboxRunner(
            limits=SandboxLimits(tree=SandboxTreeLimits()), cgroup_root=root
        ) as runner:
            count = 0
            handle = None

            async def authorize():
                nonlocal count
                count += 1
                if phase == "before_run" and count == 2:
                    print(
                        json.dumps(
                            {
                                "phase": phase,
                                "host_pid": os.getpid(),
                                "execution_id": handle.execution_id,
                            }
                        ),
                        flush=True,
                    )
                    await asyncio.to_thread(sys.stdin.readline)
                    raise AssertionError("应在原run门前被强杀")

            handle = await runner.start(
                CommandSpec(
                    argv=[
                        "/usr/bin/python3",
                        "-c",
                        "import time; print('BODY_STARTED',flush=True); time.sleep(60)",
                    ]
                ),
                authorize=authorize,
            )
            if phase == "started":
                assert await handle.wait_started()
                for _ in range(300):
                    if b"BODY_STARTED" in handle.output()[0]:
                        break
                    await asyncio.sleep(0.01)
                else:
                    raise AssertionError("真实用户命令未报告启动")
                print(
                    json.dumps(
                        {
                            "phase": phase,
                            "host_pid": os.getpid(),
                            "execution_id": handle.execution_id,
                        }
                    ),
                    flush=True,
                )
            await handle.wait()


asyncio.run(main())
