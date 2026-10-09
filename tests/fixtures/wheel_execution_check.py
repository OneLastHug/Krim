"""项目外wheel的实际隔离、标准库监督器资源与有界结果。"""

import asyncio
import json

import krim_agent
from krim_agent.execution import CommandSpec, ExecutionArtifact, LinuxSandboxRunner


async def main():
    assert "/site-packages/" in krim_agent.__file__, krim_agent.__file__
    async with LinuxSandboxRunner() as runner:
        handle = await runner.start(
            CommandSpec(
                argv=[
                    "/usr/bin/python3",
                    "-I",
                    "-c",
                    "import os;from pathlib import Path;"
                    "assert not Path('/etc/shadow').exists();assert os.getppid()==1;"
                    "Path('out.bin').write_bytes(Path('in.bin').read_bytes()+b'\\xff');"
                    "print('wheel-isolated')",
                ]
            ),
            {"in.bin": b"\0"},
        )
        result = await handle.wait()
        assert result.status == "completed" and result.stdout == b"wheel-isolated\n", result
        assert result.artifacts_complete
        assert ExecutionArtifact("out.bin", b"\0\xff", 0o644) in result.artifacts
        print(
            json.dumps(
                {
                    "package": krim_agent.__file__,
                    "isolated": True,
                    "broker_is_pid_one": True,
                    "binary_artifact": True,
                    "bubblewrap": runner.version,
                }
            )
        )


asyncio.run(main())
