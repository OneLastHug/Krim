"""Krim Agent隔离命令基础演练：文件副本、真实输出、产物与取消。"""

from __future__ import annotations

import asyncio
import json

from krim_agent.execution import CommandSpec, LinuxSandboxRunner, SandboxLimits


async def main() -> None:
    # 输入来自可信宿主；执行器只接收副本，不接受原工作区的挂载路径。
    inputs = {"input.txt": b"hello from Krim Agent\n"}
    async with LinuxSandboxRunner(limits=SandboxLimits(wall_seconds=5)) as runner:
        handle = await runner.start(
            CommandSpec(
                argv=[
                    "/usr/bin/python3",
                    "-I",
                    "-c",
                    "from pathlib import Path; import os; "
                    "Path('output.txt').write_text(Path('input.txt').read_text().upper()); "
                    "print('cwd=' + os.getcwd())",
                ]
            ),
            inputs,
        )
        result = await handle.wait()
        assert result.status == "completed" and result.artifacts_complete, result
        artifacts = {item.path: item.data for item in result.artifacts}
        assert artifacts["output.txt"] == b"HELLO FROM KRIM AGENT\n"
        assert inputs["input.txt"] == b"hello from Krim Agent\n"

        # cancel只请求停止；wait返回原执行收束后的事实，不创建第二次执行。
        waiting = await runner.start(
            CommandSpec(argv=["/usr/bin/python3", "-I", "-c", "import time; time.sleep(30)"])
        )
        assert await waiting.wait_started()
        waiting.cancel()
        cancelled = await waiting.wait()
        assert cancelled.status == "cancelled" and cancelled.started, cancelled
        print(
            json.dumps(
                {
                    "backend": runner.version,
                    "policy_fingerprint": runner.fingerprint,
                    "completed": result.status,
                    "exit_code": result.exit_code,
                    "stdout": result.stdout.decode("utf-8"),
                    "artifacts": {key: value.decode("utf-8") for key, value in artifacts.items()},
                    "input_unchanged": True,
                    "cancelled": cancelled.status,
                },
                ensure_ascii=False,
                indent=2,
            )
        )


if __name__ == "__main__":
    asyncio.run(main())
