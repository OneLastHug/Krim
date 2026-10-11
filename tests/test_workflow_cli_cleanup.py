"""CLI测试进程超时后必须回收，避免污染后续Workflow/Checkpoint回归。"""

import asyncio
import sys

import pytest
from test_workflow_api_integration import run_cli


@pytest.mark.parametrize("stop", ["timeout", "cancel"])
async def test_cli_timeout_reaps_its_original_process(monkeypatch, stop):
    original_spawn = asyncio.create_subprocess_exec
    original_wait = asyncio.wait_for
    processes = []
    entered = asyncio.Event()

    async def spawn(_executable, *_arguments, **kwargs):
        # 真实合成子进程，只验证超时收束契约，不启动业务命令或外部请求。
        process = await original_spawn(
            sys.executable, "-c", "import time; time.sleep(60)", **kwargs
        )
        processes.append(process)
        entered.set()
        return process

    async def short_wait(awaitable, _timeout):
        return await original_wait(awaitable, 0.05)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(asyncio, "wait_for", short_wait)
    try:
        if stop == "timeout":
            with pytest.raises(TimeoutError):
                await run_cli("http://127.0.0.1:1", "workflow", "get", "synthetic")
        else:
            task = asyncio.create_task(
                run_cli("http://127.0.0.1:1", "workflow", "get", "synthetic")
            )
            await original_wait(entered.wait(), 5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert len(processes) == 1
        assert processes[0].returncode is not None, "超时的原CLI子进程仍未回收"
    finally:
        for process in processes:
            if process.returncode is None:
                process.kill()
            await original_wait(process.communicate(), 5)
