"""当前宿主SIGKILL前后原内核组状态；只检查本测试创建的专用组。"""

import asyncio
import json
import sys
from pathlib import Path

import pytest
from test_execution_tree_quotas import tree_root as tree_root


@pytest.mark.parametrize("phase", ["before_run", "started"])
async def test_real_host_sigkill_stops_original_namespace_tree_and_only_own_group(
    tree_root, tmp_path, phase
):
    fixture = Path(__file__).parent / "fixtures/execution_tree_parent_process.py"
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(fixture),
        phase,
        str(tree_root.path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        line = await asyncio.wait_for(process.stdout.readline(), 20)
        if not line:
            _, error = await process.communicate()
            raise AssertionError(error.decode())
        value = json.loads(line)
        assert value["host_pid"] == process.pid and value["phase"] == phase
        group = tree_root.path / ("krim-exec-" + value["execution_id"])
        before = await asyncio.to_thread((group / "cgroup.events").read_text)
        assert "populated 1" in before
        process.kill()
        _, error = await asyncio.wait_for(process.communicate(), 10)
        assert process.returncode == -9, error.decode()
        for _ in range(200):
            current = await asyncio.to_thread((group / "cgroup.events").read_text)
            if "populated 0" in current:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("宿主死亡后本测试组仍有存活进程")
        (tmp_path / "kernel-events.json").write_text(
            json.dumps(
                {
                    "point": phase,
                    "before": before,
                    "after": current,
                    "execution_id": value["execution_id"],
                }
            )
        )
        tree_root.recover_empty(value["execution_id"])
        assert not group.exists()
    finally:
        if process.returncode is None:
            process.kill()
            _, error = await process.communicate()
            (tmp_path / "producer-stderr.log").write_bytes(error)
