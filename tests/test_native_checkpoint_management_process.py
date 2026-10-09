"""真实服务重启后，HTTP/CLI继续操作原检查点与恢复计划。"""

import asyncio
import json
import sys
from pathlib import Path

import pytest

import krim_agent

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="需要Linux文件事务")


async def test_checkpoint_api_and_cli_across_three_real_service_processes(tmp_path):
    root = tmp_path / "files"
    root.mkdir()
    (root / "file.txt").write_text("A")
    first = None
    for mode in ("create", "execute", "repeat"):
        child = await asyncio.create_subprocess_exec(
            sys.executable,
            str(Path(__file__).parent / "fixtures/native_checkpoint_management_worker.py"),
            str(tmp_path),
            mode,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            output, error = await asyncio.wait_for(child.communicate(), 55)
            assert child.returncode == 0, error.decode()
            result = json.loads(output)
            assert result["cli_verified"]
            assert Path(result["package"]) == Path(krim_agent.__file__)
            if first is None:
                first = result
            else:
                assert (result["agent_id"], result["checkpoint_id"], result["plan_id"]) == (
                    first["agent_id"],
                    first["checkpoint_id"],
                    first["plan_id"],
                )
            assert result["status"] == ("ready" if mode == "create" else "done")
        finally:
            if child.returncode is None:
                child.kill()
                await asyncio.wait_for(child.communicate(), 10)
    assert (root / "file.txt").read_text() == "C"
    assert not (root / "absent.txt").exists()
