"""实际交换两根后SIGKILL，新进程原材料/双after/原token零重放。"""

import asyncio
import json
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="真实Linux SIGKILL")


async def test_original_two_tree_exchange_sigkill_material_and_actual_after_zero_replay(tmp_path):
    (tmp_path / "files/old").mkdir(parents=True)
    (tmp_path / "files/new").mkdir()
    (tmp_path / "files/old/data.bin").write_bytes(b"old\xff\0")
    (tmp_path / "files/new/data.bin").write_bytes(b"new\0\xff")
    script = Path(__file__).parent / "fixtures/tree_exchange_process.py"
    produced = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(script),
        "produce",
        str(tmp_path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    recovered = None
    try:
        line = await asyncio.wait_for(produced.stdout.readline(), 15)
        if not line:
            _output, errors = await produced.communicate()
            raise AssertionError(errors.decode())
        metadata = json.loads(line)
        assert metadata["effect_before_ledger"] and metadata["pid"] == produced.pid
        produced.kill()
        _, errors = await asyncio.wait_for(produced.communicate(), 5)
        assert produced.returncode == -9, errors.decode()
        recovered = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(script),
            "recover",
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, errors = await asyncio.wait_for(recovered.communicate(), 15)
        assert recovered.returncode == 0, errors.decode()
        proof = json.loads(output)
        assert proof["token"] == metadata["token"] and proof["replay_calls"] == 0
        assert (
            proof["left_inode"] == metadata["left_inode"]
            and proof["right_inode"] == metadata["right_inode"]
        )
        assert proof["status"] == "done" and proof["pid"] != metadata["pid"]
        print(json.dumps({"produced": metadata, "recovered": proof}))
    finally:
        for process in (produced, recovered):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
