"""真实交换强杀后，另一进程使用新Codec恢复同token/双identity，零重放。"""

import asyncio
import json
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="真实Linux SIGKILL")


@pytest.mark.parametrize("pause", ["namespace", "after"])
async def test_strict_dual_material_sigkill_before_or_after_result_record_zero_replay(
    tmp_path, pause
):
    (tmp_path / "files/old").mkdir(parents=True)
    (tmp_path / "files/new").mkdir()
    (tmp_path / "files/old/data.bin").write_bytes(b"old\xff\0")
    (tmp_path / "files/new/data.bin").write_bytes(b"new\0\xff")
    script = Path(__file__).parent / "fixtures/tree_exchange_codec_process.py"
    produced = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(script),
        "produce",
        str(tmp_path),
        pause,
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
        original = json.loads(line)
        assert original["pid"] == produced.pid and original["pause"] == pause
        produced.kill()
        _, errors = await asyncio.wait_for(produced.communicate(), 5)
        assert produced.returncode == -9, errors.decode()
        recovered = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(script),
            "recover",
            str(tmp_path),
            pause,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, errors = await asyncio.wait_for(recovered.communicate(), 15)
        assert recovered.returncode == 0, errors.decode()
        proof = json.loads(output)
        assert proof["token"] == original["token"] and proof["replay_calls"] == 0
        assert proof["left_inode"] == original["left_inode"]
        assert proof["right_inode"] == original["right_inode"]
        assert proof["status"] == "done" and proof["pid"] != original["pid"]
        assert proof["recorded_after"] == (pause == "after")
        print(json.dumps({"produced": original, "recovered": proof}))
    finally:
        for process in (produced, recovered):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
