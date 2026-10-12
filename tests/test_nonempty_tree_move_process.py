"""真实SIGKILL原rename之后，不借读回触发第二个namespace动作。"""

import asyncio
import json
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="需要实际renameat2/SIGKILL"
)


async def test_sigkill_after_original_nonempty_move_reconciles_same_token_without_replay(tmp_path):
    files = tmp_path / "files"
    (files / "tree/sub/empty").mkdir(parents=True)
    (files / "dest").mkdir()
    (files / "tree/sub/data.bin").write_bytes(b"\0\xff")
    script = Path(__file__).parent / "fixtures/nonempty_tree_move_process.py"
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(script),
        "after-move",
        str(tmp_path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    recovery = None
    try:
        line = await asyncio.wait_for(process.stdout.readline(), 10)
        if not line:
            _output, error = await process.communicate()
            raise AssertionError(error.decode())
        original = json.loads(line)
        assert original["pid"] == process.pid and original["original_renames"] == 1
        assert original["original_effect_observed"]
        process.kill()
        _output, errors = await asyncio.wait_for(process.communicate(), 10)
        assert process.returncode == -9, errors.decode()
        recovery = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(script),
            "recover",
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, errors = await asyncio.wait_for(recovery.communicate(), 10)
        assert recovery.returncode == 0, errors.decode()
        recovered = json.loads(output)
        assert recovered["token"] == original["token"] and recovered["before"] == original["before"]
        assert recovered["status"] == "done" and recovered["replay_renames"] == 0
        assert recovered["original_nodes"] == 4 and recovered["product_persistent_store"] is False
        assert (files / "dest/tree/sub/data.bin").read_bytes() == b"\0\xff"
        assert not (files / "tree").exists()
    finally:
        for child in (process, recovery):
            if child is not None and child.returncode is None:
                child.kill()
                await child.communicate()
