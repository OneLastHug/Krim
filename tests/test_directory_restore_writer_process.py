"""物理原子恢复真实namespace后强杀，另一进程同token/双inode只核实不再生产。"""

import asyncio
import json
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux SIGKILL")


@pytest.mark.parametrize("mutate", [False, True])
async def test_directory_restore_sigkill_reconcile_original_or_ambiguous_zero_replay(
    tmp_path, mutate
):
    (tmp_path / "files/holder/live/sub/empty").mkdir(parents=True)
    (tmp_path / "files/holder/live/sub/raw.bin").write_bytes(b"\0\xff")
    script = Path(__file__).parent / "fixtures/directory_restore_writer_process.py"
    producer = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(script),
        "produce",
        str(tmp_path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    recovery = None
    try:
        line = await asyncio.wait_for(producer.stdout.readline(), 15)
        if not line:
            _, errors = await producer.communicate()
            raise AssertionError(errors.decode())
        original = json.loads(line)
        assert original["pid"] == producer.pid and original["namespace_before_receipt"]
        producer.kill()
        _, errors = await asyncio.wait_for(producer.communicate(), 5)
        assert producer.returncode == -9, errors.decode()
        if mutate:
            (tmp_path / "files/holder/live/sub/raw.bin").write_bytes(b"other\0mutation")
        recovery = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(script),
            "recover",
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, errors = await asyncio.wait_for(recovery.communicate(), 15)
        assert recovery.returncode == 0, errors.decode()
        proof = json.loads(output)
        assert proof["token"] == original["token"] and proof["pid"] != original["pid"]
        assert proof["status"] == ("uncertain" if mutate else "done") and proof["replays"] == 0
        if not mutate:
            assert (
                proof["after_inode"] == original["staged_inode"]
                and proof["retained_inode"] == original["before_inode"]
            )
            assert proof["binary_hex"] == "00ff"
        else:
            assert proof["binary_hex"] == b"other\0mutation".hex()
        print(json.dumps({"producer": original, "recovery": proof}))
    finally:
        for process in (producer, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
